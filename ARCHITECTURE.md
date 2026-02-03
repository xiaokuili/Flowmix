# Flowmix 架构文档

> 为 AI 和开发者提供的完整架构指南

---

## 📋 目录

1. [核心概念](#核心概念)
2. [架构层次](#架构层次)
3. [数据流](#数据流)
4. [关键机制](#关键机制)
5. [使用场景](#使用场景)
6. [常见误区](#常见误区)

---

## 🎯 核心概念

### 1. Task（任务定义）

**作用**: 定义一个可执行的异步任务单元

**关键属性**:
```python
class Task:
    name: str                          # 任务名称（唯一标识）
    concurrency_limit: Optional[int]   # 并发限制（每秒最大并发数）
    dedup: bool                        # 是否启用去重（结果缓存）
    dedup_ttl: Optional[int]           # 缓存过期时间（秒，None=永久）
```

**生命周期钩子**:
```python
@task.execute       # 必需：任务执行逻辑
@task.on_success    # 可选：成功后回调
@task.on_failure    # 可选：失败后回调
```

**重要**: `Task` 对象本身只是定义，真正的执行由 `TaskRunner` 驱动。

### 2. Queue（消息队列）

**作用**: 存储待执行的任务消息

**实现类型**:
- `MemoryQueue`: 内存队列（单进程，轻量测试）
- `RedisQueue`: Redis 队列（分布式，生产环境）

**核心操作**:
```python
# 推送任务到队列
msg_id = await queue.push(
    data={"url": "http://example.com"},
    task_name="crawl",
    priority=10,        # 优先级（越高越先执行）
    parent_id=None      # 父任务 ID（用于构建任务树）
)

# 拉取任务（Worker 使用）
msg = await queue.pop(worker_name="worker-1")

# 确认任务完成
await queue.ack(msg_id, failed=False, result={...})
```

### 3. TaskRunner（执行引擎）

**作用**: 管理 Worker 池，调度任务执行

**关键流程**:
```
1. 从 Queue 拉取消息
2. 检查 Cache（如果启用去重）
3. 获取 RateLimiter 令牌（如果有并发限制）
4. 执行 Task.run()
5. 释放令牌，更新缓存
6. 调用 Queue.ack() 确认完成
```

**核心配置**:
```python
config = RunnerConfig(
    num_workers=5,         # Worker 数量（并发度）
    max_retries=3,         # 失败重试次数
    retry_delay=1.0,       # 重试延迟（秒）
    batch_size=1           # 每次拉取的任务数
)
```

### 4. Pub（任务发布器）

**作用**: 推送任务到队列的便捷接口

**使用方式**:
```python
# 推荐方式：使用 URL（自动创建队列）
pub = await Pub.create(url="redis://localhost:6379/0", queue_name="tasks")

# 高级用法：直接传入 Queue 实例
from flowmix.common.queue import RedisQueue, RedisPool
pool = await RedisPool.get_instance("redis://localhost:6379/0")
queue = RedisQueue(pool=pool, queue_name="tasks")
pub = Pub(queue=queue)

# 推送任务
task_id = await pub.push(
    data={"key": "value"},
    task_name="process",
    priority=5,
    parent_id=None
)
```

### 5. Cache（结果缓存）

**作用**: 根据任务指纹（fingerprint）缓存结果，避免重复执行

**指纹生成**:
```python
# SHA256(JSON({task: "crawl", data: {"url": "..."}}))
fingerprint = hashlib.sha256(
    json.dumps({"task": task_name, "data": data}, sort_keys=True).encode()
).hexdigest()
```

**缓存策略**:
- `dedup=True`: 启用去重
- `dedup_ttl=None`: 永久缓存
- `dedup_ttl=3600`: 1小时后过期

### 6. RateLimiter（限流器）

**作用**: 控制任务并发度，防止资源耗尽

**实现方式**:
- 滑动窗口算法（1秒窗口）
- 基于任务名称独立限流

**示例**:
```python
task = Task(name="api_call", concurrency_limit=10)
# 保证 api_call 任务每秒最多10个并发
```

---

## 🏗️ 架构层次

```
┌─────────────────────────────────────────────────────────────┐
│                      用户层 (User Code)                     │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐                 │
│  │  Task    │  │   Pub    │  │  Cron    │                 │
│  │ 定义任务  │  │ 推送任务  │  │ 定时任务  │                 │
│  └──────────┘  └──────────┘  └──────────┘                 │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│                   执行层 (Execution Layer)                  │
│  ┌─────────────────────────────────────────────────────┐  │
│  │              TaskRunner (调度器)                     │  │
│  │  ┌────────┐  ┌────────┐  ┌────────┐  ┌────────┐   │  │
│  │  │Worker-1│  │Worker-2│  │Worker-3│  │Worker-N│   │  │
│  │  └────────┘  └────────┘  └────────┘  └────────┘   │  │
│  └─────────────────────────────────────────────────────┘  │
│                          ↓                                 │
│  ┌─────────────────────────────────────────────────────┐  │
│  │            TaskEngine (执行引擎)                     │  │
│  │  Cache Check → Limit Acquire → Execute → Release   │  │
│  └─────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│                  基础设施层 (Infrastructure)                │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐ │
│  │  Queue   │  │  Cache   │  │Limiter   │  │  Pool    │ │
│  │ 消息队列  │  │ 结果缓存  │  │ 限流器    │  │ 连接池   │ │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘ │
└─────────────────────────────────────────────────────────────┘
```

**分层说明**:
1. **用户层**: 声明式 API，开发者只需关注业务逻辑
2. **执行层**: 任务调度、重试、限流、缓存等运行时逻辑
3. **基础设施层**: 可插拔的后端实现（内存/Redis）

---

## 🔄 数据流

### 1. 任务提交流程

```
┌──────────┐
│ 用户代码  │
└─────┬────┘
      │ pub.push(data, task_name, priority)
      ↓
┌─────────────┐
│   Pub       │
└──────┬──────┘
       │ queue.push(...)
       ↓
┌──────────────────────┐
│  Queue (Redis/Memory)│  ← 消息持久化
└──────────────────────┘
```

### 2. 任务执行流程

```
┌───────────────────────────────────────────────────────────────┐
│                      TaskRunner                               │
│                                                               │
│  Worker Loop:                                                 │
│  ┌────────────────────────────────────────────────────────┐  │
│  │ 1. msg = queue.pop(worker_name)                        │  │
│  │    ↓                                                    │  │
│  │ 2. TaskEngine.execute(msg, task, worker_name)          │  │
│  │    ├─ Cache.check(fingerprint)                         │  │
│  │    │  ├─ Hit? → Return cached result (跳过执行)        │  │
│  │    │  └─ Miss? → Continue                              │  │
│  │    ├─ RateLimiter.acquire(task_name, limit)            │  │
│  │    │  └─ Wait until quota available                    │  │
│  │    ├─ Task.run(data, msg_id) ← 执行用户代码            │  │
│  │    │  ├─ @task.execute(data)                           │  │
│  │    │  ├─ @task.on_success(data, result)                │  │
│  │    │  └─ @task.on_failure(data, error)                 │  │
│  │    ├─ Cache.set(fingerprint, result)                   │  │
│  │    └─ RateLimiter.release(task_name)                   │  │
│  │    ↓                                                    │  │
│  │ 3. queue.ack(msg_id, failed, result, fingerprint)      │  │
│  └────────────────────────────────────────────────────────┘  │
└───────────────────────────────────────────────────────────────┘
```

### 3. 动态任务提交（task.callback）

```
┌────────────────────────────────────────────────────────────┐
│  用户代码: @task.execute                                   │
│  async def crawl(data):                                    │
│      ...                                                   │
│      await task.callback(                                 │
│          task_name="crawl",                               │
│          data={"url": child_url},                         │
│          priority=10                                      │
│      )                                                     │
└───────────────┬────────────────────────────────────────────┘
                │
                ↓
    ┌───────────────────────────────┐
    │  task._sender (由 Runner 注入)│
    └──────────┬────────────────────┘
               │
               ↓
    ┌──────────────────────────┐
    │  Pub.push(...)           │
    │  parent_id = current_msg_id  ← 自动关联父任务
    └──────────┬───────────────┘
               │
               ↓
    ┌──────────────────────────┐
    │  Queue.push(...)         │
    └──────────────────────────┘
```

**关键点**:
- `task.callback()` 只能在任务执行期间调用
- `task._sender` 和 `task._current_msg_id` 由 `TaskRunner` 在运行时注入
- `parent_id` 自动设置为当前任务的 `msg_id`，构建任务树

---

## 🔑 关键机制

### 1. 状态注入机制

**问题**: 如何让 `task.callback()` 知道当前的消息 ID 和队列？

**解决方案**: 运行时状态注入

```python
# TaskRunner 初始化时
def _setup_task_callbacks(self):
    for task in self._tasks.values():
        task._sender = self._pub  # 注入 Pub 实例

# TaskEngine 执行时
async def execute(self, msg, task, worker_name):
    task._current_msg_id = msg["id"]  # 注入当前消息 ID
    try:
        result = await task.run(msg_data, msg["id"])
    finally:
        task._current_msg_id = None  # 清理

# Task.callback 使用注入的状态
async def callback(self, task_name, data, priority=5):
    if not self._sender:
        raise RuntimeError("只能在任务执行期间调用 callback")

    return await self._sender.push(
        data=data,
        task_name=task_name,
        priority=priority,
        parent_id=self._current_msg_id  # 使用注入的 msg_id
    )
```

**对 AI 的意义**:
- 不要在任务外部调用 `task.callback()`
- `task._sender` 和 `task._current_msg_id` 是运行时依赖，不是初始化参数

### 2. 消息格式标准化

**标准格式**:
```python
{
    "id": 123,                      # 消息 ID（自增）
    "task_name": "crawl",           # 任务名称
    "data": {                       # 任务数据（标准字段）
        "url": "http://example.com",
        "depth": 0
    },
    "priority": 10,                 # 优先级
    "parent_id": None,              # 父任务 ID（用于构建任务树）
    "created_at": 1234567890.123,   # 创建时间戳
    "fingerprint": "abc123...",     # 任务指纹（用于去重）
    "retries": 0                    # 重试次数
}
```

**注意**: 旧版本可能有两种格式（扁平化格式已废弃），请统一使用 `data` 字段。

### 3. 优先级策略

**优先级范围**: 0-100（数字越大优先级越高）

**使用场景**:
- `priority >= 10`: 深度优先（DFS），适合爬虫、递归处理
- `priority < 10`: 广度优先（BFS），适合批处理、层级分析

**Redis 实现**:
```python
# Redis Sorted Set (ZSet)
# Score = priority * 1e9 + timestamp
# 先按 priority 降序，再按 timestamp 升序
ZADD queue:tasks {score} {msg_id}
```

### 4. 任务树结构

**构建方式**: 通过 `parent_id` 字段关联

```
Root Task (id=1, parent_id=None)
  ├─ Child Task A (id=2, parent_id=1)
  │   ├─ Grandchild A1 (id=5, parent_id=2)
  │   └─ Grandchild A2 (id=6, parent_id=2)
  ├─ Child Task B (id=3, parent_id=1)
  └─ Child Task C (id=4, parent_id=1)
```

**用途**:
- 任务链路追踪
- 统计分析（成功率、耗时等）
- 可视化展示

**查询**:
```python
# 获取任务树摘要
summary = await stats.task.get_chain_summary(root_id=1)
# 返回: {total: 6, success: 5, failed: 1, pending: 0}

# 获取任务树详情
details = await stats.task.get_chain_details(root_id=1)
# 递归返回所有子任务
```

### 5. 连接池复用

**单例模式**: 相同 URL 共享同一个连接池

```python
# RedisPool.get_instance(url) 返回单例
pool1 = RedisPool.get_instance("redis://localhost:6379/0")
pool2 = RedisPool.get_instance("redis://localhost:6379/0")
assert pool1 is pool2  # 同一个实例
```

**复用策略**: TaskRunner 尽量复用连接
```python
if queue_url == cache_url:
    cache._redis = queue._redis  # 复用 Queue 的 Redis 连接
```

**对 AI 的意义**: 不要手动创建多个连接池，使用框架自动管理。

---

## 💡 使用场景

### 场景 1: 网络爬虫（递归任务）

**特点**:
- 发现新 URL 后动态推送子任务
- 需要去重（相同 URL 只爬一次）
- 深度优先策略

**示例**:
```python
task = Task(name="crawl", dedup=True)

@task.execute
async def crawl(data):
    url = data["url"]
    html = await fetch(url)
    links = parse_links(html)

    # 动态推送子任务
    for link in links:
        await task.callback(
            task_name="crawl",
            data={"url": link},
            priority=10  # DFS
        )

    return {"url": url, "links": len(links)}
```

### 场景 2: 数据 ETL 管道（线性任务链）

**特点**:
- 任务间有依赖关系（A → B → C）
- 前一步完成后推送下一步

**示例**:
```python
extract_task = Task(name="extract")
transform_task = Task(name="transform")
load_task = Task(name="load")

@extract_task.on_success
async def on_extract_success(data, result):
    # 提取完成后，推送转换任务
    await extract_task.callback(
        task_name="transform",
        data={"raw": result}
    )

@transform_task.on_success
async def on_transform_success(data, result):
    # 转换完成后，推送加载任务
    await transform_task.callback(
        task_name="load",
        data={"processed": result}
    )
```

### 场景 3: API 限流调用

**特点**:
- 第三方 API 有速率限制
- 需要控制并发度

**示例**:
```python
task = Task(
    name="api_call",
    concurrency_limit=10,  # 每秒最多10个并发
    dedup=True,            # 相同请求返回缓存
    dedup_ttl=3600         # 缓存1小时
)

@task.execute
async def call_api(data):
    response = await httpx.get(data["url"])
    return response.json()
```

### 场景 4: 定时任务

**特点**:
- 周期性执行（每小时、每天）
- 支持 cron 表达式

**示例**:
```python
# 推荐方式：使用 URL
cron = await Cron.create(url="redis://localhost:6379/0", queue_name="tasks")

# 每小时执行
cron.add_interval(
    task_name="hourly_report",
    data_fn=lambda: {"timestamp": time.time()},
    hours=1
)

# 每天 2:00 AM 执行
cron.add_cron(
    task_name="daily_backup",
    data_fn=lambda: {},
    hour=2,
    minute=0
)

cron.start()
```

---

## ⚠️ 常见误区

### 误区 1: 在任务外调用 callback

❌ **错误示例**:
```python
task = Task(name="test")

@task.execute
async def execute(data):
    return data

# 错误：在任务外部调用
await task.callback(task_name="test", data={})  # RuntimeError!
```

✅ **正确示例**:
```python
@task.execute
async def execute(data):
    # 只能在任务执行期间调用
    await task.callback(task_name="test", data={})
    return data
```

### 误区 2: 混淆 Task 和 TaskRunner

❌ **错误理解**: "Task 会自动执行"

✅ **正确理解**:
- `Task` 只是定义（静态配置）
- `TaskRunner` 才是执行引擎（运行时）

```python
# 只定义，不会执行
task = Task(name="test")

# 必须由 TaskRunner 驱动
runner = TaskRunner(tasks={"test": task}, url="redis://...")
await runner.run()
```

### 误区 3: 忽略消息格式

❌ **错误**: 直接传递复杂对象
```python
await pub.push(
    data={"user": User(id=1)},  # User 对象不可序列化
    task_name="process"
)
```

✅ **正确**: 只传递可序列化的数据
```python
await pub.push(
    data={"user_id": 1},  # 只传 ID
    task_name="process"
)
```

### 误区 4: 不理解优先级

❌ **错误**: 认为 priority 只是排序
```python
# 期望: 先执行 priority=100 的任务
await pub.push(data={}, task_name="urgent", priority=100)
await pub.push(data={}, task_name="normal", priority=1)
```

✅ **正确理解**: priority 影响执行顺序，但不是绝对的
- Redis 是 ZSET 结构，score = priority * 1e9 + timestamp
- 同优先级按时间戳排序（FIFO）

### 误区 5: 忘记处理 Redis URL

❌ **错误**: 使用错误的 URL scheme
```python
runner = TaskRunner(
    tasks={...},
    url="redis://localhost:6379"  # 缺少 database
)
```

✅ **正确**: 指定 database
```python
runner = TaskRunner(
    tasks={...},
    url="redis://localhost:6379/0"  # 使用 DB 0
)
```

**支持的 URL schemes**:
- `redis://`: 标准 Redis
- `rediss://`: Redis with SSL/TLS
- `memory://`: 内存队列（单进程测试）

---

## 🔧 扩展点

### 1. 自定义 Queue 实现

```python
from flowmix.common.queue import Queue

class MyQueue(Queue):
    async def push(self, data, priority, parent_id, task_name):
        # 自定义推送逻辑
        pass

    async def pop(self, consumer_name):
        # 自定义拉取逻辑
        pass

    async def ack(self, msg_id, failed, error, result, fingerprint):
        # 自定义确认逻辑
        pass
```

### 2. 自定义 Cache 实现

```python
from flowmix.runner.cache import Cache

class MyCache(Cache):
    def generate_fingerprint(self, task_name, data):
        # 自定义指纹生成
        return custom_hash(task_name, data)

    async def check(self, task_name, data, ttl):
        # 自定义缓存查询
        pass

    async def set(self, task_name, data, result):
        # 自定义缓存存储
        pass
```

### 3. 自定义 RateLimiter 实现

```python
from flowmix.runner.limit import RateLimiter

class MyRateLimiter(RateLimiter):
    async def acquire(self, task_name, limit, timeout):
        # 自定义限流逻辑
        pass

    async def release(self, task_name):
        # 自定义释放逻辑
        pass
```

---

## 📊 性能优化建议

### 1. 合理设置 num_workers

```python
# CPU 密集型任务
num_workers = os.cpu_count()

# I/O 密集型任务（爬虫、API 调用）
num_workers = os.cpu_count() * 10
```

### 2. 使用 Redis 而非 Memory

```python
# ❌ 单进程，无法水平扩展
runner = TaskRunner(url="memory://", ...)

# ✅ 分布式，支持多进程/多机器
runner = TaskRunner(url="redis://localhost:6379/0", ...)
```

### 3. 启用去重和缓存

```python
# 对于幂等任务，启用去重
task = Task(name="fetch", dedup=True, dedup_ttl=3600)
```

### 4. 合理设置并发限制

```python
# 避免 API 限流
task = Task(name="api", concurrency_limit=10)
```

### 5. 批量推送任务

```python
# ❌ 逐个推送（慢）
for item in items:
    await pub.push(data=item, task_name="process")

# ✅ 使用 pipeline（快）
async with queue._redis.pipeline() as pipe:
    for item in items:
        await pub.push(data=item, task_name="process")
    await pipe.execute()
```

---

## 📚 参考资料

- [README.md](README.md) - 快速开始
- [examples/](examples/) - 示例代码
- [QUICK_START.md](QUICK_START.md) - 5分钟入门
- [.claude/project.md](.claude/project.md) - 项目背景

---

## 🤝 为 AI 设计的提示

如果你是 AI 助手，请遵循以下原则：

1. **理解运行时注入**: `task._sender` 和 `task._current_msg_id` 是运行时依赖
2. **使用标准消息格式**: 始终使用 `data` 字段，不要扁平化
3. **不要猜测状态**: 消息 ID、指纹等由框架生成，不要手动创建
4. **遵循优先级语义**: `priority >= 10` 表示 DFS，`< 10` 表示 BFS
5. **注意序列化**: `data` 必须是 JSON 可序列化的字典
6. **区分定义和执行**: Task 是定义，TaskRunner 是执行
7. **理解任务树**: `parent_id` 自动关联，不要手动设置

**生成代码时的检查清单**:
- [ ] Task 是否在 TaskRunner 的 `tasks` 字典中注册？
- [ ] `task.callback()` 是否只在 `@task.execute` 内调用？
- [ ] `data` 参数是否是可序列化的字典？
- [ ] URL scheme 是否正确（`redis://`、`rediss://`、`memory://`）？
- [ ] 是否需要设置 `concurrency_limit` 或 `dedup`？

---

*文档版本: v1.0.0*
*最后更新: 2025-01-XX*
