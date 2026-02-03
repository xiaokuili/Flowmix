# Flowmix 快速开始指南

> 5 分钟上手 Flowmix 异步任务队列

---

## 📝 目录

1. [核心概念](#核心概念)
2. [3 步创建任务](#3-步创建任务)
3. [完整示例](#完整示例)
4. [常见场景](#常见场景)
5. [下一步](#下一步)

---

## 🎯 核心概念

Flowmix 只有 **3 个核心概念**：

```
┌──────────┐    ┌──────────┐    ┌──────────────┐
│   Task   │ →  │   Pub    │ →  │  TaskRunner  │
│ 定义任务  │    │ 推送任务  │    │  执行任务    │
└──────────┘    └──────────┘    └──────────────┘
```

1. **Task**: 定义"做什么"（执行逻辑）
2. **Pub**: 推送任务到队列
3. **TaskRunner**: 从队列拉取并执行任务

---

## 🚀 3 步创建任务

### 步骤 1: 定义 Task

```python
from flowmix import Task

# 创建任务定义
task = Task(name='print')

# 注册执行函数
@task.execute
async def execute_print(data):
    message = data['message']
    print(f"📝 Processing: {message}")
    return {"status": "ok"}
```

**关键点**:
- `Task` 只是定义，不会自动执行
- `@task.execute` 装饰器注册执行逻辑
- `data` 必须是字典（可 JSON 序列化）

### 步骤 2: 推送任务

```python
from flowmix.sender import Pub
from flowmix.common.queue import MemoryQueue

# 创建队列和发布器
queue = MemoryQueue(queue_name="tasks")
pub = Pub(queue=queue)

# 推送任务
task_id = await pub.push(
    data={"message": "Hello, Flowmix!"},
    task_name="print"  # 必须和 Task 的 name 匹配
)
```

**关键点**:
- `task_name` 必须和 `Task(name='...')` 匹配
- `data` 是传给 `@task.execute` 函数的参数
- 返回的 `task_id` 可用于追踪任务

### 步骤 3: 启动 Runner

```python
from flowmix import TaskRunner, RunnerConfig

# 创建 Runner
runner = TaskRunner(
    tasks={"print": task},  # 注册任务
    url="memory://",        # 内存队列
    config=RunnerConfig(num_workers=5)
)

# 启动执行
await runner.run()
```

**关键点**:
- `tasks` 字典的 key 必须和 `task_name` 匹配
- `url="memory://"` 适合单进程测试
- `url="redis://localhost:6379/0"` 适合生产环境

---

## 📦 完整示例

### 示例 1: Hello World

```python
import asyncio
from flowmix import Task, TaskRunner, RunnerConfig
from flowmix.sender import Pub
from flowmix.common.queue import MemoryQueue

# 1. 定义 Task
task = Task(name='greet')

@task.execute
async def greet(data):
    name = data.get('name', 'World')
    print(f"Hello, {name}!")
    return {"greeted": name}

# 2. 推送任务
async def main():
    queue = MemoryQueue(queue_name="tasks")
    pub = Pub(queue=queue)

    # 推送 3 个任务
    await pub.push(data={"name": "Alice"}, task_name="greet")
    await pub.push(data={"name": "Bob"}, task_name="greet")
    await pub.push(data={"name": "Charlie"}, task_name="greet")

    # 3. 启动 Runner
    runner = TaskRunner(
        tasks={"greet": task},
        url="memory://",
        config=RunnerConfig(num_workers=1)
    )
    await runner.run()

asyncio.run(main())
```

**输出**:
```
Hello, Alice!
Hello, Bob!
Hello, Charlie!
```

### 示例 2: 使用 Redis（生产环境）

```python
# 只需修改 URL 和 queue_name
runner = TaskRunner(
    tasks={"greet": task},
    url="redis://localhost:6379/0",  # 使用 Redis
    queue_name="tasks",
    config=RunnerConfig(num_workers=10)  # 10 个并发 Worker
)
```

---

## 💡 常见场景

### 场景 1: 任务失败处理

```python
task = Task(name='fetch')

@task.execute
async def fetch(data):
    url = data['url']
    if not url.startswith('http'):
        raise ValueError("Invalid URL")
    return await httpx.get(url)

@task.on_success
async def on_success(data, result):
    print(f"✅ Success: {data['url']}")

@task.on_failure
async def on_failure(data, error):
    print(f"❌ Failed: {data['url']} - {error}")
```

### 场景 2: 动态任务提交（递归任务）

```python
task = Task(name='crawl')

@task.execute
async def crawl(data):
    url = data['url']
    html = await fetch(url)
    links = parse_links(html)

    # 动态提交子任务
    for link in links:
        await task.callback(
            task_name='crawl',
            data={'url': link},
            priority=10  # 深度优先
        )

    return {"url": url, "links": len(links)}
```

**关键点**:
- `task.callback()` 只能在任务执行期间调用
- 自动关联 `parent_id`，构建任务树
- `priority >= 10` 表示深度优先（DFS）

### 场景 3: 任务去重

```python
task = Task(
    name='fetch',
    dedup=True,        # 启用去重
    dedup_ttl=3600     # 1 小时内相同任务返回缓存
)

@task.execute
async def fetch(data):
    url = data['url']
    print(f"Fetching {url}...")  # 只执行一次
    return await httpx.get(url)

# 推送相同任务
await pub.push(data={"url": "http://example.com"}, task_name="fetch")
await pub.push(data={"url": "http://example.com"}, task_name="fetch")
# 第二次会命中缓存，不会重复执行
```

### 场景 4: 并发控制

```python
task = Task(
    name='api_call',
    concurrency_limit=10  # 每秒最多 10 个并发
)

@task.execute
async def api_call(data):
    # 调用第三方 API（有速率限制）
    return await third_party_api.call(data)
```

### 场景 5: 定时任务

```python
from flowmix.sender import Cron

cron = Cron(queue=queue)

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

## 🎓 常见错误

### ❌ 错误 1: 在任务外调用 callback

```python
task = Task(name='test')

# ❌ 错误
await task.callback(task_name='test', data={})
# RuntimeError: Task is not attached to a Runner
```

✅ **正确做法**: 只在 `@task.execute` 内调用

```python
@task.execute
async def execute(data):
    await task.callback(task_name='test', data={})  # ✅ 正确
```

### ❌ 错误 2: task_name 不匹配

```python
task = Task(name='process')

# ❌ 错误
await pub.push(data={}, task_name='process_data')
# Runner 找不到 'process_data' 任务
```

✅ **正确做法**: 名称必须一致

```python
task = Task(name='process')
runner = TaskRunner(tasks={'process': task}, ...)
await pub.push(data={}, task_name='process')  # ✅ 正确
```

### ❌ 错误 3: data 不是字典

```python
# ❌ 错误
await pub.push(data="http://example.com", task_name='crawl')
# ValueError: data must be a dict
```

✅ **正确做法**: 必须是字典

```python
await pub.push(data={"url": "http://example.com"}, task_name='crawl')  # ✅ 正确
```

### ❌ 错误 4: Task 未注册

```python
task = Task(name='test')

# ❌ 错误: tasks 字典为空
runner = TaskRunner(tasks={}, url="memory://")
await pub.push(data={}, task_name='test')
# Runner 找不到 'test' 任务
```

✅ **正确做法**: 注册到 Runner

```python
runner = TaskRunner(
    tasks={'test': task},  # ✅ 注册任务
    url="memory://"
)
```

---

## 📚 下一步

### 进阶主题

1. **架构深入**: 阅读 [ARCHITECTURE.md](ARCHITECTURE.md) 了解内部机制
2. **完整示例**: 查看 [examples/](examples/) 目录
3. **类型定义**: 使用 [flowmix/common/types.py](flowmix/common/types.py) 的类型

### 示例代码

```
examples/
├── basic_example.py              # Hello World
├── callback_priority_example.py  # 递归任务（DFS/BFS）
├── cache_example.py              # 任务去重
├── cron_example.py               # 定时任务
└── stats_example/                # 统计监控
```

### 生产环境部署

**1. 使用 Redis 队列**

```python
runner = TaskRunner(
    tasks=tasks,
    url="redis://localhost:6379/0",
    queue_name="production_tasks",
    config=RunnerConfig(
        num_workers=50,      # 根据 CPU 核心数调整
        max_retries=3,       # 失败重试 3 次
        retry_delay=1.0      # 重试延迟 1 秒
    )
)
```

**2. 多进程部署**

```bash
# 启动多个 Worker 进程
python worker1.py &
python worker2.py &
python worker3.py &
```

每个进程使用相同的 `url` 和 `queue_name`，自动负载均衡。

**3. 监控统计**

```python
from flowmix.stats import RedisStats

stats = RedisStats(redis=redis, queue_name="tasks")

# 查询任务信息
task_info = await stats.task.get_task(task_id=123)

# 查询 Runner 性能
perf = await stats.runner.get_performance()
print(f"Throughput: {perf['throughput']:.2f} tasks/sec")
```

---

## 🤝 为 AI 设计的提示

如果你是 AI 助手，生成代码时请遵循以下检查清单：

- [ ] Task 的 `name` 是否在 TaskRunner 的 `tasks` 字典中注册？
- [ ] `pub.push()` 的 `task_name` 是否和 Task 的 `name` 匹配？
- [ ] `data` 参数是否是字典（不是 list、str、int）？
- [ ] `task.callback()` 是否只在 `@task.execute` 内调用？
- [ ] URL scheme 是否正确（`redis://`、`rediss://`、`memory://`）？
- [ ] 是否需要设置 `concurrency_limit` 或 `dedup`？

---

## 🔗 相关资源

- [README.md](README.md) - 项目介绍
- [ARCHITECTURE.md](ARCHITECTURE.md) - 架构设计
- [examples/](examples/) - 完整示例
- [flowmix/common/types.py](flowmix/common/types.py) - 类型定义

---

*最后更新: 2025-01-XX*
*版本: v1.0.0*
