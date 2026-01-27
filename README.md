# Flowmix

只需关心业务流程，Flowmix 负责高性能执行、状态管理、简洁接口

---

## 📦 安装

### 基础安装

```bash
pip install git+https://github.com/xiaokuili/Flowmix.git
```

### 安装特定版本

```bash
# 安装指定版本标签
pip install git+https://github.com/xiaokuili/Flowmix.git@v0.3.0

# 强制安装
pip install --force-reinstall git+https://github.com/xiaokuili/Flowmix.git@v0.5.0
```

---

## 🚀 快速开始

```python
from flowmix import Task, TaskQueue, Pub, TaskRunner, RunnerConfig, Cache

# 1. 定义任务（装饰器语法）
task = Task(name='process')

@task.execute
async def process(data):
    print(f"Processing: {data['url']}")
    return {"status": "ok"}

@task.on_success
async def on_success(data, result):
    print(f"✅ Success: {result}")

# 2. 提交并执行
async def main():
    queue = TaskQueue(db_path=".flowmix/flowmix.db")
    cache = Cache(db_path=".flowmix/flowmix.db")

    # 提交任务
    pub = Pub(queue=queue)
    await pub.push(data={"url": "http://example.com"}, task_name='process')

    # 执行任务
    runner = TaskRunner(
        tasks={'process': task},
        queue=queue,
        cache=cache,
        config=RunnerConfig(num_workers=5)
    )
    await runner.run(auto_stop=True)

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
```

---

## ✨ 核心功能

| 功能 | 示例文件 | 核心效果 |
|------|---------|---------|
| 快速定义任务 | [快速开始](#-快速开始) | 装饰器语法定义任务 |
| 避免重复处理 | [dedup.py](examples/dedup.py) | 10 个任务只执行 5 次，节省 50% |
| 控制并发速率 | [concurrency.py](examples/concurrency.py), [rate_limit.py](examples/rate_limit.py) | 10 workers 快 10 倍，精确限流 |
| 任务依赖 | [task_dependency.py](examples/task_dependency.py) | 1 个根任务扩展到 13 个任务树 |
| 灵活部署环境 | [deployment.py](examples/deployment.py) | 支持 SQLite/Redis/PostgreSQL |
| 状态查询和监控告警 | [stats.py](examples/stats.py), [monitoring.py](examples/monitoring.py) | 多维度统计，自定义告警规则 |

---

### 1. 快速定义任务

使用简洁的装饰器语法快速定义任务，支持同步和异步函数，支持成功/失败回调。

```python
from flowmix import Task

# 创建任务
task = Task(name='process')

# 定义执行逻辑
@task.execute
async def process(data):
    print(f"Processing: {data['url']}")
    return {"status": "ok"}

# 定义成功回调
@task.on_success
async def on_success(data, result):
    print(f"✅ Success: {result}")

# 定义失败回调
@task.on_failure
async def on_failure(data, error):
    print(f"❌ Failed: {error}")
```

---

### 2. 避免重复处理

自动识别重复任务，复用执行结果，避免重复计算。支持设置缓存时间窗口。

**示例代码**：[examples/dedup.py](examples/dedup.py)

```python
# 创建支持去重的任务
task = Task(name='fetch', dedup=True)  # 永久去重

# 或设置缓存时间窗口（1小时内去重）
task = Task(name='api_call', dedup=True, dedup_ttl=3600)
```

**运行效果**：
```bash
python examples/dedup.py

📋 提交 10 个任务 (5 个唯一 URL，每个重复 2 次)
  ✓ 实际执行: http://example.com/page1 (第 1 次)
  ✓ 实际执行: http://example.com/page2 (第 2 次)
  ...

📊 效果统计:
  - 提交任务数: 10
  - 实际执行数: 5
  - 缓存命中数: 5
  - 节省计算: 50.0%
```

---

### 3. 控制并发速率

支持两种并发控制方式：任务级限流和全局并发数控制，精确控制执行速率。

**示例代码**：[examples/rate_limit.py](examples/rate_limit.py) 和 [examples/concurrency.py](examples/concurrency.py)

```python
# 方式1: 任务级限流（每秒最多 5 个）
task = Task(name='api_call', concurrency_limit=5)

# 方式2: 全局并发数控制（10 个并发 Worker）
runner = TaskRunner(
    tasks={'process': task},
    queue=queue,
    cache=cache,
    config=RunnerConfig(num_workers=10)
)
```

**运行效果**：
```bash
python examples/rate_limit.py

📋 提交 20 个任务 (限流: 每秒最多 5 个)

📊 效果统计:
  - 总任务数: 20
  - 总耗时: 4.02 秒
  - 限流设置: 5 tasks/s

  每秒执行数分布:
    第 0 秒: 5 个任务
    第 1 秒: 5 个任务
    第 2 秒: 5 个任务
    第 3 秒: 5 个任务
```

---

### 4. 任务依赖

支持构建任务树，实现父子任务依赖关系。可在任务执行中动态提交子任务，自动关联父子关系。

**示例代码**：[examples/task_dependency.py](examples/task_dependency.py)

```python
from flowmix import Task, TaskQueue, Pub, TaskRunner, RunnerConfig, Cache

crawl_task = Task(name='crawl')

@crawl_task.execute
async def crawl(data):
    url = data['url']
    print(f"  🔍 爬取: {url}")

    # 模拟爬取页面，发现新链接
    if data.get('depth', 0) < 2:  # 限制深度
        child_urls = [f"{url}/page{i}" for i in range(1, 4)]

        # 动态提交子任务（自动关联父任务）
        for child_url in child_urls:
            await crawl_task.callback(
                'crawl',
                {'url': child_url, 'depth': data.get('depth', 0) + 1},
                priority=10  # 高优先级 = 深度优先
            )

    return {"url": url, "status": "ok"}

async def main():
    queue = TaskQueue(db_path=".flowmix/flowmix.db")
    cache = Cache(db_path=".flowmix/flowmix.db")
    pub = Pub(queue=queue)

    # 提交根任务
    root_id = await pub.push(
        data={'url': 'http://example.com', 'depth': 0},
        task_name='crawl'
    )

    # 创建运行器并执行
    runner = TaskRunner(
        tasks={'crawl': crawl_task},
        queue=queue,
        cache=cache,
        config=RunnerConfig(num_workers=5)
    )
    await runner.run(auto_stop=True)

    # 查询任务树统计
    stats = await pub.get_tree_stats(root_id)
    print(f"\n📊 任务树统计:")
    print(f"  - 总任务数: {stats['total']}")
    print(f"  - 已完成: {stats['completed']}")
    print(f"  - 失败: {stats['failed']}")
```

**运行效果**：
```bash
python examples/task_dependency.py

  🔍 爬取: http://example.com
  🔍 爬取: http://example.com/page1
  🔍 爬取: http://example.com/page2
  🔍 爬取: http://example.com/page3
  🔍 爬取: http://example.com/page1/page1
  ...

📊 任务树统计:
  - 总任务数: 13
  - 已完成: 13
  - 失败: 0
```

---

### 5. 灵活部署环境

支持多种存储后端（SQLite、Redis、PostgreSQL），根据场景灵活选择。可单机部署，也可分布式部署。

**示例代码**：[examples/deployment.py](examples/deployment.py)

```python
from flowmix import TaskQueue, Cache

# 场景1: 单机开发（SQLite）
queue = TaskQueue(db_path=".flowmix/flowmix.db")
cache = Cache(db_path=".flowmix/flowmix.db")

# 场景2: 分布式部署（Redis）
queue = TaskQueue(
    provider_type='redis',
    redis_url='redis://localhost:6379/0'
)
cache = Cache(
    provider_type='redis',
    redis_url='redis://localhost:6379/0'
)

# 场景3: 生产环境（PostgreSQL）
queue = TaskQueue(
    provider_type='postgres',
    postgres_dsn='postgresql://user:pass@localhost/flowmix'
)
cache = Cache(
    provider_type='postgres',
    postgres_dsn='postgresql://user:pass@localhost/flowmix'
)
```

**部署架构**：
```
# 单机部署
┌─────────────────────┐
│   TaskRunner        │
│ (Producer+Consumer) │
│    SQLite           │
└─────────────────────┘

# 分布式部署
┌──────────┐    ┌──────────┐    ┌──────────┐
│ Producer │───▶│  Redis   │◀───│ Consumer │
│  (Pub)   │    │  Queue   │    │ (Runner) │
└──────────┘    └──────────┘    └──────────┘
                                 ┌──────────┐
                                 │ Consumer │
                                 │ (Runner) │
                                 └──────────┘
```

---

### 6. 状态查询和监控告警

实时查询任务执行情况，支持多维度统计分析，可集成到监控系统。

**示例代码**：[examples/stats.py](examples/stats.py) 和 [examples/monitoring.py](examples/monitoring.py)

```python
from flowmix import TaskStats, Pub

# 获取整体统计
stats = TaskStats(db_path='.flowmix/flowmix.db')
overall = stats.get_worker_stats()
print(f"成功率: {overall['success_rate']*100:.1f}%")
print(f"平均耗时: {overall['avg_duration_seconds']:.3f} 秒")

# 按任务类型统计
by_type = stats.get_worker_stats_by_task_type()
for task_type, task_stats in by_type.items():
    print(f"{task_type}: {task_stats['success_rate']*100:.1f}%")

# 查询失败任务
failed = stats.get_failed_tasks(limit=10)
for task in failed:
    print(f"[{task['task_id']}] {task['error']}")

# 错误汇总（用于告警）
errors = stats.get_error_summary()
for error, count in errors.items():
    if count > 10:  # 告警阈值
        send_alert(f"错误频繁: {error} 出现 {count} 次")

# 查询任务树状态
pub = Pub(queue=queue)
tree_stats = await pub.get_tree_stats(root_task_id)
print(f"任务树进度: {tree_stats['completed']}/{tree_stats['total']}")
```

**运行效果**：
```bash
python examples/stats.py

📊 执行统计:
总任务数: 50
已完成: 50
失败: 5
成功率: 90.0%
平均耗时: 0.051 秒

按任务类型统计:
  process: 50/50 (成功率 90.0%)

失败的任务:
  [task-1] process: 模拟错误
  [task-11] process: 模拟错误

错误汇总:
  模拟错误: 5 次
```

---

## 🎯 运行所有示例

```bash
# 克隆仓库
git clone https://github.com/xiaokuili/Flowmix.git
cd Flowmix

# 运行示例查看效果
python examples/dedup.py              # 去重效果：10 个任务只执行 5 次
python examples/concurrency.py        # 并发效果：10 workers 比 1 worker 快 10x
python examples/rate_limit.py         # 限流效果：精确控制每秒并发数
python examples/task_dependency.py    # 任务依赖：构建任务树
python examples/deployment.py         # 部署方式：SQLite/Redis/PostgreSQL
python examples/stats.py              # 状态查询：实时监控执行情况
python examples/monitoring.py         # 监控告警：集成告警系统
```

---

## 📄 License

MIT
