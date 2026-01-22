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

# 安装指定分支
pip install git+https://github.com/xiaokuili/Flowmix.git@main
```

---

## 🚀 快速开始

### 基础用法

```python
from flowmix import Task, Worker

# 定义任务
task = Task()

@task.execute
def process(data):
    print(f"Processing: {data['url']}")
    return {"status": "ok"}

@task.on_success
def on_success(data, result):
    print(f"✅ Success: {result}")

# 提交并执行
worker = Worker(tasks=task, num_workers=5)
worker.push({"url": "http://example.com"})
worker.run()
```

### 任务树（爬虫场景）

```python
crawl_task = Task(name='crawl')

@crawl_task.execute
def crawl(data):
    url = data['url']
    depth = data.get('depth', 0)

    # 爬取子页面（自动构建父子关系）
    if depth < 2:
        for i in range(1, 4):
            crawl_task.callback('crawl', {
                'url': f"{url}/page{i}",
                'depth': depth + 1
            }, priority=10)

    return {"url": url}

# 提交根任务
worker = Worker(tasks=crawl_task)
root_id = worker.push({'url': 'http://example.com', 'depth': 0})

# 查询进度
stats = worker.get_tree_stats(root_id)
print(f"进度: {stats['completed']}/{stats['total']}")

# 执行
worker.run()
```

---

## 🎯 核心功能

### 1. 高性能并发

通过 `num_workers` 参数控制并发协程数量，实现高性能并发执行：

```python
# 启动 5 个并发协程
worker = Worker(tasks=task, num_workers=5)
worker.run()
```

**启动日志示例**：
```
INFO Worker worker-MacBook-12345-1234567890 started with 5 concurrent workers
DEBUG Worker worker-MacBook-12345-1234567890-0 started
DEBUG Worker worker-MacBook-12345-1234567890-1 started
INFO [worker-0] Processing task 'crawl': 1
INFO [worker-1] Processing task 'crawl': 2
INFO [worker-2] Processing task 'crawl': 3
```

基于 asyncio 的异步并发，单个事件循环 + 多个协程，高效处理 I/O 密集型任务。

### 2. Worker 状态查询

框架基于 SQLite 持久化任务状态，可以随时查询 Worker 的执行情况。

**核心特点**：
- 支持按 Worker ID、时间范围、任务类型等多维度筛选
- 实时性能指标：吞吐量、成功率、平均执行时长
- 只读操作，不影响 Worker 执行

#### 基础用法

```python
from flowmix import StatsReader
from datetime import datetime, timedelta

# 创建状态查询器（指向 Worker 使用的同一个数据库）
reader = StatsReader(db_path=".flowmix/flowmix.db")

# 查询所有 Worker 的整体执行情况
stats = reader.get_worker_stats()
print(stats)
# {
#     'total': 10000,          # 总任务数
#     'completed': 9000,       # 已完成
#     'failed': 500,           # 失败
#     'pending': 300,          # 待处理
#     'processing': 200,       # 处理中
#     'success_rate': 0.947,   # 成功率
#     'qps': 2.78,             # 吞吐量（每秒完成数）
#     'avg_duration_seconds': 1.5  # 平均执行时长
# }

# 查询某个 Worker 的执行情况
stats = reader.get_worker_stats(worker_id='worker-MacBook-12345-1234567890')
print(f"Worker 执行: {stats['completed']}/{stats['total']} 个任务")
print(f"成功率: {stats['success_rate']*100:.1f}%")
print(f"吞吐量: {stats['qps']:.2f} tasks/s")

# 查询今天的执行情况
today_start = datetime.now().replace(hour=0, minute=0, second=0)
stats = reader.get_worker_stats(start_time=today_start)
print(f"今天执行: {stats['total']} 个任务")

# 按任务类型统计
by_type = reader.get_worker_stats_by_task_type(start_time=today_start)
for task_type, task_stats in by_type.items():
    print(f"{task_type}: {task_stats['completed']}/{task_stats['total']} "
          f"(成功率: {task_stats['success_rate']*100:.1f}%)")

# 列出所有 Worker
workers = reader.list_workers()
for w in workers:
    status = "🟢 活跃" if w['is_active'] else "🔴 停止"
    print(f"{status} {w['worker_id']}: {w['completed']}/{w['total_tasks']}")

# 查询失败的任务
failed = reader.get_failed_tasks(limit=10)
for task in failed:
    print(f"[{task['task_id']}] {task['task_type']} 失败: {task['error']}")

# 错误汇总统计
errors = reader.get_error_summary()
for error, count in errors.items():
    print(f"{error}: {count} 次")

# 查询正在处理的任务（实时监控）
processing = reader.get_processing_tasks()
for task in processing:
    print(f"Worker {task['worker_id']} 正在执行 {task['task_type']} "
          f"(已运行 {task['duration_seconds']}秒)")
```


### 3. 并发限流控制

通过 `concurrency_limit` 参数控制任务的每秒最大并发数，避免过载：

```python
# 创建带限流的任务（每秒最多 10 个并发）
task = Task(name='api_call', concurrency_limit=10)

@task.execute
async def call_api(data):
    # 自动限流，无需手动处理
    return await http_get(data['url'])

# 启动 Worker（即使有 50 个并发协程，也会自动限流）
worker = Worker(tasks=task, num_workers=50)
worker.run()
```

**工作原理**：
- 基于**滑动窗口**算法：过去 1 秒内最多执行 N 个任务
- **自动阻塞等待**：超限时任务会排队，无需手动重试
- **任务级别控制**：不同任务可以设置不同的限流值

**适用场景**：
- API 调用限流（避免超过速率限制）
- 数据库操作保护（控制并发连接数）
- 爬虫礼貌性限制（避免对目标服务器造成压力）

### 4. 简洁的接口设计

统一的 API 设计，让开发更直观：

```python
# 1. 提交任务
root_id = worker.push({"url": "http://example.com"})

# 2. 动态回调（在任务执行中自动构建任务树）
@task.execute
def crawl(data):
    task.callback('crawl', {'url': link}, priority=10)
    return result

# 3. 查询状态
stats = worker.get_tree_stats(root_id)

# 4. 配置重试
worker = Worker(tasks=task, num_workers=5, max_retries=3, retry_delay=5)

# 5. 配置限流
task = Task(name='api', concurrency_limit=10)
```

---

## 📄 License

MIT
