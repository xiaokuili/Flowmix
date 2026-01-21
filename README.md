# Flowmix

只需关心业务流程，Flowmix 负责高性能执行、状态管理、简洁接口

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

### 2. 完整的状态管理

框架自动追踪任务执行状态，支持查询任务树的完整进度：

```python
# 查询任务树统计（递归查询所有子孙任务）
stats = worker.get_tree_stats(root_id)
print(stats)
# {
#     'total': 13,       # 总任务数
#     'pending': 0,      # 待处理
#     'processing': 0,   # 处理中
#     'completed': 13,   # 已完成
#     'failed': 0        # 失败
# }

# 判断任务树是否全部完成
is_done = (stats['pending'] == 0 and stats['processing'] == 0)

# 按任务名称分组统计
stats = worker.get_tree_stats(root_id, group_by_task=True)
print(stats)
# {
#     'total': 13,
#     'pending': 0,
#     'processing': 0,
#     'completed': 13,
#     'failed': 0,
#     'by_task': {
#         'crawl': {'total': 10, 'completed': 10, 'failed': 0},
#         'parse': {'total': 3, 'completed': 3, 'failed': 0}
#     }
# }

# 获取任务树详细信息（包含任务名称、参数、结果、父子关系）
details = worker.get_tree_details(root_id)
for task in details:
    indent = '  ' if task['parent_id'] else ''
    print(f"{indent}[{task['id']}] {task['task_name']}: {task['status']}")
    print(f"{indent}  Parent: {task['parent_id']}, Data: {task['data']}")
    if task['result']:
        print(f"{indent}  Result: {task['result']}")

# 输出示例（展示完整的任务链路）：
# [1] crawl: completed
#   Parent: None, Data: {'url': 'http://example.com', 'depth': 0}
#   Result: {'status': 'ok', 'links': 3}
#   [2] crawl: completed
#     Parent: 1, Data: {'url': 'http://example.com/page1', 'depth': 1}
#     Result: {'status': 'ok', 'links': 0}
#   [3] crawl: completed
#     Parent: 1, Data: {'url': 'http://example.com/page2', 'depth': 1}
#     Result: {'status': 'ok', 'links': 2}
#     [4] parse: completed
#       Parent: 3, Data: {'html': '<html>...'}
#       Result: {'title': 'Example', 'links': [...]}
```

**任务状态流转**：
- `pending` → `processing` → `completed` (成功)
- `pending` → `processing` → `failed` (失败)

**数据持久化**：
- 任务名称 (`task_name`): 记录执行的是哪个任务
- 任务参数 (`data`): 完整保存输入参数
- 执行结果 (`result`): 保存任务的返回值
- 错误信息 (`error`): 失败时记录错误原因

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
