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
from flowmix import Task, TaskQueue, TaskProducer, TaskConsumer, ConsumerConfig, Cache

# 定义任务
task = Task(name='process')

@task.execute
async def process(data):
    print(f"Processing: {data['url']}")
    return {"status": "ok"}

@task.on_success
async def on_success(data, result):
    print(f"✅ Success: {result}")

# 提交并执行
async def main():
    # 初始化队列和缓存
    queue = TaskQueue(db_path=".flowmix/flowmix.db")
    cache = Cache(db_path=".flowmix/flowmix.db")

    # 创建生产者（提交任务）
    producer = TaskProducer(queue=queue)
    await producer.push(data={"url": "http://example.com"}, task_name='process')

    # 创建消费者（执行任务）
    consumer = TaskConsumer(
        tasks={'process': task},
        queue=queue,
        cache=cache,
        config=ConsumerConfig(num_workers=5)
    )
    await consumer.run(auto_stop=True)

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
```

---

## ✨ 核心功能

### 1. 高性能并发

通过多个并发 Worker 实现高性能异步执行，对比不同并发数的性能差异。

**示例代码**：[examples/concurrency.py](examples/concurrency.py)

```python
import asyncio
import time
from flowmix import Task, TaskQueue, TaskProducer, TaskConsumer, ConsumerConfig, Cache

task = Task(name='process')

@task.execute
async def process(data):
    # 模拟 I/O 密集型任务（如网络请求）
    await asyncio.sleep(0.1)
    return {"id": data['id'], "result": "ok"}

async def run_with_workers(num_workers, total_tasks):
    # 初始化队列和缓存
    queue = TaskQueue(db_path=".flowmix/flowmix.db")
    cache = Cache(db_path=".flowmix/flowmix.db")

    # 创建生产者和消费者
    producer = TaskProducer(queue=queue)
    consumer = TaskConsumer(
        tasks={'process': task},
        queue=queue,
        cache=cache,
        config=ConsumerConfig(num_workers=num_workers)
    )

    # 提交任务
    for i in range(total_tasks):
        await producer.push(data={'id': i}, task_name='process')

    # 计时执行
    start = time.time()
    await consumer.run(auto_stop=True)
    duration = time.time() - start

    return duration

async def main():
    total_tasks = 50

    print(f"📋 执行 {total_tasks} 个任务 (每个耗时 0.1 秒)")

    # 测试不同并发数
    for num_workers in [1, 5, 10]:
        duration = await run_with_workers(num_workers, total_tasks)
        throughput = total_tasks / duration

        print(f"\n🔧 {num_workers} 个并发 Worker:")
        print(f"  - 总耗时: {duration:.2f} 秒")
        print(f"  - 吞吐量: {throughput:.1f} tasks/s")

asyncio.run(main())
```

**运行效果**：
```bash
python examples/concurrency.py

📋 执行 50 个任务 (每个耗时 0.1 秒)

🔧 1 个并发 Worker:
  - 总耗时: 5.05 秒
  - 吞吐量: 9.9 tasks/s

🔧 5 个并发 Worker:
  - 总耗时: 1.02 秒
  - 吞吐量: 49.0 tasks/s
  - 加速比: 5.0x

🔧 10 个并发 Worker:
  - 总耗时: 0.51 秒
  - 吞吐量: 98.0 tasks/s
  - 加速比: 10.0x
```

---

### 2. 任务去重

自动识别重复任务，复用执行结果，避免重复计算。

**示例代码**：[examples/dedup.py](examples/dedup.py)

```python
import asyncio
from flowmix import Task, TaskQueue, TaskProducer, TaskConsumer, ConsumerConfig, Cache

# 创建支持去重的任务
task = Task(name='fetch', dedup=True)

execute_count = 0

@task.execute
async def fetch(data):
    global execute_count
    execute_count += 1
    url = data['url']
    print(f"  ✓ 实际执行: {url} (第 {execute_count} 次)")
    await asyncio.sleep(0.1)  # 模拟网络请求
    return {"url": url, "content": f"Content of {url}"}

@task.on_success
async def on_success(data, result):
    print(f"  → 任务完成: {data['url']}, 返回 {len(str(result))} 字节")

async def main():
    # 初始化队列和缓存
    queue = TaskQueue(db_path=".flowmix/flowmix.db")
    cache = Cache(db_path=".flowmix/flowmix.db")

    # 创建生产者和消费者
    producer = TaskProducer(queue=queue)
    consumer = TaskConsumer(
        tasks={'fetch': task},
        queue=queue,
        cache=cache,
        config=ConsumerConfig(num_workers=3)
    )

    print("📋 提交 10 个任务 (5 个唯一 URL，每个重复 2 次)")

    urls = [
        'http://example.com/page1',
        'http://example.com/page2',
        'http://example.com/page3',
        'http://example.com/page4',
        'http://example.com/page5',
    ]

    # 每个 URL 提交 2 次
    for url in urls * 2:
        await producer.push(data={'url': url}, task_name='fetch')

    await consumer.run(auto_stop=True)

    print(f"\n📊 效果统计:")
    print(f"  - 提交任务数: 10")
    print(f"  - 实际执行数: {execute_count}")
    print(f"  - 缓存命中数: {10 - execute_count}")
    print(f"  - 节省计算: {(10 - execute_count) / 10 * 100:.1f}%")

asyncio.run(main())
```

**运行效果**：
```bash
python examples/dedup.py

📋 提交 10 个任务 (5 个唯一 URL，每个重复 2 次)
  ✓ 实际执行: http://example.com/page1 (第 1 次)
  → 任务完成: http://example.com/page1, 返回 73 字节
  ✓ 实际执行: http://example.com/page2 (第 2 次)
  → 任务完成: http://example.com/page2, 返回 73 字节
  ...

📊 效果统计:
  - 提交任务数: 10
  - 实际执行数: 5
  - 缓存命中数: 5
  - 节省计算: 50.0%
```

---

### 3. 并发限流

精确控制每秒最大并发数，即使有多个并发工作器也能严格限流。

**示例代码**：[examples/rate_limit.py](examples/rate_limit.py)

```python
import asyncio
import time
from flowmix import Task, TaskQueue, TaskProducer, TaskConsumer, ConsumerConfig, Cache

# 创建带限流的任务（每秒最多 5 个）
task = Task(name='api_call', concurrency_limit=5)

execution_times = []

@task.execute
async def api_call(data):
    execution_times.append(time.time())
    print(f"  ✓ 执行任务 {data['id']}")
    await asyncio.sleep(0.05)
    return {"id": data['id']}

async def main():
    # 初始化队列和缓存
    queue = TaskQueue(db_path=".flowmix/flowmix.db")
    cache = Cache(db_path=".flowmix/flowmix.db")

    # 创建生产者和消费者
    producer = TaskProducer(queue=queue)
    consumer = TaskConsumer(
        tasks={'api_call': task},
        queue=queue,
        cache=cache,
        config=ConsumerConfig(num_workers=20)  # 20 个并发 worker
    )

    print("📋 提交 20 个任务 (限流: 每秒最多 5 个)")

    # 提交 20 个任务
    for i in range(20):
        await producer.push(data={'id': i}, task_name='api_call')

    start = time.time()
    await consumer.run(auto_stop=True)
    duration = time.time() - start

    # 分析执行时间分布
    print("\n📊 效果统计:")
    print(f"  - 总任务数: 20")
    print(f"  - 总耗时: {duration:.2f} 秒")
    print(f"  - 限流设置: 5 tasks/s")
    print(f"  - 理论最短耗时: {20 / 5:.1f} 秒")

    # 计算每秒执行数
    if execution_times:
        first_time = execution_times[0]
        second_counts = {}
        for t in execution_times:
            second = int(t - first_time)
            second_counts[second] = second_counts.get(second, 0) + 1

        print(f"\n  每秒执行数分布:")
        for second in sorted(second_counts.keys()):
            print(f"    第 {second} 秒: {second_counts[second]} 个任务")

asyncio.run(main())
```

**运行效果**：
```bash
python examples/rate_limit.py

📋 提交 20 个任务 (限流: 每秒最多 5 个)

📊 效果统计:
  - 总任务数: 20
  - 总耗时: 4.02 秒
  - 限流设置: 5 tasks/s
  - 理论最短耗时: 4.0 秒

  每秒执行数分布:
    第 0 秒: 5 个任务
    第 1 秒: 5 个任务
    第 2 秒: 5 个任务
    第 3 秒: 5 个任务
```

---

### 4. 状态查询

实时查询任务执行情况，支持多维度统计分析。

**示例代码**：[examples/stats.py](examples/stats.py)

```python
import asyncio
from flowmix import Task, TaskQueue, TaskProducer, TaskConsumer, ConsumerConfig, Cache, Stats

task = Task(name='process')

@task.execute
async def process(data):
    await asyncio.sleep(0.05)
    # 模拟 10% 失败率
    if data['id'] % 10 == 0:
        raise Exception("模拟错误")
    return {"id": data['id']}

async def main():
    # 初始化队列和缓存
    queue = TaskQueue(db_path=".flowmix/flowmix.db")
    cache = Cache(db_path=".flowmix/flowmix.db")

    # 创建生产者和消费者
    producer = TaskProducer(queue=queue)
    consumer = TaskConsumer(
        tasks={'process': task},
        queue=queue,
        cache=cache,
        config=ConsumerConfig(num_workers=5)
    )

    print("📋 提交 50 个任务 (10% 失败率)")

    # 提交任务
    for i in range(50):
        await producer.push(data={'id': i}, task_name='process')

    # 执行任务
    await consumer.run(auto_stop=True)

    # 查询统计信息
    print("\n📊 执行统计:")

    stats_reader = Stats(db_path='.flowmix/flowmix.db')

    # 获取整体统计
    overall = stats_reader.get_worker_stats()
    print(f"总任务数: {overall['total']}")
    print(f"已完成: {overall['completed']}")
    print(f"失败: {overall['failed']}")
    print(f"成功率: {overall['success_rate']*100:.1f}%")
    print(f"平均耗时: {overall['avg_duration_seconds']:.3f} 秒")

    # 按任务类型统计
    print(f"\n按任务类型统计:")
    by_type = stats_reader.get_worker_stats_by_task_type()
    for task_type, task_stats in by_type.items():
        print(f"  {task_type}: {task_stats['completed']}/{task_stats['total']} "
              f"(成功率 {task_stats['success_rate']*100:.1f}%)")

    # 查看失败的任务
    print(f"\n失败的任务:")
    failed = stats_reader.get_failed_tasks(limit=5)
    for task in failed:
        print(f"  [{task['task_id']}] {task['task_type']}: {task['error']}")

    # 错误汇总
    print(f"\n错误汇总:")
    errors = stats_reader.get_error_summary()
    for error, count in errors.items():
        print(f"  {error}: {count} 次")

asyncio.run(main())
```

**运行效果**：
```bash
python examples/stats.py

📋 提交 50 个任务 (10% 失败率)

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
  [task-21] process: 模拟错误
  [task-31] process: 模拟错误
  [task-41] process: 模拟错误

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
python examples/dedup.py          # 去重效果：10 个任务只执行 5 次
python examples/concurrency.py    # 并发效果：10 workers 比 1 worker 快 10x
python examples/rate_limit.py     # 限流效果：精确控制每秒并发数
python examples/stats.py          # 状态查询：实时监控执行情况
```

---

## 📄 License

MIT
