"""
高性能并发示例

演示：对比不同并发数的执行速度
"""
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
    queue = TaskQueue(db_path=".flowmix/flowmix.db", queue_name="concurrency_test")
    cache = Cache(db_path=".flowmix/flowmix.db", queue_name="concurrency_test")

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
    print("=" * 50)

    # 测试不同并发数
    for num_workers in [1, 5, 10]:
        duration = await run_with_workers(num_workers, total_tasks)
        throughput = total_tasks / duration

        print(f"\n🔧 {num_workers} 个并发 Worker:")
        print(f"  - 总耗时: {duration:.2f} 秒")
        print(f"  - 吞吐量: {throughput:.1f} tasks/s")

        if num_workers == 1:
            baseline = duration
        else:
            speedup = baseline / duration
            print(f"  - 加速比: {speedup:.1f}x")

if __name__ == "__main__":
    asyncio.run(main())
