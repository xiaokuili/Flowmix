"""
并发限流示例

演示：即使有多个 worker，也能精确控制每秒最大并发数
"""
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
    queue = TaskQueue(db_path=".flowmix/flowmix.db", queue_name="rate_limit_test")
    cache = Cache(db_path=".flowmix/flowmix.db", queue_name="rate_limit_test")

    # 创建生产者和消费者
    producer = TaskProducer(queue=queue)
    consumer = TaskConsumer(
        tasks={'api_call': task},
        queue=queue,
        cache=cache,
        config=ConsumerConfig(num_workers=20)  # 20 个并发 worker
    )

    print("📋 提交 20 个任务 (限流: 每秒最多 5 个)")
    print("=" * 50)

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

if __name__ == "__main__":
    asyncio.run(main())
