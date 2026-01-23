"""简单的异步测试"""
import asyncio
from flowmix import Task, Worker

# 创建任务
task = Task(name='test')

@task.execute
async def process(data):
    print(f"Processing: {data['value']}")
    await asyncio.sleep(0.1)
    return data['value'] * 2


async def main():
    print("🧪 测试异步功能")

    # 创建 Worker
    worker = Worker(
        tasks=task,
        num_workers=2,
        db_path="test_async.db"
    )

    # 提交任务
    print("📤 提交任务...")
    await worker.push({'value': 1})
    await worker.push({'value': 2})
    await worker.push({'value': 3})

    # 启动 worker
    print("🚀 启动 Worker...")
    worker.running = True
    workers = [
        asyncio.create_task(worker._worker_loop_async(f"worker-{i}"))
        for i in range(worker.num_workers)
    ]

    # 等待2秒
    await asyncio.sleep(2)
    worker.running = False

    # 停止
    for w in workers:
        w.cancel()
    await asyncio.gather(*workers, return_exceptions=True)

    # 统计
    stats = worker.get_stats()
    print(f"\n📊 执行统计: 成功 {stats['success']}/{stats['processed']}")

    # 关闭
    await worker._manager.close()

    # 清理
    import os
    if os.path.exists("test_async.db"):
        os.remove("test_async.db")

    print("✅ 测试完成！")


if __name__ == "__main__":
    asyncio.run(main())
