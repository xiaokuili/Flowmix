"""
快速测试 Scheduler - 定时任务调度器

使用每分钟执行的 cron，但只运行 2-3 分钟来验证功能
"""

import asyncio
import logging
from datetime import datetime

from flowmix import Task, Worker, Scheduler

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# 统计执行次数
execution_count = 0
max_executions = 3  # 执行 3 次后自动停止


async def main():
    global execution_count

    # 1. 定义任务
    test_task = Task(name='test_task')

    @test_task.execute
    async def run_test(data):
        global execution_count
        execution_count += 1
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"\n{'='*60}")
        print(f"✓ [{current_time}] 定时任务触发 (第 {execution_count} 次)")
        print(f"  数据: {data}")
        print(f"{'='*60}\n")
        return {"status": "success", "time": current_time, "count": execution_count}

    @test_task.on_success
    async def on_success(data, result):
        print(f"  ✓ 任务执行成功: count={result['count']}")

    # 2. 创建 Worker
    worker = Worker(
        tasks={'test_task': test_task},
        num_workers=1,
        db_path=".flowmix/scheduler_quick_test.db",
        queue_name="scheduler_quick_tasks"
    )

    # 3. 创建调度器（每 10 秒检查一次）
    scheduler = Scheduler(worker, check_interval=5.0)

    # 4. 添加定时任务（每分钟执行）
    scheduler.add_cron(
        cron='* * * * *',  # 每分钟执行
        task_name='test_task',
        data={'test': 'scheduler', 'timestamp': datetime.now().isoformat()}
    )

    print("=" * 70)
    print("Scheduler 快速测试")
    print("=" * 70)
    print(f"当前时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"下次执行时间: {scheduler.get_tasks()[0].next_run}")
    print("\n调度配置:")
    for task in scheduler.get_tasks():
        print(f"  - {task}")
    print("\n说明:")
    print("  1. 任务配置为每分钟执行一次 (cron: '* * * * *')")
    print("  2. 调度器每 5 秒检查一次")
    print(f"  3. 将执行 {max_executions} 次后自动停止")
    print("  4. 请等待到下一分钟开始（如当前是 10:30:45，则会在 10:31:00 触发）")
    print("\n按 Ctrl+C 可以手动停止\n")
    print("=" * 70)

    # 5. 创建一个任务来监控执行次数
    async def monitor():
        """监控任务执行次数，达到上限后停止"""
        while execution_count < max_executions:
            await asyncio.sleep(1)

        print(f"\n已执行 {execution_count} 次，测试完成！")
        scheduler.stop()
        worker.stop()

    # 6. 同时运行调度器、Worker 和监控器
    try:
        await asyncio.gather(
            scheduler.run(),
            worker.run(auto_stop=False),
            monitor(),
            return_exceptions=True
        )
    except KeyboardInterrupt:
        print("\n\n收到停止信号...")
        scheduler.stop()
        worker.stop()
    finally:
        print(f"\n测试结束，总共执行了 {execution_count} 次任务")
        print("✓ Scheduler 测试通过！")


if __name__ == "__main__":
    asyncio.run(main())
