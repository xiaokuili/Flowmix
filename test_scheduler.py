"""
测试 Scheduler - 定时任务调度器

测试场景：
1. 每分钟执行一次的任务（快速测试：每分钟）
2. 同时运行 Scheduler 和 Worker
3. 验证任务是否按时触发
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


async def main():
    global execution_count

    # 1. 定义任务
    daily_task = Task(name='daily_report')

    @daily_task.execute
    async def run_daily_report(data):
        global execution_count
        execution_count += 1
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"\n{'='*60}")
        print(f"[{current_time}] 执行每日报表 (第 {execution_count} 次)")
        print(f"数据: {data}")
        print(f"{'='*60}\n")
        return {"status": "success", "time": current_time, "count": execution_count}

    @daily_task.on_success
    async def on_success(data, result):
        print(f"✓ 任务完成: {result}")

    # 2. 定义另一个任务（健康检查）
    health_task = Task(name='health_check')

    @health_task.execute
    async def run_health_check(data):
        current_time = datetime.now().strftime('%H:%M:%S')
        print(f"[{current_time}] 健康检查: {data['endpoint']}")
        return {"status": "healthy"}

    # 3. 创建 Worker（支持多个任务）
    worker = Worker(
        tasks={
            'daily_report': daily_task,
            'health_check': health_task
        },
        num_workers=2,
        db_path=".flowmix/scheduler_test.db",
        queue_name="scheduler_tasks"
    )

    # 4. 创建调度器
    scheduler = Scheduler(worker, check_interval=10.0)  # 每 10 秒检查一次

    # 5. 添加定时任务
    print("添加定时任务...")

    # 每分钟执行（用于快速测试）
    # 注意：cron 表达式 "* * * * *" 表示每分钟的第 0 秒执行
    scheduler.add_cron(
        cron='* * * * *',               # 每分钟执行
        task_name='daily_report',
        data={'type': 'sales', 'date': datetime.now().strftime('%Y-%m-%d')}
    )

    # 每分钟执行（健康检查）
    scheduler.add_cron(
        cron='* * * * *',               # 每分钟执行
        task_name='health_check',
        data={'endpoint': '/api/health'}
    )

    print("\n调度器配置:")
    for task in scheduler.get_tasks():
        print(f"  - {task}")

    print("\n提示：")
    print("  1. 调度器会在每分钟的开始时触发任务")
    print("  2. 请等待最多 1 分钟看到第一次执行")
    print("  3. 按 Ctrl+C 可以停止测试")
    print(f"  4. 当前时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    # 6. 同时运行调度器和 Worker
    try:
        await asyncio.gather(
            scheduler.run(),        # 调度器：定时提交任务
            worker.run(auto_stop=False)  # Worker：持续运行处理任务
        )
    except KeyboardInterrupt:
        print("\n\n收到停止信号，正在关闭...")
        scheduler.stop()
        worker.stop()
        print(f"总共执行了 {execution_count} 次任务")


if __name__ == "__main__":
    asyncio.run(main())
