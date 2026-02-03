"""
Cron 定时任务示例 - Flowmix 定时调度

展示功能：
1. 使用 Cron 创建间隔定时任务（每隔 N 秒/分钟执行）
2. 使用 Cron 创建 cron 表达式定时任务（固定时间点执行）
3. 使用 Cron 创建一次性任务（指定时间执行一次）
4. 配合 TaskRunner 异步执行定时任务
"""

import asyncio
import time
from datetime import datetime, timedelta
from flowmix import Task, TaskRunner, RunnerConfig
from flowmix.sender import Cron


# 1. 定义任务
crawl_task = Task(name='crawl')


@crawl_task.execute
async def execute_crawl(data):
    """模拟爬虫任务"""
    url = data.get('url', 'http://example.com')
    task_type = data.get('type', 'unknown')
    timestamp = data.get('timestamp', time.time())

    print(f"  🕷️  爬取 [{task_type}]: {url}")
    print(f"      触发时间: {datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')}")

    # 模拟爬取耗时
    await asyncio.sleep(0.5)

    return {
        "url": url,
        "type": task_type,
        "status": "success",
        "timestamp": timestamp
    }


@crawl_task.on_success
async def on_success(_data, result):
    """任务成功回调"""
    print(f"      ✅ 完成: {result['url']}\n")


# 2. 定义报告任务
report_task = Task(name='report')


@report_task.execute
async def execute_report(data):
    """模拟生成报告"""
    report_type = data.get('type', 'daily')
    timestamp = data.get('timestamp', time.time())

    print(f"  📊 生成报告 [{report_type}]")
    print(f"      触发时间: {datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')}")

    await asyncio.sleep(0.3)

    return {
        "type": report_type,
        "status": "generated",
        "timestamp": timestamp
    }


@report_task.on_success
async def on_report_success(_data, result):
    """报告任务成功回调"""
    print(f"      ✅ 报告生成完成: {result['type']}\n")


async def main():
    """主函数"""

    # 创建内存队列
    queue_name = "cron_tasks"

    print("\n" + "="*70)
    print("🕐 Flowmix Cron 定时任务示例")
    print("="*70)

    # 3. 创建 Cron 调度器（自动创建队列）
    cron = await Cron.create(url="memory://", queue_name=queue_name)

    print("\n📋 配置定时任务...")
    print("-"*70)

    # 示例 1: 间隔任务 - 每 3 秒执行一次
    print("  ✓ 添加间隔任务: 每 3 秒爬取一次")
    cron.add_interval(
        task_name="crawl",
        data_fn=lambda: {
            "url": "http://example.com/news",
            "type": "interval",
            "timestamp": time.time()
        },
        seconds=3,
        job_id="crawl_interval"
    )


    print("\n" + "="*70)
    print("🚀 启动 Cron 调度器...")
    print("="*70)

    # 4. 启动 Cron 调度器
    cron.start()

    print("\n⏰ 定时任务已启动，按 Ctrl+C 停止")
    print("="*70 + "\n")

    # 5. 创建 TaskRunner（在后台运行）
    runner = TaskRunner(
        tasks={
            "crawl": crawl_task,
            "report": report_task
        },
        url="memory://",
        queue_name=queue_name,
        config=RunnerConfig(
            num_workers=2,  # 2 个并发 worker
            max_retries=1,
            retry_delay=0.5
        )
    )

    # 6. 启动 Runner（后台任务）
    runner_task = asyncio.create_task(runner.run())

    try:
        # 运行 20 秒后自动停止（演示用）
        print("💡 提示：演示将运行 20 秒后自动停止...\n")
        await asyncio.sleep(20)

    except KeyboardInterrupt:
        print("\n\n⚠️  收到停止信号...")

    finally:
        # 7. 清理资源
        print("\n" + "="*70)
        print("🛑 正在停止...")
        print("="*70)

        # 停止 Cron 调度器
        await cron.stop()
        print("  ✓ Cron 调度器已停止")

        # 取消 Runner 任务
        runner_task.cancel()
        try:
            await runner_task
        except asyncio.CancelledError:
            pass
        print("  ✓ TaskRunner 已停止")

        print("\n" + "="*70)
        print("✅ 演示完成！")
        print("="*70)
        print("\n总结：")
        print("  • Cron 支持三种定时模式：interval、cron、date")
        print("  • add_interval: 按固定间隔执行（秒/分钟/小时/天/周）")
        print("  • add_cron: 按 cron 表达式执行（支持复杂的时间规则）")
        print("  • add_date: 一次性任务（指定时间执行一次）")
        print("  • 配合 TaskRunner 实现异步任务执行")
        print("  • 适合爬虫定时采集、报告定时生成等场景")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n👋 再见！")
