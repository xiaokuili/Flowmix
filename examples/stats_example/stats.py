"""
Stats 模块使用示例

展示如何使用分层的 Stats 接口：
1. TaskQuery - 任务链查询
2. RunnerStats - Runner 执行统计
3. MonitoringQuery - 实时监控
"""

import asyncio
import redis.asyncio as aioredis
from flowmix.stats import RedisStats
from datetime import datetime


async def main():
    # 创建 Redis 连接
    redis_url = 'redis://localhost:6379/0'

    # 创建 Stats 实例
    stats = RedisStats(redis_url=redis_url, queue_name="tasks")

    print("=" * 80)
    print("Stats 模块使用示例")
    print("=" * 80)

    # ========================================================================
    # 1. 任务链查询 (stats.task)
    # ========================================================================
    print("\n【1. 任务链查询】")
    print("-" * 80)

    # 查询单个任务
    task = await stats.task.get_task(task_id=1)
    if task:
        print(f"\n任务详情:")
        print(f"  ID: {task['id']}")
        print(f"  任务名: {task['task_name']}")
        print(f"  状态: {task['status']}")
        print(f"  Worker: {task['worker_id']}")
        print(f"  创建时间: {task['created_at']}")

    # 判断任务链是否完成
    is_completed = await stats.task.is_chain_completed(root_id=1)
    print(f"\n任务链是否完成: {'✅ 是' if is_completed else '⏳ 否'}")

    # 查询任务链统计摘要
    summary = await stats.task.get_chain_summary(root_id=1)
    print(f"\n任务链统计摘要:")
    print(f"  总任务数: {summary['total']}")
    print(f"  待处理: {summary['pending']}")
    print(f"  处理中: {summary['processing']}")
    print(f"  已完成: {summary['completed']}")
    print(f"  失败: {summary['failed']}")

    # 计算进度和成功率
    if summary['total'] > 0:
        progress = (summary['completed'] + summary['failed']) / summary['total']
        print(f"  完成进度: {progress * 100:.1f}%")

        finished = summary['completed'] + summary['failed']
        if finished > 0:
            success_rate = summary['completed'] / finished
            print(f"  成功率: {success_rate * 100:.1f}%")

        # 如果完成了，检查是否全部成功
        if is_completed:
            if summary['failed'] == 0:
                print(f"  🎉 任务链全部成功！")
            else:
                print(f"  ⚠️  任务链完成，但有 {summary['failed']} 个任务失败")

    # 查询任务链详细信息
    details = await stats.task.get_chain_details(root_id=1)
    print(f"\n任务链详情 (共 {len(details)} 个任务):")
    for task in details[:5]:  # 只显示前 5 个
        print(f"  [{task['id']}] {task['task_name']}: {task['status']}")

    # ========================================================================
    # 2. Runner 执行统计 (stats.runner)
    # ========================================================================
    print("\n【2. Runner 执行统计】")
    print("-" * 80)

    # 查询所有 Worker 的整体性能（任务维度）
    perf = await stats.runner.get_performance()
    print(f"\n整体性能统计（任务维度）:")
    print(f"  总任务数: {perf['total']}")
    print(f"  已完成: {perf['completed']}")
    print(f"  失败: {perf['failed']}")
    print(f"  成功率: {perf['success_rate']*100:.1f}%")
    print(f"  吞吐量: {perf['qps']:.2f} tasks/s")
    print(f"  平均执行时长: {perf['avg_duration_seconds']:.2f} 秒")

    # 查询任务链维度统计
    chain_stats = await stats.runner.get_chain_stats()
    print(f"\n任务链统计（链维度）:")
    print(f"  总任务链数: {chain_stats['total_chains']}")
    print(f"  已完成: {chain_stats['completed_chains']}")
    print(f"  正在处理: {chain_stats['processing_chains']}")
    print(f"  全部成功: {chain_stats['success_chains']}")
    print(f"  部分失败: {chain_stats['partial_failed_chains']}")

    # 计算任务链维度的完成率和成功率
    if chain_stats['total_chains'] > 0:
        completion_rate = chain_stats['completed_chains'] / chain_stats['total_chains']
        print(f"  完成率: {completion_rate * 100:.1f}%")

        if chain_stats['completed_chains'] > 0:
            success_rate = chain_stats['success_chains'] / chain_stats['completed_chains']
            print(f"  成功率: {success_rate * 100:.1f}%")

    # 查询特定时间范围的统计
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    today_perf = await stats.runner.get_performance(start_time=today)
    print(f"\n今日性能统计:")
    print(f"  总任务数: {today_perf['total']}")
    print(f"  成功率: {today_perf['success_rate']*100:.1f}%")

    # 按任务类型统计
    by_type = await stats.runner.get_performance_by_task_type()
    print(f"\n按任务类型统计:")
    for task_type, task_stats in by_type.items():
        print(f"  {task_type}:")
        print(f"    总数: {task_stats['total']}")
        print(f"    完成: {task_stats['completed']}")
        print(f"    成功率: {task_stats['success_rate']*100:.1f}%")

    # 列出所有 Worker
    workers = await stats.runner.list_workers()
    print(f"\nWorker 列表 (共 {len(workers)} 个):")
    for w in workers:
        status_emoji = "🟢" if w['is_active'] else "🔴"
        print(f"  {status_emoji} {w['worker_id']}:")
        print(f"     总任务: {w['total_tasks']} | 完成: {w['completed']} | 失败: {w['failed']}")
        print(f"     最后活跃: {w['last_seen']}")

    # 关闭连接
    await stats.close()


    print("\n" + "=" * 80)
    print("示例结束")
    print("=" * 80)


if __name__ == "__main__":
    asyncio.run(main())
