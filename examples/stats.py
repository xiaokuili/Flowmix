"""
状态查询示例

演示：实时查询 Runner 的执行情况
"""
import asyncio
from flowmix import Task, TaskQueue, Pub, TaskRunner, RunnerConfig, Cache, Stats

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
    queue = TaskQueue(db_path=".flowmix/flowmix.db", queue_name="stats_test")
    cache = Cache(db_path=".flowmix/flowmix.db", queue_name="stats_test")

    # 创建发布器和运行器
    pub = Pub(queue=queue)
    runner = TaskRunner(
        tasks={'process': task},
        queue=queue,
        cache=cache,
        config=RunnerConfig(num_workers=5)
    )

    print("📋 提交 50 个任务 (10% 失败率)")
    print("=" * 50)

    # 提交任务
    for i in range(50):
        await pub.push(data={'id': i}, task_name='process')

    # 执行任务
    await runner.run(auto_stop=True)

    # 查询统计信息
    print("\n📊 执行统计:")
    print("=" * 50)

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

if __name__ == "__main__":
    asyncio.run(main())
