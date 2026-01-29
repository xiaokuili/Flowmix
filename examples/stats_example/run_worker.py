"""
运行 Worker - 处理 Redis 队列中的任务
"""

import asyncio
from flowmix import Task, TaskRunner, RunnerConfig


# 创建任务
process_task = Task(name='process')


@process_task.execute
async def execute_process(data):
    """处理任务并创建子任务"""
    task_id = data.get('id', 'ROOT')
    level = data.get('level', 0)
    max_level = data.get('max_level', 2)

    print(f"{'  ' * level}⚙️  [{level}] 执行任务: {task_id}")

    # 模拟处理
    await asyncio.sleep(0.2)

    # 创建子任务
    if level < max_level:
        num_children = 3 if level == 0 else 2
        print(f"{'  ' * level}   → 创建 {num_children} 个子任务")

        for i in range(num_children):
            child_id = f"{task_id}-{chr(65+i)}" if task_id == 'ROOT' else f"{task_id}.{i+1}"
            await process_task.callback(
                task_name='process',
                data={
                    'id': child_id,
                    'level': level + 1,
                    'max_level': max_level
                },
                priority=10  # 深度优先
            )

    return {"id": task_id, "level": level, "status": "completed"}


@process_task.on_success
async def on_success(data, result):
    """任务成功回调"""
    task_id = result.get('id', '')
    level = result.get('level', 0)
    print(f"{'  ' * level}✅ 完成: {task_id}")


@process_task.on_failure
async def on_failure(data, error):
    """任务失败回调"""
    task_id = data.get('id', 'UNKNOWN')
    print(f"❌ 失败: {task_id} - {error}")


async def main():
    redis_url = 'redis://localhost:6379/0'
    queue_name = "tasks"

    print("=" * 70)
    print("🚀 启动 Worker 处理任务")
    print("=" * 70)
    print(f"📍 Redis: {redis_url}")
    print(f"📍 Queue: {queue_name}")
    print(f"📍 Workers: 2")
    print("=" * 70 + "\n")

    # 创建并启动 Runner
    runner = TaskRunner(
        tasks={'process': process_task},
        url=redis_url,
        queue_name=queue_name,
        config=RunnerConfig(
            num_workers=2,       # 2个并发worker
            max_retries=2,       # 失败后重试2次
            retry_delay=1.0,     # 重试延迟1秒
        )
    )

    # 运行任务链
    await runner.run()

    print("\n" + "=" * 70)
    print("✅ 所有任务处理完成！")
    print("=" * 70)
    print("\n现在可以运行 stats_example.py 查看统计数据:")
    print("  python examples/stats_example.py")
    print("=" * 70 + "\n")


if __name__ == "__main__":
    asyncio.run(main())
