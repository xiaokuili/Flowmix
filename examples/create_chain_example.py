"""
创建任务链示例 - 用于测试 Stats 查询

这个示例会：
1. 创建一个简单的任务链（树形结构）
2. 使用 Redis 后端（与 stats_example.py 共享数据）
3. 执行任务链
4. 完成后可以运行 stats_example.py 查看统计数据
"""

import asyncio
from flowmix import Task, TaskRunner, RunnerConfig
from flowmix.sender import Pub


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
    """主函数"""
    redis_url = 'redis://localhost:6379/0'
    queue_name = "tasks"

    print("=" * 70)
    print("🚀 创建并执行任务链")
    print("=" * 70)
    print(f"📍 Redis: {redis_url}")
    print(f"📍 Queue: {queue_name}")
    print("=" * 70)

    # 创建发布器（自动创建 Redis 连接）
    print("\n🔌 连接 Redis...")
    pub = await Pub.create(url=redis_url, queue_name=queue_name)
    print("✓ Redis 连接成功")

    # 推送根任务
    print("\n📤 推送根任务...")
    root_task_id = await pub.push(
        data={'id': 'ROOT', 'level': 0, 'max_level': 2},
        task_name='process'
    )
    print(f"✓ 根任务 ID: {root_task_id}")

    # 创建并启动 Runner
    print("\n🔧 启动 Runner...")
    print("=" * 70 + "\n")

    runner = TaskRunner(
        tasks={'process': process_task},
        url=redis_url,
        queue_name=queue_name,
        config=RunnerConfig(
            num_workers=2,       # 2个并发worker
            max_retries=2,       # 失败后重试2次
            retry_delay=1.0      # 重试延迟1秒
        )
    )

    # 运行任务链（会自动执行直到所有任务完成）
    await runner.run()

    print("\n" + "=" * 70)
    print("✅ 任务链执行完成！")
    print("=" * 70)
    print(f"\n💡 现在可以运行 stats_example.py 查看统计数据:")
    print(f"   python examples/stats_example.py")
    print("=" * 70 + "\n")


if __name__ == "__main__":
    asyncio.run(main())
