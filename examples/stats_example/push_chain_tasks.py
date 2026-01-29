"""
推送任务链 - 快速推送任务到Redis
"""

import asyncio
from flowmix.sender import Pub
from flowmix.common.queue import RedisQueue
from flowmix.common.pool import RedisPool


async def main():
    redis_url = 'redis://localhost:6379/0'
    queue_name = "tasks"

    print("=" * 60)
    print("📤 推送任务链到 Redis")
    print("=" * 60)

    # 创建 Redis 连接池
    print("\n1. 连接 Redis...")
    try:
        pool = await RedisPool.get_instance(redis_url=redis_url)
        print("   ✓ Redis 连接成功")
    except Exception as e:
        print(f"   ✗ Redis 连接失败: {e}")
        return

    # 创建队列
    queue = RedisQueue(pool=pool, queue_name=queue_name)

    # 清空旧数据
    print("\n2. 清空旧数据...")
    await queue.clear_all()
    print("   ✓ 清空完成")

    # 推送根任务
    print("\n3. 推送根任务...")
    pub = Pub(queue=queue)
    root_task_id = await pub.push(
        data={'id': 'ROOT', 'level': 0, 'max_level': 2},
        task_name='process'
    )
    print(f"   ✓ 根任务 ID: {root_task_id}")

    # 推送一些独立任务以便查看统计
    print("\n4. 推送额外的测试任务...")
    for i in range(5):
        task_id = await pub.push(
            data={'id': f'TASK-{i+1}', 'level': 0, 'max_level': 0},
            task_name='process'
        )
        print(f"   ✓ 任务 #{task_id}")

    print("\n" + "=" * 60)
    print("✅ 任务推送完成！")
    print("=" * 60)
    print("\n下一步:")
    print("  1. 运行 worker: python examples/run_worker.py")
    print("  2. 查看统计: python examples/stats_example.py")
    print("=" * 60 + "\n")

    # 关闭连接池
    await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
