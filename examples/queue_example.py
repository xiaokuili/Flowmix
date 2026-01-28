"""
Queue 模块使用示例

演示如何使用 flowmix.common.queue 模块
"""

import asyncio
from flowmix.common import RedisPool, SQLitePool, RedisQueue, SQLiteQueue


async def example_redis_queue():
    """Redis 队列示例"""
    print("=== Redis Queue 示例 ===\n")

    # 1. 获取 RedisPool 单例
    pool = await RedisPool.get_instance('redis://localhost:6379/0')

    # 2. 创建 RedisQueue
    queue = RedisQueue(pool=pool, queue_name='example_tasks')

    # 3. 清空队列（测试前清理）
    await queue.clear_all()

    # 4. 推送任务
    print("推送任务到队列...")
    msg_id1 = await queue.push(
        data={'url': 'https://example.com/page1'},
        task_name='crawl',
        priority=5
    )
    print(f"  - 任务 1: id={msg_id1}, priority=5")

    msg_id2 = await queue.push(
        data={'url': 'https://example.com/page2'},
        task_name='crawl',
        priority=10,
        parent_id=msg_id1
    )
    print(f"  - 任务 2: id={msg_id2}, priority=10 (子任务)")

    msg_id3 = await queue.push(
        data={'url': 'https://example.com/page3'},
        task_name='crawl',
        priority=1
    )
    print(f"  - 任务 3: id={msg_id3}, priority=1\n")

    # 5. 查看队列状态
    pending = await queue.get_pending_count()
    total = await queue.get_stream_length()
    print(f"队列状态: pending={pending}, total={total}\n")

    # 6. 取出任务（按优先级降序）
    print("从队列取出任务（按优先级）...")
    message = await queue.pop('worker-1')
    print(f"  - 取出: id={message['id']}, task={message['task_name']}, data={message['data']}")

    # 7. 确认任务完成
    await queue.ack(
        message_id=message['id'],
        failed=False,
        result={'status': 'ok', 'links': 10},
        fingerprint='abc123'
    )
    print(f"  - 已确认任务 {message['id']} 完成\n")

    # 8. 继续取出其他任务
    message = await queue.pop('worker-1')
    print(f"  - 取出: id={message['id']}, priority=5\n")

    # 9. 关闭
    await queue.close()
    await pool.close()
    print("Redis Queue 示例完成")


async def example_sqlite_queue():
    """SQLite 队列示例"""
    print("\n=== SQLite Queue 示例 ===\n")

    # 1. 获取 SQLitePool 单例
    pool = await SQLitePool.get_instance('.flowmix/example.db')

    # 2. 创建 SQLiteQueue
    queue = SQLiteQueue(pool=pool, queue_name='example_tasks')

    # 3. 清空队列（测试前清理）
    await queue.clear_all()

    # 4. 推送任务
    print("推送任务到队列...")
    msg_id1 = await queue.push(
        data={'task': 'process_data', 'batch_id': 1},
        task_name='batch_process',
        priority=0
    )
    print(f"  - 任务 1: id={msg_id1}")

    msg_id2 = await queue.push(
        data={'task': 'process_data', 'batch_id': 2},
        task_name='batch_process',
        priority=0
    )
    print(f"  - 任务 2: id={msg_id2}\n")

    # 5. 查看队列状态
    pending = await queue.get_pending_count()
    total = await queue.get_stream_length()
    print(f"队列状态: pending={pending}, total={total}\n")

    # 6. 取出并处理任务
    print("处理任务...")
    message = await queue.pop('worker-local')
    print(f"  - 取出: id={message['id']}, data={message['data']}")

    await queue.ack(
        message_id=message['id'],
        failed=False,
        result={'processed': 100}
    )
    print(f"  - 已确认任务 {message['id']} 完成\n")

    # 7. 关闭
    await queue.close()
    await pool.close()
    print("SQLite Queue 示例完成")


async def main():
    """主函数"""
    # 运行 Redis 示例（需要先启动 Redis）
    try:
        await example_redis_queue()
    except Exception as e:
        print(f"Redis 示例失败: {e}")
        print("提示: 请先启动 Redis (docker-compose up -d redis)")

    # 运行 SQLite 示例
    await example_sqlite_queue()


if __name__ == "__main__":
    asyncio.run(main())
