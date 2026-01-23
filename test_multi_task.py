"""测试多任务场景下的 task_name 处理"""

import asyncio
import time
from flowmix import Task, Worker

# 创建第一个任务：数据处理
process_task = Task(name='process_data', concurrency_limit=2)

@process_task.execute
async def process_data(data):
    """处理数据"""
    print(f"  📊 Processing: {data['item']}")
    await asyncio.sleep(0.5)  # 模拟处理
    return {'processed': data['item'], 'timestamp': time.time()}

@process_task.on_success
async def on_process_success(data, result):
    """处理成功后的回调"""
    print(f"  ✅ Processed successfully: {result['processed']}")

# 创建第二个任务：数据验证
validate_task = Task(name='validate_data', concurrency_limit=3)

@validate_task.execute
async def validate_data(data):
    """验证数据"""
    print(f"  🔍 Validating: {data['item']}")
    await asyncio.sleep(0.3)  # 模拟验证
    is_valid = len(data['item']) > 3
    return {'item': data['item'], 'valid': is_valid}

@validate_task.on_success
async def on_validate_success(data, result):
    """验证成功后的回调"""
    status = "✓" if result['valid'] else "✗"
    print(f"  {status} Validation result: {result['item']} -> {result['valid']}")

# 创建第三个任务：数据保存
save_task = Task(name='save_data', concurrency_limit=1)

@save_task.execute
async def save_data(data):
    """保存数据"""
    print(f"  💾 Saving: {data['item']}")
    await asyncio.sleep(0.2)  # 模拟保存
    return {'saved': data['item'], 'id': hash(data['item'])}

@save_task.on_success
async def on_save_success(data, result):
    """保存成功后的回调"""
    print(f"  ✅ Saved with ID: {result['id']}")


async def test_correct_usage():
    """测试1：正确使用 task_name"""
    print("\n" + "="*60)
    print("测试 1: 正确指定 task_name（多任务场景）")
    print("="*60)

    # 创建 Worker，注册多个任务
    worker = Worker(
        tasks={
            'process_data': process_task,
            'validate_data': validate_task,
            'save_data': save_task
        },
        num_workers=3,
        db_path="test_multi_task.db"
    )

    print(f"\n📋 已注册的任务: {list(worker.tasks.keys())}")

    # 提交不同的任务（正确指定 task_name）
    print("\n📤 提交任务到队列...")

    await worker.push({'item': 'apple'}, task_name='process_data')
    await worker.push({'item': 'banana'}, task_name='validate_data')
    await worker.push({'item': 'cat'}, task_name='validate_data')
    await worker.push({'item': 'data123'}, task_name='save_data')
    await worker.push({'item': 'orange'}, task_name='process_data')

    print(f"✓ 已提交 5 个任务\n")

    # 启动 worker 处理（3秒后自动停止）
    print("🚀 启动 Worker 处理任务...\n")

    # 启动 worker
    worker.running = True
    workers = [
        asyncio.create_task(worker._worker_loop_async(f"worker-{i}"))
        for i in range(worker.num_workers)
    ]

    # 运行 3 秒后停止
    await asyncio.sleep(3)
    worker.running = False

    # 等待所有 worker 停止
    for w in workers:
        w.cancel()
    await asyncio.gather(*workers, return_exceptions=True)

    # 显示统计
    stats = worker.get_stats()
    print(f"\n📊 执行统计:")
    print(f"   - 处理总数: {stats['processed']}")
    print(f"   - 成功: {stats['success']}")
    print(f"   - 失败: {stats['failed']}")
    print(f"   - 重试: {stats['retried']}")

    # 关闭数据库连接
    await worker._manager.close()


async def test_missing_task_name():
    """测试2：缺失 task_name（演示新的错误处理）"""
    print("\n" + "="*60)
    print("测试 2: 缺失 task_name 的错误处理")
    print("="*60)

    # 创建新的 Worker
    worker = Worker(
        tasks={
            'task_a': Task(name='task_a'),
            'task_b': Task(name='task_b'),
        },
        num_workers=1,
        db_path="test_missing_name.db"
    )

    print(f"\n📋 已注册的任务: {list(worker.tasks.keys())}")

    # 先初始化数据库
    await worker._manager._init_db()

    # 直接向数据库插入一个没有 task_name 的消息（模拟旧数据）
    print("\n⚠️  模拟插入一个缺失 task_name 的旧消息...")

    conn = await worker._manager._get_connection()
    cursor = await conn.execute("""
        INSERT INTO tasks (data, status, priority, created_at)
        VALUES (?, 'pending', 0, datetime('now'))
    """, ['{"test": "old message without task_name"}'])
    await conn.commit()
    msg_id = cursor.lastrowid
    print(f"✓ 插入了消息 ID: {msg_id}\n")

    # 尝试处理这个消息
    print("🚀 启动 Worker，观察错误处理...\n")

    worker.running = True
    workers = [asyncio.create_task(worker._worker_loop_async("test-worker"))]

    await asyncio.sleep(1)  # 运行1秒
    worker.running = False

    for w in workers:
        w.cancel()
    await asyncio.gather(*workers, return_exceptions=True)

    # 检查消息状态
    cursor = await conn.execute("SELECT status, error FROM tasks WHERE id = ?", (msg_id,))
    row = await cursor.fetchone()
    status, error = row[0], row[1]

    print(f"\n✅ 错误处理验证:")
    print(f"   - 消息状态: {status}")
    print(f"   - 错误信息: {error}")
    print(f"   - Worker 继续运行: 正常")

    await worker._manager.close()


async def test_single_task():
    """测试3：单任务场景（可以省略 task_name）"""
    print("\n" + "="*60)
    print("测试 3: 单任务场景（task_name 可选）")
    print("="*60)

    single_task = Task(name='single')

    @single_task.execute
    async def process(data):
        print(f"  ⚙️  Processing: {data['value']}")
        await asyncio.sleep(0.2)
        return data['value'] * 2

    worker = Worker(
        tasks=single_task,  # 只有一个任务
        num_workers=1,
        db_path="test_single_task.db"
    )

    print(f"\n📋 已注册的任务: {list(worker.tasks.keys())}")
    print("\n📤 提交任务（不指定 task_name）...")

    # 单任务时可以省略 task_name
    await worker.push({'value': 10})
    await worker.push({'value': 20})
    await worker.push({'value': 30})

    print(f"✓ 已提交 3 个任务\n")

    print("🚀 启动 Worker...\n")

    worker.running = True
    workers = [asyncio.create_task(worker._worker_loop_async("worker-0"))]

    await asyncio.sleep(2)
    worker.running = False

    for w in workers:
        w.cancel()
    await asyncio.gather(*workers, return_exceptions=True)

    stats = worker.get_stats()
    print(f"\n📊 执行统计: 成功 {stats['success']}/{stats['processed']}")

    # 关闭数据库连接
    await worker._manager.close()


async def main():
    print("\n🧪 Flowmix 多任务 task_name 处理测试")
    print("测试版本: v0.5.3")

    # 运行所有测试
    await test_correct_usage()
    await test_single_task()
    await test_missing_task_name()

    print("\n" + "="*60)
    print("✅ 所有测试完成！")
    print("="*60)

    # 清理测试数据库
    import os
    for db in ["test_multi_task.db", "test_missing_name.db", "test_single_task.db"]:
        if os.path.exists(db):
            os.remove(db)
            print(f"🧹 清理: {db}")


if __name__ == "__main__":
    asyncio.run(main())
