"""测试多 worker 并发场景 - 验证任务不会重复执行"""
import asyncio
from flowmix import Task, Worker

# 全局计数器，记录每个任务被执行的次数
execution_count = {}
execution_lock = asyncio.Lock()

# 创建测试任务
test_task = Task(name='test')

@test_task.execute
async def process(data):
    """处理任务 - 记录执行次数"""
    task_id = data['task_id']

    async with execution_lock:
        if task_id not in execution_count:
            execution_count[task_id] = 0
        execution_count[task_id] += 1
        count = execution_count[task_id]

    # 模拟一些处理时间
    await asyncio.sleep(0.1)

    print(f"Task {task_id} executed (count={count})")
    return {'task_id': task_id, 'count': count}


async def test_concurrent_workers():
    """测试并发 worker - 验证每个任务只被执行一次"""
    print("\n" + "="*60)
    print("测试: 多 Worker 并发执行 - 验证无重复消费")
    print("="*60)

    # 清空计数器
    execution_count.clear()

    # 创建 Worker（20个并发）
    worker = Worker(
        tasks={'test': test_task},
        num_workers=20,  # 20个并发 worker
        db_path="test_concurrent.db"
    )

    # 提交 10 个任务
    num_tasks = 10
    print(f"\n📤 提交 {num_tasks} 个任务...")
    for i in range(num_tasks):
        await worker.push({'task_id': i}, task_name='test')

    print(f"🚀 启动 20 个并发 Worker...\n")

    # 运行到队列为空
    await worker.run(auto_stop=True)

    # 验证结果
    print(f"\n📊 执行结果:")
    print(f"   - 总任务数: {num_tasks}")
    print(f"   - 已执行: {len(execution_count)}")

    # 检查是否有重复执行
    duplicates = {task_id: count for task_id, count in execution_count.items() if count > 1}

    if duplicates:
        print(f"\n❌ 发现重复执行:")
        for task_id, count in duplicates.items():
            print(f"   - Task {task_id}: 执行了 {count} 次")
        success = False
    else:
        print(f"\n✅ 所有任务都只执行了一次!")
        success = True

    # 显示详细信息
    print(f"\n详细执行统计:")
    for task_id in range(num_tasks):
        count = execution_count.get(task_id, 0)
        status = "✅" if count == 1 else ("❌" if count > 1 else "⚠️")
        print(f"   {status} Task {task_id}: {count} 次")

    stats = worker.get_stats()
    print(f"\n Worker 统计:")
    print(f"   - 处理总数: {stats['processed']}")
    print(f"   - 成功: {stats['success']}")
    print(f"   - 失败: {stats['failed']}")

    await worker._manager.close()

    return success


async def test_with_callback():
    """测试带 callback 的场景"""
    print("\n" + "="*60)
    print("测试: 带 Callback 的并发场景")
    print("="*60)

    # 清空计数器
    execution_count.clear()

    # 创建爬虫任务
    crawl_task = Task(name='crawl')

    @crawl_task.execute
    async def crawl(data):
        """爬取并生成子任务"""
        task_id = data['task_id']
        depth = data.get('depth', 0)

        async with execution_lock:
            if task_id not in execution_count:
                execution_count[task_id] = 0
            execution_count[task_id] += 1
            count = execution_count[task_id]

        print(f"{'  ' * depth}Crawl Task {task_id} executed (depth={depth}, count={count})")

        # 只在第一层生成子任务
        if depth == 0:
            for i in range(3):
                child_id = f"{task_id}-{i}"
                await crawl_task.callback('crawl', {
                    'task_id': child_id,
                    'depth': depth + 1
                })

        await asyncio.sleep(0.05)
        return {'task_id': task_id, 'depth': depth}

    # 创建 Worker
    worker = Worker(
        tasks={'crawl': crawl_task},
        num_workers=20,
        db_path="test_concurrent_callback.db"
    )

    # 提交 5 个根任务
    num_root_tasks = 5
    print(f"\n📤 提交 {num_root_tasks} 个根任务（每个会生成 3 个子任务）...")
    for i in range(num_root_tasks):
        await worker.push({'task_id': i, 'depth': 0}, task_name='crawl')

    print(f"🚀 启动 20 个并发 Worker...\n")

    # 运行
    await worker.run(auto_stop=True)

    # 验证
    expected_total = num_root_tasks * 4  # 5 个根任务 + 5*3 个子任务
    print(f"\n📊 执行结果:")
    print(f"   - 预期任务数: {expected_total}")
    print(f"   - 实际执行: {len(execution_count)}")

    # 检查重复
    duplicates = {task_id: count for task_id, count in execution_count.items() if count > 1}

    if duplicates:
        print(f"\n❌ 发现重复执行:")
        for task_id, count in sorted(duplicates.items()):
            print(f"   - Task {task_id}: 执行了 {count} 次")
        success = False
    else:
        print(f"\n✅ 所有任务都只执行了一次!")
        success = True

    stats = worker.get_stats()
    print(f"\n Worker 统计:")
    print(f"   - 处理总数: {stats['processed']}")
    print(f"   - 成功: {stats['success']}")

    await worker._manager.close()

    return success


async def main():
    print("\n🧪 Flowmix 并发 Worker 测试")
    print("测试目标: 验证修复后多 worker 不会重复消费任务")

    # 测试1: 基础并发
    success1 = await test_concurrent_workers()

    # 测试2: 带 callback
    success2 = await test_with_callback()

    # 总结
    print("\n" + "="*60)
    if success1 and success2:
        print("✅ 所有测试通过 - 无重复消费问题!")
    else:
        print("❌ 测试失败 - 仍存在重复消费问题")
    print("="*60)

    # 清理测试数据库
    import os
    for db in ["test_concurrent.db", "test_concurrent_callback.db"]:
        if os.path.exists(db):
            os.remove(db)
            print(f"🧹 清理: {db}")


if __name__ == "__main__":
    asyncio.run(main())
