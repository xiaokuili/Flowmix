"""
Redis Stats 完整示例 - 展示如何使用 RedisStats 进行任务统计和监控

展示功能：
1. 使用 Redis 队列创建和执行任务
2. 使用 RedisStats 进行实时任务查询
3. 查看 Worker 性能统计
4. 监控正在处理和失败的任务
5. 按任务类型和时间范围统计
"""

import asyncio
import redis.asyncio as aioredis
from flowmix import Task, TaskRunner, RunnerConfig
from flowmix.common.queue import RedisQueue
from flowmix.sender import Pub
from flowmix.stats import RedisStats
from datetime import datetime
import random


# ============================================================================
# 定义示例任务
# ============================================================================

# 任务1: 数据处理任务（成功率高）
process_task = Task(name='process_data')

@process_task.execute
async def execute_process(data):
    """模拟数据处理"""
    item_id = data.get('item_id')
    print(f"  🔄 正在处理数据项 {item_id}")

    # 模拟处理耗时
    await asyncio.sleep(random.uniform(0.5, 1.5))

    # 极少概率失败（5%）
    if random.random() < 0.05:
        raise Exception(f"数据项 {item_id} 处理失败")

    result = {
        "item_id": item_id,
        "processed": True,
        "value": random.randint(100, 1000)
    }

    print(f"  ✅ 数据项 {item_id} 处理完成")
    return result


# 任务2: 网络请求任务（有一定失败率）
fetch_task = Task(name='fetch_url')

@fetch_task.execute
async def execute_fetch(data):
    """模拟网络请求"""
    url = data.get('url')
    print(f"  🌐 正在获取 {url}")

    # 模拟网络延迟
    await asyncio.sleep(random.uniform(0.3, 1.0))

    # 20% 失败率
    if random.random() < 0.2:
        raise Exception(f"网络错误: 无法访问 {url}")

    result = {
        "url": url,
        "status": 200,
        "content_length": random.randint(1000, 10000)
    }

    print(f"  ✅ 获取成功: {url}")
    return result


# 任务3: 计算任务（快速执行）
calculate_task = Task(name='calculate')

@calculate_task.execute
async def execute_calculate(data):
    """模拟快速计算"""
    numbers = data.get('numbers', [])
    print(f"  🧮 计算中...")

    await asyncio.sleep(0.1)

    result = {
        "sum": sum(numbers),
        "avg": sum(numbers) / len(numbers) if numbers else 0,
        "count": len(numbers)
    }

    print(f"  ✅ 计算完成")
    return result


# ============================================================================
# 任务推送函数
# ============================================================================

async def push_tasks(pub: Pub):
    """推送示例任务"""
    print("\n" + "="*80)
    print("📤 推送任务到 Redis 队列")
    print("="*80 + "\n")

    task_ids = []

    # 推送数据处理任务
    print("推送数据处理任务...")
    for i in range(10):
        task_id = await pub.push(
            data={"item_id": i + 1},
            task_name="process_data"
        )
        task_ids.append(task_id)
    print(f"  ✓ 推送了 10 个 process_data 任务\n")

    # 推送网络请求任务
    print("推送网络请求任务...")
    urls = [
        "http://api.example.com/users",
        "http://api.example.com/posts",
        "http://api.example.com/comments",
        "http://api.example.com/products",
        "http://api.example.com/orders",
    ]
    for url in urls:
        task_id = await pub.push(
            data={"url": url},
            task_name="fetch_url"
        )
        task_ids.append(task_id)
    print(f"  ✓ 推送了 {len(urls)} 个 fetch_url 任务\n")

    # 推送计算任务
    print("推送计算任务...")
    for i in range(5):
        numbers = [random.randint(1, 100) for _ in range(10)]
        task_id = await pub.push(
            data={"numbers": numbers},
            task_name="calculate"
        )
        task_ids.append(task_id)
    print(f"  ✓ 推送了 5 个 calculate 任务\n")

    print(f"总计推送 {len(task_ids)} 个任务")

    return task_ids


# ============================================================================
# 统计查询函数
# ============================================================================

async def query_stats(stats: RedisStats):
    """查询并展示统计信息"""

    print("\n" + "="*80)
    print("📊 统计信息查询")
    print("="*80)

    # ========================================================================
    # 1. 任务链查询
    # ========================================================================
    print("\n【1. 任务链查询】")
    print("-"*80)

    # 查询第一个任务
    task = await stats.task.get_task(task_id=1)
    if task:
        print(f"\n任务 #1 详情:")
        print(f"  任务名: {task['task_name']}")
        print(f"  状态: {task['status']}")
        print(f"  Worker: {task.get('worker_id', 'N/A')}")
        print(f"  创建时间: {task['created_at']}")
        if task.get('completed_at'):
            print(f"  完成时间: {task['completed_at']}")

    # 查询任务链统计（假设任务 1 是根任务）
    summary = await stats.task.get_chain_summary(root_id=1)
    print(f"\n任务链 #1 统计摘要:")
    print(f"  总任务数: {summary['total']}")
    print(f"  待处理: {summary['pending']}")
    print(f"  处理中: {summary['processing']}")
    print(f"  已完成: {summary['completed']}")
    print(f"  失败: {summary['failed']}")

    # ========================================================================
    # 2. Runner 整体性能统计
    # ========================================================================
    print("\n【2. Runner 整体性能统计】")
    print("-"*80)

    perf = await stats.runner.get_performance()
    print(f"\n整体性能指标:")
    print(f"  总任务数: {perf['total']}")
    print(f"  已完成: {perf['completed']}")
    print(f"  失败: {perf['failed']}")
    print(f"  待处理: {perf['pending']}")
    print(f"  处理中: {perf['processing']}")
    print(f"  成功率: {perf['success_rate']*100:.2f}%")
    print(f"  吞吐量: {perf['qps']:.2f} tasks/s")
    print(f"  平均执行时长: {perf['avg_duration_seconds']:.2f} 秒")

    # ========================================================================
    # 3. 按任务类型统计
    # ========================================================================
    print("\n【3. 按任务类型统计】")
    print("-"*80)

    by_type = await stats.runner.get_performance_by_task_type()
    print(f"\n任务类型性能对比:")
    for task_type, task_stats in by_type.items():
        print(f"\n  {task_type}:")
        print(f"    总数: {task_stats['total']}")
        print(f"    完成: {task_stats['completed']}")
        print(f"    失败: {task_stats['failed']}")
        print(f"    成功率: {task_stats['success_rate']*100:.2f}%")
        print(f"    平均耗时: {task_stats['avg_duration_seconds']:.2f} 秒")

    # ========================================================================
    # 4. Worker 信息
    # ========================================================================
    print("\n【4. Worker 信息】")
    print("-"*80)

    workers = await stats.runner.list_workers()
    print(f"\nWorker 列表 (共 {len(workers)} 个):")
    for w in workers:
        status_emoji = "🟢" if w['is_active'] else "🔴"
        success_rate = (w['completed'] / w['total_tasks'] * 100) if w['total_tasks'] > 0 else 0
        print(f"\n  {status_emoji} {w['worker_id']}:")
        print(f"     总任务: {w['total_tasks']} | 完成: {w['completed']} | 失败: {w['failed']}")
        print(f"     成功率: {success_rate:.1f}%")
        print(f"     首次活跃: {w['first_seen']}")
        print(f"     最后活跃: {w['last_seen']}")

    # ========================================================================
    # 5. 实时监控
    # ========================================================================
    print("\n【5. 实时监控】")
    print("-"*80)

    # 正在处理的任务
    processing = await stats.monitor.get_processing_tasks()
    if processing:
        print(f"\n正在处理的任务 (共 {len(processing)} 个):")
        for task in processing[:5]:
            print(f"  任务 #{task['task_id']}:")
            print(f"    Worker: {task['worker_id']}")
            print(f"    类型: {task['task_type']}")
            print(f"    已执行: {task['duration_seconds']:.1f} 秒")
    else:
        print("\n当前没有正在处理的任务")

    # 失败任务
    failed = await stats.monitor.get_failed_tasks(limit=10)
    if failed:
        print(f"\n失败任务 (最近 {len(failed)} 个):")
        for task in failed[:5]:
            print(f"  任务 #{task['task_id']}:")
            print(f"    类型: {task['task_type']}")
            print(f"    错误: {task['error']}")
            print(f"    失败时间: {task['failed_at']}")
    else:
        print("\n没有失败的任务")

    # 错误汇总
    errors = await stats.monitor.get_error_summary()
    if errors:
        print(f"\n错误汇总 (共 {len(errors)} 种错误):")
        for error, count in list(errors.items())[:5]:
            print(f"  [{count}次] {error}")
    else:
        print("\n没有错误记录")

    # ========================================================================
    # 6. 任务链维度统计
    # ========================================================================
    print("\n【6. 任务链维度统计】")
    print("-"*80)

    chain_stats = await stats.runner.get_chain_stats()
    print(f"\n任务链统计:")
    print(f"  总任务链数: {chain_stats['total_chains']}")
    print(f"  已完成: {chain_stats['completed_chains']}")
    print(f"  正在处理: {chain_stats['processing_chains']}")
    print(f"  全部成功: {chain_stats['success_chains']}")
    print(f"  部分失败: {chain_stats['partial_failed_chains']}")

    if chain_stats['total_chains'] > 0:
        completion_rate = chain_stats['completed_chains'] / chain_stats['total_chains']
        print(f"  完成率: {completion_rate * 100:.1f}%")

        if chain_stats['completed_chains'] > 0:
            success_rate = chain_stats['success_chains'] / chain_stats['completed_chains']
            print(f"  成功率: {success_rate * 100:.1f}%")


# ============================================================================
# 主函数
# ============================================================================

async def main():
    """主函数"""

    print("\n" + "="*80)
    print("🎯 Redis Stats 完整示例")
    print("="*80)

    # 1. 创建 Redis 连接
    print("\n正在连接 Redis...")
    redis = await aioredis.from_url("redis://localhost:6379/0", decode_responses=True)
    print("✓ Redis 连接成功")

    queue_name = "demo_tasks"

    # 2. 创建 RedisQueue 和 Pub
    queue = RedisQueue(redis=redis, queue_name=queue_name)
    pub = Pub(queue=queue)

    # 3. 推送任务
    task_ids = await push_tasks(pub)

    # 4. 创建并启动 Runner（后台执行）
    print("\n" + "="*80)
    print("🚀 启动 TaskRunner (2 个 Worker)")
    print("="*80 + "\n")

    runner = TaskRunner(
        tasks={
            "process_data": process_task,
            "fetch_url": fetch_task,
            "calculate": calculate_task,
        },
        url="redis://localhost:6379/0",
        queue_name=queue_name,
        config=RunnerConfig(
            num_workers=2,
            max_retries=1,
            retry_delay=0.5,
            log_level="WARNING"  # 降低日志级别以简化输出
        )
    )

    # 在后台启动 runner
    runner_task = asyncio.create_task(runner.run())

    # 等待一会儿让任务开始执行
    print("等待任务开始执行...\n")
    await asyncio.sleep(2)

    # 5. 创建 RedisStats 并查询统计信息
    stats = RedisStats(redis=redis, queue_name=queue_name)

    # 第一次查询（任务执行中）
    print("\n" + "="*80)
    print("📸 第一次查询（任务执行中）")
    print("="*80)
    await query_stats(stats)

    # 等待任务执行
    print("\n" + "="*80)
    print("⏳ 等待任务执行...")
    print("="*80)
    await asyncio.sleep(5)

    # 第二次查询（更多任务完成）
    print("\n" + "="*80)
    print("📸 第二次查询（更多任务完成）")
    print("="*80)
    await query_stats(stats)

    # 等待所有任务完成
    print("\n" + "="*80)
    print("⏳ 等待所有任务完成...")
    print("="*80)

    # 等待 runner 完成（超时 30 秒）
    try:
        await asyncio.wait_for(runner_task, timeout=30)
    except asyncio.TimeoutError:
        print("⚠️  Runner 超时，强制停止")

    # 最终查询（所有任务完成）
    print("\n" + "="*80)
    print("📸 最终查询（所有任务完成）")
    print("="*80)
    await query_stats(stats)

    # 6. 清理资源
    await stats.close()
    await redis.close()

    print("\n" + "="*80)
    print("✅ 示例完成！")
    print("="*80)
    print("\n总结：")
    print("  • RedisStats 提供了完整的任务统计和监控能力")
    print("  • 支持任务链查询、性能统计、实时监控等功能")
    print("  • 可按任务类型、Worker、时间范围等维度查询")
    print("  • 适合生产环境的任务监控和性能分析")
    print()


if __name__ == "__main__":
    asyncio.run(main())
