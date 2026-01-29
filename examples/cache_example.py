"""
Cache 缓存示例 - Flowmix 任务去重和结果缓存

展示功能：
1. 使用 MemoryCache 进行任务去重
2. 相同任务只执行一次，后续命中缓存
3. 支持 TTL 过期缓存
4. 展示缓存命中和未命中的效果
"""

import asyncio
from flowmix import Task, TaskRunner, RunnerConfig
from flowmix.common.queue import MemoryQueue
from flowmix.runner.cache import MemoryCache
from flowmix.sender import Pub


# 1. 定义 Task
fetch_task = Task(name='fetch')

# 用于追踪任务实际执行次数
execution_count = 0


@fetch_task.execute
async def execute_fetch(data):
    """模拟 API 调用或网页爬取（耗时操作）"""
    global execution_count
    execution_count += 1

    url = data.get('url', 'http://example.com')

    print(f"⏳ [执行 #{execution_count}] 正在获取: {url}")

    # 模拟网络请求（耗时 2 秒）
    await asyncio.sleep(2)

    result = {
        "url": url,
        "status": "ok",
        "content": f"Content from {url}",
        "execution_id": execution_count
    }

    print(f"✅ [执行 #{execution_count}] 获取成功: {url}")
    return result


@fetch_task.on_success
async def on_success(data, result):
    """任务成功回调"""
    url = result.get('url')
    execution_id = result.get('execution_id')
    print(f"   ✓ 成功回调: {url} (执行ID: {execution_id})")


async def main():
    """主函数"""
    global execution_count

    # 创建内存队列
    queue_name = "cache_tasks"
    queue = MemoryQueue(queue_name=queue_name)

    # 创建内存缓存
    cache = MemoryCache()

    print("\n" + "="*70)
    print("🎯 演示场景：爬虫 URL 去重（相同 URL 只爬取一次）")
    print("="*70)

    # 2. 推送任务（包含重复的 URL）
    print("\n📤 推送任务到队列...")
    print("-"*70)

    pub = Pub(queue=queue)

    urls = [
        "http://example.com/page1",
        "http://example.com/page2",
        "http://example.com/page1",  # 重复
        "http://example.com/page3",
        "http://example.com/page2",  # 重复
        "http://example.com/page1",  # 重复
        "http://example.com/page4",
        "http://example.com/page3",  # 重复
    ]

    for i, url in enumerate(urls):
        task_id = await pub.push(
            data={"url": url},
            task_name="fetch"
        )
        print(f"  ✓ 推送任务 #{task_id}: {url}")

    print(f"\n  总计推送 {len(urls)} 个任务（包含 4 个重复 URL）")

    # 3. 第一次运行：建立缓存
    print("\n" + "="*70)
    print("🚀 第一次运行：建立缓存")
    print("="*70 + "\n")

    execution_count = 0  # 重置计数

    runner = TaskRunner(
        tasks={"fetch": fetch_task},
        url="memory://",
        cache=cache,  # 使用内存缓存
        queue_name=queue_name,
        config=RunnerConfig(
            num_workers=2,
            max_retries=1,
            retry_delay=0.5
        )
    )

    await runner.run()

    print("\n" + "="*70)
    print(f"📊 第一次运行统计")
    print("="*70)
    print(f"  • 推送任务数: {len(urls)}")
    print(f"  • 实际执行数: {execution_count}")
    print(f"  • 缓存命中数: {len(urls) - execution_count}")

    # 查看缓存统计
    stats = cache.get_stats()
    print(f"  • 缓存条目数: {stats['total_entries']}")

    # 4. 再次推送相同任务
    print("\n" + "="*70)
    print("📤 再次推送相同的 URL...")
    print("="*70 + "\n")

    for i, url in enumerate(urls):
        task_id = await pub.push(
            data={"url": url},
            task_name="fetch"
        )
        print(f"  ✓ 推送任务 #{task_id}: {url}")

    print(f"\n  总计推送 {len(urls)} 个任务（全部已缓存）")

    # 5. 第二次运行：命中缓存
    print("\n" + "="*70)
    print("🚀 第二次运行：应该全部命中缓存（不会实际执行）")
    print("="*70 + "\n")

    previous_count = execution_count

    runner2 = TaskRunner(
        tasks={"fetch": fetch_task},
        url="memory://",
        cache=cache,  # 使用同一个缓存实例
        queue_name=queue_name,
        config=RunnerConfig(
            num_workers=2,
            max_retries=1,
            retry_delay=0.5
        )
    )

    await runner2.run()

    print("\n" + "="*70)
    print(f"📊 第二次运行统计")
    print("="*70)
    print(f"  • 推送任务数: {len(urls)}")
    print(f"  • 实际执行数: {execution_count - previous_count}")
    print(f"  • 缓存命中数: {len(urls) - (execution_count - previous_count)}")

    # 6. 测试 TTL 缓存
    print("\n" + "="*70)
    print("🕒 演示 TTL 过期缓存（3 秒后过期）")
    print("="*70 + "\n")

    # 清空缓存
    await cache.clear()
    print("  ✓ 已清空缓存")

    # 推送一个任务
    test_url = "http://example.com/ttl-test"
    await pub.push(data={"url": test_url}, task_name="fetch")

    # 创建支持 TTL 的 Task
    ttl_task = Task(name='fetch')

    @ttl_task.execute
    async def execute_with_ttl(data):
        global execution_count
        execution_count += 1
        url = data.get('url')
        print(f"⏳ [执行] 获取: {url}")
        await asyncio.sleep(1)
        print(f"✅ [执行] 完成: {url}")
        return {"url": url, "status": "ok"}

    # 第一次执行
    print("\n  第一次执行...")
    runner3 = TaskRunner(
        tasks={"fetch": ttl_task},
        url="memory://",
        cache=cache,
        queue_name=queue_name,
        config=RunnerConfig(num_workers=1)
    )
    await runner3.run()

    # 立即再次执行（应该命中缓存）
    print("\n  立即再次执行（应该命中缓存）...")
    await pub.push(data={"url": test_url}, task_name="fetch")
    previous_count = execution_count

    runner4 = TaskRunner(
        tasks={"fetch": ttl_task},
        url="memory://",
        cache=cache,
        queue_name=queue_name,
        config=RunnerConfig(num_workers=1)
    )
    await runner4.run()

    if execution_count == previous_count:
        print("  ✓ 缓存命中！")

    # 等待缓存过期
    print("\n  等待 4 秒让缓存过期...")
    await asyncio.sleep(4)

    # 清理过期缓存
    await cache.cleanup(ttl=3)

    # 再次执行（缓存已过期，应该重新执行）
    print("\n  缓存过期后再次执行（应该重新执行）...")
    await pub.push(data={"url": test_url}, task_name="fetch")
    previous_count = execution_count

    runner5 = TaskRunner(
        tasks={"fetch": ttl_task},
        url="memory://",
        cache=cache,
        queue_name=queue_name,
        config=RunnerConfig(num_workers=1)
    )
    await runner5.run()

    if execution_count > previous_count:
        print("  ✓ 缓存已过期，重新执行！")

    # 关闭资源
    await cache.close()

    print("\n" + "="*70)
    print("✅ 演示完成！")
    print("="*70)
    print("\n总结：")
    print("  • MemoryCache 可以有效避免重复任务的执行")
    print("  • 相同的任务数据会生成相同的指纹（fingerprint）")
    print("  • 支持永久缓存和 TTL 过期缓存")
    print("  • 适合爬虫 URL 去重、API 调用缓存等场景")


if __name__ == "__main__":
    asyncio.run(main())
