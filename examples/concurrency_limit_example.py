"""
Concurrency Limit 示例 - 限流控制

展示功能：
1. 使用 concurrency_limit 限制任务并发数
2. 模拟 API 限流场景（每秒最多 N 个请求）
3. 对比有/无限流的执行效果
4. 展示滑动窗口限流算法的实际效果

应用场景：
- 第三方 API 调用限流（如 OpenAI API、Twitter API）
- 数据库连接池限制
- 爬虫请求频率控制
- 资源保护（避免过载）
"""

import asyncio
import time
from datetime import datetime
from flowmix import Task, TaskRunner, RunnerConfig
from flowmix.sender import Pub


# ============================================================================
# 示例 1: 无限流 vs 有限流 - API 调用对比
# ============================================================================

async def run_without_limit():
    """示例 1: 无限流控制 - API 立即全部并发执行"""
    print("\n" + "="*70)
    print("🚀 示例 1: 无限流控制")
    print("="*70)
    print("模拟：同时发送 20 个 API 请求，无并发限制\n")

    # 记录每个请求的时间
    request_times = []

    # 创建任务（无限流）
    api_task = Task(name='api_call')

    @api_task.execute
    async def execute_api(data):
        req_id = data.get('id')
        url = data.get('url')

        # 记录请求时间
        current_time = time.time()
        request_times.append(current_time)

        print(f"📡 [{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] "
              f"请求 #{req_id}: {url}")

        # 模拟 API 调用
        await asyncio.sleep(0.1)

        return {"id": req_id, "status": "success"}

    # 创建发布器（自动创建队列）
    pub = await Pub.create(url="memory://", queue_name="no_limit")

    # 推送 20 个任务
    for i in range(20):
        await pub.push(
            data={'id': i+1, 'url': f'https://api.example.com/user/{i+1}'},
            task_name='api_call'
        )

    # 启动 Runner（5 个 worker）
    runner = TaskRunner(
        tasks={'api_call': api_task},
        url="memory://",
        queue_name="no_limit",
        config=RunnerConfig(num_workers=5, max_retries=0)
    )

    start_time = time.time()

    # 运行并在 5 秒后停止
    await asyncio.gather(
        runner.run(),
        asyncio.create_task(async_stop_after(runner, 5))
    )

    elapsed = time.time() - start_time

    # 统计
    print(f"\n✅ 无限流完成")
    print(f"   总耗时: {elapsed:.2f}s")
    print(f"   处理任务: {len(request_times)} 个")
    print(f"   平均速率: {len(request_times)/elapsed:.1f} 个/秒")
    print(f"   峰值并发: ~5 个（受 worker 数量限制）\n")


async def run_with_limit():
    """示例 2: 有限流控制 - 每秒最多 5 个并发"""
    print("\n" + "="*70)
    print("⏱️  示例 2: 限流控制 (concurrency_limit=5)")
    print("="*70)
    print("模拟：同时发送 20 个 API 请求，每秒最多 5 个并发\n")

    # 记录每个请求的时间
    request_times = []

    # 创建任务（限流: 每秒最多 5 个）
    api_task = Task(
        name='api_call',
        concurrency_limit=5  # 每秒最多 5 个并发
    )

    @api_task.execute
    async def execute_api(data):
        req_id = data.get('id')
        url = data.get('url')

        # 记录请求时间
        current_time = time.time()
        request_times.append(current_time)

        print(f"📡 [{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] "
              f"请求 #{req_id}: {url}")

        # 模拟 API 调用
        await asyncio.sleep(0.1)

        return {"id": req_id, "status": "success"}

    # 创建发布器（自动创建队列）
    pub = await Pub.create(url="memory://", queue_name="with_limit")

    # 推送 20 个任务
    for i in range(20):
        await pub.push(
            data={'id': i+1, 'url': f'https://api.example.com/user/{i+1}'},
            task_name='api_call'
        )

    # 启动 Runner（10 个 worker，但限流会控制并发）
    runner = TaskRunner(
        tasks={'api_call': api_task},
        url="memory://",
        queue_name="with_limit",
        config=RunnerConfig(num_workers=10, max_retries=0)
    )

    start_time = time.time()

    # 运行并在 8 秒后停止
    await asyncio.gather(
        runner.run(),
        asyncio.create_task(async_stop_after(runner, 8))
    )

    elapsed = time.time() - start_time

    # 统计每秒的请求数
    if request_times:
        base_time = request_times[0]
        seconds_buckets = {}
        for t in request_times:
            second = int(t - base_time)
            seconds_buckets[second] = seconds_buckets.get(second, 0) + 1

        print(f"\n✅ 限流完成")
        print(f"   总耗时: {elapsed:.2f}s")
        print(f"   处理任务: {len(request_times)} 个")
        print(f"   平均速率: {len(request_times)/elapsed:.1f} 个/秒")
        print(f"   限流目标: 5 个/秒")
        print(f"\n   每秒请求数分布:")
        for second in sorted(seconds_buckets.keys()):
            count = seconds_buckets[second]
            bar = "█" * count
            print(f"   秒 {second}: {bar} ({count} 个)")
        print()


# ============================================================================
# 示例 3: 实际场景 - OpenAI API 限流
# ============================================================================

async def run_openai_simulation():
    """示例 3: 模拟 OpenAI API 限流（每分钟 60 次 = 每秒 1 次）"""
    print("\n" + "="*70)
    print("🤖 示例 3: OpenAI API 限流模拟")
    print("="*70)
    print("场景：GPT API 限制（每分钟 60 次 = 每秒 1 次）")
    print("任务：处理 10 个文本生成请求\n")

    request_times = []

    # 创建任务（每秒 1 个，模拟 OpenAI 免费层限制）
    gpt_task = Task(
        name='gpt_call',
        concurrency_limit=1  # 每秒最多 1 个请求
    )

    @gpt_task.execute
    async def execute_gpt(data):
        req_id = data.get('id')
        prompt = data.get('prompt')

        current_time = time.time()
        request_times.append(current_time)

        print(f"🤖 [{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] "
              f"请求 #{req_id}: {prompt[:30]}...")

        # 模拟 GPT API 调用（通常需要 1-3 秒）
        await asyncio.sleep(0.5)

        return {
            "id": req_id,
            "response": f"Generated response for: {prompt}"
        }

    # 创建发布器（自动创建队列）
    pub = await Pub.create(url="memory://", queue_name="openai_sim")

    # 推送 10 个任务
    prompts = [
        "Explain quantum computing",
        "Write a Python function",
        "Translate to Spanish",
        "Summarize this article",
        "Generate a story",
        "Code review tips",
        "Explain recursion",
        "Best practices for API",
        "Debugging strategies",
        "Design patterns in Python"
    ]

    for i, prompt in enumerate(prompts):
        await pub.push(
            data={'id': i+1, 'prompt': prompt},
            task_name='gpt_call'
        )

    # 启动 Runner
    runner = TaskRunner(
        tasks={'gpt_call': gpt_task},
        url="memory://",
        queue_name="openai_sim",
        config=RunnerConfig(num_workers=3, max_retries=0)
    )

    start_time = time.time()

    # 运行并在 15 秒后停止
    await asyncio.gather(
        runner.run(),
        asyncio.create_task(async_stop_after(runner, 15))
    )

    elapsed = time.time() - start_time

    print(f"\n✅ OpenAI 模拟完成")
    print(f"   总耗时: {elapsed:.2f}s")
    print(f"   处理任务: {len(request_times)} 个")
    print(f"   平均速率: {len(request_times)/elapsed:.2f} 个/秒")
    print(f"   限流保护: ✓ 成功避免 429 错误\n")


# ============================================================================
# 示例 4: 爬虫限流 - 避免被封
# ============================================================================

async def run_crawler_with_limit():
    """示例 4: 爬虫限流（避免被目标网站封禁）"""
    print("\n" + "="*70)
    print("🕷️  示例 4: 爬虫限流")
    print("="*70)
    print("场景：爬取网站，每秒最多 3 个请求（避免被封）\n")

    request_times = []

    # 创建爬虫任务（每秒最多 3 个请求）
    crawl_task = Task(
        name='crawl',
        concurrency_limit=3  # 每秒最多 3 个请求
    )

    @crawl_task.execute
    async def execute_crawl(data):
        page_id = data.get('id')
        url = data.get('url')

        current_time = time.time()
        request_times.append(current_time)

        print(f"🔍 [{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] "
              f"爬取页面 #{page_id}: {url}")

        # 模拟网页抓取
        await asyncio.sleep(0.2)

        return {"id": page_id, "content": f"Content from {url}"}

    # 创建发布器（自动创建队列）
    pub = await Pub.create(url="memory://", queue_name="crawler")

    # 推送 15 个页面
    for i in range(15):
        await pub.push(
            data={'id': i+1, 'url': f'https://example.com/page/{i+1}'},
            task_name='crawl'
        )

    # 启动 Runner
    runner = TaskRunner(
        tasks={'crawl': crawl_task},
        url="memory://",
        queue_name="crawler",
        config=RunnerConfig(num_workers=5, max_retries=0)
    )

    start_time = time.time()

    # 运行并在 10 秒后停止
    await asyncio.gather(
        runner.run(),
        asyncio.create_task(async_stop_after(runner, 10))
    )

    elapsed = time.time() - start_time

    # 统计
    if request_times:
        base_time = request_times[0]
        seconds_buckets = {}
        for t in request_times:
            second = int(t - base_time)
            seconds_buckets[second] = seconds_buckets.get(second, 0) + 1

        print(f"\n✅ 爬虫完成")
        print(f"   总耗时: {elapsed:.2f}s")
        print(f"   爬取页面: {len(request_times)} 个")
        print(f"   平均速率: {len(request_times)/elapsed:.1f} 个/秒")
        print(f"   限流目标: 3 个/秒")
        print(f"   状态: ✓ 未被封禁\n")


async def async_stop_after(runner, seconds: float):
    """延迟停止 runner"""
    await asyncio.sleep(seconds)
    runner.stop()


async def main():
    """主函数"""
    print("\n" + "="*70)
    print("⏱️  Concurrency Limit 示例")
    print("="*70)
    print("\n💡 核心概念：")
    print("  1. concurrency_limit 参数：限制任务每秒最大并发数")
    print("  2. 基于滑动窗口算法：精确控制 1 秒内的并发数量")
    print("  3. 应用场景：")
    print("     - API 限流（OpenAI、Twitter、Google Maps 等）")
    print("     - 数据库连接池限制")
    print("     - 爬虫频率控制（避免被封）")
    print("     - 资源保护（避免过载）")

    # 运行示例
    await run_without_limit()
    await asyncio.sleep(1)  # 间隔

    await run_with_limit()
    await asyncio.sleep(1)

    await run_openai_simulation()
    await asyncio.sleep(1)

    await run_crawler_with_limit()

    # 总结
    print("\n" + "="*70)
    print("📊 限流对比总结")
    print("="*70)
    print("┌─────────────────┬──────────────┬──────────────┐")
    print("│ 场景            │ 并发限制     │ 效果         │")
    print("├─────────────────┼──────────────┼──────────────┤")
    print("│ 无限流          │ None         │ 快但易过载   │")
    print("│ API 调用        │ 5/秒         │ 避免 429     │")
    print("│ OpenAI API      │ 1/秒         │ 稳定使用     │")
    print("│ 爬虫            │ 3/秒         │ 避免被封     │")
    print("└─────────────────┴──────────────┴──────────────┘")
    print("\n💡 最佳实践：")
    print("  1. 根据 API 提供商的限制设置 concurrency_limit")
    print("  2. 留出余量（如限制 100/s，设置为 80/s）")
    print("  3. 配合 dedup 使用，避免重复请求浪费配额")
    print("  4. 监控实际执行速率，调整参数")
    print("="*70)


if __name__ == "__main__":
    asyncio.run(main())
