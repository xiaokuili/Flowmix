"""
Callback + Priority 示例 - 在 execute 中推送子任务

展示功能：
1. 在 execute 函数中使用 task.callback() 推送新任务
2. 使用 priority 控制任务执行顺序（深度优先 vs 广度优先）
3. 自动构建任务树（parent_id）

Priority 策略：
- priority >= 10 -> 深度优先（DFS）：优先执行子任务
- priority < 10  -> 广度优先（BFS）：优先执行同级任务
"""

import asyncio
from flowmix import Task, TaskRunner, RunnerConfig
from flowmix.sender import Pub


# ============================================================================
# 示例 1: 爬虫任务 - 深度优先 vs 广度优先
# ============================================================================

async def run_crawler_dfs():
    """爬虫示例 - 深度优先（DFS）"""
    print("\n" + "="*70)
    print("🕷️  示例1: 网页爬虫 - 深度优先（DFS）")
    print("="*70)

    # 创建任务
    crawl_task = Task(name='crawl')

    @crawl_task.execute
    async def execute_crawl(data):
        url = data.get('url', '')
        depth = data.get('depth', 0)
        max_depth = data.get('max_depth', 3)

        indent = "  " * depth
        print(f"{indent}🔍 [{depth}] 爬取: {url}")

        # 模拟爬取
        await asyncio.sleep(0.1)

        # 发现子链接并推送
        if depth < max_depth:
            links = [f"{url}/page{i+1}" for i in range(10)]
            print(f"{indent}   → 发现 {len(links)} 个链接，推送子任务 (priority=10)")

            for link in links:
                await crawl_task.callback(
                    task_name='crawl',
                    data={'url': link, 'depth': depth + 1, 'max_depth': max_depth},
                    priority=10  # 高优先级 = 深度优先
                )

        return {"url": url, "depth": depth}

    # 创建发布器（自动创建队列）
    pub = await Pub.create(url="memory://", queue_name="crawl_dfs")

    # 推送根任务
    await pub.push(
        data={'url': 'https://example.com', 'depth': 0, 'max_depth': 2},
        task_name='crawl'
    )

    # 启动 Runner
    runner = TaskRunner(
        tasks={'crawl': crawl_task},
        url="memory://",
        queue_name="crawl_dfs",
        config=RunnerConfig(num_workers=1, max_retries=0)
    )

    # 运行并在8秒后停止
    await asyncio.gather(
        runner.run(),
        asyncio.create_task(async_stop_after(runner, 8))
    )

    print("\n✅ DFS 爬虫完成\n")


async def run_crawler_bfs():
    """爬虫示例 - 广度优先（BFS）"""
    print("\n" + "="*70)
    print("🕷️  示例2: 网页爬虫 - 广度优先（BFS）")
    print("="*70)

    # 创建任务
    crawl_task = Task(name='crawl')

    @crawl_task.execute
    async def execute_crawl(data):
        url = data.get('url', '')
        depth = data.get('depth', 0)
        max_depth = data.get('max_depth', 3)

        indent = "  " * depth
        print(f"{indent}🔍 [{depth}] 爬取: {url}")

        # 模拟爬取
        await asyncio.sleep(0.1)

        # 发现子链接并推送
        if depth < max_depth:
            links = [f"{url}/page{i+1}" for i in range(5)]
            print(f"{indent}   → 发现 {len(links)} 个链接，推送子任务 (priority=0)")

            for link in links:
                await crawl_task.callback(
                    task_name='crawl',
                    data={'url': link, 'depth': depth + 1, 'max_depth': max_depth},
                    priority=0  # 低优先级 = 广度优先
                )

        return {"url": url, "depth": depth}

    # 创建发布器（自动创建队列）
    pub = await Pub.create(url="memory://", queue_name="crawl_bfs")

    # 推送根任务
    await pub.push(
        data={'url': 'https://example.com', 'depth': 0, 'max_depth': 2},
        task_name='crawl'
    )

    # 启动 Runner
    runner = TaskRunner(
        tasks={'crawl': crawl_task},
        url="memory://",
        queue_name="crawl_bfs",
        config=RunnerConfig(num_workers=1, max_retries=0)
    )

    # 运行并在8秒后停止
    await asyncio.gather(
        runner.run(),
        asyncio.create_task(async_stop_after(runner, 8))
    )

    print("\n✅ BFS 爬虫完成\n")


# ============================================================================
# 示例 2: 任务分解 - 展示任务树结构
# ============================================================================

async def run_task_decomposition():
    """任务分解示例"""
    print("\n" + "="*70)
    print("🌳 示例3: 任务分解 - 递归创建子任务")
    print("="*70)

    # 创建任务
    work_task = Task(name='work')

    @work_task.execute
    async def execute_work(data):
        task_id = data.get('id', 'ROOT')
        level = data.get('level', 0)
        max_level = data.get('max_level', 2)

        indent = "  " * level
        print(f"{indent}⚙️  [{level}] 执行任务: {task_id}")

        await asyncio.sleep(0.1)

        # 递归创建子任务
        if level < max_level:
            num_children = 3 if level == 0 else 2
            print(f"{indent}   → 分解为 {num_children} 个子任务 (priority=10)")

            for i in range(num_children):
                child_id = f"{task_id}-{chr(65+i)}" if level == 0 else f"{task_id}-{i+1}"
                await work_task.callback(
                    task_name='work',
                    data={'id': child_id, 'level': level + 1, 'max_level': max_level},
                    priority=10  # 深度优先
                )

        return {"id": task_id, "level": level}

    # 创建发布器（自动创建队列）
    pub = await Pub.create(url="memory://", queue_name="work_tree")

    # 推送根任务
    await pub.push(
        data={'id': 'ROOT', 'level': 0, 'max_level': 2},
        task_name='work'
    )

    # 启动 Runner
    runner = TaskRunner(
        tasks={'work': work_task},
        url="memory://",
        queue_name="work_tree",
        config=RunnerConfig(num_workers=1, max_retries=0)
    )

    # 运行并在8秒后停止
    await asyncio.gather(
        runner.run(),
        asyncio.create_task(async_stop_after(runner, 8))
    )

    print("\n✅ 任务分解完成\n")


async def async_stop_after(runner, seconds: float):
    """延迟停止 runner"""
    await asyncio.sleep(seconds)
    runner.stop()


async def main():
    """主函数"""
    print("\n" + "="*70)
    print("📋 Callback + Priority 示例")
    print("="*70)
    print("\n💡 核心概念：")
    print("  1. 在 execute 中使用 await task.callback() 推送子任务")
    print("  2. priority 参数控制任务优先级：")
    print("     - priority >= 10 → 深度优先（DFS）：先执行子任务")
    print("     - priority < 10  → 广度优先（BFS）：先执行同级任务")
    print("  3. 自动构建任务树：子任务的 parent_id 自动设置为父任务 ID")

    # 运行示例
    await run_crawler_dfs()
    await run_crawler_bfs()
    await run_task_decomposition()

    # 总结
    print("\n" + "="*70)
    print("📊 执行顺序对比")
    print("="*70)
    print("DFS（深度优先，priority=10）:")
    print("  ROOT → A → A-1 → A-2 → B → B-1 → B-2 → C → C-1 → C-2")
    print("  特点：一条路走到底，适合：爬虫、递归处理")
    print("")
    print("BFS（广度优先，priority=0）:")
    print("  ROOT → A → B → C → A-1 → A-2 → B-1 → B-2 → C-1 → C-2")
    print("  特点：逐层展开，适合：批处理、层级分析")
    print("="*70)


if __name__ == "__main__":
    asyncio.run(main())
