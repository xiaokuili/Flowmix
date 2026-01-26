"""
任务依赖示例

演示：通过 task.callback() 动态提交子任务，构建任务树
"""
import asyncio
from flowmix import Task, TaskQueue, Pub, TaskRunner, RunnerConfig, Cache

crawl_task = Task(name='crawl')

@crawl_task.execute
async def crawl(data):
    url = data['url']
    depth = data.get('depth', 0)
    print(f"  🔍 爬取: {url} (深度: {depth})")

    # 模拟网络请求
    await asyncio.sleep(0.1)

    # 模拟爬取页面，发现新链接
    if depth < 2:  # 限制深度，避免无限递归
        child_urls = [f"{url}/page{i}" for i in range(1, 4)]

        # 动态提交子任务（自动关联父任务）
        for child_url in child_urls:
            await crawl_task.callback(
                'crawl',
                {'url': child_url, 'depth': depth + 1},
                priority=10  # 高优先级 = 深度优先（DFS）
            )

    return {"url": url, "status": "ok"}

@crawl_task.on_success
async def on_success(data, result):
    print(f"    ✅ 完成: {result['url']}")

async def main():
    # 初始化队列和缓存
    queue = TaskQueue(db_path=".flowmix/flowmix.db", queue_name="task_dependency_test")
    cache = Cache(db_path=".flowmix/flowmix.db", queue_name="task_dependency_test")
    pub = Pub(queue=queue)

    print("📋 提交根任务")
    print("-" * 50)

    # 提交根任务
    root_id = await pub.push(
        data={'url': 'http://example.com', 'depth': 0},
        task_name='crawl'
    )

    # 创建运行器并执行
    runner = TaskRunner(
        tasks={'crawl': crawl_task},
        queue=queue,
        cache=cache,
        config=RunnerConfig(num_workers=5)
    )
    await runner.run(auto_stop=True)

    # 查询任务树统计
    stats = await pub.get_tree_stats(root_id)

    print("\n" + "=" * 50)
    print(f"📊 任务树统计:")
    print(f"  - 总任务数: {stats['total']}")
    print(f"  - 已完成: {stats['completed']}")
    print(f"  - 失败: {stats['failed']}")

    # 任务树结构说明
    print(f"\n📈 任务树结构:")
    print(f"  1 个根任务 (深度 0)")
    print(f"  + 3 个一级子任务 (深度 1)")
    print(f"  + 9 个二级子任务 (深度 2)")
    print(f"  = 总共 13 个任务")

if __name__ == "__main__":
    asyncio.run(main())
