"""测试异步 callback 功能"""
import asyncio
from flowmix import Task, Worker

# 创建爬虫任务
crawl_task = Task(name='crawl')

@crawl_task.execute
async def crawl(data):
    """爬取页面并发现新链接"""
    url = data['url']
    depth = data.get('depth', 0)

    print(f"{'  ' * depth}🌐 Crawling: {url} (depth={depth})")
    await asyncio.sleep(0.2)  # 模拟网络请求

    # 模拟发现新链接（只爬2层）
    if depth < 2:
        links = [f"{url}/page{i}" for i in range(2)]
        print(f"{'  ' * depth}  Found {len(links)} links")

        # 使用 callback 动态提交子任务
        for link in links:
            await crawl_task.callback('crawl', {
                'url': link,
                'depth': depth + 1
            }, priority=10)  # 高优先级 -> DFS

    return {'url': url, 'links_found': 2 if depth < 2 else 0}

@crawl_task.on_success
async def on_crawl_success(data, result):
    """爬取成功回调"""
    depth = data.get('depth', 0)
    print(f"{'  ' * depth}✅ Crawled: {result['url']} ({result['links_found']} links)")


# 创建分析任务
analyze_task = Task(name='analyze')

@analyze_task.execute
async def analyze(data):
    """分析页面内容"""
    url = data['url']
    print(f"  🔍 Analyzing: {url}")
    await asyncio.sleep(0.1)
    return {'url': url, 'word_count': 1000}

@analyze_task.on_success
async def on_analyze_success(data, result):
    """分析成功后，可以触发保存任务"""
    print(f"  ✅ Analyzed: {result['url']} ({result['word_count']} words)")

    # 在 on_success 中使用 callback
    await analyze_task.callback('save', {
        'url': result['url'],
        'content': f"content_{result['word_count']}"  # 改为 content，避免与内部 data 字段冲突
    })


# 创建保存任务
save_task = Task(name='save')

@save_task.execute
async def save(data):
    """保存数据"""
    print(f"  💾 Saving: {data['url']}")
    await asyncio.sleep(0.1)
    return {'url': data['url'], 'saved': True}

@save_task.on_success
async def on_save_success(data, result):
    """保存成功"""
    print(f"  ✅ Saved: {result['url']}")


async def test_callback_in_execute():
    """测试1：在 execute 中使用 callback（爬虫场景）"""
    print("\n" + "="*60)
    print("测试 1: 在 execute 中使用 callback（动态任务树）")
    print("="*60)

    worker = Worker(
        tasks={'crawl': crawl_task},
        num_workers=3,
        db_path="test_callback_execute.db"
    )

    print("\n📤 提交根任务...")
    await worker.push({'url': 'http://example.com', 'depth': 0}, task_name='crawl')

    print("🚀 启动 Worker...\n")

    # 直接运行到队列为空
    await worker.run(auto_stop=True)

    stats = worker.get_stats()
    print(f"\n📊 执行统计:")
    print(f"   - 总任务数: {stats['processed']}")
    print(f"   - 成功: {stats['success']}")
    print(f"   - 说明: 1个根任务 + 2个子任务 + 4个孙任务 = 7个任务")

    await worker._manager.close()


async def test_callback_in_on_success():
    """测试2：在 on_success 中使用 callback"""
    print("\n" + "="*60)
    print("测试 2: 在 on_success 中使用 callback（任务链）")
    print("="*60)

    worker = Worker(
        tasks={
            'analyze': analyze_task,
            'save': save_task
        },
        num_workers=2,
        db_path="test_callback_success.db"
    )

    print("\n📤 提交分析任务...")
    await worker.push({'url': 'http://example.com/page1'}, task_name='analyze')
    await worker.push({'url': 'http://example.com/page2'}, task_name='analyze')

    print("🚀 启动 Worker...\n")

    await worker.run(auto_stop=True)

    stats = worker.get_stats()
    print(f"\n📊 执行统计:")
    print(f"   - 总任务数: {stats['processed']}")
    print(f"   - 成功: {stats['success']}")
    print(f"   - 说明: 2个分析任务 + 2个保存任务 = 4个任务")

    await worker._manager.close()


async def test_callback_with_parent_tracking():
    """测试3：验证 callback 自动关联 parent_id"""
    print("\n" + "="*60)
    print("测试 3: 验证任务树结构（parent_id 自动关联）")
    print("="*60)

    from flowmix import StatsReader

    worker = Worker(
        tasks={'crawl': crawl_task},
        num_workers=2,
        db_path="test_callback_parent.db"
    )

    # 提交根任务
    root_id = await worker.push({'url': 'http://example.com', 'depth': 0}, task_name='crawl')
    print(f"\n📤 提交根任务 ID: {root_id}")

    print("🚀 启动 Worker...\n")

    await worker.run(auto_stop=True)

    # 查询任务树
    reader = StatsReader(db_path="test_callback_parent.db")
    tree_stats = reader.get_tree_stats(root_id)

    print(f"\n🌳 任务树统计:")
    print(f"   - 根任务 ID: {root_id}")
    print(f"   - 总任务数: {tree_stats['total']}")
    print(f"   - 已完成: {tree_stats['completed']}")
    print(f"   - 待处理: {tree_stats['pending']}")
    print(f"   - 说明: callback 自动关联了 parent_id，形成任务树")

    reader.close()
    await worker._manager.close()


async def main():
    print("\n🧪 Flowmix 异步 Callback 功能测试")
    print("测试版本: v0.5.3")

    # 运行所有测试
    await test_callback_in_execute()
    await test_callback_in_on_success()
    await test_callback_with_parent_tracking()

    print("\n" + "="*60)
    print("✅ 所有 Callback 测试完成！")
    print("="*60)

    # 清理测试数据库
    import os
    for db in ["test_callback_execute.db", "test_callback_success.db", "test_callback_parent.db"]:
        if os.path.exists(db):
            os.remove(db)
            print(f"🧹 清理: {db}")


if __name__ == "__main__":
    asyncio.run(main())
