"""
任务去重示例

演示：提交重复任务时，只执行一次，其余命中缓存
"""
import asyncio
from flowmix import Task, TaskQueue, Pub, TaskRunner, RunnerConfig, Cache

# 创建支持去重的任务
task = Task(name='fetch', dedup=True)

execute_count = 0

@task.execute
async def fetch(data):
    global execute_count
    execute_count += 1
    url = data['url']
    print(f"  ✓ 实际执行: {url} (第 {execute_count} 次)")
    await asyncio.sleep(0.1)  # 模拟网络请求
    return {"url": url, "content": f"Content of {url}"}

@task.on_success
async def on_success(data, result):
    print(f"  → 任务完成: {data['url']}, 返回 {len(str(result))} 字节")

async def main():
    # 初始化队列和缓存
    queue = TaskQueue(db_path=".flowmix/flowmix.db", queue_name="dedup_test")
    cache = Cache(db_path=".flowmix/flowmix.db", queue_name="dedup_test")

    # 创建发布器和运行器
    pub = Pub(queue=queue)
    runner = TaskRunner(
        tasks={'fetch': task},
        queue=queue,
        cache=cache,
        config=RunnerConfig(num_workers=3)
    )

    print("📋 提交 10 个任务 (5 个唯一 URL，每个重复 2 次)")
    print("-" * 50)

    urls = [
        'http://example.com/page1',
        'http://example.com/page2',
        'http://example.com/page3',
        'http://example.com/page4',
        'http://example.com/page5',
    ]

    # 每个 URL 提交 2 次
    for url in urls * 2:
        await pub.push(data={'url': url}, task_name='fetch')

    await runner.run(auto_stop=True)

    print("\n" + "=" * 50)
    print(f"📊 效果统计:")
    print(f"  - 提交任务数: 10")
    print(f"  - 实际执行数: {execute_count}")
    print(f"  - 缓存命中数: {10 - execute_count}")
    print(f"  - 节省计算: {(10 - execute_count) / 10 * 100:.1f}%")

if __name__ == "__main__":
    asyncio.run(main())
