"""
Flowmix 异步示例

演示：
1. 使用 async/await 定义异步 Task
2. 发布任务
3. 启动 Worker 执行异步任务
"""

import asyncio
from flowmix import Task, Manager, Worker


# ==========================================
# 1. 定义异步 Task
# ==========================================

task = Task()


@task.execute
async def process_url(data: dict):
    """异步处理 URL"""
    url = data['url']
    print(f"Processing: {url}")

    # 模拟异步 I/O 操作（如网络请求）
    await asyncio.sleep(0.5)

    return {"url": url, "status": "ok"}


@task.on_success
async def save_result(data: dict, result):
    """异步保存结果"""
    print(f"✅ Success: {result}")

    # 模拟异步数据库操作
    await asyncio.sleep(0.1)


@task.on_failure
async def handle_error(data: dict, error: Exception):
    """异步处理错误"""
    print(f"❌ Failed: {data['url']} - {error}")

    # 模拟异步日志记录
    await asyncio.sleep(0.1)


# ==========================================
# 2. 发布任务
# ==========================================

def publish():
    """发布任务到队列"""
    print("\n📤 Publishing tasks...")

    manager = Manager(db_path="flowmix.db")

    # 发布 5 个任务
    urls = [
        "http://example.com/1",
        "http://example.com/2",
        "http://example.com/3",
        "http://example.com/4",
        "http://example.com/5",
    ]

    for url in urls:
        manager.push({"url": url})
        print(f"  Published: {url}")

    print(f"✅ Published {len(urls)} tasks\n")
    manager.close()


# ==========================================
# 3. 启动 Worker
# ==========================================

def start_worker():
    """启动 Worker 消费任务"""
    print("\n🚀 Starting async worker...\n")

    manager = Manager(db_path="flowmix.db")

    worker = Worker(
        tasks=task,
        manager=manager,
        num_workers=3,      # 3 个并发
        max_retries=2,      # 失败后重试 2 次
        retry_delay=3,      # 重试间隔 3 秒
    )

    try:
        worker.run()
    except KeyboardInterrupt:
        print("\n⛔ Stopped by user")
    finally:
        stats = worker.get_stats()
        print(f"\n📊 Stats: {stats}")
        manager.close()


# ==========================================
# 主函数
# ==========================================

if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage:")
        print("  python async_example.py publish   # 发布任务")
        print("  python async_example.py worker    # 启动 Worker")
        sys.exit(1)

    command = sys.argv[1]

    if command == "publish":
        publish()
    elif command == "worker":
        start_worker()
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
