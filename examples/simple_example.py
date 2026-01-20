"""
最简单的 Flowmix 示例

演示：
1. 定义 Task
2. 发布任务
3. 启动 Worker
"""

import time
from flowmix import Task, Manager, Worker


# ==========================================
# 1. 定义 Task
# ==========================================

task = Task()


@task.execute
def process_url(data: dict):
    """处理 URL"""
    url = data['url']
    print(f"Processing: {url}")

    # 模拟处理
    time.sleep(0.5)

    return {"url": url, "status": "ok"}


@task.on_success
def save_result(data: dict, result):
    """成功后保存"""
    print(f"✅ Success: {result}")
    


@task.on_failure
def handle_error(data: dict, error: Exception):
    """失败后处理"""
    print(f"❌ Failed: {data['url']} - {error}")


# ==========================================
# 2. 发布任务
# ==========================================

def publish():
    """发布任务到队列"""
    print("\n📤 Publishing tasks...")

    manager = Manager(db_path="flowmix.db")

    # 发布 3 个任务
    urls = [
        "http://example.com/1",
        "http://example.com/2",
        "http://example.com/3",
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
    print("\n🚀 Starting worker...\n")

    manager = Manager(db_path="flowmix.db")

    worker = Worker(
        task=task,
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
        print("  python simple_example.py publish   # 发布任务")
        print("  python simple_example.py worker    # 启动 Worker")
        sys.exit(1)

    command = sys.argv[1]

    if command == "publish":
        publish()
    elif command == "worker":
        start_worker()
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
