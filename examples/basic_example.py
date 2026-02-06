"""
基础示例 - Flowmix 快速入门

展示核心功能：
1. 创建 Task 任务
2. 推送任务到队列
3. 创建 Runner 执行任务
"""

import asyncio
from flowmix import Task, TaskRunner, RunnerConfig
from flowmix.sender import Pub


# 1. 定义 Task
print_task = Task(name='print')


@print_task.execute
async def execute_print(data):
    """执行打印任务"""
    message = data.get('message', 'Hello')
    print(f"📝 Processing: {message}")
    return {"status": "ok", "message": message}


@print_task.on_success
async def on_success(data, result, msg_id):
    """任务成功回调"""
    print(f"✅ Success: {result['message']}, msg_id: {msg_id}")


@print_task.on_failure
async def on_failure(data, error):
    """任务失败回调"""
    print(f"❌ Failed: {error}")


async def main():
    """主函数"""
    queue_name = "tasks"

    # 2. 推送任务
    print("\n" + "="*50)
    print("📤 推送任务到队列...")
    print("="*50)

    # 创建发布器（自动创建队列）
    pub = await Pub.create(url="memory://", queue_name=queue_name)

    # 推送多个任务
    messages = [
        "Hello, Flowmix!",
        "任务 1",
        "任务 2",
        "任务 3",
    ]

    for msg in messages:
        task_id = await pub.push(
            data={"message": msg},
            task_name="print"
        )
        print(f"  ✓ 推送任务 #{task_id}: {msg}")

    # 3. 执行任务
    print("\n" + "="*50)
    print("🚀 启动 Runner 执行任务...")
    print("="*50 + "\n")

    # 创建 Runner
    runner = TaskRunner(
        tasks={"print": print_task},
        url="memory://",  # 内存队列
        queue_name=queue_name,
        config=RunnerConfig(
            num_workers=1,      # 1个并发worker
            max_retries=2,      # 失败后重试2次
            retry_delay=1.0     # 重试延迟1秒
        )
    )

    await runner.run()

    print("\n" + "="*50)
    print("✅ 所有任务执行完成！")
    print("="*50)


if __name__ == "__main__":
    asyncio.run(main())
