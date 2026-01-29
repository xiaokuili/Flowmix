"""
多 Worker 并发示例 - Flowmix 多协程任务处理

展示功能：
1. 配置多个并发 worker
2. 不同 worker 并发处理任务
3. 展示任务分配和执行效果
"""

import asyncio
from datetime import datetime
from flowmix import Task, TaskRunner, RunnerConfig
from flowmix.common.queue import MemoryQueue
from flowmix.sender import Pub
import time 

# 1. 定义 Task
process_task = Task(name='process')


@process_task.execute
async def execute_process(data):
    """执行处理任务（模拟耗时操作）"""
    message = data.get('message', 'Task')
    duration = data.get('duration', 2)

    start_time = datetime.now()
    print(f"🔄 [Worker {message}] 开始处理: {message} (预计耗时 {duration}s) - {start_time.strftime('%H:%M:%S.%f')[:-3]}")

    # 模拟耗时操作
    await asyncio.sleep(duration)

    end_time = datetime.now()
    actual_duration = (end_time - start_time).total_seconds()
    print(f"✅ [Worker {message}] 完成: {message} (实际耗时 {actual_duration:.2f}s) - {end_time.strftime('%H:%M:%S.%f')[:-3]}")
    return {"status": "ok", "message": message, "duration": duration}


@process_task.on_success
async def on_success(data, result):
    """任务成功回调"""
    print(f"   ✓ 成功回调: {result['message']} (耗时 {result['duration']}s)")


@process_task.on_failure
async def on_failure(data, error):
    """任务失败回调"""
    print(f"   ✗ 失败回调: {error}")


async def main():
    """主函数"""
    # 创建内存队列
    queue_name = "tasks"
    queue = MemoryQueue(queue_name=queue_name)

    # 2. 推送任务
    print("\n" + "="*60)
    print("📤 推送任务到队列...")
    print("="*60)

    pub = Pub(queue=queue)

    # 推送多个任务（不同的耗时时间）
    tasks = [
        {"message": "任务 A", "duration": 1},
        {"message": "任务 B", "duration": 2},
        {"message": "任务 C", "duration": 3},
        {"message": "任务 D", "duration": 1},
        {"message": "任务 E", "duration": 2},
        {"message": "任务 F", "duration": 1},
        {"message": "任务 G", "duration": 2},
        {"message": "任务 H", "duration": 1},
        {"message": "任务 I", "duration": 3},
        {"message": "任务 J", "duration": 2},
    ]

    for i, task_data in enumerate(tasks):
        task_id = await pub.push(
            data=task_data,
            task_name="process"
        )
        print(f"  ✓ 推送任务 #{task_id}: {task_data['message']} (耗时 {task_data['duration']}s)")

    print(f"\n  总计推送 {len(tasks)} 个任务，串行需要处理24s")

    # 3. 执行任务（使用多个并发 worker）
    print("\n" + "="*60)
    print("🚀 启动 Runner（3 个并发 Worker）...")
    print("="*60 + "\n")

    # 创建 Runner
    runner = TaskRunner(
        tasks={"process": process_task},
        url="memory://",
        cache_url=None,  # 不使用 cache
        queue_name=queue_name,
        config=RunnerConfig(
            num_workers=3,      # 3个并发worker
            max_retries=1,      # 失败后重试1次
            retry_delay=0.5     # 重试延迟0.5秒
        )
    )

    # 启动 Runner（自动停止模式：队列为空后自动退出）
    await runner.run()

    print("\n" + "="*60)
    print("✅ 所有任务执行完成！")
    print("="*60)


if __name__ == "__main__":
    asyncio.run(main())
