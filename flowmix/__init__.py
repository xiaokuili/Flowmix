"""
Flowmix - 简洁灵活的任务队列框架

核心组件：
- Task: 任务对象（数据 + 执行策略）
- TaskQueue: 队列管理器（存取任务，基于 SQLite）
- Worker: 任务执行器（处理任务）

Example:
    from flowmix import Task, Worker

    # 1. 定义 Task
    task = Task()

    @task.execute
    def process(data):
        url = data['url']
        return fetch(url)

    # 2. 创建 Worker（内置队列管理）
    worker = Worker(
        tasks=task,
        num_workers=5
    )

    # 3. 发布任务
    await worker.push({"url": "http://example.com"})

    # 4. 启动
    await worker.run()
"""

from .task import Task
from .worker import Worker
from .limiter import ConcurrencyLimiter
from .scheduler import Scheduler

# 队列相关（新架构）
from .queue import TaskQueue, Cache, Stats

# 向后兼容（旧 API）
Manager = TaskQueue  # Manager 重命名为 TaskQueue
StatsReader = Stats  # StatsReader 重命名为 Stats

# 可选：导出 queue 包（用于自定义后端）
try:
    from . import queue
except ImportError:
    queue = None

__all__ = [
    # 核心 API
    "Task",
    "Worker",
    "TaskQueue",
    "Stats",
    "Scheduler",
    "ConcurrencyLimiter",

    # 队列相关
    "Cache",
    "queue",

    # 向后兼容
    "Manager",
    "StatsReader",
]

__version__ = "0.5.3"
