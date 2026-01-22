"""
Flowmix - 简洁灵活的任务队列框架

核心组件：
- Task: 任务对象（数据 + 执行策略）
- Manager: 队列管理器（存取任务，基于 SQLite）
- Worker: 任务执行器（处理任务）

Example:
    from flowmix import Task, Manager, Worker

    # 1. 创建 Manager（基于 SQLite，零依赖）
    manager = Manager(db_path="flowmix.db")

    # 2. 定义 Task
    task = Task()

    @task.execute
    def process(data):
        url = data['url']
        return fetch(url)

    # 3. 发布任务
    manager.push({"url": "http://example.com"})

    # 4. 创建 Worker
    worker = Worker(
        task=task,
        manager=manager,
        num_workers=5
    )

    # 5. 启动
    worker.run()
"""

from .task import Task
from .manager import Manager
from .worker import Worker
from .limiter import ConcurrencyLimiter
from .stats_reader import StatsReader

__all__ = [
    "Task",
    "Manager",
    "Worker",
    "ConcurrencyLimiter",
    "StatsReader",
]

__version__ = "0.2.0"
