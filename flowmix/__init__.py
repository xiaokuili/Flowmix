"""
Flowmix - 简洁灵活的任务队列框架

核心组件：
- Task: 任务对象（数据 + 执行策略）
- TaskQueue: 队列管理器（存取任务，支持 SQLite/Redis/PostgreSQL）
- TaskProducer: 任务提交器（提交任务到队列）
- TaskConsumer: 任务消费器（从队列拉取并执行任务）

Example:
    from flowmix import Task, TaskQueue, TaskProducer, TaskConsumer, ConsumerConfig, Cache

    # 1. 定义 Task
    task = Task(name='process')

    @task.execute
    async def process(data):
        url = data['url']
        return await fetch(url)

    # 2. 初始化队列和缓存
    queue = TaskQueue(db_path=".flowmix/flowmix.db")
    cache = Cache(db_path=".flowmix/flowmix.db")

    # 3. 提交任务（生产者）
    producer = TaskProducer(queue=queue)
    await producer.push(data={"url": "http://example.com"}, task_name="process")

    # 4. 执行任务（消费者）
    consumer = TaskConsumer(
        tasks={"process": task},
        queue=queue,
        cache=cache,
        config=ConsumerConfig(num_workers=5, max_retries=3)
    )
    await consumer.run()
"""

from .task import Task
from .producer import TaskProducer
from .consumer import TaskConsumer, ConsumerConfig
from .limiter import ConcurrencyLimiter
from .scheduler import Scheduler

# 队列相关
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
    "TaskProducer",
    "TaskConsumer",
    "ConsumerConfig",
    "TaskQueue",
    "Cache",
    "Stats",
    "Scheduler",
    "ConcurrencyLimiter",

    # 队列相关
    "queue",

    # 向后兼容
    "Manager",
    "StatsReader",
]

__version__ = "0.5.3"
