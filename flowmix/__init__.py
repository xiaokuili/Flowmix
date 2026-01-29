"""
Flowmix - 简洁灵活的任务队列框架

核心组件：
- Task: 任务对象（数据 + 执行策略）
- TaskQueue: 队列管理器（存取任务，支持 SQLite/Redis/PostgreSQL）
- Pub: 任务发布器（提交任务到队列）
- TaskRunner: 任务运行器（从队列拉取并执行任务）

Example:
    from flowmix import Task, TaskQueue, Pub, TaskRunner, RunnerConfig, Cache

    # 1. 定义 Task
    task = Task(name='process')

    @task.execute
    async def process(data):
        url = data['url']
        return await fetch(url)

    # 2. 初始化队列和缓存
    queue = TaskQueue(db_path=".flowmix/flowmix.db")
    cache = Cache(db_path=".flowmix/flowmix.db")

    # 3. 提交任务（发布器）
    pub = Pub(queue=queue)
    await pub.push(data={"url": "http://example.com"}, task_name="process")

    # 4. 执行任务（运行器）
    runner = TaskRunner(
        tasks=[task],
        queue=queue,
        cache=cache,
        config=RunnerConfig(num_workers=5, max_retries=3)
    )
    await runner.run()
"""

from .runner.task import Task
from .sender.pub import Pub
from .runner.runner import TaskRunner, RunnerConfig

# 限流器（兼容旧 API）
from .runner.limit import (
    RateLimiter,
    MemoryRateLimiter,
    RedisRateLimiter,
    ConcurrencyLimiter  # 别名，向后兼容
)

# 统计模块
try:
    from .stats import TaskStats
except ImportError:
    TaskStats = None

__all__ = [
    # 核心 API
    "Task",
    "Pub",
    "TaskRunner",
    "RunnerConfig",

    # 限流器
    "RateLimiter",
    "MemoryRateLimiter",
    "RedisRateLimiter",
    "ConcurrencyLimiter",  # 向后兼容

    # 统计
    "TaskStats",
]

__version__ = "0.5.3"
