"""
Storage - 存储相关模块

包含任务队列、缓存、统计和后端实现
"""

from .task_queue import TaskQueue
from .task_stats import TaskStats

# 缓存模块
from .cache import (
    CacheBackend,
    SQLiteCache,
    RedisCache,
    Cache,  # 向后兼容
)

# 队列模块
from .queue import (
    QueueBackend,
    SQLiteQueue,
    RedisQueue,
    PostgreSQLQueue,
)

# 统计模块
from .stats import (
    Stats,
    SQLiteStats,
    TaskInfo,
    TaskTreeStats,
    WorkerStats,
    WorkerInfo,
    FailedTask,
    ProcessingTask,
)

__all__ = [
    # 队列
    'TaskQueue',
    # 统计
    'TaskStats',
    # 缓存
    'CacheBackend',
    'SQLiteCache',
    'RedisCache',
    'Cache',  # 向后兼容
    # 队列后端
    'QueueBackend',
    'SQLiteQueue',
    'RedisQueue',
    'PostgreSQLQueue',
    # 统计后端
    'Stats',
    'SQLiteStats',
    'TaskInfo',
    'TaskTreeStats',
    'WorkerStats',
    'WorkerInfo',
    'FailedTask',
    'ProcessingTask',
]
