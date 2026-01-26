"""
Storage - 存储相关模块

包含任务队列、缓存、统计和后端实现
"""

from .task_queue import TaskQueue
from .stats import Stats

# 缓存模块
from .cache import (
    CacheBackend,
    CacheProvider,  # 向后兼容
    SQLiteCache,
    RedisCache,
    Cache,  # 向后兼容
)

# 队列模块
from .queue import (
    QueueBackend,
    QueueProvider,  # 向后兼容
    SQLiteQueue,
    RedisQueue,
    PostgreSQLQueue,
    # 向后兼容
    SQLiteProvider,
    RedisProvider,
    PostgreSQLProvider,
)

__all__ = [
    # 队列
    'TaskQueue',
    # 统计
    'Stats',
    # 缓存
    'CacheBackend',
    'CacheProvider',  # 向后兼容
    'SQLiteCache',
    'RedisCache',
    'Cache',  # 向后兼容
    # 队列
    'QueueBackend',
    'QueueProvider',  # 向后兼容
    'SQLiteQueue',
    'RedisQueue',
    'PostgreSQLQueue',
    # 向后兼容
    'SQLiteProvider',
    'RedisProvider',
    'PostgreSQLProvider',
]
