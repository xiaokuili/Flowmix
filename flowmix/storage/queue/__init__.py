"""
Queue - 队列模块

支持多种后端实现：
- SQLiteQueue: 基于 SQLite 的本地队列（默认）
- RedisQueue: 基于 Redis 的分布式队列
- PostgreSQLQueue: 基于 PostgreSQL 的持久化队列
"""

from .base import QueueBackend
from .sqlite import SQLiteQueue
from .redis import RedisQueue
from .postgresql import PostgreSQLQueue

# 向后兼容：QueueProvider 作为 QueueBackend 的别名
QueueProvider = QueueBackend

# 向后兼容：旧的 Provider 类名
SQLiteProvider = SQLiteQueue
RedisProvider = RedisQueue
PostgreSQLProvider = PostgreSQLQueue

__all__ = [
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
