"""
Queue - 队列模块

基于连接池的任务队列实现，为 states 和 sender 模块提供能力

支持的队列后端：
- RedisQueue: 基于 Redis 的高性能分布式队列
- SQLiteQueue: 基于 SQLite 的本地持久化队列
"""

from .base import Queue
from .redis import RedisQueue
from .memory import MemoryQueue

__all__ = [
    "Queue",
    "RedisQueue",
    "MemoryQueue",
]
