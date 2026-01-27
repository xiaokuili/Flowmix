"""
Stats - 统计查询模块

支持多种后端实现：
- SQLiteStats: 基于 SQLite 的统计查询（默认）
- RedisStats: 基于 Redis 的统计查询（未来）
- PostgreSQLStats: 基于 PostgreSQL 的统计查询（未来）
"""

from .base import (
    Stats,
    TaskInfo,
    TaskTreeStats,
    WorkerStats,
    WorkerInfo,
    FailedTask,
    ProcessingTask,
)
from .sqlite import SQLiteStats

__all__ = [
    # 抽象基类
    'Stats',
    # TypedDict
    'TaskInfo',
    'TaskTreeStats',
    'WorkerStats',
    'WorkerInfo',
    'FailedTask',
    'ProcessingTask',
    # 实现
    'SQLiteStats',
]
