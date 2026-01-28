"""
Cache - 缓存模块

提供任务去重和结果缓存
"""

from .base import Cache
from .redis import RedisCache
from .sqlite import SQLiteCache

__all__ = [
    'Cache',
    'RedisCache',
    'SQLiteCache',
]
