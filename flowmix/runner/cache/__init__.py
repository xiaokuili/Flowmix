"""
Cache - 缓存模块

提供任务去重和结果缓存
"""

from .base import Cache
from .memory import MemoryCache
from .redis import RedisCache

__all__ = [
    'Cache',
    'MemoryCache',
    'RedisCache',
]
