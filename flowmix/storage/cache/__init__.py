"""
Cache - 缓存模块（去重）

支持多种后端实现：
- SQLiteCache: 基于 SQLite 的本地缓存（默认）
- RedisCache: 基于 Redis 的分布式缓存
"""

from .base import CacheBackend
from .sqlite import SQLiteCache
from .redis import RedisCache

# 向后兼容：CacheProvider 作为 CacheBackend 的别名
CacheProvider = CacheBackend

# 向后兼容：Cache 作为 SQLiteCache 的别名
Cache = SQLiteCache

__all__ = [
    'CacheBackend',
    'CacheProvider',  # 向后兼容
    'SQLiteCache',
    'RedisCache',
    'Cache',  # 向后兼容
]
