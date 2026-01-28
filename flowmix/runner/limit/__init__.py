"""
Limit - 限流模块

提供并发限流控制
支持多种后端：Memory（内存）、Redis（分布式）
"""

from .base import RateLimiter
from .memory import MemoryRateLimiter
from .redis import RedisRateLimiter

# 兼容性：保留旧的类名
ConcurrencyLimiter = MemoryRateLimiter

__all__ = [
    'RateLimiter',
    'MemoryRateLimiter',
    'RedisRateLimiter',
    'ConcurrencyLimiter',  # 兼容性
]
