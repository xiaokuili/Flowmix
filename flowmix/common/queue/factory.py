"""
Queue Factory - 从 URL 创建队列实例

职责：
- 统一的队列创建逻辑
- 支持多种 URL scheme (redis://, rediss://, memory://)
"""

from typing import Optional
from urllib.parse import urlparse

from .base import Queue
from .redis import RedisQueue
from .memory import MemoryQueue


async def create_queue_from_url(url: str, queue_name: str = "tasks") -> Queue:
    """
    从 URL 创建队列实例

    Args:
        url: 队列 URL
            - redis://localhost:6379/0
            - rediss://localhost:6379/0 (SSL/TLS)
            - memory://
        queue_name: 队列名称

    Returns:
        Queue 实例

    Raises:
        ValueError: 不支持的 URL scheme

    Example:
        # Redis 队列
        queue = await create_queue_from_url("redis://localhost:6379/0", "tasks")

        # 内存队列
        queue = await create_queue_from_url("memory://", "tasks")
    """
    parsed = urlparse(url)
    scheme = parsed.scheme

    if scheme in ("redis", "rediss"):
        from ..pool import RedisPool
        pool = await RedisPool.get_instance(url)
        return RedisQueue(pool=pool, queue_name=queue_name)

    elif scheme == "memory":
        return MemoryQueue(queue_name=queue_name)

    else:
        raise ValueError(
            f"Unsupported URL scheme: {scheme}. "
            f"Only 'redis://', 'rediss://', and 'memory://' are supported."
        )
