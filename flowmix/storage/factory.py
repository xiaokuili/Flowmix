"""
Storage Factory - 存储组件工厂函数

提供便捷的工厂函数，用于创建共享连接的存储组件集合

设计原则：
- 连接由外部管理（factory 或用户）
- 组件只负责业务逻辑，不创建连接
- 完全解耦，支持依赖注入
"""

from typing import Any


class RedisStorage:
    """
    Redis 存储组件集合（共享同一个 Redis 连接）

    包含 cache、queue、stats 三个组件，所有组件共享同一个 Redis 连接池

    Example:
        from flowmix.storage import create_redis_storage

        # 创建共享连接的存储组件
        storage = await create_redis_storage(
            redis_url="redis://localhost:6379/0",
            queue_name="tasks"
        )

        # 使用各个组件
        cache = storage.cache
        queue = storage.queue
        stats = storage.stats

        # 用完后关闭
        await storage.close()
    """

    def __init__(self, redis: Any, queue_name: str):
        """
        初始化 RedisStorage

        Args:
            redis: Redis 连接实例
            queue_name: 队列名称
        """
        from .cache import RedisCache
        from .queue import RedisQueue
        from .stats import RedisStats

        self.redis = redis
        self.queue_name = queue_name

        # 创建共享连接的组件
        self.cache = RedisCache(redis=redis, queue_name=queue_name)
        self.queue = RedisQueue(redis=redis, queue_name=queue_name)
        self.stats = RedisStats(redis=redis, queue_name=queue_name)

    async def close(self):
        """
        关闭所有组件和 Redis 连接

        注意：会关闭底层的 Redis 连接，所有组件将无法继续使用
        """
        # 关闭各个组件（它们不会关闭共享的连接）
        await self.cache.close()
        await self.queue.close()
        await self.stats.close()

        # 关闭底层 Redis 连接
        if self.redis is not None:
            await self.redis.close()


async def create_redis_storage(
    redis_url: str = "redis://localhost:6379/0",
    queue_name: str = "tasks"
) -> RedisStorage:
    """
    创建共享 Redis 连接的存储组件集合

    这个工厂函数会创建一个 Redis 连接池，并将其共享给 cache、queue、stats 三个组件。
    相比分别创建三个组件，这种方式可以节省 Redis 连接资源。

    Args:
        redis_url: Redis 连接 URL（格式: redis://host:port/db）
        queue_name: 队列名称（Redis key 前缀）

    Returns:
        RedisStorage 实例，包含 cache、queue、stats 三个组件

    Example:
        from flowmix.storage import create_redis_storage

        # 创建共享连接的存储组件
        storage = await create_redis_storage(
            redis_url="redis://localhost:6379/0",
            queue_name="tasks"
        )

        # 使用各个组件
        result = await storage.cache.check("crawl", {"url": "http://example.com"})
        msg_id = await storage.queue.push({"url": "http://example.com"})
        worker_stats = await storage.stats.get_worker_stats()

        # 用完后关闭
        await storage.close()

    注意：
        - 使用 create_redis_storage 创建的组件共享同一个连接
        - 调用 storage.close() 会关闭底层连接，之后所有组件都无法使用
        - 如果需要独立管理各组件的生命周期，请使用 create_redis_connection + 组件构造函数
    """
    try:
        import redis.asyncio as aioredis
    except ImportError:
        raise ImportError(
            "redis is required for RedisStorage. "
            "Install it with: pip install 'flowmix[redis]'"
        )

    # 创建 Redis 连接池
    redis = await aioredis.from_url(redis_url, decode_responses=True)

    # 创建存储组件集合
    return RedisStorage(redis, queue_name)


async def create_redis_connection(redis_url: str = "redis://localhost:6379/0") -> Any:
    """
    创建 Redis 连接（用于自定义场景）

    这个函数用于需要自己管理连接和组件的场景

    Args:
        redis_url: Redis 连接 URL（格式: redis://host:port/db）

    Returns:
        Redis 连接实例

    Example:
        from flowmix.storage import create_redis_connection
        from flowmix.storage.queue import RedisQueue
        from flowmix.storage.cache import RedisCache

        # 创建连接
        redis = await create_redis_connection("redis://localhost:6379/0")

        # 创建组件
        queue = RedisQueue(redis=redis, queue_name="tasks")
        cache = RedisCache(redis=redis, queue_name="tasks")

        # 使用组件
        msg_id = await queue.push({"url": "http://example.com"})
        result = await cache.check("crawl", {"url": "http://example.com"})

        # 关闭组件
        await queue.close()
        await cache.close()

        # 关闭连接
        await redis.close()
    """
    try:
        import redis.asyncio as aioredis
    except ImportError:
        raise ImportError(
            "redis is required. "
            "Install it with: pip install 'flowmix[redis]'"
        )

    return await aioredis.from_url(redis_url, decode_responses=True)
