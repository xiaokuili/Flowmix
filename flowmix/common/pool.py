"""
连接池管理 (Connection Pool)

提供单例的 Redis 和 SQLite 连接池
"""

import asyncio
import logging
from typing import Optional, Any
from contextlib import asynccontextmanager


logger = logging.getLogger(__name__)


class RedisPool:
    """
    Redis 连接池（单例）

    使用 redis.asyncio 的连接池，支持并发访问

    Example:
        # 获取单例
        pool = await RedisPool.get_instance(redis_url="redis://localhost:6379/0")

        # 使用连接
        async with pool.acquire() as conn:
            await conn.set("key", "value")

        # 关闭连接池
        await pool.close()
    """

    _instance: Optional['RedisPool'] = None
    _lock = asyncio.Lock()

    def __init__(self, redis_client: Any):
        """
        初始化 Redis 连接池

        Args:
            redis_client: redis.asyncio 客户端实例（已包含连接池）
        """
        self._client = redis_client
        self._closed = False
        logger.info("RedisPool initialized")

    @classmethod
    async def get_instance(
        cls,
        redis_url: str = "redis://localhost:6379/0",
        **kwargs
    ) -> 'RedisPool':
        """
        获取 RedisPool 单例

        Args:
            redis_url: Redis 连接 URL
            **kwargs: 其他传递给 redis.asyncio.from_url 的参数

        Returns:
            RedisPool 单例实例
        """
        if cls._instance is None:
            async with cls._lock:
                if cls._instance is None:
                    try:
                        import redis.asyncio as aioredis
                    except ImportError:
                        raise ImportError(
                            "redis is required. "
                            "Install it with: pip install 'flowmix[redis]'"
                        )

                    # 创建 Redis 客户端（内置连接池）
                    client = await aioredis.from_url(
                        redis_url,
                        decode_responses=True,
                        **kwargs
                    )

                    cls._instance = cls(client)
                    logger.info(f"RedisPool singleton created: {redis_url}")

        return cls._instance

    @asynccontextmanager
    async def acquire(self):
        """
        获取 Redis 连接（上下文管理器）

        redis.asyncio 的客户端本身就是线程安全的，可以直接使用
        这里提供统一的接口

        Example:
            async with pool.acquire() as conn:
                await conn.set("key", "value")
        """
        if self._closed:
            raise RuntimeError("RedisPool is closed")

        # redis.asyncio 客户端可以直接使用，内部已经有连接池
        yield self._client

    def get_client(self) -> Any:
        """
        直接获取 Redis 客户端（不推荐，建议使用 acquire）

        Returns:
            Redis 客户端实例
        """
        if self._closed:
            raise RuntimeError("RedisPool is closed")
        return self._client

    async def close(self):
        """关闭连接池"""
        if not self._closed:
            await self._client.close()
            self._closed = True
            logger.info("RedisPool closed")

    @classmethod
    async def reset(cls):
        """重置单例（主要用于测试）"""
        if cls._instance is not None:
            await cls._instance.close()
            cls._instance = None


class SQLitePool:
    """
    SQLite 连接池（单例）

    使用 asyncio.Queue 实现简单的连接池

    Example:
        # 获取单例
        pool = await SQLitePool.get_instance(
            db_path=".flowmix/flowmix.db",
            pool_size=5
        )

        # 使用连接
        async with pool.acquire() as conn:
            cursor = await conn.execute("SELECT * FROM tasks")
            rows = await cursor.fetchall()

        # 关闭连接池
        await pool.close()
    """

    _instance: Optional['SQLitePool'] = None
    _lock = asyncio.Lock()

    def __init__(self, db_path: str, pool_size: int = 5):
        """
        初始化 SQLite 连接池

        Args:
            db_path: SQLite 数据库文件路径
            pool_size: 连接池大小
        """
        self.db_path = db_path
        self.pool_size = pool_size
        self._pool: asyncio.Queue = asyncio.Queue(maxsize=pool_size)
        self._all_connections = []
        self._closed = False
        self._initialized = False
        logger.info(f"SQLitePool initialized: db_path={db_path}, pool_size={pool_size}")

    async def _init_pool(self):
        """初始化连接池（延迟初始化）"""
        if self._initialized:
            return

        try:
            import aiosqlite
        except ImportError:
            raise ImportError(
                "aiosqlite is required. "
                "Install it with: pip install aiosqlite"
            )

        for _ in range(self.pool_size):
            conn = await aiosqlite.connect(
                self.db_path,
                timeout=30.0
            )
            conn.row_factory = aiosqlite.Row
            # 启用 WAL 模式
            await conn.execute("PRAGMA journal_mode=WAL")

            self._all_connections.append(conn)
            await self._pool.put(conn)

        self._initialized = True
        logger.info(f"SQLitePool created {self.pool_size} connections")

    @classmethod
    async def get_instance(
        cls,
        db_path: str = ".flowmix/flowmix.db",
        pool_size: int = 5
    ) -> 'SQLitePool':
        """
        获取 SQLitePool 单例

        Args:
            db_path: SQLite 数据库文件路径
            pool_size: 连接池大小

        Returns:
            SQLitePool 单例实例
        """
        if cls._instance is None:
            async with cls._lock:
                if cls._instance is None:
                    cls._instance = cls(db_path, pool_size)
                    await cls._instance._init_pool()
                    logger.info(f"SQLitePool singleton created: {db_path}")

        return cls._instance

    @asynccontextmanager
    async def acquire(self):
        """
        获取 SQLite 连接（上下文管理器）

        Example:
            async with pool.acquire() as conn:
                cursor = await conn.execute("SELECT * FROM tasks")
                rows = await cursor.fetchall()
        """
        if self._closed:
            raise RuntimeError("SQLitePool is closed")

        if not self._initialized:
            await self._init_pool()

        # 从池中获取连接
        conn = await self._pool.get()
        try:
            yield conn
        finally:
            # 归还连接到池中
            await self._pool.put(conn)

    async def close(self):
        """关闭连接池"""
        if not self._closed:
            # 关闭所有连接
            for conn in self._all_connections:
                await conn.close()

            self._all_connections.clear()
            self._closed = True
            logger.info("SQLitePool closed")

    @classmethod
    async def reset(cls):
        """重置单例（主要用于测试）"""
        if cls._instance is not None:
            await cls._instance.close()
            cls._instance = None
