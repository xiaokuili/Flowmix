"""
队列提供者（Queue Providers）

支持多种后端实现：
- SQLiteProvider: 基于 SQLite 的本地队列（默认）
- RedisProvider: 基于 Redis 的分布式队列
- PostgreSQLProvider: 基于 PostgreSQL 的持久化队列
"""

import asyncio
import json
import logging
import os
import time
from abc import ABC, abstractmethod
from typing import Optional, Dict, Any

import aiosqlite


class QueueProvider(ABC):
    """
    队列提供者抽象基类

    定义所有后端必须实现的接口
    """

    @abstractmethod
    async def push(
        self,
        data: Dict[str, Any],
        priority: int = 0,
        parent_id: Optional[int] = None,
        task_name: Optional[str] = None
    ) -> int:
        """
        将消息放入队列

        Args:
            data: 消息数据（任意字典）
            priority: 优先级（默认 0，数字越大越优先）
            parent_id: 父任务 ID（用于构建任务树）
            task_name: 任务名称

        Returns:
            消息 ID
        """
        pass

    @abstractmethod
    async def pop(self, consumer_name: str) -> Optional[Dict[str, Any]]:
        """
        从队列取出消息

        Args:
            consumer_name: 消费者名称

        Returns:
            消息字典（包含 id、task_name、data 等字段）
            如果没有消息返回 None
        """
        pass

    @abstractmethod
    async def ack(
        self,
        message_id: int,
        failed: bool = False,
        error: str = None,
        result: Any = None,
        fingerprint: Optional[str] = None
    ):
        """
        确认消息已处理

        Args:
            message_id: 消息 ID
            failed: 是否失败
            error: 失败原因
            result: 执行结果
            fingerprint: 任务指纹（用于去重）
        """
        pass

    @abstractmethod
    async def get_pending_count(self) -> int:
        """获取待处理消息数量"""
        pass

    @abstractmethod
    async def get_stream_length(self) -> int:
        """获取队列总长度"""
        pass

    @abstractmethod
    async def clear_all(self):
        """清空所有消息"""
        pass

    @abstractmethod
    async def close(self):
        """关闭连接"""
        pass


class SQLiteProvider(QueueProvider):
    """
    SQLite 队列提供者

    特点：
    - 零外部依赖
    - 单机部署
    - 持久化存储
    - 适合开发测试、小规模任务

    Example:
        provider = SQLiteProvider(
            db_path=".flowmix/flowmix.db",
            queue_name="tasks"
        )
    """

    def __init__(
        self,
        db_path: str = ".flowmix/flowmix.db",
        queue_name: str = "tasks",
        timeout: float = 1.0,
    ):
        """
        初始化 SQLite Provider

        Args:
            db_path: SQLite 数据库文件路径
            queue_name: 队列名称（表名）
            timeout: pop() 等待超时时间（秒）
        """
        # 确保数据库目录存在
        db_dir = os.path.dirname(db_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir, exist_ok=True)

        self.db_path = db_path
        self.queue_name = queue_name
        self.timeout = timeout

        # 数据库连接（异步）
        self._db: Optional[aiosqlite.Connection] = None
        self._initialized = False
        self._init_lock = asyncio.Lock()

        self.logger = logging.getLogger(__name__)
        self.logger.info(f"SQLiteProvider initialized: db_path={db_path}, queue_name={queue_name}")

    async def _get_connection(self) -> aiosqlite.Connection:
        """获取数据库连接"""
        if self._db is None:
            self._db = await aiosqlite.connect(
                self.db_path,
                timeout=30.0
            )
            self._db.row_factory = aiosqlite.Row
            # 启用 WAL 模式，提高并发性能
            await self._db.execute("PRAGMA journal_mode=WAL")
        return self._db

    async def _init_db(self):
        """初始化数据库表"""
        async with self._init_lock:
            if self._initialized:
                return

            conn = await self._get_connection()
            await conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.queue_name} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    parent_id INTEGER,
                    task_name TEXT,
                    data TEXT NOT NULL,
                    priority INTEGER DEFAULT 0,
                    status TEXT DEFAULT 'pending',
                    consumer TEXT,
                    error TEXT,
                    result TEXT,
                    fingerprint TEXT,
                    created_at REAL DEFAULT (julianday('now')),
                    updated_at REAL DEFAULT (julianday('now')),
                    completed_at REAL
                )
            """)

            # 创建索引
            await conn.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_{self.queue_name}_status
                ON {self.queue_name}(status, priority DESC, id ASC)
            """)
            await conn.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_{self.queue_name}_parent
                ON {self.queue_name}(parent_id)
            """)
            await conn.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_{self.queue_name}_fingerprint
                ON {self.queue_name}(fingerprint, status, completed_at DESC)
            """)
            await conn.commit()
            self._initialized = True

    async def push(
        self,
        data: Dict[str, Any],
        priority: int = 0,
        parent_id: Optional[int] = None,
        task_name: Optional[str] = None
    ) -> int:
        """将消息放入队列"""
        if not self._initialized:
            await self._init_db()

        conn = await self._get_connection()
        cursor = await conn.execute(
            f"INSERT INTO {self.queue_name} (data, priority, parent_id, task_name) VALUES (?, ?, ?, ?)",
            (json.dumps(data), priority, parent_id, task_name)
        )
        await conn.commit()

        msg_id = cursor.lastrowid
        self.logger.debug(f"Pushed message {msg_id} (task_name={task_name}, priority={priority})")
        return msg_id

    async def pop(self, consumer_name: str) -> Optional[Dict[str, Any]]:
        """从队列取出消息"""
        if not self._initialized:
            await self._init_db()

        conn = await self._get_connection()
        start_time = time.time()

        while True:
            try:
                # 原子操作：UPDATE ... RETURNING
                cursor = await conn.execute(f"""
                    UPDATE {self.queue_name}
                    SET status = 'processing',
                        consumer = ?,
                        updated_at = julianday('now')
                    WHERE id = (
                        SELECT id FROM {self.queue_name}
                        WHERE status = 'pending'
                        ORDER BY priority DESC, id ASC
                        LIMIT 1
                    )
                    RETURNING id, data, task_name
                """, (consumer_name,))

                row = await cursor.fetchone()
                await conn.commit()

                if row:
                    msg_id, data_json, task_name = row['id'], row['data'], row['task_name']
                    data = json.loads(data_json)
                    result = {"id": msg_id, "task_name": task_name, "data": data}
                    self.logger.debug(f"Popped message {msg_id} (task_name={task_name})")
                    return result
                else:
                    # 检查超时
                    if time.time() - start_time >= self.timeout:
                        return None
                    await asyncio.sleep(0.1)

            except aiosqlite.OperationalError as e:
                if "locked" in str(e).lower():
                    await asyncio.sleep(0.05)
                    if time.time() - start_time >= self.timeout:
                        self.logger.error(f"Timeout waiting for database lock: {e}")
                        return None
                else:
                    self.logger.error(f"Database error: {e}")
                    return None

    async def ack(
        self,
        message_id: int,
        failed: bool = False,
        error: str = None,
        result: Any = None,
        fingerprint: Optional[str] = None
    ):
        """确认消息已处理"""
        if not self._initialized:
            await self._init_db()

        conn = await self._get_connection()
        status = 'failed' if failed else 'completed'
        result_json = json.dumps(result) if result is not None else None

        # 只在成功时保存 fingerprint
        if not failed and fingerprint:
            await conn.execute(
                f"""UPDATE {self.queue_name}
                    SET status = ?, error = ?, result = ?, fingerprint = ?, completed_at = julianday('now'), updated_at = julianday('now')
                    WHERE id = ?""",
                (status, error, result_json, fingerprint, message_id)
            )
        else:
            await conn.execute(
                f"""UPDATE {self.queue_name}
                    SET status = ?, error = ?, result = ?, completed_at = julianday('now'), updated_at = julianday('now')
                    WHERE id = ?""",
                (status, error, result_json, message_id)
            )

        await conn.commit()
        self.logger.debug(f"ACKed message {message_id} as {status}")

    async def get_pending_count(self) -> int:
        """获取待处理消息数量"""
        if not self._initialized:
            await self._init_db()

        conn = await self._get_connection()
        cursor = await conn.execute(
            f"SELECT COUNT(*) FROM {self.queue_name} WHERE status = 'pending'"
        )
        row = await cursor.fetchone()
        return row[0]

    async def get_stream_length(self) -> int:
        """获取队列总长度"""
        if not self._initialized:
            await self._init_db()

        conn = await self._get_connection()
        cursor = await conn.execute(f"SELECT COUNT(*) FROM {self.queue_name}")
        row = await cursor.fetchone()
        return row[0]

    async def clear_all(self):
        """清空所有消息"""
        if not self._initialized:
            await self._init_db()

        conn = await self._get_connection()
        await conn.execute(f"DELETE FROM {self.queue_name}")
        await conn.commit()
        self.logger.warning("Cleared all messages from queue")

    async def close(self):
        """关闭数据库连接"""
        if self._db is not None:
            await self._db.close()
            self._db = None
        self.logger.info("Closed SQLite connection")


class RedisProvider(QueueProvider):
    """
    Redis 队列提供者

    特点：
    - 高性能内存队列
    - 支持分布式部署
    - 可选持久化（AOF/RDB）
    - 适合高并发、分布式场景

    Example:
        provider = RedisProvider(
            redis_url="redis://localhost:6379/0",
            queue_name="tasks"
        )
    """

    def __init__(
        self,
        redis_url: str = "redis://localhost:6379/0",
        queue_name: str = "tasks",
        timeout: float = 1.0,
    ):
        """
        初始化 Redis Provider

        Args:
            redis_url: Redis 连接 URL
            queue_name: 队列名称（Redis key 前缀）
            timeout: pop() 等待超时时间（秒）
        """
        try:
            import redis.asyncio as aioredis
            self._aioredis = aioredis
        except ImportError:
            raise ImportError(
                "redis is required for RedisProvider. "
                "Install it with: pip install 'flowmix[redis]'"
            )

        self.redis_url = redis_url
        self.queue_name = queue_name
        self.timeout = timeout

        # Redis 连接
        self._redis: Optional[Any] = None
        self._next_id = 0  # 消息 ID 计数器

        self.logger = logging.getLogger(__name__)
        self.logger.info(f"RedisProvider initialized: redis_url={redis_url}, queue_name={queue_name}")

    async def _get_connection(self):
        """获取 Redis 连接"""
        if self._redis is None:
            self._redis = await self._aioredis.from_url(
                self.redis_url,
                decode_responses=True
            )
        return self._redis

    def _get_key(self, suffix: str) -> str:
        """生成 Redis key"""
        return f"{self.queue_name}:{suffix}"

    async def push(
        self,
        data: Dict[str, Any],
        priority: int = 0,
        parent_id: Optional[int] = None,
        task_name: Optional[str] = None
    ) -> int:
        """将消息放入队列"""
        redis = await self._get_connection()

        # 生成消息 ID
        msg_id = await redis.incr(self._get_key("counter"))

        # 构建消息对象
        message = {
            "id": msg_id,
            "task_name": task_name,
            "parent_id": parent_id,
            "data": json.dumps(data),
            "priority": priority,
            "status": "pending",
            "created_at": time.time()
        }

        # 保存消息数据（使用 Hash）
        await redis.hset(
            self._get_key("messages"),
            msg_id,
            json.dumps(message)
        )

        # 添加到优先级队列（使用 Sorted Set，score 为 -priority 实现高优先级优先）
        await redis.zadd(
            self._get_key("pending"),
            {str(msg_id): -priority * 1e10 + msg_id}  # 优先级高的 score 小，相同优先级按 ID 升序
        )

        self.logger.debug(f"Pushed message {msg_id} (task_name={task_name}, priority={priority})")
        return msg_id

    async def pop(self, consumer_name: str) -> Optional[Dict[str, Any]]:
        """从队列取出消息"""
        redis = await self._get_connection()
        start_time = time.time()

        while True:
            # 从优先级队列取出最小 score 的消息（最高优先级）
            items = await redis.zpopmin(self._get_key("pending"), 1)

            if items:
                msg_id_str, score = items[0]
                msg_id = int(msg_id_str)

                # 获取消息数据
                msg_json = await redis.hget(self._get_key("messages"), msg_id)
                if not msg_json:
                    continue

                message = json.loads(msg_json)

                # 更新状态为 processing
                message["status"] = "processing"
                message["consumer"] = consumer_name
                message["updated_at"] = time.time()

                await redis.hset(
                    self._get_key("messages"),
                    msg_id,
                    json.dumps(message)
                )

                # 构建返回结果
                data = json.loads(message["data"])
                result = {
                    "id": msg_id,
                    "task_name": message.get("task_name"),
                    **data
                }

                self.logger.debug(f"Popped message {msg_id} (task_name={message.get('task_name')})")
                return result
            else:
                # 检查超时
                if time.time() - start_time >= self.timeout:
                    return None
                await asyncio.sleep(0.1)

    async def ack(
        self,
        message_id: int,
        failed: bool = False,
        error: str = None,
        result: Any = None,
        fingerprint: Optional[str] = None
    ):
        """确认消息已处理"""
        redis = await self._get_connection()

        # 获取消息
        msg_json = await redis.hget(self._get_key("messages"), message_id)
        if not msg_json:
            return

        message = json.loads(msg_json)

        # 更新状态
        message["status"] = "failed" if failed else "completed"
        message["error"] = error
        message["result"] = json.dumps(result) if result is not None else None
        message["fingerprint"] = fingerprint
        message["completed_at"] = time.time()
        message["updated_at"] = time.time()

        await redis.hset(
            self._get_key("messages"),
            message_id,
            json.dumps(message)
        )

        self.logger.debug(f"ACKed message {message_id} as {message['status']}")

    async def get_pending_count(self) -> int:
        """获取待处理消息数量"""
        redis = await self._get_connection()
        count = await redis.zcard(self._get_key("pending"))
        return count

    async def get_stream_length(self) -> int:
        """获取队列总长度"""
        redis = await self._get_connection()
        count = await redis.hlen(self._get_key("messages"))
        return count

    async def clear_all(self):
        """清空所有消息"""
        redis = await self._get_connection()
        await redis.delete(
            self._get_key("pending"),
            self._get_key("messages"),
            self._get_key("counter")
        )
        self.logger.warning("Cleared all messages from queue")

    async def close(self):
        """关闭 Redis 连接"""
        if self._redis is not None:
            await self._redis.close()
            self._redis = None
        self.logger.info("Closed Redis connection")


class PostgreSQLProvider(QueueProvider):
    """
    PostgreSQL 队列提供者

    特点：
    - 强大的查询能力
    - 事务保证
    - 持久化存储
    - 适合需要复杂查询、强一致性的场景

    Example:
        provider = PostgreSQLProvider(
            dsn="postgresql://user:password@localhost:5432/flowmix",
            queue_name="tasks"
        )
    """

    def __init__(
        self,
        dsn: str,
        queue_name: str = "tasks",
        timeout: float = 1.0,
    ):
        """
        初始化 PostgreSQL Provider

        Args:
            dsn: PostgreSQL 连接字符串
            queue_name: 队列名称（表名）
            timeout: pop() 等待超时时间（秒）
        """
        try:
            import asyncpg
            self._asyncpg = asyncpg
        except ImportError:
            raise ImportError(
                "asyncpg is required for PostgreSQLProvider. "
                "Install it with: pip install 'flowmix[postgresql]'"
            )

        self.dsn = dsn
        self.queue_name = queue_name
        self.timeout = timeout

        # PostgreSQL 连接池
        self._pool: Optional[Any] = None
        self._initialized = False
        self._init_lock = asyncio.Lock()

        self.logger = logging.getLogger(__name__)
        self.logger.info(f"PostgreSQLProvider initialized: dsn={dsn}, queue_name={queue_name}")

    async def _get_pool(self):
        """获取连接池"""
        if self._pool is None:
            self._pool = await self._asyncpg.create_pool(self.dsn)
        return self._pool

    async def _init_db(self):
        """初始化数据库表"""
        async with self._init_lock:
            if self._initialized:
                return

            pool = await self._get_pool()
            async with pool.acquire() as conn:
                await conn.execute(f"""
                    CREATE TABLE IF NOT EXISTS {self.queue_name} (
                        id SERIAL PRIMARY KEY,
                        parent_id INTEGER,
                        task_name TEXT,
                        data JSONB NOT NULL,
                        priority INTEGER DEFAULT 0,
                        status TEXT DEFAULT 'pending',
                        consumer TEXT,
                        error TEXT,
                        result JSONB,
                        fingerprint TEXT,
                        created_at TIMESTAMP DEFAULT NOW(),
                        updated_at TIMESTAMP DEFAULT NOW(),
                        completed_at TIMESTAMP
                    )
                """)

                # 创建索引
                await conn.execute(f"""
                    CREATE INDEX IF NOT EXISTS idx_{self.queue_name}_status
                    ON {self.queue_name}(status, priority DESC, id ASC)
                """)
                await conn.execute(f"""
                    CREATE INDEX IF NOT EXISTS idx_{self.queue_name}_parent
                    ON {self.queue_name}(parent_id)
                """)
                await conn.execute(f"""
                    CREATE INDEX IF NOT EXISTS idx_{self.queue_name}_fingerprint
                    ON {self.queue_name}(fingerprint, status, completed_at DESC)
                """)

            self._initialized = True

    async def push(
        self,
        data: Dict[str, Any],
        priority: int = 0,
        parent_id: Optional[int] = None,
        task_name: Optional[str] = None
    ) -> int:
        """将消息放入队列"""
        if not self._initialized:
            await self._init_db()

        pool = await self._get_pool()
        async with pool.acquire() as conn:
            msg_id = await conn.fetchval(
                f"INSERT INTO {self.queue_name} (data, priority, parent_id, task_name) "
                f"VALUES ($1, $2, $3, $4) RETURNING id",
                json.dumps(data), priority, parent_id, task_name
            )

        self.logger.debug(f"Pushed message {msg_id} (task_name={task_name}, priority={priority})")
        return msg_id

    async def pop(self, consumer_name: str) -> Optional[Dict[str, Any]]:
        """从队列取出消息"""
        if not self._initialized:
            await self._init_db()

        pool = await self._get_pool()
        start_time = time.time()

        while True:
            async with pool.acquire() as conn:
                # 使用 FOR UPDATE SKIP LOCKED 实现原子操作
                row = await conn.fetchrow(f"""
                    UPDATE {self.queue_name}
                    SET status = 'processing',
                        consumer = $1,
                        updated_at = NOW()
                    WHERE id = (
                        SELECT id FROM {self.queue_name}
                        WHERE status = 'pending'
                        ORDER BY priority DESC, id ASC
                        LIMIT 1
                        FOR UPDATE SKIP LOCKED
                    )
                    RETURNING id, data, task_name
                """, consumer_name)

                if row:
                    msg_id = row['id']
                    data = json.loads(row['data']) if isinstance(row['data'], str) else row['data']
                    task_name = row['task_name']

                    result = {"id": msg_id, "task_name": task_name, **data}
                    self.logger.debug(f"Popped message {msg_id} (task_name={task_name})")
                    return result
                else:
                    # 检查超时
                    if time.time() - start_time >= self.timeout:
                        return None
                    await asyncio.sleep(0.1)

    async def ack(
        self,
        message_id: int,
        failed: bool = False,
        error: str = None,
        result: Any = None,
        fingerprint: Optional[str] = None
    ):
        """确认消息已处理"""
        if not self._initialized:
            await self._init_db()

        pool = await self._get_pool()
        status = 'failed' if failed else 'completed'
        result_json = json.dumps(result) if result is not None else None

        async with pool.acquire() as conn:
            if not failed and fingerprint:
                await conn.execute(
                    f"""UPDATE {self.queue_name}
                        SET status = $1, error = $2, result = $3, fingerprint = $4,
                            completed_at = NOW(), updated_at = NOW()
                        WHERE id = $5""",
                    status, error, result_json, fingerprint, message_id
                )
            else:
                await conn.execute(
                    f"""UPDATE {self.queue_name}
                        SET status = $1, error = $2, result = $3,
                            completed_at = NOW(), updated_at = NOW()
                        WHERE id = $4""",
                    status, error, result_json, message_id
                )

        self.logger.debug(f"ACKed message {message_id} as {status}")

    async def get_pending_count(self) -> int:
        """获取待处理消息数量"""
        if not self._initialized:
            await self._init_db()

        pool = await self._get_pool()
        async with pool.acquire() as conn:
            count = await conn.fetchval(
                f"SELECT COUNT(*) FROM {self.queue_name} WHERE status = 'pending'"
            )
        return count

    async def get_stream_length(self) -> int:
        """获取队列总长度"""
        if not self._initialized:
            await self._init_db()

        pool = await self._get_pool()
        async with pool.acquire() as conn:
            count = await conn.fetchval(f"SELECT COUNT(*) FROM {self.queue_name}")
        return count

    async def clear_all(self):
        """清空所有消息"""
        if not self._initialized:
            await self._init_db()

        pool = await self._get_pool()
        async with pool.acquire() as conn:
            await conn.execute(f"DELETE FROM {self.queue_name}")

        self.logger.warning("Cleared all messages from queue")

    async def close(self):
        """关闭连接池"""
        if self._pool is not None:
            await self._pool.close()
            self._pool = None
        self.logger.info("Closed PostgreSQL connection pool")
