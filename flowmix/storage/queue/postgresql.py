"""
PostgreSQLQueue - 基于 PostgreSQL 的任务队列

特点：
- 强大的查询能力
- 事务保证
- 持久化存储
- 适合需要复杂查询、强一致性的场景
"""

import asyncio
import json
import logging
import time
from typing import Optional, Dict, Any

from .base import QueueBackend


class PostgreSQLQueue(QueueBackend):
    """
    PostgreSQL 队列提供者

    特点：
    - 强大的查询能力
    - 事务保证
    - 持久化存储
    - 适合需要复杂查询、强一致性的场景

    Example:
        queue = PostgreSQLQueue(
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
        初始化 PostgreSQL Queue

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
                "asyncpg is required for PostgreSQLQueue. "
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
        self.logger.info(f"PostgreSQLQueue initialized: dsn={dsn}, queue_name={queue_name}")

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
            # 获取当前任务的 parent_id
            parent_id = await conn.fetchval(
                f"SELECT parent_id FROM {self.queue_name} WHERE id = $1",
                message_id
            )

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

            # 如果有父任务，检查是否需要将父任务状态更新为 'done'
            if parent_id:
                await self._update_parent_status_if_done(conn, parent_id)

        self.logger.debug(f"ACKed message {message_id} as {status}")

    async def _update_parent_status_if_done(self, conn, parent_id: int):
        """
        检查父任务是否应该更新为 'done' 状态

        当父任务本身是 'completed' 状态，且所有子任务都已完成时，将父任务状态更新为 'done'
        """
        # 检查父任务当前状态
        row = await conn.fetchrow(
            f"SELECT status, parent_id FROM {self.queue_name} WHERE id = $1",
            parent_id
        )
        if not row or row['status'] != 'completed':
            return  # 父任务不存在或状态不是 completed，无需更新

        # 检查父任务的所有子任务是否都已完成
        row = await conn.fetchrow(
            f"""SELECT COUNT(*) as total,
                       SUM(CASE WHEN status IN ('completed', 'failed', 'done') THEN 1 ELSE 0 END) as finished
                FROM {self.queue_name}
                WHERE parent_id = $1""",
            parent_id
        )
        total = row['total']
        finished = row['finished'] or 0

        # 如果所有子任务都已完成，更新父任务状态为 'done'
        if total > 0 and total == finished:
            await conn.execute(
                f"UPDATE {self.queue_name} SET status = 'done', updated_at = NOW() WHERE id = $1",
                parent_id
            )
            self.logger.debug(f"Updated task {parent_id} status to 'done' (all children completed)")

            # 递归检查父任务的父任务
            grandparent_id = await conn.fetchval(
                f"SELECT parent_id FROM {self.queue_name} WHERE id = $1",
                parent_id
            )
            if grandparent_id:
                await self._update_parent_status_if_done(conn, grandparent_id)

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
