"""
SQLiteQueue - 基于 SQLite 的任务队列

特点：
- 零外部依赖
- 单机部署
- 持久化存储
- 基于 SQLitePool 连接池
- 适合开发测试、小规模任务
"""

import asyncio
import json
import logging
import time
from typing import Optional, Dict, Any

from ..pool import SQLitePool
from .base import Queue


logger = logging.getLogger(__name__)


class SQLiteQueue(Queue):
    """
    SQLite 队列实现

    基于 SQLitePool 连接池，提供持久化的本地任务队列

    数据表结构：
    - id: 消息 ID（自增主键）
    - parent_id: 父任务 ID
    - task_name: 任务名称
    - data: 任务数据（JSON）
    - priority: 优先级
    - status: 状态（pending/processing/completed/failed/done）
    - consumer: 消费者名称
    - error: 错误信息
    - result: 执行结果（JSON）
    - fingerprint: 任务指纹
    - created_at: 创建时间
    - updated_at: 更新时间
    - completed_at: 完成时间

    Example:
        # 获取 SQLitePool 单例
        pool = await SQLitePool.get_instance('.flowmix/flowmix.db')

        # 创建队列
        queue = SQLiteQueue(pool=pool, queue_name='tasks')

        # 使用队列
        msg_id = await queue.push({'url': 'https://example.com'}, task_name='crawl')
        message = await queue.pop('worker-1')
        await queue.ack(message['id'], failed=False, result={'status': 'ok'})
    """

    def __init__(
        self,
        pool: SQLitePool,
        queue_name: str = "tasks",
        timeout: float = 1.0,
    ):
        """
        初始化 SQLiteQueue

        Args:
            pool: SQLitePool 实例
            queue_name: 队列名称（表名）
            timeout: pop() 等待超时时间（秒）
        """
        self._pool = pool
        self.queue_name = queue_name
        self.timeout = timeout
        self._initialized = False
        self._init_lock = asyncio.Lock()
        logger.info(f"SQLiteQueue initialized: queue_name={queue_name}")

    async def _init_db(self):
        """初始化数据库表"""
        async with self._init_lock:
            if self._initialized:
                return

            async with self._pool.acquire() as conn:
                # 创建任务表
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

                # 创建索引（优化查询性能）
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
            logger.debug(f"Database table '{self.queue_name}' initialized")

    async def push(
        self,
        data: Dict[str, Any],
        priority: int = 0,
        parent_id: Optional[int] = None,
        task_name: Optional[str] = None
    ) -> int:
        """将任务放入队列"""
        if not self._initialized:
            await self._init_db()

        async with self._pool.acquire() as conn:
            cursor = await conn.execute(
                f"INSERT INTO {self.queue_name} (data, priority, parent_id, task_name) VALUES (?, ?, ?, ?)",
                (json.dumps(data), priority, parent_id, task_name)
            )
            await conn.commit()

            msg_id = cursor.lastrowid
            logger.debug(f"Pushed message {msg_id} (task_name={task_name}, priority={priority})")
            return msg_id

    async def pop(self, consumer_name: str) -> Optional[Dict[str, Any]]:
        """从队列取出任务"""
        if not self._initialized:
            await self._init_db()

        start_time = time.time()

        while True:
            try:
                async with self._pool.acquire() as conn:
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
                    result = {
                        "id": msg_id,
                        "task_name": task_name,
                        "data": data
                    }
                    logger.debug(f"Popped message {msg_id} (task_name={task_name})")
                    return result
                else:
                    # 检查超时
                    if time.time() - start_time >= self.timeout:
                        return None
                    await asyncio.sleep(0.1)

            except Exception as e:
                if "locked" in str(e).lower():
                    await asyncio.sleep(0.05)
                    if time.time() - start_time >= self.timeout:
                        logger.error(f"Timeout waiting for database lock: {e}")
                        return None
                else:
                    logger.error(f"Database error: {e}")
                    raise

    async def ack(
        self,
        message_id: int,
        failed: bool = False,
        error: Optional[str] = None,
        result: Optional[Any] = None,
        fingerprint: Optional[str] = None
    ):
        """确认任务已处理"""
        if not self._initialized:
            await self._init_db()

        async with self._pool.acquire() as conn:
            status = 'failed' if failed else 'completed'
            result_json = json.dumps(result) if result is not None else None

            # 获取当前任务的 parent_id
            cursor = await conn.execute(
                f"SELECT parent_id FROM {self.queue_name} WHERE id = ?",
                (message_id,)
            )
            row = await cursor.fetchone()
            parent_id = row['parent_id'] if row else None

            # 更新任务状态
            if not failed and fingerprint:
                await conn.execute(
                    f"""UPDATE {self.queue_name}
                        SET status = ?, error = ?, result = ?, fingerprint = ?,
                            completed_at = julianday('now'), updated_at = julianday('now')
                        WHERE id = ?""",
                    (status, error, result_json, fingerprint, message_id)
                )
            else:
                await conn.execute(
                    f"""UPDATE {self.queue_name}
                        SET status = ?, error = ?, result = ?,
                            completed_at = julianday('now'), updated_at = julianday('now')
                        WHERE id = ?""",
                    (status, error, result_json, message_id)
                )

            await conn.commit()
            logger.debug(f"ACKed message {message_id} as {status}")

            # 如果有父任务，检查是否需要将父任务状态更新为 'done'
            if parent_id:
                await self._update_parent_status_if_done(conn, parent_id)

    async def _update_parent_status_if_done(self, conn, parent_id: int):
        """
        检查父任务是否应该更新为 'done' 状态

        当父任务本身是 'completed' 状态，且所有子任务都已完成时，将父任务状态更新为 'done'
        """
        # 检查父任务当前状态
        cursor = await conn.execute(
            f"SELECT status, parent_id FROM {self.queue_name} WHERE id = ?",
            (parent_id,)
        )
        row = await cursor.fetchone()
        if not row or row['status'] != 'completed':
            return  # 父任务不存在或状态不是 completed，无需更新

        # 检查父任务的所有子任务是否都已完成
        cursor = await conn.execute(
            f"""SELECT COUNT(*) as total,
                       SUM(CASE WHEN status IN ('completed', 'failed', 'done') THEN 1 ELSE 0 END) as finished
                FROM {self.queue_name}
                WHERE parent_id = ?""",
            (parent_id,)
        )
        row = await cursor.fetchone()
        total = row['total']
        finished = row['finished'] or 0

        # 如果所有子任务都已完成，更新父任务状态为 'done'
        if total > 0 and total == finished:
            await conn.execute(
                f"UPDATE {self.queue_name} SET status = 'done', updated_at = julianday('now') WHERE id = ?",
                (parent_id,)
            )
            await conn.commit()
            logger.debug(f"Updated task {parent_id} status to 'done' (all children completed)")

            # 递归检查父任务的父任务
            cursor = await conn.execute(
                f"SELECT parent_id FROM {self.queue_name} WHERE id = ?",
                (parent_id,)
            )
            row = await cursor.fetchone()
            if row and row['parent_id']:
                await self._update_parent_status_if_done(conn, row['parent_id'])


    async def clear_all(self):
        """清空所有消息"""
        if not self._initialized:
            await self._init_db()

        async with self._pool.acquire() as conn:
            await conn.execute(f"DELETE FROM {self.queue_name}")
            await conn.commit()
            logger.warning("Cleared all messages from queue")

    async def close(self):
        """
        关闭队列（释放资源）

        注意：不会关闭 SQLitePool，连接池由单例管理
        """
        logger.info("SQLiteQueue closed")
