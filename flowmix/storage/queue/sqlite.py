"""
SQLiteQueue - 基于 SQLite 的任务队列

特点：
- 零外部依赖
- 单机部署
- 持久化存储
- 适合开发测试、小规模任务
"""

import asyncio
import json
import logging
import os
import time
from typing import Optional, Dict, Any

import aiosqlite

from .base import QueueBackend


class SQLiteQueue(QueueBackend):
    """
    SQLite 队列提供者

    特点：
    - 零外部依赖
    - 单机部署
    - 持久化存储
    - 适合开发测试、小规模任务

    Example:
        queue = SQLiteQueue(
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
        初始化 SQLite Queue

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
        self._operation_lock = asyncio.Lock()  # 保护并发数据库操作

        self.logger = logging.getLogger(__name__)
        self.logger.info(f"SQLiteQueue initialized: db_path={db_path}, queue_name={queue_name}")

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

        async with self._operation_lock:
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
                async with self._operation_lock:
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

        async with self._operation_lock:
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
        """获取待处理消息数量（包括 pending 和 processing）"""
        if not self._initialized:
            await self._init_db()

        conn = await self._get_connection()
        cursor = await conn.execute(
            f"SELECT COUNT(*) FROM {self.queue_name} WHERE status IN ('pending', 'processing')"
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
