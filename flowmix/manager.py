"""
Manager - 消息队列管理器

基于 SQLite 的持久化消息队列
不依赖 Task、Worker 等任何业务概念
"""

import asyncio
import json
import logging
import os
import time
from typing import Optional, Dict, Any

import aiosqlite


class Manager:
    """
    消息队列管理器（基于 SQLite）

    职责：
    - 持久化消息队列
    - 存储消息（push）
    - 读取消息（pop）
    - 确认消息（ack）

    特点：
    - 完全独立，不依赖任何业务概念
    - 只处理字典数据 Dict[str, Any]
    - 基于 SQLite，零外部依赖
    - 支持并发（使用行锁）
    - 持久化，进程重启数据不丢

    Example:
        manager = Manager(
            db_path=".flowmix/flowmix.db",
            queue_name="my-queue"
        )

        # 发送消息
        msg_id = manager.push({"url": "http://example.com"})

        # 接收消息
        msg = manager.pop(consumer_name="worker-1")
        # msg = {
        #     "id": 123,
        #     "url": "http://example.com"
        # }

        # 确认消息
        manager.ack(msg["id"])
    """

    def __init__(
        self,
        db_path: str = ".flowmix/flowmix.db",
        queue_name: str = "tasks",
        timeout: float = 1.0,
    ):
        """
        初始化 Manager

        Args:
            db_path: SQLite 数据库文件路径（默认: .flowmix/flowmix.db）
            queue_name: 队列名称（表名）
            timeout: pop() 等待超时时间（秒，默认 1.0 秒以便快速响应停止信号）
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
        self.logger.info(f"Manager initialized with db_path={db_path}, queue_name={queue_name}")

    async def _get_connection(self) -> aiosqlite.Connection:
        """获取数据库连接"""
        if self._db is None:
            self._db = await aiosqlite.connect(
                self.db_path,
                timeout=30.0  # 锁超时
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

            # 创建索引加速查询（优先级降序，ID 升序）
            await conn.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_{self.queue_name}_status
                ON {self.queue_name}(status, priority DESC, id ASC)
            """)
            # 创建索引加速 parent_id 查询
            await conn.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_{self.queue_name}_parent
                ON {self.queue_name}(parent_id)
            """)
            # 创建索引加速 fingerprint 查询（用于去重）
            await conn.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_{self.queue_name}_fingerprint
                ON {self.queue_name}(fingerprint, status, completed_at DESC)
            """)
            await conn.commit()
            self._initialized = True

    async def push(self, data: Dict[str, Any], priority: int = 0, parent_id: Optional[int] = None, task_name: Optional[str] = None) -> int:
        """
        将消息放入队列

        Args:
            data: 消息数据（任意字典）
            priority: 优先级（默认 0，数字越大越优先）
                     - 用于实现 DFS（深度优先）：新任务设置高优先级
                     - 用于实现 BFS（广度优先）：新任务设置低优先级
            parent_id: 父任务 ID（用于构建任务树，默认 None 表示根任务）
            task_name: 任务名称（可选，用于记录任务类型）

        Returns:
            消息 ID

        Example:
            # 根任务
            root_id = manager.push({"url": "http://example.com"}, task_name="crawl")

            # 子任务
            child_id = manager.push({"url": "http://example.com/page1"}, parent_id=root_id, task_name="crawl")

            # 高优先级任务（DFS）
            msg_id = await manager.push({"url": "http://example.com"}, priority=10, task_name="parse")
        """
        if not self._initialized:
            await self._init_db()

        conn = await self._get_connection()
        cursor = await conn.execute(
            f"INSERT INTO {self.queue_name} (data, priority, parent_id, task_name) VALUES (?, ?, ?, ?)",
            (json.dumps(data), priority, parent_id, task_name)
        )
        await conn.commit()

        msg_id = cursor.lastrowid
        self.logger.debug(f"Pushed message {msg_id} to queue (task_name={task_name}, priority={priority}, parent_id={parent_id})")
        return msg_id

    async def pop(self, consumer_name: str) -> Optional[Dict[str, Any]]:
        """
        从队列取出消息

        Args:
            consumer_name: 消费者名称

        Returns:
            消息字典，包含 "id" 和原始数据字段
            如果没有消息返回 None

        Example:
            msg = await manager.pop("worker-1")
            if msg:
                msg_id = msg["id"]  # 用于 ack
                url = msg["url"]    # 业务数据
        """
        if not self._initialized:
            await self._init_db()

        conn = await self._get_connection()
        start_time = time.time()

        while True:
            # 使用 UPDATE ... RETURNING 实现原子性获取（避免多 worker 重复消费）
            try:
                # 原子操作：只有一个 worker 能成功 UPDATE 并获取数据
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

                    # 解析数据
                    data = json.loads(data_json)
                    result = {"id": msg_id, "task_name": task_name, **data}

                    self.logger.debug(f"Popped message {msg_id} from queue (task_name={task_name})")
                    return result

                else:
                    # 检查超时
                    if time.time() - start_time >= self.timeout:
                        return None

                    # 没有消息，等待一小段时间再重试
                    await asyncio.sleep(0.1)

            except aiosqlite.OperationalError as e:
                if "locked" in str(e).lower():
                    # 数据库被锁，短暂等待后重试
                    await asyncio.sleep(0.05)
                    if time.time() - start_time >= self.timeout:
                        self.logger.error(f"Timeout waiting for database lock: {e}")
                        return None
                else:
                    self.logger.error(f"Database error: {e}")
                    return None

    async def ack(self, message_id: int, failed: bool = False, error: str = None, result: Any = None, fingerprint: Optional[str] = None):
        """
        确认消息已处理（标记为 completed/failed，不再删除以保留任务树结构）

        Args:
            message_id: 消息 ID（从 pop() 返回的 "id" 字段）
            failed: 是否失败（默认 False）
            error: 失败原因（可选，仅在 failed=True 时有效）
            result: 执行结果（可选，成功时保存）
            fingerprint: 任务指纹（可选，用于去重缓存，只在成功时保存）

        Example:
            msg = await manager.pop("worker-1")
            # ... 处理消息 ...

            # 成功
            await manager.ack(msg["id"], result={"status": "ok"}, fingerprint="abc123...")

            # 失败（不保存 fingerprint）
            await manager.ack(msg["id"], failed=True, error="Connection timeout")
        """
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
        """
        获取待处理消息数量

        Returns:
            待处理的消息数量
        """
        if not self._initialized:
            await self._init_db()

        conn = await self._get_connection()
        cursor = await conn.execute(
            f"SELECT COUNT(*) FROM {self.queue_name} WHERE status = 'pending'"
        )
        row = await cursor.fetchone()
        return row[0]

    async def get_stream_length(self) -> int:
        """
        获取队列总长度

        Returns:
            队列中的消息总数（包括 pending 和 processing）
        """
        if not self._initialized:
            await self._init_db()

        conn = await self._get_connection()
        cursor = await conn.execute(f"SELECT COUNT(*) FROM {self.queue_name}")
        row = await cursor.fetchone()
        return row[0]

    async def clear_all(self):
        """
        清空所有消息（谨慎使用）

        警告：此操作不可逆
        """
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
