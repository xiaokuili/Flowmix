"""
Manager - 消息队列管理器

基于 SQLite 的持久化消息队列
不依赖 Task、Worker 等任何业务概念
"""

import json
import logging
import os
import sqlite3
import threading
import time
from typing import Optional, Dict, Any


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
        timeout: float = 5.0,
    ):
        """
        初始化 Manager

        Args:
            db_path: SQLite 数据库文件路径（默认: .flowmix/flowmix.db）
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

        # 每个线程使用独立的连接
        self._local = threading.local()

        # 初始化数据库
        self._init_db()

        self.logger = logging.getLogger(__name__)
        self.logger.info(f"Manager initialized with db_path={db_path}, queue_name={queue_name}")

    def _get_connection(self) -> sqlite3.Connection:
        """获取当前线程的数据库连接"""
        if not hasattr(self._local, 'conn'):
            self._local.conn = sqlite3.connect(
                self.db_path,
                check_same_thread=False,
                timeout=30.0  # 锁超时
            )
            self._local.conn.row_factory = sqlite3.Row
            # 启用 WAL 模式，提高并发性能
            self._local.conn.execute("PRAGMA journal_mode=WAL")
        return self._local.conn

    def _init_db(self):
        """初始化数据库表"""
        conn = self._get_connection()
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.queue_name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                parent_id INTEGER,
                data TEXT NOT NULL,
                priority INTEGER DEFAULT 0,
                status TEXT DEFAULT 'pending',
                consumer TEXT,
                error TEXT,
                created_at REAL DEFAULT (julianday('now')),
                updated_at REAL DEFAULT (julianday('now')),
                completed_at REAL
            )
        """)
        # 创建索引加速查询（优先级降序，ID 升序）
        conn.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_{self.queue_name}_status
            ON {self.queue_name}(status, priority DESC, id ASC)
        """)
        # 创建索引加速 parent_id 查询
        conn.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_{self.queue_name}_parent
            ON {self.queue_name}(parent_id)
        """)
        conn.commit()

    def push(self, data: Dict[str, Any], priority: int = 0, parent_id: Optional[int] = None) -> int:
        """
        将消息放入队列

        Args:
            data: 消息数据（任意字典）
            priority: 优先级（默认 0，数字越大越优先）
                     - 用于实现 DFS（深度优先）：新任务设置高优先级
                     - 用于实现 BFS（广度优先）：新任务设置低优先级
            parent_id: 父任务 ID（用于构建任务树，默认 None 表示根任务）

        Returns:
            消息 ID

        Example:
            # 根任务
            root_id = manager.push({"url": "http://example.com"})

            # 子任务
            child_id = manager.push({"url": "http://example.com/page1"}, parent_id=root_id)

            # 高优先级任务（DFS）
            msg_id = manager.push({"url": "http://example.com"}, priority=10)
        """
        conn = self._get_connection()
        cursor = conn.execute(
            f"INSERT INTO {self.queue_name} (data, priority, parent_id) VALUES (?, ?, ?)",
            (json.dumps(data), priority, parent_id)
        )
        conn.commit()

        msg_id = cursor.lastrowid
        self.logger.debug(f"Pushed message {msg_id} to queue (priority={priority}, parent_id={parent_id})")
        return msg_id

    def pop(self, consumer_name: str) -> Optional[Dict[str, Any]]:
        """
        从队列取出消息

        Args:
            consumer_name: 消费者名称

        Returns:
            消息字典，包含 "id" 和原始数据字段
            如果没有消息返回 None

        Example:
            msg = manager.pop("worker-1")
            if msg:
                msg_id = msg["id"]  # 用于 ack
                url = msg["url"]    # 业务数据
        """
        conn = self._get_connection()
        start_time = time.time()

        while True:
            # 使用事务 + SELECT ... FOR UPDATE 实现原子性获取
            try:
                conn.execute("BEGIN IMMEDIATE")

                cursor = conn.execute(f"""
                    SELECT id, data FROM {self.queue_name}
                    WHERE status = 'pending'
                    ORDER BY priority DESC, id ASC
                    LIMIT 1
                """)

                row = cursor.fetchone()

                if row:
                    msg_id, data_json = row['id'], row['data']

                    # 标记为处理中
                    conn.execute(f"""
                        UPDATE {self.queue_name}
                        SET status = 'processing',
                            consumer = ?,
                            updated_at = julianday('now')
                        WHERE id = ?
                    """, (consumer_name, msg_id))

                    conn.commit()

                    # 解析数据
                    data = json.loads(data_json)
                    result = {"id": msg_id, **data}

                    self.logger.debug(f"Popped message {msg_id} from queue")
                    return result

                else:
                    conn.commit()

                    # 检查超时
                    if time.time() - start_time >= self.timeout:
                        return None

                    # 没有消息，等待一小段时间再重试
                    time.sleep(0.1)

            except sqlite3.OperationalError as e:
                conn.rollback()
                if "locked" in str(e).lower():
                    # 数据库被锁，短暂等待后重试
                    time.sleep(0.05)
                    if time.time() - start_time >= self.timeout:
                        self.logger.error(f"Timeout waiting for database lock: {e}")
                        return None
                else:
                    self.logger.error(f"Database error: {e}")
                    return None

    def ack(self, message_id: int, failed: bool = False, error: str = None):
        """
        确认消息已处理（标记为 completed/failed，不再删除以保留任务树结构）

        Args:
            message_id: 消息 ID（从 pop() 返回的 "id" 字段）
            failed: 是否失败（默认 False）
            error: 失败原因（可选，仅在 failed=True 时有效）

        Example:
            msg = manager.pop("worker-1")
            # ... 处理消息 ...

            # 成功
            manager.ack(msg["id"])

            # 失败
            manager.ack(msg["id"], failed=True, error="Connection timeout")
        """
        conn = self._get_connection()
        status = 'failed' if failed else 'completed'
        conn.execute(
            f"""UPDATE {self.queue_name}
                SET status = ?, error = ?, completed_at = julianday('now'), updated_at = julianday('now')
                WHERE id = ?""",
            (status, error, message_id)
        )
        conn.commit()
        self.logger.debug(f"ACKed message {message_id} as {status}")

    def get_pending_count(self) -> int:
        """
        获取待处理消息数量

        Returns:
            待处理的消息数量
        """
        conn = self._get_connection()
        cursor = conn.execute(
            f"SELECT COUNT(*) FROM {self.queue_name} WHERE status = 'pending'"
        )
        return cursor.fetchone()[0]

    def get_stream_length(self) -> int:
        """
        获取队列总长度

        Returns:
            队列中的消息总数（包括 pending 和 processing）
        """
        conn = self._get_connection()
        cursor = conn.execute(f"SELECT COUNT(*) FROM {self.queue_name}")
        return cursor.fetchone()[0]

    def clear_all(self):
        """
        清空所有消息（谨慎使用）

        警告：此操作不可逆
        """
        conn = self._get_connection()
        conn.execute(f"DELETE FROM {self.queue_name}")
        conn.commit()
        self.logger.warning("Cleared all messages from queue")

    def get_task_info(self, task_id: int) -> Optional[dict]:
        """
        获取任务详情

        Args:
            task_id: 任务 ID

        Returns:
            任务详情字典，如果不存在返回 None

        Example:
            info = manager.get_task_info(123)
            print(info)
            # {
            #     'id': 123,
            #     'parent_id': 100,
            #     'data': '{"url": "..."}',
            #     'status': 'completed',
            #     'error': None,
            #     ...
            # }
        """
        conn = self._get_connection()
        cursor = conn.execute(
            f"SELECT * FROM {self.queue_name} WHERE id = ?",
            (task_id,)
        )
        row = cursor.fetchone()
        if row:
            return dict(row)
        return None

    def get_children(self, parent_id: int) -> list:
        """
        获取所有直接子任务

        Args:
            parent_id: 父任务 ID

        Returns:
            子任务列表

        Example:
            children = manager.get_children(100)
            for child in children:
                print(f"子任务 {child['id']}: {child['status']}")
        """
        conn = self._get_connection()
        cursor = conn.execute(
            f"SELECT * FROM {self.queue_name} WHERE parent_id = ? ORDER BY id ASC",
            (parent_id,)
        )
        return [dict(row) for row in cursor.fetchall()]

    def get_tree_stats(self, root_id: int) -> dict:
        """
        获取任务树的统计信息（递归查询所有子孙任务）

        Args:
            root_id: 根任务 ID

        Returns:
            统计字典，包含 total, pending, processing, completed, failed

        Example:
            stats = manager.get_tree_stats(100)
            print(stats)
            # {
            #     'total': 150,
            #     'pending': 10,
            #     'processing': 5,
            #     'completed': 130,
            #     'failed': 5
            # }
        """
        conn = self._get_connection()

        # 使用 CTE 递归查询整棵树
        cursor = conn.execute(f"""
            WITH RECURSIVE tree AS (
                SELECT * FROM {self.queue_name} WHERE id = ?
                UNION ALL
                SELECT t.* FROM {self.queue_name} t
                INNER JOIN tree ON t.parent_id = tree.id
            )
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) as pending,
                SUM(CASE WHEN status = 'processing' THEN 1 ELSE 0 END) as processing,
                SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed,
                SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed
            FROM tree
        """, (root_id,))

        row = cursor.fetchone()
        return {
            "total": row["total"] or 0,
            "pending": row["pending"] or 0,
            "processing": row["processing"] or 0,
            "completed": row["completed"] or 0,
            "failed": row["failed"] or 0,
        }

    def close(self):
        """关闭数据库连接"""
        if hasattr(self._local, 'conn'):
            self._local.conn.close()
            delattr(self._local, 'conn')
        self.logger.info("Closed SQLite connection")
