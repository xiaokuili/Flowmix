"""
TaskQueue - 任务队列操作接口

支持多种后端实现（SQLite、Redis、PostgreSQL）
不依赖 Task、Worker 等任何业务概念
"""

import logging
from typing import Optional, Dict, Any, Union

from .queue import QueueBackend, SQLiteQueue

# 向后兼容
QueueProvider = QueueBackend
SQLiteProvider = SQLiteQueue


class TaskQueue:
    """
    任务队列操作接口

    职责：
    - 持久化消息队列
    - 存储消息（push）
    - 读取消息（pop）
    - 确认消息（ack）

    特点：
    - 支持多种后端（SQLite、Redis、PostgreSQL）
    - 完全独立，不依赖任何业务概念
    - 只处理字典数据 Dict[str, Any]
    - 持久化，进程重启数据不丢

    Example:
        # 使用默认 SQLite 后端
        queue = TaskQueue(
            db_path=".flowmix/flowmix.db",
            queue_name="my-queue"
        )

        # 使用 Redis 后端
        from flowmix.queue.providers import RedisProvider
        queue = TaskQueue(
            provider=RedisProvider(redis_url="redis://localhost:6379/0")
        )

        # 使用 PostgreSQL 后端
        from flowmix.queue.providers import PostgreSQLProvider
        queue = TaskQueue(
            provider=PostgreSQLProvider(dsn="postgresql://user:pass@localhost/db")
        )

        # 发送消息
        msg_id = await queue.push({"url": "http://example.com"})

        # 接收消息
        msg = await queue.pop(consumer_name="worker-1")

        # 确认消息
        await queue.ack(msg["id"])
    """

    def __init__(
        self,
        provider: Optional[QueueProvider] = None,
        db_path: str = ".flowmix/flowmix.db",
        queue_name: str = "tasks",
        timeout: float = 1.0,
    ):
        """
        初始化 TaskQueue

        Args:
            provider: 队列提供者实例（可选）
                     - 如果不指定，默认使用 SQLiteProvider
                     - 可以传入 RedisProvider、PostgreSQLProvider 等
            db_path: SQLite 数据库文件路径（仅当 provider=None 时使用）
            queue_name: 队列名称（仅当 provider=None 时使用）
            timeout: pop() 等待超时时间（仅当 provider=None 时使用）
        """
        if provider is None:
            # 默认使用 SQLite 后端
            self._provider = SQLiteProvider(
                db_path=db_path,
                queue_name=queue_name,
                timeout=timeout
            )
        else:
            self._provider = provider

        self.logger = logging.getLogger(__name__)
        self.logger.info(f"TaskQueue initialized with provider: {self._provider.__class__.__name__}")

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
                     - 用于实现 DFS（深度优先）：新任务设置高优先级
                     - 用于实现 BFS（广度优先）：新任务设置低优先级
            parent_id: 父任务 ID（用于构建任务树，默认 None 表示根任务）
            task_name: 任务名称（可选，用于记录任务类型）

        Returns:
            消息 ID

        Example:
            # 根任务
            root_id = await queue.push({"url": "http://example.com"}, task_name="crawl")

            # 子任务
            child_id = await queue.push(
                {"url": "http://example.com/page1"},
                parent_id=root_id,
                task_name="crawl"
            )

            # 高优先级任务（DFS）
            msg_id = await queue.push(
                {"url": "http://example.com"},
                priority=10,
                task_name="parse"
            )
        """
        return await self._provider.push(data, priority, parent_id, task_name)

    async def pop(self, consumer_name: str) -> Optional[Dict[str, Any]]:
        """
        从队列取出消息

        Args:
            consumer_name: 消费者名称

        Returns:
            消息字典，包含 "id" 和原始数据字段
            如果没有消息返回 None

        Example:
            msg = await queue.pop("worker-1")
            if msg:
                msg_id = msg["id"]  # 用于 ack
                url = msg["url"]    # 业务数据
        """
        return await self._provider.pop(consumer_name)

    async def ack(
        self,
        message_id: int,
        failed: bool = False,
        error: str = None,
        result: Any = None,
        fingerprint: Optional[str] = None
    ):
        """
        确认消息已处理（标记为 completed/failed）

        Args:
            message_id: 消息 ID（从 pop() 返回的 "id" 字段）
            failed: 是否失败（默认 False）
            error: 失败原因（可选，仅在 failed=True 时有效）
            result: 执行结果（可选，成功时保存）
            fingerprint: 任务指纹（可选，用于去重缓存，只在成功时保存）

        Example:
            msg = await queue.pop("worker-1")
            # ... 处理消息 ...

            # 成功
            await queue.ack(msg["id"], result={"status": "ok"}, fingerprint="abc123...")

            # 失败（不保存 fingerprint）
            await queue.ack(msg["id"], failed=True, error="Connection timeout")
        """
        await self._provider.ack(message_id, failed, error, result, fingerprint)

    async def get_pending_count(self) -> int:
        """
        获取待处理消息数量

        Returns:
            待处理的消息数量
        """
        return await self._provider.get_pending_count()

    async def get_stream_length(self) -> int:
        """
        获取队列总长度

        Returns:
            队列中的消息总数（包括 pending 和 processing）
        """
        return await self._provider.get_stream_length()

    async def clear_all(self):
        """
        清空所有消息（谨慎使用）

        警告：此操作不可逆
        """
        await self._provider.clear_all()

    async def close(self):
        """关闭连接"""
        await self._provider.close()
