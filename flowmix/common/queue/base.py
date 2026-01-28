"""
队列抽象基类 (Queue Backend)

定义所有队列后端必须实现的接口
"""

from abc import ABC, abstractmethod
from typing import Optional, Dict, Any


class Queue(ABC):
    """
    队列抽象基类

    定义所有队列后端必须实现的接口，为 states 和 sender 模块提供能力

    职责：
    - 消息的入队、出队、确认
    - 优先级队列支持（priority）
    - 任务树支持（parent_id）
    - 任务去重支持（fingerprint）
    - 统计能力（pending count, stream length）

    子类需要实现：
    - Redis: 基于 RedisPool
    - SQLite: 基于 SQLitePool
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
        将任务放入队列

        Args:
            data: 任务数据（任意字典）
            priority: 优先级（默认 0，数字越大越优先）
            parent_id: 父任务 ID（用于构建任务树）
            task_name: 任务名称（用于 Worker 路由）

        Returns:
            消息 ID

        Example:
            msg_id = await queue.push(
                data={'url': 'https://example.com'},
                task_name='crawl',
                priority=10,
                parent_id=100
            )
        """
        pass

    @abstractmethod
    async def pop(self, consumer_name: str) -> Optional[Dict[str, Any]]:
        """
        从队列取出任务（按优先级降序）

        Args:
            consumer_name: 消费者名称

        Returns:
            任务字典（包含 id、task_name、data 等字段）
            如果没有任务返回 None

        Example:
            message = await queue.pop('worker-1')
            if message:
                print(message)  # {'id': 123, 'task_name': 'crawl', 'data': {...}}
        """
        pass

    @abstractmethod
    async def ack(
        self,
        message_id: int,
        failed: bool = False,
        error: Optional[str] = None,
        result: Optional[Any] = None,
        fingerprint: Optional[str] = None
    ):
        """
        确认任务已处理

        Args:
            message_id: 消息 ID
            failed: 是否失败
            error: 失败原因（可选）
            result: 执行结果（可选）
            fingerprint: 任务指纹（用于去重，可选）

        Example:
            # 成功
            await queue.ack(
                message_id=123,
                failed=False,
                result={'status': 'ok'},
                fingerprint='abc123'
            )

            # 失败
            await queue.ack(
                message_id=123,
                failed=True,
                error='Connection timeout'
            )
        """
        pass


    @abstractmethod
    async def clear_all(self):
        """
        清空所有消息（危险操作，仅用于测试）
        """
        pass

    @abstractmethod
    async def close(self):
        """
        关闭连接（释放资源）
        """
        pass
