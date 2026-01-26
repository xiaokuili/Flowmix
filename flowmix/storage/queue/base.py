"""
队列提供者基类（Queue Backend）

定义所有队列后端必须实现的接口
"""

from abc import ABC, abstractmethod
from typing import Optional, Dict, Any


class QueueBackend(ABC):
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
