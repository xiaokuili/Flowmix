"""
队列提供者基类（Queue Backend）

定义所有队列后端必须实现的接口
"""

from abc import ABC, abstractmethod
from typing import Optional, Dict, Any, TypedDict


class TaskInfo(TypedDict):
    """任务信息数据结构"""
    id: int
    parent_id: Optional[int]
    task_name: Optional[str]
    data: Optional[Dict[str, Any]]
    priority: int
    status: str  # 'pending' | 'processing' | 'completed' | 'failed'
    consumer: Optional[str]
    error: Optional[str]
    result: Any
    fingerprint: Optional[str]
    created_at: float  # Julian day number
    updated_at: float  # Julian day number
    completed_at: Optional[float]  # Julian day number


class TreeStats(TypedDict):
    """任务树统计信息数据结构"""
    total: int
    pending: int
    processing: int
    completed: int
    failed: int


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

    @abstractmethod
    async def get_task_info(self, task_id: int) -> TaskInfo:
        """
        查询任务信息

        Args:
            task_id: 任务 ID

        Returns:
            任务信息字典，包含以下字段：
            - id: 任务 ID
            - parent_id: 父任务 ID
            - task_name: 任务名称
            - data: 任务数据
            - priority: 优先级
            - status: 状态（pending/processing/completed/failed）
            - consumer: 消费者名称
            - error: 错误信息
            - result: 执行结果
            - fingerprint: 任务指纹
            - created_at: 创建时间（Julian day）
            - updated_at: 更新时间（Julian day）
            - completed_at: 完成时间（Julian day）
        """
        pass

    @abstractmethod
    async def get_tree_stats(self, root_id: int) -> TreeStats:
        """
        查询任务树统计信息

        Args:
            root_id: 根任务 ID

        Returns:
            统计字典，包含以下字段：
            - total: 总任务数
            - pending: 待处理任务数
            - processing: 处理中任务数
            - completed: 已完成任务数
            - failed: 失败任务数
        """
        pass
