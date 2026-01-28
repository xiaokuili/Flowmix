"""
RateLimiter - 限流器基类

定义所有限流器必须实现的接口
支持基于任务名称的并发控制
"""

from abc import ABC, abstractmethod
from typing import Optional


class RateLimiter(ABC):
    """
    限流器抽象基类

    定义所有限流器后端必须实现的接口
    支持基于任务名称的并发控制和速率限制

    实现类:
    - MemoryRateLimiter: 基于内存的限流器（单机）
    - RedisRateLimiter: 基于 Redis 的限流器（分布式）
    """

    @abstractmethod
    async def acquire(
        self,
        task_name: str,
        limit: int,
        timeout: Optional[float] = None
    ) -> bool:
        """
        获取执行许可（阻塞等待直到有空位或超时）

        Args:
            task_name: 任务名称
            limit: 每秒最大并发数
            timeout: 超时时间（秒），None 表示无限等待

        Returns:
            True: 获取成功
            False: 超时失败

        Raises:
            asyncio.TimeoutError: 超时
        """
        pass

    @abstractmethod
    def release(self, task_name: str):
        """
        释放执行许可

        Args:
            task_name: 任务名称
        """
        pass

    @abstractmethod
    def current_count(self, task_name: str) -> int:
        """
        获取当前并发数（用于监控）

        Args:
            task_name: 任务名称

        Returns:
            当前正在执行的任务数量
        """
        pass

    @abstractmethod
    def get_stats(self, task_name: str) -> dict:
        """
        获取统计信息（用于调试和监控）

        Args:
            task_name: 任务名称

        Returns:
            统计字典，包含：
            - active: 当前正在执行的任务数
            - window_total: 1 秒滑动窗口内的总任务数
        """
        pass

    @abstractmethod
    def reset(self, task_name: Optional[str] = None):
        """
        重置限流器（主要用于测试）

        Args:
            task_name: 任务名称，None 表示重置所有任务
        """
        pass

    async def close(self):
        """
        关闭限流器，释放资源（可选实现）

        默认实现为空，基于内存的限流器无需关闭
        基于 Redis 的限流器可以覆盖此方法释放连接
        """
        pass
