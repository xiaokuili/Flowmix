"""
ConcurrencyLimiter - 并发限流器

基于滑动窗口算法，控制任务的并发执行数量
"""

import asyncio
import time
from collections import deque
from typing import Dict, Optional


class ConcurrencyLimiter:
    """
    并发限流器（全局单例）

    功能：
    - 基于任务名称（task_name）控制每秒最大并发数
    - 使用滑动窗口算法：过去 1 秒内最多 N 个并发
    - 异步阻塞：超限时自动等待，直到有空位

    实现原理：
    - 为每个 task_name 维护一个时间戳队列（执行开始时间）
    - acquire() 时检查队列中过去 1 秒的任务数，超限则等待
    - release() 时无需操作（依靠时间窗口自动清理）

    Example:
        limiter = ConcurrencyLimiter()

        async def worker():
            # 获取执行许可（阻塞等待）
            await limiter.acquire('api_call', limit=10)
            try:
                result = await call_api()
            finally:
                # 释放执行许可
                limiter.release('api_call')
    """

    _instance: Optional['ConcurrencyLimiter'] = None
    _lock = asyncio.Lock()

    def __new__(cls):
        """单例模式"""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        """初始化限流器"""
        if self._initialized:
            return

        # 记录每个任务的执行时间戳（滑动窗口）
        # key: task_name, value: deque of (start_timestamp, end_timestamp or None)
        self._timestamps: Dict[str, deque] = {}

        # 每个任务的锁（保证并发安全）
        self._task_locks: Dict[str, asyncio.Lock] = {}

        self._initialized = True

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
        if limit <= 0:
            return True  # 无限制

        # 获取任务锁
        if task_name not in self._task_locks:
            self._task_locks[task_name] = asyncio.Lock()

        task_lock = self._task_locks[task_name]

        start_time = time.time()

        while True:
            async with task_lock:
                # 初始化时间戳队列
                if task_name not in self._timestamps:
                    self._timestamps[task_name] = deque()

                timestamps = self._timestamps[task_name]
                current_time = time.time()

                # 清理过期的时间戳（超过 1 秒的）
                while timestamps and current_time - timestamps[0][0] > 1.0:
                    timestamps.popleft()

                # 统计当前并发数（包括正在执行的和已完成但在 1 秒窗口内的）
                active_count = sum(1 for start, end in timestamps if end is None or current_time - start <= 1.0)

                # 检查是否超限
                if active_count < limit:
                    # 记录开始时间（end_timestamp 为 None 表示正在执行）
                    timestamps.append((current_time, None))
                    return True

            # 检查超时
            if timeout is not None:
                elapsed = time.time() - start_time
                if elapsed >= timeout:
                    raise asyncio.TimeoutError(
                        f"Failed to acquire concurrency limit for '{task_name}' "
                        f"(limit={limit}) within {timeout}s"
                    )

            # 等待一小段时间后重试（避免忙等待）
            await asyncio.sleep(0.01)

    def release(self, task_name: str):
        """
        释放执行许可

        Args:
            task_name: 任务名称

        Note:
            调用此方法会标记任务执行结束，但时间戳会保留在队列中
            直到超出 1 秒滑动窗口才会被清理
        """
        if task_name not in self._timestamps:
            return

        timestamps = self._timestamps[task_name]
        current_time = time.time()

        # 找到最近的一个未结束的任务（end_timestamp 为 None），标记为结束
        for i in range(len(timestamps) - 1, -1, -1):
            start, end = timestamps[i]
            if end is None:
                timestamps[i] = (start, current_time)
                break

    def current_count(self, task_name: str) -> int:
        """
        获取当前并发数（用于监控）

        Args:
            task_name: 任务名称

        Returns:
            当前正在执行的任务数量
        """
        if task_name not in self._timestamps:
            return 0

        timestamps = self._timestamps[task_name]
        current_time = time.time()

        # 统计正在执行的任务数（end_timestamp 为 None）
        return sum(1 for start, end in timestamps if end is None)

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
        if task_name not in self._timestamps:
            return {'active': 0, 'window_total': 0}

        timestamps = self._timestamps[task_name]
        current_time = time.time()

        # 清理过期的时间戳
        while timestamps and current_time - timestamps[0][0] > 1.0:
            timestamps.popleft()

        active = sum(1 for start, end in timestamps if end is None)
        window_total = len(timestamps)

        return {
            'active': active,
            'window_total': window_total
        }

    def reset(self, task_name: Optional[str] = None):
        """
        重置限流器（主要用于测试）

        Args:
            task_name: 任务名称，None 表示重置所有任务
        """
        if task_name is None:
            self._timestamps.clear()
            self._task_locks.clear()
        else:
            if task_name in self._timestamps:
                del self._timestamps[task_name]
            if task_name in self._task_locks:
                del self._task_locks[task_name]
