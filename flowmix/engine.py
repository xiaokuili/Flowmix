"""
TaskEngine - 任务执行引擎

职责：
- 缓存检查
- 限流控制
- 重试逻辑
- 执行任务
"""

import asyncio
import logging
from typing import Dict, Any, Tuple, Optional

from .task import Task
from .queue.cache import Cache
from .limiter import ConcurrencyLimiter
from .queue.task_queue import TaskQueue


class TaskEngine:
    """
    任务执行引擎

    职责：
    - 缓存检查（如果 Task 启用了 dedup）
    - 限流控制（如果 Task 设置了 concurrency_limit）
    - 重试逻辑（根据配置的 max_retries 和 retry_delay）
    - 执行任务

    这是一个内部类，由 TaskConsumer 使用
    """

    def __init__(
        self,
        cache: Cache,
        limiter: ConcurrencyLimiter,
        queue: TaskQueue,
        max_retries: int = 0,
        retry_delay: float = 0
    ):
        """
        初始化 TaskEngine

        Args:
            cache: 缓存实例
            limiter: 限流器实例
            queue: 队列实例
            max_retries: 最大重试次数
            retry_delay: 重试延迟（秒）
        """
        self._cache = cache
        self._limiter = limiter
        self._queue = queue
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.logger = logging.getLogger(__name__)

    async def execute(
        self,
        msg: Dict[str, Any],
        task: Task,
        worker_name: str
    ) -> Tuple[Dict[str, Any], str]:
        """
        执行任务

        Args:
            msg: 消息对象（包含 id, task_name, data）
            task: Task 实例
            worker_name: Worker 名称（用于日志）

        Returns:
            (result_dict, status)
            - status: "success" | "failed" | "cached"
            - result_dict: {"data": ..., "fingerprint": ..., "error": ...}
        """
        task_name = msg.get("task_name")
        msg_id = msg["id"]

        # 提取任务数据
        if "data" in msg:
            task_data = msg["data"]
        else:
            # 去掉 "id" 和 "task_name" 字段，剩下的都是业务数据
            task_data = {k: v for k, v in msg.items() if k not in ("id", "task_name")}

        # 1. 缓存检查
        if task.dedup:
            cached = await self._check_cache(task_name, task_data, task.dedup_ttl)
            if cached is not None:
                fingerprint_preview = self._cache.generate_fingerprint(task_name, task_data)[:8]
                self.logger.info(
                    f"[{worker_name}] Task '{task_name}' {msg_id} "
                    f"hit cache (fingerprint={fingerprint_preview}...), skipping execution"
                )
                return {"data": cached, "fingerprint": None}, "cached"

        # 2. 执行任务（带重试）
        for attempt in range(self.max_retries + 1):
            try:
                if attempt > 0:
                    self.logger.info(
                        f"[{worker_name}] Processing task '{task_name}': {msg_id} "
                        f"(retry {attempt}/{self.max_retries})"
                    )
                else:
                    self.logger.info(f"[{worker_name}] Processing task '{task_name}': {msg_id}")

                # 限流控制
                if task.concurrency_limit:
                    await self._limiter.acquire(task_name, task.concurrency_limit)

                try:
                    # 执行任务
                    result = await task.run(task_data, msg_id=msg_id)

                    # 生成指纹（用于去重）
                    fingerprint = None
                    if task.dedup:
                        fingerprint = self._cache.generate_fingerprint(task_name, task_data)

                    self.logger.info(f"[{worker_name}] Task '{task_name}' {msg_id} completed successfully")
                    return {"data": result, "fingerprint": fingerprint}, "success"

                finally:
                    # 释放限流许可
                    if task.concurrency_limit:
                        self._limiter.release(task_name)

            except Exception as e:
                last_error = e
                self.logger.error(
                    f"Task '{task_name}' {msg_id} failed (attempt {attempt + 1}): {e}",
                    exc_info=True
                )

                # 判断是否需要重试
                if attempt < self.max_retries:
                    if self.retry_delay > 0:
                        self.logger.info(
                            f"Retrying task '{task_name}' {msg_id} in {self.retry_delay}s..."
                        )
                        await asyncio.sleep(self.retry_delay)
                else:
                    # 达到最大重试次数
                    self.logger.error(
                        f"Task '{task_name}' {msg_id} permanently failed after {self.max_retries} retries: {last_error}"
                    )
                    return {"error": str(last_error)}, "failed"

        # 不应该到达这里
        return {"error": "Unknown error"}, "failed"

    async def _check_cache(
        self,
        task_name: str,
        data: Dict[str, Any],
        ttl: Optional[int]
    ) -> Optional[Any]:
        """
        检查缓存

        Args:
            task_name: 任务名称
            data: 任务数据
            ttl: 缓存过期时间（秒）

        Returns:
            缓存的结果，如果未命中则返回 None
        """
        return await self._cache.check(task_name=task_name, data=data, ttl=ttl)
