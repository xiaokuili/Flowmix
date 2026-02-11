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
import traceback
import os
from typing import Dict, Any, Tuple, Optional

from .task import Task
from .cache.base import Cache
from .limit.base import RateLimiter
from ..common.queue import Queue


class TaskEngine:
    """
    任务执行引擎

    职责：
    - 缓存检查（如果 Task 启用了 dedup）
    - 限流控制（如果 Task 设置了 concurrency_limit）
    - 重试逻辑（根据配置的 max_retries 和 retry_delay）
    - 执行任务

    这是一个内部类，由 TaskRunner 使用
    """

    def __init__(
        self,
        cache: Optional[Cache],
        limiter: RateLimiter,
        queue: Queue,
        max_retries: int = 0,
        retry_delay: float = 0,
        stop_event: Optional[asyncio.Event] = None,
        execution_timeout: Optional[float] = None,
    ):
        """
        初始化 TaskEngine

        Args:
            cache: 缓存实例（可选）
            limiter: 限流器实例
            queue: 队列实例
            max_retries: 最大重试次数
            retry_delay: 重试延迟（秒）
            stop_event: 停止事件（用于取消任务）
            execution_timeout: 单任务执行超时（秒）
        """
        self._cache = cache
        self._limiter = limiter
        self._queue = queue
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self._stop_event = stop_event
        self.execution_timeout = execution_timeout
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
        task_name_raw = msg.get("task_name")
        if not isinstance(task_name_raw, str) or not task_name_raw:
            return {"error": f"Invalid task_name: {task_name_raw!r}"}, "failed"

        task_name = task_name_raw
        msg_id = msg["id"]

        # 提取任务数据
        if "data" in msg:
            task_data = msg["data"]
        else:
            # 去掉 "id" 和 "task_name" 字段，剩下的都是业务数据
            task_data = {k: v for k, v in msg.items() if k not in ("id", "task_name")}

        # 1. 缓存检查
        if task.dedup and self._cache:
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
                    async def run_task_with_controls():
                        # 执行任务（支持停止信号取消）
                        if self._stop_event:
                            task_future = asyncio.create_task(task.run(task_data, msg_id=msg_id))
                            stop_task = asyncio.create_task(self._stop_event.wait())

                            done, pending = await asyncio.wait(
                                {task_future, stop_task},
                                return_when=asyncio.FIRST_COMPLETED
                            )

                            for pending_task in pending:
                                pending_task.cancel()

                            if task_future in done:
                                return task_future.result()

                            task_future.cancel()
                            await asyncio.gather(task_future, return_exceptions=True)
                            self.logger.warning(
                                f"[{worker_name}] Task '{task_name}' {msg_id} cancelled due to shutdown"
                            )
                            raise asyncio.CancelledError("Task cancelled due to shutdown")

                        return await task.run(task_data, msg_id=msg_id)

                    # 超时控制：超时后抛异常，走 failed/retry 逻辑
                    if self.execution_timeout is not None and self.execution_timeout > 0:
                        try:
                            result = await asyncio.wait_for(
                                run_task_with_controls(),
                                timeout=self.execution_timeout
                            )
                        except asyncio.TimeoutError as exc:
                            raise TimeoutError(
                                f"Task execution timed out after {self.execution_timeout}s"
                            ) from exc
                    else:
                        result = await run_task_with_controls()

                    # 生成指纹（用于去重）
                    fingerprint = None
                    if task.dedup and self._cache:
                        fingerprint = self._cache.generate_fingerprint(task_name, task_data)
                        # 保存结果到缓存
                        await self._cache.set(task_name, task_data, result)

                    self.logger.info(f"[{worker_name}] Task '{task_name}' {msg_id} completed successfully")
                    return {"data": result, "fingerprint": fingerprint}, "success"

                finally:
                    # 释放限流许可
                    if task.concurrency_limit:
                        self._limiter.release(task_name)

            except Exception as e:
                last_error = e

                # 提取用户代码的错误位置（跳过框架内部的调用栈）
                error_location = self._extract_user_error_location(e)

                # 判断是否需要重试
                if attempt < self.max_retries:
                    self.logger.warning(
                        f"Task '{task_name}' {msg_id} failed (attempt {attempt + 1}/{self.max_retries + 1}): "
                        f"{type(e).__name__}: {e}"
                        f"{error_location}"
                    )
                    if self.retry_delay > 0:
                        self.logger.info(
                            f"Retrying task '{task_name}' {msg_id} in {self.retry_delay}s..."
                        )
                        await asyncio.sleep(self.retry_delay)
                else:
                    # 达到最大重试次数
                    self.logger.error(
                        f"Task '{task_name}' {msg_id} permanently failed after {self.max_retries} retries: "
                        f"{type(e).__name__}: {e}"
                        f"{error_location}"
                    )
                    return {"error": str(last_error)}, "failed"

        # 不应该到达这里
        return {"error": "Unknown error"}, "failed"

    def _extract_user_error_location(self, exception: Exception) -> str:
        """
        提取用户代码中的错误位置（跳过框架内部调用栈）

        Args:
            exception: 异常对象

        Returns:
            错误位置字符串，例如 " at examples/stats.py:16"
        """
        tb = traceback.extract_tb(exception.__traceback__)

        # 从后往前找，跳过 flowmix 框架内部的路径
        for frame in reversed(tb):
            # 如果不是 flowmix 内部文件，就是用户代码
            if 'flowmix' not in frame.filename or 'examples' in frame.filename:
                # 转换为相对路径
                try:
                    rel_path = os.path.relpath(frame.filename)
                except ValueError:
                    # 如果无法转换（不同驱动器等），使用原路径
                    rel_path = frame.filename
                return f" at {rel_path}:{frame.lineno}"

        # 如果都是框架内部调用，返回最后一个
        if tb:
            last_frame = tb[-1]
            try:
                rel_path = os.path.relpath(last_frame.filename)
            except ValueError:
                rel_path = last_frame.filename
            return f" at {rel_path}:{last_frame.lineno}"

        return ""

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
        if not self._cache:
            return None
        return await self._cache.check(task_name=task_name, data=data, ttl=ttl)
