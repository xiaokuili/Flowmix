"""
Worker - 任务执行器

负责从队列获取数据并执行 Task
"""

import asyncio
import logging
import signal
from typing import Optional, Dict, Any, Union

from .manager import Manager
from .task import Task
from .limiter import ConcurrencyLimiter


class Worker:
    """
    Worker - 任务管理器

    职责：
    - 提交任务到队列（push）
    - 执行任务（run）
    - 查询任务状态（get_task_info, get_tree_stats）
    - 支持异步并发处理（async/await）
    - 支持重试机制

    内部使用 Manager 作为队列实现（不暴露给用户）

    Example:
        # 定义 Task
        task = Task()

        @task.execute
        async def crawl(data):
            return await fetch(data['url'])

        @task.on_success
        async def save(data, result):
            await save_to_db(result)

        # 创建 Worker
        worker = Worker(
            tasks=task,
            num_workers=5,        # 5 个并发协程
            max_retries=3,        # 最多重试 3 次
            retry_delay=5,        # 重试间隔 5 秒
        )

        # 提交任务
        root_id = worker.push({"url": "http://example.com"})

        # 查询状态
        stats = worker.get_tree_stats(root_id)

        # 执行任务
        worker.run()
    """

    def __init__(
        self,
        tasks: Union[Task, Dict[str, Task]],
        manager: Optional[Manager] = None,
        db_path: str = ".flowmix/flowmix.db",
        queue_name: str = "tasks",
        name: Optional[str] = None,
        num_workers: int = 1,
        max_retries: int = 0,
        retry_delay: float = 0,
    ):
        """
        初始化 Worker

        Args:
            tasks: Task 实例或 Task 字典 {task_name: Task}
                  - 单个 Task: 兼容旧版本
                  - 字典: 支持多个 Task，基于 task_name 路由
            manager: Manager 实例（可选，用于向后兼容）
            db_path: SQLite 数据库文件路径（默认: .flowmix/flowmix.db）
            queue_name: 队列名称（默认: tasks）
            name: Worker 名称（默认自动生成）
            num_workers: 并发协程数量（默认 1）
            max_retries: 失败后最大重试次数（默认 0，即不重试）
            retry_delay: 重试间隔秒数（默认 0，即立即重试）
        """
        # 统一处理为字典格式
        if isinstance(tasks, Task):
            # 单个 Task，使用其 name 作为 key（兼容旧版本）
            task_name = tasks.name or 'default'
            self.tasks = {task_name: tasks}
        else:
            self.tasks = tasks

        # Manager 作为内部实现（支持向后兼容）
        if manager is not None:
            self._manager = manager
        else:
            self._manager = Manager(db_path=db_path, queue_name=queue_name)

        # 设置 Task 的数据库配置（用于缓存功能）
        for task in self.tasks.values():
            task.set_db_config(db_path, queue_name)
            # 设置 Worker 引用（用于 callback 立即提交任务）
            task._worker = self

        self.name = name or self._generate_name()
        self.num_workers = max(1, num_workers)  # 至少 1 个
        self.max_retries = max_retries
        self.retry_delay = retry_delay

        # 统计信息
        self.stats = {
            "processed": 0,
            "success": 0,
            "failed": 0,
            "retried": 0,
        }

        # 控制标志
        self.running = False

        # 并发限流器（全局单例）
        self._limiter = ConcurrencyLimiter()

        self.logger = logging.getLogger(__name__)
        self.logger.info(
            f"Worker initialized: {self.name} "
            f"(num_workers={self.num_workers}, "
            f"max_retries={self.max_retries}, "
            f"retry_delay={self.retry_delay}s)"
        )

    @staticmethod
    def _generate_name() -> str:
        """生成 Worker 名称"""
        import socket
        import os
        import time
        hostname = socket.gethostname()
        pid = os.getpid()
        timestamp = int(time.time())
        return f"worker-{hostname}-{pid}-{timestamp}"

    async def push(self, data: Dict[str, Any], priority: int = 0, parent_id: Optional[int] = None, task_name: Optional[str] = None) -> int:
        """
        提交任务到队列

        Args:
            data: 任务数据（字典）
            priority: 优先级（默认 0，数字越大越优先）
            parent_id: 父任务 ID（可选，用于构建任务树）
            task_name: 任务名称（可选，如果未指定则使用默认任务名）

        Returns:
            任务 ID

        Example:
            # 提交根任务
            root_id = await worker.push({"url": "http://example.com"}, task_name="crawl")

            # 提交子任务
            child_id = await worker.push({"url": "http://example.com/page1"}, parent_id=root_id, task_name="crawl")
        """
        # 如果未指定 task_name，使用默认任务名（当只有单个 Task 时）
        if task_name is None and len(self.tasks) == 1:
            task_name = list(self.tasks.keys())[0]

        return await self._manager.push(data, priority, parent_id, task_name)

    async def run(self, check_interval: float = 0.5, max_idle_time: float = 2.0):
        """
        启动 Worker（运行直到队列为空）

        默认行为：自动运行直到队列为空并且所有任务处理完成
        适用于测试、批处理任务、以及大部分使用场景

        Args:
            check_interval: 检查队列的时间间隔（秒）
            max_idle_time: 队列为空后等待的最大时间（秒），确保所有任务都完成

        Example:
            # 最简单的用法（直接 await）
            worker = Worker(tasks=task, num_workers=3)
            await worker.push({'url': 'http://example.com'})
            await worker.run()  # 自动运行直到完成

            # 同步脚本
            asyncio.run(worker.run())
        """
        self.running = True

        # 注册信号处理
        try:
            signal.signal(signal.SIGINT, self._signal_handler)
            signal.signal(signal.SIGTERM, self._signal_handler)
        except ValueError:
            pass

        self.logger.info(f"Worker {self.name} started with {self.num_workers} concurrent workers")

        # 创建 worker 协程
        workers = [
            asyncio.create_task(self._worker_loop_async(f"{self.name}-{i}"))
            for i in range(self.num_workers)
        ]

        # 监控队列，直到为空
        idle_start = None
        try:
            while self.running:
                pending_count = await self._manager.get_pending_count()

                if pending_count == 0:
                    # 队列为空，开始计时
                    if idle_start is None:
                        idle_start = asyncio.get_event_loop().time()
                        self.logger.debug("Queue is empty, waiting for tasks to complete...")

                    # 检查是否已经空闲足够长时间
                    idle_duration = asyncio.get_event_loop().time() - idle_start
                    if idle_duration >= max_idle_time:
                        self.logger.info(f"Queue empty for {idle_duration:.1f}s, stopping workers")
                        break
                else:
                    # 队列不为空，重置计时
                    idle_start = None

                await asyncio.sleep(check_interval)

            # 停止所有 worker
            self.running = False
            for w in workers:
                w.cancel()
            await asyncio.gather(*workers, return_exceptions=True)

        except Exception as e:
            self.logger.error(f"Worker error: {e}", exc_info=True)
            self.running = False
            for w in workers:
                w.cancel()
            await asyncio.gather(*workers, return_exceptions=True)
            raise
        finally:
            self._cleanup()

    async def _run_async_workers(self):
        """异步模式：并发处理消息（单个 event loop + 多个协程）"""
        # 创建多个并发协程
        workers = [
            asyncio.create_task(self._worker_loop_async(f"{self.name}-{i}"))
            for i in range(self.num_workers)
        ]

        # 等待所有协程完成
        try:
            await asyncio.gather(*workers)
        except Exception as e:
            self.logger.error(f"Worker error: {e}", exc_info=True)
            # 取消所有协程
            for worker in workers:
                worker.cancel()
            # 等待取消完成
            await asyncio.gather(*workers, return_exceptions=True)

    async def _worker_loop_async(self, consumer_name: str):
        """异步 Worker 循环：持续从队列拉取并处理消息"""
        self.logger.debug(f"Worker {consumer_name} started")

        while self.running:
            try:
                # 从队列获取消息（异步调用）
                msg = await self._manager.pop(consumer_name)

                if not msg:
                    # 短暂等待，避免空转
                    await asyncio.sleep(0.1)
                    continue

                # 处理消息（异步）
                await self._process_message_async(msg, consumer_name)

            except asyncio.CancelledError:
                self.logger.info(f"Worker {consumer_name} cancelled")
                break
            except Exception as e:
                self.logger.error(
                    f"Worker {consumer_name} processing error: {e}",
                    exc_info=True
                )
                # 继续处理下一条消息
                continue

        self.logger.debug(f"Worker {consumer_name} stopped")

    async def _process_message_async(self, msg: Dict[str, Any], consumer_name: Optional[str] = None):
        """
        异步处理单个消息

        Args:
            msg: 从 Manager.pop() 返回的消息
                - msg["id"]: 消息 ID（用于 ack）
                - msg["task_name"]: 任务名称（用于路由）
                - msg["data"]: 业务数据（传给 Task）
            consumer_name: 消费者名称（用于日志）
        """
        msg_id = msg["id"]

        # 提取任务名称和数据
        task_name = msg.get("task_name")
        # 如果未指定 task_name，使用默认任务名（当只有单个 Task 时）
        if task_name is None and len(self.tasks) == 1:
            task_name = list(self.tasks.keys())[0]
        elif task_name is None:
            # 多个任务时必须指定 task_name
            self.logger.error(
                f"Message {msg_id} has no task_name. When multiple tasks are registered, "
                f"task_name must be specified. Available tasks: {list(self.tasks.keys())}"
            )
            await self._manager.ack(
                msg_id,
                True,  # failed
                "Missing task_name field"
            )
            return

        if "data" in msg:
            task_data = msg["data"]
        else:
            # 去掉 "id" 和 "task_name" 字段，剩下的都是业务数据
            task_data = {k: v for k, v in msg.items() if k not in ("id", "task_name")}

        # 根据 task_name 找到对应的 Task
        task = self.tasks.get(task_name)
        if not task:
            self.logger.error(
                f"Task '{task_name}' not found. Available tasks: {list(self.tasks.keys())}"
            )
            await self._manager.ack(msg_id)
            return

        # ========== 缓存检查（Task 层功能） ==========
        cached_result = None
        fingerprint = None

        if task.dedup:
            # Task 检查缓存
            cached_result = await task.check_cache(task_data)

            if cached_result is not None:
                # 命中缓存，直接返回结果
                fingerprint_preview = task.get_fingerprint(task_data)[:8] if task.get_fingerprint(task_data) else "unknown"
                log_prefix = f"[{consumer_name}] " if consumer_name else ""
                self.logger.info(
                    f"{log_prefix}Task '{task_name}' {msg_id} "
                    f"hit cache (fingerprint={fingerprint_preview}...), skipping execution"
                )

                self.stats["processed"] += 1
                self.stats["success"] += 1

                # 标记为 completed，使用缓存的结果
                await self._manager.ack(
                    msg_id,
                    False,  # 未失败
                    None,   # 无错误
                    cached_result,  # 缓存的结果
                    None    # 不需要保存 fingerprint（已存在）
                )

                return

        # 执行任务（带重试）
        retry_count = 0
        last_error = None

        while retry_count <= self.max_retries:
            try:
                if retry_count > 0:
                    log_prefix = f"[{consumer_name}] " if consumer_name else ""
                    self.logger.info(
                        f"{log_prefix}Processing task '{task_name}': {msg_id} "
                        f"(retry {retry_count}/{self.max_retries})"
                    )
                else:
                    log_prefix = f"[{consumer_name}] " if consumer_name else ""
                    self.logger.info(f"{log_prefix}Processing task '{task_name}': {msg_id}")

                # 限流控制：获取执行许可（如果配置了 concurrency_limit）
                if task.concurrency_limit:
                    await self._limiter.acquire(task_name, task.concurrency_limit)

                try:
                    # 执行任务（异步），传入 msg_id
                    result = await task.run(task_data, msg_id=msg_id)
                finally:
                    # 释放执行许可
                    if task.concurrency_limit:
                        self._limiter.release(task_name)

                # 任务成功（callback 已在 task.run() 中立即提交）
                self.stats["processed"] += 1
                self.stats["success"] += 1

                # 获取任务指纹（由 Task 生成）
                fingerprint = task.get_fingerprint(task_data)

                # 确认消息（标记为 completed），保存执行结果和指纹
                await self._manager.ack(
                    msg_id,
                    False,  # 未失败
                    None,   # 无错误
                    result,  # 执行结果
                    fingerprint  # 任务指纹（用于去重，如果 task.dedup=False 则为 None）
                )

                log_prefix = f"[{consumer_name}] " if consumer_name else ""
                self.logger.info(f"{log_prefix}Task '{task_name}' {msg_id} completed successfully")
                return

            except Exception as e:
                last_error = e
                self.logger.error(
                    f"Task '{task_name}' {msg_id} failed (attempt {retry_count + 1}): {e}",
                    exc_info=True
                )

                # 判断是否需要重试
                if retry_count < self.max_retries:
                    retry_count += 1
                    self.stats["retried"] += 1

                    # 延迟（异步）
                    if self.retry_delay > 0:
                        self.logger.info(
                            f"Retrying task '{task_name}' {msg_id} in {self.retry_delay}s..."
                        )
                        await asyncio.sleep(self.retry_delay)
                else:
                    # 达到最大重试次数
                    break

        # 所有重试都失败了
        self.stats["processed"] += 1
        self.stats["failed"] += 1

        self.logger.error(
            f"Task '{task_name}' {msg_id} permanently failed after {self.max_retries} retries: {last_error}"
        )

        # 确认消息（标记为 failed）
        await self._manager.ack(msg_id, True, str(last_error))

    def _signal_handler(self, signum, _):
        """信号处理器（优雅关闭）"""
        self.logger.info(f"Received signal {signum}, shutting down...")
        self.running = False

    def _cleanup(self):
        """清理资源"""
        self.logger.info(
            f"Worker {self.name} stopped. Stats: {self.stats}"
        )

    def stop(self):
        """停止 Worker"""
        self.running = False

    def get_stats(self) -> Dict[str, int]:
        """
        获取统计信息

        Returns:
            统计字典，包含：
            - processed: 处理的任务总数
            - success: 成功的任务数
            - failed: 失败的任务数
            - retried: 重试的次数
        """
        return self.stats.copy()
