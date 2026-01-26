"""
TaskRunner - 任务运行器

职责：
- 从队列拉取任务
- 执行任务
- 处理重试、限流、缓存
"""

import asyncio
import logging
import signal
import socket
import os
import time
from dataclasses import dataclass
from typing import Dict, Any, Optional

from .task import Task
from .storage.task_queue import TaskQueue
from .storage.cache import Cache
from ._internal.limiter import ConcurrencyLimiter
from ._internal.engine import TaskEngine
from .pub import Pub


@dataclass
class RunnerConfig:
    """
    运行器配置

    Attributes:
        num_workers: 并发协程数量（默认 1）
        max_retries: 失败后最大重试次数（默认 0，即不重试）
        retry_delay: 重试间隔秒数（默认 0，即立即重试）
        name: 运行器名称（默认自动生成）
    """
    num_workers: int = 1
    max_retries: int = 0
    retry_delay: float = 0
    name: Optional[str] = None


class TaskRunner:
    """
    任务运行器

    职责：
    - 从队列拉取任务
    - 执行任务
    - 处理重试、限流、缓存

    Example:
        # 定义 Task
        crawl_task = Task(name="crawl")

        @crawl_task.execute
        async def crawl(data):
            return await fetch(data['url'])

        # 初始化共享组件
        queue = TaskQueue(db_path=".flowmix/flowmix.db")
        cache = Cache(db_path=".flowmix/flowmix.db")

        # 创建 Runner
        runner = TaskRunner(
            tasks={"crawl": crawl_task},
            queue=queue,
            cache=cache,
            config=RunnerConfig(num_workers=5, max_retries=3)
        )

        # 执行任务
        await runner.run()
    """

    def __init__(
        self,
        tasks: Dict[str, Task],
        queue: TaskQueue,
        cache: Cache,
        config: Optional[RunnerConfig] = None
    ):
        """
        初始化 TaskRunner

        Args:
            tasks: 任务字典 {task_name: Task}
            queue: TaskQueue 实例
            cache: Cache 实例
            config: 运行器配置
        """
        self.tasks = tasks
        self._queue = queue
        self._cache = cache
        self.config = config or RunnerConfig()

        # 设置名称
        self.name = self.config.name or self._generate_name()
        self.num_workers = max(1, self.config.num_workers)

        # 核心组件
        self._limiter = ConcurrencyLimiter()
        # TaskEngine 将在 run() 时设置 stop_event
        self._engine = None

        # 设置 Task 的 producer 引用（用于 callback 中提交新任务）
        self._setup_task_callbacks()

        # 统计信息
        self.stats = {
            "processed": 0,
            "success": 0,
            "failed": 0,
            "cached": 0,
            "retried": 0,
        }

        # 控制标志
        self.running = False
        self._stop_event = None
        self._workers = []  # 保存 worker 任务引用
        self._shutdown_count = 0  # 关闭信号计数

        self.logger = logging.getLogger(__name__)
        self.logger.info(
            f"TaskRunner initialized: {self.name} "
            f"(num_workers={self.num_workers}, "
            f"max_retries={self.config.max_retries}, "
            f"retry_delay={self.config.retry_delay}s)"
        )

    @staticmethod
    def _generate_name() -> str:
        """生成运行器名称"""
        hostname = socket.gethostname()
        pid = os.getpid()
        timestamp = int(time.time())
        return f"runner-{hostname}-{pid}-{timestamp}"

    def _setup_task_callbacks(self):
        """为每个 Task 设置回调中使用的 producer"""
        producer = Pub(self._queue)
        for task in self.tasks.values():
            task._producer = producer

    async def run(
        self,
        auto_stop: bool = False,
        check_interval: float = 0.5,
        max_idle_time: float = 2.0
    ):
        """
        启动运行器

        默认行为：持续运行等待新任务，直到收到停止信号（Ctrl+C 或 stop()）
        适用于生产环境、长期运行的后台服务

        Args:
            auto_stop: 是否自动停止（默认 False）
                      - False: 持续运行，直到收到停止信号（适合生产环境）
                      - True: 队列为空后自动停止（适合测试、批处理任务）
            check_interval: 检查队列的时间间隔（秒），仅在 auto_stop=True 时使用
            max_idle_time: 队列为空后等待的最大时间（秒），仅在 auto_stop=True 时使用

        Example:
            # 生产环境：持续运行
            await runner.run()  # 一直运行，按 Ctrl+C 停止

            # 测试/批处理：自动停止
            await runner.run(auto_stop=True)  # 队列为空后自动停止
        """
        self.running = True
        self._stop_event = asyncio.Event()

        # 初始化 TaskEngine（传入 stop_event）
        self._engine = TaskEngine(
            cache=self._cache,
            limiter=self._limiter,
            queue=self._queue,
            max_retries=self.config.max_retries,
            retry_delay=self.config.retry_delay,
            stop_event=self._stop_event
        )

        # 注册信号处理
        try:
            signal.signal(signal.SIGINT, self._signal_handler)
            signal.signal(signal.SIGTERM, self._signal_handler)
        except ValueError:
            pass

        self.logger.info(f"TaskRunner {self.name} started with {self.num_workers} concurrent workers")

        # 创建 worker 协程
        self._workers = [
            asyncio.create_task(self._worker_loop(i))
            for i in range(self.num_workers)
        ]

        try:
            if auto_stop:
                # 自动停止模式：监控队列，直到为空
                idle_start = None
                while self.running:
                    pending_count = await self._queue.get_pending_count()

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
                self._stop_event.set()
                for w in self._workers:
                    w.cancel()
                await asyncio.gather(*self._workers, return_exceptions=True)
            else:
                # 持续运行模式：等待所有 worker 协程完成（通常是收到停止信号后）
                await asyncio.gather(*self._workers, return_exceptions=True)

        except Exception as e:
            self.logger.error(f"TaskRunner error: {e}", exc_info=True)
            self.running = False
            self._stop_event.set()
            for w in self._workers:
                w.cancel()
            await asyncio.gather(*self._workers, return_exceptions=True)
            raise
        finally:
            self._cleanup()

    async def _worker_loop(self, worker_id: int):
        """单个 worker 的执行循环"""
        worker_name = f"{self.name}-{worker_id}"
        self.logger.debug(f"Worker {worker_name} started")

        while self.running and not self._stop_event.is_set():
            try:
                # 从队列获取消息（使用 asyncio.wait_for 让它可以被中断）
                try:
                    msg = await asyncio.wait_for(
                        self._queue.pop(worker_name),
                        timeout=0.5  # 500ms 超时，让停止信号能及时响应
                    )
                except asyncio.TimeoutError:
                    # 超时后检查停止信号
                    if self._stop_event.is_set():
                        break
                    continue

                if not msg:
                    # 检查是否需要停止
                    if self._stop_event.is_set():
                        break
                    # 短暂等待，避免空转
                    await asyncio.sleep(0.1)
                    continue

                # 处理消息前再次检查停止信号
                if self._stop_event.is_set():
                    self.logger.info(f"Worker {worker_name} stopping, message {msg['id']} will be requeued")
                    break

                # 处理消息
                await self._process_message(msg, worker_name)

            except asyncio.CancelledError:
                self.logger.info(f"Worker {worker_name} cancelled")
                break
            except Exception as e:
                self.logger.error(
                    f"Worker {worker_name} processing error: {e}",
                    exc_info=True
                )
                # 继续处理下一条消息
                continue

        self.logger.debug(f"Worker {worker_name} stopped")

    async def _process_message(self, msg: Dict[str, Any], worker_name: str):
        """
        处理单条消息

        Args:
            msg: 消息对象（包含 id, task_name, data）
            worker_name: Worker 名称（用于日志）
        """
        msg_id = msg["id"]
        task_name = msg.get("task_name")

        # 1. 查找任务
        task = self.tasks.get(task_name)
        if not task:
            self.logger.error(
                f"Task '{task_name}' not found. Available tasks: {list(self.tasks.keys())}"
            )
            await self._queue.ack(
                msg_id,
                failed=True,
                error=f"Task '{task_name}' not found"
            )
            return

        # 2. 执行任务（委托给 TaskEngine）
        result, status = await self._engine.execute(msg, task, worker_name)

        # 3. 更新统计
        self.stats["processed"] += 1
        if status == "success":
            self.stats["success"] += 1
        elif status == "failed":
            self.stats["failed"] += 1
        elif status == "cached":
            self.stats["cached"] += 1

        # 4. 确认消息
        await self._queue.ack(
            msg_id,
            failed=(status == "failed"),
            error=result.get("error") if status == "failed" else None,
            result=result.get("data") if status in ("success", "cached") else None,
            fingerprint=result.get("fingerprint")
        )

    def _signal_handler(self, signum, _):
        """信号处理器（两级关闭机制）"""
        self._shutdown_count += 1

        if self._shutdown_count == 1:
            # 第一次 Ctrl+C：优雅关闭
            self.logger.info(f"Received signal {signum}, gracefully shutting down...")
            self.logger.info("Press Ctrl+C again to force quit immediately")
            self.running = False
            if self._stop_event:
                self._stop_event.set()
        else:
            # 第二次 Ctrl+C：强制关闭
            self.logger.warning("Force shutdown requested, cancelling all workers...")
            for w in self._workers:
                if not w.done():
                    w.cancel()
            # 直接退出
            import sys
            sys.exit(1)

    def _cleanup(self):
        """清理资源"""
        self.logger.info(
            f"TaskRunner {self.name} stopped. Stats: {self.stats}"
        )

    def stop(self):
        """停止运行器（设置停止标志和事件）"""
        self.logger.info("Stopping runner...")
        self.running = False
        if self._stop_event:
            self._stop_event.set()

    def get_stats(self) -> Dict[str, int]:
        """
        获取统计信息

        Returns:
            统计字典，包含：
            - processed: 处理的任务总数
            - success: 成功的任务数
            - failed: 失败的任务数
            - cached: 命中缓存的任务数
            - retried: 重试的次数
        """
        return self.stats.copy()
