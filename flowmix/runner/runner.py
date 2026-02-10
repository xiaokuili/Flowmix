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
from urllib.parse import urlparse

from .task import Task
from ..common.queue import Queue
from .cache.base import Cache
from .cache.redis import RedisCache
from .limit.base import RateLimiter
from .limit.memory import MemoryRateLimiter
from .limit.redis import RedisRateLimiter
from .engine import TaskEngine
from ..sender.pub import Pub


@dataclass
class RunnerConfig:
    """
    运行器配置

    Attributes:
        num_workers: 并发协程数量（默认 1）
        max_retries: 失败后最大重试次数（默认 0，即不重试）
        retry_delay: 重试间隔秒数（默认 0，即立即重试）
        name: 运行器名称（默认自动生成）
        limiter_url: 限流器 URL（默认为 None，使用内存限流器）
                     - None: 使用 MemoryRateLimiter（基于内存，单机）
                     - redis://... 或 rediss://...: 使用 RedisRateLimiter（基于 Redis，分布式）
        execution_timeout: 单个任务最大执行时长（秒）
                          - None: 不限制执行时长
                          - >0: 超时后标记任务失败
        recover_processing_on_start: 启动时是否恢复 processing 中的任务
                                     - True: 自动回滚到 pending，防止重启丢任务
        processing_stale_after: 恢复 processing 任务的最小停滞时长（秒）
                                - 0: 恢复所有 processing 任务
    """

    num_workers: int = 1
    max_retries: int = 0
    retry_delay: float = 0
    name: Optional[str] = None
    limiter_url: Optional[str] = None
    execution_timeout: Optional[float] = None
    recover_processing_on_start: bool = True
    processing_stale_after: float = 0.0


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

        # 创建 Runner
        runner = TaskRunner(
            tasks={"crawl": crawl_task},
            url="redis://localhost:6379/0",
            cache_url=None,  # 可选，不提供则不使用缓存
            queue_name="tasks",
            config=RunnerConfig(num_workers=5, max_retries=3)
        )

        # 执行任务
        await runner.run()
    """

    def __init__(
        self,
        tasks: Dict[str, Task],
        url: str,
        queue_name: str = "tasks",
        cache_url: Optional[str] = None,
        cache: Optional[Cache] = None,
        config: Optional[RunnerConfig] = None,
    ):
        """
        初始化 TaskRunner

        Args:
            tasks: 任务字典 {task_name: Task}
            url: 队列 URL（支持 redis://, rediss://, postgresql://）
            queue_name: 队列名称
            cache_url: 缓存 URL（可选，如果不提供则不使用缓存，支持 redis://, rediss://）
            cache: 缓存实例（可选，如果提供则直接使用，优先级高于 cache_url）
            config: 运行器配置
        """
        self.tasks = tasks
        self._url = url
        self._cache_url = cache_url
        self._cache_instance = cache  # 用户提供的缓存实例
        self._queue_name = queue_name
        self.config = config or RunnerConfig()

        # 设置名称
        self.name = self.config.name or self._generate_name()
        self.num_workers = max(1, self.config.num_workers)

        # 核心组件（延迟初始化）
        self._queue: Optional[Queue] = None
        self._cache: Optional[Cache] = None
        self._limiter: Optional[RateLimiter] = None
        self._engine = None

        # 外部连接（需要关闭）
        self._redis_conn = None
        self._cache_redis_conn = None
        self._limiter_redis_conn = None

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

    async def _setup_queue(self):
        """设置队列"""
        from ..common.queue import RedisQueue, MemoryQueue
        from ..common.pool import RedisPool

        parsed = urlparse(self._url)
        scheme = parsed.scheme

        if scheme in ("redis", "rediss"):
            # 获取 RedisPool 单例
            pool = await RedisPool.get_instance(self._url)
            self._queue = RedisQueue(pool=pool, queue_name=self._queue_name)


        elif scheme == "memory":
            # 内存队列
            self._queue = MemoryQueue(queue_name=self._queue_name)

        else:
            raise ValueError(
                f"Unsupported URL scheme: {scheme}. Only 'redis://', 'rediss://', 'sqlite://', and 'memory://' are supported."
            )

    async def _setup_cache(self):
        """设置缓存"""
        # 优先使用用户提供的缓存实例
        if self._cache_instance is not None:
            self._cache = self._cache_instance
            return

        if self._cache_url is None:
            # 不使用缓存
            self._cache = None
            return

        from ..common.pool import RedisPool

        parsed = urlparse(self._cache_url)
        scheme = parsed.scheme


        if scheme in ("redis", "rediss"):
            # 获取 RedisPool 单例（可能复用队列的连接池）
            pool = await RedisPool.get_instance(self._cache_url)
            self._cache = RedisCache(pool=pool, queue_name=self._queue_name)

        elif scheme == "memory":
            # 内存缓存
            from .cache.memory import MemoryCache
            self._cache = MemoryCache()

        else:
            raise ValueError(
                f"Unsupported cache URL scheme: {scheme}. Only 'redis://', 'rediss://', 'memory://' are supported."
            )

    async def _setup_limiter(self):
        """设置限流器"""
        limiter_url = self.config.limiter_url

        if limiter_url is None:
            # 默认使用内存限流器
            self._limiter = MemoryRateLimiter()
            return

        parsed = urlparse(limiter_url)
        scheme = parsed.scheme

        if scheme in ("redis", "rediss"):
            try:
                import redis.asyncio as aioredis
            except ImportError:
                raise ImportError(
                    "redis is required for redis:// or rediss:// URLs. "
                    "Install it with: pip install 'flowmix[redis]'"
                )

            # 检查是否可以复用现有的 Redis 连接
            if limiter_url == self._url and self._redis_conn:
                # 复用队列的 Redis 连接
                self._limiter = RedisRateLimiter(redis=self._redis_conn)
            elif self._cache_url and limiter_url == self._cache_url and self._cache_redis_conn:
                # 复用缓存的 Redis 连接
                self._limiter = RedisRateLimiter(redis=self._cache_redis_conn)
            else:
                # 创建新的 Redis 连接
                self._limiter_redis_conn = await aioredis.from_url(
                    limiter_url, decode_responses=True
                )
                self._limiter = RedisRateLimiter(redis=self._limiter_redis_conn)

        else:
            raise ValueError(
                f"Unsupported limiter URL scheme: {scheme}. Only 'redis://' and 'rediss://' are supported."
            )

    def _setup_task_callbacks(self):
        """为每个 Task 设置回调中使用的 sender"""
        sender = Pub(self._queue)
        for task in self.tasks.values():
            task._sender = sender  # 设置 _sender 以支持 callback()

    async def run(self):
        """
        启动运行器

        默认行为：持续运行等待新任务，直到收到停止信号（Ctrl+C 或 stop()）
        适用于生产环境、长期运行的后台服务

        Args:
            

        Example:
            # 生产环境：持续运行
            await runner.run()  # 一直运行，按 Ctrl+C 停止

            # 测试/批处理：自动停止
            await runner.run(auto_stop=True)  # 队列为空后自动停止
        """
        # 初始化队列、缓存和限流器
        await self._setup_queue()
        await self._setup_cache()
        await self._setup_limiter()

        # 设置 Task 回调
        self._setup_task_callbacks()

        self.running = True
        self._stop_event = asyncio.Event()

        # 初始化 TaskEngine（传入 stop_event）
        self._engine = TaskEngine(
            cache=self._cache,
            limiter=self._limiter,
            queue=self._queue,
            max_retries=self.config.max_retries,
            retry_delay=self.config.retry_delay,
            stop_event=self._stop_event,
            execution_timeout=self.config.execution_timeout,
        )

        if self.config.recover_processing_on_start:
            recovered = await self._queue.recover_processing_tasks(
                stale_after=self.config.processing_stale_after
            )
            if recovered > 0:
                self.logger.warning(
                    f"Recovered {recovered} processing tasks back to pending queue"
                )

        # 注册信号处理
        try:
            signal.signal(signal.SIGINT, self._signal_stop)
            signal.signal(signal.SIGTERM, self._signal_stop)
        except ValueError:
            pass

        self.logger.info(
            f"TaskRunner {self.name} started with {self.num_workers} concurrent workers"
        )

        # 创建 worker 协程
        self._workers = [
            asyncio.create_task(self._worker_loop(i)) for i in range(self.num_workers)
        ]

        try:
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
            await self._cleanup()

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
                        timeout=0.5,  # 500ms 超时，让停止信号能及时响应
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
                    self.logger.info(
                        f"Worker {worker_name} stopping, message {msg['id']} will be requeued"
                    )
                    break

                # 处理消息
                await self._process_message(msg, worker_name)

            except asyncio.CancelledError:
                self.logger.info(f"Worker {worker_name} cancelled")
                break
            except Exception as e:
                self.logger.error(
                    f"Worker {worker_name} processing error: {e}", exc_info=True
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
                msg_id, failed=True, error=f"Task '{task_name}' not found"
            )
            return

        # 2. 执行任务（委托给 TaskEngine）
        result, status = await self._engine.execute(msg, task, worker_name)

        # 3. 确认消息
        await self._queue.ack(
            msg_id,
            failed=(status == "failed"),
            error=result.get("error") if status == "failed" else None,
            result=result.get("data") if status in ("success", "cached") else None,
            fingerprint=result.get("fingerprint"),
        )

    def _signal_stop(self, signum, _):
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

    async def _cleanup(self):
        """清理资源"""
        # 关闭限流器
        if self._limiter:
            await self._limiter.close()

        # 关闭缓存
        if self._cache:
            await self._cache.close()

        # 关闭队列
        if self._queue:
            await self._queue.close()

        # 关闭 Redis 连接
        if self._limiter_redis_conn:
            await self._limiter_redis_conn.close()
        if self._cache_redis_conn:
            await self._cache_redis_conn.close()
        if self._redis_conn:
            await self._redis_conn.close()

    def stop(self):
        """停止运行器（设置停止标志和事件）"""
        self.logger.info("Stopping runner...")
        self.running = False
        if self._stop_event:
            self._stop_event.set()
