"""
Worker - 任务执行器

负责从队列获取数据并执行 Task
"""

import logging
import signal
import time
from typing import Optional, Dict, Any, Union, List
from concurrent.futures import ThreadPoolExecutor, as_completed

from .manager import Manager
from .task import Task


class Worker:
    """
    Worker - 任务执行器

    职责：
    - 从 Manager 获取消息（字典数据）
    - 调用 Task.run(data) 执行任务
    - 处理任务执行的成功和失败
    - 支持并发处理（多线程）
    - 支持重试机制

    Example:
        # 定义 Task
        task = Task()

        @task.execute
        def crawl(data):
            return fetch(data['url'])

        @task.on_success
        def save(data, result):
            save_to_db(result)

        # 创建 Worker
        manager = Manager(redis_url="redis://localhost:6379")
        worker = Worker(
            task=task,
            manager=manager,
            num_workers=5,        # 5 个并发
            max_retries=3,        # 最多重试 3 次
            retry_delay=5,        # 重试间隔 5 秒
        )

        # 启动
        worker.run()
    """

    def __init__(
        self,
        tasks: Union[Task, Dict[str, Task]],
        manager: Manager,
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
            manager: Manager 实例（队列管理器）
            name: Worker 名称（默认自动生成）
            num_workers: 并发处理的 worker 数量（默认 1，即单线程）
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

        self.manager = manager
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
        hostname = socket.gethostname()
        pid = os.getpid()
        timestamp = int(time.time())
        return f"worker-{hostname}-{pid}-{timestamp}"

    def run(self):
        """
        启动 Worker（阻塞运行）

        流程：
        1. 如果 num_workers = 1，单线程处理
        2. 如果 num_workers > 1，使用线程池并发处理
        """
        self.running = True

        # 注册信号处理
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        self.logger.info(f"Worker {self.name} started with {self.num_workers} workers")

        try:
            if self.num_workers == 1:
                # 单线程模式
                self._run_single_worker()
            else:
                # 多线程模式
                self._run_multi_workers()

        except Exception as e:
            self.logger.error(f"Worker error: {e}", exc_info=True)
            raise
        finally:
            self._cleanup()

    def _run_single_worker(self):
        """单线程模式：顺序处理消息"""
        consumer_name = f"{self.name}-0"

        while self.running:
            # 从队列获取消息
            msg = self.manager.pop(consumer_name=consumer_name)

            if not msg:
                continue

            # 处理消息
            self._process_message(msg)

    def _run_multi_workers(self):
        """多线程模式：并发处理消息"""
        with ThreadPoolExecutor(max_workers=self.num_workers) as executor:
            # 为每个 worker 创建独立的 consumer name
            consumer_names = [
                f"{self.name}-{i}" for i in range(self.num_workers)
            ]

            # 提交初始任务
            futures = {}
            for consumer_name in consumer_names:
                future = executor.submit(self._worker_loop, consumer_name)
                futures[future] = consumer_name

            # 等待所有 worker 完成
            for future in as_completed(futures):
                consumer_name = futures[future]
                try:
                    future.result()
                except Exception as e:
                    self.logger.error(
                        f"Worker {consumer_name} error: {e}",
                        exc_info=True
                    )

    def _worker_loop(self, consumer_name: str):
        """Worker 循环：持续从队列拉取并处理消息"""
        self.logger.debug(f"Worker {consumer_name} started")

        while self.running:
            try:
                # 从队列获取消息
                msg = self.manager.pop(consumer_name=consumer_name)

                if not msg:
                    continue

                # 处理消息
                self._process_message(msg, consumer_name)

            except Exception as e:
                self.logger.error(
                    f"Worker {consumer_name} processing error: {e}",
                    exc_info=True
                )
                # 继续处理下一条消息
                continue

        self.logger.debug(f"Worker {consumer_name} stopped")

    def _process_message(self, msg: Dict[str, Any], consumer_name: Optional[str] = None):
        """
        处理单个消息

        Args:
            msg: 从 Manager.pop() 返回的消息
                - msg["id"]: 消息 ID（用于 ack）
                - msg["task_name"]: 任务名称（用于路由）
                - msg["data"]: 业务数据（传给 Task）
            consumer_name: 消费者名称（用于日志）
        """
        msg_id = msg["id"]

        # 提取任务名称和数据
        task_name = msg.get("task_name", "default")
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
            self.manager.ack(msg_id)
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

                # 执行任务
                task.run(task_data)

                # 处理通过 task.callback() 提交的任务
                if task._pending_callbacks:
                    for callback_info in task._pending_callbacks:
                        callback_task_name = callback_info['task_name']
                        callback_data = callback_info['data']
                        callback_priority = callback_info['priority']

                        # 将 task_name 和 data 合并提交到队列
                        message = {
                            'task_name': callback_task_name,
                            **callback_data
                        }
                        self.manager.push(message, priority=callback_priority)
                        self.logger.debug(
                            f"Callback task '{callback_task_name}' with priority {callback_priority}: {callback_data}"
                        )

                # 任务成功
                self.stats["processed"] += 1
                self.stats["success"] += 1

                # 确认消息
                self.manager.ack(msg_id)

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

                    # 延迟
                    if self.retry_delay > 0:
                        self.logger.info(
                            f"Retrying task '{task_name}' {msg_id} in {self.retry_delay}s..."
                        )
                        time.sleep(self.retry_delay)
                else:
                    # 达到最大重试次数
                    break

        # 所有重试都失败了
        self.stats["processed"] += 1
        self.stats["failed"] += 1

        self.logger.error(
            f"Task '{task_name}' {msg_id} permanently failed after {self.max_retries} retries: {last_error}"
        )

        # 确认消息（不再重试）
        self.manager.ack(msg_id)

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
