"""
Scheduler - 定时任务调度器

支持 Cron 表达式的周期性任务调度
"""

import asyncio
import logging
from datetime import datetime
from typing import Optional, Dict, Any, List

try:
    from croniter import croniter
    CRONITER_AVAILABLE = True
except ImportError:
    CRONITER_AVAILABLE = False


class ScheduledTask:
    """调度任务定义"""

    def __init__(
        self,
        cron: str,
        task_name: str,
        data: Dict[str, Any],
        priority: int = 0,
        enabled: bool = True
    ):
        """
        初始化调度任务

        Args:
            cron: Cron 表达式（如 "0 8 * * *" 表示每天 8 点）
            task_name: 任务名称（对应 Consumer 中的 Task）
            data: 任务数据
            priority: 优先级（默认 0）
            enabled: 是否启用（默认 True）
        """
        if not CRONITER_AVAILABLE:
            raise ImportError(
                "croniter is required for Scheduler. "
                "Install it with: pip install croniter"
            )

        self.cron = cron
        self.task_name = task_name
        self.data = data
        self.priority = priority
        self.enabled = enabled

        # 验证 cron 表达式
        try:
            croniter(cron)
        except Exception as e:
            raise ValueError(f"Invalid cron expression '{cron}': {e}")

        self.last_run: Optional[datetime] = None
        self.next_run: Optional[datetime] = None
        self._update_next_run()

    def _update_next_run(self):
        """更新下次执行时间"""
        now = datetime.now()
        cron_iter = croniter(self.cron, now)
        self.next_run = cron_iter.get_next(datetime)

    def should_run(self) -> bool:
        """检查是否应该执行"""
        if not self.enabled:
            return False

        now = datetime.now()
        if self.next_run and now >= self.next_run:
            return True
        return False

    def mark_run(self):
        """标记已执行，更新下次执行时间"""
        self.last_run = datetime.now()
        self._update_next_run()

    def __repr__(self) -> str:
        return f"ScheduledTask(cron='{self.cron}', task='{self.task_name}', next_run={self.next_run})"


class Scheduler:
    """
    定时任务调度器

    支持基于 Cron 表达式的周期性任务调度

    职责：
    - 管理调度任务列表
    - 定时检查并触发任务
    - 自动提交任务到队列

    Example:
        from flowmix import Task, TaskQueue, TaskProducer, Scheduler
        import asyncio

        # 定义任务
        task = Task(name='daily_report')

        @task.execute
        async def run(data):
            print(f"执行每日报表: {data}")
            return "done"

        # 初始化队列
        queue = TaskQueue(db_path=".flowmix/flowmix.db")
        producer = TaskProducer(queue=queue)

        # 创建调度器
        scheduler = Scheduler(producer)

        # 添加定时任务
        scheduler.add_cron(
            cron='0 8 * * *',           # 每天 8 点
            task_name='daily_report',
            data={'type': 'sales'}
        )

        scheduler.add_cron(
            cron='*/5 * * * *',         # 每 5 分钟
            task_name='health_check',
            data={'endpoint': '/api/health'}
        )

        # 运行调度器
        await scheduler.run()

    Cron 表达式格式：
        * * * * *
        │ │ │ │ │
        │ │ │ │ └─── 星期几 (0-6, 0=周日)
        │ │ │ └───── 月份 (1-12)
        │ │ └─────── 日期 (1-31)
        │ └───────── 小时 (0-23)
        └─────────── 分钟 (0-59)

    示例：
        '0 8 * * *'      # 每天 8:00
        '*/5 * * * *'    # 每 5 分钟
        '0 */2 * * *'    # 每 2 小时
        '0 0 * * 0'      # 每周日 0:00
        '30 14 1 * *'    # 每月 1 号 14:30
    """

    def __init__(
        self,
        producer: Any,
        check_interval: float = 30.0
    ):
        """
        初始化调度器

        Args:
            producer: TaskProducer 实例（用于提交任务）
            check_interval: 检查间隔（秒，默认 30 秒）
        """
        if not CRONITER_AVAILABLE:
            raise ImportError(
                "croniter is required for Scheduler. "
                "Install it with: pip install croniter"
            )

        self.producer = producer
        self.check_interval = check_interval
        self.tasks: List[ScheduledTask] = []
        self.running = False
        self._stop_event: Optional[asyncio.Event] = None

        self.logger = logging.getLogger(__name__)
        self.logger.info(f"Scheduler initialized (check_interval={check_interval}s)")

    def add_cron(
        self,
        cron: str,
        task_name: str,
        data: Dict[str, Any],
        priority: int = 0,
        enabled: bool = True
    ) -> ScheduledTask:
        """
        添加 Cron 调度任务

        Args:
            cron: Cron 表达式（如 "0 8 * * *"）
            task_name: 任务名称
            data: 任务数据
            priority: 优先级（默认 0）
            enabled: 是否启用（默认 True）

        Returns:
            ScheduledTask 实例

        Example:
            # 每天 8 点执行
            scheduler.add_cron('0 8 * * *', 'daily_report', {'type': 'sales'})

            # 每 5 分钟执行
            scheduler.add_cron('*/5 * * * *', 'health_check', {'endpoint': '/api'})
        """
        scheduled_task = ScheduledTask(
            cron=cron,
            task_name=task_name,
            data=data,
            priority=priority,
            enabled=enabled
        )
        self.tasks.append(scheduled_task)

        self.logger.info(
            f"Added scheduled task: {scheduled_task.task_name} "
            f"(cron='{scheduled_task.cron}', next_run={scheduled_task.next_run})"
        )
        return scheduled_task

    def remove_task(self, task: ScheduledTask):
        """
        移除调度任务

        Args:
            task: 要移除的 ScheduledTask 实例
        """
        if task in self.tasks:
            self.tasks.remove(task)
            self.logger.info(f"Removed scheduled task: {task.task_name}")

    def get_tasks(self) -> List[ScheduledTask]:
        """
        获取所有调度任务

        Returns:
            调度任务列表
        """
        return self.tasks.copy()

    async def run(self):
        """
        启动调度器

        持续运行，定期检查并触发调度任务
        """
        self.running = True
        self._stop_event = asyncio.Event()

        self.logger.info(f"Scheduler started with {len(self.tasks)} scheduled tasks")

        # 打印所有调度任务
        for task in self.tasks:
            self.logger.info(f"  - {task}")

        try:
            while self.running and not self._stop_event.is_set():
                # 检查所有调度任务
                for task in self.tasks:
                    if task.should_run():
                        self.logger.info(
                            f"Triggering scheduled task: {task.task_name} "
                            f"(cron='{task.cron}')"
                        )

                        try:
                            # 提交任务到队列
                            await self.producer.push(
                                data=task.data,
                                priority=task.priority,
                                task_name=task.task_name
                            )

                            # 标记已执行
                            task.mark_run()

                            self.logger.info(
                                f"Scheduled task submitted: {task.task_name} "
                                f"(next_run={task.next_run})"
                            )
                        except Exception as e:
                            self.logger.error(
                                f"Failed to submit scheduled task {task.task_name}: {e}",
                                exc_info=True
                            )

                # 等待下一次检查
                await asyncio.sleep(self.check_interval)

        except asyncio.CancelledError:
            self.logger.info("Scheduler cancelled")
        except Exception as e:
            self.logger.error(f"Scheduler error: {e}", exc_info=True)
            raise
        finally:
            self._cleanup()

    def stop(self):
        """停止调度器"""
        self.logger.info("Stopping scheduler...")
        self.running = False
        if self._stop_event:
            self._stop_event.set()

    def _cleanup(self):
        """清理资源"""
        self.logger.info("Scheduler stopped")

    def __repr__(self) -> str:
        return f"Scheduler(tasks={len(self.tasks)}, running={self.running})"
