"""
Cron - 定时任务提交器

职责：
- 按照 cron 表达式定时提交任务
- 支持一次性、间隔、cron 表达式多种模式

基于 APScheduler 实现
"""

import logging
from typing import Dict, Any, Optional, Callable, Awaitable
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.date import DateTrigger

from ..common.queue import Queue


class Cron:
    """
    定时任务提交器

    职责：
    - 定时提交任务到队列

    Example:
        # 初始化
        from flowmix.common import SQLitePool, SQLiteQueue
        from flowmix.sender import Cron

        pool = await SQLitePool.get_instance('.flowmix/flowmix.db')
        queue = SQLiteQueue(pool=pool, queue_name='tasks')
        cron = Cron(queue=queue)

        # 每小时提交一次任务
        cron.add_interval(
            task_name="hourly_crawl",
            data_fn=lambda: {"url": "http://example.com", "timestamp": time.time()},
            hours=1
        )

        # 每天凌晨 2 点提交任务
        cron.add_cron(
            task_name="daily_crawl",
            data_fn=lambda: {"url": "http://example.com/daily"},
            hour=2,
            minute=0
        )

        # 启动调度器
        cron.start()

        # 停止调度器
        await cron.stop()
    """

    def __init__(self, queue: Queue):
        """
        初始化 Cron

        Args:
            queue: Queue 实例（SQLiteQueue 或 RedisQueue）
        """
        self._queue = queue
        self._scheduler = AsyncIOScheduler()
        self.logger = logging.getLogger(__name__)

    def add_interval(
        self,
        task_name: str,
        data_fn: Callable[[], Dict[str, Any]],
        priority: int = 0,
        weeks: int = 0,
        days: int = 0,
        hours: int = 0,
        minutes: int = 0,
        seconds: int = 0,
        job_id: Optional[str] = None
    ):
        """
        添加间隔任务

        Args:
            task_name: 任务名称
            data_fn: 返回任务数据的函数（每次执行时调用）
            priority: 优先级（默认 0）
            weeks: 间隔周数
            days: 间隔天数
            hours: 间隔小时数
            minutes: 间隔分钟数
            seconds: 间隔秒数
            job_id: 任务 ID（可选，用于后续删除）

        Example:
            # 每 30 分钟提交一次
            cron.add_interval(
                task_name="check_updates",
                data_fn=lambda: {"source": "api"},
                minutes=30
            )

            # 每 5 秒提交一次
            cron.add_interval(
                task_name="heartbeat",
                data_fn=lambda: {"status": "alive"},
                seconds=5,
                job_id="heartbeat_job"
            )
        """
        trigger = IntervalTrigger(
            weeks=weeks,
            days=days,
            hours=hours,
            minutes=minutes,
            seconds=seconds
        )

        self._scheduler.add_job(
            func=self._submit_task,
            trigger=trigger,
            args=(task_name, data_fn, priority),
            id=job_id,
            replace_existing=True
        )

        self.logger.info(
            f"Added interval job: task_name={task_name}, "
            f"interval={weeks}w {days}d {hours}h {minutes}m {seconds}s"
        )

    def add_cron(
        self,
        task_name: str,
        data_fn: Callable[[], Dict[str, Any]],
        priority: int = 0,
        year: Optional[int] = None,
        month: Optional[int] = None,
        day: Optional[int] = None,
        week: Optional[int] = None,
        day_of_week: Optional[int] = None,
        hour: Optional[int] = None,
        minute: Optional[int] = None,
        second: Optional[int] = None,
        job_id: Optional[str] = None
    ):
        """
        添加 cron 定时任务

        Args:
            task_name: 任务名称
            data_fn: 返回任务数据的函数
            priority: 优先级（默认 0）
            year: 年份 (4 位数字)
            month: 月份 (1-12)
            day: 日期 (1-31)
            week: 周数 (1-53)
            day_of_week: 星期几 (0-6, 0=周一)
            hour: 小时 (0-23)
            minute: 分钟 (0-59)
            second: 秒 (0-59)
            job_id: 任务 ID（可选）

        Example:
            # 每天凌晨 2 点
            cron.add_cron(
                task_name="daily_report",
                data_fn=lambda: {"type": "daily"},
                hour=2,
                minute=0
            )

            # 每周一上午 9 点
            cron.add_cron(
                task_name="weekly_report",
                data_fn=lambda: {"type": "weekly"},
                day_of_week=0,
                hour=9,
                minute=0
            )

            # 每小时的第 15 分钟
            cron.add_cron(
                task_name="hourly_sync",
                data_fn=lambda: {"type": "sync"},
                minute=15
            )
        """
        trigger = CronTrigger(
            year=year,
            month=month,
            day=day,
            week=week,
            day_of_week=day_of_week,
            hour=hour,
            minute=minute,
            second=second
        )

        self._scheduler.add_job(
            func=self._submit_task,
            trigger=trigger,
            args=(task_name, data_fn, priority),
            id=job_id,
            replace_existing=True
        )

        self.logger.info(f"Added cron job: task_name={task_name}, trigger={trigger}")

    def add_date(
        self,
        task_name: str,
        data_fn: Callable[[], Dict[str, Any]],
        run_date: str,
        priority: int = 0,
        job_id: Optional[str] = None
    ):
        """
        添加一次性任务

        Args:
            task_name: 任务名称
            data_fn: 返回任务数据的函数
            run_date: 执行时间（ISO 格式字符串，如 "2024-12-31 23:59:59"）
            priority: 优先级（默认 0）
            job_id: 任务 ID（可选）

        Example:
            # 在指定时间执行一次
            cron.add_date(
                task_name="special_task",
                data_fn=lambda: {"type": "special"},
                run_date="2024-12-31 23:59:59"
            )
        """
        trigger = DateTrigger(run_date=run_date)

        self._scheduler.add_job(
            func=self._submit_task,
            trigger=trigger,
            args=(task_name, data_fn, priority),
            id=job_id,
            replace_existing=True
        )

        self.logger.info(f"Added date job: task_name={task_name}, run_date={run_date}")

    def remove_job(self, job_id: str):
        """
        删除定时任务

        Args:
            job_id: 任务 ID

        Example:
            cron.remove_job("heartbeat_job")
        """
        self._scheduler.remove_job(job_id)
        self.logger.info(f"Removed job: job_id={job_id}")

    def start(self):
        """启动调度器"""
        self._scheduler.start()
        self.logger.info("Cron scheduler started")

    async def stop(self):
        """停止调度器"""
        self._scheduler.shutdown()
        self.logger.info("Cron scheduler stopped")

    async def _submit_task(
        self,
        task_name: str,
        data_fn: Callable[[], Dict[str, Any]],
        priority: int
    ):
        """
        内部方法：提交任务到队列

        Args:
            task_name: 任务名称
            data_fn: 返回任务数据的函数
            priority: 优先级
        """
        try:
            data = data_fn()
            task_id = await self._queue.push(
                data=data,
                task_name=task_name,
                priority=priority
            )
            self.logger.info(
                f"Submitted task: task_name={task_name}, task_id={task_id}, priority={priority}"
            )
        except Exception as e:
            self.logger.error(f"Failed to submit task: task_name={task_name}, error={e}")
