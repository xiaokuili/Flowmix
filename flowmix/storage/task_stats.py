"""
TaskStats - 任务统计查询接口

支持多种后端实现（SQLite、Redis、PostgreSQL）
提供任务信息查询、任务树统计、Worker 性能分析等功能
"""

import logging
from typing import Optional, Dict, Any, List
from datetime import datetime

from .stats import Stats, SQLiteStats


class TaskStats:
    """
    任务统计查询接口

    职责：
    - 查询任务详细信息
    - 统计任务树状态
    - 分析 Worker 性能
    - 监控任务执行情况

    特点：
    - 支持多种后端（SQLite、Redis、PostgreSQL）
    - 完全独立，不依赖队列实现
    - 提供丰富的统计维度

    Example:
        # 使用默认 SQLite 后端
        stats = TaskStats(db_path=".flowmix/flowmix.db")

        # 使用 Redis 后端（未来）
        from flowmix.storage.stats import RedisStats
        stats = TaskStats(provider=RedisStats(redis_url="redis://localhost:6379/0"))

        # 查询任务信息
        info = stats.get_task_info(task_id=123)
        print(f"状态: {info['status']}")

        # 查询任务树统计
        tree_stats = stats.get_task_tree_stats(root_id=100)
        progress = tree_stats['completed'] / tree_stats['total'] * 100
        print(f"进度: {progress:.1f}%")

        # 查询 Worker 统计
        worker_stats = stats.get_worker_stats()
        print(f"总执行: {worker_stats['total']}, 成功率: {worker_stats['success_rate']*100:.1f}%")
    """

    def __init__(
        self,
        provider: Optional[Stats] = None,
        db_path: str = ".flowmix/flowmix.db",
        queue_name: str = "tasks",
    ):
        """
        初始化 TaskStats

        Args:
            provider: 统计后端实例（可选）
                     - 如果不指定，默认使用 SQLiteStats
                     - 可以传入 RedisStats、PostgreSQLStats 等
            db_path: SQLite 数据库文件路径（仅当 provider=None 时使用）
            queue_name: 队列名称（仅当 provider=None 时使用）
        """
        if provider is None:
            # 默认使用 SQLite 后端
            self._provider = SQLiteStats(
                db_path=db_path,
                queue_name=queue_name
            )
        else:
            self._provider = provider

        self.logger = logging.getLogger(__name__)
        self.logger.info(f"TaskStats initialized with provider: {self._provider.__class__.__name__}")

    def get_task_info(self, task_id: int) -> Optional[Dict[str, Any]]:
        """
        获取任务详细信息

        Args:
            task_id: 任务 ID

        Returns:
            任务信息字典，如果不存在返回 None

        Example:
            info = stats.get_task_info(123)
            if info:
                print(f"状态: {info['status']}")
                print(f"数据: {info['data']}")
        """
        return self._provider.get_task_info(task_id)

    def get_children(self, parent_id: int) -> List[Dict[str, Any]]:
        """
        获取所有直接子任务

        Args:
            parent_id: 父任务 ID

        Returns:
            子任务列表

        Example:
            children = stats.get_children(100)
            for child in children:
                print(f"子任务 {child['id']}: {child['status']}")
        """
        return self._provider.get_children(parent_id)

    def get_task_tree_stats(self, root_id: int) -> Dict[str, int]:
        """
        获取任务树的统计信息（递归统计所有子孙任务）

        从指定根任务开始，递归统计整棵任务树的状态分布

        Args:
            root_id: 根任务 ID

        Returns:
            统计字典，包含：
            - total: 总任务数（包括根任务和所有子孙任务）
            - pending: 待处理任务数
            - processing: 处理中任务数
            - completed: 已完成任务数
            - failed: 失败任务数

        Example:
            # 查询某个爬虫任务树的进度
            stats = stats.get_task_tree_stats(root_id=100)
            progress = stats['completed'] / stats['total'] * 100
            print(f"进度: {progress:.1f}%")
            print(f"成功: {stats['completed']}, 失败: {stats['failed']}")
        """
        return self._provider.get_task_tree_stats(root_id)

    def get_task_tree_details(self, root_id: int) -> List[Dict[str, Any]]:
        """
        获取任务树的详细信息（递归查询所有子孙任务）

        Args:
            root_id: 根任务 ID

        Returns:
            任务列表（按 ID 顺序），每个任务包含完整信息

        Example:
            details = stats.get_task_tree_details(100)
            for task in details:
                print(f"[{task['id']}] {task['task_name']}: {task['status']}")
        """
        return self._provider.get_task_tree_details(root_id)

    def get_worker_stats(
        self,
        worker_id: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        查询 Worker 执行统计（支持多维度筛选）

        Args:
            worker_id: Worker ID（不指定则统计所有 Worker）
            start_time: 开始时间（筛选 created_at >= start_time 的任务）
            end_time: 结束时间（筛选 created_at <= end_time 的任务）

        Returns:
            统计字典，包含总任务数、完成数、失败数、成功率、吞吐量等

        Example:
            # 查询所有 Worker 的整体情况
            stats = stats.get_worker_stats()
            print(f"总执行: {stats['total']}, 成功率: {stats['success_rate']*100:.1f}%")

            # 查询某个 Worker 的执行情况
            stats = stats.get_worker_stats(worker_id='worker-1')

            # 查询今天的执行情况
            from datetime import datetime
            today_start = datetime.now().replace(hour=0, minute=0, second=0)
            stats = stats.get_worker_stats(start_time=today_start)
        """
        return self._provider.get_worker_stats(worker_id, start_time, end_time)

    def get_worker_stats_by_task_type(
        self,
        worker_id: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> Dict[str, Dict[str, Any]]:
        """
        按任务类型统计 Worker 执行情况

        Args:
            worker_id: Worker ID（不指定则统计所有 Worker）
            start_time: 开始时间
            end_time: 结束时间

        Returns:
            字典，key 为任务类型，value 为该类型的统计信息

        Example:
            # 查询今天各类型任务的执行情况
            stats = stats.get_worker_stats_by_task_type(start_time=today_start)
            for task_type, task_stats in stats.items():
                print(f"{task_type}: {task_stats['completed']}/{task_stats['total']}")
        """
        return self._provider.get_worker_stats_by_task_type(worker_id, start_time, end_time)

    def list_workers(
        self,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        active_threshold_seconds: int = 300
    ) -> List[Dict[str, Any]]:
        """
        列出所有 Worker

        Args:
            start_time: 开始时间
            end_time: 结束时间
            active_threshold_seconds: 判断 Worker 是否活跃的阈值（秒，默认 300 秒）

        Returns:
            Worker 列表

        Example:
            # 列出所有 Worker
            workers = stats.list_workers()
            for w in workers:
                status = "🟢 活跃" if w['is_active'] else "🔴 停止"
                print(f"{status} {w['worker_id']}: {w['completed']}/{w['total_tasks']}")
        """
        return self._provider.list_workers(start_time, end_time, active_threshold_seconds)

    def get_failed_tasks(
        self,
        worker_id: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        查询失败的任务

        Args:
            worker_id: Worker ID（不指定则查询所有 Worker）
            start_time: 开始时间
            end_time: 结束时间
            limit: 最多返回多少条

        Returns:
            失败任务列表

        Example:
            # 查询最近失败的任务
            failed = stats.get_failed_tasks(limit=10)
            for task in failed:
                print(f"任务 {task['task_id']} 失败: {task['error']}")

            # 查询某个 Worker 的失败任务
            failed = stats.get_failed_tasks(worker_id='worker-1')
        """
        return self._provider.get_failed_tasks(worker_id, start_time, end_time, limit)

    def get_error_summary(
        self,
        worker_id: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> Dict[str, int]:
        """
        错误汇总统计

        Args:
            worker_id: Worker ID（不指定则统计所有 Worker）
            start_time: 开始时间
            end_time: 结束时间

        Returns:
            字典，key 为错误信息，value 为出现次数

        Example:
            # 查询错误分布
            errors = stats.get_error_summary()
            for error, count in errors.items():
                print(f"{error}: {count} 次")
        """
        return self._provider.get_error_summary(worker_id, start_time, end_time)

    def get_processing_tasks(
        self,
        worker_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        获取正在处理的任务（用于实时监控）

        Args:
            worker_id: Worker ID（不指定则查询所有 Worker）

        Returns:
            正在处理的任务列表

        Example:
            # 查看正在执行的任务
            processing = stats.get_processing_tasks()
            for task in processing:
                print(f"Worker {task['worker_id']} 正在执行 {task['task_type']}")
        """
        return self._provider.get_processing_tasks(worker_id)

    def close(self):
        """关闭连接"""
        self._provider.close()
