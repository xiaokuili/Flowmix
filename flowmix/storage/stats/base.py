"""
Stats - 统计查询接口基类

定义所有统计后端必须实现的接口
支持任务信息查询、任务树统计、Worker 性能分析等
"""

from abc import ABC, abstractmethod
from typing import Optional, Dict, Any, List, TypedDict
from datetime import datetime


class TaskInfo(TypedDict, total=False):
    """任务详细信息"""
    id: int
    parent_id: Optional[int]
    task_name: Optional[str]
    data: Optional[Dict[str, Any]]
    priority: int
    status: str  # 'pending' | 'processing' | 'completed' | 'failed' | 'done'
    worker_id: Optional[str]  # 处理该任务的 Worker ID
    error: Optional[str]
    result: Any
    fingerprint: Optional[str]
    created_at: str  # ISO 格式时间字符串
    updated_at: str
    completed_at: Optional[str]


class TaskTreeStats(TypedDict):
    """任务树统计信息"""
    total: int
    pending: int
    processing: int
    completed: int
    failed: int


class WorkerStats(TypedDict, total=False):
    """Worker 执行统计"""
    worker_id: Optional[str]
    total: int
    completed: int
    failed: int
    pending: int
    processing: int
    success_rate: float  # 成功率 (0.0 - 1.0)
    qps: float  # 吞吐量 (tasks per second)
    avg_duration_seconds: float  # 平均执行时长
    start_time: Optional[str]  # ISO 格式时间字符串
    end_time: Optional[str]


class WorkerInfo(TypedDict):
    """Worker 信息"""
    worker_id: str
    total_tasks: int
    completed: int
    failed: int
    pending: int
    processing: int
    first_seen: str  # ISO 格式时间字符串
    last_seen: str
    is_active: bool


class FailedTask(TypedDict):
    """失败任务信息"""
    task_id: int
    worker_id: Optional[str]
    task_type: str
    data: Dict[str, Any]
    error: str
    created_at: str  # ISO 格式时间字符串
    failed_at: str


class ProcessingTask(TypedDict):
    """正在处理的任务"""
    task_id: int
    worker_id: Optional[str]
    task_type: str
    data: Dict[str, Any]
    started_at: str  # ISO 格式时间字符串
    duration_seconds: float


class Stats(ABC):
    """
    统计查询接口抽象基类

    定义所有统计后端必须实现的接口
    支持任务信息查询、任务树统计、Worker 性能分析等
    """

    @abstractmethod
    def get_task_info(self, task_id: int) -> Optional[TaskInfo]:
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
        pass

    @abstractmethod
    def get_task_tree_stats(self, root_id: int) -> TaskTreeStats:
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
            # 查询任务树进度
            s = stats.get_task_tree_stats(root_id=100)

            # 判断任务树是否完成（没有 pending 和 processing）
            is_done = s['pending'] == 0 and s['processing'] == 0

            # 计算完成进度
            progress = (s['completed'] + s['failed']) / s['total']
            print(f"进度: {progress * 100:.1f}%")

            # 计算成功率（仅已完成的任务）
            finished = s['completed'] + s['failed']
            success_rate = s['completed'] / finished if finished > 0 else 0.0
            print(f"成功率: {success_rate * 100:.1f}%")

            # 判断是否有失败
            has_failed = s['failed'] > 0
        """
        pass

    @abstractmethod
    def get_task_tree_details(self, root_id: int) -> List[TaskInfo]:
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
        pass

    @abstractmethod
    def get_worker_stats(
        self,
        worker_id: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> WorkerStats:
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
        """
        pass

    @abstractmethod
    def get_worker_stats_by_task_type(
        self,
        worker_id: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> Dict[str, WorkerStats]:
        """
        按任务类型统计 Worker 执行情况

        Args:
            worker_id: Worker ID（不指定则统计所有 Worker）
            start_time: 开始时间
            end_time: 结束时间

        Returns:
            字典，key 为任务类型，value 为该类型的统计信息

        Example:
            stats = stats.get_worker_stats_by_task_type()
            for task_type, task_stats in stats.items():
                print(f"{task_type}: {task_stats['completed']}/{task_stats['total']}")
        """
        pass

    @abstractmethod
    def list_workers(
        self,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        active_threshold_seconds: int = 300
    ) -> List[WorkerInfo]:
        """
        列出所有 Worker

        Args:
            start_time: 开始时间
            end_time: 结束时间
            active_threshold_seconds: 判断 Worker 是否活跃的阈值（秒）

        Returns:
            Worker 列表

        Example:
            workers = stats.list_workers()
            for w in workers:
                status = "🟢 活跃" if w['is_active'] else "🔴 停止"
                print(f"{status} {w['worker_id']}: {w['completed']}/{w['total_tasks']}")
        """
        pass

    @abstractmethod
    def get_failed_tasks(
        self,
        worker_id: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 100
    ) -> List[FailedTask]:
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
            failed = stats.get_failed_tasks(limit=10)
            for task in failed:
                print(f"任务 {task['task_id']} 失败: {task['error']}")
        """
        pass

    @abstractmethod
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
            errors = stats.get_error_summary()
            for error, count in errors.items():
                print(f"{error}: {count} 次")
        """
        pass

    @abstractmethod
    def get_processing_tasks(
        self,
        worker_id: Optional[str] = None
    ) -> List[ProcessingTask]:
        """
        获取正在处理的任务（用于实时监控）

        Args:
            worker_id: Worker ID（不指定则查询所有 Worker）

        Returns:
            正在处理的任务列表

        Example:
            processing = stats.get_processing_tasks()
            for task in processing:
                print(f"Worker {task['worker_id']} 正在执行 {task['task_type']}")
        """
        pass

    @abstractmethod
    def close(self):
        """关闭连接或释放资源"""
        pass
