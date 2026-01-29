"""
Stats - 统计查询接口基类

分层设计：
1. TaskQuery - 任务链查询（任务信息、任务链条统计）
2. RunnerStats - Runner 执行统计（Worker 性能、吞吐量）
3. MonitoringQuery - 实时监控（正在处理的任务、失败任务、错误汇总）
4. Stats - 统一门面（组合上述三个模块）
"""

from abc import ABC, abstractmethod
from typing import Optional, Dict, Any, List, TypedDict
from datetime import datetime


# ============================================================================
# TypedDict 定义
# ============================================================================

class TaskInfo(TypedDict, total=False):
    """任务详细信息"""
    id: int
    parent_id: Optional[int]
    task_name: Optional[str]
    data: Optional[Dict[str, Any]]
    priority: int
    status: str  # 'pending' | 'processing' | 'completed' | 'failed'
    chain_status: str  # 'pending' | 'processing' | 'completed' - 任务链完成状态
    worker_id: Optional[str]
    error: Optional[str]
    result: Any
    fingerprint: Optional[str]
    created_at: str
    updated_at: str
    completed_at: Optional[str]


class TaskChainSummary(TypedDict):
    """任务链统计摘要"""
    total: int
    pending: int
    processing: int
    completed: int
    failed: int


class ChainStats(TypedDict):
    """任务链维度统计（用于 Runner 统计）"""
    total_chains: int           # 总任务链数（根任务数）
    completed_chains: int       # 已完成的任务链数
    processing_chains: int      # 正在处理的任务链数
    success_chains: int         # 完全成功的任务链数（完成且无失败）
    partial_failed_chains: int  # 部分失败的任务链数（完成但有失败）


class WorkerPerformance(TypedDict, total=False):
    """Worker 性能统计"""
    worker_id: Optional[str]
    total: int
    completed: int
    failed: int
    pending: int
    processing: int
    success_rate: float
    qps: float
    avg_duration_seconds: float
    start_time: Optional[str]
    end_time: Optional[str]


class WorkerInfo(TypedDict):
    """Worker 基本信息"""
    worker_id: str
    total_tasks: int
    completed: int
    failed: int
    pending: int
    processing: int
    first_seen: str
    last_seen: str
    is_active: bool


class FailedTask(TypedDict):
    """失败任务信息"""
    task_id: int
    worker_id: Optional[str]
    task_type: str
    data: Dict[str, Any]
    error: str
    created_at: str
    failed_at: str


class ProcessingTask(TypedDict):
    """正在处理的任务"""
    task_id: int
    worker_id: Optional[str]
    task_type: str
    data: Dict[str, Any]
    started_at: str
    duration_seconds: float


# ============================================================================
# 第一层：TaskQuery - 任务链查询
# ============================================================================

class TaskQuery(ABC):
    """
    任务链查询接口

    职责：
    - 查询单个任务的详细信息
    - 判断任务链是否执行完成
    - 查询任务链（树）的统计摘要
    - 查询任务链（树）的详细信息列表

    Example:
        # 查询单个任务
        task = await task_query.get_task(task_id=123)
        print(f"状态: {task['status']}")

        # 判断任务链是否完成
        is_done = await task_query.is_chain_completed(root_id=100)
        print(f"任务链是否完成: {is_done}")

        # 查询任务链摘要
        summary = await task_query.get_chain_summary(root_id=100)
        progress = (summary['completed'] + summary['failed']) / summary['total']
        print(f"进度: {progress * 100:.1f}%")

        # 查询任务链详情
        details = await task_query.get_chain_details(root_id=100)
        for task in details:
            print(f"[{task['id']}] {task['task_name']}: {task['status']}")
    """

    @abstractmethod
    async def get_task(self, task_id: int) -> Optional[TaskInfo]:
        """
        获取单个任务的详细信息

        Args:
            task_id: 任务 ID

        Returns:
            任务信息，不存在则返回 None
        """
        pass

    @abstractmethod
    async def is_chain_completed(self, root_id: int) -> bool:
        """
        判断任务链是否已经执行完成

        当任务链中没有 pending 和 processing 状态的任务时，认为任务链已完成
        （注意：完成不等于成功，可能包含失败的任务）

        Args:
            root_id: 根任务 ID

        Returns:
            True 表示任务链已完成，False 表示还有任务未完成

        Example:
            # 等待任务链完成
            while not await task_query.is_chain_completed(root_id=100):
                await asyncio.sleep(1)
            print("任务链执行完成！")

            # 检查是否全部成功
            summary = await task_query.get_chain_summary(root_id=100)
            if summary['failed'] == 0:
                print("全部成功！")
            else:
                print(f"有 {summary['failed']} 个任务失败")
        """
        pass

    @abstractmethod
    async def get_chain_summary(self, root_id: int) -> TaskChainSummary:
        """
        获取任务链的统计摘要（递归统计所有子孙任务）

        Args:
            root_id: 根任务 ID

        Returns:
            统计摘要，包含总数和各状态的数量

        Example:
            summary = await task_query.get_chain_summary(root_id=100)

            # 判断任务链是否完成
            is_done = summary['pending'] == 0 and summary['processing'] == 0

            # 计算完成进度
            progress = (summary['completed'] + summary['failed']) / summary['total']

            # 计算成功率
            finished = summary['completed'] + summary['failed']
            success_rate = summary['completed'] / finished if finished > 0 else 0.0
        """
        pass

    @abstractmethod
    async def get_chain_details(self, root_id: int) -> List[TaskInfo]:
        """
        获取任务链的详细信息列表（递归查询所有子孙任务）

        Args:
            root_id: 根任务 ID

        Returns:
            任务列表（按 ID 排序）
        """
        pass


# ============================================================================
# 第二层：RunnerStats - Runner 执行统计
# ============================================================================

class RunnerStats(ABC):
    """
    Runner 执行统计接口

    职责：
    - 查询 Worker 的性能统计（成功率、吞吐量、平均执行时长）
    - 按任务类型统计 Worker 执行情况
    - 查询任务链维度的统计（任务链完成情况）
    - 列出所有 Worker 的基本信息

    Example:
        # 查询所有 Worker 的整体性能
        perf = await runner_stats.get_performance()
        print(f"成功率: {perf['success_rate']*100:.1f}%")
        print(f"吞吐量: {perf['qps']:.2f} tasks/s")

        # 查询特定 Worker 的性能
        worker_perf = await runner_stats.get_performance(worker_id='worker-1')

        # 查询任务链维度统计
        chain_stats = await runner_stats.get_chain_stats()
        print(f"总任务链: {chain_stats['total_chains']}")
        print(f"已完成: {chain_stats['completed_chains']}")
        print(f"全部成功: {chain_stats['success_chains']}")

        # 按任务类型统计
        by_type = await runner_stats.get_performance_by_task_type()
        for task_type, stats in by_type.items():
            print(f"{task_type}: {stats['completed']}/{stats['total']}")

        # 列出所有 Worker
        workers = await runner_stats.list_workers()
        for w in workers:
            status = "🟢" if w['is_active'] else "🔴"
            print(f"{status} {w['worker_id']}: {w['completed']}/{w['total_tasks']}")
    """

    @abstractmethod
    async def get_performance(
        self,
        worker_id: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> WorkerPerformance:
        """
        查询 Worker 性能统计

        Args:
            worker_id: Worker ID（不指定则统计所有 Worker）
            start_time: 开始时间（筛选 created_at >= start_time 的任务）
            end_time: 结束时间（筛选 created_at <= end_time 的任务）

        Returns:
            性能统计，包含总任务数、完成数、失败数、成功率、吞吐量、平均执行时长
        """
        pass

    @abstractmethod
    async def get_chain_stats(
        self,
        worker_id: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> ChainStats:
        """
        查询任务链维度的统计

        Args:
            worker_id: Worker ID（不指定则统计所有 Worker）
            start_time: 开始时间（筛选根任务 created_at >= start_time）
            end_time: 结束时间（筛选根任务 created_at <= end_time）

        Returns:
            任务链统计，包含：
            - total_chains: 总任务链数（根任务数，parent_id 为 None 的任务）
            - completed_chains: 已完成的任务链数（链中无 pending/processing）
            - processing_chains: 正在处理的任务链数（链中有 pending/processing）
            - success_chains: 完全成功的任务链数（已完成且无失败任务）
            - partial_failed_chains: 部分失败的任务链数（已完成但有失败任务）

        Example:
            # 查询所有任务链统计
            chain_stats = await runner_stats.get_chain_stats()
            print(f"总任务链: {chain_stats['total_chains']}")
            print(f"已完成: {chain_stats['completed_chains']}")
            print(f"正在处理: {chain_stats['processing_chains']}")
            print(f"全部成功: {chain_stats['success_chains']}")
            print(f"部分失败: {chain_stats['partial_failed_chains']}")

            # 计算完成率
            if chain_stats['total_chains'] > 0:
                completion_rate = chain_stats['completed_chains'] / chain_stats['total_chains']
                print(f"完成率: {completion_rate * 100:.1f}%")

            # 计算成功率（在已完成的链中）
            if chain_stats['completed_chains'] > 0:
                success_rate = chain_stats['success_chains'] / chain_stats['completed_chains']
                print(f"成功率: {success_rate * 100:.1f}%")
        """
        pass

    @abstractmethod
    async def get_performance_by_task_type(
        self,
        worker_id: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> Dict[str, WorkerPerformance]:
        """
        按任务类型统计 Worker 性能

        Args:
            worker_id: Worker ID（不指定则统计所有 Worker）
            start_time: 开始时间
            end_time: 结束时间

        Returns:
            字典，key 为任务类型，value 为该类型的性能统计
        """
        pass

    @abstractmethod
    async def list_workers(
        self,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        active_threshold_seconds: int = 300
    ) -> List[WorkerInfo]:
        """
        列出所有 Worker 的基本信息

        Args:
            start_time: 开始时间
            end_time: 结束时间
            active_threshold_seconds: 判断 Worker 是否活跃的阈值（秒）

        Returns:
            Worker 列表
        """
        pass


class Stats(ABC):
    """
    统计查询统一门面

    组合了三个专注的查询接口：
    - task: TaskQuery - 任务链查询
    - runner: RunnerStats - Runner 执行统计

    Example:
        # 创建 Stats
        stats = RedisStats(redis=redis, queue_name="tasks")

        # 任务链查询
        task = await stats.task.get_task(123)
        summary = await stats.task.get_chain_summary(100)
        details = await stats.task.get_chain_details(100)

        # Runner 统计
        perf = await stats.runner.get_performance()
        workers = await stats.runner.list_workers()

  
    """

    @property
    @abstractmethod
    def task(self) -> TaskQuery:
        """任务链查询"""
        pass

    @property
    @abstractmethod
    def runner(self) -> RunnerStats:
        """Runner 执行统计"""
        pass

 

    @abstractmethod
    async def close(self):
        """关闭连接或释放资源"""
        pass
