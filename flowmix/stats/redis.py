"""
RedisStats - 基于 Redis 的统计查询实现

实现分层架构：
- RedisTaskQuery - 任务链查询实现
- RedisRunnerStats - Runner 执行统计实现
- RedisMonitoringQuery - 实时监控实现
- RedisStats - 统一门面
"""

import json
import logging
import time
from typing import Optional, Dict, Any, List
from datetime import datetime

import redis.asyncio as aioredis

from .base import (
    Stats,
    TaskQuery,
    RunnerStats,
    TaskInfo,
    TaskChainSummary,
    ChainStats,
    WorkerPerformance,
    WorkerInfo,
)


# ============================================================================
# 内部辅助类 - 共享数据访问和工具方法
# ============================================================================

class RedisDataAccess:
    """Redis 数据访问辅助类（内部使用）"""

    def __init__(self, redis: aioredis.Redis, queue_name: str):
        self._redis = redis
        self.queue_name = queue_name
        self.logger = logging.getLogger(__name__)

    def _get_key(self, suffix: str) -> str:
        """生成 Redis key"""
        return f"{self.queue_name}:{suffix}"

    async def get_all_messages(self) -> Dict[int, Dict[str, Any]]:
        """获取所有消息并解析"""
        messages = await self._redis.hgetall(self._get_key("messages"))

        result = {}
        for msg_id, msg_json in messages.items():
            try:
                message = json.loads(msg_json)
                result[int(msg_id)] = message
            except (json.JSONDecodeError, ValueError) as e:
                self.logger.warning(f"Failed to parse message {msg_id}: {e}")
                continue

        return result

    def message_to_task_info(self, message: Dict[str, Any]) -> TaskInfo:
        """将消息转换为 TaskInfo"""
        return {
            'id': message.get('id'),
            'parent_id': message.get('parent_id'),
            'task_name': message.get('task_name'),
            'data': self._parse_json_field(message.get('data', '{}')),
            'priority': message.get('priority', 0),
            'status': message.get('status', 'pending'),
            'chain_status': message.get('chain_status', 'pending'),
            'worker_id': message.get('consumer'),
            'error': message.get('error'),
            'result': self._parse_json_field(message.get('result')),
            'fingerprint': message.get('fingerprint'),
            'created_at': self._format_timestamp(message.get('created_at')),
            'updated_at': self._format_timestamp(message.get('updated_at')),
            'completed_at': self._format_timestamp(message.get('completed_at'))
        }

    @staticmethod
    def _parse_json_field(value: Any) -> Any:
        """解析 JSON 字段"""
        if isinstance(value, str):
            try:
                return json.loads(value)
            except (json.JSONDecodeError, ValueError):
                return value
        return value

    @staticmethod
    def _format_timestamp(timestamp: Optional[float]) -> Optional[str]:
        """将时间戳转换为 ISO 格式字符串"""
        if timestamp is None:
            return None
        dt = datetime.fromtimestamp(timestamp)
        return dt.strftime('%Y-%m-%d %H:%M:%S')


# ============================================================================
# 第一层实现：RedisTaskQuery - 任务链查询
# ============================================================================

class RedisTaskQuery(TaskQuery):
    """Redis 任务链查询实现"""

    def __init__(self, data_access: RedisDataAccess):
        self._data = data_access

    async def get_task(self, task_id: int) -> Optional[TaskInfo]:
        """获取单个任务的详细信息"""
        msg_json = await self._data._redis.hget(
            self._data._get_key("messages"), task_id
        )

        if not msg_json:
            return None

        try:
            message = json.loads(msg_json)
            return self._data.message_to_task_info(message)
        except json.JSONDecodeError as e:
            self._data.logger.warning(f"Failed to parse message {task_id}: {e}")
            return None

    async def is_chain_completed(self, root_id: int) -> bool:
        """
        判断任务链是否已经执行完成

        使用 chain_status 判断，chain_status == 'completed' 表示任务链已完成
        """
        task = await self.get_task(root_id)
        if not task:
            return False
        return task.get('chain_status') == 'completed'

    async def get_chain_summary(self, root_id: int) -> TaskChainSummary:
        """
        获取任务链的统计摘要（递归统计所有子孙任务）

        返回的是任务本身的状态分布（status），而不是 chain_status
        """
        messages = await self._data.get_all_messages()

        # 递归获取所有子孙任务 ID
        def get_descendants(task_id: int) -> List[int]:
            result = [task_id]
            for msg_id, message in messages.items():
                if message.get('parent_id') == task_id:
                    result.extend(get_descendants(msg_id))
            return result

        task_ids = get_descendants(root_id)

        # 统计各状态数量（任务本身的状态）
        stats: TaskChainSummary = {
            'total': 0,
            'pending': 0,
            'processing': 0,
            'completed': 0,
            'failed': 0
        }

        for task_id in task_ids:
            if task_id in messages:
                stats['total'] += 1
                status = messages[task_id].get('status', 'pending')
                if status in stats:
                    stats[status] += 1

        return stats

    async def get_chain_details(self, root_id: int) -> List[TaskInfo]:
        """获取任务链的详细信息列表（递归查询所有子孙任务）"""
        messages = await self._data.get_all_messages()

        # 递归获取所有子孙任务
        def get_descendants(task_id: int) -> List[int]:
            result = [task_id]
            for msg_id, message in messages.items():
                if message.get('parent_id') == task_id:
                    result.extend(get_descendants(msg_id))
            return result

        task_ids = get_descendants(root_id)

        # 返回任务详情列表
        tasks = []
        for task_id in sorted(task_ids):
            if task_id in messages:
                tasks.append(self._data.message_to_task_info(messages[task_id]))

        return tasks


# ============================================================================
# 第二层实现：RedisRunnerStats - Runner 执行统计
# ============================================================================

class RedisRunnerStats(RunnerStats):
    """Redis Runner 执行统计实现"""

    def __init__(self, data_access: RedisDataAccess):
        self._data = data_access

    async def get_performance(
        self,
        worker_id: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> WorkerPerformance:
        """查询 Worker 性能统计"""
        messages = await self._data.get_all_messages()

        # 过滤消息
        filtered = self._filter_messages(
            messages.values(), worker_id, start_time, end_time
        )

        # 计算统计指标
        return self._compute_performance(filtered, worker_id, start_time, end_time)

    async def get_chain_stats(
        self,
        worker_id: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> ChainStats:
        """查询任务链维度的统计"""
        messages = await self._data.get_all_messages()

        # 找到所有根任务（parent_id 为 None）
        root_tasks = []
        for message in messages.values():
            if message.get('parent_id') is None:
                # 过滤时间范围
                created_at = message.get('created_at')
                if created_at:
                    if start_time and created_at < start_time.timestamp():
                        continue
                    if end_time and created_at > end_time.timestamp():
                        continue
                root_tasks.append(message)

        # 统计任务链
        total_chains = len(root_tasks)
        completed_chains = 0
        processing_chains = 0
        success_chains = 0
        partial_failed_chains = 0

        for root_task in root_tasks:
            root_id = root_task.get('id')

            # 获取该任务链的所有任务
            chain_tasks = self._get_chain_tasks(messages, root_id)

            # 过滤 worker_id（如果指定）
            if worker_id:
                chain_tasks = [t for t in chain_tasks if t.get('consumer') == worker_id]
                if not chain_tasks:  # 该链没有被指定 worker 处理的任务
                    total_chains -= 1
                    continue

            # 检查任务链状态（使用 chain_status 判断）
            root_chain_status = root_task.get('chain_status', 'pending')
            has_failed = any(t.get('status') == 'failed' for t in chain_tasks)

            if root_chain_status == 'completed':
                completed_chains += 1
                if has_failed:
                    partial_failed_chains += 1
                else:
                    success_chains += 1
            else:
                processing_chains += 1

        return {
            'total_chains': total_chains,
            'completed_chains': completed_chains,
            'processing_chains': processing_chains,
            'success_chains': success_chains,
            'partial_failed_chains': partial_failed_chains
        }

    def _get_chain_tasks(
        self,
        messages: Dict[int, Dict[str, Any]],
        root_id: int
    ) -> List[Dict[str, Any]]:
        """获取任务链的所有任务（递归）"""
        def get_descendants(task_id: int) -> List[Dict[str, Any]]:
            result = []
            if task_id in messages:
                result.append(messages[task_id])
            for msg_id, message in messages.items():
                if message.get('parent_id') == task_id:
                    result.extend(get_descendants(msg_id))
            return result

        return get_descendants(root_id)

    async def get_performance_by_task_type(
        self,
        worker_id: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> Dict[str, WorkerPerformance]:
        """按任务类型统计 Worker 性能"""
        messages = await self._data.get_all_messages()

        # 按任务类型分组
        grouped: Dict[str, List[Dict[str, Any]]] = {}

        for message in messages.values():
            # 过滤 worker_id 和时间
            if not self._match_filters(message, worker_id, start_time, end_time):
                continue

            task_name = message.get('task_name') or 'unknown'
            if task_name not in grouped:
                grouped[task_name] = []
            grouped[task_name].append(message)

        # 对每种任务类型进行统计
        result = {}
        for task_name, task_messages in grouped.items():
            result[task_name] = self._compute_performance(task_messages)

        return result

    async def list_workers(
        self,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        active_threshold_seconds: int = 300
    ) -> List[WorkerInfo]:
        """列出所有 Worker 的基本信息"""
        messages = await self._data.get_all_messages()

        # 按 worker 分组
        workers: Dict[str, List[Dict[str, Any]]] = {}

        for message in messages.values():
            consumer = message.get('consumer')
            if not consumer:
                continue

            # 过滤时间范围
            created_at = message.get('created_at')
            if created_at:
                if start_time and created_at < start_time.timestamp():
                    continue
                if end_time and created_at > end_time.timestamp():
                    continue

            if consumer not in workers:
                workers[consumer] = []
            workers[consumer].append(message)

        # 统计每个 worker
        result = []
        now = time.time()

        for worker_id, worker_messages in workers.items():
            total_tasks = len(worker_messages)
            pending = sum(1 for m in worker_messages if m.get('status') == 'pending')
            processing = sum(1 for m in worker_messages if m.get('status') == 'processing')
            completed = sum(1 for m in worker_messages if m.get('status') == 'completed')
            failed = sum(1 for m in worker_messages if m.get('status') == 'failed')

            # 首次/最后活跃时间
            created_times = [m['created_at'] for m in worker_messages if m.get('created_at')]
            updated_times = [m['updated_at'] for m in worker_messages if m.get('updated_at')]

            first_seen = min(created_times) if created_times else None
            last_seen = max(updated_times) if updated_times else None

            # 判断是否活跃
            is_active = False
            if last_seen:
                is_active = (now - last_seen) < active_threshold_seconds

            result.append({
                'worker_id': worker_id,
                'total_tasks': total_tasks,
                'completed': completed,
                'failed': failed,
                'pending': pending,
                'processing': processing,
                'first_seen': self._data._format_timestamp(first_seen),
                'last_seen': self._data._format_timestamp(last_seen),
                'is_active': is_active
            })

        # 按最后活跃时间降序排序
        result.sort(key=lambda x: x['last_seen'] or '', reverse=True)

        return result

    # 辅助方法
    def _filter_messages(
        self,
        messages: Any,
        worker_id: Optional[str],
        start_time: Optional[datetime],
        end_time: Optional[datetime]
    ) -> List[Dict[str, Any]]:
        """过滤消息"""
        filtered = []
        for message in messages:
            if self._match_filters(message, worker_id, start_time, end_time):
                filtered.append(message)
        return filtered

    def _match_filters(
        self,
        message: Dict[str, Any],
        worker_id: Optional[str],
        start_time: Optional[datetime],
        end_time: Optional[datetime]
    ) -> bool:
        """检查消息是否匹配过滤条件"""
        # 过滤 worker_id
        if worker_id and message.get('consumer') != worker_id:
            return False

        # 过滤时间范围
        created_at = message.get('created_at')
        if created_at:
            if start_time and created_at < start_time.timestamp():
                return False
            if end_time and created_at > end_time.timestamp():
                return False

        return True

    def _compute_performance(
        self,
        messages: List[Dict[str, Any]],
        worker_id: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> WorkerPerformance:
        """计算性能统计"""
        total = len(messages)
        pending = sum(1 for m in messages if m.get('status') == 'pending')
        processing = sum(1 for m in messages if m.get('status') == 'processing')
        completed = sum(1 for m in messages if m.get('status') == 'completed')
        failed = sum(1 for m in messages if m.get('status') == 'failed')

        # 计算成功率
        finished = completed + failed
        success_rate = completed / finished if finished > 0 else 0.0

        # 计算平均执行时长
        durations = []
        for m in messages:
            if m.get('status') in ('completed', 'failed') and m.get('completed_at') and m.get('created_at'):
                duration = m['completed_at'] - m['created_at']
                durations.append(duration)

        avg_duration = sum(durations) / len(durations) if durations else 0.0

        # 计算 QPS
        qps = 0.0
        if messages:
            timestamps = [m['created_at'] for m in messages if m.get('created_at')]
            if timestamps:
                first_time = min(timestamps)
                completed_times = [m['completed_at'] for m in messages
                                 if m.get('status') in ('completed', 'failed') and m.get('completed_at')]
                if completed_times:
                    last_time = max(completed_times)
                    duration = last_time - first_time
                    if duration > 0:
                        qps = finished / duration

        result: WorkerPerformance = {
            'total': total,
            'completed': completed,
            'failed': failed,
            'pending': pending,
            'processing': processing,
            'success_rate': round(success_rate, 4),
            'qps': round(qps, 2),
            'avg_duration_seconds': round(avg_duration, 2),
        }

        if worker_id:
            result['worker_id'] = worker_id

        if start_time:
            result['start_time'] = start_time.isoformat()

        if end_time:
            result['end_time'] = end_time.isoformat()

        return result



# ============================================================================
# 统一门面：RedisStats
# ============================================================================

class RedisStats(Stats):
    """
    Redis 统计查询实现（统一门面）

    使用分层架构，组合了：
    - task: RedisTaskQuery - 任务链查询
    - runner: RedisRunnerStats - Runner 执行统计
    - monitor: RedisMonitoringQuery - 实时监控

    Example:
        # 使用 Redis URL 初始化
        stats = RedisStats(redis_url="redis://localhost:6379/0", queue_name="tasks")

        # 任务链查询
        task = await stats.task.get_task(123)
        summary = await stats.task.get_chain_summary(100)
        details = await stats.task.get_chain_details(100)

        # Runner 统计
        perf = await stats.runner.get_performance()
        print(f"成功率: {perf['success_rate']*100:.1f}%")
        print(f"吞吐量: {perf['qps']:.2f} tasks/s")

        by_type = await stats.runner.get_performance_by_task_type()
        workers = await stats.runner.list_workers()

        # 实时监控
        processing = await stats.monitor.get_processing_tasks()
        failed = await stats.monitor.get_failed_tasks(limit=10)
        errors = await stats.monitor.get_error_summary()

        await stats.close()
    """

    def __init__(
        self,
        redis_url: str,
        queue_name: str = "tasks"
    ):
        """
        初始化 RedisStats

        Args:
            redis_url: Redis 连接 URL（如: redis://localhost:6379/0）
            queue_name: 队列名称（默认: tasks）
        """
        if not redis_url:
            raise ValueError("redis_url is required")

        # 从 URL 创建 Redis 连接池
        self._redis = aioredis.from_url(
            redis_url,
            encoding="utf-8",
            decode_responses=True
        )

        # 创建共享的数据访问对象
        self._data_access = RedisDataAccess(self._redis, queue_name)

        # 创建三个查询模块
        self._task_query = RedisTaskQuery(self._data_access)
        self._runner_stats = RedisRunnerStats(self._data_access)

        self.logger = logging.getLogger(__name__)
        self.logger.info(f"RedisStats initialized: redis_url={redis_url}, queue_name={queue_name}")

    @property
    def task(self) -> TaskQuery:
        """任务链查询"""
        return self._task_query

    @property
    def runner(self) -> RunnerStats:
        """Runner 执行统计"""
        return self._runner_stats


    async def close(self):
        """
        关闭统计（释放资源）

        会关闭 Redis 连接池
        """
        await self._redis.aclose()
        self.logger.info("RedisStats closed")
