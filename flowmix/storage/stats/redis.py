"""
RedisStats - 基于 Redis 的统计查询实现

查询 Worker 的执行统计（基于 Redis 数据库）
支持按 worker_id、时间范围、任务类型等多维度查询
"""

import json
import logging
from typing import Optional, Dict, Any, List
from datetime import datetime

from .base import (
    Stats,
    TaskInfo,
    TaskTreeStats,
    WorkerStats,
    WorkerInfo,
    FailedTask,
    ProcessingTask
)


class RedisStats(Stats):
    """
    Redis 统计查询实现

    查询 Worker 的执行统计和性能指标

    Example:
        import redis.asyncio as aioredis

        redis = await aioredis.from_url("redis://localhost:6379/0")
        stats = RedisStats(redis=redis, queue_name="tasks")

        # 查询所有 Worker 的整体执行情况
        overall = await stats.get_worker_stats()
        print(f"总执行: {overall['total']} 个任务, 成功率: {overall['success_rate']*100:.1f}%")
        print(f"吞吐量: {overall['qps']:.2f} tasks/s")

        # 查询今天的执行情况
        from datetime import datetime
        today = datetime.now().replace(hour=0, minute=0, second=0)
        today_stats = await stats.get_worker_stats(start_time=today)

        # 查询某个 Worker 的执行情况
        worker_stats = await stats.get_worker_stats(worker_id='worker-1')

        # 列出所有 Worker
        workers = await stats.list_workers()
        for w in workers:
            print(f"{w['worker_id']}: {w['completed']}/{w['total_tasks']}")

        # 或者使用 factory
        from flowmix.storage import create_redis_storage
        storage = await create_redis_storage()
        stats = storage.stats
    """

    def __init__(
        self,
        redis: Any,
        queue_name: str = "tasks"
    ):
        """
        初始化 RedisStats

        Args:
            redis: Redis 连接实例（必需）
            queue_name: 队列名称（默认: tasks）
        """
        if redis is None:
            raise ValueError("redis connection is required")

        self._redis = redis
        self.queue_name = queue_name

        self.logger = logging.getLogger(__name__)
        self.logger.info(f"RedisStats initialized: queue_name={queue_name}")

    def _get_key(self, suffix: str) -> str:
        """生成 Redis key"""
        return f"{self.queue_name}:{suffix}"

    async def _get_all_messages(self) -> Dict[int, Dict[str, Any]]:
        """获取所有消息并解析"""
        redis = self._redis
        messages = await redis.hgetall(self._get_key("messages"))

        result = {}
        for msg_id, msg_json in messages.items():
            try:
                message = json.loads(msg_json)
                result[int(msg_id)] = message
            except (json.JSONDecodeError, ValueError) as e:
                self.logger.warning(f"Failed to parse message {msg_id}: {e}")
                continue

        return result

    def _message_to_task_info(self, message: Dict[str, Any]) -> TaskInfo:
        """将消息转换为 TaskInfo"""
        return {
            'id': message.get('id'),
            'parent_id': message.get('parent_id'),
            'task_name': message.get('task_name'),
            'data': json.loads(message.get('data', '{}')) if isinstance(message.get('data'), str) else message.get('data'),
            'priority': message.get('priority', 0),
            'status': message.get('status', 'pending'),
            'worker_id': message.get('consumer'),  # 数据库字段名为 consumer，API 字段名为 worker_id
            'error': message.get('error'),
            'result': json.loads(message.get('result')) if isinstance(message.get('result'), str) else message.get('result'),
            'fingerprint': message.get('fingerprint'),
            'created_at': self._format_timestamp(message.get('created_at')),
            'updated_at': self._format_timestamp(message.get('updated_at')),
            'completed_at': self._format_timestamp(message.get('completed_at'))
        }

    def _format_timestamp(self, timestamp: Optional[float]) -> Optional[str]:
        """将时间戳转换为 ISO 格式字符串"""
        if timestamp is None:
            return None
        dt = datetime.fromtimestamp(timestamp)
        return dt.strftime('%Y-%m-%d %H:%M:%S')

    async def get_task_info(self, task_id: int) -> Optional[TaskInfo]:
        """获取任务详情"""
        redis = self._redis
        msg_json = await redis.hget(self._get_key("messages"), task_id)

        if not msg_json:
            return None

        try:
            message = json.loads(msg_json)
            return self._message_to_task_info(message)
        except json.JSONDecodeError as e:
            self.logger.warning(f"Failed to parse message {task_id}: {e}")
            return None

    async def get_children(self, parent_id: int) -> List[TaskInfo]:
        """获取所有直接子任务"""
        messages = await self._get_all_messages()

        children = []
        for msg_id, message in sorted(messages.items()):
            if message.get('parent_id') == parent_id:
                children.append(self._message_to_task_info(message))

        return children

    async def get_task_tree_stats(self, root_id: int) -> TaskTreeStats:
        """获取任务树的统计信息（递归查询所有子孙任务）"""
        messages = await self._get_all_messages()

        # 递归获取所有子孙任务 ID
        def get_descendants(task_id: int) -> List[int]:
            result = [task_id]
            for msg_id, message in messages.items():
                if message.get('parent_id') == task_id:
                    result.extend(get_descendants(msg_id))
            return result

        task_ids = get_descendants(root_id)

        # 统计各状态数量
        stats = {
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

    async def get_task_tree_details(self, root_id: int) -> List[TaskInfo]:
        """获取任务树的详细信息（递归查询所有子孙任务）"""
        messages = await self._get_all_messages()

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
                tasks.append(self._message_to_task_info(messages[task_id]))

        return tasks

    async def get_worker_stats(
        self,
        worker_id: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> WorkerStats:
        """查询 Worker 执行统计（支持多维度筛选）"""
        messages = await self._get_all_messages()

        # 过滤消息
        filtered = []
        for message in messages.values():
            # 过滤 worker_id
            if worker_id and message.get('consumer') != worker_id:
                continue

            # 过滤时间范围
            created_at = message.get('created_at')
            if created_at:
                if start_time and created_at < start_time.timestamp():
                    continue
                if end_time and created_at > end_time.timestamp():
                    continue

            filtered.append(message)

        # 统计
        total = len(filtered)
        pending = sum(1 for m in filtered if m.get('status') == 'pending')
        processing = sum(1 for m in filtered if m.get('status') == 'processing')
        completed = sum(1 for m in filtered if m.get('status') == 'completed')
        failed = sum(1 for m in filtered if m.get('status') == 'failed')

        # 计算成功率
        finished = completed + failed
        success_rate = completed / finished if finished > 0 else 0.0

        # 计算平均执行时长
        durations = []
        for m in filtered:
            if m.get('status') in ('completed', 'failed') and m.get('completed_at') and m.get('created_at'):
                duration = m['completed_at'] - m['created_at']
                durations.append(duration)

        avg_duration = sum(durations) / len(durations) if durations else 0.0

        # 计算 QPS
        qps = 0.0
        if filtered:
            timestamps = [m['created_at'] for m in filtered if m.get('created_at')]
            if timestamps:
                first_time = min(timestamps)
                completed_times = [m['completed_at'] for m in filtered
                                 if m.get('status') in ('completed', 'failed') and m.get('completed_at')]
                if completed_times:
                    last_time = max(completed_times)
                    duration = last_time - first_time
                    if duration > 0:
                        qps = finished / duration

        result: WorkerStats = {
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

    async def get_worker_stats_by_task_type(
        self,
        worker_id: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> Dict[str, WorkerStats]:
        """按任务类型统计 Worker 执行情况"""
        messages = await self._get_all_messages()

        # 按任务类型分组
        grouped: Dict[str, List[Dict[str, Any]]] = {}

        for message in messages.values():
            # 过滤 worker_id
            if worker_id and message.get('consumer') != worker_id:
                continue

            # 过滤时间范围
            created_at = message.get('created_at')
            if created_at:
                if start_time and created_at < start_time.timestamp():
                    continue
                if end_time and created_at > end_time.timestamp():
                    continue

            task_name = message.get('task_name') or 'unknown'
            if task_name not in grouped:
                grouped[task_name] = []
            grouped[task_name].append(message)

        # 对每种任务类型进行统计
        result = {}
        for task_name, task_messages in grouped.items():
            total = len(task_messages)
            pending = sum(1 for m in task_messages if m.get('status') == 'pending')
            processing = sum(1 for m in task_messages if m.get('status') == 'processing')
            completed = sum(1 for m in task_messages if m.get('status') == 'completed')
            failed = sum(1 for m in task_messages if m.get('status') == 'failed')

            finished = completed + failed
            success_rate = completed / finished if finished > 0 else 0.0

            # 平均执行时长
            durations = []
            for m in task_messages:
                if m.get('status') in ('completed', 'failed') and m.get('completed_at') and m.get('created_at'):
                    duration = m['completed_at'] - m['created_at']
                    durations.append(duration)

            avg_duration = sum(durations) / len(durations) if durations else 0.0

            # QPS
            qps = 0.0
            if task_messages:
                timestamps = [m['created_at'] for m in task_messages if m.get('created_at')]
                if timestamps:
                    first_time = min(timestamps)
                    completed_times = [m['completed_at'] for m in task_messages
                                     if m.get('status') in ('completed', 'failed') and m.get('completed_at')]
                    if completed_times:
                        last_time = max(completed_times)
                        duration = last_time - first_time
                        if duration > 0:
                            qps = finished / duration

            result[task_name] = {
                'total': total,
                'completed': completed,
                'failed': failed,
                'pending': pending,
                'processing': processing,
                'success_rate': round(success_rate, 4),
                'qps': round(qps, 2),
                'avg_duration_seconds': round(avg_duration, 2),
            }

        return result

    async def list_workers(
        self,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        active_threshold_seconds: int = 300
    ) -> List[WorkerInfo]:
        """列出所有活跃的 Worker"""
        import time

        messages = await self._get_all_messages()

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
                'first_seen': self._format_timestamp(first_seen),
                'last_seen': self._format_timestamp(last_seen),
                'is_active': is_active
            })

        # 按最后活跃时间降序排序
        result.sort(key=lambda x: x['last_seen'] or '', reverse=True)

        return result

    async def get_failed_tasks(
        self,
        worker_id: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 100
    ) -> List[FailedTask]:
        """查询失败的任务"""
        messages = await self._get_all_messages()

        failed_tasks = []

        for message in messages.values():
            if message.get('status') != 'failed':
                continue

            # 过滤 worker_id
            if worker_id and message.get('consumer') != worker_id:
                continue

            # 过滤时间范围
            created_at = message.get('created_at')
            if created_at:
                if start_time and created_at < start_time.timestamp():
                    continue
                if end_time and created_at > end_time.timestamp():
                    continue

            failed_tasks.append({
                'task_id': message.get('id'),
                'worker_id': message.get('consumer'),
                'task_type': message.get('task_name') or 'unknown',
                'data': json.loads(message.get('data', '{}')) if isinstance(message.get('data'), str) else message.get('data', {}),
                'error': message.get('error', ''),
                'created_at': self._format_timestamp(message.get('created_at')),
                'failed_at': self._format_timestamp(message.get('completed_at'))
            })

        # 按失败时间降序排序
        failed_tasks.sort(key=lambda x: x['failed_at'] or '', reverse=True)

        # 限制返回数量
        return failed_tasks[:limit]

    async def get_error_summary(
        self,
        worker_id: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> Dict[str, int]:
        """错误汇总统计"""
        messages = await self._get_all_messages()

        error_counts: Dict[str, int] = {}

        for message in messages.values():
            if message.get('status') != 'failed':
                continue

            error = message.get('error')
            if not error:
                continue

            # 过滤 worker_id
            if worker_id and message.get('consumer') != worker_id:
                continue

            # 过滤时间范围
            created_at = message.get('created_at')
            if created_at:
                if start_time and created_at < start_time.timestamp():
                    continue
                if end_time and created_at > end_time.timestamp():
                    continue

            error_counts[error] = error_counts.get(error, 0) + 1

        # 按出现次数降序排序
        return dict(sorted(error_counts.items(), key=lambda x: x[1], reverse=True))

    async def get_processing_tasks(
        self,
        worker_id: Optional[str] = None
    ) -> List[ProcessingTask]:
        """获取正在处理的任务（用于实时监控）"""
        import time

        messages = await self._get_all_messages()
        now = time.time()

        processing_tasks = []

        for message in messages.values():
            if message.get('status') != 'processing':
                continue

            # 过滤 worker_id
            if worker_id and message.get('consumer') != worker_id:
                continue

            # 计算执行时长
            updated_at = message.get('updated_at')
            duration = (now - updated_at) if updated_at else 0.0

            processing_tasks.append({
                'task_id': message.get('id'),
                'worker_id': message.get('consumer'),
                'task_type': message.get('task_name') or 'unknown',
                'data': json.loads(message.get('data', '{}')) if isinstance(message.get('data'), str) else message.get('data', {}),
                'started_at': self._format_timestamp(updated_at),
                'duration_seconds': round(duration, 2)
            })

        # 按开始时间降序排序
        processing_tasks.sort(key=lambda x: x['started_at'] or '', reverse=True)

        return processing_tasks

    async def close(self):
        """
        关闭统计（释放资源）

        注意：不会关闭 Redis 连接，连接应由外部管理
        """
        self.logger.info("RedisStats closed")
