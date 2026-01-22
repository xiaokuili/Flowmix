"""
StatsReader - Worker 状态查询器

查询 Worker 的执行统计（基于 SQLite 数据库）
支持按 worker_id、时间范围、任务类型等多维度查询
"""

import sqlite3
import threading
from typing import Optional, Dict, Any, List
from datetime import datetime


class StatsReader:
    """
    Worker 状态查询器

    查询 Worker 的执行统计和性能指标

    Example:
        from flowmix import StatsReader
        from datetime import datetime

        reader = StatsReader(db_path=".flowmix/flowmix.db")

        # 查询所有 Worker 的整体执行情况
        stats = reader.get_worker_stats()
        print(f"总执行: {stats['total']} 个任务, 成功率: {stats['success_rate']*100:.1f}%")
        print(f"吞吐量: {stats['qps']:.2f} tasks/s")

        # 查询今天的执行情况
        today = datetime.now().replace(hour=0, minute=0, second=0)
        stats = reader.get_worker_stats(start_time=today)

        # 查询某个 Worker 的执行情况
        stats = reader.get_worker_stats(worker_id='worker-MacBook-12345-1234567890')

        # 列出所有 Worker
        workers = reader.list_workers()
        for w in workers:
            print(f"{w['worker_id']}: {w['completed']}/{w['total_tasks']}")
    """

    def __init__(
        self,
        db_path: str = ".flowmix/flowmix.db",
        queue_name: str = "tasks"
    ):
        """
        初始化 StatsReader

        Args:
            db_path: SQLite 数据库文件路径（与 Worker 使用相同路径）
            queue_name: 队列名称（默认: tasks）
        """
        self.db_path = db_path
        self.queue_name = queue_name

        # 每个线程使用独立的连接
        self._local = threading.local()

    def _get_connection(self) -> sqlite3.Connection:
        """获取当前线程的数据库连接"""
        if not hasattr(self._local, 'conn'):
            self._local.conn = sqlite3.connect(
                self.db_path,
                check_same_thread=False,
                timeout=30.0
            )
            self._local.conn.row_factory = sqlite3.Row
            # 启用 WAL 模式，提高并发性能
            self._local.conn.execute("PRAGMA journal_mode=WAL")
        return self._local.conn

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
            {
                'worker_id': 'worker-1',  # 如果指定了 worker_id
                'total': 5000,            # 总任务数
                'completed': 4500,        # 已完成
                'failed': 200,            # 失败
                'pending': 200,           # 待处理
                'processing': 100,        # 处理中
                'success_rate': 0.957,    # 成功率（completed / (completed + failed)）
                'qps': 2.5,               # 吞吐量（tasks per second）
                'avg_duration_seconds': 1.5,  # 平均执行时长
                'start_time': '2024-01-20 00:00:00',
                'end_time': '2024-01-20 23:59:59'
            }

        Example:
            # 查询所有 Worker 的整体情况
            stats = reader.get_worker_stats()

            # 查询某个 Worker 的执行情况
            stats = reader.get_worker_stats(worker_id='worker-1')

            # 查询今天的执行情况
            from datetime import datetime
            today_start = datetime.now().replace(hour=0, minute=0, second=0)
            stats = reader.get_worker_stats(start_time=today_start)
        """
        conn = self._get_connection()

        # 构建 WHERE 条件
        where_clauses = []
        params = []

        if worker_id:
            where_clauses.append("consumer = ?")
            params.append(worker_id)

        if start_time:
            where_clauses.append("created_at >= julianday(?)")
            params.append(start_time.isoformat())

        if end_time:
            where_clauses.append("created_at <= julianday(?)")
            params.append(end_time.isoformat())

        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

        # 查询统计信息
        cursor = conn.execute(f"""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) as pending,
                SUM(CASE WHEN status = 'processing' THEN 1 ELSE 0 END) as processing,
                SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed,
                SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed,
                AVG(CASE WHEN status IN ('completed', 'failed') AND completed_at IS NOT NULL
                    THEN (completed_at - created_at) * 86400.0  -- 转换为秒
                    ELSE NULL END) as avg_duration_seconds,
                MIN(created_at) as first_task_at,
                MAX(CASE WHEN status IN ('completed', 'failed') THEN completed_at ELSE NULL END) as last_task_at
            FROM {self.queue_name}
            {where_sql}
        """, params)

        row = cursor.fetchone()

        total = row['total'] or 0
        completed = row['completed'] or 0
        failed = row['failed'] or 0
        pending = row['pending'] or 0
        processing = row['processing'] or 0

        # 计算成功率
        finished = completed + failed
        success_rate = completed / finished if finished > 0 else 0.0

        # 计算 QPS（吞吐量）
        qps = 0.0
        if row['first_task_at'] and row['last_task_at']:
            duration = (row['last_task_at'] - row['first_task_at']) * 86400.0  # 转换为秒
            if duration > 0:
                qps = finished / duration

        result = {
            'total': total,
            'completed': completed,
            'failed': failed,
            'pending': pending,
            'processing': processing,
            'success_rate': round(success_rate, 4),
            'qps': round(qps, 2),
            'avg_duration_seconds': round(row['avg_duration_seconds'] or 0.0, 2),
        }

        if worker_id:
            result['worker_id'] = worker_id

        if start_time:
            result['start_time'] = start_time.isoformat()

        if end_time:
            result['end_time'] = end_time.isoformat()

        return result

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
            {
                'crawl': {
                    'total': 2500,
                    'completed': 2300,
                    'failed': 100,
                    'pending': 50,
                    'processing': 50,
                    'success_rate': 0.958,
                    'qps': 1.25,
                    'avg_duration_seconds': 2.1
                },
                'parse': {
                    'total': 2500,
                    'completed': 2200,
                    'failed': 100,
                    'pending': 150,
                    'processing': 50,
                    'success_rate': 0.956,
                    'qps': 1.25,
                    'avg_duration_seconds': 0.5
                }
            }

        Example:
            # 查询今天各类型任务的执行情况
            stats = reader.get_worker_stats_by_task_type(start_time=today_start)
            for task_type, task_stats in stats.items():
                print(f"{task_type}: {task_stats['completed']}/{task_stats['total']}")
        """
        conn = self._get_connection()

        # 构建 WHERE 条件
        where_clauses = []
        params = []

        if worker_id:
            where_clauses.append("consumer = ?")
            params.append(worker_id)

        if start_time:
            where_clauses.append("created_at >= julianday(?)")
            params.append(start_time.isoformat())

        if end_time:
            where_clauses.append("created_at <= julianday(?)")
            params.append(end_time.isoformat())

        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

        # 按任务类型分组统计
        cursor = conn.execute(f"""
            SELECT
                task_name,
                COUNT(*) as total,
                SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) as pending,
                SUM(CASE WHEN status = 'processing' THEN 1 ELSE 0 END) as processing,
                SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed,
                SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed,
                AVG(CASE WHEN status IN ('completed', 'failed') AND completed_at IS NOT NULL
                    THEN (completed_at - created_at) * 86400.0
                    ELSE NULL END) as avg_duration_seconds,
                MIN(created_at) as first_task_at,
                MAX(CASE WHEN status IN ('completed', 'failed') THEN completed_at ELSE NULL END) as last_task_at
            FROM {self.queue_name}
            {where_sql}
            GROUP BY task_name
        """, params)

        result = {}
        for row in cursor.fetchall():
            task_name = row['task_name'] or 'unknown'
            total = row['total'] or 0
            completed = row['completed'] or 0
            failed = row['failed'] or 0

            # 计算成功率
            finished = completed + failed
            success_rate = completed / finished if finished > 0 else 0.0

            # 计算 QPS
            qps = 0.0
            if row['first_task_at'] and row['last_task_at']:
                duration = (row['last_task_at'] - row['first_task_at']) * 86400.0
                if duration > 0:
                    qps = finished / duration

            result[task_name] = {
                'total': total,
                'completed': completed,
                'failed': failed,
                'pending': row['pending'] or 0,
                'processing': row['processing'] or 0,
                'success_rate': round(success_rate, 4),
                'qps': round(qps, 2),
                'avg_duration_seconds': round(row['avg_duration_seconds'] or 0.0, 2),
            }

        return result

    def list_workers(
        self,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        active_threshold_seconds: int = 300  # 5 分钟
    ) -> List[Dict[str, Any]]:
        """
        列出所有活跃的 Worker

        Args:
            start_time: 开始时间
            end_time: 结束时间
            active_threshold_seconds: 判断 Worker 是否活跃的阈值（秒，默认 300 秒）

        Returns:
            [
                {
                    'worker_id': 'worker-MacBook-12345-1234567890',
                    'total_tasks': 5000,
                    'completed': 4500,
                    'failed': 200,
                    'pending': 200,
                    'processing': 100,
                    'first_seen': '2024-01-20 00:00:00',
                    'last_seen': '2024-01-20 23:59:59',
                    'is_active': True  # 最近 active_threshold_seconds 内有活动
                },
                ...
            ]

        Example:
            # 列出所有 Worker
            workers = reader.list_workers()
            for w in workers:
                status = "🟢 活跃" if w['is_active'] else "🔴 停止"
                print(f"{status} {w['worker_id']}: {w['completed']}/{w['total_tasks']}")
        """
        conn = self._get_connection()

        # 构建 WHERE 条件
        where_clauses = ["consumer IS NOT NULL"]  # 只统计被 Worker 处理过的任务
        params = []

        if start_time:
            where_clauses.append("created_at >= julianday(?)")
            params.append(start_time.isoformat())

        if end_time:
            where_clauses.append("created_at <= julianday(?)")
            params.append(end_time.isoformat())

        where_sql = f"WHERE {' AND '.join(where_clauses)}"

        # 按 Worker 分组统计
        cursor = conn.execute(f"""
            SELECT
                consumer as worker_id,
                COUNT(*) as total_tasks,
                SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) as pending,
                SUM(CASE WHEN status = 'processing' THEN 1 ELSE 0 END) as processing,
                SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed,
                SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed,
                MIN(created_at) as first_seen,
                MAX(updated_at) as last_seen
            FROM {self.queue_name}
            {where_sql}
            GROUP BY consumer
            ORDER BY last_seen DESC
        """, params)

        # 获取当前时间（Julian Day）
        now_cursor = conn.execute("SELECT julianday('now') as now")
        now = now_cursor.fetchone()['now']

        result = []
        for row in cursor.fetchall():
            last_seen = row['last_seen']
            # 判断是否活跃（最近 active_threshold_seconds 内有活动）
            is_active = (now - last_seen) * 86400.0 < active_threshold_seconds if last_seen else False

            result.append({
                'worker_id': row['worker_id'],
                'total_tasks': row['total_tasks'] or 0,
                'completed': row['completed'] or 0,
                'failed': row['failed'] or 0,
                'pending': row['pending'] or 0,
                'processing': row['processing'] or 0,
                'first_seen': self._format_datetime(row['first_seen']),
                'last_seen': self._format_datetime(row['last_seen']),
                'is_active': is_active
            })

        return result

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
            [
                {
                    'task_id': 456,
                    'worker_id': 'worker-1',
                    'task_type': 'crawl',
                    'data': {'url': '...'},
                    'error': 'Connection timeout',
                    'created_at': '2024-01-20 10:00:00',
                    'failed_at': '2024-01-20 10:00:10'
                },
                ...
            ]

        Example:
            # 查询最近失败的任务
            failed = reader.get_failed_tasks(limit=10)
            for task in failed:
                print(f"任务 {task['task_id']} 失败: {task['error']}")

            # 查询某个 Worker 的失败任务
            failed = reader.get_failed_tasks(worker_id='worker-1')
        """
        conn = self._get_connection()

        # 构建 WHERE 条件
        where_clauses = ["status = 'failed'"]
        params = []

        if worker_id:
            where_clauses.append("consumer = ?")
            params.append(worker_id)

        if start_time:
            where_clauses.append("created_at >= julianday(?)")
            params.append(start_time.isoformat())

        if end_time:
            where_clauses.append("created_at <= julianday(?)")
            params.append(end_time.isoformat())

        where_sql = f"WHERE {' AND '.join(where_clauses)}"
        params.append(limit)

        # 查询失败任务
        cursor = conn.execute(f"""
            SELECT
                id,
                consumer,
                task_name,
                data,
                error,
                created_at,
                completed_at
            FROM {self.queue_name}
            {where_sql}
            ORDER BY completed_at DESC
            LIMIT ?
        """, params)

        result = []
        for row in cursor.fetchall():
            import json
            result.append({
                'task_id': row['id'],
                'worker_id': row['consumer'],
                'task_type': row['task_name'] or 'unknown',
                'data': json.loads(row['data']) if row['data'] else {},
                'error': row['error'],
                'created_at': self._format_datetime(row['created_at']),
                'failed_at': self._format_datetime(row['completed_at'])
            })

        return result

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
            {
                'Connection timeout': 30,
                'HTTP 404': 15,
                'Parse error': 5
            }

        Example:
            # 查询错误分布
            errors = reader.get_error_summary()
            for error, count in errors.items():
                print(f"{error}: {count} 次")
        """
        conn = self._get_connection()

        # 构建 WHERE 条件
        where_clauses = ["status = 'failed'", "error IS NOT NULL"]
        params = []

        if worker_id:
            where_clauses.append("consumer = ?")
            params.append(worker_id)

        if start_time:
            where_clauses.append("created_at >= julianday(?)")
            params.append(start_time.isoformat())

        if end_time:
            where_clauses.append("created_at <= julianday(?)")
            params.append(end_time.isoformat())

        where_sql = f"WHERE {' AND '.join(where_clauses)}"

        # 按错误类型统计
        cursor = conn.execute(f"""
            SELECT error, COUNT(*) as count
            FROM {self.queue_name}
            {where_sql}
            GROUP BY error
            ORDER BY count DESC
        """, params)

        result = {}
        for row in cursor.fetchall():
            result[row['error']] = row['count']

        return result

    def get_processing_tasks(
        self,
        worker_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        获取正在处理的任务（用于实时监控）

        Args:
            worker_id: Worker ID（不指定则查询所有 Worker）

        Returns:
            [
                {
                    'task_id': 123,
                    'worker_id': 'worker-1',
                    'task_type': 'crawl',
                    'data': {'url': '...'},
                    'started_at': '2024-01-20 10:00:00',
                    'duration_seconds': 5.2
                },
                ...
            ]

        Example:
            # 查看正在执行的任务
            processing = reader.get_processing_tasks()
            for task in processing:
                print(f"Worker {task['worker_id']} 正在执行 {task['task_type']}")
        """
        conn = self._get_connection()

        # 构建 WHERE 条件
        where_clauses = ["status = 'processing'"]
        params = []

        if worker_id:
            where_clauses.append("consumer = ?")
            params.append(worker_id)

        where_sql = f"WHERE {' AND '.join(where_clauses)}"

        # 获取当前时间
        now_cursor = conn.execute("SELECT julianday('now') as now")
        now = now_cursor.fetchone()['now']

        # 查询处理中的任务
        cursor = conn.execute(f"""
            SELECT
                id,
                consumer,
                task_name,
                data,
                updated_at
            FROM {self.queue_name}
            {where_sql}
            ORDER BY updated_at DESC
        """, params)

        result = []
        for row in cursor.fetchall():
            import json
            duration = (now - row['updated_at']) * 86400.0 if row['updated_at'] else 0.0

            result.append({
                'task_id': row['id'],
                'worker_id': row['consumer'],
                'task_type': row['task_name'] or 'unknown',
                'data': json.loads(row['data']) if row['data'] else {},
                'started_at': self._format_datetime(row['updated_at']),
                'duration_seconds': round(duration, 2)
            })

        return result

    def _format_datetime(self, julian_day: Optional[float]) -> Optional[str]:
        """将 Julian Day 转换为 ISO 格式字符串"""
        if julian_day is None:
            return None

        # SQLite Julian Day 转换为 Unix 时间戳
        # Julian Day 0 = -4712-01-01 12:00:00 UTC
        # Unix Epoch (1970-01-01 00:00:00) = Julian Day 2440587.5
        unix_timestamp = (julian_day - 2440587.5) * 86400.0

        dt = datetime.fromtimestamp(unix_timestamp)
        return dt.strftime('%Y-%m-%d %H:%M:%S')

    def close(self):
        """关闭数据库连接"""
        if hasattr(self._local, 'conn'):
            self._local.conn.close()
            delattr(self._local, 'conn')
