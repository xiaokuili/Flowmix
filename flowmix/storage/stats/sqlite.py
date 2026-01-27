"""
SQLiteStats - 基于 SQLite 的统计查询实现

查询 Worker 的执行统计（基于 SQLite 数据库）
支持按 worker_id、时间范围、任务类型等多维度查询
"""

import sqlite3
import threading
import json
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


class SQLiteStats(Stats):
    """
    SQLite 统计查询实现

    查询 Worker 的执行统计和性能指标

    Example:
        from flowmix.storage.stats import SQLiteStats
        from datetime import datetime

        stats = SQLiteStats(db_path=".flowmix/flowmix.db")

        # 查询所有 Worker 的整体执行情况
        overall = stats.get_worker_stats()
        print(f"总执行: {overall['total']} 个任务, 成功率: {overall['success_rate']*100:.1f}%")
        print(f"吞吐量: {overall['qps']:.2f} tasks/s")

        # 查询今天的执行情况
        today = datetime.now().replace(hour=0, minute=0, second=0)
        today_stats = stats.get_worker_stats(start_time=today)

        # 查询某个 Worker 的执行情况
        worker_stats = stats.get_worker_stats(worker_id='worker-1')

        # 列出所有 Worker
        workers = stats.list_workers()
        for w in workers:
            print(f"{w['worker_id']}: {w['completed']}/{w['total_tasks']}")
    """

    def __init__(
        self,
        db_path: str = ".flowmix/flowmix.db",
        queue_name: str = "tasks"
    ):
        """
        初始化 SQLiteStats

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

    def get_task_info(self, task_id: int) -> Optional[TaskInfo]:
        """获取任务详情"""
        conn = self._get_connection()
        cursor = conn.execute(
            f"SELECT * FROM {self.queue_name} WHERE id = ?",
            (task_id,)
        )
        row = cursor.fetchone()
        if not row:
            return None

        return {
            'id': row['id'],
            'parent_id': row['parent_id'],
            'task_name': row['task_name'],
            'data': json.loads(row['data']) if row['data'] else None,
            'priority': row['priority'],
            'status': row['status'],
            'consumer': row['consumer'],
            'error': row['error'],
            'result': json.loads(row['result']) if row['result'] else None,
            'fingerprint': row['fingerprint'],
            'created_at': self._format_datetime(row['created_at']),
            'updated_at': self._format_datetime(row['updated_at']),
            'completed_at': self._format_datetime(row['completed_at'])
        }

    def get_children(self, parent_id: int) -> List[TaskInfo]:
        """获取所有直接子任务"""
        conn = self._get_connection()
        cursor = conn.execute(
            f"SELECT * FROM {self.queue_name} WHERE parent_id = ? ORDER BY id ASC",
            (parent_id,)
        )

        result = []
        for row in cursor.fetchall():
            result.append({
                'id': row['id'],
                'parent_id': row['parent_id'],
                'task_name': row['task_name'],
                'data': json.loads(row['data']) if row['data'] else None,
                'priority': row['priority'],
                'status': row['status'],
                'consumer': row['consumer'],
                'error': row['error'],
                'result': json.loads(row['result']) if row['result'] else None,
                'fingerprint': row['fingerprint'],
                'created_at': self._format_datetime(row['created_at']),
                'updated_at': self._format_datetime(row['updated_at']),
                'completed_at': self._format_datetime(row['completed_at'])
            })
        return result

    def get_task_tree_stats(self, root_id: int) -> TaskTreeStats:
        """获取任务树的统计信息（递归查询所有子孙任务）"""
        conn = self._get_connection()

        # 使用 CTE 递归查询整棵树
        cursor = conn.execute(f"""
            WITH RECURSIVE tree AS (
                SELECT * FROM {self.queue_name} WHERE id = ?
                UNION ALL
                SELECT t.* FROM {self.queue_name} t
                INNER JOIN tree ON t.parent_id = tree.id
            )
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) as pending,
                SUM(CASE WHEN status = 'processing' THEN 1 ELSE 0 END) as processing,
                SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed,
                SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed
            FROM tree
        """, (root_id,))

        row = cursor.fetchone()
        return {
            "total": row["total"] or 0,
            "pending": row["pending"] or 0,
            "processing": row["processing"] or 0,
            "completed": row["completed"] or 0,
            "failed": row["failed"] or 0,
        }

    def get_task_tree_details(self, root_id: int) -> List[TaskInfo]:
        """获取任务树的详细信息（递归查询所有子孙任务）"""
        conn = self._get_connection()

        # 使用 CTE 递归查询整棵树
        cursor = conn.execute(f"""
            WITH RECURSIVE tree AS (
                SELECT * FROM {self.queue_name} WHERE id = ?
                UNION ALL
                SELECT t.* FROM {self.queue_name} t
                INNER JOIN tree ON t.parent_id = tree.id
            )
            SELECT * FROM tree ORDER BY id ASC
        """, (root_id,))

        rows = cursor.fetchall()

        # 返回任务列表
        tasks = []
        for row in rows:
            tasks.append({
                'id': row['id'],
                'parent_id': row['parent_id'],
                'task_name': row['task_name'],
                'data': json.loads(row['data']) if row['data'] else None,
                'priority': row['priority'],
                'status': row['status'],
                'consumer': row['consumer'],
                'error': row['error'],
                'result': json.loads(row['result']) if row['result'] else None,
                'fingerprint': row['fingerprint'],
                'created_at': self._format_datetime(row['created_at']),
                'updated_at': self._format_datetime(row['updated_at']),
                'completed_at': self._format_datetime(row['completed_at'])
            })
        return tasks

    def get_worker_stats(
        self,
        worker_id: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> WorkerStats:
        """查询 Worker 执行统计（支持多维度筛选）"""
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

        result: WorkerStats = {
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
    ) -> Dict[str, WorkerStats]:
        """按任务类型统计 Worker 执行情况"""
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
        active_threshold_seconds: int = 300
    ) -> List[WorkerInfo]:
        """列出所有活跃的 Worker"""
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
    ) -> List[FailedTask]:
        """查询失败的任务"""
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
        """错误汇总统计"""
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
    ) -> List[ProcessingTask]:
        """获取正在处理的任务（用于实时监控）"""
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
