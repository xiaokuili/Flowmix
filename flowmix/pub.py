"""
Pub - 任务发布器

职责：
- 提交任务到队列
- 查询任务状态
"""

import logging
from typing import Dict, Any, Optional

from .storage.task_queue import TaskQueue


class Pub:
    """
    任务发布器

    职责：
    - 提交任务到队列
    - 查询任务状态

    Example:
        # 初始化
        queue = TaskQueue(db_path=".flowmix/flowmix.db")
        pub = Pub(queue=queue)

        # 提交任务
        task_id = await pub.push(
            data={"url": "http://example.com"},
            task_name="crawl",
            priority=10
        )

        # 查询状态
        info = await pub.get_task_info(task_id)
        stats = await pub.get_tree_stats(task_id)
    """

    def __init__(self, queue: TaskQueue):
        """
        初始化 Pub

        Args:
            queue: TaskQueue 实例（可以是 SQLite/Redis/PostgreSQL 后端）
        """
        self._queue = queue
        self.logger = logging.getLogger(__name__)

    async def push(
        self,
        data: Dict[str, Any],
        task_name: str,
        priority: int = 0,
        parent_id: Optional[int] = None
    ) -> int:
        """
        提交任务到队列

        Args:
            data: 任务数据（字典）
            task_name: 任务名称（必填，用于路由到对应的 Task）
            priority: 优先级（默认 0，数字越大越优先）
            parent_id: 父任务 ID（可选，用于构建任务树）

        Returns:
            任务 ID

        Example:
            # 提交根任务
            root_id = await pub.push(
                data={"url": "http://example.com"},
                task_name="crawl"
            )

            # 提交子任务
            child_id = await pub.push(
                data={"url": "http://example.com/page1"},
                task_name="crawl",
                parent_id=root_id
            )
        """
        return await self._queue.push(data, priority, parent_id, task_name)

    async def get_task_info(self, task_id: int) -> Dict[str, Any]:
        """
        查询任务状态

        Args:
            task_id: 任务 ID

        Returns:
            任务信息字典，包含：
            - id: 任务 ID
            - status: 任务状态（pending/processing/completed/failed）
            - data: 任务数据
            - result: 执行结果
            - error: 错误信息
            - created_at: 创建时间
            - updated_at: 更新时间
            等
        """
        return await self._queue.get_task_info(task_id)

    async def get_tree_stats(self, root_id: int) -> Dict[str, Any]:
        """
        查询任务树统计信息

        Args:
            root_id: 根任务 ID

        Returns:
            统计字典，包含：
            - total: 总任务数
            - pending: 待处理任务数
            - processing: 处理中任务数
            - completed: 已完成任务数
            - failed: 失败任务数
        """
        return await self._queue.get_tree_stats(root_id)
