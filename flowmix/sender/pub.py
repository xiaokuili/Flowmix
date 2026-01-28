"""
Pub - 任务发布器（Sender 模块）

职责：
- 提交任务到队列
- 为外部脚本/服务提供任务提交能力

基于 flowmix.common.queue.Queue 实现
"""

import logging
from typing import Dict, Any, Optional

from ..common.queue import Queue


class Pub:
    """
    任务发布器

    职责：
    - 提交任务到队列

    Example:
        # 使用 SQLite 队列
        from flowmix.common import SQLitePool, SQLiteQueue
        from flowmix.sender import Pub

        pool = await SQLitePool.get_instance('.flowmix/flowmix.db')
        queue = SQLiteQueue(pool=pool, queue_name='tasks')
        pub = Pub(queue=queue)

        # 提交任务
        task_id = await pub.push(
            data={"url": "http://example.com"},
            task_name="crawl",
            priority=10
        )

        # 使用 Redis 队列
        from flowmix.common import RedisPool, RedisQueue
        from flowmix.sender import Pub

        pool = await RedisPool.get_instance('redis://localhost:6379/0')
        queue = RedisQueue(pool=pool, queue_name='tasks')
        pub = Pub(queue=queue)
    """

    def __init__(self, queue: Queue):
        """
        初始化 Pub

        Args:
            queue: Queue 实例（SQLiteQueue 或 RedisQueue）
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

            # 提交高优先级任务
            urgent_id = await pub.push(
                data={"url": "http://example.com/urgent"},
                task_name="crawl",
                priority=100
            )

            # 提交子任务
            child_id = await pub.push(
                data={"url": "http://example.com/page1"},
                task_name="crawl",
                parent_id=root_id
            )
        """
        return await self._queue.push(
            data=data,
            task_name=task_name,
            priority=priority,
            parent_id=parent_id
        )
