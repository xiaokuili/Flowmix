"""
Pub - 任务发布器（Sender 模块）

职责：
- 提交任务到队列
- 为外部脚本/服务提供任务提交能力

基于 flowmix.common.queue.Queue 实现

为 AI 理解的关键点:
1. Pub 只负责推送任务，不负责执行
2. 任务的执行由 TaskRunner 驱动
3. data 必须是可 JSON 序列化的字典
4. task_name 必须在 TaskRunner 中注册
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
            data: 任务数据（必须是可 JSON 序列化的字典）
            task_name: 任务名称（必须在 TaskRunner 的 tasks 字典中注册）
            priority: 优先级（0-100，数字越大越优先）
                     - priority >= 10: 深度优先（DFS），适合递归处理
                     - priority < 10: 广度优先（BFS），适合批处理
            parent_id: 父任务 ID（可选，用于构建任务树）

        Returns:
            任务 ID（消息 ID）

        Raises:
            ValueError: 如果 data 不是字典或不可 JSON 序列化

        Example:
            # 提交根任务
            root_id = await pub.push(
                data={"url": "http://example.com"},
                task_name="crawl"
            )

            # 提交高优先级任务（深度优先）
            urgent_id = await pub.push(
                data={"url": "http://example.com/urgent"},
                task_name="crawl",
                priority=10  # DFS
            )

            # 提交子任务（关联父任务）
            child_id = await pub.push(
                data={"url": "http://example.com/page1"},
                task_name="crawl",
                parent_id=root_id
            )

            # 提交低优先级任务（广度优先）
            batch_id = await pub.push(
                data={"batch": 1},
                task_name="process",
                priority=0  # BFS
            )

        AI 注意事项:
            - data 必须是字典，不能是其他类型（list、str、int 等）
            - task_name 必须在 TaskRunner 初始化时注册
            - 返回的 ID 可用作其他任务的 parent_id
            - 优先级建议: DFS=10, BFS=0, 默认=5
        """
        # 验证 data 是字典
        if not isinstance(data, dict):
            raise ValueError(
                f"data must be a dict, got {type(data).__name__}. "
                f"Example: {{'key': 'value'}}"
            )

        return await self._queue.push(
            data=data,
            task_name=task_name,
            priority=priority,
            parent_id=parent_id
        )
