"""
RedisQueue - 基于 Redis 的任务队列

特点：
- 高性能内存队列
- 支持分布式部署
- 可选持久化（AOF/RDB）
- 适合高并发、分布式场景
"""

import asyncio
import json
import logging
import time
from typing import Optional, Dict, Any

from .base import QueueBackend


class RedisQueue(QueueBackend):
    """
    Redis 队列提供者

    特点：
    - 高性能内存队列
    - 支持分布式部署
    - 可选持久化（AOF/RDB）
    - 适合高并发、分布式场景

    Example:
        import redis.asyncio as aioredis

        redis = await aioredis.from_url("redis://localhost:6379/0")
        queue = RedisQueue(redis=redis, queue_name="tasks")

        # 或者使用 factory
        from flowmix.storage import create_redis_storage
        storage = await create_redis_storage()
        queue = storage.queue
    """

    def __init__(
        self,
        redis: Any,
        queue_name: str = "tasks",
        timeout: float = 1.0,
    ):
        """
        初始化 Redis Queue

        Args:
            redis: Redis 连接实例（必需）
            queue_name: 队列名称（Redis key 前缀）
            timeout: pop() 等待超时时间（秒）
        """
        if redis is None:
            raise ValueError("redis connection is required")

        self._redis = redis
        self.queue_name = queue_name
        self.timeout = timeout
        self._next_id = 0  # 消息 ID 计数器

        self.logger = logging.getLogger(__name__)
        self.logger.info(f"RedisQueue initialized: queue_name={queue_name}")

    def _get_key(self, suffix: str) -> str:
        """生成 Redis key"""
        return f"{self.queue_name}:{suffix}"

    async def push(
        self,
        data: Dict[str, Any],
        priority: int = 0,
        parent_id: Optional[int] = None,
        task_name: Optional[str] = None
    ) -> int:
        """将消息放入队列"""
        redis = self._redis

        # 生成消息 ID
        msg_id = await redis.incr(self._get_key("counter"))

        # 构建消息对象
        message = {
            "id": msg_id,
            "task_name": task_name,
            "parent_id": parent_id,
            "data": json.dumps(data),
            "priority": priority,
            "status": "pending",
            "created_at": time.time()
        }

        # 保存消息数据（使用 Hash）
        await redis.hset(
            self._get_key("messages"),
            msg_id,
            json.dumps(message)
        )

        # 添加到优先级队列（使用 Sorted Set，score 为 -priority 实现高优先级优先）
        await redis.zadd(
            self._get_key("pending"),
            {str(msg_id): -priority * 1e10 + msg_id}  # 优先级高的 score 小，相同优先级按 ID 升序
        )

        self.logger.debug(f"Pushed message {msg_id} (task_name={task_name}, priority={priority})")
        return msg_id

    async def pop(self, consumer_name: str) -> Optional[Dict[str, Any]]:
        """从队列取出消息"""
        redis = self._redis
        start_time = time.time()

        while True:
            # 从优先级队列取出最小 score 的消息（最高优先级）
            items = await redis.zpopmin(self._get_key("pending"), 1)

            if items:
                msg_id_str, _ = items[0]  # score 不需要使用
                msg_id = int(msg_id_str)

                # 获取消息数据
                msg_json = await redis.hget(self._get_key("messages"), msg_id)
                if not msg_json:
                    continue

                message = json.loads(msg_json)

                # 更新状态为 processing
                message["status"] = "processing"
                message["consumer"] = consumer_name
                message["updated_at"] = time.time()

                await redis.hset(
                    self._get_key("messages"),
                    msg_id,
                    json.dumps(message)
                )

                # 构建返回结果
                data = json.loads(message["data"])
                result = {
                    "id": msg_id,
                    "task_name": message.get("task_name"),
                    **data
                }

                self.logger.debug(f"Popped message {msg_id} (task_name={message.get('task_name')})")
                return result
            else:
                # 检查超时
                if time.time() - start_time >= self.timeout:
                    return None
                await asyncio.sleep(0.1)

    async def ack(
        self,
        message_id: int,
        failed: bool = False,
        error: str = None,
        result: Any = None,
        fingerprint: Optional[str] = None
    ):
        """确认消息已处理"""
        redis = self._redis

        # 获取消息
        msg_json = await redis.hget(self._get_key("messages"), message_id)
        if not msg_json:
            return

        message = json.loads(msg_json)
        parent_id = message.get("parent_id")

        # 更新状态
        message["status"] = "failed" if failed else "completed"
        message["error"] = error
        message["result"] = json.dumps(result) if result is not None else None
        message["fingerprint"] = fingerprint
        message["completed_at"] = time.time()
        message["updated_at"] = time.time()

        await redis.hset(
            self._get_key("messages"),
            message_id,
            json.dumps(message)
        )

        self.logger.debug(f"ACKed message {message_id} as {message['status']}")

        # 如果有父任务，检查是否需要将父任务状态更新为 'done'
        if parent_id:
            await self._update_parent_status_if_done(redis, parent_id)

    async def _update_parent_status_if_done(self, redis, parent_id: int):
        """
        检查父任务是否应该更新为 'done' 状态

        当父任务本身是 'completed' 状态，且所有子任务都已完成时，将父任务状态更新为 'done'
        """
        # 获取父任务
        msg_json = await redis.hget(self._get_key("messages"), parent_id)
        if not msg_json:
            return

        parent_message = json.loads(msg_json)
        if parent_message.get("status") != "completed":
            return  # 父任务状态不是 completed，无需更新

        # 获取所有消息，检查父任务的子任务
        all_messages = await redis.hgetall(self._get_key("messages"))
        total_children = 0
        finished_children = 0

        for msg_id, msg_json in all_messages.items():
            msg = json.loads(msg_json)
            if msg.get("parent_id") == parent_id:
                total_children += 1
                if msg.get("status") in ("completed", "failed", "done"):
                    finished_children += 1

        # 如果所有子任务都已完成，更新父任务状态为 'done'
        if total_children > 0 and total_children == finished_children:
            parent_message["status"] = "done"
            parent_message["updated_at"] = time.time()
            await redis.hset(
                self._get_key("messages"),
                parent_id,
                json.dumps(parent_message)
            )
            self.logger.debug(f"Updated task {parent_id} status to 'done' (all children completed)")

            # 递归检查父任务的父任务
            if parent_message.get("parent_id"):
                await self._update_parent_status_if_done(redis, parent_message["parent_id"])

    async def get_pending_count(self) -> int:
        """获取待处理消息数量"""
        redis = self._redis
        count = await redis.zcard(self._get_key("pending"))
        return count

    async def get_stream_length(self) -> int:
        """获取队列总长度"""
        redis = self._redis
        count = await redis.hlen(self._get_key("messages"))
        return count

    async def clear_all(self):
        """清空所有消息"""
        redis = self._redis
        await redis.delete(
            self._get_key("pending"),
            self._get_key("messages"),
            self._get_key("counter")
        )
        self.logger.warning("Cleared all messages from queue")

    async def close(self):
        """
        关闭队列（释放资源）

        注意：不会关闭 Redis 连接，连接应由外部管理
        """
        self.logger.info("RedisQueue closed")
