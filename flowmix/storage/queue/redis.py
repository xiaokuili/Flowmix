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
        queue = RedisQueue(
            redis_url="redis://localhost:6379/0",
            queue_name="tasks"
        )
    """

    def __init__(
        self,
        redis_url: str = "redis://localhost:6379/0",
        queue_name: str = "tasks",
        timeout: float = 1.0,
    ):
        """
        初始化 Redis Queue

        Args:
            redis_url: Redis 连接 URL
            queue_name: 队列名称（Redis key 前缀）
            timeout: pop() 等待超时时间（秒）
        """
        try:
            import redis.asyncio as aioredis
            self._aioredis = aioredis
        except ImportError:
            raise ImportError(
                "redis is required for RedisQueue. "
                "Install it with: pip install 'flowmix[redis]'"
            )

        self.redis_url = redis_url
        self.queue_name = queue_name
        self.timeout = timeout

        # Redis 连接
        self._redis: Optional[Any] = None
        self._next_id = 0  # 消息 ID 计数器

        self.logger = logging.getLogger(__name__)
        self.logger.info(f"RedisQueue initialized: redis_url={redis_url}, queue_name={queue_name}")

    async def _get_connection(self):
        """获取 Redis 连接"""
        if self._redis is None:
            self._redis = await self._aioredis.from_url(
                self.redis_url,
                decode_responses=True
            )
        return self._redis

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
        redis = await self._get_connection()

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
        redis = await self._get_connection()
        start_time = time.time()

        while True:
            # 从优先级队列取出最小 score 的消息（最高优先级）
            items = await redis.zpopmin(self._get_key("pending"), 1)

            if items:
                msg_id_str, score = items[0]
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
        redis = await self._get_connection()

        # 获取消息
        msg_json = await redis.hget(self._get_key("messages"), message_id)
        if not msg_json:
            return

        message = json.loads(msg_json)

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

    async def get_pending_count(self) -> int:
        """获取待处理消息数量"""
        redis = await self._get_connection()
        count = await redis.zcard(self._get_key("pending"))
        return count

    async def get_stream_length(self) -> int:
        """获取队列总长度"""
        redis = await self._get_connection()
        count = await redis.hlen(self._get_key("messages"))
        return count

    async def clear_all(self):
        """清空所有消息"""
        redis = await self._get_connection()
        await redis.delete(
            self._get_key("pending"),
            self._get_key("messages"),
            self._get_key("counter")
        )
        self.logger.warning("Cleared all messages from queue")

    async def close(self):
        """关闭 Redis 连接"""
        if self._redis is not None:
            await self._redis.close()
            self._redis = None
        self.logger.info("Closed Redis connection")
