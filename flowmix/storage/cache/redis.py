"""
RedisCache - 基于 Redis 的任务缓存管理

基于任务指纹实现去重和结果缓存
支持永久缓存和 TTL 过期缓存
"""

import json
import logging
from typing import Optional, Dict, Any

from .base import CacheBackend


class RedisCache(CacheBackend):
    """
    Redis 缓存管理器

    特点：
    - 高性能内存缓存
    - 支持分布式部署
    - 适合高并发场景

    功能：
    - 基于任务指纹（fingerprint）实现去重
    - 支持永久缓存（适合爬虫 URL 去重）
    - 支持 TTL 过期缓存（适合 API 调用缓存）

    实现原理：
    - 任务指纹 = SHA256(task_name + data)
    - 查询 Redis Hash 中 fingerprint 相同且状态为 completed 的任务
    - 如果设置了 TTL，只查找最近 TTL 秒内完成的任务

    Example:
        import redis.asyncio as aioredis

        redis = await aioredis.from_url("redis://localhost:6379/0")
        cache = RedisCache(redis=redis, queue_name="tasks")

        # 检查缓存（永久缓存）
        result = await cache.check(task_name="crawl", data={"url": "http://example.com"})
        if result:
            print("缓存命中:", result)

        # 检查缓存（1小时 TTL）
        result = await cache.check(
            task_name="api_call",
            data={"endpoint": "/users/123"},
            ttl=3600
        )

        # 或者使用 factory
        from flowmix.storage import create_redis_storage
        storage = await create_redis_storage()
        cache = storage.cache
    """

    def __init__(
        self,
        redis: Any,
        queue_name: str = "tasks"
    ):
        """
        初始化缓存管理器

        Args:
            redis: Redis 连接实例（必需）
            queue_name: 队列名称（Redis key 前缀）
        """
        if redis is None:
            raise ValueError("redis connection is required")

        self._redis = redis
        self.queue_name = queue_name

        self.logger = logging.getLogger(__name__)
        self.logger.info(f"RedisCache initialized: queue_name={queue_name}")

    def _get_key(self, suffix: str) -> str:
        """生成 Redis key"""
        return f"{self.queue_name}:{suffix}"

    def generate_fingerprint(self, task_name: str, data: Dict[str, Any]) -> str:
        """
        生成任务指纹（用于去重）

        Args:
            task_name: 任务名称
            data: 任务数据

        Returns:
            SHA256 哈希字符串

        Example:
            fingerprint = cache.generate_fingerprint("crawl", {"url": "http://example.com"})
            # 返回: "a3c8f9e2..."
        """
        # 使用默认实现
        return self._default_fingerprint(task_name, data)

    async def check(
        self,
        task_name: str,
        data: Dict[str, Any],
        ttl: Optional[int] = None
    ) -> Optional[Any]:
        """
        检查缓存是否命中

        Args:
            task_name: 任务名称
            data: 任务数据
            ttl: 缓存有效期（秒），None 表示永久缓存

        Returns:
            缓存的结果，如果未命中返回 None

        Example:
            # 永久缓存
            result = await cache.check("crawl", {"url": "http://example.com"})

            # 1小时缓存
            result = await cache.check("api_call", {"endpoint": "/users"}, ttl=3600)
        """
        import time

        fingerprint = self.generate_fingerprint(task_name, data)
        redis = self._redis

        # 获取所有消息
        messages = await redis.hgetall(self._get_key("messages"))

        # 查找匹配的消息
        for msg_id, msg_json in messages.items():
            try:
                message = json.loads(msg_json)

                # 检查状态和指纹
                if message.get("status") != "completed":
                    continue

                if message.get("fingerprint") != fingerprint:
                    continue

                # 检查 TTL
                if ttl is not None:
                    completed_at = message.get("completed_at")
                    if completed_at is None:
                        continue

                    elapsed = time.time() - completed_at
                    if elapsed > ttl:
                        continue

                # 缓存命中
                result = message.get("result")
                if result:
                    self.logger.debug(f"Cache hit for task '{task_name}' (fingerprint={fingerprint[:8]}...)")
                    return json.loads(result) if isinstance(result, str) else result

            except (json.JSONDecodeError, KeyError) as e:
                self.logger.warning(f"Failed to parse message {msg_id}: {e}")
                continue

        self.logger.debug(f"Cache miss for task '{task_name}' (fingerprint={fingerprint[:8]}...)")
        return None

    async def close(self):
        """
        关闭缓存（释放资源）

        注意：不会关闭 Redis 连接，连接应由外部管理
        """
        self.logger.info("RedisCache closed")
