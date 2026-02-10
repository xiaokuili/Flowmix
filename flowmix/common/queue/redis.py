"""
RedisQueue - 基于 Redis 的任务队列

特点：
- 高性能内存队列
- 支持分布式部署
- 基于 RedisPool 单例连接池
- 适合高并发、分布式场景
"""

import asyncio
import json
import logging
import time
from typing import Optional, Dict, Any

from ..pool import RedisPool
from .base import Queue


logger = logging.getLogger(__name__)


class RedisQueue(Queue):
    """
    Redis 队列实现

    基于 RedisPool 单例连接池，提供高性能的分布式任务队列

    数据结构：
    - {queue_name}:counter - 消息 ID 计数器（String）
    - {queue_name}:messages - 消息数据存储（Hash）
    - {queue_name}:pending - 待处理队列（Sorted Set，按优先级排序）

    Example:
        # 获取 RedisPool 单例
        pool = await RedisPool.get_instance('redis://localhost:6379/0')

        # 创建队列
        queue = RedisQueue(pool=pool, queue_name='tasks')

        # 使用队列
        msg_id = await queue.push({'url': 'https://example.com'}, task_name='crawl')
        message = await queue.pop('worker-1')
        await queue.ack(message['id'], failed=False, result={'status': 'ok'})
    """

    def __init__(
        self,
        pool: RedisPool,
        queue_name: str = "tasks",
        timeout: float = 1.0,
    ):
        """
        初始化 RedisQueue

        Args:
            pool: RedisPool 实例
            queue_name: 队列名称（Redis key 前缀）
            timeout: pop() 等待超时时间（秒）
        """
        self._pool = pool
        self.queue_name = queue_name
        self.timeout = timeout
        logger.info(f"RedisQueue initialized: queue_name={queue_name}")

    def _get_key(self, suffix: str) -> str:
        """生成 Redis key"""
        return f"{self.queue_name}:{suffix}"

    def _get_children_key(self, parent_id: int) -> str:
        """生成父任务的子任务索引 key"""
        return f"{self.queue_name}:children:{parent_id}"

    async def push(
        self,
        data: Dict[str, Any],
        priority: int = 0,
        parent_id: Optional[int] = None,
        task_name: Optional[str] = None
    ) -> int:
        """将任务放入队列"""
        async with self._pool.acquire() as redis:
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
                "chain_status": "pending",  # 任务链状态：初始为 pending
                "created_at": time.time()
            }

            # 保存消息数据（使用 Hash）
            await redis.hset(
                self._get_key("messages"),
                msg_id,
                json.dumps(message)
            )

            # 添加到优先级队列（使用 Sorted Set，score 为 -priority 实现高优先级优先）
            # score = -priority * 1e10 + msg_id，保证：优先级高的先执行，相同优先级按 ID 升序
            await redis.zadd(
                self._get_key("pending"),
                {str(msg_id): -priority * 1e10 + msg_id}
            )

            # 维护父子关系索引：如果有父任务，添加到父任务的子任务集合
            if parent_id:
                await redis.sadd(self._get_children_key(parent_id), msg_id)

            logger.debug(f"Pushed message {msg_id} (task_name={task_name}, priority={priority}, parent_id={parent_id})")
            return msg_id

    async def pop(self, consumer_name: str) -> Optional[Dict[str, Any]]:
        """从队列取出任务"""
        start_time = time.time()

        while True:
            async with self._pool.acquire() as redis:
                # 从优先级队列取出最小 score 的消息（最高优先级）
                items = await redis.zpopmin(self._get_key("pending"), 1)

                if items:
                    msg_id_str, _ = items[0]
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
                        "data": data
                    }

                    logger.debug(f"Popped message {msg_id} (task_name={message.get('task_name')})")
                    return result

            # 检查超时
            if time.time() - start_time >= self.timeout:
                return None
            await asyncio.sleep(0.1)

    async def ack(
        self,
        message_id: int,
        failed: bool = False,
        error: Optional[str] = None,
        result: Optional[Any] = None,
        fingerprint: Optional[str] = None
    ):
        """确认任务已处理"""
        async with self._pool.acquire() as redis:
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

            logger.debug(f"ACKed message {message_id} as {message['status']}")

            # 如果任务本身完成或失败，更新其 chain_status
            if message["status"] in ("completed", "failed"):
                await self._update_chain_status_if_done(redis, message_id, parent_id)

    async def recover_processing_tasks(self, stale_after: float = 0.0) -> int:
        """恢复 processing 状态任务到 pending 队列"""
        async with self._pool.acquire() as redis:
            all_messages = await redis.hgetall(self._get_key("messages"))
            now = time.time()
            recovered = 0

            for msg_id_raw, msg_json in all_messages.items():
                msg_id = int(msg_id_raw)
                message = json.loads(msg_json)

                if message.get("status") != "processing":
                    continue

                last_update = message.get("updated_at", message.get("created_at", now))
                if stale_after > 0 and (now - last_update) < stale_after:
                    continue

                priority = int(message.get("priority", 0))
                message["status"] = "pending"
                message.pop("consumer", None)
                message["updated_at"] = now

                await redis.hset(
                    self._get_key("messages"),
                    msg_id,
                    json.dumps(message)
                )
                await redis.zadd(
                    self._get_key("pending"),
                    {str(msg_id): -priority * 1e10 + msg_id}
                )
                recovered += 1

            return recovered

    async def _update_chain_status_if_done(self, redis, task_id: int, parent_id: Optional[int]):
        """
        更新任务（及其父任务）的 chain_status

        优化：使用父子关系索引，避免 HGETALL 获取所有消息

        当任务本身是 'completed' 或 'failed' 状态时：
        1. 如果任务没有子任务，更新其 chain_status 为 'completed'
        2. 如果有父任务，检查父任务是否应该更新 chain_status
        """
        # 1. 检查是否有子任务（使用索引，O(1)）
        children = await redis.smembers(self._get_children_key(task_id))
        has_children = len(children) > 0

        if not has_children:
            # 叶子任务，更新其 chain_status 为 'completed'
            task_json = await redis.hget(self._get_key("messages"), task_id)
            if task_json:
                task_message = json.loads(task_json)
                if task_message.get("status") in ("completed", "failed") and task_message.get("chain_status") != "completed":
                    task_message["chain_status"] = "completed"
                    task_message["updated_at"] = time.time()
                    await redis.hset(
                        self._get_key("messages"),
                        task_id,
                        json.dumps(task_message)
                    )
                    logger.debug(f"Updated task {task_id} chain_status to 'completed' (leaf task)")

        # 2. 如果有父任务，递归检查并更新父任务的 chain_status
        if parent_id:
            await self._update_parent_chain_status(redis, parent_id)

    async def _update_parent_chain_status(self, redis, parent_id: int):
        """
        更新父任务的 chain_status（内部方法）

        优化：使用父子关系索引，只获取直接子任务
        """
        # 获取父任务
        parent_json = await redis.hget(self._get_key("messages"), parent_id)
        if not parent_json:
            return

        parent_message = json.loads(parent_json)

        if parent_message.get("status") not in ("completed", "failed"):
            return  # 父任务状态不是 completed/failed，无需更新
        if parent_message.get("chain_status") == "completed":
            return  # 已经是 completed，无需重复更新

        # 使用索引获取所有子任务 ID（O(1)）
        child_ids = await redis.smembers(self._get_children_key(parent_id))

        if not child_ids:
            # 没有子任务，chain_status 跟随 status
            new_chain_status = "completed"
        else:
            # 批量获取子任务状态（O(k)，k = 子任务数）
            child_messages_json = await redis.hmget(
                self._get_key("messages"),
                *[int(cid) for cid in child_ids]
            )

            # 检查是否所有子任务都已完成
            has_pending = False
            for child_json in child_messages_json:
                if not child_json:
                    continue
                child = json.loads(child_json)
                child_status = child.get("status")
                child_chain_status = child.get("chain_status", "pending")

                # 只有子任务真正完成时才算（status 完成或 chain_status 完成）
                is_child_finished = (
                    child_status in ("completed", "failed") or
                    child_chain_status == "completed"
                )

                if not is_child_finished and (
                    child_status in ("pending", "processing") or
                    child_chain_status == "processing"
                ):
                    has_pending = True
                    break

            new_chain_status = "processing" if has_pending else "completed"

        # 更新父任务的 chain_status
        if new_chain_status != parent_message.get("chain_status"):
            parent_message["chain_status"] = new_chain_status
            parent_message["updated_at"] = time.time()
            await redis.hset(
                self._get_key("messages"),
                parent_id,
                json.dumps(parent_message)
            )
            logger.debug(f"Updated task {parent_id} chain_status to '{new_chain_status}'")

            # 递归检查父任务的父任务
            if parent_message.get("parent_id"):
                await self._update_parent_chain_status(redis, parent_message["parent_id"])



    async def clear_all(self):
        """清空所有消息和索引"""
        async with self._pool.acquire() as redis:
            # 获取所有任务 ID，清理对应的子任务索引
            all_messages = await redis.hgetall(self._get_key("messages"))
            for msg_id in all_messages.keys():
                await redis.delete(self._get_children_key(int(msg_id)))

            # 清空队列数据
            await redis.delete(
                self._get_key("pending"),
                self._get_key("messages"),
                self._get_key("counter")
            )
            logger.warning("Cleared all messages and indexes from queue")

    async def close(self):
        """
        关闭队列（释放资源）

        注意：不会关闭 RedisPool，连接池由单例管理
        """
        logger.info("RedisQueue closed")
