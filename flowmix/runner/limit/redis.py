"""
RedisRateLimiter - 基于 Redis 的限流器

基于 Redis 的滑动窗口算法，支持分布式限流
适用于多机器部署场景
"""

import asyncio
import time
from typing import Optional

from .base import RateLimiter


class RedisRateLimiter(RateLimiter):
    """
    基于 Redis 的分布式限流器

    功能：
    - 基于任务名称（task_name）控制每秒最大并发数
    - 使用 Redis ZSET 实现滑动窗口算法
    - 支持分布式部署，多个 Runner 实例共享限流状态
    - 异步阻塞：超限时自动等待，直到有空位

    实现原理：
    - 为每个 task_name 在 Redis 中维护一个 ZSET（Sorted Set）
    - ZSET 的 score 为时间戳，member 为唯一 ID
    - acquire() 时：
      1. 清理过期数据（1秒前的）
      2. 检查当前窗口内的数量
      3. 未超限则添加新记录
    - release() 时：删除对应的 member

    Example:
        import redis.asyncio as aioredis

        redis_conn = await aioredis.from_url("redis://localhost:6379/0")
        limiter = RedisRateLimiter(redis=redis_conn)

        async def worker():
            # 获取执行许可（阻塞等待）
            await limiter.acquire('api_call', limit=10)
            try:
                result = await call_api()
            finally:
                # 释放执行许可
                limiter.release('api_call')
    """

    def __init__(self, redis, key_prefix: str = "flowmix:limiter"):
        """
        初始化 Redis 限流器

        Args:
            redis: Redis 异步客户端（redis.asyncio.Redis）
            key_prefix: Redis key 前缀（默认 "flowmix:limiter"）
        """
        self.redis = redis
        self.key_prefix = key_prefix
        self._task_counters = {}  # 用于生成唯一 ID

    def _get_key(self, task_name: str) -> str:
        """
        获取 Redis key

        Args:
            task_name: 任务名称

        Returns:
            Redis key
        """
        return f"{self.key_prefix}:{task_name}"

    def _generate_member_id(self, task_name: str) -> str:
        """
        生成唯一的 member ID

        Args:
            task_name: 任务名称

        Returns:
            唯一的 member ID（格式：task_name:timestamp:counter）
        """
        if task_name not in self._task_counters:
            self._task_counters[task_name] = 0

        self._task_counters[task_name] += 1
        counter = self._task_counters[task_name]
        timestamp = int(time.time() * 1000000)  # 微秒级时间戳

        return f"{task_name}:{timestamp}:{counter}"

    async def acquire(
        self,
        task_name: str,
        limit: int,
        timeout: Optional[float] = None
    ) -> bool:
        """
        获取执行许可（阻塞等待直到有空位或超时）

        Args:
            task_name: 任务名称
            limit: 每秒最大并发数
            timeout: 超时时间（秒），None 表示无限等待

        Returns:
            True: 获取成功
            False: 超时失败

        Raises:
            asyncio.TimeoutError: 超时
        """
        if limit <= 0:
            return True  # 无限制

        key = self._get_key(task_name)
        start_time = time.time()

        while True:
            current_time = time.time()
            window_start = current_time - 1.0  # 1 秒滑动窗口

            # Lua 脚本：原子操作清理过期数据并检查限流
            lua_script = """
            local key = KEYS[1]
            local window_start = tonumber(ARGV[1])
            local limit = tonumber(ARGV[2])
            local current_time = tonumber(ARGV[3])
            local member_id = ARGV[4]

            -- 清理过期数据（1秒前的）
            redis.call('ZREMRANGEBYSCORE', key, '-inf', window_start)

            -- 检查当前窗口内的数量
            local count = redis.call('ZCARD', key)

            if count < limit then
                -- 未超限，添加新记录
                redis.call('ZADD', key, current_time, member_id)
                -- 设置过期时间（2秒后自动清理，防止 key 永久存在）
                redis.call('EXPIRE', key, 2)
                return 1
            else
                return 0
            end
            """

            member_id = self._generate_member_id(task_name)

            result = await self.redis.eval(
                lua_script,
                1,  # key 数量
                key,
                window_start,
                limit,
                current_time,
                member_id
            )

            if result == 1:
                # 获取成功，保存 member_id 用于 release
                if not hasattr(self, '_current_member_ids'):
                    self._current_member_ids = {}
                if task_name not in self._current_member_ids:
                    self._current_member_ids[task_name] = []
                self._current_member_ids[task_name].append(member_id)
                return True

            # 检查超时
            if timeout is not None:
                elapsed = time.time() - start_time
                if elapsed >= timeout:
                    raise asyncio.TimeoutError(
                        f"Failed to acquire concurrency limit for '{task_name}' "
                        f"(limit={limit}) within {timeout}s"
                    )

            # 等待一小段时间后重试（避免忙等待）
            await asyncio.sleep(0.01)

    def release(self, task_name: str):
        """
        释放执行许可

        Args:
            task_name: 任务名称

        Note:
            此方法是同步的，但会在后台异步删除 Redis 记录
            不会阻塞调用者
        """
        if not hasattr(self, '_current_member_ids'):
            return

        if task_name not in self._current_member_ids:
            return

        member_ids = self._current_member_ids[task_name]
        if not member_ids:
            return

        # 获取最后一个 member_id（LIFO）
        member_id = member_ids.pop()
        key = self._get_key(task_name)

        # 创建后台任务删除记录（不阻塞）
        asyncio.create_task(self._async_release(key, member_id))

    async def _async_release(self, key: str, member_id: str):
        """
        异步释放 Redis 记录

        Args:
            key: Redis key
            member_id: member ID
        """
        try:
            await self.redis.zrem(key, member_id)
        except Exception:
            # 忽略错误（可能已经过期自动清理了）
            pass

    def current_count(self, task_name: str) -> int:
        """
        获取当前并发数（用于监控）

        Args:
            task_name: 任务名称

        Returns:
            当前正在执行的任务数量

        Note:
            此方法是同步的，但内部使用 asyncio.run() 运行异步查询
            不建议在异步上下文中频繁调用
        """
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # 如果已在事件循环中，创建任务并等待
                task = asyncio.create_task(self._async_current_count(task_name))
                # 注意：这里无法阻塞等待，返回 0
                return 0
            else:
                return asyncio.run(self._async_current_count(task_name))
        except Exception:
            return 0

    async def _async_current_count(self, task_name: str) -> int:
        """
        异步获取当前并发数

        Args:
            task_name: 任务名称

        Returns:
            当前正在执行的任务数量
        """
        key = self._get_key(task_name)
        current_time = time.time()
        window_start = current_time - 1.0

        # 清理过期数据并统计
        await self.redis.zremrangebyscore(key, '-inf', window_start)
        count = await self.redis.zcard(key)

        return count

    def get_stats(self, task_name: str) -> dict:
        """
        获取统计信息（用于调试和监控）

        Args:
            task_name: 任务名称

        Returns:
            统计字典，包含：
            - active: 当前正在执行的任务数
            - window_total: 1 秒滑动窗口内的总任务数

        Note:
            此方法是同步的，但内部使用 asyncio.run() 运行异步查询
            不建议在异步上下文中频繁调用
        """
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # 如果已在事件循环中，返回空统计
                return {'active': 0, 'window_total': 0}
            else:
                return asyncio.run(self._async_get_stats(task_name))
        except Exception:
            return {'active': 0, 'window_total': 0}

    async def _async_get_stats(self, task_name: str) -> dict:
        """
        异步获取统计信息

        Args:
            task_name: 任务名称

        Returns:
            统计字典
        """
        count = await self._async_current_count(task_name)
        return {
            'active': count,
            'window_total': count
        }

    def reset(self, task_name: Optional[str] = None):
        """
        重置限流器（主要用于测试）

        Args:
            task_name: 任务名称，None 表示重置所有任务

        Note:
            此方法是同步的，但会在后台异步删除 Redis 数据
        """
        asyncio.create_task(self._async_reset(task_name))

    async def _async_reset(self, task_name: Optional[str] = None):
        """
        异步重置限流器

        Args:
            task_name: 任务名称，None 表示重置所有任务
        """
        if task_name is None:
            # 删除所有限流 key
            pattern = f"{self.key_prefix}:*"
            keys = await self.redis.keys(pattern)
            if keys:
                await self.redis.delete(*keys)
        else:
            key = self._get_key(task_name)
            await self.redis.delete(key)

    async def close(self):
        """
        关闭限流器，释放资源

        Note:
            此方法不会关闭 Redis 连接，因为连接可能被其他组件共享
            如需关闭连接，请在外部调用 redis.close()
        """
        # 清理内部状态
        if hasattr(self, '_current_member_ids'):
            self._current_member_ids.clear()
        self._task_counters.clear()
