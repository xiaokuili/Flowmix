"""
MemoryCache - 基于内存的任务缓存管理

基于任务指纹实现去重和结果缓存
支持永久缓存和 TTL 过期缓存
适合单机、开发和测试场景
"""

import time
import logging
from typing import Optional, Dict, Any

from .base import Cache


class MemoryCache(Cache):
    """
    内存缓存管理器

    特点：
    - 简单快速的内存缓存
    - 无需外部依赖
    - 适合单机和测试场景

    功能：
    - 基于任务指纹（fingerprint）实现去重
    - 支持永久缓存（适合爬虫 URL 去重）
    - 支持 TTL 过期缓存（适合 API 调用缓存）

    实现原理：
    - 任务指纹 = SHA256(task_name + data)
    - 使用 dict 存储 fingerprint -> (result, completed_at)
    - 如果设置了 TTL，只返回最近 TTL 秒内完成的任务

    注意：
    - 数据存储在内存中，进程重启后丢失
    - 不适合分布式场景
    - 不会自动清理过期数据（需要手动调用 cleanup）

    Example:
        cache = MemoryCache()

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
    """

    def __init__(self):
        """
        初始化缓存管理器

        数据结构：
            _cache: Dict[fingerprint, Dict]
                {
                    "result": Any,          # 缓存的结果
                    "completed_at": float,  # 完成时间戳
                    "task_name": str,       # 任务名称（用于调试）
                }
        """
        self._cache: Dict[str, Dict[str, Any]] = {}
        self.logger = logging.getLogger(__name__)
        self.logger.info("MemoryCache initialized")

    def generate_fingerprint(self, task_name: str, data: Dict[str, Any]) -> str:
        """
        生成任务指纹（用于去重）

        Args:
            task_name: 任务名称
            data: 任务数据

        Returns:
            SHA256 哈希字符串
        """
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
        """
        fingerprint = self.generate_fingerprint(task_name, data)

        cache_entry = self._cache.get(fingerprint)
        if cache_entry is None:
            self.logger.debug(f"Cache miss for task '{task_name}' (fingerprint={fingerprint[:8]}...)")
            return None

        # 检查 TTL
        if ttl is not None:
            completed_at = cache_entry.get("completed_at")
            if completed_at is None:
                return None

            elapsed = time.time() - completed_at
            if elapsed > ttl:
                self.logger.debug(
                    f"Cache expired for task '{task_name}' "
                    f"(fingerprint={fingerprint[:8]}..., elapsed={elapsed:.1f}s, ttl={ttl}s)"
                )
                return None

        result = cache_entry.get("result")
        self.logger.debug(f"Cache hit for task '{task_name}' (fingerprint={fingerprint[:8]}...)")
        return result

    async def set(
        self,
        task_name: str,
        data: Dict[str, Any],
        result: Any
    ):
        """
        设置缓存（通常由 Runner 自动调用）

        Args:
            task_name: 任务名称
            data: 任务数据
            result: 任务结果
        """
        fingerprint = self.generate_fingerprint(task_name, data)

        self._cache[fingerprint] = {
            "result": result,
            "completed_at": time.time(),
            "task_name": task_name,
        }

        self.logger.debug(f"Cache set for task '{task_name}' (fingerprint={fingerprint[:8]}...)")

    async def cleanup(self, ttl: Optional[int] = None):
        """
        清理过期缓存

        Args:
            ttl: 缓存有效期（秒），None 表示不清理
        """
        if ttl is None:
            return

        now = time.time()
        expired_keys = []

        for fingerprint, cache_entry in self._cache.items():
            completed_at = cache_entry.get("completed_at")
            if completed_at is None:
                continue

            elapsed = now - completed_at
            if elapsed > ttl:
                expired_keys.append(fingerprint)

        for key in expired_keys:
            del self._cache[key]

        if expired_keys:
            self.logger.info(f"Cleaned up {len(expired_keys)} expired cache entries")

    def get_stats(self) -> Dict[str, Any]:
        """
        获取缓存统计信息

        Returns:
            统计信息字典
        """
        return {
            "total_entries": len(self._cache),
            "tasks": [entry.get("task_name") for entry in self._cache.values()],
        }

    async def clear(self):
        """清空所有缓存"""
        self._cache.clear()
        self.logger.info("Cache cleared")

    async def close(self):
        """关闭缓存（释放资源）"""
        self.logger.info(f"MemoryCache closed (total_entries={len(self._cache)})")
