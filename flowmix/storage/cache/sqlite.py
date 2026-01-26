"""
SQLiteCache - 基于 SQLite 的任务缓存管理

基于任务指纹实现去重和结果缓存
支持永久缓存和 TTL 过期缓存
"""

import json
import logging
from typing import Optional, Dict, Any

import aiosqlite

from .base import CacheBackend


class SQLiteCache(CacheBackend):
    """
    SQLite 缓存管理器

    特点：
    - 零外部依赖
    - 持久化存储
    - 适合单机部署、开发测试

    功能：
    - 基于任务指纹（fingerprint）实现去重
    - 支持永久缓存（适合爬虫 URL 去重）
    - 支持 TTL 过期缓存（适合 API 调用缓存）

    实现原理：
    - 任务指纹 = SHA256(task_name + data)
    - 查询数据库中 fingerprint 相同且状态为 completed 的任务
    - 如果设置了 TTL，只查找最近 TTL 秒内完成的任务

    Example:
        from flowmix.storage.cache import SQLiteCache

        # 创建缓存管理器
        cache = SQLiteCache(db_path=".flowmix/flowmix.db", queue_name="tasks")

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

    def __init__(
        self,
        db_path: str = ".flowmix/flowmix.db",
        queue_name: str = "tasks"
    ):
        """
        初始化缓存管理器

        Args:
            db_path: SQLite 数据库文件路径
            queue_name: 队列名称（表名）
        """
        self.db_path = db_path
        self.queue_name = queue_name
        self._db: Optional[aiosqlite.Connection] = None

        self.logger = logging.getLogger(__name__)
        self.logger.info(f"SQLiteCache initialized: db_path={db_path}, queue_name={queue_name}")

    async def _get_db_connection(self) -> aiosqlite.Connection:
        """获取数据库连接（用于缓存查询）"""
        if self._db is None:
            self._db = await aiosqlite.connect(
                self.db_path,
                timeout=30.0
            )
            self._db.row_factory = aiosqlite.Row
            # 启用 WAL 模式
            await self._db.execute("PRAGMA journal_mode=WAL")
        return self._db

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
        fingerprint = self.generate_fingerprint(task_name, data)
        conn = await self._get_db_connection()

        if ttl is None:
            # 永久缓存：只查找 completed 的任务
            cursor = await conn.execute(f"""
                SELECT result FROM {self.queue_name}
                WHERE fingerprint = ? AND status = 'completed'
                ORDER BY completed_at DESC
                LIMIT 1
            """, (fingerprint,))
        else:
            # 带 TTL：查找最近 ttl 秒内完成的任务
            cursor = await conn.execute(f"""
                SELECT result FROM {self.queue_name}
                WHERE fingerprint = ?
                  AND status = 'completed'
                  AND julianday('now') - completed_at < ?
                ORDER BY completed_at DESC
                LIMIT 1
            """, (fingerprint, ttl / 86400.0))  # 转换为天数

        row = await cursor.fetchone()
        if row and row['result']:
            self.logger.debug(f"Cache hit for task '{task_name}' (fingerprint={fingerprint[:8]}...)")
            return json.loads(row['result'])

        self.logger.debug(f"Cache miss for task '{task_name}' (fingerprint={fingerprint[:8]}...)")
        return None

    async def close(self):
        """关闭数据库连接"""
        if self._db is not None:
            await self._db.close()
            self._db = None
            self.logger.info("SQLiteCache connection closed")
