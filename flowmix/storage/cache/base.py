"""
缓存提供者基类（Cache Backend）

定义所有缓存后端必须实现的接口
支持基于任务指纹的去重和结果缓存
"""

import hashlib
import json
from abc import ABC, abstractmethod
from typing import Optional, Dict, Any


class CacheBackend(ABC):
    """
    缓存提供者抽象基类

    定义所有缓存后端必须实现的接口
    支持基于任务指纹的去重和结果缓存
    """

    @abstractmethod
    def generate_fingerprint(self, task_name: str, data: Dict[str, Any]) -> str:
        """
        生成任务指纹（用于去重）

        Args:
            task_name: 任务名称
            data: 任务数据

        Returns:
            哈希字符串（通常为 SHA256）

        Example:
            fingerprint = cache.generate_fingerprint("crawl", {"url": "http://example.com"})
        """
        pass

    @abstractmethod
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
        pass

    @abstractmethod
    async def close(self):
        """关闭连接或释放资源"""
        pass

    def _default_fingerprint(self, task_name: str, data: Dict[str, Any]) -> str:
        """
        默认指纹生成算法（SHA256）

        子类可以直接调用此方法，或实现自己的算法
        """
        # 标准化 JSON（排序 key，确保相同数据生成相同哈希）
        normalized = json.dumps(
            {'task': task_name, 'data': data},
            sort_keys=True,
            ensure_ascii=False
        )
        return hashlib.sha256(normalized.encode()).hexdigest()
