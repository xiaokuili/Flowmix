"""
内存队列实现 - 用于测试和简单场景
"""

import asyncio
from typing import Dict, Any, Optional, List
from .base import Queue


class _MemoryQueueState:
    """内存队列的共享状态"""

    def __init__(self):
        self._queues: Dict[str, Dict[str, Any]] = {}

    def get_queue_state(self, queue_name: str) -> Dict[str, Any]:
        """获取或创建队列状态"""
        if queue_name not in self._queues:
            self._queues[queue_name] = {
                "_queue": asyncio.Queue(),
                "_processing": {},
                "_counter": 0,
                "_closed": False,
            }
        return self._queues[queue_name]


# 全局共享状态
_global_state = _MemoryQueueState()


class MemoryQueue(Queue):
    """
    内存队列实现

    适用于测试、开发环境和小规模场景
    相同 queue_name 的实例共享同一个队列
    """

    def __init__(self, queue_name: str = "default"):
        """
        初始化内存队列

        Args:
            queue_name: 队列名称（相同名称的实例共享同一个队列）
        """
        self.queue_name = queue_name
        # 使用全局共享状态
        self._state = _global_state.get_queue_state(queue_name)

    async def push(
        self,
        data: Dict[str, Any],
        priority: int = 0,
        parent_id: Optional[int] = None,
        task_name: Optional[str] = None,
    ) -> int:
        """推送消息到队列"""
        if self._state["_closed"]:
            raise RuntimeError("Queue is closed")

        self._state["_counter"] += 1
        msg_id = self._state["_counter"]

        message = {
            "id": msg_id,
            "data": data,
            "task_name": task_name or data.get("task_name", "unknown"),
            "priority": priority,
            "parent_id": parent_id,
            "created_at": asyncio.get_event_loop().time(),
        }

        queue = self._state["_queue"]

        # 简单的优先级处理：高优先级的插入到前面
        if priority > 5:
            # 对于高优先级消息，临时存储然后重新构建队列
            temp_list = []
            while not queue.empty():
                try:
                    temp_list.append(queue.get_nowait())
                except asyncio.QueueEmpty:
                    break

            # 插入高优先级消息
            await queue.put(message)

            # 重新插入其他消息（按优先级降序）
            temp_list.sort(key=lambda x: x.get("priority", 0), reverse=True)
            for item in temp_list:
                await queue.put(item)
        else:
            await queue.put(message)

        return msg_id

    async def pop(self, consumer_name: str = "default") -> Optional[Dict[str, Any]]:
        """从队列弹出消息"""
        if self._state["_closed"]:
            return None

        try:
            queue = self._state["_queue"]
            processing = self._state["_processing"]
            message = await asyncio.wait_for(queue.get(), timeout=0.1)
            processing[message["id"]] = {
                "message": message,
                "consumer": consumer_name,
                "started_at": asyncio.get_event_loop().time(),
            }
            return message
        except asyncio.TimeoutError:
            return None

    async def ack(
        self,
        message_id: int,
        failed: bool = False,
        error: Optional[str] = None,
        result: Optional[Any] = None,
        fingerprint: Optional[str] = None,
    ):
        """确认消息处理完成"""
        processing = self._state["_processing"]
        if message_id in processing:
            del processing[message_id]

    async def size(self) -> int:
        """获取队列大小"""
        return self._state["_queue"].qsize()

    async def clear(self):
        """清空队列"""
        queue = self._state["_queue"]
        while not queue.empty():
            try:
                queue.get_nowait()
            except asyncio.QueueEmpty:
                break

    async def clear_all(self):
        """清空所有队列（包括处理中的消息）"""
        await self.clear()
        self._state["_processing"].clear()

    async def close(self):
        """关闭队列"""
        self._state["_closed"] = True
        # 清空队列
        await self.clear()
        # 清理处理中的消息
        self._state["_processing"].clear()
