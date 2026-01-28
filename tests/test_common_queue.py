"""
测试 flowmix.common.queue 模块
"""

import asyncio
import pytest
from flowmix.common import RedisPool, SQLitePool, RedisQueue, SQLiteQueue


class TestSQLiteQueue:
    """测试 SQLiteQueue"""

    @pytest.fixture
    async def queue(self):
        """创建测试队列"""
        pool = await SQLitePool.get_instance(':memory:', pool_size=3)
        queue = SQLiteQueue(pool=pool, queue_name='test_tasks')
        await queue.clear_all()
        yield queue
        await queue.close()
        await pool.reset()

    @pytest.mark.asyncio
    async def test_push_and_pop(self, queue):
        """测试推送和取出任务"""
        # 推送任务
        msg_id = await queue.push(
            data={'url': 'https://example.com'},
            task_name='crawl',
            priority=5
        )
        assert msg_id > 0

        # 取出任务
        message = await queue.pop('worker-1')
        assert message is not None
        assert message['id'] == msg_id
        assert message['task_name'] == 'crawl'
        assert message['data'] == {'url': 'https://example.com'}

    @pytest.mark.asyncio
    async def test_priority_order(self, queue):
        """测试优先级排序"""
        # 推送不同优先级的任务
        id1 = await queue.push(data={'priority': 'low'}, priority=1)
        id2 = await queue.push(data={'priority': 'high'}, priority=10)
        id3 = await queue.push(data={'priority': 'medium'}, priority=5)

        # 应该按优先级降序取出
        msg1 = await queue.pop('worker-1')
        assert msg1['id'] == id2  # priority=10

        msg2 = await queue.pop('worker-1')
        assert msg2['id'] == id3  # priority=5

        msg3 = await queue.pop('worker-1')
        assert msg3['id'] == id1  # priority=1

    @pytest.mark.asyncio
    async def test_ack(self, queue):
        """测试确认任务"""
        # 推送并取出任务
        msg_id = await queue.push(data={'test': 'data'})
        message = await queue.pop('worker-1')

        # 确认任务完成
        await queue.ack(
            message_id=message['id'],
            failed=False,
            result={'status': 'ok'},
            fingerprint='test123'
        )

        # 验证不会再次取出
        message2 = await queue.pop('worker-1')
        assert message2 is None

    @pytest.mark.asyncio
    async def test_parent_child_relationship(self, queue):
        """测试父子任务关系"""
        # 推送父任务
        parent_id = await queue.push(data={'parent': True}, task_name='parent')
        parent_msg = await queue.pop('worker-1')

        # 推送子任务
        child_id = await queue.push(
            data={'child': True},
            task_name='child',
            parent_id=parent_id
        )

        # 确认父任务完成
        await queue.ack(parent_msg['id'], failed=False, result={'done': True})

        # 确认子任务完成
        child_msg = await queue.pop('worker-1')
        await queue.ack(child_msg['id'], failed=False, result={'done': True})

        # 父任务应该更新为 'done' 状态
        # 这里可以添加更多验证逻辑

    @pytest.mark.asyncio
    async def test_pending_count(self, queue):
        """测试待处理任务统计"""
        # 初始为 0
        count = await queue.get_pending_count()
        assert count == 0

        # 推送 3 个任务
        await queue.push(data={'a': 1})
        await queue.push(data={'b': 2})
        await queue.push(data={'c': 3})

        count = await queue.get_pending_count()
        assert count == 3

        # 取出一个任务
        msg = await queue.pop('worker-1')
        count = await queue.get_pending_count()
        assert count == 2

        # 确认任务完成
        await queue.ack(msg['id'], failed=False)
        count = await queue.get_pending_count()
        assert count == 2

    @pytest.mark.asyncio
    async def test_clear_all(self, queue):
        """测试清空队列"""
        # 推送任务
        await queue.push(data={'test': 1})
        await queue.push(data={'test': 2})

        total = await queue.get_stream_length()
        assert total == 2

        # 清空
        await queue.clear_all()

        total = await queue.get_stream_length()
        assert total == 0


@pytest.mark.skipif(
    True,  # 默认跳过，需要 Redis 环境
    reason="需要 Redis 环境"
)
class TestRedisQueue:
    """测试 RedisQueue"""

    @pytest.fixture
    async def queue(self):
        """创建测试队列"""
        pool = await RedisPool.get_instance('redis://localhost:6379/0')
        queue = RedisQueue(pool=pool, queue_name='test_tasks')
        await queue.clear_all()
        yield queue
        await queue.close()
        await pool.reset()

    @pytest.mark.asyncio
    async def test_push_and_pop(self, queue):
        """测试推送和取出任务"""
        msg_id = await queue.push(
            data={'url': 'https://example.com'},
            task_name='crawl',
            priority=5
        )
        assert msg_id > 0

        message = await queue.pop('worker-1')
        assert message is not None
        assert message['id'] == msg_id
        assert message['task_name'] == 'crawl'

    @pytest.mark.asyncio
    async def test_priority_order(self, queue):
        """测试优先级排序"""
        id1 = await queue.push(data={'priority': 'low'}, priority=1)
        id2 = await queue.push(data={'priority': 'high'}, priority=10)
        id3 = await queue.push(data={'priority': 'medium'}, priority=5)

        msg1 = await queue.pop('worker-1')
        assert msg1['id'] == id2

        msg2 = await queue.pop('worker-1')
        assert msg2['id'] == id3

        msg3 = await queue.pop('worker-1')
        assert msg3['id'] == id1
