import asyncio

import pytest

from flowmix.common.queue.memory import MemoryQueue
from flowmix.runner.engine import TaskEngine
from flowmix.runner.limit.memory import MemoryRateLimiter
from flowmix.runner.task import Task


@pytest.mark.asyncio
async def test_memory_queue_can_recover_processing_messages():
    queue_name = "test-recover-processing"
    queue = MemoryQueue(queue_name=queue_name)
    await queue.clear_all()

    msg_id = await queue.push({"value": 1}, task_name="demo")
    popped = await queue.pop("worker-a")

    assert popped is not None
    assert popped["id"] == msg_id

    recovered = await queue.recover_processing_tasks()
    assert recovered == 1

    recovered_msg = await queue.pop("worker-b")
    assert recovered_msg is not None
    assert recovered_msg["id"] == msg_id


@pytest.mark.asyncio
async def test_task_engine_marks_timeout_as_failed():
    task = Task(name="slow-task")

    @task.execute
    async def run(_):
        await asyncio.sleep(0.05)
        return {"ok": True}

    queue = MemoryQueue(queue_name="test-engine-timeout")
    engine = TaskEngine(
        cache=None,
        limiter=MemoryRateLimiter(),
        queue=queue,
        max_retries=0,
        execution_timeout=0.01,
    )

    result, status = await engine.execute(
        msg={"id": 1, "task_name": "slow-task", "data": {}},
        task=task,
        worker_name="worker-timeout",
    )

    assert status == "failed"
    assert "timed out" in result["error"]
