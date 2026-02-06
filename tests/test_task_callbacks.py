import pytest

from flowmix.runner.task import Task


@pytest.mark.asyncio
async def test_on_success_sync_without_msg_id():
    task = Task(name="sync-no-id")

    @task.execute
    def execute(data):
        return {"value": data["value"] * 2}

    captured = {}

    @task.on_success
    def on_success(data, result):
        captured["data"] = data
        captured["result"] = result

    result = await task.run({"value": 21}, msg_id=7)

    assert result == {"value": 42}
    assert captured["data"] == {"value": 21}
    assert captured["result"] == {"value": 42}


@pytest.mark.asyncio
async def test_on_success_sync_with_msg_id():
    task = Task(name="sync-with-id")

    @task.execute
    def execute(data):
        return data["payload"]

    captured = {}

    @task.on_success
    def on_success(data, result, msg_id):
        captured["msg_id"] = msg_id
        captured["result"] = result

    await task.run({"payload": "ok"}, msg_id=123)

    assert captured == {"msg_id": 123, "result": "ok"}


@pytest.mark.asyncio
async def test_on_success_async_with_msg_id():
    task = Task(name="async-success")

    @task.execute
    async def execute(data):
        return data["items"]

    received = {}

    @task.on_success
    async def on_success(data, result, msg_id):
        received[msg_id] = list(result)

    await task.run({"items": [1, 2, 3]}, msg_id=999)

    assert received == {999: [1, 2, 3]}


@pytest.mark.asyncio
async def test_on_failure_sync_with_msg_id():
    task = Task(name="failure-sync")

    @task.execute
    def execute(_):
        raise RuntimeError("boom")

    captured = {}

    @task.on_failure
    def on_failure(data, error, msg_id):
        captured["error"] = str(error)
        captured["msg_id"] = msg_id
        captured["data"] = data

    with pytest.raises(RuntimeError):
        await task.run({"input": "triggers"}, msg_id=55)

    assert captured == {
        "error": "boom",
        "msg_id": 55,
        "data": {"input": "triggers"},
    }


@pytest.mark.asyncio
async def test_on_failure_async_without_msg_id():
    task = Task(name="failure-async")

    @task.execute
    async def execute(_):
        raise ValueError("bad")

    seen = {}

    @task.on_failure
    async def on_failure(data, error):
        seen["error"] = str(error)
        seen["data"] = data

    with pytest.raises(ValueError):
        await task.run({"payload": 1}, msg_id=101)

    assert seen == {
        "error": "bad",
        "data": {"payload": 1},
    }
