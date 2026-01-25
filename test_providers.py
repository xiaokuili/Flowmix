"""测试多后端支持"""
import asyncio
import os
from flowmix import Task, Worker, Manager
from flowmix.providers import SQLiteProvider


async def test_sqlite_provider():
    """测试 SQLiteProvider"""
    print("🧪 测试 SQLiteProvider")

    # 创建 Task
    task = Task(name='test')

    @task.execute
    async def process(data):
        print(f"  Processing: {data['value']}")
        await asyncio.sleep(0.1)
        return data['value'] * 2

    # 方式1: 直接使用 Manager（默认 SQLite）
    print("\n📦 方式1: 使用默认 Manager")
    worker1 = Worker(
        tasks=task,
        num_workers=2,
        db_path="test_provider_default.db"
    )

    await worker1.push({'value': 1})
    await worker1.push({'value': 2})

    # 启动并运行（auto_stop 模式）
    await worker1.run(auto_stop=True)

    stats1 = worker1.get_stats()
    print(f"  ✅ 统计: 成功 {stats1['success']}/{stats1['processed']}")

    # 清理
    await worker1._manager.close()
    if os.path.exists("test_provider_default.db"):
        os.remove("test_provider_default.db")

    # 方式2: 显式传入 SQLiteProvider
    print("\n📦 方式2: 显式传入 SQLiteProvider")
    provider = SQLiteProvider(
        db_path="test_provider_explicit.db",
        queue_name="tasks"
    )

    manager2 = Manager(provider=provider)
    worker2 = Worker(
        tasks=task,
        manager=manager2,
        num_workers=2
    )

    await worker2.push({'value': 3})
    await worker2.push({'value': 4})

    await worker2.run(auto_stop=True)

    stats2 = worker2.get_stats()
    print(f"  ✅ 统计: 成功 {stats2['success']}/{stats2['processed']}")

    # 清理
    await worker2._manager.close()
    if os.path.exists("test_provider_explicit.db"):
        os.remove("test_provider_explicit.db")

    print("\n✅ SQLiteProvider 测试完成！")


async def test_redis_provider():
    """测试 RedisProvider（需要 Redis 服务）"""
    try:
        from flowmix.providers import RedisProvider

        print("\n🧪 测试 RedisProvider")

        # 创建 Task
        task = Task(name='test')

        @task.execute
        async def process(data):
            print(f"  Processing: {data['value']}")
            await asyncio.sleep(0.1)
            return data['value'] * 2

        # 使用 RedisProvider
        provider = RedisProvider(
            redis_url="redis://localhost:6379/0",
            queue_name="test_tasks"
        )

        manager = Manager(provider=provider)
        worker = Worker(
            tasks=task,
            manager=manager,
            num_workers=2
        )

        await worker.push({'value': 5})
        await worker.push({'value': 6})

        await worker.run(auto_stop=True)

        stats = worker.get_stats()
        print(f"  ✅ 统计: 成功 {stats['success']}/{stats['processed']}")

        # 清理
        await worker._manager.close()

        print("\n✅ RedisProvider 测试完成！")

    except ImportError:
        print("\n⚠️  RedisProvider 未安装，跳过测试")
        print("   安装: pip install 'flowmix[redis]'")
    except Exception as e:
        print(f"\n⚠️  RedisProvider 测试失败: {e}")
        print("   请确保 Redis 服务正在运行")


async def main():
    print("=" * 60)
    print("测试多后端支持")
    print("=" * 60)

    # 测试 SQLite（默认后端，总是可用）
    await test_sqlite_provider()

    # 测试 Redis（可选后端）
    await test_redis_provider()

    print("\n" + "=" * 60)
    print("✅ 所有测试完成！")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
