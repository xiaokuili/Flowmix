"""
灵活部署环境示例

演示：支持 SQLite、Redis、PostgreSQL 等多种存储后端
"""
import asyncio
from flowmix import Task, TaskQueue, Pub, TaskRunner, RunnerConfig, Cache

# 定义任务
task = Task(name='process')

@task.execute
async def process(data):
    print(f"  ✓ 处理任务: {data['id']}")
    await asyncio.sleep(0.05)
    return {"id": data['id'], "result": "ok"}

async def run_with_backend(backend_name: str, queue: TaskQueue, cache: Cache):
    """使用指定存储后端运行任务"""
    print(f"\n{'=' * 50}")
    print(f"📦 使用 {backend_name} 后端")
    print("=" * 50)

    # 创建发布器和运行器
    pub = Pub(queue=queue)
    runner = TaskRunner(
        tasks={'process': task},
        queue=queue,
        cache=cache,
        config=RunnerConfig(num_workers=3)
    )

    # 提交任务
    print(f"📋 提交 10 个任务")
    for i in range(10):
        await pub.push(data={'id': i}, task_name='process')

    # 执行任务
    await runner.run(auto_stop=True)

    print(f"✅ 所有任务完成")

async def demo_sqlite():
    """场景1: 单机开发环境（SQLite）"""
    queue = TaskQueue(db_path=".flowmix/flowmix_sqlite.db")
    cache = Cache(db_path=".flowmix/flowmix_sqlite.db")
    await run_with_backend("SQLite（单机开发）", queue, cache)

async def demo_redis():
    """场景2: 分布式环境（Redis）"""
    try:
        queue = TaskQueue(
            provider_type='redis',
            redis_url='redis://localhost:6379/0'
        )
        cache = Cache(
            provider_type='redis',
            redis_url='redis://localhost:6379/0'
        )
        await run_with_backend("Redis（分布式）", queue, cache)
    except Exception as e:
        print(f"\n⚠️  Redis 后端不可用: {e}")
        print("   提示：请确保 Redis 服务已启动")
        print("   安装: pip install redis")
        print("   启动: redis-server")

async def demo_postgres():
    """场景3: 生产环境（PostgreSQL）"""
    try:
        queue = TaskQueue(
            provider_type='postgres',
            postgres_dsn='postgresql://user:pass@localhost/flowmix'
        )
        cache = Cache(
            provider_type='postgres',
            postgres_dsn='postgresql://user:pass@localhost/flowmix'
        )
        await run_with_backend("PostgreSQL（生产环境）", queue, cache)
    except Exception as e:
        print(f"\n⚠️  PostgreSQL 后端不可用: {e}")
        print("   提示：请确保 PostgreSQL 服务已启动并配置正确")
        print("   安装: pip install psycopg2-binary")

async def main():
    print("""
╔══════════════════════════════════════════════════╗
║         Flowmix 灵活部署环境演示                  ║
╚══════════════════════════════════════════════════╝

支持多种存储后端：
  1. SQLite      - 适合单机开发和测试
  2. Redis       - 适合分布式部署
  3. PostgreSQL  - 适合生产环境大规模部署
""")

    # 演示 SQLite（默认始终可用）
    await demo_sqlite()

    # 演示 Redis（需要 Redis 服务）
    # await demo_redis()

    # 演示 PostgreSQL（需要 PostgreSQL 服务）
    # await demo_postgres()

    print(f"\n{'=' * 50}")
    print("""
📖 部署架构说明：

1️⃣ 单机部署（SQLite）：
   ┌─────────────────────┐
   │   TaskRunner        │
   │ (Producer+Consumer) │
   │    SQLite           │
   └─────────────────────┘

   特点：简单、轻量、适合开发和小规模应用

2️⃣ 分布式部署（Redis/PostgreSQL）：
   ┌──────────┐    ┌──────────┐    ┌──────────┐
   │ Producer │───▶│  Queue   │◀───│ Consumer │
   │  (Pub)   │    │  Redis   │    │ (Runner) │
   └──────────┘    └──────────┘    └──────────┘
                                    ┌──────────┐
                                    │ Consumer │
                                    │ (Runner) │
                                    └──────────┘

   特点：高可用、可水平扩展、适合生产环境

💡 提示：
   - 开发阶段使用 SQLite 即可
   - 生产环境推荐使用 Redis 或 PostgreSQL
   - 不同后端之间可以无缝切换，只需改变配置
""")

if __name__ == "__main__":
    asyncio.run(main())
