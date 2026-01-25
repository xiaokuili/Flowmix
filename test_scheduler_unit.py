"""
单元测试 Scheduler - 验证核心功能

测试内容：
1. Scheduler 初始化
2. 添加调度任务
3. Cron 表达式验证
4. 手动触发测试（不等待实际时间）
"""

import asyncio
import logging
from datetime import datetime, timedelta

from flowmix import Task, Worker, Scheduler

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


async def test_scheduler_basic():
    """测试 1: 基本功能 - 初始化和添加任务"""
    print("\n" + "=" * 70)
    print("测试 1: Scheduler 基本功能")
    print("=" * 70)

    # 创建简单的任务
    task = Task(name='test_task')

    @task.execute
    async def run(data):
        return {"result": "success"}

    # 创建 Worker
    worker = Worker(
        tasks={'test_task': task},
        num_workers=1,
        db_path=".flowmix/scheduler_unit_test.db",
        queue_name="unit_test_tasks"
    )

    # 创建 Scheduler
    scheduler = Scheduler(worker, check_interval=5.0)

    # 添加调度任务
    scheduled_task = scheduler.add_cron(
        cron='0 8 * * *',  # 每天 8 点
        task_name='test_task',
        data={'test': 'daily'}
    )

    # 验证
    assert len(scheduler.get_tasks()) == 1, "应该有 1 个调度任务"
    assert scheduled_task.cron == '0 8 * * *', "Cron 表达式应该正确"
    assert scheduled_task.task_name == 'test_task', "任务名称应该正确"
    assert scheduled_task.next_run is not None, "应该计算出下次运行时间"

    print(f"✓ Scheduler 初始化成功")
    print(f"✓ 调度任务添加成功: {scheduled_task}")
    print(f"  - Cron: {scheduled_task.cron}")
    print(f"  - 下次运行: {scheduled_task.next_run}")

    return True


async def test_cron_expressions():
    """测试 2: 各种 Cron 表达式"""
    print("\n" + "=" * 70)
    print("测试 2: Cron 表达式验证")
    print("=" * 70)

    task = Task(name='test')

    @task.execute
    async def run(data):
        return {"result": "ok"}

    worker = Worker(
        tasks={'test': task},
        num_workers=1,
        db_path=".flowmix/scheduler_unit_test.db"
    )

    scheduler = Scheduler(worker)

    # 测试各种 cron 表达式
    test_cases = [
        ('* * * * *', '每分钟'),
        ('0 * * * *', '每小时'),
        ('0 8 * * *', '每天 8:00'),
        ('0 0 * * 0', '每周日 0:00'),
        ('0 0 1 * *', '每月 1 号 0:00'),
        ('*/5 * * * *', '每 5 分钟'),
        ('0 */2 * * *', '每 2 小时'),
    ]

    for cron, description in test_cases:
        scheduled_task = scheduler.add_cron(
            cron=cron,
            task_name='test',
            data={'type': description}
        )
        print(f"✓ {description:20s} - cron='{cron:15s}' - 下次运行: {scheduled_task.next_run}")

    assert len(scheduler.get_tasks()) == len(test_cases), f"应该有 {len(test_cases)} 个调度任务"
    print(f"\n✓ 所有 Cron 表达式验证通过")

    return True


async def test_invalid_cron():
    """测试 3: 无效的 Cron 表达式"""
    print("\n" + "=" * 70)
    print("测试 3: 无效 Cron 表达式处理")
    print("=" * 70)

    task = Task(name='test')

    @task.execute
    async def run(data):
        return {"result": "ok"}

    worker = Worker(
        tasks={'test': task},
        num_workers=1,
        db_path=".flowmix/scheduler_unit_test.db"
    )

    scheduler = Scheduler(worker)

    # 测试无效的 cron 表达式
    invalid_crons = [
        'invalid',
        '60 * * * *',  # 分钟超出范围
        '* 25 * * *',  # 小时超出范围
        '* * 32 * *',  # 日期超出范围
    ]

    for cron in invalid_crons:
        try:
            scheduler.add_cron(
                cron=cron,
                task_name='test',
                data={'test': 'invalid'}
            )
            print(f"✗ 应该拒绝无效的 cron: '{cron}'")
            return False
        except (ValueError, Exception) as e:
            print(f"✓ 正确拒绝无效 cron '{cron}': {type(e).__name__}")

    print(f"\n✓ 无效 Cron 表达式处理正确")
    return True


async def test_manual_trigger():
    """测试 4: 手动测试任务触发逻辑"""
    print("\n" + "=" * 70)
    print("测试 4: 任务触发机制")
    print("=" * 70)

    execution_log = []

    task = Task(name='trigger_test')

    @task.execute
    async def run(data):
        execution_log.append({
            'time': datetime.now(),
            'data': data
        })
        print(f"  ✓ 任务执行: {data}")
        return {"result": "ok"}

    worker = Worker(
        tasks={'trigger_test': task},
        num_workers=1,
        db_path=".flowmix/scheduler_unit_test.db"
    )

    # 手动提交几个任务（模拟调度器行为）
    print("手动提交 3 个任务（模拟调度器）...")
    for i in range(3):
        await worker.push(
            data={'test': f'manual_{i}', 'index': i},
            task_name='trigger_test'
        )
        print(f"  - 提交任务 {i + 1}")

    # 运行 worker 处理任务
    print("\n启动 Worker 处理任务...")
    worker_task = asyncio.create_task(worker.run(auto_stop=True, max_idle_time=2.0))

    # 等待 worker 完成
    await worker_task

    # 验证
    assert len(execution_log) == 3, f"应该执行 3 次，实际执行了 {len(execution_log)} 次"
    print(f"\n✓ 任务触发机制正常，共执行 {len(execution_log)} 次")

    return True


async def main():
    """运行所有测试"""
    print("\n" + "=" * 70)
    print("Scheduler 单元测试")
    print("=" * 70)

    tests = [
        ("基本功能", test_scheduler_basic),
        ("Cron 表达式", test_cron_expressions),
        ("无效输入处理", test_invalid_cron),
        ("任务触发机制", test_manual_trigger),
    ]

    results = []
    for name, test_func in tests:
        try:
            result = await test_func()
            results.append((name, result))
        except Exception as e:
            logger.error(f"测试失败: {name}", exc_info=True)
            results.append((name, False))

    # 总结
    print("\n" + "=" * 70)
    print("测试总结")
    print("=" * 70)
    for name, result in results:
        status = "✓ 通过" if result else "✗ 失败"
        print(f"{status} - {name}")

    passed = sum(1 for _, r in results if r)
    total = len(results)
    print(f"\n总计: {passed}/{total} 通过")

    if passed == total:
        print("\n🎉 所有测试通过！Scheduler 功能正常")
        return True
    else:
        print("\n❌ 部分测试失败")
        return False


if __name__ == "__main__":
    success = asyncio.run(main())
    exit(0 if success else 1)
