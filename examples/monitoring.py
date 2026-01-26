"""
监控告警示例

演示：实时监控任务执行状态，设置告警阈值
"""
import asyncio
import time
from flowmix import Task, TaskQueue, Pub, TaskRunner, RunnerConfig, Cache, Stats

# 创建任务
task = Task(name='api_call')

@task.execute
async def api_call(data):
    task_id = data['id']
    await asyncio.sleep(0.05)

    # 模拟不同的失败场景
    if task_id % 10 == 0:
        raise Exception("网络超时")
    elif task_id % 7 == 0:
        raise Exception("API限流")
    elif task_id % 13 == 0:
        raise Exception("认证失败")

    return {"id": task_id, "status": "success"}

@task.on_failure
async def on_failure(data, error):
    # 记录失败任务
    print(f"  ❌ 任务 {data['id']} 失败: {error}")

def send_alert(message: str):
    """模拟发送告警"""
    print(f"\n🚨 告警触发: {message}")

def check_metrics(stats: Stats):
    """检查指标并触发告警"""
    overall = stats.get_worker_stats()

    # 告警规则1: 成功率低于 80%
    if overall['success_rate'] < 0.8:
        send_alert(
            f"任务成功率过低: {overall['success_rate']*100:.1f}% "
            f"(阈值: 80%)"
        )

    # 告警规则2: 失败任务数超过 10
    if overall['failed'] > 10:
        send_alert(
            f"失败任务数过多: {overall['failed']} 个 "
            f"(阈值: 10)"
        )

    # 告警规则3: 特定错误频繁出现
    errors = stats.get_error_summary()
    for error, count in errors.items():
        if count >= 5:
            send_alert(
                f"错误频繁: '{error}' 出现 {count} 次 "
                f"(阈值: 5)"
            )

    # 告警规则4: 平均耗时过长
    if overall['avg_duration_seconds'] > 1.0:
        send_alert(
            f"任务平均耗时过长: {overall['avg_duration_seconds']:.2f}秒 "
            f"(阈值: 1.0秒)"
        )

async def monitor_in_realtime(queue_name: str, interval: int = 5):
    """实时监控（后台任务）"""
    stats = Stats(db_path='.flowmix/flowmix.db', queue_name=queue_name)

    while True:
        await asyncio.sleep(interval)

        # 获取统计数据
        overall = stats.get_worker_stats()

        if overall['total'] == 0:
            continue

        # 打印监控信息
        print(f"\n📊 实时监控 ({time.strftime('%H:%M:%S')})")
        print(f"  总任务: {overall['total']}")
        print(f"  已完成: {overall['completed']}")
        print(f"  成功率: {overall['success_rate']*100:.1f}%")
        print(f"  平均耗时: {overall['avg_duration_seconds']:.3f}秒")

        # 检查告警
        check_metrics(stats)

async def main():
    # 初始化队列和缓存
    queue_name = "monitoring_test"
    queue = TaskQueue(db_path=".flowmix/flowmix.db", queue_name=queue_name)
    cache = Cache(db_path=".flowmix/flowmix.db", queue_name=queue_name)

    print("""
╔══════════════════════════════════════════════════╗
║         Flowmix 监控告警演示                      ║
╚══════════════════════════════════════════════════╝

监控指标：
  ✓ 任务成功率
  ✓ 失败任务数
  ✓ 错误类型分布
  ✓ 平均执行耗时

告警规则：
  1. 成功率 < 80%
  2. 失败任务数 > 10
  3. 特定错误出现 >= 5 次
  4. 平均耗时 > 1.0 秒
""")

    # 创建发布器和运行器
    pub = Pub(queue=queue)
    runner = TaskRunner(
        tasks={'api_call': task},
        queue=queue,
        cache=cache,
        config=RunnerConfig(num_workers=5)
    )

    print("📋 提交 50 个任务 (模拟多种失败场景)")
    print("-" * 50)

    # 提交任务
    for i in range(50):
        await pub.push(data={'id': i}, task_name='api_call')

    # 执行任务
    await runner.run(auto_stop=True)

    # 执行后分析
    print("\n" + "=" * 50)
    print("📊 最终统计报告")
    print("=" * 50)

    stats = Stats(db_path='.flowmix/flowmix.db', queue_name=queue_name)

    # 整体统计
    overall = stats.get_worker_stats()
    print(f"\n总体情况:")
    print(f"  总任务数: {overall['total']}")
    print(f"  已完成: {overall['completed']}")
    print(f"  成功: {overall['completed'] - overall['failed']}")
    print(f"  失败: {overall['failed']}")
    print(f"  成功率: {overall['success_rate']*100:.1f}%")
    print(f"  平均耗时: {overall['avg_duration_seconds']:.3f} 秒")

    # 按任务类型统计
    print(f"\n按任务类型:")
    by_type = stats.get_worker_stats_by_task_type()
    for task_type, task_stats in by_type.items():
        print(f"  {task_type}:")
        print(f"    - 完成: {task_stats['completed']}/{task_stats['total']}")
        print(f"    - 成功率: {task_stats['success_rate']*100:.1f}%")

    # 错误汇总
    print(f"\n错误分析:")
    errors = stats.get_error_summary()
    for error, count in errors.items():
        percentage = count / overall['failed'] * 100 if overall['failed'] > 0 else 0
        print(f"  {error}: {count} 次 ({percentage:.1f}%)")

    # 失败任务列表
    print(f"\n失败任务 (最近 10 个):")
    failed = stats.get_failed_tasks(limit=10)
    for task in failed:
        print(f"  [{task['task_id']}] {task['error']}")

    # 执行告警检查
    print(f"\n{'=' * 50}")
    print("🔍 告警检查")
    print("=" * 50)
    check_metrics(stats)

    print(f"\n{'=' * 50}")
    print("""
💡 集成到监控系统：

1️⃣ Prometheus 集成：
   from prometheus_client import Gauge

   success_rate = Gauge('flowmix_success_rate', 'Task success rate')
   stats = Stats(db_path='.flowmix/flowmix.db')
   overall = stats.get_worker_stats()
   success_rate.set(overall['success_rate'])

2️⃣ 邮件告警：
   if overall['success_rate'] < 0.8:
       send_email(
           to='admin@example.com',
           subject='Flowmix 告警：成功率过低',
           body=f"当前成功率: {overall['success_rate']*100:.1f}%"
       )

3️⃣ Slack/钉钉告警：
   if overall['failed'] > 10:
       send_webhook(
           url=WEBHOOK_URL,
           message=f"失败任务数: {overall['failed']}"
       )

4️⃣ 自定义监控面板：
   - 使用 Stats API 获取实时数据
   - 可视化展示任务状态、成功率、错误分布等
   - 设置自定义告警规则和阈值
""")

if __name__ == "__main__":
    asyncio.run(main())
