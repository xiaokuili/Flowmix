"""
运行所有示例

一次性展示所有功能效果
"""
import subprocess
import sys
import os
import shutil

examples = [
    ('dedup.py', '避免重复处理'),
    ('concurrency.py', '控制并发速率 - 并发数控制'),
    ('rate_limit.py', '控制并发速率 - 任务级限流'),
    ('task_dependency.py', '任务依赖 - 构建任务树'),
    ('deployment.py', '灵活部署环境 - 多种存储后端'),
    ('stats.py', '状态查询 - 实时统计'),
    ('monitoring.py', '监控告警 - 告警规则'),
]

def clean_db():
    """清理数据库"""
    if os.path.exists('.flowmix'):
        shutil.rmtree('.flowmix')

def run_example(filename, description):
    """运行单个示例"""
    print(f"\n{'='*60}")
    print(f"运行示例: {description}")
    print(f"{'='*60}\n")

    # 清理数据库
    clean_db()

    # 运行示例
    result = subprocess.run(
        [sys.executable, f'examples/{filename}'],
        capture_output=False,
        text=True
    )

    if result.returncode != 0:
        print(f"⚠️  示例 {filename} 运行出错")

    print("\n")

def main():
    print("🚀 Flowmix 功能演示")
    print("="*60)

    for filename, description in examples:
        try:
            run_example(filename, description)
        except KeyboardInterrupt:
            print("\n\n用户中断，退出演示")
            sys.exit(0)
        except Exception as e:
            print(f"❌ 运行 {filename} 时出错: {e}")

    print("="*60)
    print("✅ 所有示例运行完成")

if __name__ == "__main__":
    main()
