"""
Sender - 任务提交模块

提供任务提交能力：
- Pub: 任务发布器（手动提交）
- Cron: 定时任务提交器（自动提交）
"""

from .pub import Pub

# Cron 为可选依赖
try:
    from .cron import Cron
except ImportError:
    Cron = None

__all__ = [
    'Pub',
    'Cron',
]
