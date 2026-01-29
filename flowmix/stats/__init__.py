"""
Stats - 统计查询模块

分层架构设计：
1. TaskQuery - 任务链查询（任务信息、任务链条统计）
2. RunnerStats - Runner 执行统计（Worker 性能、吞吐量）
3. MonitoringQuery - 实时监控（正在处理的任务、失败任务、错误汇总）
4. Stats - 统一门面（组合上述三个模块）

支持多种后端实现：
- RedisStats: 基于 Redis 的统计查询
"""

from .base import (
    # 统一门面
    Stats,
    # 分层接口
    TaskQuery,
    RunnerStats,
    # TypedDict
    TaskInfo,
    TaskChainSummary,
    ChainStats,
    WorkerPerformance,
    WorkerInfo,
    FailedTask,
    ProcessingTask,
)
from .redis import RedisStats

__all__ = [
    # 统一门面
    'Stats',
    # 分层接口
    'TaskQuery',
    'RunnerStats',
    # TypedDict
    'TaskInfo',
    'TaskChainSummary',
    'ChainStats',
    'WorkerPerformance',
    'WorkerInfo',
    'FailedTask',
    'ProcessingTask',
    # 实现
    'RedisStats',
]
