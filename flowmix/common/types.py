"""
Flowmix 标准类型定义

为 AI 和开发者提供明确的类型约束，避免隐式数据结构导致的理解困难。
"""

from typing import TypedDict, Optional, Any, Literal
from datetime import datetime


# ============================================================================
# 消息类型 (Message Types)
# ============================================================================

class TaskMessage(TypedDict, total=False):
    """
    标准任务消息格式

    这是队列中存储的消息结构，也是 Task.run() 接收的参数。

    注意事项:
    - `data` 字段必须是可 JSON 序列化的字典
    - `id` 由队列自动生成，不要手动设置
    - `fingerprint` 由 Cache 自动生成（如果启用 dedup）
    - `parent_id` 由 task.callback() 自动设置

    示例:
    ```python
    msg: TaskMessage = {
        "id": 123,
        "task_name": "crawl",
        "data": {"url": "http://example.com", "depth": 0},
        "priority": 10,
        "parent_id": None
    }
    ```
    """
    id: int                             # 消息 ID（自增，由队列生成）
    task_name: str                      # 任务名称（必须在 TaskRunner 中注册）
    data: dict[str, Any]                # 任务数据（必须可 JSON 序列化）
    priority: int                       # 优先级（0-100，越高越优先）
    parent_id: Optional[int]            # 父任务 ID（用于构建任务树）
    created_at: float                   # 创建时间戳（Unix 时间）
    fingerprint: Optional[str]          # 任务指纹（SHA256，用于去重）
    retries: int                        # 重试次数


class TaskResult(TypedDict, total=False):
    """
    任务执行结果

    由 @task.execute 装饰的函数返回，存储在 Queue.ack() 中。

    注意事项:
    - 返回值必须可 JSON 序列化
    - 如果启用 dedup，结果会被缓存

    示例:
    ```python
    @task.execute
    async def crawl(data: dict) -> TaskResult:
        return {
            "url": data["url"],
            "status": "ok",
            "content": "...",
            "links": ["http://..."]
        }
    ```
    """
    status: str                         # 状态（通常是 "ok" 或 "error"）
    # 其他字段由用户自定义


class TaskError(TypedDict):
    """
    任务执行错误信息

    当任务失败时，由 TaskEngine 生成，传递给 @task.on_failure。

    示例:
    ```python
    @task.on_failure
    async def on_failure(data: dict, error: TaskError):
        print(f"Task failed: {error['message']}")
    ```
    """
    type: str                           # 异常类型（例如 "ValueError"）
    message: str                        # 错误消息
    traceback: str                      # 堆栈跟踪（格式化后的字符串）
    location: Optional[str]             # 错误位置（文件:行号）


# ============================================================================
# 配置类型 (Configuration Types)
# ============================================================================

class RunnerConfigDict(TypedDict, total=False):
    """
    TaskRunner 配置

    对应 RunnerConfig 类的参数。

    示例:
    ```python
    config = RunnerConfigDict(
        num_workers=10,
        max_retries=3,
        retry_delay=1.0
    )
    runner = TaskRunner(tasks={...}, url="redis://...", config=config)
    ```
    """
    num_workers: int                    # Worker 数量（并发度）
    max_retries: int                    # 最大重试次数
    retry_delay: float                  # 重试延迟（秒）
    batch_size: int                     # 每次拉取的任务数


class TaskConfigDict(TypedDict, total=False):
    """
    Task 配置

    对应 Task 类的参数。

    示例:
    ```python
    task_config = TaskConfigDict(
        name="crawl",
        concurrency_limit=10,
        dedup=True,
        dedup_ttl=3600
    )
    task = Task(**task_config)
    ```
    """
    name: str                           # 任务名称（唯一标识）
    concurrency_limit: Optional[int]    # 并发限制（每秒最大并发数）
    dedup: bool                         # 是否启用去重（结果缓存）
    dedup_ttl: Optional[int]            # 缓存过期时间（秒，None=永久）


# ============================================================================
# 统计类型 (Statistics Types)
# ============================================================================

class TaskInfo(TypedDict):
    """
    单个任务的详细信息

    由 Stats.task.get_task() 返回。

    示例:
    ```python
    task_info = await stats.task.get_task(task_id=123)
    print(f"Task {task_info['id']} status: {task_info['status']}")
    ```
    """
    id: int                             # 任务 ID
    task_name: str                      # 任务名称
    data: dict[str, Any]                # 任务数据
    status: Literal["pending", "success", "failed"]  # 任务状态
    priority: int                       # 优先级
    parent_id: Optional[int]            # 父任务 ID
    created_at: float                   # 创建时间戳
    started_at: Optional[float]         # 开始时间戳
    finished_at: Optional[float]        # 完成时间戳
    result: Optional[dict[str, Any]]    # 执行结果
    error: Optional[str]                # 错误消息
    retries: int                        # 重试次数
    fingerprint: Optional[str]          # 任务指纹


class ChainSummary(TypedDict):
    """
    任务链摘要

    由 Stats.task.get_chain_summary() 返回。

    示例:
    ```python
    summary = await stats.task.get_chain_summary(root_id=1)
    print(f"Total: {summary['total']}, Success: {summary['success']}")
    ```
    """
    root_id: int                        # 根任务 ID
    total: int                          # 总任务数
    success: int                        # 成功任务数
    failed: int                         # 失败任务数
    pending: int                        # 待处理任务数


class RunnerPerformance(TypedDict):
    """
    Runner 性能统计

    由 Stats.runner.get_performance() 返回。

    示例:
    ```python
    perf = await stats.runner.get_performance()
    print(f"Throughput: {perf['throughput']:.2f} tasks/sec")
    ```
    """
    total_tasks: int                    # 总任务数
    success_rate: float                 # 成功率（0-1）
    avg_duration: float                 # 平均执行时间（秒）
    throughput: float                   # 吞吐量（任务/秒）
    active_workers: int                 # 活跃 Worker 数


class WorkerInfo(TypedDict):
    """
    Worker 信息

    由 Stats.runner.list_workers() 返回。

    示例:
    ```python
    workers = await stats.runner.list_workers()
    for worker in workers:
        print(f"{worker['name']}: {worker['status']}")
    ```
    """
    name: str                           # Worker 名称
    status: Literal["idle", "busy"]     # Worker 状态
    current_task: Optional[str]         # 当前执行的任务名称
    total_completed: int                # 已完成任务数
    last_active: float                  # 最后活跃时间戳


# ============================================================================
# 回调函数类型 (Callback Types)
# ============================================================================

from typing import Awaitable, Callable

# Task.execute 装饰的函数类型
TaskExecuteFunc = Callable[[dict[str, Any]], Awaitable[Optional[dict[str, Any]]]]

# Task.on_success 装饰的函数类型
TaskSuccessFunc = Callable[[dict[str, Any], dict[str, Any]], Awaitable[None]]

# Task.on_failure 装饰的函数类型
TaskFailureFunc = Callable[[dict[str, Any], TaskError], Awaitable[None]]

# Cron 的 data_fn 类型
CronDataFunc = Callable[[], dict[str, Any]]


# ============================================================================
# URL 类型 (URL Types)
# ============================================================================

# 支持的 URL Scheme
URLScheme = Literal["redis", "rediss", "memory"]

# Redis URL 示例: "redis://localhost:6379/0"
# Redis SSL URL 示例: "rediss://localhost:6379/0"
# Memory URL 示例: "memory://"
RedisURL = str
MemoryURL = Literal["memory://"]
QueueURL = str  # RedisURL | MemoryURL


# ============================================================================
# 类型别名 (Type Aliases)
# ============================================================================

# 任务 ID
TaskID = int

# 消息 ID
MessageID = int

# 任务名称
TaskName = str

# 优先级（0-100）
Priority = int

# 指纹（SHA256 hash）
Fingerprint = str

# Worker 名称
WorkerName = str


# ============================================================================
# 类型守卫 (Type Guards)
# ============================================================================

def is_valid_task_message(data: Any) -> bool:
    """
    检查数据是否是有效的 TaskMessage

    示例:
    ```python
    if is_valid_task_message(msg):
        await queue.push(**msg)
    ```
    """
    if not isinstance(data, dict):
        return False

    required_fields = {"task_name", "data"}
    if not required_fields.issubset(data.keys()):
        return False

    if not isinstance(data["task_name"], str):
        return False

    if not isinstance(data["data"], dict):
        return False

    return True


def is_json_serializable(data: Any) -> bool:
    """
    检查数据是否可 JSON 序列化

    示例:
    ```python
    if not is_json_serializable(data):
        raise ValueError("Data must be JSON serializable")
    ```
    """
    import json
    try:
        json.dumps(data)
        return True
    except (TypeError, ValueError):
        return False


# ============================================================================
# 常量 (Constants)
# ============================================================================

# 默认优先级
DEFAULT_PRIORITY = 5

# 深度优先阈值（priority >= 10 表示 DFS）
DFS_PRIORITY_THRESHOLD = 10

# 广度优先优先级（priority < 10 表示 BFS）
BFS_PRIORITY = 0

# 默认 Worker 数量
DEFAULT_NUM_WORKERS = 5

# 默认最大重试次数
DEFAULT_MAX_RETRIES = 3

# 默认重试延迟（秒）
DEFAULT_RETRY_DELAY = 1.0

# 默认批次大小
DEFAULT_BATCH_SIZE = 1

# Redis 默认 DB
DEFAULT_REDIS_DB = 0

# 默认队列名称
DEFAULT_QUEUE_NAME = "tasks"


# ============================================================================
# 工厂函数 (Factory Functions)
# ============================================================================

def create_task_message(
    task_name: str,
    data: dict[str, Any],
    priority: int = DEFAULT_PRIORITY,
    parent_id: Optional[int] = None
) -> TaskMessage:
    """
    创建标准任务消息

    示例:
    ```python
    msg = create_task_message(
        task_name="crawl",
        data={"url": "http://example.com"},
        priority=10
    )
    await pub.push(**msg)
    ```
    """
    if not is_json_serializable(data):
        raise ValueError(f"Data must be JSON serializable, got: {type(data)}")

    return TaskMessage(
        task_name=task_name,
        data=data,
        priority=priority,
        parent_id=parent_id
    )


def create_dfs_message(task_name: str, data: dict[str, Any]) -> TaskMessage:
    """
    创建深度优先任务消息（priority=10）

    示例:
    ```python
    msg = create_dfs_message("crawl", {"url": "..."})
    ```
    """
    return create_task_message(task_name, data, priority=DFS_PRIORITY_THRESHOLD)


def create_bfs_message(task_name: str, data: dict[str, Any]) -> TaskMessage:
    """
    创建广度优先任务消息（priority=0）

    示例:
    ```python
    msg = create_bfs_message("crawl", {"url": "..."})
    ```
    """
    return create_task_message(task_name, data, priority=BFS_PRIORITY)


# ============================================================================
# 导出 (Exports)
# ============================================================================

__all__ = [
    # 消息类型
    "TaskMessage",
    "TaskResult",
    "TaskError",

    # 配置类型
    "RunnerConfigDict",
    "TaskConfigDict",

    # 统计类型
    "TaskInfo",
    "ChainSummary",
    "RunnerPerformance",
    "WorkerInfo",

    # 回调函数类型
    "TaskExecuteFunc",
    "TaskSuccessFunc",
    "TaskFailureFunc",
    "CronDataFunc",

    # URL 类型
    "URLScheme",
    "RedisURL",
    "MemoryURL",
    "QueueURL",

    # 类型别名
    "TaskID",
    "MessageID",
    "TaskName",
    "Priority",
    "Fingerprint",
    "WorkerName",

    # 类型守卫
    "is_valid_task_message",
    "is_json_serializable",

    # 常量
    "DEFAULT_PRIORITY",
    "DFS_PRIORITY_THRESHOLD",
    "BFS_PRIORITY",
    "DEFAULT_NUM_WORKERS",
    "DEFAULT_MAX_RETRIES",
    "DEFAULT_RETRY_DELAY",
    "DEFAULT_BATCH_SIZE",
    "DEFAULT_REDIS_DB",
    "DEFAULT_QUEUE_NAME",

    # 工厂函数
    "create_task_message",
    "create_dfs_message",
    "create_bfs_message",
]
