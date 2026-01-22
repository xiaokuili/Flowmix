"""
Task - 任务定义

通过装饰器注册执行函数和钩子函数
"""

import hashlib
import inspect
import json
import sqlite3
import threading
from typing import Callable, Optional, Any


class Task:
    """
    Task 定义

    通过装饰器注册执行函数和钩子函数

    职责：
    - 定义任务的执行逻辑
    - 定义成功/失败后的处理逻辑
    - 不负责队列、调度、重试等（由 Worker 处理）

    Example:
        # 创建 Task
        task = Task()

        # 注册执行函数（必须）
        @task.execute
        def run(data):
            return fetch(data['url'])

        # 注册成功回调（可选）
        @task.on_success
        def success(data, result):
            save_to_db(result)

        # 注册失败回调（可选）
        @task.on_failure
        def failure(data, error):
            alert_admin(error)

        # 使用
        worker = Worker(task=task, manager=manager)
        worker.run()

    动态回调任务（callback）:
        # callback() 可在任何地方使用，立即提交任务到队列
        crawl_task = Task(name='crawl')

        @crawl_task.execute
        def crawl(data):
            html = fetch(data['url'])
            links = parse_links(html)

            # 立即提交子任务（自动关联父子关系）
            for link in links:
                crawl_task.callback('crawl', {'url': link}, priority=10)  # DFS

            return html

        @crawl_task.on_success
        def on_success(data, result):
            # 也可以在 on_success 中使用
            crawl_task.callback('analyze', {'html': result})

        # 优先级说明：
        # - priority 越大越优先执行
        # - 高优先级 -> 深度优先（DFS）：先处理新发现的任务
        # - 低优先级 -> 广度优先（BFS）：先处理旧任务
    """

    def __init__(
        self,
        name: Optional[str] = None,
        concurrency_limit: Optional[int] = None,
        dedup: bool = False,
        dedup_ttl: Optional[int] = None
    ):
        """
        初始化 Task

        Args:
            name: Task 名称（用于 Worker 路由和 callback）
            concurrency_limit: 每秒最大并发数（默认 None，表示无限制）
                              - 基于滑动窗口算法控制并发
                              - 例如：concurrency_limit=10 表示每秒最多执行 10 个任务
            dedup: 是否启用任务去重/缓存（默认 False）
                  - True: 相同任务名和参数的任务会复用之前的执行结果
                  - False: 每次都执行
            dedup_ttl: 去重时间窗口（秒，默认 None）
                      - None: 永久去重（适合爬虫 URL 去重）
                      - 3600: 1小时内去重（适合 API 调用缓存）
                      - 只对成功的任务生效，失败的任务不缓存

        Example:
            # 爬虫场景：永久去重
            crawl_task = Task(name='crawl', dedup=True)

            # API 调用：1小时内复用结果
            api_task = Task(name='fetch_user', dedup=True, dedup_ttl=3600)
        """
        self.name = name
        self.concurrency_limit = concurrency_limit
        self.dedup = dedup
        self.dedup_ttl = dedup_ttl
        self._execute_func: Optional[Callable[[dict], Any]] = None
        self._on_success_func: Optional[Callable[[dict, Any], None]] = None
        self._on_failure_func: Optional[Callable[[dict, Exception], None]] = None
        # Worker 引用（由 Worker 设置，用于 callback 立即提交任务）
        self._worker: Optional[Any] = None
        # 当前执行的消息 ID（用于自动关联 parent_id）
        self._current_msg_id: Optional[int] = None
        # 数据库配置（由 Worker 设置）
        self._db_path: Optional[str] = None
        self._queue_name: Optional[str] = None
        # 线程本地存储（用于数据库连接）
        self._local = threading.local()

    def execute(self, func: Callable[[dict], Any]) -> Callable:
        """
        注册执行函数（必须）

        Args:
            func: 执行函数 (data: dict) -> Any
                 - 接收输入数据 data
                 - 返回执行结果（传递给 on_success）
                 - 可选：返回 {'submit': [...]} 动态提交新任务
                 - 抛出异常会触发 on_failure

        Returns:
            原函数（支持装饰器语法）

        Example:
            # 简单返回结果
            @task.execute
            def my_execute(data):
                url = data['url']
                return fetch(url)

            # 动态回调任务（爬虫示例）
            @task.execute
            def crawl(data):
                html = fetch(data['url'])
                links = parse_links(html)

                # 调用 task.callback() 回调任务
                for link in links:
                    task.callback('crawl', {'url': link}, priority=10)

                return html
        """
        self._execute_func = func
        return func

    def on_success(self, func: Callable[[dict, Any], None]) -> Callable:
        """
        注册成功回调（可选）

        在 execute() 成功执行后调用

        Args:
            func: 成功回调 (data: dict, result: Any) -> None
                 - data: 输入数据
                 - result: execute() 的返回值

        Returns:
            原函数（支持装饰器语法）

        Example:
            @task.on_success
            def my_success(data, result):
                save_to_db(result)
                print(f"Processed {data['url']}")
        """
        self._on_success_func = func
        return func

    def on_failure(self, func: Callable[[dict, Exception], None]) -> Callable:
        """
        注册失败回调（可选）

        在 execute() 抛出异常时调用

        Args:
            func: 失败回调 (data: dict, error: Exception) -> None
                 - data: 输入数据
                 - error: 异常对象

        Returns:
            原函数（支持装饰器语法）

        Example:
            @task.on_failure
            def my_failure(data, error):
                print(f"Failed: {error}")
                alert_admin(data['url'], error)
        """
        self._on_failure_func = func
        return func

    def callback(self, task_name: str, data: dict, priority: int = 0):
        """
        立即提交任务到队列（可在任何地方使用）

        在 execute()、on_success()、on_failure() 或外部任意位置调用此方法，
        任务会立即提交到队列。如果在 execute() 执行期间调用，会自动关联
        parent_id 为当前任务的 msg_id。

        Args:
            task_name: 要回调的任务名称（Worker 会根据此名称路由到对应的 Task）
            data: 任务数据（字典）
            priority: 优先级（默认 0，数字越大越优先）
                     - 高优先级 -> 深度优先（DFS）
                     - 低优先级 -> 广度优先（BFS）

        Raises:
            RuntimeError: 如果 Task 未关联到 Worker

        Example:
            # 在 execute 中使用（自动关联父任务）
            @task.execute
            def crawl(data):
                html = fetch(data['url'])
                links = parse_links(html)

                # 立即提交子任务
                for link in links:
                    task.callback('crawl', {'url': link}, priority=10)

                return html

            # 在 on_success 中使用
            @task.on_success
            def on_success(data, result):
                task.callback('analyze', {'html': result})

            # 在外部使用
            task.callback('crawl', {'url': 'http://example.com'})
        """
        if not self._worker:
            raise RuntimeError(
                f"Task '{self.name}' is not attached to a Worker. "
                "Please create a Worker instance with this task first."
            )

        # 立即提交到队列（如果在 execute 中，自动关联父任务）
        parent_id = self._current_msg_id
        self._worker.push(data, priority, parent_id, task_name)

    async def run(self, data: dict, msg_id: Optional[int] = None) -> Any:
        """
        执行任务（内部使用，由 Worker 调用）

        执行流程：
        1. 记录当前消息 ID（用于 callback 自动关联 parent_id）
        2. 清空待提交任务列表
        3. 调用 execute(data) - 支持同步和异步
        4. 成功 -> 调用 on_success(data, result) - 支持同步和异步
        5. 失败 -> 调用 on_failure(data, error)，然后重新抛出异常 - 支持同步和异步

        Args:
            data: 输入数据
            msg_id: 当前消息 ID（由 Worker 传入，用于构建任务树）

        Returns:
            执行结果

        Raises:
            RuntimeError: 如果未定义 execute()
            Exception: execute() 抛出的任何异常
        """
        if self._execute_func is None:
            raise RuntimeError("Task.execute() is not defined. Use @task.execute to register execute function.")

        # 记录当前消息 ID（用于 callback 自动关联父任务）
        self._current_msg_id = msg_id

        try:
            # 执行核心逻辑（支持同步和异步）
            if inspect.iscoroutinefunction(self._execute_func):
                result = await self._execute_func(data)
            else:
                result = self._execute_func(data)

            # 调用成功回调（支持同步和异步）
            if self._on_success_func:
                if inspect.iscoroutinefunction(self._on_success_func):
                    await self._on_success_func(data, result)
                else:
                    self._on_success_func(data, result)

            return result

        except Exception as error:
            # 调用失败回调（支持同步和异步）
            if self._on_failure_func:
                try:
                    if inspect.iscoroutinefunction(self._on_failure_func):
                        await self._on_failure_func(data, error)
                    else:
                        self._on_failure_func(data, error)
                except Exception as callback_error:
                    # 如果回调本身失败，记录但不影响原始异常
                    print(f"Warning: on_failure callback raised exception: {callback_error}")

            # 重新抛出原始异常
            raise 

    def set_db_config(self, db_path: str, queue_name: str):
        """
        设置数据库配置（由 Worker 调用）

        Args:
            db_path: 数据库文件路径
            queue_name: 队列表名
        """
        self._db_path = db_path
        self._queue_name = queue_name

    def _get_db_connection(self) -> sqlite3.Connection:
        """获取当前线程的数据库连接（用于缓存查询）"""
        if not hasattr(self._local, 'conn'):
            if not self._db_path:
                raise RuntimeError("Database not configured. Worker should call set_db_config().")

            self._local.conn = sqlite3.connect(
                self._db_path,
                check_same_thread=False,
                timeout=30.0
            )
            self._local.conn.row_factory = sqlite3.Row
            # 启用 WAL 模式
            self._local.conn.execute("PRAGMA journal_mode=WAL")
        return self._local.conn

    def _generate_fingerprint(self, data: dict) -> str:
        """
        生成任务指纹（用于去重）

        Args:
            data: 任务数据

        Returns:
            SHA256 哈希字符串
        """
        # 标准化 JSON（排序 key，确保相同数据生成相同哈希）
        normalized = json.dumps(
            {'task': self.name, 'data': data},
            sort_keys=True,
            ensure_ascii=False
        )
        return hashlib.sha256(normalized.encode()).hexdigest()

    def check_cache(self, data: dict) -> Optional[Any]:
        """
        检查缓存是否命中

        Args:
            data: 任务数据

        Returns:
            缓存的结果，如果未命中返回 None
        """
        if not self.dedup:
            return None

        fingerprint = self._generate_fingerprint(data)
        conn = self._get_db_connection()

        if self.dedup_ttl is None:
            # 永久缓存：只查找 completed 的任务
            cursor = conn.execute(f"""
                SELECT result FROM {self._queue_name}
                WHERE fingerprint = ? AND status = 'completed'
                ORDER BY completed_at DESC
                LIMIT 1
            """, (fingerprint,))
        else:
            # 带 TTL：查找最近 ttl 秒内完成的任务
            cursor = conn.execute(f"""
                SELECT result FROM {self._queue_name}
                WHERE fingerprint = ?
                  AND status = 'completed'
                  AND julianday('now') - completed_at < ?
                ORDER BY completed_at DESC
                LIMIT 1
            """, (fingerprint, self.dedup_ttl / 86400.0))  # 转换为天数

        row = cursor.fetchone()
        if row and row['result']:
            return json.loads(row['result'])

        return None

    def get_fingerprint(self, data: dict) -> Optional[str]:
        """
        获取任务指纹（供 Worker 保存到数据库）

        Args:
            data: 任务数据

        Returns:
            指纹字符串，如果未启用去重返回 None
        """
        if not self.dedup:
            return None
        return self._generate_fingerprint(data)

    def __repr__(self) -> str:
        """字符串表示"""
        execute_name = self._execute_func.__name__ if self._execute_func else "None"
        return f"Task(execute={execute_name})"
