"""
Task - 任务定义

通过装饰器注册执行函数和钩子函数
"""

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
    """

    def __init__(self):
        """初始化 Task"""
        self._execute_func: Optional[Callable[[dict], Any]] = None
        self._on_success_func: Optional[Callable[[dict, Any], None]] = None
        self._on_failure_func: Optional[Callable[[dict, Exception], None]] = None

    def execute(self, func: Callable[[dict], Any]) -> Callable:
        """
        注册执行函数（必须）

        Args:
            func: 执行函数 (data: dict) -> Any
                 - 接收输入数据 data
                 - 返回执行结果（传递给 on_success）
                 - 抛出异常会触发 on_failure

        Returns:
            原函数（支持装饰器语法）

        Example:
            @task.execute
            def my_execute(data):
                url = data['url']
                return fetch(url)
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

    def run(self, data: dict) -> Any:
        """
        执行任务（内部使用，由 Worker 调用）

        执行流程：
        1. 调用 execute(data)
        2. 成功 -> 调用 on_success(data, result)
        3. 失败 -> 调用 on_failure(data, error)，然后重新抛出异常

        Args:
            data: 输入数据

        Returns:
            执行结果

        Raises:
            RuntimeError: 如果未定义 execute()
            Exception: execute() 抛出的任何异常
        """
        if self._execute_func is None:
            raise RuntimeError("Task.execute() is not defined. Use @task.execute to register execute function.")

        try:
            # 执行核心逻辑
            result = self._execute_func(data)

            # 调用成功回调
            if self._on_success_func:
                self._on_success_func(data, result)

            return result

        except Exception as error:
            # 调用失败回调
            if self._on_failure_func:
                try:
                    self._on_failure_func(data, error)
                except Exception as callback_error:
                    # 如果回调本身失败，记录但不影响原始异常
                    print(f"Warning: on_failure callback raised exception: {callback_error}")

            # 重新抛出原始异常
            raise 

    def __repr__(self) -> str:
        """字符串表示"""
        execute_name = self._execute_func.__name__ if self._execute_func else "None"
        return f"Task(execute={execute_name})"
