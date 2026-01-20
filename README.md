# Flowmix

简洁灵活的任务队列框架（基于 SQLite）

## 核心概念

### 职责划分

```
Task (任务)
├─ 定义执行逻辑
├─ 定义成功/失败回调
└─ 由装饰器注册函数

Manager (队列管理器)
├─ 持久化队列（SQLite）
├─ 存取任务（push/pop）
└─ 确认任务（ack）

Worker (执行器)
├─ 从 Manager 取任务
├─ 调用 Task 执行
└─ 支持重试和并发
```

## 特点

- ✅ **零依赖**：基于 SQLite，无需 Redis 或其他外部服务
- ✅ **持久化**：任务数据持久化存储，进程重启不丢失
- ✅ **并发安全**：支持多线程并发处理
- ✅ **简单易用**：装饰器风格 API，上手即用

## 快速开始

### 安装

```bash
# 核心无依赖，直接使用即可
pip install flowmix
```

### 基础用法

```python
from flowmix import Task, Manager, Worker

# 1. 定义 Task
task = Task()

@task.execute
def process_url(data: dict):
    """处理 URL"""
    url = data['url']
    print(f"Processing: {url}")
    return {"url": url, "status": "ok"}

@task.on_success
def save_result(data: dict, result):
    """成功后保存"""
    print(f"✅ Success: {result}")

@task.on_failure
def handle_error(data: dict, error: Exception):
    """失败后处理"""
    print(f"❌ Failed: {error}")

# 2. 创建 Manager（基于 SQLite）
manager = Manager(db_path="flowmix.db")

# 3. 发布任务
manager.push({"url": "http://example.com"})

# 4. 创建 Worker
worker = Worker(
    task=task,
    manager=manager,
    num_workers=5,      # 5 个并发
    max_retries=3,      # 失败后重试 3 次
    retry_delay=5       # 重试间隔 5 秒
)

# 5. 启动
worker.run()
```

## 运行示例

```bash
# 发布任务
python examples/simple_example.py publish

# 启动 Worker
python examples/simple_example.py worker
```
