# Flowmix

只需关心业务流程，Flowmix 负责高性能执行、状态管理、简洁接口

---

## 📦 安装

### 基础安装

```bash
pip install git+https://github.com/xiaokuili/Flowmix.git
```

### 安装特定版本

```bash
# 安装指定版本标签
pip install git+https://github.com/xiaokuili/Flowmix.git@v0.3.0

# 强制安装
pip install --force-reinstall git+https://github.com/xiaokuili/Flowmix.git@v0.5.0
```

---

## 🚀 快速开始

**5 分钟上手 Flowmix** - 查看 [QUICK_START.md](QUICK_START.md)

### 核心概念

```python
# 1. 定义任务
task = Task(name='greet')

@task.execute
async def greet(data):
    print(f"Hello, {data['name']}!")
    return {"status": "ok"}

# 2. 推送任务
pub = Pub(queue=queue)
await pub.push(data={"name": "Alice"}, task_name="greet")

# 3. 启动执行
runner = TaskRunner(
    tasks={"greet": task},
    url="redis://localhost:6379/0"
)
await runner.run()
```

---

## ✨ 特性 (Features)

1. **友好的 Task 接口** - 减轻开发难度，声明式 API
2. **并发控制** - 支持并发、重试、缓存、限流
3. **状态查询** - 完整的任务链追踪和性能监控
4. **动态任务** - 执行中动态提交子任务，构建任务树
5. **多后端支持** - Redis/Memory 队列，易于扩展

---

## 📚 文档

- [QUICK_START.md](QUICK_START.md) - 5 分钟快速入门
- [ARCHITECTURE.md](ARCHITECTURE.md) - 架构设计详解（为 AI 优化）
- [examples/](examples/) - 完整示例代码
- [flowmix/common/types.py](flowmix/common/types.py) - 类型定义

---

## 💡 使用场景

- **网络爬虫**: 递归爬取，URL 去重
- **数据 ETL**: 任务链，依赖处理
- **API 限流**: 并发控制，结果缓存
- **定时任务**: 周期执行，Cron 调度

---

## 🤝 为 AI 设计

Flowmix 特别优化了文档，帮助 AI 理解框架：

- 明确的类型定义 ([types.py](flowmix/common/types.py))
- 详细的架构文档 ([ARCHITECTURE.md](ARCHITECTURE.md))
- 运行时依赖说明（状态注入机制）
- 常见错误和解决方案

查看 [ARCHITECTURE.md](ARCHITECTURE.md) 了解框架内部机制。

---

## 📄 License

MIT License 
