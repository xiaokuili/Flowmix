# Flowmix 示例

本目录包含了 Flowmix 的所有核心功能示例，演示了 6 大核心功能。

## 📚 示例列表

### 1. 快速定义任务
见主 [README 快速开始部分](../README.md#-快速开始)

展示如何使用装饰器语法快速定义任务和回调函数。

---

### 2. 避免重复处理
**文件**: [dedup.py](dedup.py)

展示任务去重功能，自动识别重复任务并复用执行结果。

**后端**: Redis（高并发支持，适合分布式部署）

**运行前准备**:
```bash
# 1. 启动 Redis 服务
redis-server

# 2. 安装 Redis 依赖
pip install redis

# 3. 运行示例
python examples/dedup.py
```

**效果**: 10 个任务只执行 5 次，节省 50% 计算资源。

---

### 3. 控制并发速率
**文件**:
- [concurrency.py](concurrency.py) - 全局并发数控制
- [rate_limit.py](rate_limit.py) - 任务级限流

展示两种并发控制方式：
- 全局并发数控制：通过 `RunnerConfig(num_workers=N)` 控制
- 任务级限流：通过 `Task(concurrency_limit=N)` 精确控制每秒并发数

```bash
python examples/concurrency.py
python examples/rate_limit.py
```

**效果**:
- 并发效果：10 workers 比 1 worker 快 10 倍
- 限流效果：精确控制每秒执行数，符合限流设置

---

### 4. 任务依赖
**文件**: [task_dependency.py](task_dependency.py)

展示如何构建任务树，通过 `task.callback()` 动态提交子任务，自动关联父子关系。

```bash
python examples/task_dependency.py
```

**效果**: 从 1 个根任务扩展到 13 个任务树，展示深度优先爬取。

---

### 5. 灵活部署环境
**文件**: [deployment.py](deployment.py)

展示多种存储后端支持：
- SQLite - 单机开发
- Redis - 分布式部署
- PostgreSQL - 生产环境

```bash
python examples/deployment.py
```

**效果**: 演示不同存储后端的使用方式和部署架构。

---

### 6. 状态查询和监控告警
**文件**:
- [stats.py](stats.py) - 状态查询
- [monitoring.py](monitoring.py) - 监控告警

展示如何查询任务执行状态，设置告警规则，集成到监控系统。

```bash
python examples/stats.py
python examples/monitoring.py
```

**效果**:
- 多维度统计：整体统计、按任务类型、错误汇总
- 告警规则：成功率、失败数、错误频率、执行耗时

---

## 🎯 运行所有示例

```bash
# 运行单个示例
python examples/dedup.py

# 运行所有示例
python examples/run_all.py
```

## 💡 使用提示

1. **开发环境**：大部分示例使用 SQLite，无需额外配置
2. **Redis 示例**：`dedup.py` 需要先启动 Redis 服务（`redis-server`）和安装依赖（`pip install redis`）
3. **分布式部署**：`deployment.py` 演示 Redis 和 PostgreSQL 后端的使用
4. **清理数据**：每次运行会自动清理 `.flowmix` 目录
5. **自定义修改**：可以修改示例参数查看不同效果

## 📖 更多信息

查看主 [README](../README.md) 了解完整文档和 API 说明。
