# Flowmix 示例

每个示例展示一个核心功能的实际效果。

## 运行示例

```bash
# 1. 去重效果 - 10 个任务只执行 5 次
python examples/dedup.py

# 2. 并发效果 - 10 workers 比 1 worker 快数倍
python examples/concurrency.py

# 3. 限流效果 - 精确控制每秒并发数
python examples/rate_limit.py

# 4. 状态查询 - 实时监控执行情况
python examples/stats.py
```

## 示例说明

| 文件 | 功能 | 期望输出 |
|------|------|----------|
| [dedup.py](dedup.py) | 任务去重/缓存 | 提交 10 个任务（5 个唯一 URL，每个重复 2 次），实际只执行 5 次，节省 50% |
| [concurrency.py](concurrency.py) | 高性能并发 | 对比 1/5/10 个并发 worker 的执行速度，展示加速效果 |
| [rate_limit.py](rate_limit.py) | 并发限流控制 | 20 个 worker 提交 20 个任务，限流 5 tasks/s，精确分布在 4 秒内 |
| [stats.py](stats.py) | Worker 状态查询 | 查询任务统计、成功率、吞吐量、失败任务、错误汇总 |

## 注意事项

- 每次运行前会自动清理旧的数据库文件（`.flowmix/` 目录）
- 示例使用 `auto_stop=True` 参数，执行完自动退出
- 如果看到 SQLite 并发错误，这是框架本身的已知问题，不影响功能演示
