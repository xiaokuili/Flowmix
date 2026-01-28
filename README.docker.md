# Flowmix Docker 部署指南

本文档介绍如何使用 Docker 和 Docker Compose 部署 Flowmix 项目。

## 目录结构
flowmix/
├── __init__.py
├── task.py                # 任务模型：Task 装饰器、Callback 定义
├── sender /              # 提交任务
│   ├── __init__.py
│   ├── pub.py            # 直接推送任务
│   └── cron.py           # 定时任务调度
├── runner/                # 执行器：运行任务
│   ├── __init__.py
│   ├── task.py           # 定义任务通用接口
│   ├── runner.py         # 任务运行器
│   ├── engine.py         # 实现feature， retry ，limit ，cache
│   ├── limit/            # 限流
│   │   ├── __init__.py
│   │   └── base.py
│   └── cache/            # 缓存
│       ├── __init__.py
│       ├── base.py
│       ├── redis.py
│       └── sqlite.py
├── stats.py                # 统计：查询状态
└── common/                # 基础设施层
    ├── __init__.py
    ├── pool.py
    └── queue/
        ├── __init__.py
        ├── base.py
        ├── task_queue.py
        ├── redis.py
        ├── postgresql.py
        └── sqlite.py

# 简单场景：cache 自动跟随 queue
runner = TaskRunner(
    tasks={...},
    url="redis://localhost:6379/0",
    queue_name="tasks"
)
# 内部：queue 和 cache 都用 redis://localhost:6379/0，但 key 前缀不同

# 高级场景：单独指定 cache
runner = TaskRunner(
    tasks={...},
    url="redis://localhost:6379/0",
    cache_url="redis://localhost:6379/1",  # 可选
    queue_name="tasks"
)


## 快速开始

### 1. 开发环境（仅 Redis）

最简单的方式，只启动 Redis 服务：

```bash
# 启动 Redis
docker-compose up -d redis

# 查看日志
docker-compose logs -f redis

# 本地运行示例
python examples/deployment.py
```

访问 Redis Commander (Web UI): http://localhost:8081

### 2. 完整开发环境

启动 Redis + Worker:

```bash
# 取消 docker-compose.yml 中 flowmix-worker 的注释
# 启动所有服务
docker-compose up -d

# 查看所有服务状态
docker-compose ps

# 查看 worker 日志
docker-compose logs -f flowmix-worker
```

### 3. 生产环境部署

```bash
# 1. 复制环境变量配置
cp .env.example .env

# 2. 修改 .env 中的密码和配置
vim .env

# 3. 启动生产环境
docker-compose -f docker-compose.prod.yml up -d

# 4. 查看服务状态
docker-compose -f docker-compose.prod.yml ps

# 5. 查看日志
docker-compose -f docker-compose.prod.yml logs -f
```

## 配置说明

### Redis 持久化

`redis.conf` 配置了混合持久化策略：

- **RDB 快照**: 每 15 分钟、5 分钟、1 分钟自动保存
- **AOF 日志**: 每秒同步一次，最多丢失 1 秒数据
- **混合模式**: 结合 RDB 和 AOF 优点

数据保存在 Docker Volume `redis_data` 中，容器重启不会丢失。

### 内存管理

默认配置：
- 最大内存: 512MB
- 淘汰策略: allkeys-lru (适合缓存场景)

如果主要用作任务队列，建议修改为 `noeviction`：

```redis
# redis.conf
maxmemory-policy noeviction
```

### Worker 扩展

在 `docker-compose.prod.yml` 中已配置 2 个 worker，可以添加更多：

```yaml
flowmix-worker-3:
  build:
    context: .
    dockerfile: Dockerfile
  environment:
    - WORKER_ID=worker-3
  # ... 其他配置与 worker-1 相同
```

## 常用命令

### 服务管理

```bash
# 启动服务
docker-compose up -d

# 停止服务
docker-compose down

# 重启服务
docker-compose restart

# 停止并删除数据卷（警告：会删除所有数据）
docker-compose down -v

# 查看服务状态
docker-compose ps

# 查看日志
docker-compose logs -f [service_name]
```

### Redis 操作

```bash
# 连接到 Redis
docker exec -it flowmix-redis redis-cli

# 查看持久化信息
docker exec -it flowmix-redis redis-cli INFO persistence

# 查看内存使用
docker exec -it flowmix-redis redis-cli INFO memory

# 查看数据文件
docker exec -it flowmix-redis ls -lh /data

# 手动保存快照
docker exec -it flowmix-redis redis-cli BGSAVE

# 查看队列任务数
docker exec -it flowmix-redis redis-cli LLEN flowmix:tasks:pending
```

### 数据备份

```bash
# 备份 Redis 数据
docker cp flowmix-redis:/data/dump.rdb ./backup/
docker cp flowmix-redis:/data/appendonly.aof ./backup/

# 恢复 Redis 数据
docker cp ./backup/dump.rdb flowmix-redis:/data/
docker cp ./backup/appendonly.aof flowmix-redis:/data/
docker-compose restart redis
```

### PostgreSQL 操作

```bash
# 连接到 PostgreSQL
docker exec -it flowmix-postgres psql -U flowmix -d flowmix

# 备份数据库
docker exec -it flowmix-postgres pg_dump -U flowmix flowmix > backup.sql

# 恢复数据库
docker exec -i flowmix-postgres psql -U flowmix flowmix < backup.sql
```

## 监控和调试

### 查看 Redis 性能

```bash
# 实时监控命令
docker exec -it flowmix-redis redis-cli MONITOR

# 慢查询日志
docker exec -it flowmix-redis redis-cli SLOWLOG GET 10

# 查看客户端连接
docker exec -it flowmix-redis redis-cli CLIENT LIST
```

### 查看 Worker 日志

```bash
# 实时日志
docker-compose logs -f flowmix-worker-1

# 查看最近 100 行
docker-compose logs --tail=100 flowmix-worker-1

# 查看所有 worker 日志
docker-compose logs -f flowmix-worker-1 flowmix-worker-2
```

### Redis Exporter (生产环境)

生产环境配置了 Redis Exporter，可以集成到 Prometheus + Grafana 监控系统。

Metrics 端点: http://localhost:9121/metrics

## 故障排查

### Redis 无法启动

```bash
# 查看错误日志
docker-compose logs redis

# 检查配置文件
docker exec -it flowmix-redis cat /usr/local/etc/redis/redis.conf

# 检查数据目录权限
docker exec -it flowmix-redis ls -la /data
```

### Worker 无法连接 Redis

```bash
# 检查 Redis 是否健康
docker-compose ps redis

# 测试网络连接
docker-compose exec flowmix-worker ping redis

# 检查环境变量
docker-compose exec flowmix-worker env | grep REDIS
```

### 数据丢失问题

1. 检查持久化配置是否生效：
```bash
docker exec -it flowmix-redis redis-cli CONFIG GET save
docker exec -it flowmix-redis redis-cli CONFIG GET appendonly
```

2. 检查数据文件是否存在：
```bash
docker exec -it flowmix-redis ls -lh /data
```

3. 检查 Docker Volume：
```bash
docker volume inspect flowmix_redis_data
```

## 安全建议

### 生产环境必做

1. **设置 Redis 密码**
   ```bash
   # .env 文件
   REDIS_PASSWORD=<strong_random_password>
   ```

2. **禁用危险命令**
   ```redis
   # redis.conf
   rename-command FLUSHDB ""
   rename-command FLUSHALL ""
   rename-command CONFIG ""
   ```

3. **限制网络访问**
   - 不要暴露 Redis 端口到公网
   - 使用防火墙规则限制访问

4. **定期备份**
   - 设置自动备份脚本
   - 异地存储备份文件

5. **监控告警**
   - 集成 Prometheus + Grafana
   - 设置内存、磁盘、连接数告警

## 性能优化

### Redis 优化

1. **调整持久化频率**（根据业务需求）
```redis
# 降低 RDB 频率
save 3600 1
save 1800 10

# AOF 改为每秒同步
appendfsync everysec
```

2. **增加最大内存**
```redis
maxmemory 2gb
```

3. **优化网络**
```redis
tcp-backlog 511
tcp-keepalive 300
```

### Worker 优化

1. **增加 Worker 数量**
```yaml
environment:
  - NUM_WORKERS=8
```

2. **水平扩展**
```bash
docker-compose -f docker-compose.prod.yml up -d --scale flowmix-worker=4
```

## 参考资源

- [Redis 官方文档](https://redis.io/documentation)
- [Docker Compose 文档](https://docs.docker.com/compose/)
- [Flowmix 项目主页](https://github.com/xiaokuili/Flowmix)

## 常见问题 (FAQ)

**Q: 如何验证持久化是否生效？**

A:
```bash
# 1. 写入测试数据
docker exec -it flowmix-redis redis-cli SET test_key "test_value"

# 2. 重启容器
docker-compose restart redis

# 3. 验证数据还在
docker exec -it flowmix-redis redis-cli GET test_key
```

**Q: 如何查看任务队列长度？**

A:
```bash
docker exec -it flowmix-redis redis-cli LLEN flowmix:tasks:pending
```

**Q: 如何清空所有任务？**

A:
```bash
docker exec -it flowmix-redis redis-cli FLUSHDB
```

**Q: 如何扩展到多台服务器？**

A: 使用 Docker Swarm 或 Kubernetes 进行多节点部署。参考 `docker-compose.prod.yml` 中的配置。
