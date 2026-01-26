# Flowmix Dockerfile
# 用于构建 Flowmix 应用容器

FROM python:3.11-slim

# 设置工作目录
WORKDIR /app

# 设置环境变量
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# 安装系统依赖
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# 复制项目文件
COPY pyproject.toml ./
COPY flowmix ./flowmix
COPY examples ./examples

# 安装 Python 依赖
# 安装所有可选依赖以支持所有后端
RUN pip install --no-cache-dir -e ".[all]"

# 创建数据目录
RUN mkdir -p .flowmix

# 默认命令（可在 docker-compose 中覆盖）
CMD ["python", "-m", "flowmix.runner"]
