FROM ghcr.io/astral-sh/uv:0.12.1 AS uv

FROM python:3.12-slim
# 基础镜像会滞后于 Debian 安全更新：构建时先装上已发布的安全修复，
# 否则镜像漏洞门禁会被上游镜像的滞后驱动，而不是本仓库的决策。
RUN apt-get update \
    && apt-get upgrade -y \
    && rm -rf /var/lib/apt/lists/*


COPY --from=uv /uv /uvx /bin/
WORKDIR /app

COPY pyproject.toml uv.lock README.md ./
COPY src ./src
RUN uv sync --frozen --no-dev --no-editable

RUN useradd --uid 10001 --no-create-home --home /nonexistent --shell /usr/sbin/nologin gateway
USER 10001:10001

EXPOSE 8082 8083
CMD ["/app/.venv/bin/python", "-m", "feishu_dify_gateway"]
