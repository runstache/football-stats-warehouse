FROM ghcr.io/astral-sh/uv:python3.12-bookworm AS builder

RUN apt update -qy && apt install -qyy \
    -o APT::Install-Recommends=false \
    -o APT::Install-Suggests=false \
    build-essential \
    ca-certificates \
    python3-setuptools \
    && apt clean

ENV UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    UV_COMPILE_BYTECODE=1 \
    UV_PYTHON=python3.12 \
    UV_NO_CACHE=1 \
    UV_SYSTEM_PYTHON=1

WORKDIR /app

RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --locked --no-dev

FROM python:3.12-bookworm

RUN apt update -qy && apt upgrade -qyy && apt install chromium-driver -qyy && apt clean
COPY --from=builder /app /app
COPY ./src /app

ENV VIRTUAL_ENV=/app/.venv
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

RUN groupadd -r app && useradd -r -d /app -g app -N app

USER app
WORKDIR /app

CMD ["python", "download_stats.py", "-h"]
