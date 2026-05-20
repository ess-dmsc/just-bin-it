FROM python:3.14-slim-trixie

COPY --from=ghcr.io/astral-sh/uv:0.10 /uv /uvx /bin/

RUN groupadd --system --gid 999 nonroot \
 && useradd --system --gid 999 --uid 999 --create-home nonroot

WORKDIR /app
ARG JBI_VERSION=""

ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy
ENV UV_NO_DEV=1
ENV UV_TOOL_BIN_DIR=/usr/local/bin

RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --locked --no-install-project

COPY . /app

RUN --mount=type=cache,target=/root/.cache/uv \
    if [ -n "$JBI_VERSION" ]; then \
        uv version --no-sync "$JBI_VERSION"; \
    fi && \
    uv sync --locked

ENV PATH="/app/.venv/bin:$PATH"

ENTRYPOINT []

USER nonroot

CMD ["just-bin-it", "-h"]
