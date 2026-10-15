# ==========================
# Builder stage: сборка зависимостей
# ==========================
FROM python:3.13-slim AS builder

WORKDIR /app

# gcc + libpq-dev нужны для компиляции C-расширений при uv sync.
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# uv для воспроизводимой установки из uv.lock.
RUN pip install uv

COPY pyproject.toml uv.lock ./

# --no-dev: dev-зависимости (black/pytest/testcontainers) не попадают в образ.
RUN uv sync --no-dev


# ==========================
# Runtime stage: лёгкий образ
# ==========================
FROM python:3.13-slim AS runtime

WORKDIR /app

# libpq5 содержит libpq.so.5, необходимую asyncpg на проде.
RUN apt-get update && apt-get install -y libpq5 && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/.venv /app/.venv

COPY . .

# Чтобы gunicorn/uvicorn находились без указания полного пути.
ENV PATH="/app/.venv/bin:$PATH"

CMD ["gunicorn", "main:app", "-c", "gunicorn.conf.py"]
