# ==============================================================================
# Multi-stage Dockerfile para FlowEngine usando Astral uv
# ==============================================================================

# --- Stage 1: Builder ---
FROM python:3.12-slim-bookworm AS builder

# Instalar uv binario desde la imagen oficial
COPY --from=ghcr.io/astral-sh/uv:0.6.5 /uv /uvx /bin/

WORKDIR /app

# Optimizar caché de capas: copiar manifiestos primero
COPY pyproject.toml uv.lock ./

# Instalar dependencias en entorno virtual aislado sin el proyecto raíz
ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy
RUN uv sync --frozen --no-install-project --no-dev

# Copiar el código fuente e instalar el proyecto
COPY README.md ./
COPY src/ ./src/
RUN uv sync --frozen --no-dev


# --- Stage 2: Runtime Final ---
FROM python:3.12-slim-bookworm AS runtime

WORKDIR /app

# Instalar utilidades de sistema mínimas para comprobaciones de estado
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl && \
    rm -rf /var/lib/apt/lists/*

# Crear usuario sin privilegios para ejecución segura
RUN groupadd -g 1000 appuser && \
    useradd -u 1000 -g appuser -s /bin/bash -m appuser

# Copiar el entorno virtual y el código fuente desde el builder
COPY --from=builder --chown=appuser:appuser /app /app

# Directorio de datos persistente para SQLite en modo WAL
RUN mkdir -p /data && chown -R appuser:appuser /data

ENV PATH="/app/.venv/bin:$PATH"
ENV FLOWENGINE_DB_PATH="/data/flowengine.db"
ENV PYTHONUNBUFFERED=1

USER appuser

EXPOSE 8000

# Healthcheck nativo
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
  CMD curl -f http://localhost:8000/health || exit 1

# Comando por defecto: Servidor de Webhooks y API con Scheduler integrado
CMD ["flow", "serve", "--host", "0.0.0.0", "--port", "8000", "--with-scheduler"]
