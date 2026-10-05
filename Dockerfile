FROM python:3.12-slim

# The virtualenv lives outside /app so the bind-mounted source tree (docker-compose.yml)
# never shadows it with a host-built .venv.
ENV UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PYTHONUNBUFFERED=1 \
    PATH="/opt/venv/bin:$PATH"

RUN pip install --no-cache-dir uv

WORKDIR /app

# Install dependencies from the committed lockfile first so this layer caches across code changes.
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

COPY . .

# Railway (and anything else that runs this image) injects the port to listen on, so bind what
# the platform asks for and fall back to Compose's 8000. 0.0.0.0, not loopback: inside a
# container that is the only interface the platform's router can reach — the loopback binding
# SPEC §1 wants is enforced by the local launchd agent instead, where it is the one that counts.
#
# No migrations here. Running `alembic upgrade head` from the start command would race between
# the web and scheduler replicas starting together; it belongs in Railway's own pre-deploy step
# (see railway.toml and the README), which runs once per deployment.
ENV PORT=8000
CMD ["sh", "-c", "exec uvicorn web.app:app --host 0.0.0.0 --port ${PORT}"]
