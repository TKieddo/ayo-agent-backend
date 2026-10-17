FROM python:3.12-slim

# System deps (wget/curl kept for Coolify UI healthchecks that shell out to them)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential curl wget ca-certificates git && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy project metadata and install deps
COPY pyproject.toml poetry.lock ./

# Install Poetry lightweightly
ENV POETRY_VIRTUALENVS_CREATE=false \
    POETRY_NO_INTERACTION=1
RUN pip install --no-cache-dir poetry && \
    poetry install --no-interaction --no-ansi --no-root

# Copy source
COPY src ./src

# API defaults (Coolify / App Platform use PORT; default 8080)
ENV APP_PORT=8080
ENV API_PORT=8080
ENV API_HOST=0.0.0.0
ENV PORT=8080
EXPOSE 8080

# Prefer a pure-Python probe so health still works if curl/wget are missing.
# Long start-period covers exchange bootstrap; early health answers sooner.
HEALTHCHECK --interval=15s --timeout=5s --start-period=120s --retries=8 \
  CMD python -c "import os,urllib.request; p=os.getenv('PORT') or os.getenv('APP_PORT') or '8080'; urllib.request.urlopen(f'http://127.0.0.1:{p}/', timeout=4)"

# Default command: run as module to keep absolute imports working
ENTRYPOINT ["poetry", "run", "python", "-m", "src.main"]
