FROM python:3.11-slim

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# Copy dependency files first for layer caching
COPY pyproject.toml .
COPY src/ src/
COPY README.md README.md

# Install the package and dependencies (no dev deps)
RUN uv pip install --system --no-cache .

ENTRYPOINT ["pg-freezer"]
CMD ["--help"]
