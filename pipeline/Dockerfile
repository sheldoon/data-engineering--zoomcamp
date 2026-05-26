FROM python:3.13.11-slim

WORKDIR  /app

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin 

ENV PATH="app/.venv/bin:$PATH"

COPY pyproject.toml .python-version uv.lock ./
RUN uv sync --locked  



COPY ingest_zone.py .

ENTRYPOINT [ "uv", "run", "python", "ingest_zone.py" ]