FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONPATH=/app \
    WORKFLOW_DB_PATH=/data/workflows.db

WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt \
    && useradd --create-home --uid 10001 --shell /usr/sbin/nologin sky \
    && mkdir -p /data \
    && chown -R sky:sky /app /data
COPY --chown=sky:sky src ./src

USER 10001:10001
VOLUME ["/data"]
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8080/healthz', timeout=2).read()" || exit 1
CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8080", "--no-server-header"]
