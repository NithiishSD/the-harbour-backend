# ── Backend image (Python + Java for Spark) ───────────────────────────────────
# One image for every Python service: init, streaming, dashboard, weather, replay, live, nightly.
# Java for Spark is copied from the Temurin image (same JRE as HBase), so no apt step.
FROM eclipse-temurin:21-jre-noble AS java

FROM python:3.14-slim-trixie AS backend

COPY --from=java /opt/java/openjdk /opt/java/openjdk
ENV JAVA_HOME=/opt/java/openjdk \
    PATH=/opt/java/openjdk/bin:$PATH \
    PYTHONUNBUFFERED=1 \
    DATA_DIR=/app/data \
    STATIC_DIR=/app/frontend/src

RUN useradd --create-home --uid 1000 app && mkdir -p /app/data && chown app:app /app/data
WORKDIR /app/backend

COPY backend/pyproject.toml ./
RUN pip install --no-cache-dir . && rm -rf build

COPY --chown=app:app backend/common    common
COPY --chown=app:app backend/ingest    ingest
COPY --chown=app:app backend/streaming streaming
COPY --chown=app:app backend/batch     batch
COPY --chown=app:app backend/storage   storage
COPY --chown=app:app backend/dashboard dashboard
COPY --chown=app:app backend/measure   measure
COPY --chown=app:app backend/geo       geo

# Copy frontend static files so FastAPI can serve them in local/dev mode.
# In production Nginx serves frontend/src/ directly.
COPY --chown=app:app frontend/src /app/frontend/src

USER app
# Pre-fetch the Spark Kafka connector so the streaming job starts without internet access.
RUN python -c "from common.spark import session; session('warm', kafka=True).stop()"

EXPOSE 8000 4040
CMD ["uvicorn", "dashboard.app:app", "--host", "0.0.0.0", "--port", "8000"]
