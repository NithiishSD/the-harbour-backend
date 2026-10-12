# HarbourWatch Backend

The core real-time processing engine and REST/SSE API for HarbourWatch.

Monitors real-time ship movements, tracks port queues and waiting times, detects AIS transmission anomalies (dark ships), and archives historical vessel journeys.

## Architecture

- **Ingestion**: Live WebSocket ingestion from `aisstream.io` (`ingest/aisstream.py`, `ingest/follow.py`), plus marine weather ingestion (`ingest/weather.py`).
- **Messaging**: Apache Kafka (`common/kafka.py`, `common/topics.py`).
- **Stream Processing**: Apache Spark Structured Streaming (`streaming/job.py`) statefully tracking ship trajectories and queue states.
- **Storage**: Apache HBase (`storage/hbase/`) for real-time state and DuckDB warehouse (`storage/warehouse/`) for analytical reporting.
- **API**: FastAPI (`dashboard/app.py`) providing REST endpoints and Server-Sent Events (SSE).

## Quick Start (Docker Compose)

1. Copy `.env.example` to `.env` and fill in your `AISSTREAM_API_KEY`:
   ```bash
   cp .env.example .env
   ```

2. Start the core backend services (Kafka, HBase, schema init, streaming engine, API, and weather):
   ```bash
   docker compose up -d --build
   ```

3. Start live ingestion (Singapore waters and global vessel tracking):
   ```bash
   docker compose --profile live up -d
   ```

4. Verify backend health:
   ```bash
   curl http://127.0.0.1:8000/health
   ```

## Local Development

Prerequisites: Python 3.14+, Java 21+ (for PySpark local execution).

```bash
# Create and activate virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install editable package
pip install -e .

# Run linters and tests
ruff check .
ruff format --check .
mypy common dashboard ingest storage streaming batch measure
pytest tests/unit

# Run the API server directly
uvicorn dashboard.app:app --host 127.0.0.1 --port 8000 --reload
```

## Docker Build

To build the standalone backend container:
```bash
docker build -t harbourwatch-backend .
```
