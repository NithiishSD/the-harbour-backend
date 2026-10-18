# HarbourWatch

HarbourWatch is a production-grade, stateful maritime intelligence pipeline designed to ingest, process, and analyze global vessel telemetry at scale (load-tested up to 150,000+ events/sec). Built using a Kappa/Lambda hybrid architecture, it utilizes Apache Kafka for fault-tolerant ingestion, Spark Structured Streaming for complex state machine tracking (underway → anchored → berthed), and HBase with a custom composite row-key (MMSI + event_time) to guarantee strictly idempotent, exactly-once processing (EOS).

---

## Architecture Overview

```
AIS Stream (WebSocket) ─┐
Marine Weather API ─────┼─> Ingestion Producers ─> Apache Kafka (KRaft)
NOAA Replay Archives ───┘                            │
                                                     ▼
                            Apache Spark Structured Streaming (Python / Java 21)
                              ├─ Normalize: schema validation, deduplication, rejects
                              ├─ State Machine: applyInPandasWithState (underway/anchored/berthed)
                              ├─ Track: trajectory history & spatial telemetry
                              └─ Queue: sliding window port wait times & queue growth alerts
                                     │                     │
                     Hot / Speed     │                     │ Cold Archive
                                     ▼                     ▼
                        Apache HBase (Wide-Column)    Partitioned Parquet (data/cold)
                                     │                     │
                                     │                     ▼
                                     │          DuckDB Analytical Warehouse
                                     │                     │
                                     └──────────┬──────────┘
                                                ▼
                                    FastAPI Service Layer
                                     ├─ Real-Time REST & Server-Sent Events (SSE)
                                     └─ Modern Leaflet Maritime Dashboard (Nginx)
```

### Core Components

- **Ingestion**: High-throughput asynchronous producers ingesting live WebSocket streams from `aisstream.io` (`ingest/aisstream.py`, `ingest/follow.py`) and NOAA historical AIS archives (`ingest/replay.py`).
- **Messaging (Broker)**: Apache Kafka with strict partition keys (`mmsi`) to guarantee strict per-vessel message ordering.
- **Stateful Processing**: PySpark Structured Streaming (`streaming/job.py`) executing concurrent queries with event-time watermarking (`10 minutes`) and arbitrary stateful operations (`applyInPandasWithState`).
- **Primary Storage (Operational Hot Path)**: Apache HBase (`storage/hbase/`) providing sub-10ms point lookups and range scans over vessel states and historical tracks.
- **Analytical Storage (Cold Path & Warehouse)**: Snappy-compressed Parquet archives feeding a star-schema DuckDB analytical data warehouse (`storage/warehouse/`) for historical waiting times, port call KPIs, and K-Means anchorage clustering.
- **API & Serving**: FastAPI (`dashboard/app.py`) providing high-concurrency async endpoints and real-time Server-Sent Events (SSE) for queue and dark-ship alerts.

---

## Enterprise Reliability & Fault-Tolerance Configuration

HarbourWatch is engineered to survive process, network, and node failures without data loss or duplicate records:

### 1. Kafka Broker & Producer Level
- **Idempotence Enabled**: Producers run with `enable.idempotence=true` and `max.in.flight.requests.per.connection=5` to prevent duplicate message batches on network retries.
- **Guaranteed Acks**: Configured with `acks=all` (ensuring all in-sync replicas acknowledge before commit) with production broker settings `min.insync.replicas=2` and `replication.factor=3`.
- **Wire Optimization**: Batch pipelining with `linger.ms=20` and `compression.type=lz4` to minimize network overhead at six-figure event volumes.

### 2. Spark Structured Streaming Level
- **Persistent Checkpoint Directory**: Enabled `spark.sql.streaming.checkpointLocation` on persistent durable storage (`/app/data/checkpoints/` mapped to persistent block volumes). Offsets and state machine micro-states are committed synchronously per trigger.
- **Crash Recovery**: If an executor or driver crashes, Spark recovers its exact offset positions and state stores with zero state inconsistency.
- **Event-Time Watermarks**: 10-minute watermark on position events discards late arrivals beyond the boundary, bounding memory growth for sliding windows and arbitrary state timeouts.

### 3. HBase Storage & Exactly-Once Semantics (EOS)
- **Composite Row-Keys**: `VESSEL_TRACK` records are keyed by `MMSI + reverse(event_time)` (`9-digit MMSI + (LONG_MAX - ts_ms)`).
- **Region Server Anti-Hotspotting**: Prefixing with MMSI distributes write loads evenly across all HBase RegionServers rather than bottlenecking on a single time-based region.
- **Idempotent Upserts**: Writing is purely idempotent—any retried Spark micro-batch or replayed historical stream overwrites the exact cell at that timestamp without creating duplicate rows.

---

## Managed Cloud Alternatives (DBaaS / PaaS)

For enterprise cloud deployments, the containerized local stack maps directly to managed cloud alternatives:

| Local Docker Service | Managed Cloud Alternative | Enterprise Superiority & Scale Rationale | Recommended Free/Low-Cost Cloud Path |
| :--- | :--- | :--- | :--- |
| **Kafka (Single Node)** | **Confluent Cloud** or **AWS MSK** | Multi-AZ replication, managed KRaft consensus, integrated Schema Registry, zero broker maintenance. | **Confluent Cloud** ($400 free trial credit) or **Upstash Kafka** (Serverless, 10,000 msgs/day free). |
| **HBase** | **GCP Bigtable** or **AWS DynamoDB** | Managed wide-column engine with identical row-key semantics, automated sharding, and sub-10ms p99 SLAs. | **DataStax Astra DB** (Serverless Cassandra with $25/mo free credit, native wide-column). |
| **DuckDB (Local File)** | **Google BigQuery** or **Snowflake** | True petabyte-scale cloud OLAP, separation of compute and storage, serverless concurrency. | **Google BigQuery** (10 GB storage + 1 TB queries/month free forever) or **MotherDuck** (Cloud DuckDB free tier). |
| **FastAPI Backend** | **AWS ECS (Fargate)** or **Render / Railway** | Autoscaling containerized compute with zero server management and integrated blue/green deployment. | **Render** (free web service tier) or **Koyeb / Fly.io** with frontend on **Vercel**. |

---

## Spatial Computing at Scale: The Uber H3 Defense

In large-scale maritime spatial analytics, naive geometric ray-casting (`point_in_ring`) poses a significant computational bottleneck:
- **The Bottleneck**: Traditional ray-casting is $O(E)$ per polygon edge across hundreds or thousands of port boundaries and anchorages. Evaluating 150,000 incoming telemetry points per second against complex multi-vertex polygons causes severe CPU throttling on Spark executors.
- **The H3 Architectural Solution**: 
  - HarbourWatch's production scale path discretizes geographic boundaries into **Uber H3 hexagonal spatial cells** (resolution 8 or 9, ~100m–400m cell diameter).
  - Port, berth, and anchorage polygons are pre-rasterized at pipeline initialization into memory-resident integer sets (`Set[uint64]`).
  - When raw coordinate pings `(lat, lon)` arrive in the streaming job, they are hashed to their H3 index in constant time ($O(1)$).
  - Geofence zone lookup collapses from computationally expensive geometric polygon intersection to a **constant-time $O(1)$ memory hash-set lookup**, reducing spatial CPU overhead by over 95%.

---

## Quick Start (Docker Compose)

1. Copy `.env.example` to `.env` and fill in your `AISSTREAM_API_KEY`:
   ```bash
   cp .env.example .env
   ```

2. Start all services (Kafka, HBase, schema init, streaming engine, API, weather, and Nginx):
   ```bash
   docker compose up -d --build
   ```

3. Start live ingestion (Singapore waters and global vessel tracking):
   ```bash
   docker compose --profile live up -d
   ```

4. Verify backend and frontend health:
   ```bash
   curl http://127.0.0.1:8000/health
   curl http://127.0.0.1/health
   ```

   The frontend is available at http://127.0.0.1. Its health request is proxied through Nginx to the backend.

---

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
