#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# HarbourWatch — Start Demo Stack & Health Check Script
#
# Starts the stack in correct dependency order, validates readiness,
# and verifies health endpoints.
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACKEND_DIR="${SCRIPT_DIR}/backend"
if [[ ! -d "${BACKEND_DIR}" ]]; then
  BACKEND_DIR="${SCRIPT_DIR}"
fi

cd "${BACKEND_DIR}"

log()  { echo -e "\033[1;34m[demo]\033[0m $*"; }
ok()   { echo -e "\033[1;32m[  ok]\033[0m $*"; }
warn() { echo -e "\033[1;33m[warn]\033[0m $*"; }
err()  { echo -e "\033[1;31m[fail]\033[0m $*" >&2; exit 1; }

# ── 1. Environment Check ──────────────────────────────────────────────────────
log "Checking configuration..."
if [[ ! -f .env ]]; then
  if [[ -f .env.example ]]; then
    warn "No .env found; copying .env.example"
    cp .env.example .env
  else
    err "Neither .env nor .env.example found in ${BACKEND_DIR}"
  fi
fi
ok "Environment configuration ready"

# ── 2. Start Core Infrastructure (Kafka + HBase) ─────────────────────────────
log "Starting core storage and messaging (Kafka, HBase)..."
docker compose up -d kafka hbase

log "Waiting for Kafka and HBase health checks to pass..."
MAX_WAIT=120
WAITED=0
while [[ $WAITED -lt $MAX_WAIT ]]; do
  KAFKA_HEALTH=$(docker compose ps --format json kafka 2>/dev/null | grep -o '"Health":"[^"]*"' | cut -d'"' -f4 || echo "starting")
  HBASE_HEALTH=$(docker compose ps --format json hbase 2>/dev/null | grep -o '"Health":"[^"]*"' | cut -d'"' -f4 || echo "starting")

  # Fallback check if JSON format is not supported
  if [[ -z "$KAFKA_HEALTH" ]]; then
    if docker compose ps kafka | grep -qi "healthy"; then KAFKA_HEALTH="healthy"; fi
  fi
  if [[ -z "$HBASE_HEALTH" ]]; then
    if docker compose ps hbase | grep -qi "healthy"; then HBASE_HEALTH="healthy"; fi
  fi

  if [[ "$KAFKA_HEALTH" == "healthy" && "$HBASE_HEALTH" == "healthy" ]]; then
    break
  fi

  sleep 5
  WAITED=$((WAITED + 5))
  echo -n "."
done
echo ""

if [[ $WAITED -ge $MAX_WAIT ]]; then
  warn "Healthcheck timeout reached. Verifying endpoints directly..."
fi

# Direct endpoint probes
curl -fs http://127.0.0.1:8080/version/cluster >/dev/null 2>&1 && ok "HBase REST gateway responding on 8080" || warn "HBase still starting"

# ── 3. Run Schema Initialization ─────────────────────────────────────────────
log "Initializing Kafka topics and HBase tables..."
docker compose run --rm init
ok "Kafka topics and HBase tables initialized"

# ── 4. Start Application Services ─────────────────────────────────────────────
log "Starting streaming, API, weather, frontend, ingest, and nightly batch services..."
docker compose up -d streaming dashboard frontend weather history ingest nightly
ok "All service containers launched"

# ── 5. Run Health Checks ──────────────────────────────────────────────────────
log "Validating API and Frontend health..."
HEALTH_MAX=60
H_WAITED=0
API_OK=false

while [[ $H_WAITED -lt $HEALTH_MAX ]]; do
  if curl -sf http://127.0.0.1:8000/health >/dev/null 2>&1; then
    API_OK=true
    break
  fi
  sleep 2
  H_WAITED=$((H_WAITED + 2))
done

if [[ "$API_OK" == "true" ]]; then
  ok "FastAPI Backend is healthy: http://127.0.0.1:8000/health"
else
  warn "FastAPI health check did not respond within ${HEALTH_MAX}s"
fi

if curl -sf http://127.0.0.1/health >/dev/null 2>&1; then
  ok "Nginx Frontend is healthy: http://127.0.0.1/health"
fi

# ── 6. Summary ────────────────────────────────────────────────────────────────
echo ""
echo "=========================================================================="
echo "  HarbourWatch Automated Stack is Running!"
echo "=========================================================================="
echo "  Frontend Dashboard : http://localhost"
echo "  FastAPI Swagger UI : http://localhost:8000/docs"
echo "  FastAPI Health     : http://localhost:8000/health"
echo "  HBase Master UI    : http://localhost:16010"
echo "  HBase REST Gateway : http://localhost:8080"
echo ""
echo "  Automated Ingest   : Active (live if AISSTREAM_API_KEY set, else looping replay)"
echo "  Automated DuckDB   : Active (nightly batch updates warehouse KPIs hourly)"
echo "=========================================================================="
