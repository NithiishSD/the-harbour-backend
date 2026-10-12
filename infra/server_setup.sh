#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# HarbourWatch — server bootstrap / first-time setup script
#
# Run once on a fresh Cloud VM (Ubuntu 24.04 LTS recommended):
#   curl -fsSL https://raw.githubusercontent.com/YOUR_ORG/YOUR_REPO/main/infra/server_setup.sh | bash
#
# After this script completes, the CI/CD pipeline (cd.yml) handles all future
# deployments automatically on every push to main.
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

DEPLOY_DIR="$HOME/harbourwatch"
COMPOSE_FILE="$DEPLOY_DIR/infra/docker-compose.yml"

log()  { echo -e "\033[1;34m[setup]\033[0m $*"; }
ok()   { echo -e "\033[1;32m[  ok]\033[0m $*"; }
warn() { echo -e "\033[1;33m[warn]\033[0m $*"; }

# ── 1. Install Docker Engine (skip if already present) ────────────────────────
if ! command -v docker &>/dev/null; then
  log "Installing Docker Engine …"
  curl -fsSL https://get.docker.com | sh
  sudo usermod -aG docker "$USER"
  warn "Docker installed. You may need to log out and back in for group membership."
else
  ok "Docker already installed: $(docker --version)"
fi

# ── 2. Install Docker Compose plugin (v2) ─────────────────────────────────────
if ! docker compose version &>/dev/null; then
  log "Installing Docker Compose plugin …"
  DOCKER_CONFIG="${DOCKER_CONFIG:-$HOME/.docker}"
  mkdir -p "$DOCKER_CONFIG/cli-plugins"
  COMPOSE_URL="https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)"
  curl -SL "$COMPOSE_URL" -o "$DOCKER_CONFIG/cli-plugins/docker-compose"
  chmod +x "$DOCKER_CONFIG/cli-plugins/docker-compose"
fi
ok "Docker Compose: $(docker compose version)"

# ── 3. Create deploy directory structure ──────────────────────────────────────
log "Creating deploy directory: $DEPLOY_DIR"
mkdir -p "$DEPLOY_DIR"/{infra,data/{noaa/csv,cold,checkpoints,checkpoints-live/{state,queue},warehouse}}

# ── 4. Write the .env file if it does not exist ───────────────────────────────
if [ ! -f "$DEPLOY_DIR/.env" ]; then
  log "Creating blank .env from template …"
  cat > "$DEPLOY_DIR/.env" << 'ENV'
# ── HarbourWatch production .env ──
# Fill in secrets before first deploy.

AISSTREAM_API_KEY=

KAFKA_BOOTSTRAP=kafka:19092
HBASE_REST_URL=http://hbase:8080
DATA_DIR=/app/data

MONITOR_COUNTRY=SG
MONITOR_PORTS=
MONITOR_PARTNERS=auto
MONITOR_REGION_KM=150
MONITOR_MAX_PARTNERS=20
MONITOR_MIN_PARTNERS=5
MONITOR_AIS_PAD=2
MONITOR_AIS_REGION_KM=800
MONITOR_AIS_SOURCE_KM=500
MONITOR_FOLLOWABLE_HOURS=48
MONITOR_SOURCE_PORT_KM=60
MONITOR_STOP_KM=15
MONITOR_LEFT_KM=30
MONITOR_SLOW_KN=1.5

MAP_TRANSIT_HOURS=6
MAP_ARRIVED_HOURS=24
MAP_SILENT_HOURS=24

DARK_TIMEOUT_HOURS=6
MIN_ACTIVE_SHIPS=3

QUEUE_GROWTH_PCT=50
QUEUE_GROWTH_MIN=5

SPARK_MASTER=local[*]
SPARK_DRIVER_MEMORY=2g
SPARK_SHUFFLE_PARTITIONS=8
SPARK_UI=0

STREAM_SPARK_MASTER=local[2]
STREAM_DRIVER_MEMORY=1g
STREAM_SPARK_UI=0
STREAM_MEM_LIMIT=3g
STREAM_CPUS=2

SHIPS_CACHE_S=2
HISTORY_EVERY_S=600
LIVE_SINCE=$(date +%Y-%m-%d)
HISTORY_MAX_MB=1024
COLD_MAX_MB=2048
ENV
  warn "Review and fill in $DEPLOY_DIR/.env before starting the stack."
else
  ok ".env already exists, skipping."
fi

# ── 5. Install nightly cron job ───────────────────────────────────────────────
CRON_JOB="0 2 * * * cd $DEPLOY_DIR && NIGHTLY_DATE=\$(date -d yesterday +\%Y-\%m-\%d) docker compose -f infra/docker-compose.yml run --rm nightly >> /var/log/harbourwatch-nightly.log 2>&1"
if ! crontab -l 2>/dev/null | grep -qF "harbourwatch"; then
  log "Installing nightly cron job …"
  (crontab -l 2>/dev/null; echo "$CRON_JOB") | crontab -
  ok "Cron installed: 02:00 UTC daily nightly batch"
else
  ok "Cron job already present."
fi

# ── 6. Print next steps ───────────────────────────────────────────────────────
echo ""
echo "═══════════════════════════════════════════════════════════════════"
echo "  HarbourWatch server is ready."
echo ""
echo "  Next steps:"
echo "  1.  Edit $DEPLOY_DIR/.env  — add AISSTREAM_API_KEY etc."
echo "  2.  Add the following GitHub Actions secrets to your repo:"
echo "        DEPLOY_HOST         — public IP or hostname of this VM"
echo "        DEPLOY_USER         — SSH username (e.g. ubuntu)"
echo "        DEPLOY_SSH_KEY      — private key (matching ~/.ssh/authorized_keys here)"
echo "        DEPLOY_KNOWN_HOSTS  — output of: ssh-keyscan <HOST>"
echo "  3.  Push to main — the cd.yml pipeline will build, push, and deploy."
echo "  4.  First manual start (images must already be pulled by CD):"
echo "        docker compose -f $COMPOSE_FILE up -d --no-build"
echo "═══════════════════════════════════════════════════════════════════"
