#!/usr/bin/env bash
# Auto-deployment script for API-owned stack.
# Pulls new commits on main and deploys API services from the API-local compose file.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
API_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
PROJECT_ROOT_DEFAULT="$(cd "$API_DIR/.." && pwd)"

PROJECT_ROOT="${PROJECT_ROOT:-$PROJECT_ROOT_DEFAULT}"
BRANCH="${BRANCH:-main}"
COMPOSE_FILE="${COMPOSE_FILE:-$SCRIPT_DIR/docker-compose.api.prod.yml}"
LOG_FILE="${LOG_FILE:-$PROJECT_ROOT/auto-deploy.log}"
LOCK_FILE="${LOCK_FILE:-/tmp/3dtrees-api-auto-deploy.lock}"
DEPLOY_SERVICES="${DEPLOY_SERVICES:-api status-pooler download-worker}"

mkdir -p "$(dirname "$LOG_FILE")"

log() {
    printf "%s: %s\n" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$1" >> "$LOG_FILE"
}

exec 9>"$LOCK_FILE"
if ! flock -n 9; then
    log "Deployment already running; skipping this cron tick."
    exit 0
fi

if [[ ! -f "$COMPOSE_FILE" ]]; then
    log "Compose file not found: $COMPOSE_FILE"
    exit 1
fi

cd "$API_DIR"

git fetch origin "$BRANCH" >> "$LOG_FILE" 2>&1

LOCAL_SHA="$(git rev-parse HEAD)"
REMOTE_SHA="$(git rev-parse "origin/$BRANCH")"

if [[ "$LOCAL_SHA" == "$REMOTE_SHA" ]]; then
    log "No changes"
    exit 0
fi

if ! git merge-base --is-ancestor "$LOCAL_SHA" "$REMOTE_SHA"; then
    log "Local branch diverged from origin/$BRANCH; aborting deploy."
    exit 1
fi

log "New changes detected; deploying $REMOTE_SHA"
git pull --ff-only origin "$BRANCH" >> "$LOG_FILE" 2>&1

export GIT_SHA
GIT_SHA="$(git rev-parse HEAD)"
export BUILD_TIME
BUILD_TIME="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
export DEPLOY_ENV
DEPLOY_ENV="${DEPLOY_ENV:-production}"

IFS=' ' read -r -a SERVICES <<< "$DEPLOY_SERVICES"

log "Building services: $DEPLOY_SERVICES"
docker compose -f "$COMPOSE_FILE" build "${SERVICES[@]}" >> "$LOG_FILE" 2>&1

log "Starting services: $DEPLOY_SERVICES"
docker compose -f "$COMPOSE_FILE" up -d "${SERVICES[@]}" >> "$LOG_FILE" 2>&1

log "Deployment complete ($(git rev-parse --short HEAD))"
