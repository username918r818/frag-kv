#!/usr/bin/env bash
# Запуск одного эксперимента на уже работающем кластере.
# Кластер должен быть поднят заранее!
#
# Использование:
#   ./run_single.sh throughput
#   ./run_single.sh loadbalance
#   ./run_single.sh availability
#   ./run_single.sh availability-sim
#   ./run_single.sh rebalance
#   ./run_single.sh rolling
#   ./run_single.sh chaos
#
# Если кластер не запущен:
#   docker-compose -f deploy/docker-compose.yml up -d meta-1 meta-2 meta-3 \
#     storage-1 storage-2 storage-3 storage-4 storage-5 prometheus
#   sleep 30
set -euo pipefail

COMPOSE="docker-compose -f deploy/docker-compose.yml"
META="http://meta-1:9000"
META_HOST="${META_HOST:-http://localhost:9000}"
PROM="http://prometheus:9090"
PROM_HOST="${PROM_HOST:-http://localhost:9090}"
RESULTS="/results"
RESULTS_HOST="${RESULTS_HOST:-experiments/results}"
BENCH_LOG="/tmp/fragkv_bench_single.log"
SIZE_MB="${SIZE_MB:-100}"
KEYS="${KEYS:-20}"
REBALANCE_SETTLE_SECS="${REBALANCE_SETTLE_SECS:-30}"

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'
log()  { echo -e "${GREEN}[+]${NC} $*"; }
warn() { echo -e "${YELLOW}[!]${NC} $*"; }
die()  { echo -e "${RED}[✗]${NC} $*" >&2; exit 1; }

SCENARIO="${1:-throughput}"
log "Запускаем: $SCENARIO (keys=$KEYS, size=${SIZE_MB}MB)"
log "Лог bench: $BENCH_LOG"
> "$BENCH_LOG"

bench() {
  $COMPOSE --profile bench run --rm --build bench \
    --meta "$META" --prometheus "$PROM" --out "$RESULTS" "$@" 2>&1 | tee -a "$BENCH_LOG"
}

bench_bg() {
  $COMPOSE --profile bench run --rm --build bench \
    --meta "$META" --prometheus "$PROM" --out "$RESULTS" "$@" >> "$BENCH_LOG" 2>&1 &
  echo $!
}

prom_query() {
  local query="$1"
  curl -fsG "$PROM_HOST/api/v1/query" --data-urlencode "query=$query" | \
    python3 -c 'import json,sys; r=json.load(sys.stdin).get("data",{}).get("result",[]); print(int(float(r[0]["value"][1])) if r else 0)'
}

now_ms() {
  python3 -c 'import time; print(int(time.time() * 1000))'
}

wait_prom_query_gt() {
  local query="$1" min_value="$2" timeout="${3:-60}" value=0
  for _ in $(seq 1 "$timeout"); do
    value=$(prom_query "$query" 2>/dev/null || echo 0)
    [ "$value" -gt "$min_value" ] && { echo "$value"; return 0; }
    sleep 1
  done
  return 1
}

wait_storage_nodes() {
  local expected="${1:-5}" timeout="${2:-90}" count=0
  log "Ждём регистрации $expected storage-узлов..."
  for _ in $(seq 1 "$timeout"); do
    count=$(curl -fs "$META_HOST/nodes" 2>/dev/null | python3 -c 'import json,sys; data=sys.stdin.read(); print(sum(1 for n in json.loads(data) if n.get("status")=="up")) if data else print(0)' 2>/dev/null || echo 0)
    [ "$count" -ge "$expected" ] && { log "$count storage-узлов зарегистрированы"; return 0; }
    sleep 1
  done
  die "За ${timeout} сек зарегистрировалось только $count/$expected storage-узлов. Проверь Raft/metad: curl $META_HOST/status && curl $META_HOST/nodes"
}

ensure_storage6_absent() {
  if curl -fs "$META_HOST/nodes" 2>/dev/null | python3 -c 'import json,sys; data=sys.stdin.read(); print(any(n.get("id")=="storage-6" for n in json.loads(data)) if data else False)' | grep -q True; then
    die "storage-6 уже есть в membership. Перезапусти кластер без storage-6 перед rebalance (например через ./run_all.sh или docker-compose down -v)."
  fi
}

write_rebalance_csv() {
  local total_bytes="$1" moved_bytes="$2" duration_ms="$3" method="$4"
  mkdir -p "$RESULTS_HOST"
  python3 - "$RESULTS_HOST/rebalance.csv" "$total_bytes" "$moved_bytes" "$duration_ms" "$method" <<'PY'
import csv
import sys

path, total_s, moved_s, duration_s, method = sys.argv[1:]
total = int(total_s)
moved = int(moved_s)
fraction = (moved / total) if total else 0.0

with open(path, "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["total_bytes", "moved_bytes", "fraction_moved", "duration_ms", "expected_fraction", "measure_method"])
    w.writerow([total, moved, f"{fraction:.4f}", duration_s, "0.0000", method])
PY
}

wait_bench_ready() {
  local marker="$1" extra="${2:-3}"
  log "Ждём маркера '$marker' в логе..."
  for i in $(seq 1 150); do
    grep -q "$marker" "$BENCH_LOG" 2>/dev/null && {
      log "Bench готов ✓ (${i}×2с)"
      sleep "$extra"
      return 0
    }
    sleep 2
  done
  warn "Таймаут — продолжаем"
}

case "$SCENARIO" in

  throughput)
    bench --scenario throughput --keys "$KEYS"
    ;;

  loadbalance)
    wait_storage_nodes 5 90
    bench --scenario loadbalance --keys "$KEYS" --size "$SIZE_MB"
    ;;

  availability)
    wait_storage_nodes 5 90
    BENCH_PID=$(bench_bg --scenario availability --keys "$KEYS" --size "$SIZE_MB"; echo $!)
    wait_bench_ready "Kill storage nodes now" 3
    log "Останавливаем storage-1..."
    docker stop deploy-storage-1-1
    sleep 20
    log "Останавливаем storage-2..."
    docker stop deploy-storage-2-1
    wait "$BENCH_PID" || true
    [ -f "experiments/results/availability.csv" ] && log "OK: availability.csv ✓" || die "Файл не создан"
    log "Восстанавливаем узлы..."
    docker start deploy-storage-1-1 deploy-storage-2-1
    ;;

  availability-sim)
    wait_storage_nodes 5 90
    BENCH_PID=$(bench_bg --scenario availability-sim --keys "$KEYS" --size "$SIZE_MB"; echo $!)
    wait_bench_ready "Kill storage nodes now" 3
    log "Одновременно останавливаем storage-1 и storage-3..."
    docker stop deploy-storage-1-1 deploy-storage-3-1
    sleep 45
    wait "$BENCH_PID" || true
    [ -f "experiments/results/availability_sim.csv" ] && log "OK: availability_sim.csv ✓" || {
      warn "Файл не создан. Лог:"; tail -20 "$BENCH_LOG"; die "Ошибка"
    }
    log "Восстанавливаем узлы..."
    docker start deploy-storage-1-1 deploy-storage-3-1
    ;;

  rebalance)
    wait_storage_nodes 5 90
    ensure_storage6_absent
    bench --scenario loadbalance --keys "$KEYS" --size "$SIZE_MB"
    log "Ждём Prometheus scrape (10 сек)..."
    sleep 10

    log "Снимаем baseline до запуска storage-6..."
    TOTAL_BEFORE=$(wait_prom_query_gt 'sum(kv_node_used_bytes{node_id!="storage-6"})' 1 60) || die "Prometheus не вернул kv_node_used_bytes"
    REBALANCE_BEFORE=$(prom_query 'sum(kv_rebalance_bytes_total{instance="storage-6:8006"})' 2>/dev/null || echo 0)

    log "Запускаем storage-6..."
    START_MS=$(now_ms)
    $COMPOSE --profile rebalance up -d storage-6
    log "Ждём завершения rebalance и scrape Prometheus (${REBALANCE_SETTLE_SECS} сек)..."
    sleep "$REBALANCE_SETTLE_SECS"
    END_MS=$(now_ms)

    REBALANCE_AFTER=$(prom_query 'sum(kv_rebalance_bytes_total{instance="storage-6:8006"})' 2>/dev/null || echo 0)
    MOVED_BYTES=$((REBALANCE_AFTER - REBALANCE_BEFORE))
    [ "$MOVED_BYTES" -gt 0 ] || die "Prometheus не зафиксировал перемещение на storage-6 (before=$REBALANCE_BEFORE after=$REBALANCE_AFTER)"

    DURATION_MS=$((END_MS - START_MS))
    write_rebalance_csv "$TOTAL_BEFORE" "$MOVED_BYTES" "$DURATION_MS" "prometheus_counter_storage6"
    log "rebalance: moved=$((MOVED_BYTES / 1024 / 1024)) MB / total=$((TOTAL_BEFORE / 1024 / 1024)) MB"
    [ -f "experiments/results/rebalance.csv" ] && log "OK: rebalance.csv ✓" || die "Файл не создан"
    ;;

  rolling)
    wait_storage_nodes 5 90
    BENCH_PID=$(bench_bg --scenario rolling --keys "$KEYS" --size "$SIZE_MB"; echo $!)
    wait_bench_ready "Bash script will now kill nodes" 3
    for NODE in storage-1 storage-2 storage-3 storage-4 storage-5; do
      log "Убиваем $NODE..."
      docker stop "deploy-${NODE}-1"
      sleep 20
      log "Восстанавливаем $NODE..."
      docker start "deploy-${NODE}-1"
      sleep 20
    done
    wait "$BENCH_PID" || true
    [ -f "experiments/results/rolling.csv" ] && log "OK: rolling.csv ✓" || die "Файл не создан"
    ;;

  chaos)
    wait_storage_nodes 5 90
    bench --scenario chaos --keys "$KEYS" --size "$SIZE_MB" --seed 0
    [ -f "experiments/results/chaos.csv" ] && log "OK: chaos.csv ✓" || die "Файл не создан"
    ;;

  screenshots)
    log "Заполняем данные для визуализации (30 ключей × 10 МБ)..."
    wait_storage_nodes 5 90
    ensure_storage6_absent
    bench --scenario loadbalance --keys 30 --size 10

    log "Запускаем storage-6 для rebalance-графика..."
    log "Ждём scrape Prometheus (10 сек)..."
    sleep 10
    TOTAL_BEFORE=$(wait_prom_query_gt 'sum(kv_node_used_bytes{node_id!="storage-6"})' 1 60) || die "Prometheus не вернул kv_node_used_bytes"
    REBALANCE_BEFORE=$(prom_query 'sum(kv_rebalance_bytes_total{instance="storage-6:8006"})' 2>/dev/null || echo 0)
    START_MS=$(now_ms)
    $COMPOSE --profile rebalance up -d storage-6
    sleep "$REBALANCE_SETTLE_SECS"
    END_MS=$(now_ms)
    REBALANCE_AFTER=$(prom_query 'sum(kv_rebalance_bytes_total{instance="storage-6:8006"})' 2>/dev/null || echo 0)
    MOVED_BYTES=$((REBALANCE_AFTER - REBALANCE_BEFORE))
    [ "$MOVED_BYTES" -gt 0 ] || die "Prometheus не зафиксировал перемещение на storage-6 (before=$REBALANCE_BEFORE after=$REBALANCE_AFTER)"
    DURATION_MS=$((END_MS - START_MS))
    write_rebalance_csv "$TOTAL_BEFORE" "$MOVED_BYTES" "$DURATION_MS" "prometheus_counter_storage6"

    log "Ждём scrape Prometheus (10 сек)..."
    sleep 10

    log "Открываем туннель на localhost:9090..."
    COLIMA_SSH_CFG=$(mktemp)
    colima ssh-config > "$COLIMA_SSH_CFG"
    ssh -F "$COLIMA_SSH_CFG" -N -L 9090:localhost:9090 colima &
    TUNNEL_PID=$!
    rm -f "$COLIMA_SSH_CFG"
    sleep 2

    echo ""
    echo "  ┌─────────────────────────────────────────────────────────────┐"
    echo "  │  Prometheus: http://localhost:9090                          │"
    echo "  │                                                             │"
    echo "  │  Скриншот 1 → запрос: kv_node_used_bytes                   │"
    echo "  │               вкладка Graph → сохранить:                   │"
    echo "  │               experiments/results/prometheus_disk_usage.png │"
    echo "  │                                                             │"
    echo "  │  Скриншот 2 → запрос: kv_rebalance_bytes_total             │"
    echo "  │               вкладка Graph → сохранить:                   │"
    echo "  │               experiments/results/prometheus_rebalance.png  │"
    echo "  └─────────────────────────────────────────────────────────────┘"
    echo ""
    read -r -p "  Нажми Enter когда скриншоты сохранены..."

    kill $TUNNEL_PID 2>/dev/null || true
    log "Туннель закрыт ✓"
    ;;

  plots)
    log "Генерируем все графики из CSV..."
    VENV="/tmp/thesis-venv"
    [ ! -d "$VENV" ] && python3 -m venv "$VENV" && "$VENV/bin/pip" install matplotlib numpy -q
    "$VENV/bin/python3" experiments/results/plot.py
    log "Готово: experiments/results/fig*.png"
    ;;

  *)
    die "Неизвестный сценарий: $SCENARIO. Доступны: throughput loadbalance availability availability-sim rebalance rolling chaos screenshots plots"
    ;;
esac

log "Готово!"
