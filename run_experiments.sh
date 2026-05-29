#!/usr/bin/env bash
# Полный прогон всех экспериментов для ВКР.
# Запуск: ./run_experiments.sh
set -euo pipefail

COMPOSE="docker-compose -f deploy/docker-compose.yml"
META="http://meta-1:9000"
PROM="http://prometheus:9090"
RESULTS="/results"

# ---- цвета -------------------------------------------------------------------
GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'
log()  { echo -e "${GREEN}[+]${NC} $*"; }
warn() { echo -e "${YELLOW}[!]${NC} $*"; }
die()  { echo -e "${RED}[✗]${NC} $*" >&2; exit 1; }

bench() {
  $COMPOSE --profile bench run --rm bench \
    --meta "$META" --prometheus "$PROM" --out "$RESULTS" "$@"
}

# ---- утилиты -----------------------------------------------------------------

wait_nodes() {
  local expected=$1
  log "Ждём регистрации $expected storage-узлов..."
  for i in $(seq 1 30); do
    count=$(docker exec deploy-meta-1-1 \
      wget -qO- http://localhost:9000/nodes 2>/dev/null | \
      python3 -c "import sys,json; n=json.load(sys.stdin); print(sum(1 for x in n if x['status']=='up'))" 2>/dev/null || echo 0)
    [ "$count" -ge "$expected" ] && { log "$count узлов up"; return 0; }
    sleep 2
  done
  die "Узлы не зарегистрировались за 60 сек"
}

prom_query() {
  colima ssh -- curl -s "http://localhost:9090/api/v1/query?query=$1" 2>/dev/null | \
    python3 -c "import sys,json; r=json.load(sys.stdin)['data']['result']; print(int(float(r[0]['value'][1])) if r else 0)"
}

# =============================================================================
# ШАГ 0. Проверка
# =============================================================================
log "Проверка окружения..."
go test ./... -count=1 -timeout 60s 2>&1 | grep -E "^ok|FAIL" || die "Тесты не прошли"
docker info > /dev/null 2>&1 || die "Docker не запущен"
log "OK"

# =============================================================================
# ШАГ 1. Сборка образов
# =============================================================================
log "Сборка Docker-образов..."
$COMPOSE build --parallel

# =============================================================================
# ШАГ 2. Запуск кластера
# =============================================================================
log "Очистка предыдущего запуска..."
$COMPOSE down -v 2>/dev/null || true

log "Запуск metadata-кластера (Raft)..."
$COMPOSE up -d meta-1 meta-2 meta-3

log "Ждём лидера Raft..."
for i in $(seq 1 20); do
  leader=$(docker exec deploy-meta-1-1 \
    wget -qO- http://localhost:9000/status 2>/dev/null | \
    python3 -c "import sys,json; print(json.load(sys.stdin).get('is_leader','false'))" 2>/dev/null || echo false)
  [ "$leader" = "True" ] || [ "$leader" = "true" ] && break
  sleep 2
done

log "Запуск storage-узлов и Prometheus..."
$COMPOSE up -d storage-1 storage-2 storage-3 storage-4 storage-5 prometheus
wait_nodes 5

# =============================================================================
# ШАГ 3. Эксперимент 1 — Пропускная способность
# =============================================================================
log "=== Эксперимент 1: Throughput & Latency ==="
bench --scenario throughput --keys 10
log "Результат: experiments/results/throughput.csv ✓"

# =============================================================================
# ШАГ 4. Эксперимент 3 — Равномерность нагрузки
# (до availability, чтобы кластер был чистым)
# =============================================================================
log "=== Эксперимент 3: Load Balance ==="
bench --scenario loadbalance --keys 50 --size 10
log "Результат: experiments/results/loadbalance.csv ✓"

# =============================================================================
# ШАГ 5. Эксперимент 2 — Доступность при отказах
# =============================================================================
log "=== Эксперимент 2: Availability ==="

# Запускаем bench в фоне (90 сек мониторинга)
$COMPOSE --profile bench run --rm \
  -e META="$META" -e PROM="$PROM" \
  bench \
  --meta "$META" --prometheus "$PROM" --out "$RESULTS" \
  --scenario availability --keys 20 --size 10 &
BENCH_PID=$!

# Ждём пока данные запишутся (~15 сек)
log "Пишем данные, ждём 18 сек..."
sleep 18

log "Останавливаем storage-1..."
docker stop deploy-storage-1-1

log "Ждём 20 сек, останавливаем storage-2..."
sleep 20
docker stop deploy-storage-2-1

# Ждём завершения bench
wait $BENCH_PID
log "Результат: experiments/results/availability.csv ✓"

# Восстанавливаем узлы
log "Восстанавливаем storage-1 и storage-2..."
docker start deploy-storage-1-1 deploy-storage-2-1
log "Ждём recovery (20 сек)..."
sleep 20
wait_nodes 5

# =============================================================================
# ШАГ 6. Эксперимент 4 — Rebalance при масштабировании
# =============================================================================
log "=== Эксперимент 4: Rebalance ==="

# Поднимаем storage-6 ДО запуска bench
# (bench сам зарегистрирует его через metad API и измерит)
log "Запускаем storage-6..."
$COMPOSE --profile rebalance up -d storage-6
sleep 3  # даём контейнеру стартовать

log "Запускаем rebalance-эксперимент (заполнение + регистрация узла + измерение)..."
bench \
  --scenario rebalance \
  --keys 20 --size 10 \
  --new-node-id storage-6 \
  --new-node-addr storage-6:8006
log "Результат: experiments/results/rebalance.csv ✓"

# =============================================================================
# ШАГ 7. Prometheus-скриншоты (интерактивно)
# =============================================================================
log "=== Prometheus-скриншоты ==="

# Открываем SSH-туннель чтобы Prometheus был доступен на localhost:9090
COLIMA_SSH_CFG=$(mktemp)
colima ssh-config > "$COLIMA_SSH_CFG"
ssh -F "$COLIMA_SSH_CFG" -N -L 9090:localhost:9090 colima &
rm -f "$COLIMA_SSH_CFG"
TUNNEL_PID=$!
sleep 2

echo ""
echo "  Prometheus доступен на http://localhost:9090"
echo ""
echo "  Сделай два скриншота:"
echo ""
echo "  1) Вставь запрос: kv_node_used_bytes"
echo "     → вкладка Graph"
echo "     → сохрани как: experiments/results/prometheus_disk_usage.png"
echo ""
echo "  2) Вставь запрос: kv_rebalance_bytes_total"
echo "     → вкладка Graph"
echo "     → сохрани как: experiments/results/prometheus_rebalance.png"
echo ""
read -r -p "  Нажми Enter когда оба скриншота сохранены... "

kill $TUNNEL_PID 2>/dev/null || true
log "Туннель закрыт"

# =============================================================================
# ШАГ 8. Генерация графиков
# =============================================================================
log "=== Генерация графиков ==="

VENV="/tmp/thesis-venv"
if [ ! -d "$VENV" ]; then
  python3 -m venv "$VENV"
  "$VENV/bin/pip" install matplotlib numpy -q
fi

"$VENV/bin/python3" experiments/results/plot.py
log "Графики: experiments/results/fig1-4.png ✓"

# =============================================================================
# ШАГ 8. Итог
# =============================================================================
log "=== Готово ==="
echo ""
echo "Файлы для ВКР:"
ls -lh experiments/results/*.csv experiments/results/*.png 2>/dev/null
echo ""
warn "Prometheus-скриншоты — вручную:"
warn "  Открыть туннель: ssh -N -L 9090:localhost:9090 ..."
warn "  Перейти: http://localhost:9090"
warn "  Запрос 1: kv_node_used_bytes  → сохранить как prometheus_disk_usage.png"
warn "  Запрос 2: kv_rebalance_bytes_total → сохранить как prometheus_rebalance.png"
echo ""
log "Кластер работает. Остановить: docker-compose -f deploy/docker-compose.yml down"
