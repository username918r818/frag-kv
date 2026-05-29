#!/usr/bin/env bash
# Полный воспроизводимый прогон экспериментов для ВКР.
# Каждый эксперимент изолирован: кластер пересоздаётся с нуля.
# Объём данных: 100 ключей × 100 МБ = 10 ГБ уникального, 30 ГБ физического.
#
# Запуск: ./run_experiments_v2.sh
# Время: ~40-60 минут
set -euo pipefail

COMPOSE="docker-compose -f deploy/docker-compose.yml"
META="http://meta-1:9000"
PROM="http://prometheus:9090"
RESULTS="/results"

# Параметры экспериментов
KEYS_THROUGHPUT=20       # ключей × (1 + 10 + 100 МБ) = ~2.2 ГБ уникального
KEYS_LOADBALANCE=100     # ключей × 100 МБ = 10 ГБ уникального
KEYS_AVAILABILITY=50     # ключей × 100 МБ = 5 ГБ уникального
SIZE_MB=100              # размер одного значения
KEYS_REBALANCE=50        # ключей × 100 МБ = 5 ГБ уникального

# ---- вспомогательные функции ------------------------------------------------

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; CYAN='\033[0;36m'; NC='\033[0m'
log()   { echo -e "${GREEN}[+]${NC} $*"; }
step()  { echo -e "\n${CYAN}════════════════════════════════════════${NC}"; echo -e "${CYAN} $*${NC}"; echo -e "${CYAN}════════════════════════════════════════${NC}"; }
warn()  { echo -e "${YELLOW}[!]${NC} $*"; }
die()   { echo -e "${RED}[✗]${NC} $*" >&2; exit 1; }

BENCH_LOG="/tmp/fragkv_bench.log"

bench() {
  $COMPOSE --profile bench run --rm bench \
    --meta "$META" --prometheus "$PROM" --out "$RESULTS" "$@" 2>&1 | tee -a "$BENCH_LOG"
  return "${PIPESTATUS[0]}"
}

check_result() {
  local file="$1" label="$2"
  if [ -f "experiments/results/$file" ]; then
    log "Результат → experiments/results/$file ✓"
  else
    warn "ФАЙЛ НЕ СОЗДАН: experiments/results/$file"
    warn "Последние строки лога bench:"
    tail -20 "$BENCH_LOG" | sed 's/^/  /'
    die "Эксперимент '$label' не завершился успешно"
  fi
}

# Ждёт пока bench напишет в лог что данные записаны, затем доп. пауза.
wait_bench_ready() {
  local marker="${1:-data written}"
  local extra_sleep="${2:-5}"
  log "Ждём готовности bench (маркер: '$marker')..."
  for i in $(seq 1 120); do
    grep -q "$marker" "$BENCH_LOG" 2>/dev/null && {
      log "Bench готов ✓ (нашли '$marker' на $((i*2))с)"
      sleep "$extra_sleep"
      return 0
    }
    sleep 2
  done
  warn "Таймаут ожидания bench — продолжаем всё равно"
}

fresh_cluster() {
  log "Пересоздаём кластер..."
  # Явно останавливаем storage-6 (запускается через --profile rebalance, down его не трогает)
  $COMPOSE --profile rebalance down -v 2>/dev/null || true
  $COMPOSE down -v 2>/dev/null || true
  docker rm -f deploy-storage-6-1 2>/dev/null || true
  $COMPOSE up -d meta-1 meta-2 meta-3

  log "Ждём лидера Raft (до 60 сек)..."
  for i in $(seq 1 30); do
    state=$(docker exec deploy-meta-1-1 wget -qO- http://localhost:9000/status 2>/dev/null \
      | python3 -c "import sys,json; print(json.load(sys.stdin).get('state',''))" 2>/dev/null || echo "")
    if [ "$state" = "Leader" ]; then log "Raft leader elected ✓"; break; fi
    sleep 2
  done

  log "Запускаем storage + Prometheus..."
  $COMPOSE up -d storage-1 storage-2 storage-3 storage-4 storage-5 prometheus

  log "Ждём storage-узлов (до 3 мин)..."
  for i in $(seq 1 60); do
    count=$(docker exec deploy-meta-1-1 wget -qO- http://localhost:9000/nodes 2>/dev/null \
      | python3 -c "import sys,json; n=json.load(sys.stdin); print(sum(1 for x in n if x['status']=='up'))" 2>/dev/null || echo 0)
    [ "$count" -ge 5 ] && { log "$count storage-узлов up ✓"; break; }
    [ "$i" -eq 60 ] && die "Storage-узлы не зарегистрировались за 3 мин"
    sleep 3
  done

  log "Ждём Prometheus (до 60 сек)..."
  for i in $(seq 1 20); do
    prom=$(docker exec deploy-prometheus-1 wget -qO- http://localhost:9090/-/ready 2>/dev/null || echo "")
    if [ "$prom" = "Prometheus Server is Ready." ]; then log "Prometheus ready ✓"; return 0; fi
    sleep 3
  done
  die "Prometheus не поднялся за 60 сек"
}

verify_prometheus() {
  log "Проверяем Prometheus из bench-контейнера..."
  result=$($COMPOSE --profile bench run --rm --entrypoint="" bench \
    wget -qO- "http://prometheus:9090/-/ready" 2>/dev/null || echo "FAIL")
  if [ "$result" = "Prometheus Server is Ready." ]; then
    log "Prometheus доступен из bench ✓"
  else
    warn "Prometheus недоступен из bench ($result) — продолжаем без него"
  fi
}

# =============================================================================
step "0. Проверка окружения"
# =============================================================================
log "Проверяем тесты..."
go test ./... -count=1 -timeout 60s 2>&1 | grep -E "^ok|FAIL"
go build ./... 2>&1
docker info > /dev/null 2>&1 || die "Docker не запущен"
log "Сборка образов..."
$COMPOSE build --parallel
log "Окружение готово ✓"

# =============================================================================
step "1. Эксперимент: Пропускная способность (Throughput)"
# Изолированный кластер, 20 ключей × (1 + 10 + 100 МБ)
# =============================================================================
fresh_cluster
verify_prometheus

bench --scenario throughput --keys "$KEYS_THROUGHPUT"
log "Результат → experiments/results/throughput.csv ✓"
$COMPOSE down -v

# =============================================================================
step "2. Эксперимент: Равномерность нагрузки (Load Balance)"
# Изолированный кластер, 100 ключей × 100 МБ = 10 ГБ уникального
# =============================================================================
fresh_cluster
verify_prometheus

bench --scenario loadbalance --keys "$KEYS_LOADBALANCE" --size "$SIZE_MB"
log "Результат → experiments/results/loadbalance.csv ✓"
$COMPOSE down -v

# =============================================================================
step "3. Эксперимент: Доступность при отказах (Availability)"
# Изолированный кластер, 50 ключей × 100 МБ
# Убиваем storage-1 на t=18с, storage-2 на t=38с
# =============================================================================
fresh_cluster
verify_prometheus

$COMPOSE --profile bench run --rm bench \
  --meta "$META" --prometheus "$PROM" --out "$RESULTS" \
  --scenario availability --keys "$KEYS_AVAILABILITY" --size "$SIZE_MB" >> "$BENCH_LOG" 2>&1 &
BENCH_PID=$!

wait_bench_ready "Kill storage nodes now" 3
log "Останавливаем storage-1..."
docker stop deploy-storage-1-1

sleep 20
log "Останавливаем storage-2..."
docker stop deploy-storage-2-1

wait "$BENCH_PID" || true
check_result "availability.csv" "availability"
$COMPOSE down -v

# =============================================================================
step "4. Эксперимент: Rebalance при масштабировании"
# Изолированный кластер, 50 ключей × 100 МБ = 5 ГБ уникального
# Добавляем storage-6, измеряем через Prometheus
# =============================================================================
fresh_cluster
verify_prometheus

# Заполняем кластер данными отдельно (loadbalance сценарий)
log "Заполняем кластер: $KEYS_REBALANCE ключей × ${SIZE_MB} МБ..."
bench --scenario loadbalance --keys "$KEYS_REBALANCE" --size "$SIZE_MB"

# Ждём чтобы Prometheus успел снять свежие метрики
log "Ждём scrape Prometheus (10 сек)..."
sleep 10

# Поднимаем storage-6 (bench зарегистрирует его через metad API)
log "Запускаем storage-6..."
$COMPOSE --profile rebalance up -d storage-6
sleep 5  # даём контейнеру стартовать и зарегистрироваться

# Запускаем измерение (bench сам регистрирует узел и меряет через Prometheus)
bench --scenario rebalance \
  --keys 0 --size 1 \
  --new-node-id storage-6 \
  --new-node-addr storage-6:8006
log "Результат → experiments/results/rebalance.csv ✓"
$COMPOSE down -v

# =============================================================================
step "3b. Эксперимент 2b: Availability — 2 одновременных отказа"
# Изолированный кластер, 50 ключей × SIZE_MB
# Оба узла убиваются одновременно
# =============================================================================
fresh_cluster
verify_prometheus

$COMPOSE --profile bench run --rm bench \
  --meta "$META" --prometheus "$PROM" --out "$RESULTS" \
  --scenario availability-sim --keys "$KEYS_AVAILABILITY" --size "$SIZE_MB" >> "$BENCH_LOG" 2>&1 &
BENCH_PID=$!

wait_bench_ready "Kill storage nodes now" 3
log "Одновременно останавливаем storage-1 и storage-3..."
docker stop deploy-storage-1-1 deploy-storage-3-1

sleep 45

wait "$BENCH_PID" || true
check_result "availability_sim.csv" "availability-sim"
$COMPOSE down -v

# =============================================================================
step "4b. Эксперимент 5: Rolling Failure (каждый узел хоть раз упал)"
# Изолированный кластер, 20 ключей × SIZE_MB
# Каждые 50 сек убиваем следующий узел, через 20 сек поднимаем
# =============================================================================
fresh_cluster
verify_prometheus

$COMPOSE --profile bench run --rm bench \
  --meta "$META" --prometheus "$PROM" --out "$RESULTS" \
  --scenario rolling --keys 20 --size "$SIZE_MB" >> "$BENCH_LOG" 2>&1 &
BENCH_PID=$!

wait_bench_ready "Bash script will now kill nodes" 3

for NODE in storage-1 storage-2 storage-3 storage-4 storage-5; do
  log "Убиваем $NODE..."
  docker stop "deploy-${NODE}-1"

  sleep 20
  log "Восстанавливаем $NODE..."
  docker start "deploy-${NODE}-1"

  log "Ждём recovery $NODE (20 сек)..."
  sleep 20
done

wait "$BENCH_PID" || true
check_result "rolling.csv" "rolling"
$COMPOSE down -v

# =============================================================================
step "5. Эксперимент 6: Chaos Monkey"
# =============================================================================
fresh_cluster
verify_prometheus

log "Запускаем chaos monkey (300 сек, случайный seed)..."
bench \
  --scenario chaos \
  --keys 20 --size "$SIZE_MB" \
  --seed 0
check_result "chaos.csv" "chaos"
$COMPOSE down -v

# =============================================================================
step "6. Prometheus-скриншоты"
# Поднимаем кластер с данными специально для скриншотов
# =============================================================================
log "Поднимаем кластер для Prometheus-скриншотов..."
fresh_cluster
verify_prometheus

log "Заполняем данные для визуализации..."
bench --scenario loadbalance --keys 30 --size 10

log "Запускаем storage-6 для демонстрации rebalance на графике..."
$COMPOSE --profile rebalance up -d storage-6
sleep 3
bench --scenario rebalance --keys 0 --size 1 \
  --new-node-id storage-6 --new-node-addr storage-6:8006

log "Ждём scrape Prometheus (10 сек)..."
sleep 10

# Открываем SSH-туннель
log "Открываем туннель на localhost:9090..."
COLIMA_SSH_CFG=$(mktemp)
colima ssh-config > "$COLIMA_SSH_CFG"
ssh -F "$COLIMA_SSH_CFG" -N -L 9090:localhost:9090 colima &
rm -f "$COLIMA_SSH_CFG"
TUNNEL_PID=$!
sleep 2

echo ""
echo "  ┌─────────────────────────────────────────────────────────────┐"
echo "  │  Prometheus: http://localhost:9090                          │"
echo "  │                                                             │"
echo "  │  Скриншот 1 → запрос: kv_node_used_bytes                   │"
echo "  │               вкладка Graph                                 │"
echo "  │               сохранить: prometheus_disk_usage.png          │"
echo "  │                                                             │"
echo "  │  Скриншот 2 → запрос: kv_rebalance_bytes_total             │"
echo "  │               вкладка Graph                                 │"
echo "  │               сохранить: prometheus_rebalance.png           │"
echo "  │                                                             │"
echo "  │  Оба файла положить в: experiments/results/                 │"
echo "  └─────────────────────────────────────────────────────────────┘"
echo ""
read -r -p "  Нажми Enter когда скриншоты сохранены..."

kill $TUNNEL_PID 2>/dev/null || true
log "Туннель закрыт ✓"
$COMPOSE down -v

# =============================================================================
step "6. Генерация графиков"
# =============================================================================
VENV="/tmp/thesis-venv"
[ ! -d "$VENV" ] && python3 -m venv "$VENV" && "$VENV/bin/pip" install matplotlib numpy -q
"$VENV/bin/python3" experiments/results/plot.py

# =============================================================================
step "Готово!"
# =============================================================================
echo ""
log "Все файлы для ВКР:"
ls -lh experiments/results/*.csv experiments/results/*.png 2>/dev/null | awk '{print "  "$NF, $5}'
echo ""
warn "Следующий шаг: обнови цифры в docs/thesis_draft.md из новых CSV"
