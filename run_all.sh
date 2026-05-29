#!/usr/bin/env bash
# Полный прогон всех экспериментов по одному.
# Запускать после: colima start && docker-compose build
set -euo pipefail

COMPOSE="docker-compose -f deploy/docker-compose.yml"

up() {
  docker rm -f deploy-storage-6-1 2>/dev/null || true
  $COMPOSE down -v 2>/dev/null || true
  $COMPOSE up -d meta-1 meta-2 meta-3 storage-1 storage-2 storage-3 storage-4 storage-5 prometheus
  echo "Ждём кластер (20 сек)..."
  sleep 20
}

echo "=== Сборка образов ==="
$COMPOSE build --parallel

echo ""
echo "=== 1. Throughput ==="
up
./run_single.sh throughput

echo ""
echo "=== 2. Load Balance ==="
up
./run_single.sh loadbalance

echo ""
echo "=== 3. Availability (sequential) ==="
up
./run_single.sh availability

echo ""
echo "=== 4. Availability (2 одновременных) ==="
up
./run_single.sh availability-sim

echo ""
echo "=== 5. Rebalance ==="
up
./run_single.sh rebalance

echo ""
echo "=== 6. Rolling Failure ==="
up
./run_single.sh rolling

echo ""
echo "=== 7. Chaos Monkey ==="
up
./run_single.sh chaos

echo ""
echo "=== 9. Графики ==="
./run_single.sh plots

echo ""
echo "=== Готово! ==="
ls -lh experiments/results/*.csv experiments/results/*.png 2>/dev/null | awk '{print $NF, $5}'
