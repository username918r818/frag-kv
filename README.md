# fragkv — устойчивое к фрагментации KV-хранилище высокой доступности

Прототип распределённого KV-хранилища для больших бинарных значений (100 МБ+).
Разработан как часть ВКР Закусова К.Я., ИТМО, группа P3408.

## Архитектура

```
kvctl (CLI) ──── metad-кластер (3 узла, Raft) ──── storaged × N
                  каталог ключей + placement            файлы + bbolt-индекс
```

| Компонент | Описание |
|---|---|
| `metad` | Metadata-сервис: каталог `key→fragments`, membership, Raft-репликация |
| `storaged` | Storage-узел: хранит фрагменты на диске, HTTP API, heartbeat |
| `kvctl` | CLI: `put/get/delete/list/status` |

## Быстрый старт (Docker)

```bash
cd deploy
docker-compose up --build -d

# подождать ~10 сек пока кластер поднимется
docker-compose ps

# загрузить файл
go run ./cmd/kvctl --meta http://localhost:9000 put mykey ./largefile.bin

# скачать файл
go run ./cmd/kvctl --meta http://localhost:9000 get mykey ./out.bin

# сравнить
sha256sum largefile.bin out.bin

# статус кластера
go run ./cmd/kvctl --meta http://localhost:9000 status
```

## Локальный запуск (без Docker)

```bash
# 1. Запустить meta-1 (single-node bootstrap)
mkdir -p /tmp/meta1
go run ./cmd/metad /dev/stdin <<EOF
node_id: meta-1
addr: ":9000"
raft_addr: ":9001"
data_dir: /tmp/meta1
bootstrap: true
replication_factor: 3
chunk_size_bytes: 8388608
placement_strategy: weighted
dead_threshold: 15s
EOF

# 2. Запустить storage-узлы (в отдельных терминалах)
for i in 1 2 3; do
  mkdir -p /tmp/storage$i
  go run ./cmd/storaged /dev/stdin <<EOF
node_id: storage-$i
addr: ":800$i"
public_addr: "localhost:800$i"
data_dir: /tmp/storage$i
meta_addr: http://localhost:9000
total_bytes: 1073741824
heartbeat_interval: 5s
EOF
done

# 3. Работа с данными
go run ./cmd/kvctl put testkey ./myfile.bin
go run ./cmd/kvctl get testkey ./out.bin
go run ./cmd/kvctl status
```

## Запуск тестов

```bash
go test ./...                           # все тесты
go test ./internal/transport/... -v    # интеграционные тесты storaged
go test ./internal/metadata/... -v     # FSM тесты
go test ./internal/placement/... -v    # placement тесты
```

## Эксперименты

```bash
# Запустить кластер через docker-compose, затем:
go run ./experiments/harness/main \
  --meta http://localhost:9000 \
  --out experiments/results \
  --scenario throughput \
  --keys 10

# Построить графики
python3 experiments/results/plot.py
```

## Параметры конфигурации

### metad.yaml
| Параметр | Дефолт | Описание |
|---|---|---|
| `replication_factor` | 3 | Число реплик каждого фрагмента |
| `chunk_size_bytes` | 8388608 | Размер фрагмента (8 МБ) |
| `placement_strategy` | `weighted` | `consistent_hash` \| `rendezvous` \| `weighted` |
| `dead_threshold` | 15s | Таймаут до пометки узла как DOWN |

### storaged.yaml
| Параметр | Дефолт | Описание |
|---|---|---|
| `public_addr` | `localhost:8001` | Адрес для доступа других узлов/клиентов |
| `total_bytes` | 10 ГБ | Объём диска (для weighted placement) |
| `heartbeat_interval` | 5s | Частота heartbeat в metad |

## Структура проекта

```
cmd/
  metad/       — metadata-сервис (main)
  storaged/    — storage-узел (main)
  kvctl/       — CLI-клиент (main)
internal/
  fragment/    — Split/Assemble
  placement/   — ConsistentHash, Rendezvous, Weighted
  storage/     — локальный движок (файлы + bbolt)
  metadata/    — Raft FSM, каталог, воркеры
  transport/   — HTTP-сервер storaged
  metrics/     — Prometheus-метрики
deploy/
  docker-compose.yml
  configs/     — конфиги узлов
  prometheus/  — конфиг Prometheus
experiments/
  harness/     — Go-код экспериментов
  results/     — CSV + plot.py
docs/
  requirements.md
  architecture.md
  analogs.md
  thesis_draft.md
```

## Метрики (Prometheus)

| Метрика | Тип | Описание |
|---|---|---|
| `kv_put_duration_seconds` | Histogram | Latency PUT |
| `kv_get_duration_seconds` | Histogram | Latency GET |
| `kv_fragment_put_duration_seconds` | Histogram | Latency одного фрагмента |
| `kv_node_used_bytes` | Gauge | Занятое место на узле |
| `kv_rebalance_bytes_total` | Counter | Байт перемещено при rebalance |
| `kv_recovery_duration_seconds` | Histogram | Время восстановления реплики |
| `kv_under_replicated_fragments` | Gauge | Число under-replicated фрагментов |
| `kv_live_nodes_total` | Gauge | Число живых узлов |
