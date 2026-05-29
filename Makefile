.PHONY: build test clean up down logs bench plot

build:
	go build ./...

test:
	go test ./... -timeout 60s

test-race:
	go test -race ./... -timeout 60s

# ---- Docker cluster --------------------------------------------------------

up:
	cd deploy && docker-compose up --build -d
	@echo "Cluster starting... run 'make logs' to watch"

down:
	cd deploy && docker-compose down

logs:
	cd deploy && docker-compose logs -f --tail=50

status:
	go run ./cmd/kvctl --meta http://localhost:9000 status

# Kill node k (usage: make kill-node K=3)
kill-node:
	cd deploy && docker-compose stop storage-$(K)

# Add a 6th node (triggers rebalance)
add-node:
	cd deploy && docker-compose up --build -d storage-6

# ---- Experiments -----------------------------------------------------------

bench:
	go run ./experiments/harness/main \
		--meta http://localhost:9000 \
		--out experiments/results \
		--scenario all \
		--keys 10

bench-throughput:
	go run ./experiments/harness/main --scenario throughput --keys 5

bench-loadbalance:
	go run ./experiments/harness/main --scenario loadbalance --keys 20 --size 50

plot:
	python3 experiments/results/plot.py

# ---- Sanity check ----------------------------------------------------------

sanity:
	@echo "=== PUT 50MB test file ==="
	dd if=/dev/urandom of=/tmp/testfile.bin bs=1M count=50 2>/dev/null
	go run ./cmd/kvctl --meta http://localhost:9000 put sanity-test /tmp/testfile.bin
	go run ./cmd/kvctl --meta http://localhost:9000 get sanity-test /tmp/testfile-out.bin
	sha256sum /tmp/testfile.bin /tmp/testfile-out.bin
	@echo "=== SHA256 should match ==="

clean:
	rm -f /tmp/testfile.bin /tmp/testfile-out.bin
	rm -f experiments/results/*.csv experiments/results/*.png
