.PHONY: build test bench bench-mixed lint fmt tidy clean run docker docker-run hooks release release-check release-smoke

BINARY := amber
BUILD_FLAGS := -ldflags="-s -w"
VERSION ?= 0.4.0-dev
DIST_DIR ?= dist/$(VERSION)

build:
	go build $(BUILD_FLAGS) -o $(BINARY) ./cmd/amber
	go build $(BUILD_FLAGS) -o amberctl ./cmd/amberctl

release:
	 rm -rf "$(DIST_DIR)"
	 mkdir -p "$(DIST_DIR)"
	 go build $(BUILD_FLAGS) -o "$(DIST_DIR)/amber" ./cmd/amber
	 go build $(BUILD_FLAGS) -o "$(DIST_DIR)/amberctl" ./cmd/amberctl
	 go build $(BUILD_FLAGS) -o "$(DIST_DIR)/amber-backup" ./cmd/amber-backup
	 go build $(BUILD_FLAGS) -o "$(DIST_DIR)/amber-migrate" ./cmd/amber-migrate
	 (cd "$(DIST_DIR)" && sha256sum amber amberctl amber-backup amber-migrate > SHA256SUMS)

release-check:
	 go test ./cmd/amber ./cmd/amberctl ./cmd/amber-backup ./cmd/amber-migrate ./internal/config ./internal/api/http ./internal/backup ./internal/gyreadapter -count=1
	 go vet ./cmd/amber ./cmd/amberctl ./cmd/amber-backup ./cmd/amber-migrate ./internal/config ./internal/api/http ./internal/backup ./internal/gyreadapter

release-smoke: release
	 (cd "$(DIST_DIR)" && sha256sum -c SHA256SUMS)
	 "$(DIST_DIR)/amberctl" help >/dev/null
	 ! "$(DIST_DIR)/amber-backup" >/dev/null 2>&1
	 ! "$(DIST_DIR)/amber-migrate" >/dev/null 2>&1

run: build
	./$(BINARY) config.example.yaml

test:
	go test ./... -race -count=1

bench:
	go test ./benchmarks/ -bench=. -benchtime=2s -run=^$$ -timeout=30m

# Mixed-query RSS measurement over the campaign fixture: defaults vs a
# production-like 800MiB memory limit with the derived cache budget.
# Tune with AMBER_MIXED_SECS / AMBER_BENCH_SERIES / AMBER_BENCH_TICKS.
bench-mixed:
	AMBER_MIXED=1 go test ./internal/metricsengine/store/ -run TestMixedWorkloadRSS -v -count=1 -timeout=30m

lint:
	go vet ./...
	@which golangci-lint >/dev/null 2>&1 && golangci-lint run ./... || echo "golangci-lint not installed, skipping"

fmt:
	gofmt -w .
	@which goimports >/dev/null 2>&1 && goimports -w -local github.com/hnlbs/amber . || true

tidy:
	go mod tidy

clean:
	rm -f $(BINARY) amberctl
	go clean -testcache

docker:
	docker build -t amber:latest .

docker-run:
	docker run -p 8080:8080 -p 4317:4317 -v amber-data:/data amber:latest

hooks:
	@which lefthook >/dev/null 2>&1 || go install github.com/evilmartians/lefthook@latest
	lefthook install
