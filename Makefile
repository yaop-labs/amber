.PHONY: build test bench bench-mixed lint fmt tidy clean run docker docker-run hooks

BINARY := amber
BUILD_FLAGS := -ldflags="-s -w"

build:
	go build $(BUILD_FLAGS) -o $(BINARY) ./cmd/amber
	go build $(BUILD_FLAGS) -o amberctl ./cmd/amberctl

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
