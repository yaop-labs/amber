package obsbench

import (
	"fmt"
	"math/rand/v2"
)

// GenConfig parameterizes dataset generation. Everything derives from Seed so
// two runs with the same config produce byte-identical datasets — the
// reproducibility contract of METHODOLOGY.md §5.
type GenConfig struct {
	Count uint64
	Seed  uint64

	// RareTokenEvery injects the rare-token marker into every Nth record for
	// the rare-token FTS scenario (Q4). 0 disables.
	RareTokenEvery uint64
}

// RareToken returns the marker token for a seed. Deterministic per seed so a
// query run knows what to search for without carrying state around.
func RareToken(seed uint64) string {
	return fmt.Sprintf("RARETOKEN%08x", seed)
}

var genServices = []string{
	"api-gateway", "checkout", "payments", "inventory", "shipping",
	"auth", "search", "notifications", "billing", "catalog",
}

var genHosts = []string{
	"node-01", "node-02", "node-03", "node-04", "node-05", "node-06",
}

// genLevels is weighted the way production logs are: INFO dominates, ERROR
// is rare, FATAL is exceptional. Point queries (Q1) filter on the rare end,
// so the distribution matters for index selectivity.
var genLevels = []struct {
	name   string
	weight int
}{
	{"DEBUG", 20},
	{"INFO", 60},
	{"WARN", 10},
	{"ERROR", 8},
	{"TRACE", 1},
	{"FATAL", 1},
}

var genBodies = []string{
	"request completed method=GET path=/api/v1/orders status=200 duration_ms=%d req_id=%s",
	"request completed method=POST path=/api/v1/checkout status=201 duration_ms=%d req_id=%s",
	"cache miss key=user:%d fetching from origin req_id=%s",
	"connection pool acquired conn_id=%d wait_ms=0 req_id=%s",
	"payment authorized amount_cents=%d provider=stripe req_id=%s",
	"slow query detected duration_ms=%d table=orders req_id=%s",
	"retrying upstream call attempt=%d backoff_ms=200 req_id=%s",
	"connection refused upstream=inventory-svc retries=%d req_id=%s",
	"timeout waiting for lock holder_ms=%d resource=stock req_id=%s",
	"deserialization failed offset=%d topic=events req_id=%s",
}

// Generator produces a deterministic record stream.
type Generator struct {
	cfg GenConfig
	rng *rand.Rand
	seq uint64
}

func NewGenerator(cfg GenConfig) *Generator {
	return &Generator{
		cfg: cfg,
		rng: rand.New(rand.NewPCG(cfg.Seed, cfg.Seed^0x9e3779b97f4a7c15)),
	}
}

func (g *Generator) pickLevel() string {
	total := 0
	for _, l := range genLevels {
		total += l.weight
	}
	n := g.rng.IntN(total)
	for _, l := range genLevels {
		if n < l.weight {
			return l.name
		}
		n -= l.weight
	}
	return "INFO"
}

// uuidLike renders a deterministic UUID-shaped token. Real UUIDs in bodies
// are what broke ClickHouse's hasToken in the v0 round; keeping the shape
// preserves that property of the workload.
func (g *Generator) uuidLike() string {
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		g.rng.Uint32(), g.rng.Uint32()&0xffff, g.rng.Uint32()&0xffff,
		g.rng.Uint32()&0xffff, g.rng.Uint64()&0xffffffffffff)
}

// Next returns the next record. The caller stops at cfg.Count.
func (g *Generator) Next() Record {
	g.seq++
	tmpl := genBodies[g.rng.IntN(len(genBodies))]
	body := fmt.Sprintf(tmpl, g.rng.IntN(10_000), g.uuidLike())
	if g.cfg.RareTokenEvery > 0 && g.seq%g.cfg.RareTokenEvery == 0 {
		body += " marker=" + RareToken(g.cfg.Seed)
	}
	return Record{
		Seq:     g.seq,
		Service: genServices[g.rng.IntN(len(genServices))],
		Level:   g.pickLevel(),
		Host:    genHosts[g.rng.IntN(len(genHosts))],
		Body:    body,
	}
}

// Generate writes cfg.Count records to path, reporting progress to onProgress
// (every ~1M records; nil to disable).
func Generate(path string, cfg GenConfig, onProgress func(done uint64)) error {
	w, err := NewDatasetWriter(path)
	if err != nil {
		return err
	}
	g := NewGenerator(cfg)
	for i := uint64(0); i < cfg.Count; i++ {
		if err := w.Write(g.Next()); err != nil {
			_ = w.Close()
			return err
		}
		if onProgress != nil && (i+1)%1_000_000 == 0 {
			onProgress(i + 1)
		}
	}
	return w.Close()
}
