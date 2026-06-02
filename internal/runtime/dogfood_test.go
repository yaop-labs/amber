package runtime

import (
	"io"
	"log/slog"
	"testing"
	"time"

	mestore "github.com/yaop-labs/amber/internal/metricsengine/store"
	"github.com/yaop-labs/amber/internal/selfobs"
)

// TestDogfoodScraper_AppendsSelfObs verifies the end-to-end loop: register a
// counter into selfobs, run the scraper, and check the metric store ends up
// with a series whose __name__ matches the counter.
func TestDogfoodScraper_AppendsSelfObs(t *testing.T) {
	cv := selfobs.NewCounterVec("dogfood_test_total", "test", "kind")
	selfobs.RegisterCounterVec(cv)
	cv.WithLabelValues("hit").Add(5)

	store, err := mestore.Open(t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	stop := make(chan struct{})
	done := make(chan struct{})
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	go runDogfoodScraper(10*time.Millisecond, store, log, stop, done)

	// Wait for at least one tick to land.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		names := store.MetricNames()
		for _, n := range names {
			if n == "dogfood_test_total" {
				close(stop)
				<-done
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	close(stop)
	<-done
	t.Fatalf("scraper did not append dogfood_test_total within timeout; saw: %v", store.MetricNames())
}
