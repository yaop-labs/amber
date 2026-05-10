package bootstrap

import (
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/hnlbs/amber/internal/index"
	"github.com/hnlbs/amber/internal/metrics"
	"github.com/hnlbs/amber/internal/query"
	"github.com/hnlbs/amber/internal/storage"
)

// retryBuild runs fn up to 3 times with exponential backoff (100ms, 500ms).
// The seal callback is fire-and-forget — there's no upstream to bubble errors
// to — so on final failure we bump the metric and let the on-demand index
// build paths in Executor cover the gap.
func retryBuild(name string, log *slog.Logger, fn func() error) error {
	delays := []time.Duration{100 * time.Millisecond, 500 * time.Millisecond}
	var err error
	for attempt := 0; attempt < 3; attempt++ {
		if err = fn(); err == nil {
			if attempt > 0 {
				log.Info("seal: build recovered", "step", name, "attempt", attempt+1)
			}
			return nil
		}
		if attempt < len(delays) {
			log.Warn("seal: build failed, retrying", "step", name, "attempt", attempt+1, "err", err)
			time.Sleep(delays[attempt])
		}
	}
	return err
}

func LoadSealedIndexes(
	exec *query.Executor,
	logManager, spanManager *storage.SegmentManager,
	logDir, spanDir string,
	log *slog.Logger,
) {
	workers := runtime.NumCPU()
	if workers < 2 {
		workers = 2
	}

	loadLogSegments(exec, logManager, logDir, workers, log)
	loadSpanSegments(exec, spanManager, spanDir, workers, log)
}

func loadLogSegments(
	exec *query.Executor,
	logManager *storage.SegmentManager,
	logDir string,
	workers int,
	log *slog.Logger,
) {
	segs := logManager.Segments()
	if len(segs) == 0 {
		return
	}

	jobs := make(chan storage.SegmentMeta, len(segs))
	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Go(func() {
			for seg := range jobs {
				segPath := filepath.Join(logDir, seg.FileName)

				bidxPath := filepath.Join(logDir, seg.FileName+".bidx")
				if _, err := os.Stat(bidxPath); err != nil {
					if _, err := index.BuildLogBitmapIndex(segPath, log); err != nil {
						log.Warn("failed to build log bitmap on startup", "segment", seg.FileName, "err", err)
					}
				}

				fidxPath := filepath.Join(logDir, seg.FileName+".fidx")
				if _, err := os.Stat(fidxPath); err != nil {
					if _, err := index.BuildLogFTSIndex(segPath, log); err != nil {
						log.Warn("failed to build log fts on startup", "segment", seg.FileName, "err", err)
					}
				}

				if ribbon, err := index.LoadRibbonFilter(filepath.Join(logDir, seg.FileName+".filt")); err == nil {
					exec.RegisterLogRibbon(seg.FileName, ribbon)
				} else if ribbon, err := index.BuildLogRibbonFilter(segPath, log); err == nil {
					exec.RegisterLogRibbon(seg.FileName, ribbon)
				} else {
					log.Warn("failed to build log ribbon on startup", "segment", seg.FileName, "err", err)
				}

				if ribbon, err := index.LoadRibbonFilter(filepath.Join(logDir, seg.FileName+".fts.filt")); err == nil {
					exec.RegisterLogFTSRibbon(seg.FileName, ribbon)
				} else if ribbon, err := index.BuildLogFTSRibbon(segPath, log); err == nil {
					exec.RegisterLogFTSRibbon(seg.FileName, ribbon)
				} else {
					log.Warn("failed to build log fts ribbon on startup", "segment", seg.FileName, "err", err)
				}
			}
		})
	}

	for _, seg := range segs {
		jobs <- seg
	}
	close(jobs)
	wg.Wait()
}

func loadSpanSegments(
	exec *query.Executor,
	spanManager *storage.SegmentManager,
	spanDir string,
	workers int,
	log *slog.Logger,
) {
	segs := spanManager.Segments()
	if len(segs) == 0 {
		return
	}

	jobs := make(chan storage.SegmentMeta, len(segs))
	var wg sync.WaitGroup

	for range workers {
		wg.Go(func() {
			for seg := range jobs {
				segPath := filepath.Join(spanDir, seg.FileName)

				bidxPath := filepath.Join(spanDir, seg.FileName+".bidx")
				if _, err := os.Stat(bidxPath); err != nil {
					if _, err := index.BuildSpanBitmapIndex(segPath, log); err != nil {
						log.Warn("failed to build span bitmap on startup", "segment", seg.FileName, "err", err)
					}
				}

				if ribbon, err := index.LoadRibbonFilter(filepath.Join(spanDir, seg.FileName+".filt")); err == nil {
					exec.RegisterSpanRibbon(seg.FileName, ribbon)
				} else if ribbon, err := index.BuildSpanRibbonFilter(segPath, log); err == nil {
					exec.RegisterSpanRibbon(seg.FileName, ribbon)
				} else {
					log.Warn("failed to build span ribbon on startup", "segment", seg.FileName, "err", err)
				}
			}
		})
	}

	for _, seg := range segs {
		jobs <- seg
	}
	close(jobs)
	wg.Wait()
}

func SetupSealCallbacks(
	exec *query.Executor,
	logManager, spanManager *storage.SegmentManager,
	logDir, spanDir string,
	log *slog.Logger,
) {
	logManager.SetOnSeal(func(meta storage.SegmentMeta) {
		segPath := filepath.Join(logDir, meta.FileName)

		if err := retryBuild("log bitmap", log, func() error {
			_, err := index.BuildLogBitmapIndex(segPath, log)
			return err
		}); err != nil {
			metrics.SealIndexErrors.WithLabelValues("log", "bitmap").Inc()
			log.Error("seal: build log bitmap gave up", "segment", meta.FileName, "err", err)
		}

		if err := retryBuild("log fts", log, func() error {
			_, err := index.BuildLogFTSIndex(segPath, log)
			return err
		}); err != nil {
			metrics.SealIndexErrors.WithLabelValues("log", "fts").Inc()
			log.Error("seal: build log fts gave up", "segment", meta.FileName, "err", err)
		}

		var logRibbon *index.RibbonFilter
		if err := retryBuild("log ribbon", log, func() error {
			r, err := index.BuildLogRibbonFilter(segPath, log)
			logRibbon = r
			return err
		}); err != nil {
			metrics.SealIndexErrors.WithLabelValues("log", "ribbon").Inc()
			log.Error("seal: build log ribbon gave up", "segment", meta.FileName, "err", err)
		} else {
			exec.RegisterLogRibbon(meta.FileName, logRibbon)
		}

		var ftsRibbon *index.RibbonFilter
		if err := retryBuild("log fts ribbon", log, func() error {
			r, err := index.BuildLogFTSRibbon(segPath, log)
			ftsRibbon = r
			return err
		}); err != nil {
			metrics.SealIndexErrors.WithLabelValues("log", "fts_ribbon").Inc()
			log.Error("seal: build log fts ribbon gave up", "segment", meta.FileName, "err", err)
		} else {
			exec.RegisterLogFTSRibbon(meta.FileName, ftsRibbon)
		}
	})

	spanManager.SetOnSeal(func(meta storage.SegmentMeta) {
		segPath := filepath.Join(spanDir, meta.FileName)

		if err := retryBuild("span bitmap", log, func() error {
			_, err := index.BuildSpanBitmapIndex(segPath, log)
			return err
		}); err != nil {
			metrics.SealIndexErrors.WithLabelValues("span", "bitmap").Inc()
			log.Error("seal: build span bitmap gave up", "segment", meta.FileName, "err", err)
		}

		var spanRibbon *index.RibbonFilter
		if err := retryBuild("span ribbon", log, func() error {
			r, err := index.BuildSpanRibbonFilter(segPath, log)
			spanRibbon = r
			return err
		}); err != nil {
			metrics.SealIndexErrors.WithLabelValues("span", "ribbon").Inc()
			log.Error("seal: build span ribbon gave up", "segment", meta.FileName, "err", err)
		} else {
			exec.RegisterSpanRibbon(meta.FileName, spanRibbon)
		}
	})
}
