package query

import (
	"github.com/yaop-labs/amber/internal/metricsengine/block"
	"github.com/yaop-labs/amber/internal/metricsengine/index"
	"github.com/yaop-labs/amber/internal/metricsengine/model"
)

// resident.go wires a block.ResidentBlock (the compact resident form) into the
// range-step exact scan path. It resolves a selector into per-block dict-code
// predicates once, then the block scan matches series by integer code with no
// per-series model.Label allocation. Semantics mirror matchLabels /
// matchTimeRange / matchZoneMap exactly so results equal the Directory path.

// ResidentGroupFunc receives one matching series with its group value resolved.
type ResidentGroupFunc func(seriesID uint64, typ model.MetricType, groupValue string, timestamps, values []int64) error

// ScanResidentBlock scans rb with selector/opts, invoking fn for each matching
// series with the value of groupLabel already resolved from the block dict.
func ScanResidentBlock(path string, rb *block.ResidentBlock, selector index.Selector, opts Options, groupLabel string, fn ResidentGroupFunc) error {
	if err := selector.Validate(); err != nil {
		return err
	}
	preds, possible := buildResidentPredicates(rb, selector)
	if !possible {
		return nil
	}
	filter := block.ResidentFilter{
		StartMillis: opts.StartMillis,
		EndMillis:   opts.EndMillis,
		MinValue:    opts.MinValue,
		MaxValue:    opts.MaxValue,
	}
	groupIdx := rb.NameIndex(groupLabel)
	return rb.Scan(path, preds, filter, groupIdx, block.ResidentSeriesFunc(fn))
}

// buildResidentPredicates resolves a selector into per-block predicates. The
// second return is false when the block provably matches nothing (a positive
// matcher whose name or value is absent from the block), letting the caller skip
// the scan entirely.
func buildResidentPredicates(rb *block.ResidentBlock, selector index.Selector) ([]block.ResidentPredicate, bool) {
	if len(selector.Matchers) == 0 {
		return nil, true
	}
	dict := rb.ValueDict()
	preds := make([]block.ResidentPredicate, 0, len(selector.Matchers))
	for _, matcher := range selector.Matchers {
		if !supportedMatcher(matcher.Op) {
			return nil, false
		}
		// matchLabels treats a missing label as a match only for negative ops
		// (the "ok==false ⇒ continue" branch).
		acceptMissing := matcher.Op == index.MatchNotEqual || matcher.Op == index.MatchNotRegexp
		nameIdx := rb.NameIndex(matcher.Name)
		if nameIdx < 0 {
			if !acceptMissing {
				return nil, false
			}
			// Name absent from the block: every series lacks it, so a negative
			// matcher accepts all. Represent as an always-accept predicate.
			preds = append(preds, block.ResidentPredicate{NameIdx: -1, AcceptMissing: true})
			continue
		}
		allowed := make([]bool, len(dict))
		any := false
		for code, value := range dict {
			if matcher.Matches(value) {
				allowed[code] = true
				any = true
			}
		}
		if !any && !acceptMissing {
			// No present value satisfies a positive matcher: block matches nothing.
			return nil, false
		}
		preds = append(preds, block.ResidentPredicate{
			NameIdx:       nameIdx,
			Allowed:       allowed,
			AcceptMissing: acceptMissing,
		})
	}
	return preds, true
}
