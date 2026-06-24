package obsbench

import (
	"encoding/json"
	"fmt"
	"os"
)

// ResultFile is one target's query-phase output.
type ResultFile struct {
	Target   string         `json:"target"`
	Outcomes []QueryOutcome `json:"outcomes"`
}

func WriteResultFile(path string, rf ResultFile) error {
	data, err := json.MarshalIndent(rf, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

func ReadResultFile(path string) (ResultFile, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return ResultFile{}, err
	}
	var rf ResultFile
	if err := json.Unmarshal(data, &rf); err != nil {
		return ResultFile{}, fmt.Errorf("results %s: %w", path, err)
	}
	return rf, nil
}

// Mismatch is one equality-gate violation: the same query instance returned
// different counts on different systems, so the latency comparison for that
// instance is void (METHODOLOGY.md).
type Mismatch struct {
	Scenario  string
	Iteration int
	Params    string
	Counts    map[string]int // target -> count
	Errors    map[string]string
}

// CompareCounts applies the equality gate across two or more result files.
// Instances are matched by (scenario, iteration, params) - identical seeds
// produce identical params, so a params mismatch is itself a harness bug and
// reported as such.
func CompareCounts(files []ResultFile) []Mismatch {
	type key struct {
		scenario  string
		iteration int
	}
	byKey := make(map[key]map[string]QueryOutcome)
	for _, rf := range files {
		for _, oc := range rf.Outcomes {
			k := key{oc.Scenario, oc.Iteration}
			if byKey[k] == nil {
				byKey[k] = make(map[string]QueryOutcome)
			}
			byKey[k][rf.Target] = oc
		}
	}

	out := make([]Mismatch, 0, len(byKey))
	for k, perTarget := range byKey {
		if len(perTarget) < 2 {
			continue // nothing to compare
		}
		agree := true
		var refCount = -1
		var refParams string
		for _, oc := range perTarget {
			if oc.Error != "" {
				agree = false
				break
			}
			if refCount == -1 {
				refCount, refParams = oc.Count, oc.Params
				continue
			}
			if oc.Count != refCount || oc.Params != refParams {
				agree = false
				break
			}
		}
		if agree {
			continue
		}
		m := Mismatch{
			Scenario:  k.scenario,
			Iteration: k.iteration,
			Counts:    make(map[string]int),
			Errors:    make(map[string]string),
		}
		for target, oc := range perTarget {
			m.Params = oc.Params
			m.Counts[target] = oc.Count
			if oc.Error != "" {
				m.Errors[target] = oc.Error
			}
		}
		out = append(out, m)
	}
	return out
}
