package block

import "testing"

func TestBuildResidentFromDirectoryFields(t *testing.T) {
	dir := sampleDirectory()
	rb := BuildResidentFromDirectory(dir)

	if rb.Len() != len(dir.Series) {
		t.Fatalf("Len = %d, want %d", rb.Len(), len(dir.Series))
	}
	for i, e := range dir.Series {
		if rb.seriesID[i] != e.SeriesID {
			t.Errorf("series %d id = %d, want %d", i, rb.seriesID[i], e.SeriesID)
		}
		if rb.typ[i] != e.Type {
			t.Errorf("series %d type = %v, want %v", i, rb.typ[i], e.Type)
		}
		if rb.timeMin[i] != e.TimeMin || rb.timeMax[i] != e.TimeMax {
			t.Errorf("series %d time = [%d,%d], want [%d,%d]", i, rb.timeMin[i], rb.timeMax[i], e.TimeMin, e.TimeMax)
		}
		if rb.zoneMin[i] != e.ZoneMap.Min || rb.zoneMax[i] != e.ZoneMap.Max {
			t.Errorf("series %d zone = [%d,%d], want [%d,%d]", i, rb.zoneMin[i], rb.zoneMax[i], e.ZoneMap.Min, e.ZoneMap.Max)
		}
		if rb.tsOff[i] != e.TimestampOff || rb.tsLen[i] != e.TimestampLen || int(rb.tsN[i]) != e.TimestampN {
			t.Errorf("series %d ts chunk mismatch", i)
		}
		if rb.valOff[i] != e.ValueOff || rb.valLen[i] != e.ValueLen || int(rb.valN[i]) != e.ValueN {
			t.Errorf("series %d val chunk mismatch", i)
		}
	}

	// dataEnd is the max chunk end across all entries (== directory offset).
	var wantEnd int64
	for _, e := range dir.Series {
		if end := e.ValueOff + e.ValueLen; end > wantEnd {
			wantEnd = end
		}
		if end := e.TimestampOff + e.TimestampLen; end > wantEnd {
			wantEnd = end
		}
	}
	if rb.dataEnd != wantEnd {
		t.Errorf("dataEnd = %d, want %d", rb.dataEnd, wantEnd)
	}
}

func TestResidentGroupValueMatchesDirectory(t *testing.T) {
	dir := sampleDirectory()
	rb := BuildResidentFromDirectory(dir)
	svc := rb.NameIndex("service")
	if svc < 0 {
		t.Fatal("service label not indexed")
	}
	for i, e := range dir.Series {
		want, _ := e.Labels.Get("service")
		if got := rb.groupValue(svc, i); got != want {
			t.Errorf("series %d group value = %q, want %q", i, got, want)
		}
	}
	// A label name absent from the block resolves to "" with a -1 column.
	if rb.NameIndex("nonexistent") != -1 {
		t.Error("absent label should report column -1")
	}
	if got := rb.groupValue(-1, 0); got != "" {
		t.Errorf("absent-column group value = %q, want \"\"", got)
	}
}

// equalPredicate builds an Equal predicate for one (name,value) directly against
// the block dict, mirroring what the query package produces.
func equalPredicate(rb *ResidentBlock, name, value string) ResidentPredicate {
	allowed := make([]bool, len(rb.ValueDict()))
	if code, ok := rb.ValueCode(value); ok {
		allowed[code] = true
	}
	return ResidentPredicate{NameIdx: rb.NameIndex(name), Allowed: allowed, AcceptMissing: false}
}

func TestResidentMatchSeriesEqualAndNegative(t *testing.T) {
	dir := sampleDirectory()
	rb := BuildResidentFromDirectory(dir)

	// Equal service=api matches only series 0 (id 1).
	eq := []ResidentPredicate{equalPredicate(rb, "service", "api")}
	if !rb.matchSeries(0, eq) {
		t.Error("series 0 should match service=api")
	}
	if rb.matchSeries(1, eq) {
		t.Error("series 1 should not match service=api")
	}

	// NotEqual service=api matches only series 1, and accepts series missing the
	// label (acceptMissing).
	dictLen := len(rb.ValueDict())
	allowed := make([]bool, dictLen)
	for i := range allowed {
		allowed[i] = true
	}
	if code, ok := rb.ValueCode("api"); ok {
		allowed[code] = false
	}
	ne := []ResidentPredicate{{NameIdx: rb.NameIndex("service"), Allowed: allowed, AcceptMissing: true}}
	if rb.matchSeries(0, ne) {
		t.Error("series 0 should not match service!=api")
	}
	if !rb.matchSeries(1, ne) {
		t.Error("series 1 should match service!=api")
	}

	// A negative matcher on an absent name (-1 column) accepts every series.
	absent := []ResidentPredicate{{NameIdx: -1, AcceptMissing: true}}
	for i := 0; i < rb.Len(); i++ {
		if !rb.matchSeries(i, absent) {
			t.Errorf("series %d should match an absent-name negative predicate", i)
		}
	}
}

func TestResidentRetainedBytesPositive(t *testing.T) {
	rb := BuildResidentFromDirectory(sampleDirectory())
	if rb.ResidentRetainedBytes() <= 0 {
		t.Fatal("retained bytes must be positive")
	}
}
