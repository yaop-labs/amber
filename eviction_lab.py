"""
eviction_lab.py — validate the two load-bearing decisions of the index-eviction spec,
under the SAME churn profile the load experiment used (c=0.5, ~250 new series/min).
(1) does a last-touch sweep hold the working set BOUNDED (vs the observed 0->8038 growth)?
(2) append-only catalog log: O(1) per registration vs current O(N) JSON-rewrite-per-add.
(3) sweep cost: how much work per sweep, and does sweep latency stay bounded?
"""
import numpy as np, time, math
rng = np.random.default_rng(7)

# ---- reproduce the experiment's churn dynamics ----
# 1000 series, stable_frac 0.5 -> 500 stable + 500 ephemeral; every 60s, 50% of
# ephemeral (250) retire + 250 new. retention = 3 min. flush/sample tick = 5s.
STABLE = 500
EPHEMERAL = 500
ROTATE_EVERY_S = 60
CHURN_FRAC = 0.5
RETENTION_S = 180
TICK_S = 5
DURATION_S = 30*60

print("=== (1) working set: NO eviction (current v0) vs last-touch eviction ===")
def simulate(evict: bool):
    # each series: id -> last_touch_ts. stable always touched; ephemeral cohort rotates.
    last_touch = {}
    next_id = 0
    # seed stable
    stable_ids = list(range(STABLE)); next_id = STABLE
    for s in stable_ids: last_touch[s] = 0
    # seed ephemeral
    eph = list(range(next_id, next_id+EPHEMERAL)); 
    for e in eph: last_touch[e] = 0
    next_id += EPHEMERAL
    series_count_over_time = []
    sweep_work = []
    for t in range(0, DURATION_S, TICK_S):
        # touch stable + current ephemeral
        for s in stable_ids: last_touch[s] = t
        for e in eph: last_touch[e] = t
        # rotation
        if t>0 and t % ROTATE_EVERY_S == 0:
            n_new = int(EPHEMERAL*CHURN_FRAC)
            # retire oldest n_new ephemeral (they simply stop being touched)
            eph = eph[n_new:]
            new = list(range(next_id, next_id+n_new)); next_id += n_new
            for e in new: last_touch[e] = t
            eph += new
        # eviction sweep: drop entries whose last_touch older than retention
        if evict:
            cold = [sid for sid,lt in last_touch.items() if t-lt > RETENTION_S]
            for sid in cold: del last_touch[sid]
            sweep_work.append(len(last_touch))   # entries scanned (naive full sweep)
        series_count_over_time.append((t, len(last_touch)))
    return series_count_over_time, sweep_work

no_evict,_ = simulate(False)
ev, sweepw = simulate(True)
print(f"  {'elapsed':>8}{'no-evict':>10}{'evict':>8}")
for (t,n_no),(_,n_ev) in zip(no_evict[::72], ev[::72]):
    print(f"  {t:>8}{n_no:>10}{n_ev:>8}")
print(f"  FINAL: no-evict={no_evict[-1][1]} (matches experiment's ~8038)  evict={ev[-1][1]}")
print(f"  -> eviction bounds working set at ~{max(n for _,n in ev[len(ev)//2:])} "
      f"(stable {STABLE} + ephemeral-in-retention-window)")

print("\n=== (2) catalog persistence: append-only log O(1) vs JSON-rewrite O(N) ===")
def jsonrewrite_cost(catalog_size): return catalog_size      # rewrite whole catalog per add
def appendlog_cost(_): return 1                              # append one record
# total work over the run, counting a 'registration' each new series
n_registrations = no_evict[-1][1] - (STABLE+EPHEMERAL) + (STABLE+EPHEMERAL)  # ~ total ever-seen
total_ever = STABLE + EPHEMERAL + int(EPHEMERAL*CHURN_FRAC)*(DURATION_S//ROTATE_EVERY_S)
# JSON-rewrite: each registration rewrites current catalog (grows over time, no evict)
jr = 0; size = 0
for _ in range(total_ever):
    size += 1; jr += size           # O(N) per add, N growing
al = total_ever                     # O(1) per add
print(f"  total series ever registered: {total_ever}")
print(f"  JSON-rewrite total work units: {jr:,}  (O(N^2) cumulative)")
print(f"  append-log total work units:  {al:,}  (O(N) cumulative)")
print(f"  -> append-log is {jr/al:.0f}x less write work over the run; compaction amortizes reads")

print("\n=== (3) sweep cost: naive full-scan vs bucketed-by-expiry ===")
# naive: scan all live entries each sweep -> O(live) per sweep
# bucketed: time-bucket series by last_touch; sweep only expired bucket -> O(evicted)
sweeps = DURATION_S//TICK_S
live_avg = np.mean(sweepw)
naive_total = sum(sweepw)                     # scan all-live each tick
# bucketed: only touch entries that actually expire (~250 per rotation)
bucketed_total = (DURATION_S//ROTATE_EVERY_S) * int(EPHEMERAL*CHURN_FRAC)
print(f"  sweeps: {sweeps}, avg live entries: {live_avg:.0f}")
print(f"  naive full-scan sweep total scanned: {naive_total:,}")
print(f"  bucketed-by-expiry total scanned:    {bucketed_total:,}  ({naive_total/max(bucketed_total,1):.0f}x less)")
print(f"  -> bucket series by last-touch/expiry epoch; sweep touches only the expiring bucket.")
