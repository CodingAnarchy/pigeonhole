

<a id="d169"></a>
## D169 — The tiered picker needs no extra write-amplification bound; the proptest guards it (approved; compaction, #228, #245; supersedes D165's write-amp follow-up)
#228 was filed from a review of #227. Its concern: since L1 is always taken when it holds a run, every L0 merge would then rewrite a growing L1, so write amplification would grow about quadratically. D165 makes a fix required before the next release.

**Finding:** the picker never reaches that state while a free level exists. An L0 merge's output goes just above the first run it leaves out, which is the *deepest* free level. Runs therefore fill the levels from the bottom up, and L1 holds a run only once every level 1..last does. From then on some merge of existing runs is unavoidable, and the size-ratio cascade (L0 + L1, then L2 once L1 has grown to its size, and so on) behaves like a size-tiered scheme with `max_levels - 1` runs below L0. Measured with insert-only data (no GC shrinkage), 1 MiB flushes, `l0_trigger` 4 and the default 1% ratio:

| max_levels | flushes | current picker | format-free fix (shift runs down, else merge the most similar adjacent pair) |
|---|---|---|---|
| 7 | 1000 | 4.1× | 5.2× |
| 7 | 4000 | 5.8× | 8.8× |
| 7 (space-amp cap 1000×) | 4000 | 5.8× | 8.8× |
| 3 | 1000 | 42× | 63× |
| 3 | 4000 | 165× | 251× |

At the default 7 levels growth is logarithmic, about +1.7× per 4× more data. With 3 levels (2 runs below L0) it grows linearly in the data for both pickers, as any scheme with two runs must above O(√N). The format-free fix was worse everywhere, because it rewrites deep runs that the cascade leaves alone. A shift-only variant (push runs down into a free level, else take L1 as now) never fires, for the reason above, and matches the current picker exactly.

**Proposal:** keep the picker. Guard against a regression with a write-amplification bound in `tiered_picker_keeps_runs_and_space_amp_within_bounds`, run with insert-only merges: `levels × batches^(1/(levels-2)) + 2`, where `batches` is flushes over `l0_trigger`. A picker that rewrote a growing L1 into every L0 merge breaks it: a hacked picker measured 75.9× against a bound of 28.6 at 5 levels. Close #228. If shallow tiered families (`max_levels` 3–4) matter, sorted runs in L0 (a format change) is the real lever; that is a separate issue, if wanted.

**Interim behavior:** the picker is unchanged; only the test gained the bound.

**Coordinator:** confirmed. #228 is closed by #245: no picker change, a write-amplification bound in the picker proptest. Shallow tiered families (`max_levels` 3) grow write amplification linearly; the guide should say so (docs follow-up).
