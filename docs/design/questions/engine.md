# Engine: open questions

## Q: Should one writer task own the manifest, with every other caller only queueing and waiting? (#134; changes D150)

For the owner. #134 step 2 asked for this note after the close gate (#524) and the watermark-waiter registry (#528) landed. The manifest exclusion protocol it would replace is now loom-modelled as it stands (#536).

### Today
There is one manifest writer and at most one root commit in flight. The right to turn the request queue into a commit is the exclusion `manifest_busy`, a try-lock (`docs/design/concurrency.md`, "The manifest exclusion and pump"). Three kinds of thread take it.

| Path | Who | Calls |
|---|---|---|
| **Pump** | A cooperative `ManifestPump` task on the submitting shard. It claims the exclusion, takes the queue, submits the root commit, and finishes on its completion. | Flush, compaction, tablet changes, WAL checkpoints; large-value release (on shard 0) |
| **Thread drain** | An application thread that finds the exclusion free drains the queue itself, with blocking root commits. | Catalog changes (`create_table`, `add_family`, `drop_table`), `shrink`, large-value separation in `submit`/`commit` (#230) |
| **Holder** | A thread that already holds the exclusion. | The final close (`forget_checkpoints`, the clean mark) |

Three rules hold this together:
1. **Release, then re-check.** Whoever releases looks at the queue again, because a request pushed while the exclusion was held found it busy and its pump left.
2. **Take-over.** A blocked thread may finish a pump's commit whose root commit completed (`Flight::take_done`). Otherwise a thread that drives the pump's shard would wait on itself (#135, #166).
3. **Blocked threads make their own progress.** A blocked thread reaps its own I/O, then orphaned I/O, then parks (#207, ICR 0028). It's woken by releases, by root-commit completions, and by its own request's answer.

Rules 2 and 3 are what D150 rests on. In application-owned mode, catalog changes and `shrink` are **not refused on a thread that drives a shard**: "a blocked thread finishes a pump commit whose root commit completed (`manifest::Flight`), so they cannot deadlock on a driving thread". Large-value separation inside `submit` relies on the same rule.

### The proposal
A single long-lived **writer task** owns the manifest writer and the queue. Every other caller only enqueues and waits for its reply.

What it removes:
- **Primitives:** `manifest_busy`, the three `Flight` primitives (`state`, `threads`, `thread_count`) and the `Shared::manifest` mutex (the writer becomes the task's own state). That's 5 of the 153 in `concurrency.md`; the queue's mutex stays, or becomes the shard inbox.
- **Protocols:**
  - the release-then-re-check rule;
  - the take-over;
  - the blocked-thread park/wake protocol for the manifest;
  - the try-lock claim inside the close gate (`CloseGate::try_start`'s `claim`): the final close becomes the writer's last message instead.
- **Code:** roughly 300 lines of manifest.rs and `Flight`, and the test hooks `race_window` and `manifest_thread_parks` (#536).
- **Bug history:** these paths are where Phase 1's bugs clustered: #78, #81, #135, #166, #207 and #306.

What it doesn't change: the commit path. A write commit never touches the manifest, so no commit shape's instruction count moves. Flush and compaction already queue and wait; their latency is unchanged.

### What it costs: D150's guarantee
The engine runs no threads of its own in application-owned mode: `compaction_threads` must be 0, and application-owned io_uring has no reaper. So the writer task has to live on a shard the application drives. Place it on shard 0, or the shard of the first `open`. A thread that blocks on a manifest reply while it is that shard's driver then waits on itself. The waiting thread can't run the writer task instead: it has no `&mut EngineShard`, and letting it would be today's take-over again.

So in application-owned mode, these would deadlock on the writer shard's driver unless they change:
- **Catalog changes and `shrink`.** D150 (a) today says these are not refused. Under the proposal, on a thread that drives the writer shard, they are **refused before submitting** (`InvalidArgument`, nothing done), as blocking commits already are. Async variants returning a future are added for event loops (`create_table_async`, and so on: new API, ICR, C ABI).
- **Large-value separation in `submit`.** It can't block on the writer from the event loop. The blob-file commit becomes a request whose reply submits the batch, so `submit` returns at once. That's a real restructuring of #230's path, and it changes when a separated put's `PendingCommit` first exists.
- **Single-threaded applications.** A typical event loop drives every shard from one thread. For them, every blocking catalog change would be refused, not just some, so they'd have to use the async variants.

Engine-owned mode loses nothing: the writer runs on a shard thread, and application threads never drive shards.

Two cases need a rule:
- **The writer's shard is dropped** (application-owned) **or its thread panics** (engine-owned, D152). Either every later manifest request fails with `Closed` and the close is unclean (simple: no writer, no manifest), or the writer moves to another live shard (more machinery: the thing this note is trying to remove).
- **The close.** The final close becomes the writer's last job. The documented application-owned close loop already drives every shard until `closed()`, so it keeps working, provided the writer's shard is still driven.

### Options
1. **Keep today's design** (the exclusion protocol, now loom-modelled in #536), and stop #134's manifest work here. No API change; D150 stands.
2. **One writer task, as above.** D150 (a) is amended for the writer shard's driver. Async catalog and maintenance APIs are added, and large-value separation is restructured. Rough size: 3–4 PRs (the writer task and queue; async APIs with an ICR; separation; removing the old paths), each with loom models and the standard sweeps.
3. **Writer task in engine-owned mode only;** application-owned mode keeps today's paths. That's two designs instead of one, which is worse than either. Listed only to rule it out.

### Recommendation
**Option 1, for now.** Today's protocol was the riskiest coordination in the engine, but it is now:
- loom-modelled around the real `Flight`;
- pinned by mutation-checked engine tests (`milestone_b`'s release window, `blocking_waits` and `deferred_waits`' take-overs, `manifest_wake`);
- documented in `concurrency.md`.

The redesign's win is about 5 primitives and three protocols. Its price is a public guarantee (D150) plus new async API surface. That's better decided together with the public API and C ABI review than inside #134.

Revisit if one of these happens:
- a new bug lands in these paths;
- the C ABI review wants async catalog calls anyway: then option 2 costs little more, because the async variants are the expensive part.

### Decisions needed (if option 2)
1. **D150 (a):** may a catalog change or `shrink` be refused (`InvalidArgument`, before submitting) on the thread that drives the writer's shard, with async variants as the supported path there?
2. **Placement:** is the writer on shard 0 (simplest) or on a shard named at open?
3. **Writer shard lost** (dropped or panicked): fail later manifest work with `Closed` and mark the close unclean, or move the writer?
4. **Large-value separation:** may `submit` of a separated put return before the blob-file commit (its reply submits the batch)?

**Interim behavior:** today's design (option 1). Nothing in the code changes until the owner decides.
