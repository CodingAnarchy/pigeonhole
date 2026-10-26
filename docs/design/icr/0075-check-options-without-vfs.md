# 0075: `pigeonhole::CheckOptions` loses its hidden `vfs`

**Status:** Approved (coordinator, 2026-10-26; #951, the Phase 4 baseline cleanup). Amends ICR 0048. Breaking in 0.x on a `#[doc(hidden)]` item, removed before the 1.0 API freeze so the unused seam is never frozen.

## Change

**`pigeonhole`:** `CheckOptions::vfs(self, VfsRef) -> Self` is removed, along with `CheckOptions`' private `vfs` field. `check` always opens the file through `PreadVfs`.

## Why

ICR 0048 added `vfs` as a seam for the simulation, but nothing ever called it: every `CheckOptions` in the workspace is `default()`, sometimes with `.deep(..)`. The simulated and engine-level checks call `pigeonhole_engine::Engine::check(&vfs, path, &options)` directly, which takes the VFS as an argument, so they keep their coverage.

## Callers

None. The workspace has no call to `CheckOptions::vfs`: not in `pigeonhole`, `pigeonhole-cli` (`phdb check`, `phdb backup`), the tests, benches or examples. `cargo semver-checks` sees a removal of a hidden method. It is not part of the documented API.
