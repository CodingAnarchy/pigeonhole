# 0058: `ErrorCode::UnsupportedFeature`

**Status:** Approved (owner, via the coordinator, 2026-10-21; #496, D4, PR 1). Additive.

## Change

- **`pigeonhole`:** `ErrorCode::UnsupportedFeature = 30`. The file uses a feature this build doesn't support. 29 is `NoWriter`, ICR 0057.
- **`pigeonhole-engine`:** `Error::UnsupportedFeature(String)`, whose payload names the feature, for example `"value tag 4"`. The enum is `#[non_exhaustive]`.
- **`pigeonhole-format`:** `Error::UnsupportedFeature { what, value }`. The enum is `#[non_exhaustive]`.

**Every persisted enum's unknown value now decodes to it, not `Corrupt`:**
- key kind;
- value tag;
- block kind;
- filter kind;
- manifest block kind;
- family compression, cache priority, compaction style and kind;
- WAL record type.

The bytes passed their checksum, so an unknown value means a newer build wrote them.

**Compression:** `format::Error::UnsupportedCompression` (from the block trailer) stays, so the format crate's API isn't broken, but the engine now maps it to `UnsupportedFeature`; it used to map to `Corruption`. `sst::Error::is_unsupported()` includes the new variant.

In the CLI it exits 6, like `UnsupportedFormat`.

## Why

#496's freeze makes "the file uses a feature from a newer version" a normal, nameable outcome. A program can then tell "upgrade the library" from damage (`Corruption`, restore a backup) and from a whole-file version it can't read (`UnsupportedFormat`, another major). Before this, an unknown value read as corruption, which would send a user to their backups for a file that's fine.

## Not in this change

- **WAL fragment types:** an unknown type in a fragment whose CRC passes is still a stop. That's a WAL-choice change, and it's in #496 PR 2 with the other silent-misread fixes, under perf287's review.
- **Manifest edit tags:** unknown tags are still skipped. Splitting them into must-understand and skippable ranges is D5, which is with the owner.
- **The offline `check`:** it still records an unknown value as a corruption finding. After the freeze the superblock's feature bits stop it first. Today, within format 3, a file with values this build doesn't know isn't realistic.

## Callers

- `pigeonhole-format`:
  - `error.rs` (the variant);
  - `key.rs`, `value.rs`, `block.rs`, `filter.rs`, `manifest.rs`, `wal.rs` (the decoders);
  - `tests/roundtrip.rs` (an unknown value of each enum gives `UnsupportedFeature`).
- `pigeonhole-sst`: `is_unsupported`.
- `pigeonhole-engine`: `error.rs` (the variant, `Display`, and the mapping, including compression).
- `pigeonhole`: `error.rs`, and `tests/api.rs` (the number and mapping).
- `pigeonhole-cli`: `exit.rs` (exit 6).
- Docs: errors.md, the agent reference, cli.md, and the changelog.
