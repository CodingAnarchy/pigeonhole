# 0001: `pigeonhole_format::Error::InvalidArgument` and a fallible `StreamList::encode`

**Status:** proposed (format agent, from the PR #4 review). Code is unchanged until this is approved.

## Change

1. Add `Error::InvalidArgument { what: &'static str }` to `pigeonhole_format::Error`, which is `#[non_exhaustive]`, so adding a variant is not a breaking change. Use it for caller misuse that is not a size limit:
   - `key::encode_key` with `Kind::FamilyDelete`: a marker key must be built with `encode_marker_key`.
   - `wal::BatchBuilder::push` with a delete that carries a value, or a `FamilyDelete` that carries a qualifier.
   - `block::BlockBuilder::add` after `finish`, or with keys out of order.

   Decoding the same conditions from bytes stays `Corrupt`.
2. Change `wal::StreamList::encode(streams, out)` to return `Result<()>`, failing with `InvalidArgument` on more than `u16::MAX` streams. Today it asserts, and documents the panic.

## Why

The review asked that caller misuse not be reported as `Corrupt`, which means "the bytes on disk are bad" and will surface to users as data corruption. No existing variant fits: `KeyTooLarge` and `ValueTooLarge` would mislead. Size-limit misuse already uses them; for example, a block over the 128 MiB cap is `ValueTooLarge`.

## Callers

- `Error` is matched in `pigeonhole-engine` (`impl From<pigeonhole_format::Error> for engine::Error`, still `todo!()`). It must map `InvalidArgument` to `engine::Error::InvalidArgument`, which flattens to the public `ErrorCode::InvalidArgument` (19). Because the enum is non-exhaustive, the match already needs a wildcard arm.
- `StreamList::encode`: no callers outside `pigeonhole-format` yet. The planned caller is the engine's two-phase commit, which writes the COMMIT record (`docs/design/interfaces.md`, Write path step 6).
- `encode_key`, `BatchBuilder::push`, `BlockBuilder::add`: already return `Result`, so callers are unaffected beyond the new variant.
