//! A family kind's parameters, `kind_params` (decision D298; FORMAT §9.2.1): a canonical
//! list of tag-length-value entries, appended to `PutFamily` after the `kind` byte only when
//! the kind has parameters. The same encoding serves #488's aggregate kinds and #490's vector
//! kind.
//!
//! - An entry is `tag` (u8), `len` (varint), then `len` bytes. Entries are sorted by tag, each
//!   tag at most once, and the list is not empty: so the encoding of a set of parameters is
//!   one byte string.
//! - Tags `1..=127` are must-understand: a kind meeting one it doesn't know refuses the family
//!   (`UnsupportedFeature`, the backstop behind the kind's feature bit). Tags `128..=255` are
//!   skippable hints. Tag 0 is never written.
//! - A kind fixes its tags, and their value encodings, when it lands; a later must-understand
//!   tag on an existing kind takes a new `incompat` bit.
//!
//! ```
//! use pigeonhole_format::kind_params;
//!
//! let bytes = kind_params::encode(&[(2, &7u32.to_le_bytes()), (1, &[3])]);
//! let entries = kind_params::parse(&bytes).unwrap();
//! assert_eq!(entries, [(1, &[3][..]), (2, &7u32.to_le_bytes()[..])]);
//! assert!(kind_params::parse(&[]).is_err()); // present means non-empty
//! ```

use crate::Error;

/// The first skippable tag: tags below it are must-understand.
pub const FIRST_SKIPPABLE: u8 = 128;

fn corrupt() -> Error {
    Error::Corrupt {
        what: "family kind parameters",
    }
}

/// The canonical encoding of `entries`, which are sorted by tag here; the caller passes each
/// tag once and never tag 0 (checked in debug builds).
pub fn encode(entries: &[(u8, &[u8])]) -> Vec<u8> {
    let mut sorted: Vec<&(u8, &[u8])> = entries.iter().collect();
    sorted.sort_by_key(|e| e.0);
    debug_assert!(
        sorted.windows(2).all(|w| w[0].0 < w[1].0) && sorted.first().is_none_or(|e| e.0 != 0),
        "kind_params tags must be unique and nonzero"
    );
    let mut out = Vec::new();
    for (tag, value) in sorted {
        out.push(*tag);
        crate::varint::put_bytes(&mut out, value);
    }
    out
}

/// The entries of a present `kind_params` field, in tag order. It is `Corrupt` unless the
/// field is canonical: non-empty, tags nonzero, strictly increasing, and lengths in bounds.
pub fn parse(bytes: &[u8]) -> crate::Result<Vec<(u8, &[u8])>> {
    if bytes.is_empty() {
        return Err(corrupt());
    }
    let mut out: Vec<(u8, &[u8])> = Vec::new();
    let mut at = 0;
    while at < bytes.len() {
        let tag = bytes[at];
        at += 1;
        if tag == 0 || out.last().is_some_and(|(t, _)| *t >= tag) {
            return Err(corrupt());
        }
        let (value, used) = crate::varint::get_bytes(&bytes[at..]).map_err(|_| corrupt())?;
        at += used;
        out.push((tag, value));
    }
    Ok(out)
}

/// Whether `tag` is must-understand: a kind that doesn't define it refuses the family.
pub fn must_understand(tag: u8) -> bool {
    tag < FIRST_SKIPPABLE
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn non_canonical_lists_are_corrupt() {
        // tag 0; out of order; duplicate; truncated length; truncated value.
        for bad in [
            &[0u8, 0][..],
            &[2, 0, 1, 0],
            &[1, 0, 1, 0],
            &[1],
            &[1, 3, 0xaa],
        ] {
            assert!(parse(bad).is_err(), "{bad:?}");
        }
        assert_eq!(
            parse(&[1, 0, 200, 1, 9]).unwrap(),
            [(1, &[][..]), (200, &[9][..])]
        );
        assert!(must_understand(127) && !must_understand(128));
    }
}
