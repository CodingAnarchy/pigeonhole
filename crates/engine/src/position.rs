//! A position within a row: the column (family and qualifier) a walk last handed out, so a
//! later read resumes after it (D297, ICR 0095). D296's reads may start at one too.

use pigeonhole_format::varint;

use crate::{Error, FamilyId, Result, TableId};

/// A column of a row in one table: the family and the unescaped qualifier. A walk of a row
/// resumes **after** this column (all of its versions). It names no row: a position from
/// one row of a table positions a read of another row of that table at the same column.
///
/// Its byte form ([`ColumnPosition::to_bytes`]) is stable: byte 0 is the encoding version
/// (1), then the table id and the family id (`u32` LE each), then the qualifier,
/// length-prefixed with a varint.
///
/// ```
/// use pigeonhole_engine::{ColumnPosition, FamilyId, TableId};
///
/// let p = ColumnPosition::new(TableId(3), FamilyId(7), b"q:42".to_vec());
/// let bytes = p.to_bytes();
/// assert_eq!(bytes[0], 1);
/// assert_eq!(ColumnPosition::from_bytes(&bytes).unwrap(), p);
/// assert!(ColumnPosition::from_bytes(&[2]).is_err());
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ColumnPosition {
    table: TableId,
    family: FamilyId,
    qualifier: Vec<u8>,
}

/// The byte form's version.
const VERSION: u8 = 1;

impl ColumnPosition {
    /// The position of column `qualifier` of `family` in `table`.
    pub fn new(table: TableId, family: FamilyId, qualifier: Vec<u8>) -> Self {
        Self {
            table,
            family,
            qualifier,
        }
    }

    /// The table.
    pub fn table(&self) -> TableId {
        self.table
    }

    /// The family.
    pub fn family(&self) -> FamilyId {
        self.family
    }

    /// The unescaped qualifier.
    pub fn qualifier(&self) -> &[u8] {
        &self.qualifier
    }

    /// The stable byte form.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(10 + self.qualifier.len());
        out.push(VERSION);
        out.extend_from_slice(&self.table.0.to_le_bytes());
        out.extend_from_slice(&self.family.0.to_le_bytes());
        varint::put_bytes(&mut out, &self.qualifier);
        out
    }

    /// Decodes [`ColumnPosition::to_bytes`]'s form. An unknown version, a truncated or
    /// overlong input is `InvalidArgument`.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let bad = |why: &str| Error::InvalidArgument(format!("column position: {why}"));
        let (&version, rest) = bytes.split_first().ok_or_else(|| bad("empty"))?;
        if version != VERSION {
            return Err(bad(&format!("unknown encoding version {version}")));
        }
        if rest.len() < 8 {
            return Err(bad("truncated"));
        }
        let table = u32::from_le_bytes(rest[..4].try_into().expect("4 bytes"));
        let family = u32::from_le_bytes(rest[4..8].try_into().expect("4 bytes"));
        let (qualifier, used) = varint::get_bytes(&rest[8..]).map_err(|_| bad("truncated"))?;
        if 8 + used != rest.len() {
            return Err(bad("trailing bytes"));
        }
        Ok(Self::new(
            TableId(table),
            FamilyId(family),
            qualifier.to_vec(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_byte_form_round_trips_and_refuses_malformed_input() {
        for q in [&b""[..], b"q", &[0u8, 0, 1, 0xff][..], &vec![7u8; 300][..]] {
            let p = ColumnPosition::new(TableId(u32::MAX), FamilyId(1), q.to_vec());
            assert_eq!(ColumnPosition::from_bytes(&p.to_bytes()).unwrap(), p);
        }
        let good = ColumnPosition::new(TableId(1), FamilyId(2), b"abc".to_vec()).to_bytes();
        for bad in [
            &[][..],
            &good[..good.len() - 1],
            &[&good[..], &[0u8][..]].concat()[..],
            &[&[9u8][..], &good[1..]].concat()[..],
            &good[..5],
        ] {
            assert!(ColumnPosition::from_bytes(bad).is_err(), "{bad:?}");
        }
    }
}
