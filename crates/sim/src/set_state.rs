//! The roaring set oracle (D298; FORMAT §3.8): a set family's states and operands written
//! from the encoding's description over a `BTreeSet<u32>`, apart from the store's
//! `pigeonhole_format::roaring`.

use std::collections::BTreeSet;

/// The element and whether it is added, of a set family's input `delta` (the workload's
/// `i64`): added when `delta` is at least 0, else removed; the element `(|delta| % 7) ·
/// 13107`, one of seven spanning two containers, so a cell's adds and removes of one element
/// meet often and their order shows.
pub fn set_input(delta: i64) -> (bool, u32) {
    (delta >= 0, (delta.unsigned_abs() % 7) as u32 * 13107)
}

fn encode(set: &BTreeSet<u32>) -> Vec<u8> {
    let mut containers: Vec<(u16, Vec<u16>)> = Vec::new();
    for &x in set {
        let (key, low) = ((x >> 16) as u16, x as u16);
        match containers.last_mut() {
            Some((k, lows)) if *k == key => lows.push(low),
            _ => containers.push((key, vec![low])),
        }
    }
    let n = containers.len();
    let mut out = Vec::new();
    out.extend_from_slice(&12346u32.to_le_bytes());
    out.extend_from_slice(&(n as u32).to_le_bytes());
    for (key, lows) in &containers {
        out.extend_from_slice(&key.to_le_bytes());
        out.extend_from_slice(&((lows.len() - 1) as u16).to_le_bytes());
    }
    let mut at = 8 + 8 * n;
    for (_, lows) in &containers {
        out.extend_from_slice(&(at as u32).to_le_bytes());
        at += if lows.len() <= 4096 {
            2 * lows.len()
        } else {
            8192
        };
    }
    for (_, lows) in &containers {
        if lows.len() <= 4096 {
            lows.iter()
                .for_each(|l| out.extend_from_slice(&l.to_le_bytes()));
        } else {
            let mut bits = vec![0u8; 8192];
            lows.iter()
                .for_each(|&l| bits[usize::from(l) / 8] |= 1 << (l % 8));
            out.extend_from_slice(&bits);
        }
    }
    out
}

/// The set a canonical state holds, `None` if `s` is not one (decoded here and checked by
/// re-encoding).
fn decode(s: &[u8]) -> Option<BTreeSet<u32>> {
    let u16_at =
        |i: usize| -> Option<u16> { Some(u16::from_le_bytes(s.get(i..i + 2)?.try_into().ok()?)) };
    let u32_at =
        |i: usize| -> Option<u32> { Some(u32::from_le_bytes(s.get(i..i + 4)?.try_into().ok()?)) };
    if u32_at(0)? != 12346 {
        return None;
    }
    let n = u32_at(4)? as usize;
    if n > 1 << 16 {
        return None;
    }
    let mut set = BTreeSet::new();
    for i in 0..n {
        let key = u32::from(u16_at(8 + 4 * i)?);
        let card = usize::from(u16_at(8 + 4 * i + 2)?) + 1;
        let at = u32_at(8 + 4 * n + 4 * i)? as usize;
        if card <= 4096 {
            for j in 0..card {
                set.insert(key << 16 | u32::from(u16_at(at + 2 * j)?));
            }
        } else {
            let bits = s.get(at..at + 8192)?;
            for (b, byte) in bits.iter().enumerate() {
                for k in 0..8 {
                    if byte >> k & 1 == 1 {
                        set.insert(key << 16 | (8 * b + k) as u32);
                    }
                }
            }
        }
    }
    (encode(&set) == s).then_some(set)
}

/// The state of `base` (if any) with the inputs `elements` applied in commit order
/// ([`set_input`]). `None` if `base` is malformed, or there is neither a base nor an input.
pub fn set_state(base: Option<&[u8]>, elements: impl IntoIterator<Item = i64>) -> Option<Vec<u8>> {
    let mut any = base.is_some();
    let mut set = match base {
        Some(b) => decode(b)?,
        None => BTreeSet::new(),
    };
    for d in elements {
        any = true;
        let (add, x) = set_input(d);
        if add {
            set.insert(x);
        } else {
            set.remove(&x);
        }
    }
    any.then(|| encode(&set))
}

/// Whether `s` is a canonical state.
pub fn set_state_valid(s: &[u8]) -> bool {
    decode(s).is_some()
}

/// The operand the inputs `elements` (commit order) combine into: the elements they leave
/// added, and those they leave removed, as `A`'s length (u32 LE), `A`, then `R`.
pub fn set_operand(elements: impl IntoIterator<Item = i64>) -> Vec<u8> {
    let (mut adds, mut removes) = (BTreeSet::new(), BTreeSet::new());
    for d in elements {
        let (add, x) = set_input(d);
        if add {
            removes.remove(&x);
            adds.insert(x);
        } else {
            adds.remove(&x);
            removes.insert(x);
        }
    }
    let a = encode(&adds);
    let mut out = (a.len() as u32).to_le_bytes().to_vec();
    out.extend_from_slice(&a);
    out.extend_from_slice(&encode(&removes));
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_oracle_agrees_with_the_format() {
        // Inputs in commit order, onto a base, against the store's operator code.
        let base = set_state(None, [1, 2, 3, 50]).unwrap();
        let inputs = [-2, 7, 2, -50, 96, -96];
        let want = set_state(Some(&base), inputs).unwrap();
        let mut got = Vec::new();
        pigeonhole_format::roaring::apply(&mut got, Some(&base), &set_operand(inputs)).unwrap();
        assert_eq!(got, want);
        // A large set turns its containers into bitmaps the same way.
        let big: BTreeSet<u32> = (0..9000).collect();
        let mut f = Vec::new();
        pigeonhole_format::roaring::encode_sorted(&mut f, big.iter().copied());
        assert_eq!(encode(&big), f);
        assert!(set_state_valid(&f));
    }
}
