//! x86 instruction counts of CRC32C candidates on WAL-sized inputs (#1206 lever 4).
//! Each `b_*` function is called N times per size; callgrind counts each by name.
use std::hint::black_box;

const N: usize = 10_000;
const SIZES: [usize; 6] = [16, 64, 160, 256, 1024, 4096];

#[inline(never)]
fn b_crc32c(d: &[u8]) -> u32 { crc32c::crc32c(d) }

#[inline(never)]
fn b_crcfast(d: &[u8]) -> u32 {
    crc_fast::checksum(crc_fast::CrcAlgorithm::Crc32Iscsi, d) as u32
}

static CRC: crc::Crc<u32, crc::Table<16>> = crc::Crc::<u32, crc::Table<16>>::new(&crc::CRC_32_ISCSI);
#[inline(never)]
fn b_crc16t(d: &[u8]) -> u32 { CRC.checksum(d) }

#[cfg(target_arch = "x86_64")]
#[inline(never)]
fn b_hand(d: &[u8]) -> u32 {
    #[target_feature(enable = "sse4.2")]
    unsafe fn go(d: &[u8]) -> u32 {
        use std::arch::x86_64::*;
        let mut c: u64 = 0xFFFF_FFFF;
        let mut ch = d.chunks_exact(8);
        for w in &mut ch { c = _mm_crc32_u64(c, u64::from_le_bytes(w.try_into().unwrap())); }
        let mut c = c as u32;
        for &b in ch.remainder() { c = _mm_crc32_u8(c, b); }
        !c
    }
    unsafe { go(d) }
}

fn main() {
    let data: Vec<u8> = (0..4096u32).map(|i| (i.wrapping_mul(2654435761) >> 13) as u8).collect();
    for &n in &SIZES {
        let d = &data[..n];
        let want = b_crc32c(d);
        assert_eq!(b_crcfast(d), want, "crc-fast {n}");
        assert_eq!(b_crc16t(d), want, "crc {n}");
        #[cfg(target_arch = "x86_64")]
        assert_eq!(b_hand(d), want, "hand {n}");
    }
    let which = std::env::args().nth(1).expect("which");
    let n: usize = std::env::args().nth(2).expect("size").parse().unwrap();
    let d = &data[..n];
    let mut acc = 0u32;
    for _ in 0..N {
        acc ^= match which.as_str() {
            "crc32c" => b_crc32c(black_box(d)),
            "crcfast" => b_crcfast(black_box(d)),
            "crc16t" => b_crc16t(black_box(d)),
            #[cfg(target_arch = "x86_64")]
            "hand" => b_hand(black_box(d)),
            _ => panic!("unknown"),
        };
    }
    println!("{acc}");
}
