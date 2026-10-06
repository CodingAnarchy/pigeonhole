//! Time series with a TTL: one row per sensor and day, one column per second (big-endian, so
//! columns sort by time), the reading's event time as the version timestamp, and a family
//! TTL that expires old readings by that timestamp.
//!
//! Run with `cargo run -p pigeonhole --example time_series_ttl`.

use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use pigeonhole::{Family, Options, Pigeonhole, days};

const MICROS_PER_SEC: u64 = 1_000_000;

/// Runs the example in a temporary directory, removed afterwards.
pub fn main() -> pigeonhole::Result<()> {
    let dir = std::env::temp_dir().join(format!(
        "pigeonhole-example-time-series-{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).expect("create a temporary directory");
    let result = run(&dir.join("readings.phdb"));
    let _ = std::fs::remove_dir_all(&dir);
    result
}

/// The row of `sensor` for the day containing `secs`: `sensor:7:20367` (days since epoch).
fn row_key(sensor: u32, secs: u64) -> Vec<u8> {
    format!("sensor:{sensor}:{}", secs / 86_400).into_bytes()
}

fn run(path: &Path) -> pigeonhole::Result<()> {
    let db = Pigeonhole::open(path, Options::default())?;
    let readings = db
        .table("readings")?
        .family("temp", Family::default().max_versions(1).ttl(days(30)))
        .create_if_missing()?;

    let now_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after 1970")
        .as_secs();
    // Midnight today, so every reading below lands in one day's row.
    let day_start = now_secs - now_secs % 86_400;

    // An hour of readings, one a minute, written with their event time.
    for minute in 0..60u64 {
        let secs = day_start + minute * 60;
        let celsius = 20.0 + (minute as f64) / 10.0;
        readings
            .mutate(&row_key(7, secs))
            .put_at(
                "temp",
                &secs.to_be_bytes(),
                secs * MICROS_PER_SEC,
                &celsius.to_le_bytes(),
            )
            .commit()?;
    }

    // A ten-minute window is one contiguous qualifier range of one row.
    let lo = (day_start + 10 * 60).to_be_bytes();
    let hi = (day_start + 20 * 60).to_be_bytes();
    let window = readings
        .row(&row_key(7, day_start))
        .family("temp")
        .qualifier_range(&lo[..]..&hi[..])
        .read()?
        .expect("readings in the window");
    let temps: Vec<f64> = window
        .iter()
        .map(|e| f64::from_le_bytes(e.cell.value().try_into().expect("8-byte f64")))
        .collect();
    println!(
        "{} readings between minute 10 and 20: {temps:?}",
        temps.len()
    );
    assert_eq!(temps.len(), 10);

    // A reading timestamped 31 days ago is already past the family's TTL: never visible.
    let old_secs = now_secs - 31 * 86_400;
    readings
        .mutate(&row_key(7, old_secs))
        .put_at(
            "temp",
            &old_secs.to_be_bytes(),
            old_secs * MICROS_PER_SEC,
            &1.0f64.to_le_bytes(),
        )
        .commit()?;
    assert!(readings.row(&row_key(7, old_secs)).read()?.is_none());
    println!("the 31-day-old reading expired by its timestamp");

    // Every day of sensor 7 is one prefix scan.
    let days_with_data = readings.scan_prefix(b"sensor:7:").iter()?.count();
    println!("sensor 7 has data on {days_with_data} day(s)");
    assert_eq!(days_with_data, 1);
    db.close()
}
