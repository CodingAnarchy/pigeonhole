//! A shared-memory region that cannot be allocated fails the open with `ShmUnavailable`,
//! naming its size and the remedies, instead of letting the process die of `SIGBUS` later.

use pigeonhole::doc_support::temp_dir;
use pigeonhole::{ErrorCode, Options, Pigeonhole};

#[test]
fn missing_shm_dir_fails_at_open() {
    let dir = temp_dir();
    let shm = dir.join("no-such-dir");
    let options = Options::default()
        .shards(2)
        .memtable_budget(1 << 20)
        .shm_dir(&shm);
    let err = Pigeonhole::open(dir.join("db.phdb"), options).unwrap_err();
    assert_eq!(err.code(), ErrorCode::ShmUnavailable, "{err}");
    let m = err.message();
    assert!(m.contains(&shm.display().to_string()), "{m}");
    assert!(
        m.contains("14 MiB (2 shards × 1 MiB memtable_budget"),
        "{m}"
    );

    // The failed open left nothing locked: a good configuration opens the same file.
    let db = Pigeonhole::open(dir.join("db.phdb"), Options::default().shards(1)).unwrap();
    db.close().unwrap();
}

/// The tmpfs size the Linux test mounts: room for a 12 MiB region, not for a 74 MiB one.
#[cfg(target_os = "linux")]
const SMALL_TMPFS: &str = "size=32m";

/// Set (to the small tmpfs mount point) in the child that runs inside its own mount namespace.
#[cfg(target_os = "linux")]
const CHILD_ENV: &str = "PIGEONHOLE_TEST_SMALL_SHM";

/// Printed by the child once it runs, so the parent can tell "could not set up" from "failed".
#[cfg(target_os = "linux")]
const CHILD_MARKER: &str = "small-shm child running";

/// A region larger than its tmpfs (Docker's 64 MiB `/dev/shm`, in miniature). Without the
/// free-space check at open, the open succeeded and the first store past the tmpfs size
/// raised `SIGBUS`.
///
/// Mounting a tmpfs needs privileges, so the test re-runs itself in a private mount namespace:
/// an unprivileged user namespace where the kernel allows one, else `sudo -n` (CI), dropping
/// back to this user before running. Without either it prints why and passes, except on CI.
#[cfg(target_os = "linux")]
#[test]
fn small_shm_fails_at_open() {
    use std::process::Command;

    if let Ok(mount) = std::env::var(CHILD_ENV) {
        println!("{CHILD_MARKER}");
        small_shm_child(std::path::Path::new(&mount));
        return;
    }

    let dir = temp_dir();
    let mount = dir.join("shm");
    std::fs::create_dir(&mount).unwrap();
    let exe = std::env::current_exe().unwrap();
    let id = |flag: &str| {
        let out = Command::new("id").arg(flag).output().unwrap();
        String::from_utf8(out.stdout).unwrap().trim().to_owned()
    };
    let (uid, gid) = (id("-u"), id("-g"));
    // `sh -c SCRIPT sh MOUNT EXE`: mount the tmpfs, then run this test again in the child.
    let script = |mount_options: &str, as_user: &str| {
        format!(
            "mount -t tmpfs -o {SMALL_TMPFS}{mount_options} tmpfs \"$1\" && \
             exec env {CHILD_ENV}=\"$1\" {as_user} \"$2\" --exact small_shm_fails_at_open \
             --nocapture --test-threads=1"
        )
    };
    let attempts = [
        // Root in a new user namespace: mount, then run as that (mapped) root.
        vec![
            "unshare".to_owned(),
            "--user".to_owned(),
            "--map-root-user".to_owned(),
            "--mount".to_owned(),
            "sh".to_owned(),
            "-c".to_owned(),
            script("", ""),
        ],
        // Real root without a password prompt (CI): mount, then drop back to this user.
        vec![
            "sudo".to_owned(),
            "-n".to_owned(),
            "unshare".to_owned(),
            "--mount".to_owned(),
            "sh".to_owned(),
            "-c".to_owned(),
            script(
                &format!(",uid={uid},gid={gid},mode=0700"),
                &format!("setpriv --reuid={uid} --regid={gid} --clear-groups"),
            ),
        ],
    ];
    let mut why = Vec::new();
    for argv in attempts {
        let out = Command::new(&argv[0])
            .args(&argv[1..])
            .arg("sh")
            .arg(&mount)
            .arg(&exe)
            .output();
        let out = match out {
            Ok(out) => out,
            Err(e) => {
                why.push(format!("{}: {e}", argv[0]));
                continue;
            }
        };
        let stdout = String::from_utf8_lossy(&out.stdout);
        let stderr = String::from_utf8_lossy(&out.stderr);
        if stdout.contains(CHILD_MARKER) {
            assert!(
                out.status.success(),
                "the test failed in its mount namespace:\n{stdout}\n{stderr}"
            );
            return;
        }
        why.push(format!("{}: {}", argv[0], stderr.trim()));
    }
    let why = why.join("; ");
    // CI runners have passwordless sudo: a skip there would hide a regression.
    assert!(
        std::env::var_os("CI").is_none(),
        "cannot mount a small tmpfs on CI: {why}"
    );
    println!("skipped: cannot mount a small tmpfs here ({why})");
}

#[cfg(target_os = "linux")]
fn small_shm_child(mount: &std::path::Path) {
    use pigeonhole::Family;

    let dir = temp_dir();
    let path = dir.join("db.phdb");
    let big = Options::default()
        .shards(4)
        .memtable_budget(16 << 20)
        .shm_dir(mount);
    let err = Pigeonhole::open(&path, big).unwrap_err();
    assert_eq!(err.code(), ErrorCode::ShmUnavailable, "{err}");
    let m = err.message();
    assert!(m.contains(&mount.display().to_string()), "{m}");
    assert!(
        m.contains("74 MiB (4 shards × 16 MiB memtable_budget"),
        "{m}"
    );

    // The failed region was removed, so a region that fits gets the whole tmpfs and works.
    let small = Options::default()
        .shards(1)
        .memtable_budget(2 << 20)
        .shm_dir(mount);
    let db = Pigeonhole::open(&path, small).unwrap();
    let t = db
        .table("t")
        .unwrap()
        .family("f", Family::default())
        .create_if_missing()
        .unwrap();
    for i in 0..2_000u32 {
        t.mutate(&i.to_be_bytes())
            .put("f", b"q", &[7u8; 512])
            .commit()
            .unwrap();
    }
    drop(t);
    db.close().unwrap();
}
