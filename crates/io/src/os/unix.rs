use std::fs;
use std::io;
use std::os::fd::AsRawFd;
use std::os::unix::fs::{FileExt, MetadataExt};
use std::path::Path;
use std::ptr::NonNull;

use super::Liveness;
use crate::{Error, ErrorKind, FileIdentity, LockMode, Result, SharedOpen};

pub(crate) fn read_exact_at(file: &fs::File, buf: &mut [u8], offset: u64) -> io::Result<()> {
    file.read_exact_at(buf, offset)
}

pub(crate) fn write_all_at(file: &fs::File, buf: &[u8], offset: u64) -> io::Result<()> {
    file.write_all_at(buf, offset)
}

fn off(v: u64, context: &'static str) -> Result<libc::off_t> {
    libc::off_t::try_from(v).map_err(|_| Error::new(ErrorKind::Other, context))
}

fn last_error(context: &'static str) -> Error {
    Error::os(context, io::Error::last_os_error())
}

pub(crate) fn identity(file: &fs::File) -> Result<FileIdentity> {
    let meta = file.metadata().map_err(|e| Error::os("stat", e))?;
    Ok(FileIdentity {
        device: meta.dev(),
        inode: meta.ino(),
    })
}

#[cfg(any(target_os = "linux", target_os = "android"))]
pub(crate) fn is_local(file: &fs::File) -> Result<bool> {
    // Filesystem magic numbers of network and cluster filesystems (linux/magic.h).
    const REMOTE: &[u32] = &[
        0x6969,      // NFS
        0x517B,      // SMB
        0xFF53_4D42, // CIFS
        0xFE53_4D42, // SMB2
        0x7375_7245, // CODA
        0x5346_414F, // AFS
        0x6B41_4653, // kAFS
        0x0102_1997, // 9P (v9fs)
        0x00C3_6400, // Ceph
        0x0BD0_0BD0, // Lustre
        0x0116_1970, // GFS2
        0x7461_636F, // OCFS2
    ];
    // SAFETY: `statfs` is plain old data; all-zero is a valid value.
    let mut st: libc::statfs = unsafe { std::mem::zeroed() };
    // SAFETY: the fd is open for the life of `file`; `st` is a valid out pointer.
    if unsafe { libc::fstatfs(file.as_raw_fd(), &mut st) } != 0 {
        return Err(last_error("fstatfs"));
    }
    #[allow(clippy::unnecessary_cast)] // `f_type` is a different integer type per target.
    let magic = st.f_type as u32;
    Ok(!REMOTE.contains(&magic))
}

#[cfg(target_vendor = "apple")]
pub(crate) fn is_local(file: &fs::File) -> Result<bool> {
    // SAFETY: `statfs` is plain old data; all-zero is a valid value.
    let mut st: libc::statfs = unsafe { std::mem::zeroed() };
    // SAFETY: the fd is open for the life of `file`; `st` is a valid out pointer.
    if unsafe { libc::fstatfs(file.as_raw_fd(), &mut st) } != 0 {
        return Err(last_error("fstatfs"));
    }
    Ok(st.f_flags & libc::MNT_LOCAL as u32 != 0)
}

#[cfg(not(any(target_os = "linux", target_os = "android", target_vendor = "apple")))]
pub(crate) fn is_local(_file: &fs::File) -> Result<bool> {
    // No portable way to ask; assume local.
    Ok(true)
}

#[cfg(any(target_os = "linux", target_os = "android"))]
pub(crate) fn allocate(file: &fs::File, offset: u64, len: u64) -> Result<()> {
    let start = off(offset, "allocate: offset too large")?;
    let count = off(len, "allocate: length too large")?;
    // SAFETY: plain syscall on an fd owned by `file`.
    if unsafe { libc::fallocate(file.as_raw_fd(), 0, start, count) } == 0 {
        return Ok(());
    }
    let err = io::Error::last_os_error();
    match err.raw_os_error() {
        // The filesystem cannot preallocate: fall back to extending the length.
        Some(libc::EOPNOTSUPP) | Some(libc::ENOSYS) => extend(file, offset, len),
        _ => Err(Error::os("fallocate", err)),
    }
}

#[cfg(target_vendor = "apple")]
pub(crate) fn allocate(file: &fs::File, offset: u64, len: u64) -> Result<()> {
    let end = offset
        .checked_add(len)
        .ok_or(Error::new(ErrorKind::Other, "allocate: range overflows"))?;
    let cur = file.metadata().map_err(|e| Error::os("stat", e))?.len();
    if end <= cur {
        return Ok(());
    }
    let mut store = libc::fstore_t {
        fst_flags: libc::F_ALLOCATECONTIG,
        fst_posmode: libc::F_PEOFPOSMODE,
        fst_offset: 0,
        fst_length: off(end - cur, "allocate: length too large")?,
        fst_bytesalloc: 0,
    };
    // SAFETY: `store` is a valid `fstore_t` for F_PREALLOCATE on an fd owned by `file`.
    if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_PREALLOCATE, &mut store) } == -1 {
        store.fst_flags = libc::F_ALLOCATEALL;
        // SAFETY: as above.
        if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_PREALLOCATE, &mut store) } == -1 {
            let err = io::Error::last_os_error();
            if err.raw_os_error() != Some(libc::ENOTSUP) {
                return Err(Error::os("F_PREALLOCATE", err));
            }
        }
    }
    extend(file, offset, len)
}

#[cfg(not(any(target_os = "linux", target_os = "android", target_vendor = "apple")))]
pub(crate) fn allocate(file: &fs::File, offset: u64, len: u64) -> Result<()> {
    extend(file, offset, len)
}

/// Grows the file to cover `[offset, offset + len)` if it is shorter.
fn extend(file: &fs::File, offset: u64, len: u64) -> Result<()> {
    let end = offset
        .checked_add(len)
        .ok_or(Error::new(ErrorKind::Other, "allocate: range overflows"))?;
    let cur = file.metadata().map_err(|e| Error::os("stat", e))?.len();
    if end > cur {
        file.set_len(end).map_err(|e| Error::os("allocate", e))?;
    }
    Ok(())
}

pub(crate) fn sync_dir(dir: &Path) -> Result<()> {
    let d = fs::File::open(dir).map_err(|e| Error::os("open directory", e))?;
    d.sync_all().map_err(|e| Error::os("sync directory", e))
}

/// Sets (or with `None`, clears) the lock on the single byte `byte`, never blocking.
///
/// On Linux this is an open-file-description lock, owned by this handle. Elsewhere it is a
/// classic process-wide `fcntl` lock; `pread` layers a per-process registry on top.
pub(crate) fn set_lock(file: &fs::File, byte: u64, mode: Option<LockMode>) -> Result<()> {
    #[cfg(target_os = "linux")]
    const CMD: libc::c_int = libc::F_OFD_SETLK;
    #[cfg(not(target_os = "linux"))]
    const CMD: libc::c_int = libc::F_SETLK;

    // SAFETY: `flock` is plain old data; all-zero is valid (and `l_pid` must be 0 for OFD).
    let mut fl: libc::flock = unsafe { std::mem::zeroed() };
    #[allow(clippy::unnecessary_cast)] // the constants' types differ per target.
    {
        fl.l_type = match mode {
            Some(LockMode::Shared) => libc::F_RDLCK,
            Some(LockMode::Exclusive) => libc::F_WRLCK,
            None => libc::F_UNLCK,
        } as libc::c_short;
        fl.l_whence = libc::SEEK_SET as libc::c_short;
    }
    fl.l_start = off(byte, "lock: byte offset too large")?;
    fl.l_len = 1;
    // SAFETY: `fl` is a valid `flock` for this command on an fd owned by `file`.
    if unsafe { libc::fcntl(file.as_raw_fd(), CMD, &mut fl) } == 0 {
        return Ok(());
    }
    let err = io::Error::last_os_error();
    match err.raw_os_error() {
        Some(libc::EAGAIN) | Some(libc::EACCES) => Err(Error {
            kind: ErrorKind::Locked,
            context: "lock",
            source: Some(err),
        }),
        _ => Err(Error::os("lock", err)),
    }
}

/// A `mmap`ed shared region; unmapped on drop.
#[derive(Debug)]
pub(crate) struct Mapping {
    ptr: NonNull<u8>,
    len: usize,
}

impl Mapping {
    pub(crate) fn ptr(&self) -> NonNull<u8> {
        self.ptr
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }
}

impl Drop for Mapping {
    fn drop(&mut self) {
        // SAFETY: `ptr`/`len` describe a mapping created by `map_file` and not yet unmapped;
        // every `SharedRegion` clone that could reach it is gone.
        unsafe { libc::munmap(self.ptr.as_ptr().cast(), self.len) };
    }
}

/// Maps `len` bytes of `file` read-write and shared. The file must be at least `len` long.
pub(crate) fn map_file(file: &fs::File, len: usize) -> Result<Mapping> {
    let size = file.metadata().map_err(|e| Error::os("stat", e))?.len();
    if len == 0 || size < len as u64 {
        return Err(Error::new(
            ErrorKind::Other,
            "shared region is empty or smaller than requested",
        ));
    }
    // SAFETY: a fresh shared mapping of an fd we own; the kernel picks the address.
    let p = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            len,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED,
            file.as_raw_fd(),
            0,
        )
    };
    if p == libc::MAP_FAILED {
        return Err(last_error("mmap"));
    }
    let ptr = NonNull::new(p.cast()).ok_or(Error::new(ErrorKind::Other, "mmap returned null"))?;
    Ok(Mapping { ptr, len })
}

/// Creates or opens `path` for a file-backed region and sizes it on creation.
pub(crate) fn open_region_file(path: &Path, len: u64, mode: SharedOpen) -> Result<fs::File> {
    use std::os::unix::fs::OpenOptionsExt;
    let mut opts = fs::OpenOptions::new();
    opts.read(true).write(true).mode(0o600);
    if mode == SharedOpen::CreateNew {
        opts.create_new(true);
    }
    let file = opts
        .open(path)
        .map_err(|e| Error::os("open shared region", e))?;
    if mode == SharedOpen::CreateNew {
        file.set_len(len)
            .map_err(|e| Error::os("size shared region", e))?;
    }
    Ok(file)
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn default_shared_path(name: &str) -> std::path::PathBuf {
    Path::new("/dev/shm").join(name)
}

/// Opens a region in the default memory-backed location: `/dev/shm` on Linux.
#[cfg(any(target_os = "linux", target_os = "android"))]
pub(crate) fn open_default_shared(name: &str, len: u64, mode: SharedOpen) -> Result<Mapping> {
    let path = default_shared_path(name);
    let file = open_region_file(&path, len, mode)?;
    let len = usize::try_from(len).map_err(|_| Error::new(ErrorKind::Other, "region too large"))?;
    map_file(&file, len).inspect_err(|_| {
        if mode == SharedOpen::CreateNew {
            let _ = fs::remove_file(&path);
        }
    })
}

#[cfg(any(target_os = "linux", target_os = "android"))]
pub(crate) fn remove_default_shared(name: &str) -> Result<()> {
    fs::remove_file(default_shared_path(name)).map_err(|e| Error::os("remove shared region", e))
}

#[cfg(not(any(target_os = "linux", target_os = "android")))]
fn shm_name(name: &str) -> Result<std::ffi::CString> {
    std::ffi::CString::new(format!("/{name}"))
        .map_err(|_| Error::new(ErrorKind::Other, "invalid shared-memory name"))
}

/// Opens a region in the default memory-backed location: a POSIX `shm_open` object.
#[cfg(not(any(target_os = "linux", target_os = "android")))]
pub(crate) fn open_default_shared(name: &str, len: u64, mode: SharedOpen) -> Result<Mapping> {
    let cname = shm_name(name)?;
    let flags = match mode {
        SharedOpen::CreateNew => libc::O_RDWR | libc::O_CREAT | libc::O_EXCL,
        SharedOpen::Attach => libc::O_RDWR,
    };
    // SAFETY: `cname` is NUL-terminated; the mode is passed as the variadic `mode_t`.
    let fd = unsafe { libc::shm_open(cname.as_ptr(), flags, 0o600 as libc::c_uint) };
    if fd < 0 {
        return Err(last_error("shm_open"));
    }
    // SAFETY: `fd` was just returned by `shm_open` and is owned by nobody else.
    let file = fs::File::from(unsafe {
        <std::os::fd::OwnedFd as std::os::fd::FromRawFd>::from_raw_fd(fd)
    });
    let result = (|| {
        if mode == SharedOpen::CreateNew {
            let size = off(len, "region too large")?;
            // SAFETY: plain syscall on an fd we own.
            if unsafe { libc::ftruncate(file.as_raw_fd(), size) } != 0 {
                return Err(last_error("size shared region"));
            }
        }
        let len =
            usize::try_from(len).map_err(|_| Error::new(ErrorKind::Other, "region too large"))?;
        map_file(&file, len)
    })();
    if result.is_err() && mode == SharedOpen::CreateNew {
        // SAFETY: `cname` is NUL-terminated.
        unsafe { libc::shm_unlink(cname.as_ptr()) };
    }
    result
}

#[cfg(not(any(target_os = "linux", target_os = "android")))]
pub(crate) fn remove_default_shared(name: &str) -> Result<()> {
    let cname = shm_name(name)?;
    // SAFETY: `cname` is NUL-terminated.
    if unsafe { libc::shm_unlink(cname.as_ptr()) } != 0 {
        return Err(last_error("shm_unlink"));
    }
    Ok(())
}

#[cfg(target_os = "linux")]
pub(crate) fn bind_numa(ptr: *mut u8, len: usize, node: u32) -> Result<()> {
    if len == 0 {
        return Ok(());
    }
    let page = pigeonhole_format::PAGE_SIZE;
    let addr = ptr as usize;
    let start = addr & !(page - 1);
    let span = (addr + len - start).next_multiple_of(page);
    let bits = libc::c_ulong::BITS as usize;
    let mut mask = vec![0 as libc::c_ulong; node as usize / bits + 1];
    mask[node as usize / bits] |= 1 << (node as usize % bits);
    // SAFETY: `[start, start + span)` covers whole pages of a live mapping we own; `mask` is
    // valid for `mask.len() * bits` bits (the kernel reads `maxnode - 1` bits).
    let rc = unsafe {
        libc::syscall(
            libc::SYS_mbind,
            start as *mut libc::c_void,
            span,
            libc::MPOL_BIND,
            mask.as_ptr(),
            mask.len() * bits + 1,
            0 as libc::c_uint,
        )
    };
    if rc != 0 {
        return Err(last_error("mbind"));
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
pub(crate) fn bind_numa(_ptr: *mut u8, _len: usize, _node: u32) -> Result<()> {
    Ok(())
}

/// Start time of `pid` in clock ticks since boot (field 22 of `/proc/<pid>/stat`), or
/// `None` if the process is gone or a zombie.
#[cfg(any(target_os = "linux", target_os = "android"))]
fn proc_start_time(pid: u32) -> Option<u64> {
    let stat = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    // The command name (field 2) is parenthesized and may contain spaces.
    let rest = &stat[stat.rfind(')')? + 1..];
    let mut fields = rest.split_ascii_whitespace();
    let state = fields.next()?; // field 3
    if state == "Z" || state == "X" {
        return None;
    }
    fields.nth(18)?.parse().ok() // field 22
}

#[cfg(any(target_os = "linux", target_os = "android"))]
pub(crate) fn process_liveness(pid: u32) -> Liveness {
    match proc_start_time(pid) {
        Some(t) => Liveness::Alive(Some(t)),
        None => Liveness::Dead,
    }
}

#[cfg(target_vendor = "apple")]
pub(crate) fn process_liveness(pid: u32) -> Liveness {
    let Ok(cpid) = libc::c_int::try_from(pid) else {
        return Liveness::Dead;
    };
    // SAFETY: `proc_bsdinfo` is plain old data; all-zero is valid.
    let mut info: libc::proc_bsdinfo = unsafe { std::mem::zeroed() };
    let size = std::mem::size_of::<libc::proc_bsdinfo>() as libc::c_int;
    // SAFETY: `info` is a valid buffer of `size` bytes for PROC_PIDTBSDINFO.
    let n = unsafe {
        libc::proc_pidinfo(
            cpid,
            libc::PROC_PIDTBSDINFO,
            0,
            (&raw mut info).cast(),
            size,
        )
    };
    if n == size {
        if info.pbi_status == libc::SZOMB {
            return Liveness::Dead;
        }
        return Liveness::Alive(Some(
            info.pbi_start_tvsec * 1_000_000 + info.pbi_start_tvusec,
        ));
    }
    kill_probe(cpid)
}

#[cfg(not(any(target_os = "linux", target_os = "android", target_vendor = "apple")))]
pub(crate) fn process_liveness(pid: u32) -> Liveness {
    match libc::pid_t::try_from(pid) {
        Ok(p) => kill_probe(p),
        Err(_) => Liveness::Dead,
    }
}

/// `kill(pid, 0)`: alive (start time unknown) unless the pid does not exist.
#[cfg(not(any(target_os = "linux", target_os = "android")))]
fn kill_probe(pid: libc::pid_t) -> Liveness {
    if pid <= 0 {
        return Liveness::Dead;
    }
    // SAFETY: signal 0 performs only the existence and permission check.
    if unsafe { libc::kill(pid, 0) } == 0
        || io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
    {
        Liveness::Alive(None)
    } else {
        Liveness::Dead
    }
}

/// The calling process's start time (0 if unknown).
pub(crate) fn current_start_time() -> u64 {
    match process_liveness(std::process::id()) {
        Liveness::Alive(Some(t)) => t,
        _ => 0,
    }
}

pub(crate) fn available_cpus() -> usize {
    std::thread::available_parallelism().map_or(1, |n| n.get())
}

/// The CPU ids this thread may run on, in order.
#[cfg(target_os = "linux")]
fn allowed_cpus() -> Result<Vec<usize>> {
    // SAFETY: `cpu_set_t` is plain old data; all-zero is the empty set.
    let mut set: libc::cpu_set_t = unsafe { std::mem::zeroed() };
    let size = std::mem::size_of::<libc::cpu_set_t>();
    // SAFETY: `set` is a valid buffer of `size` bytes.
    if unsafe { libc::sched_getaffinity(0, size, &mut set) } != 0 {
        return Err(last_error("sched_getaffinity"));
    }
    let mut cpus = Vec::new();
    for cpu in 0..size * 8 {
        // SAFETY: `cpu` is below the set's bit size.
        if unsafe { libc::CPU_ISSET(cpu, &set) } {
            cpus.push(cpu);
        }
    }
    Ok(cpus)
}

#[cfg(target_os = "linux")]
pub(crate) fn pin_current_thread(cpu: usize) -> Result<()> {
    let id = *allowed_cpus()?
        .get(cpu)
        .ok_or(Error::new(ErrorKind::Other, "cpu index out of range"))?;
    // SAFETY: `cpu_set_t` is plain old data; all-zero is the empty set.
    let mut set: libc::cpu_set_t = unsafe { std::mem::zeroed() };
    // SAFETY: `id` came from the set's own bit range.
    unsafe { libc::CPU_SET(id, &mut set) };
    // SAFETY: `set` is a valid, initialized `cpu_set_t`.
    if unsafe { libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set) } != 0 {
        return Err(last_error("sched_setaffinity"));
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
pub(crate) fn pin_current_thread(_cpu: usize) -> Result<()> {
    Err(Error::new(
        ErrorKind::Unsupported,
        "thread affinity is not available on this platform",
    ))
}

#[cfg(target_os = "linux")]
pub(crate) fn numa_node_of(cpu: usize) -> Option<u32> {
    let nodes = fs::read_dir("/sys/devices/system/node")
        .ok()?
        .filter_map(|e| e.ok())
        .filter(|e| node_number(&e.file_name()).is_some())
        .count();
    if nodes <= 1 {
        return None;
    }
    let id = *allowed_cpus().ok()?.get(cpu)?;
    fs::read_dir(format!("/sys/devices/system/cpu/cpu{id}"))
        .ok()?
        .filter_map(|e| e.ok())
        .find_map(|e| node_number(&e.file_name()))
}

#[cfg(target_os = "linux")]
fn node_number(name: &std::ffi::OsStr) -> Option<u32> {
    name.to_str()?.strip_prefix("node")?.parse().ok()
}

#[cfg(not(target_os = "linux"))]
pub(crate) fn numa_node_of(_cpu: usize) -> Option<u32> {
    None
}

#[cfg(all(test, not(miri)))]
mod tests {
    use super::*;

    #[test]
    fn own_process_is_alive_and_has_stable_start() {
        let pid = std::process::id();
        let start = current_start_time();
        assert!(super::super::process_alive(pid, start));
        assert_eq!(current_start_time(), start);
        if start != 0 {
            assert!(!super::super::process_alive(pid, start + 1));
        }
    }
}
