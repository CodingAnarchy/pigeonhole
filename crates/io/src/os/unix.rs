use std::fs;
use std::io;
use std::os::fd::AsRawFd;
use std::os::unix::fs::{FileExt, MetadataExt};
use std::path::Path;
use std::ptr::NonNull;

use super::Liveness;
use crate::{Error, ErrorKind, FileIdentity, Locality, LockMode, Result, SharedOpen};

pub(crate) fn read_exact_at(file: &fs::File, buf: &mut [u8], offset: u64) -> io::Result<()> {
    file.read_exact_at(buf, offset)
}

/// Reads `buf` at `offset` only if the page cache holds all of it, without waiting on a
/// device (`preadv2` with `RWF_NOWAIT`, D292).
#[cfg(target_os = "linux")]
pub(crate) fn read_cached_at(
    file: &fs::File,
    buf: &mut [u8],
    offset: u64,
) -> crate::nowait::Cached {
    use crate::nowait::Cached;
    use std::os::fd::AsRawFd;
    let Ok(off) = libc::off_t::try_from(offset) else {
        return Cached::Miss;
    };
    let iov = libc::iovec {
        iov_base: buf.as_mut_ptr().cast(),
        iov_len: buf.len(),
    };
    // SAFETY: one iovec over `buf`, valid for writes of its length; `file` keeps the
    // descriptor open for the call.
    let n = unsafe { libc::preadv2(file.as_raw_fd(), &iov, 1, off, libc::RWF_NOWAIT) };
    if usize::try_from(n) == Ok(buf.len()) {
        return Cached::Hit;
    }
    if n < 0
        && matches!(
            io::Error::last_os_error().raw_os_error(),
            Some(libc::EOPNOTSUPP | libc::EINVAL)
        )
    {
        return Cached::Refused;
    }
    Cached::Miss
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
pub(crate) fn locality(file: &fs::File) -> Result<Locality> {
    /// FUSE's filesystem magic.
    const FUSE: u32 = 0x6573_5546;
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
        0x4750_4653, // GPFS (IBM Spectrum Scale)
    ];
    // SAFETY: `statfs` is plain old data; all-zero is a valid value.
    let mut st: libc::statfs = unsafe { std::mem::zeroed() };
    // SAFETY: the fd is open for the life of `file`; `st` is a valid out pointer.
    if unsafe { libc::fstatfs(file.as_raw_fd(), &mut st) } != 0 {
        return Err(last_error("fstatfs"));
    }
    #[allow(clippy::unnecessary_cast)] // `f_type` is a different integer type per target.
    let magic = st.f_type as u32;
    // FUSE (sshfs, s3fs, gcsfuse, JuiceFS, rclone, but also local ntfs-3g): its byte-range
    // locks are often local to one host, so two hosts could both be writers (#147, D173).
    Ok(if magic == FUSE {
        Locality::Fuse
    } else if REMOTE.contains(&magic) {
        Locality::Network
    } else {
        Locality::Local
    })
}

#[cfg(target_vendor = "apple")]
pub(crate) fn locality(file: &fs::File) -> Result<Locality> {
    // SAFETY: `statfs` is plain old data; all-zero is a valid value.
    let mut st: libc::statfs = unsafe { std::mem::zeroed() };
    // SAFETY: the fd is open for the life of `file`; `st` is a valid out pointer.
    if unsafe { libc::fstatfs(file.as_raw_fd(), &mut st) } != 0 {
        return Err(last_error("fstatfs"));
    }
    if st.f_flags & libc::MNT_LOCAL as u32 != 0 {
        return Ok(Locality::Local);
    }
    // A non-local macFUSE mount names its type `macfuse` (or `osxfuse` before macFUSE 4).
    let name: Vec<u8> = st
        .f_fstypename
        .iter()
        .take_while(|&&c| c != 0)
        .map(|&c| c as u8)
        .collect();
    Ok(
        if name.starts_with(b"macfuse") || name.starts_with(b"osxfuse") {
            Locality::Fuse
        } else {
            Locality::Network
        },
    )
}

#[cfg(not(any(target_os = "linux", target_os = "android", target_vendor = "apple")))]
pub(crate) fn locality(_file: &fs::File) -> Result<Locality> {
    // No portable way to ask; assume local.
    Ok(Locality::Local)
}

#[cfg(any(target_os = "linux", target_os = "android"))]
pub(crate) fn allocate(file: &fs::File, offset: u64, len: u64) -> Result<()> {
    let start = off(offset, "allocate: offset too large")?;
    let count = off(len, "allocate: length too large")?;
    if count == 0 {
        // fallocate rejects a zero length with EINVAL; keep the cross-backend semantics
        // (extend to `offset` if the file is shorter) without the syscall.
        return extend(file, offset, len);
    }
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
    if mode == SharedOpen::CreateNew
        && let Err(e) = size_region_file(&file, len)
    {
        let _ = fs::remove_file(path);
        return Err(e);
    }
    Ok(file)
}

/// Sizes a new region file to `len`, sparsely: memory (on tmpfs) or disk is used only as the
/// memtables touch it. First checks that the filesystem has `len` bytes free, so a region that
/// cannot fit (Docker's 64 MiB `/dev/shm`) fails here with `NoSpace` instead of raising
/// `SIGBUS` on a later store into the mapping. Space another process takes after this check
/// can still cause that; reserving instead would commit the whole region's memory at open.
fn size_region_file(file: &fs::File, len: u64) -> Result<()> {
    // SAFETY: `statvfs` is plain old data, valid when zeroed.
    let mut st: libc::statvfs = unsafe { std::mem::zeroed() };
    // SAFETY: plain syscall on an fd we own, writing into `st`, which outlives the call.
    if unsafe { libc::fstatvfs(file.as_raw_fd(), &mut st) } != 0 {
        return Err(last_error("statvfs shared region"));
    }
    if (st.f_bavail as u128) * (st.f_frsize as u128) < u128::from(len) {
        return Err(Error::new(
            ErrorKind::NoSpace,
            "not enough free space for the shared region",
        ));
    }
    file.set_len(len)
        .map_err(|e| Error::os("size shared region", e))
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
        // A POSIX shared-memory object here (macOS, the BSDs) is anonymous memory backed by
        // swap, not a size-capped filesystem, so there is no free space to check: `ftruncate`
        // is all it takes.
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

/// The kernel's page size (`sysconf(_SC_PAGESIZE)`: 4 KiB, or 16 or 64 KiB on some arm64
/// kernels), which `madvise` and `mincore` work in; `pigeonhole_format::PAGE_SIZE` is the
/// file's page.
#[cfg(target_os = "linux")]
fn system_page_size() -> usize {
    // SAFETY: plain syscall with a constant name.
    let n = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    usize::try_from(n)
        .ok()
        .filter(|n| n.is_power_of_two())
        .unwrap_or(4096)
}

/// The whole pages inside `[ptr, ptr + len)`: the start rounded up and the end rounded down
/// to a page, or `None` when no whole page lies inside.
#[cfg(target_os = "linux")]
fn whole_pages(ptr: *mut u8, len: usize) -> Option<(usize, usize)> {
    let page = system_page_size();
    let start = (ptr as usize).next_multiple_of(page);
    let end = (ptr as usize + len) & !(page - 1);
    (start < end).then_some((start, end - start))
}

/// Frees the pages of `[ptr, ptr + len)` and their backing store (`madvise(MADV_REMOVE)`:
/// a hole in the region's file; a later touch reads zeros). Whole pages only: a partial
/// page at either end stays. The mapping stays valid.
#[cfg(target_os = "linux")]
pub(crate) fn discard(ptr: *mut u8, len: usize) -> Result<()> {
    let Some((start, span)) = whole_pages(ptr, len) else {
        return Ok(());
    };
    // SAFETY: `[start, start + span)` covers whole pages of a live mapping we own; the
    // pages stay mapped (`MADV_REMOVE` never unmaps), so no pointer into them dangles.
    let rc = unsafe { libc::madvise(start as *mut libc::c_void, span, libc::MADV_REMOVE) };
    if rc != 0 {
        return Err(last_error("madvise"));
    }
    Ok(())
}

/// How many bytes of the whole pages inside `[ptr, ptr + len)` are resident (`mincore`).
#[cfg(target_os = "linux")]
pub(crate) fn resident_bytes(ptr: *mut u8, len: usize) -> Result<usize> {
    let Some((start, span)) = whole_pages(ptr, len) else {
        return Ok(0);
    };
    let page = system_page_size();
    let mut vec = vec![0u8; span / page];
    // SAFETY: `[start, start + span)` covers whole pages of a live mapping we own, and `vec`
    // holds one byte per page of it.
    let rc = unsafe { libc::mincore(start as *mut libc::c_void, span, vec.as_mut_ptr()) };
    if rc != 0 {
        return Err(last_error("mincore"));
    }
    Ok(vec.iter().filter(|b| **b & 1 != 0).count() * page)
}

#[cfg(not(target_os = "linux"))]
pub(crate) fn discard(_ptr: *mut u8, _len: usize) -> Result<()> {
    // A POSIX shared-memory object's pages cannot be punched (`madvise(MADV_FREE)` acts on
    // anonymous memory, and the io crate keeps no descriptor for `F_PUNCHHOLE`).
    Err(Error::new(
        ErrorKind::Unsupported,
        "discarding shared-memory pages is Linux only",
    ))
}

#[cfg(not(target_os = "linux"))]
pub(crate) fn resident_bytes(_ptr: *mut u8, _len: usize) -> Result<usize> {
    Err(Error::new(
        ErrorKind::Unsupported,
        "counting resident shared-memory pages is Linux only",
    ))
}

/// What `/proc/<pid>/stat` says: `Some(Alive(start))` with the start time in clock ticks
/// since boot (field 22), `Some(Dead)` for a zombie, or `None` if it can't be read: the
/// process is gone, or `/proc` hides it (`hidepid`, another user's process).
#[cfg(any(target_os = "linux", target_os = "android"))]
fn proc_liveness(pid: u32) -> Option<Liveness> {
    let stat = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    // The command name (field 2) is parenthesized and may contain spaces.
    let rest = &stat[stat.rfind(')')? + 1..];
    let mut fields = rest.split_ascii_whitespace();
    let state = fields.next()?; // field 3
    if state == "Z" || state == "X" {
        return Some(Liveness::Dead);
    }
    Some(Liveness::Alive(Some(fields.nth(18)?.parse().ok()?))) // field 22
}

/// `/proc` first, for the start time (which catches a reused pid); where `/proc` can't show
/// the process, `kill(pid, 0)`. Under `hidepid` another user's live reader is invisible in
/// `/proc`, and reading that as dead would free a live reader's slot (#834 review).
#[cfg(any(target_os = "linux", target_os = "android"))]
pub(crate) fn process_liveness(pid: u32) -> Liveness {
    if let Some(l) = proc_liveness(pid) {
        return l;
    }
    match libc::pid_t::try_from(pid) {
        Ok(p) => kill_probe(p),
        Err(_) => Liveness::Dead,
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

/// `kill(pid, 0)`: alive (start time unknown) unless the pid does not exist. `EPERM` (a
/// process of another user) is alive.
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

#[cfg(target_os = "linux")]
pub(crate) fn physical_cores() -> Option<usize> {
    let first_of_core = first_of_core()?;
    Some(first_of_core.iter().filter(|&&first| first).count())
}

#[cfg(target_os = "macos")]
pub(crate) fn physical_cores() -> Option<usize> {
    let mut cores: libc::c_int = 0;
    let mut len = std::mem::size_of::<libc::c_int>();
    // SAFETY: a NUL-terminated name, and `cores` is a valid buffer of `len` bytes.
    let ok = unsafe {
        libc::sysctlbyname(
            c"hw.physicalcpu".as_ptr(),
            (&raw mut cores).cast(),
            &mut len,
            std::ptr::null_mut(),
            0,
        )
    } == 0;
    usize::try_from(cores).ok().filter(|&n| ok && n > 0)
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub(crate) fn physical_cores() -> Option<usize> {
    None
}

#[cfg(target_os = "linux")]
pub(crate) fn spread_cpu(i: usize) -> usize {
    match first_of_core() {
        Some(first_of_core) => super::spread(&first_of_core, i),
        None => i % available_cpus().max(1),
    }
}

#[cfg(not(target_os = "linux"))]
pub(crate) fn spread_cpu(i: usize) -> usize {
    i % available_cpus().max(1)
}

/// For each CPU this thread may run on (in order), whether it is the first of its physical
/// core among them. A core is named by the first CPU of its sysfs sibling list
/// (`core_cpus_list`, or `thread_siblings_list` before Linux 5.6), which, unlike `core_id`,
/// is unique across packages, dies and clusters. `None` if sysfs does not say.
#[cfg(target_os = "linux")]
fn first_of_core() -> Option<Vec<bool>> {
    let cores = allowed_cpus()
        .ok()?
        .into_iter()
        .map(|cpu| {
            let topology = format!("/sys/devices/system/cpu/cpu{cpu}/topology/");
            let list = fs::read_to_string(topology.clone() + "core_cpus_list")
                .or_else(|_| fs::read_to_string(topology + "thread_siblings_list"))
                .ok()?;
            super::first_cpu_of_list(&list)
        })
        .collect::<Option<Vec<usize>>>()?;
    Some(super::first_of_core(&cores))
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

/// The CPU ids this process may run on, in pinning order: one per physical core first, then
/// their SMT siblings (D220). `None` where threads cannot be hard-pinned.
#[cfg(target_os = "linux")]
pub(crate) fn pin_order() -> Option<Vec<usize>> {
    let ids = allowed_cpus().ok()?;
    let order = match first_of_core() {
        Some(first) => (0..ids.len()).map(|i| super::spread(&first, i)).collect(),
        None => (0..ids.len()).collect::<Vec<_>>(),
    };
    Some(order.into_iter().map(|k| ids[k]).collect())
}

/// Pins the calling thread to CPU id `id` (one of [`pin_order`]'s).
#[cfg(target_os = "linux")]
pub(crate) fn pin_current_thread_to(id: usize) -> Result<()> {
    if id >= 8 * std::mem::size_of::<libc::cpu_set_t>() {
        return Err(Error::new(ErrorKind::Other, "cpu id out of range"));
    }
    // SAFETY: `cpu_set_t` is plain old data; all-zero is the empty set.
    let mut set: libc::cpu_set_t = unsafe { std::mem::zeroed() };
    // SAFETY: `id` is below the set's bit size (checked above).
    unsafe { libc::CPU_SET(id, &mut set) };
    // SAFETY: `set` is a valid, initialized `cpu_set_t`.
    if unsafe { libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set) } != 0 {
        return Err(last_error("sched_setaffinity"));
    }
    Ok(())
}

/// macOS and the other Unixes: no hard thread affinity (macOS affinity tags are hints).
#[cfg(not(target_os = "linux"))]
pub(crate) fn pin_order() -> Option<Vec<usize>> {
    None
}

#[cfg(not(target_os = "linux"))]
pub(crate) fn pin_current_thread_to(_id: usize) -> Result<()> {
    Err(Error::new(
        ErrorKind::Unsupported,
        "thread affinity is not available on this platform",
    ))
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

/// Asks for direct I/O (#403) when opening: `O_DIRECT` on Linux. macOS sets `F_NOCACHE` after
/// the open instead ([`after_open_direct`]).
pub(crate) fn open_direct(o: &mut fs::OpenOptions) {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::fs::OpenOptionsExt;
        o.custom_flags(libc::O_DIRECT);
    }
    #[cfg(not(target_os = "linux"))]
    let _ = o;
}

/// Finishes opening `file` for direct I/O: `F_NOCACHE` on macOS; nothing elsewhere.
pub(crate) fn after_open_direct(file: &fs::File) -> io::Result<()> {
    #[cfg(target_os = "macos")]
    {
        // SAFETY: a plain fcntl on a descriptor `file` owns.
        if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_NOCACHE, 1) } == -1 {
            return Err(io::Error::last_os_error());
        }
    }
    #[cfg(not(target_os = "macos"))]
    let _ = file;
    Ok(())
}

/// The alignment direct I/O on `file` needs: what `statx` reports (`STATX_DIOALIGN`, Linux
/// 6.1+), else 4096 (the common device and page size, and what the simulator enforces).
pub(crate) fn direct_align(file: &fs::File) -> usize {
    #[cfg(target_os = "linux")]
    {
        // SAFETY: zeroed `statx` is a valid out-parameter.
        let mut stx: libc::statx = unsafe { std::mem::zeroed() };
        // SAFETY: an empty path with AT_EMPTY_PATH stats the descriptor itself, writing into
        // a live `statx`.
        let r = unsafe {
            libc::statx(
                file.as_raw_fd(),
                c"".as_ptr(),
                libc::AT_EMPTY_PATH,
                libc::STATX_DIOALIGN,
                &mut stx,
            )
        };
        if r == 0 && stx.stx_mask & libc::STATX_DIOALIGN != 0 && stx.stx_dio_offset_align > 0 {
            return (stx.stx_dio_offset_align as usize).max(stx.stx_dio_mem_align as usize);
        }
    }
    #[cfg(not(target_os = "linux"))]
    let _ = file;
    4096
}

#[cfg(all(test, not(miri)))]
mod liveness_tests {
    use super::*;

    /// #834 review: `kill(pid, 0)` failing with `EPERM` (another user's process, which
    /// `hidepid` also hides from `/proc`) is alive. Pid 1 is root's: any other user gets
    /// `EPERM`, root gets success; alive either way.
    #[test]
    fn kill_probe_reads_eperm_as_alive() {
        assert_eq!(kill_probe(1), Liveness::Alive(None));
        assert_eq!(kill_probe(0), Liveness::Dead);
    }

    #[test]
    fn a_reaped_process_is_dead() {
        let mut child = std::process::Command::new("true").spawn().unwrap();
        let pid = child.id();
        child.wait().unwrap();
        // The pid could be reused in between; not within a test's few microseconds.
        assert_eq!(kill_probe(pid as libc::pid_t), Liveness::Dead);
        assert_eq!(process_liveness(pid), Liveness::Dead);
        assert!(matches!(
            process_liveness(std::process::id()),
            Liveness::Alive(_)
        ));
    }
}

/// The cached process id ([`crate::sys::process_id`]); 0 until first read and in a child
/// right after `fork`.
static PROCESS_ID: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);

/// `pthread_atfork` child handler: runs in the child, on the forking thread, before `fork`
/// returns there. A single atomic store, which is async-signal-safe.
extern "C" fn forked_child() {
    PROCESS_ID.store(0, std::sync::atomic::Ordering::Relaxed);
}

/// The cached id: one load on the fast path; the first call (and the first in a forked
/// child) takes [`process_id_slow`].
#[inline]
pub(crate) fn process_id() -> u32 {
    let id = PROCESS_ID.load(std::sync::atomic::Ordering::Relaxed);
    if id != 0 {
        return id;
    }
    process_id_slow()
}

#[cold]
#[inline(never)]
fn process_id_slow() -> u32 {
    use std::sync::atomic::Ordering;
    static HANDLER: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    // SAFETY: registers a handler that only stores to a static atomic.
    let registered = *HANDLER
        .get_or_init(|| unsafe { libc::pthread_atfork(None, None, Some(forked_child)) } == 0);
    let id = std::process::id();
    // Cached only with the handler registered, so a later fork always resets it; if
    // registration failed (`ENOMEM`), every call asks the system.
    if registered {
        PROCESS_ID.store(id, Ordering::Relaxed);
    }
    id
}
