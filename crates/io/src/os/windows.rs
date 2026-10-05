use std::fs;
use std::io;
use std::os::windows::ffi::OsStrExt;
use std::os::windows::fs::FileExt;
use std::os::windows::io::AsRawHandle;
use std::path::Path;
use std::ptr::NonNull;

use windows_sys::Win32::Foundation::{
    CloseHandle, ERROR_ACCESS_DENIED, ERROR_ALREADY_EXISTS, ERROR_LOCK_VIOLATION, FILETIME,
    GetLastError, HANDLE, INVALID_HANDLE_VALUE, STILL_ACTIVE,
};
use windows_sys::Win32::Storage::FileSystem::{
    BY_HANDLE_FILE_INFORMATION, FILE_NAME_NORMALIZED, GetDriveTypeW, GetFileInformationByHandle,
    GetFinalPathNameByHandleW, LOCKFILE_EXCLUSIVE_LOCK, LOCKFILE_FAIL_IMMEDIATELY, LockFileEx,
    UnlockFileEx, VOLUME_NAME_DOS,
};
use windows_sys::Win32::System::IO::OVERLAPPED;
use windows_sys::Win32::System::Memory::{
    CreateFileMappingW, FILE_MAP_ALL_ACCESS, MEMORY_MAPPED_VIEW_ADDRESS, MapViewOfFile,
    OpenFileMappingW, PAGE_READWRITE, UnmapViewOfFile,
};
use windows_sys::Win32::System::Threading::{
    GetCurrentProcess, GetCurrentThread, GetExitCodeProcess, GetProcessAffinityMask,
    GetProcessTimes, OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION, SetThreadAffinityMask,
};

use super::Liveness;
use crate::{Error, ErrorKind, FileIdentity, LockMode, Result, SharedOpen};

/// `GetDriveTypeW` result for a network drive (WindowsProgramming, a feature we don't enable).
const DRIVE_REMOTE: u32 = 4;

fn handle(file: &fs::File) -> HANDLE {
    file.as_raw_handle()
}

fn last_error(context: &'static str) -> Error {
    Error::os(context, io::Error::last_os_error())
}

pub(crate) fn read_exact_at(
    file: &fs::File,
    mut buf: &mut [u8],
    mut offset: u64,
) -> io::Result<()> {
    while !buf.is_empty() {
        match file.seek_read(buf, offset) {
            Ok(0) => return Err(io::ErrorKind::UnexpectedEof.into()),
            Ok(n) => {
                buf = &mut buf[n..];
                offset += n as u64;
            }
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

pub(crate) fn write_all_at(file: &fs::File, mut buf: &[u8], mut offset: u64) -> io::Result<()> {
    while !buf.is_empty() {
        match file.seek_write(buf, offset) {
            Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
            Ok(n) => {
                buf = &buf[n..];
                offset += n as u64;
            }
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

pub(crate) fn identity(file: &fs::File) -> Result<FileIdentity> {
    // SAFETY: plain old data; all-zero is valid.
    let mut info: BY_HANDLE_FILE_INFORMATION = unsafe { std::mem::zeroed() };
    // SAFETY: the handle is open for the life of `file`; `info` is a valid out pointer.
    if unsafe { GetFileInformationByHandle(handle(file), &mut info) } == 0 {
        return Err(last_error("GetFileInformationByHandle"));
    }
    Ok(FileIdentity {
        device: u64::from(info.dwVolumeSerialNumber),
        inode: (u64::from(info.nFileIndexHigh) << 32) | u64::from(info.nFileIndexLow),
    })
}

pub(crate) fn is_local(file: &fs::File) -> Result<bool> {
    let mut buf = vec![0u16; 1024];
    loop {
        // SAFETY: `buf` is valid for `buf.len()` UTF-16 units.
        let n = unsafe {
            GetFinalPathNameByHandleW(
                handle(file),
                buf.as_mut_ptr(),
                buf.len() as u32,
                FILE_NAME_NORMALIZED | VOLUME_NAME_DOS,
            )
        } as usize;
        if n == 0 {
            return Err(last_error("GetFinalPathNameByHandleW"));
        }
        if n < buf.len() {
            buf.truncate(n);
            break;
        }
        buf.resize(n + 1, 0);
    }
    let path = String::from_utf16_lossy(&buf);
    if path.starts_with(r"\\?\UNC\") || path.starts_with(r"\\") && !path.starts_with(r"\\?\") {
        return Ok(false);
    }
    // `\\?\C:\...` → `C:\`
    let rest = path.strip_prefix(r"\\?\").unwrap_or(&path);
    let bytes = rest.as_bytes();
    if bytes.len() >= 2 && bytes[1] == b':' {
        let root: Vec<u16> = rest[..2]
            .encode_utf16()
            .chain([u16::from(b'\\'), 0])
            .collect();
        // SAFETY: `root` is a NUL-terminated UTF-16 string.
        return Ok(unsafe { GetDriveTypeW(root.as_ptr()) } != DRIVE_REMOTE);
    }
    Ok(true)
}

pub(crate) fn allocate(file: &fs::File, offset: u64, len: u64) -> Result<()> {
    let end = offset
        .checked_add(len)
        .ok_or(Error::new(ErrorKind::Other, "allocate: range overflows"))?;
    let cur = file.metadata().map_err(|e| Error::os("stat", e))?.len();
    if end > cur {
        file.set_len(end).map_err(|e| Error::os("allocate", e))?;
    }
    Ok(())
}

/// Windows cannot flush a directory handle; NTFS journals directory entries itself.
pub(crate) fn sync_dir(dir: &Path) -> Result<()> {
    if dir.is_dir() {
        Ok(())
    } else {
        Err(Error::new(ErrorKind::NotFound, "sync directory"))
    }
}

fn overlapped(byte: u64) -> OVERLAPPED {
    let mut ov = OVERLAPPED::default();
    ov.Anonymous.Anonymous.Offset = byte as u32;
    ov.Anonymous.Anonymous.OffsetHigh = (byte >> 32) as u32;
    ov
}

/// Takes a lock on the single byte `byte` through this handle, never blocking.
pub(crate) fn lock_range(file: &fs::File, byte: u64, mode: LockMode) -> Result<()> {
    let mut flags = LOCKFILE_FAIL_IMMEDIATELY;
    if mode == LockMode::Exclusive {
        flags |= LOCKFILE_EXCLUSIVE_LOCK;
    }
    let mut ov = overlapped(byte);
    // SAFETY: `ov` is a valid OVERLAPPED naming the byte; the handle is synchronous, so the
    // call completes before returning and `ov` outlives it.
    if unsafe { LockFileEx(handle(file), flags, 0, 1, 0, &mut ov) } != 0 {
        return Ok(());
    }
    let err = io::Error::last_os_error();
    if err.raw_os_error() == Some(ERROR_LOCK_VIOLATION as i32) {
        return Err(Error {
            kind: ErrorKind::Locked,
            context: "lock",
            source: Some(err),
        });
    }
    Err(Error::os("lock", err))
}

/// Releases one lock on `byte` held through this handle (the exclusive one first, if both).
pub(crate) fn unlock_range(file: &fs::File, byte: u64) -> Result<()> {
    let mut ov = overlapped(byte);
    // SAFETY: as in `lock_range`.
    if unsafe { UnlockFileEx(handle(file), 0, 1, 0, &mut ov) } == 0 {
        return Err(last_error("unlock"));
    }
    Ok(())
}

/// A mapped view plus its file-mapping handle; unmapped and closed on drop.
pub(crate) struct Mapping {
    view: MEMORY_MAPPED_VIEW_ADDRESS,
    mapping: HANDLE,
    len: usize,
}

impl std::fmt::Debug for Mapping {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Mapping")
            .field("view", &self.view.Value)
            .field("len", &self.len)
            .finish()
    }
}

impl Mapping {
    pub(crate) fn ptr(&self) -> NonNull<u8> {
        NonNull::new(self.view.Value.cast()).expect("mapped view is non-null")
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }
}

impl Drop for Mapping {
    fn drop(&mut self) {
        // SAFETY: `view` and `mapping` were created by `map_view` and are released once, here,
        // after every `SharedRegion` clone that could reach them is gone.
        unsafe {
            UnmapViewOfFile(self.view);
            CloseHandle(self.mapping);
        }
    }
}

/// Maps `len` bytes of the file-mapping object `mapping`, taking ownership of the handle.
fn map_view(mapping: HANDLE, len: usize) -> Result<Mapping> {
    // SAFETY: `mapping` is a live file-mapping handle we own.
    let view = unsafe { MapViewOfFile(mapping, FILE_MAP_ALL_ACCESS, 0, 0, len) };
    if view.Value.is_null() {
        let err = last_error("MapViewOfFile");
        // SAFETY: we own the handle and release it once.
        unsafe { CloseHandle(mapping) };
        return Err(err);
    }
    Ok(Mapping { view, mapping, len })
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
    // SAFETY: a file-backed mapping of a handle we own, sized to the file, unnamed.
    let mapping = unsafe {
        CreateFileMappingW(
            handle(file),
            std::ptr::null(),
            PAGE_READWRITE,
            0,
            0,
            std::ptr::null(),
        )
    };
    if mapping.is_null() {
        return Err(last_error("CreateFileMappingW"));
    }
    map_view(mapping, len)
}

/// Creates or opens `path` for a file-backed region and sizes it on creation.
pub(crate) fn open_region_file(path: &Path, len: u64, mode: SharedOpen) -> Result<fs::File> {
    let mut opts = fs::OpenOptions::new();
    opts.read(true).write(true);
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

fn wide_name(name: &str) -> Vec<u16> {
    std::ffi::OsStr::new(&format!(r"Local\{name}"))
        .encode_wide()
        .chain([0])
        .collect()
}

/// Opens a pagefile-backed named mapping `Local\<name>`.
pub(crate) fn open_default_shared(name: &str, len: u64, mode: SharedOpen) -> Result<Mapping> {
    let wide = wide_name(name);
    let size =
        usize::try_from(len).map_err(|_| Error::new(ErrorKind::Other, "region too large"))?;
    if size == 0 {
        return Err(Error::new(ErrorKind::Other, "shared region is empty"));
    }
    let mapping = match mode {
        SharedOpen::CreateNew => {
            // SAFETY: `wide` is NUL-terminated; INVALID_HANDLE_VALUE requests pagefile backing.
            let h = unsafe {
                CreateFileMappingW(
                    INVALID_HANDLE_VALUE,
                    std::ptr::null(),
                    PAGE_READWRITE,
                    (len >> 32) as u32,
                    len as u32,
                    wide.as_ptr(),
                )
            };
            if h.is_null() {
                return Err(last_error("CreateFileMappingW"));
            }
            // SAFETY: reads the calling thread's last-error value set by the call above.
            if unsafe { GetLastError() } == ERROR_ALREADY_EXISTS {
                // SAFETY: we own the handle and release it once.
                unsafe { CloseHandle(h) };
                return Err(Error::new(ErrorKind::AlreadyExists, "open shared region"));
            }
            h
        }
        SharedOpen::Attach => {
            // SAFETY: `wide` is NUL-terminated.
            let h = unsafe { OpenFileMappingW(FILE_MAP_ALL_ACCESS, 0, wide.as_ptr()) };
            if h.is_null() {
                return Err(last_error("OpenFileMappingW"));
            }
            h
        }
    };
    map_view(mapping, size)
}

/// Named mappings vanish when their last handle closes; there is no name to remove.
pub(crate) fn remove_default_shared(_name: &str) -> Result<()> {
    Ok(())
}

pub(crate) fn bind_numa(_ptr: *mut u8, _len: usize, _node: u32) -> Result<()> {
    Ok(())
}

fn filetime(t: FILETIME) -> u64 {
    (u64::from(t.dwHighDateTime) << 32) | u64::from(t.dwLowDateTime)
}

/// Creation time of the process behind `h`.
fn creation_time(h: HANDLE) -> Option<u64> {
    let mut times = [FILETIME {
        dwLowDateTime: 0,
        dwHighDateTime: 0,
    }; 4];
    let [c, e, k, u] = &mut times;
    // SAFETY: `h` is a live process handle; all four out pointers are valid.
    if unsafe { GetProcessTimes(h, c, e, k, u) } == 0 {
        return None;
    }
    Some(filetime(times[0]))
}

pub(crate) fn process_liveness(pid: u32) -> Liveness {
    // SAFETY: plain call; the returned handle is closed below.
    let h = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid) };
    if h.is_null() {
        // SAFETY: reads the calling thread's last-error value set by `OpenProcess`.
        return if unsafe { GetLastError() } == ERROR_ACCESS_DENIED {
            Liveness::Alive(None)
        } else {
            Liveness::Dead
        };
    }
    let mut code = 0u32;
    // SAFETY: `h` is a live process handle; `code` is a valid out pointer.
    let ok = unsafe { GetExitCodeProcess(h, &mut code) } != 0;
    let start = creation_time(h);
    // SAFETY: we own `h` and close it once.
    unsafe { CloseHandle(h) };
    if ok && code != STILL_ACTIVE as u32 {
        return Liveness::Dead;
    }
    Liveness::Alive(start)
}

/// The calling process's start time (0 if unknown).
pub(crate) fn current_start_time() -> u64 {
    // SAFETY: the pseudo-handle for the current process needs no closing.
    creation_time(unsafe { GetCurrentProcess() }).unwrap_or(0)
}

pub(crate) fn available_cpus() -> usize {
    std::thread::available_parallelism().map_or(1, |n| n.get())
}

pub(crate) fn pin_current_thread(cpu: usize) -> Result<()> {
    let (mut process_mask, mut system_mask) = (0usize, 0usize);
    // SAFETY: the current-process pseudo-handle and two valid out pointers.
    if unsafe { GetProcessAffinityMask(GetCurrentProcess(), &mut process_mask, &mut system_mask) }
        == 0
    {
        return Err(last_error("GetProcessAffinityMask"));
    }
    let bit = (0..usize::BITS)
        .filter(|b| process_mask & (1 << b) != 0)
        .nth(cpu)
        .ok_or(Error::new(ErrorKind::Other, "cpu index out of range"))?;
    // SAFETY: the current-thread pseudo-handle and a mask within the process mask.
    if unsafe { SetThreadAffinityMask(GetCurrentThread(), 1 << bit) } == 0 {
        return Err(last_error("SetThreadAffinityMask"));
    }
    Ok(())
}

pub(crate) fn numa_node_of(_cpu: usize) -> Option<u32> {
    None
}
