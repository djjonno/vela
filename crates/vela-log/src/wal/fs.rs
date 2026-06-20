//! Filesystem seam for real I/O and deterministic fault injection.
//!
//! The WAL never touches `std::fs` directly. Instead it goes through the
//! [`FileSystem`] trait and its [`WalFile`] handle, so production uses a thin
//! [`RealFileSystem`] over `std::fs` while tests use an in-memory
//! [`fault::MemFileSystem`] that can deterministically reproduce crash, torn
//! write, fsync-failure, and locked/missing-directory scenarios without real
//! process kills (Requirements 5.5, 6.1, 10.2, 10.3). This seam is internal to
//! `wal` and is **not** part of the [`crate::LogStorage`] trait.
//!
//! # Design
//!
//! The trait is **generic-friendly with an associated handle type** rather than
//! object-safe: `DurableWal<F: FileSystem>` (the design's `mod.rs`) carries the
//! concrete filesystem as a type parameter, so the index, segments, and
//! manifest can hold `F::File` handles with no dynamic dispatch.
//!
//! All [`WalFile`] methods take `&self` (mirroring `std::fs::File`, whose
//! positional `read_at`/`write_at`, `sync_all`, `sync_data`, `set_len`, and
//! `metadata` are all `&self`). This is what lets the disk-backed read path
//! (`entry`/`read`/`snapshot`, which take `&self` on `DurableWal`) read payload
//! bytes from a segment without an exclusive borrow.
//!
//! Positional reads and writes are used everywhere (no implicit file cursor):
//! segment appends write at the tracked end offset, manifest slots write at
//! fixed offsets, and payload reads read at a recorded `(offset, len)`.
//!
//! # Operations
//!
//! open (`open_read`/`open_read_write`) · read (`read_at`) · write (`write_at`)
//! · fsync (`sync_all`/`sync_data`) · fsync_dir (`sync_dir`) · `rename` ·
//! remove (`remove_file`) · lock (`lock_exclusive`), plus directory listing
//! (`read_dir`) and creation (`create_dir_all`) needed by recovery and open.

// `allow(dead_code)`: the production [`RealFileSystem`], the [`FileSystem`] /
// [`WalFile`] traits, and several handle methods are reachable only once later
// tasks consume them — config + directory lock (task 4), segment files
// (task 5), the manifest (task 6), `DurableWal::open`/`append` (task 8),
// disk-backed reads (task 9), and recovery (task 12). Until the `wal` subtree is
// wired into a live root they have no in-crate caller on the library target and
// would otherwise trip the `dead_code` lint under `-D warnings`. The allow is
// removed as those tasks land their call sites, mirroring how `frame.rs` scopes
// the same allow.
#![allow(dead_code)]

use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};

use fs4::FileExt;

// The WAL filesystem seam (`FileSystem` and its `WalFile` handle) is
// crate-private in production. The deterministic-simulation harness, however,
// needs to name and inject it, so it is re-exported through `vela_log::sim`
// behind the non-default `sim` feature. To expose it there without changing
// production privacy, the seam traits are emitted at `pub` visibility only when
// `sim` is enabled and stay `pub(crate)` otherwise. The bodies are written once
// and emitted at the chosen visibility; exactly one invocation compiles.
macro_rules! wal_fs_seam {
    ($seam_vis:vis) => {
/// The filesystem abstraction the WAL is built on.
///
/// A `FileSystem` mints [`WalFile`] handles and performs whole-directory
/// operations (create, list, fsync, lock). It is intentionally **not**
/// object-safe: the associated [`File`](FileSystem::File) and
/// [`Lock`](FileSystem::Lock) types let `DurableWal<F: FileSystem>` use a
/// concrete handle type without boxing.
///
/// Errors are surfaced as [`std::io::Error`]; the WAL maps them to
/// [`crate::LogError::Io`] with the in-progress operation name at the call site
/// (Requirement 10.2). Every method that can fail does so without leaving a
/// partially mutated filesystem where avoidable.
$seam_vis trait FileSystem {
    /// An open file handle yielding positional reads/writes and fsync.
    type File: WalFile;

    /// An RAII guard representing an exclusively-held directory lock; releasing
    /// the lock happens when the guard is dropped.
    type Lock;

    /// Create `path` and all missing parent directories.
    ///
    /// Succeeds if the directory already exists. A failure here during open
    /// must leave no partially initialized log (Requirement 11.4).
    fn create_dir_all(&self, path: &Path) -> io::Result<()>;

    /// List the directory `path`, returning the full path of each entry.
    ///
    /// Used by recovery to discover segment files; the caller filters by
    /// extension and orders by parsed base index (Requirements 5.1, 3.5).
    fn read_dir(&self, path: &Path) -> io::Result<Vec<PathBuf>>;

    /// Whether `path` names an existing file or directory.
    fn exists(&self, path: &Path) -> bool;

    /// Open an existing file for positional reading.
    ///
    /// Fails with [`io::ErrorKind::NotFound`] if `path` does not exist.
    fn open_read(&self, path: &Path) -> io::Result<Self::File>;

    /// Open `path` for positional reading and writing, creating it (empty) if
    /// it does not exist. Existing contents are preserved (not truncated).
    fn open_read_write(&self, path: &Path) -> io::Result<Self::File>;

    /// Rename `from` to `to`, replacing `to` if it exists.
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()>;

    /// Remove the file at `path` (used to reclaim compacted segments, R7.5).
    fn remove_file(&self, path: &Path) -> io::Result<()>;

    /// Force the directory entry for `path` to stable storage.
    ///
    /// After creating a new segment file under the `Always` policy, the parent
    /// directory must be fsynced so a crash cannot lose the new file
    /// (Requirement 4.1). On platforms without directory fsync this is a
    /// best-effort no-op.
    fn sync_dir(&self, path: &Path) -> io::Result<()>;

    /// Acquire an exclusive lock on the data directory `dir`.
    ///
    /// Fails with [`io::ErrorKind::AlreadyExists`] if the lock is already held,
    /// which the caller maps to [`crate::LogError::Io`] without modifying the
    /// directory (Requirement 11.8). The returned guard releases the lock on
    /// drop.
    fn lock_exclusive(&self, dir: &Path) -> io::Result<Self::Lock>;
}

/// An open file handle with positional I/O and durability control.
///
/// All methods take `&self`: positional access carries its own offset, so no
/// `&mut` cursor is needed, and the disk-backed read path can read through a
/// shared `&self` handle (see the module docs).
$seam_vis trait WalFile {
    /// Read into `buf` starting at byte `offset`, returning the number of bytes
    /// read. A short read (including `0` at end-of-file) is not an error.
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize>;

    /// Read exactly `buf.len()` bytes starting at `offset`.
    ///
    /// Fails with [`io::ErrorKind::UnexpectedEof`] if the file ends before the
    /// buffer is filled. Provided in terms of [`read_at`](WalFile::read_at).
    fn read_exact_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<()> {
        let mut filled = 0;
        while filled < buf.len() {
            let n = self.read_at(offset + filled as u64, &mut buf[filled..])?;
            if n == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "read_exact_at: file ended before buffer was filled",
                ));
            }
            filled += n;
        }
        Ok(())
    }

    /// Write all of `buf` starting at byte `offset`, extending the file if
    /// `offset + buf.len()` exceeds its current size.
    fn write_at(&self, offset: u64, buf: &[u8]) -> io::Result<()>;

    /// Force this file's data and metadata to stable storage (`fsync`).
    fn sync_all(&self) -> io::Result<()>;

    /// Force this file's data (but not necessarily all metadata) to stable
    /// storage (`fdatasync`).
    fn sync_data(&self) -> io::Result<()>;

    /// The current on-disk size of the file, in bytes.
    ///
    /// Segment rollover uses this to track the active segment's size
    /// (Requirement 3.3).
    fn size(&self) -> io::Result<u64>;

    /// Truncate or extend the file to exactly `len` bytes.
    ///
    /// Used by `revert` to drop a reverted suffix from the active segment
    /// (Requirement 9.6).
    fn set_len(&self, len: u64) -> io::Result<()>;
}
    };
}

#[cfg(feature = "sim")]
wal_fs_seam!(pub);
#[cfg(not(feature = "sim"))]
wal_fs_seam!(pub(crate));

// ---------------------------------------------------------------------------
// Production implementation over `std::fs`.
// ---------------------------------------------------------------------------

/// The production [`FileSystem`] backed directly by `std::fs`.
#[derive(Debug, Clone, Default)]
pub struct RealFileSystem;

impl RealFileSystem {
    /// Construct the real filesystem seam.
    pub(crate) fn new() -> Self {
        Self
    }
}

/// A real open file handle wrapping [`std::fs::File`].
#[cfg(feature = "sim")]
#[derive(Debug)]
pub struct RealFile {
    file: std::fs::File,
}

/// A real open file handle wrapping [`std::fs::File`].
#[cfg(not(feature = "sim"))]
#[derive(Debug)]
pub(crate) struct RealFile {
    file: std::fs::File,
}

/// An exclusive directory lock held via an OS **advisory** lock (`flock` on
/// unix, `LockFileEx` on Windows) on an open `.wal.lock` handle.
///
/// The lock is taken with [`FileExt::try_lock_exclusive`] on the open file
/// descriptor; the guard keeps that handle alive for the lifetime of the open
/// log. Crucially the lock is an *OS advisory lock tied to the open
/// descriptor*, not the existence of the file, so the kernel releases it
/// automatically when the holding process exits — including an abrupt crash or
/// `SIGKILL`. A leftover `.wal.lock` from a previous run therefore never blocks
/// a restart: the next open simply re-opens the same file and re-acquires the
/// lock. A *live* second holder is still excluded — its `try_lock_exclusive`
/// fails with [`io::ErrorKind::WouldBlock`] while the first process holds the
/// lock, giving single-writer-per-directory exclusion.
///
/// The lock file is intentionally **left in place** across runs and **not**
/// removed on drop: its existence is not the lock (the held advisory lock is),
/// and unlinking it would race a concurrent opener that has already created and
/// locked a fresh file of the same name. Dropping this guard closes the handle,
/// which releases the advisory lock.
#[cfg(feature = "sim")]
#[derive(Debug)]
pub struct RealDirLock {
    /// The locked file handle. Held solely so that dropping the guard closes
    /// the descriptor and releases the OS advisory lock; never read or written.
    _file: File,
}

/// Exclusive directory lock; see the `sim`-gated sibling above for details.
#[cfg(not(feature = "sim"))]
#[derive(Debug)]
pub(crate) struct RealDirLock {
    /// The locked file handle. Held solely so that dropping the guard closes
    /// the descriptor and releases the OS advisory lock; never read or written.
    _file: File,
}

/// Name of the sentinel lock file created inside a locked data directory.
const LOCK_FILE_NAME: &str = ".wal.lock";

/// Positional single read against a real file, abstracting the per-OS API.
#[cfg(unix)]
fn real_read_at(file: &std::fs::File, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
    std::os::unix::fs::FileExt::read_at(file, buf, offset)
}

/// Positional single read against a real file, abstracting the per-OS API.
#[cfg(windows)]
fn real_read_at(file: &std::fs::File, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
    std::os::windows::fs::FileExt::seek_read(file, buf, offset)
}

/// Positional single write against a real file, abstracting the per-OS API.
#[cfg(unix)]
fn real_write_at(file: &std::fs::File, offset: u64, buf: &[u8]) -> io::Result<usize> {
    std::os::unix::fs::FileExt::write_at(file, buf, offset)
}

/// Positional single write against a real file, abstracting the per-OS API.
#[cfg(windows)]
fn real_write_at(file: &std::fs::File, offset: u64, buf: &[u8]) -> io::Result<usize> {
    std::os::windows::fs::FileExt::seek_write(file, buf, offset)
}

impl WalFile for RealFile {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        real_read_at(&self.file, offset, buf)
    }

    fn write_at(&self, offset: u64, buf: &[u8]) -> io::Result<()> {
        // Loop to handle short writes; positional writes never move a cursor.
        let mut written = 0usize;
        while written < buf.len() {
            let n = real_write_at(&self.file, offset + written as u64, &buf[written..])?;
            if n == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "write_at: wrote zero bytes",
                ));
            }
            written += n;
        }
        Ok(())
    }

    fn sync_all(&self) -> io::Result<()> {
        self.file.sync_all()
    }

    fn sync_data(&self) -> io::Result<()> {
        self.file.sync_data()
    }

    fn size(&self) -> io::Result<u64> {
        Ok(self.file.metadata()?.len())
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        self.file.set_len(len)
    }
}

impl FileSystem for RealFileSystem {
    type File = RealFile;
    type Lock = RealDirLock;

    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        std::fs::create_dir_all(path)
    }

    fn read_dir(&self, path: &Path) -> io::Result<Vec<PathBuf>> {
        let mut entries = Vec::new();
        for entry in std::fs::read_dir(path)? {
            entries.push(entry?.path());
        }
        Ok(entries)
    }

    fn exists(&self, path: &Path) -> bool {
        path.exists()
    }

    fn open_read(&self, path: &Path) -> io::Result<Self::File> {
        let file = std::fs::OpenOptions::new().read(true).open(path)?;
        Ok(RealFile { file })
    }

    fn open_read_write(&self, path: &Path) -> io::Result<Self::File> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        Ok(RealFile { file })
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        std::fs::rename(from, to)
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        std::fs::remove_file(path)
    }

    #[cfg(unix)]
    fn sync_dir(&self, path: &Path) -> io::Result<()> {
        // On unix a directory can be opened and fsynced to durably record newly
        // created entries within it (Requirement 4.1).
        let dir = std::fs::File::open(path)?;
        dir.sync_all()
    }

    #[cfg(not(unix))]
    fn sync_dir(&self, path: &Path) -> io::Result<()> {
        // Platforms without directory fsync: best-effort no-op. Existence of
        // the directory is still verified so a missing dir surfaces an error.
        if path.is_dir() {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::NotFound,
                "sync_dir: directory does not exist",
            ))
        }
    }

    fn lock_exclusive(&self, dir: &Path) -> io::Result<Self::Lock> {
        let path = dir.join(LOCK_FILE_NAME);
        // Open (creating if absent) the lock file. Unlike `create_new`, a
        // leftover `.wal.lock` from a previous run is reused rather than
        // rejected — an unclean shutdown can never strand the directory, since
        // the *advisory lock*, not the file's existence, provides exclusion.
        // `truncate(false)` leaves any existing file untouched.
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;
        // Take an OS advisory exclusive lock on the open handle, non-blocking.
        // It is held only while THIS process is alive — the kernel releases it
        // on exit, including a crash/SIGKILL — and is refused (WouldBlock) while
        // another live process holds it (Requirement 11.8). The caller maps the
        // error to `LogError::Io` without modifying the directory.
        FileExt::try_lock_exclusive(&file)?;
        Ok(RealDirLock { _file: file })
    }
}

// ---------------------------------------------------------------------------
// Test implementation: deterministic in-memory filesystem with fault injection.
// ---------------------------------------------------------------------------

#[cfg(any(test, feature = "sim"))]
pub(crate) mod fault {
    //! An in-memory [`FileSystem`] for deterministic crash/fault testing.
    //!
    //! [`MemFileSystem`] backs files with byte vectors behind a shared
    //! `Arc<Mutex<_>>`, so cloning the handle (or dropping a `DurableWal` and
    //! opening a new one on the same `MemFileSystem`) sees the same persisted
    //! bytes — exactly the "reopen after crash" shape recovery tests need.
    //!
    //! Faults are armed via the inherent `arm_*`/`hold_lock`/`tear_*` methods
    //! and fire deterministically (by fsync call count or by path), with no
    //! reliance on timing or real process kills:
    //!
    //! - **crash-durability boundary** — [`MemFileSystem::crash`] drops every
    //!   file's un-fsynced tail (truncating it to the length forced to stable
    //!   storage by its last successful fsync), modelling loss of unsynced
    //!   writes at a Node_Crash while retaining exactly the durable bytes
    //!   (Requirement 7.2).
    //! - **torn write** — [`MemFileSystem::tear_last_write`] /
    //!   [`MemFileSystem::truncate_file`] drop the tail of the most recent (or a
    //!   named) file, modelling bytes that never reached disk (Requirements 6.1,
    //!   7.3).
    //! - **fsync failure** — [`MemFileSystem::arm_fsync_failure_at`] /
    //!   [`MemFileSystem::arm_fsync_failure_for`] fail a chosen fsync
    //!   (Requirements 4.5, 10.3).
    //! - **locked / missing directory** — [`MemFileSystem::hold_lock`] and
    //!   [`MemFileSystem::arm_create_dir_failure`] reproduce an already-held
    //!   directory lock and an uncreatable data directory (Requirements 11.4,
    //!   11.8).
    //! - **read failure** — [`MemFileSystem::arm_read_failure_for`] injects a
    //!   read-path I/O error for the fail-stop path (Requirement 10.4).

    use super::{FileSystem, WalFile, LOCK_FILE_NAME};
    use std::collections::{HashMap, HashSet};
    use std::io;
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, Mutex};

    /// Record of the most recent write, enabling [`MemFileSystem::tear_last_write`].
    #[derive(Debug, Clone)]
    struct LastWrite {
        /// File that was written.
        path: PathBuf,
        /// File length immediately after the write completed.
        end_after: u64,
    }

    /// Shared, mutable backing state for a [`MemFileSystem`] and its handles.
    #[derive(Debug, Default)]
    struct MemState {
        /// File contents keyed by absolute path.
        files: HashMap<PathBuf, Vec<u8>>,
        /// Existing directories.
        dirs: HashSet<PathBuf>,
        /// Currently-held lock sentinel paths (`dir/.wal.lock`).
        locks: HashSet<PathBuf>,
        /// The most recent successful write, for [`MemFileSystem::tear_last_write`].
        last_write: Option<LastWrite>,
        /// Total fsync calls observed (`sync_all`/`sync_data`/`sync_dir`).
        fsync_count: u64,
        /// If set, the fsync whose 1-based call number equals this fails once.
        fsync_fail_at: Option<u64>,
        /// fsync targeting any of these paths fails.
        fsync_fail_paths: HashSet<PathBuf>,
        /// Reads of any of these paths fail.
        read_fail_paths: HashSet<PathBuf>,
        /// Writes to any of these paths fail.
        write_fail_paths: HashSet<PathBuf>,
        /// When true, `create_dir_all` fails (uncreatable data directory).
        create_dir_fails: bool,
        /// Per-file byte length forced to stable storage by that file's last
        /// successful fsync (`sync_all`/`sync_data`). This is the length a
        /// [`MemFileSystem::crash`] truncates the file back to, modelling loss
        /// of the un-fsynced tail; a file never fsynced has no entry and so is
        /// truncated to zero (Requirement 7.2).
        synced_len: HashMap<PathBuf, u64>,
    }

    /// An in-memory [`FileSystem`]; clones share one backing store.
    ///
    /// `pub` under the non-default `sim` feature so it can be re-exported as
    /// [`crate::wal::sim::FaultFileSystem`] for the simulation harness;
    /// `pub(crate)` otherwise (test-only), leaving production privacy unchanged.
    #[cfg(feature = "sim")]
    #[derive(Debug, Clone, Default)]
    pub struct MemFileSystem {
        state: Arc<Mutex<MemState>>,
    }

    /// An in-memory [`FileSystem`]; clones share one backing store.
    #[cfg(not(feature = "sim"))]
    #[derive(Debug, Clone, Default)]
    pub(crate) struct MemFileSystem {
        state: Arc<Mutex<MemState>>,
    }

    impl MemFileSystem {
        /// Create an empty in-memory filesystem.
        pub(crate) fn new() -> Self {
            Self::default()
        }

        /// Lock the shared state, panicking on a poisoned mutex (a poisoned
        /// mutex means a prior test thread panicked while holding it).
        fn lock(&self) -> std::sync::MutexGuard<'_, MemState> {
            self.state.lock().expect("mem fs mutex poisoned")
        }

        // --- inspection helpers (for assertions) ---------------------------

        /// The current byte length of `path`, or `None` if it does not exist.
        pub(crate) fn file_size(&self, path: &Path) -> Option<u64> {
            self.lock().files.get(path).map(|b| b.len() as u64)
        }

        /// A copy of the bytes of `path`, or `None` if it does not exist.
        pub(crate) fn file_bytes(&self, path: &Path) -> Option<Vec<u8>> {
            self.lock().files.get(path).cloned()
        }

        // --- open-time fault injection (crate-internal) --------------------

        /// Make [`create_dir_all`](FileSystem::create_dir_all) fail, modelling
        /// an uncreatable data directory.
        pub(crate) fn arm_create_dir_failure(&self) {
            self.lock().create_dir_fails = true;
        }

        /// Pre-hold the exclusive lock for `dir`, so a later
        /// [`lock_exclusive`](FileSystem::lock_exclusive) sees it as already
        /// held (Requirement 11.8).
        pub(crate) fn hold_lock(&self, dir: &Path) {
            self.lock().locks.insert(dir.join(LOCK_FILE_NAME));
        }
    }

    // The crash-durability boundary (`crash`) and the storage-fault arming
    // methods (torn-tail and I/O errors) are part of the simulation harness's
    // public surface: emitted at `pub` visibility when the non-default `sim`
    // feature is on (so `vela_log::sim::FaultFileSystem` can drive them) and
    // `pub(crate)` otherwise, leaving production privacy unchanged. The bodies
    // are written once; exactly one invocation compiles.
    macro_rules! mem_fault_methods {
        ($vis:vis) => {
            impl MemFileSystem {
                /// Truncate `path` to `new_len` bytes, dropping any tail beyond
                /// it.
                ///
                /// Models a torn write where trailing bytes never reached disk.
                $vis fn truncate_file(&self, path: &Path, new_len: u64) {
                    let mut state = self.lock();
                    if let Some(bytes) = state.files.get_mut(path) {
                        bytes.truncate(new_len as usize);
                    }
                }

                /// Drop the final `bytes_dropped` bytes of the most recent
                /// write, modelling a torn last write (Requirement 6.1, 7.3).
                $vis fn tear_last_write(&self, bytes_dropped: u64) {
                    let last = self.lock().last_write.clone();
                    if let Some(last) = last {
                        let new_len = last.end_after.saturating_sub(bytes_dropped);
                        self.truncate_file(&last.path, new_len);
                    }
                }

                /// Model a Node_Crash: for every file, drop the un-fsynced tail
                /// by truncating it to the length forced to stable storage by
                /// that file's last successful fsync, retaining exactly the
                /// bytes that were made durable (Requirement 7.2). A file that
                /// was never fsynced is truncated to zero. Bytes only shrink —
                /// the durable extent can never exceed the file's current
                /// length — so this can lose unsynced writes but never fabricate
                /// data. The volatile last-write marker is cleared so a torn-tail
                /// fault cannot reference a write that did not survive the crash.
                $vis fn crash(&self) {
                    let mut state = self.lock();
                    let paths: Vec<PathBuf> = state.files.keys().cloned().collect();
                    for path in paths {
                        let durable = state.synced_len.get(&path).copied().unwrap_or(0);
                        if let Some(bytes) = state.files.get_mut(&path) {
                            if bytes.len() as u64 > durable {
                                bytes.truncate(durable as usize);
                            }
                        }
                    }
                    state.last_write = None;
                }

                /// Fail the fsync whose 1-based call number equals `nth`
                /// (counting `sync_all`, `sync_data`, and `sync_dir`).
                $vis fn arm_fsync_failure_at(&self, nth: u64) {
                    self.lock().fsync_fail_at = Some(nth);
                }

                /// Fail the next fsync call, whatever its number.
                $vis fn arm_next_fsync_failure(&self) {
                    let mut state = self.lock();
                    let next = state.fsync_count + 1;
                    state.fsync_fail_at = Some(next);
                }

                /// Fail every fsync that targets `path`.
                $vis fn arm_fsync_failure_for(&self, path: &Path) {
                    self.lock().fsync_fail_paths.insert(path.to_path_buf());
                }

                /// Fail every read of `path` (read-path I/O error for the
                /// fail-stop path; Requirement 7.4).
                $vis fn arm_read_failure_for(&self, path: &Path) {
                    self.lock().read_fail_paths.insert(path.to_path_buf());
                }

                /// Fail every write to `path` (Requirement 7.4).
                $vis fn arm_write_failure_for(&self, path: &Path) {
                    self.lock().write_fail_paths.insert(path.to_path_buf());
                }
            }
        };
    }

    #[cfg(feature = "sim")]
    mem_fault_methods!(pub);
    #[cfg(not(feature = "sim"))]
    mem_fault_methods!(pub(crate));

    /// An in-memory file handle sharing its filesystem's backing store.
    #[cfg(feature = "sim")]
    #[derive(Debug, Clone)]
    pub struct MemFile {
        /// Shared backing store.
        state: Arc<Mutex<MemState>>,
        /// Path this handle refers to.
        path: PathBuf,
    }

    /// An in-memory file handle sharing its filesystem's backing store.
    #[cfg(not(feature = "sim"))]
    #[derive(Debug, Clone)]
    pub(crate) struct MemFile {
        /// Shared backing store.
        state: Arc<Mutex<MemState>>,
        /// Path this handle refers to.
        path: PathBuf,
    }

    impl MemFile {
        fn lock(&self) -> std::sync::MutexGuard<'_, MemState> {
            self.state.lock().expect("mem fs mutex poisoned")
        }
    }

    impl WalFile for MemFile {
        fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
            let state = self.lock();
            if state.read_fail_paths.contains(&self.path) {
                return Err(io::Error::other("injected read failure"));
            }
            let bytes = state
                .files
                .get(&self.path)
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "read_at: no such file"))?;
            let start = offset as usize;
            if start >= bytes.len() {
                return Ok(0);
            }
            let n = buf.len().min(bytes.len() - start);
            buf[..n].copy_from_slice(&bytes[start..start + n]);
            Ok(n)
        }

        fn write_at(&self, offset: u64, buf: &[u8]) -> io::Result<()> {
            let mut state = self.lock();
            if state.write_fail_paths.contains(&self.path) {
                return Err(io::Error::other("injected write failure"));
            }
            let bytes = state
                .files
                .get_mut(&self.path)
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "write_at: no such file"))?;
            let start = offset as usize;
            let end = start + buf.len();
            if bytes.len() < end {
                // Extend with zeros up to the write region, like a real file
                // written past its current end.
                bytes.resize(end, 0);
            }
            bytes[start..end].copy_from_slice(buf);
            let end_after = bytes.len() as u64;
            state.last_write = Some(LastWrite {
                path: self.path.clone(),
                end_after,
            });
            Ok(())
        }

        fn sync_all(&self) -> io::Result<()> {
            self.lock().tick_fsync(&self.path)
        }

        fn sync_data(&self) -> io::Result<()> {
            self.lock().tick_fsync(&self.path)
        }

        fn size(&self) -> io::Result<u64> {
            let state = self.lock();
            state
                .files
                .get(&self.path)
                .map(|b| b.len() as u64)
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "size: no such file"))
        }

        fn set_len(&self, len: u64) -> io::Result<()> {
            let mut state = self.lock();
            let bytes = state
                .files
                .get_mut(&self.path)
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "set_len: no such file"))?;
            bytes.resize(len as usize, 0);
            Ok(())
        }
    }

    impl MemState {
        /// Account for one fsync against `path` and fail it if armed.
        fn tick_fsync(&mut self, path: &Path) -> io::Result<()> {
            self.fsync_count += 1;
            if self.fsync_fail_paths.contains(path) {
                return Err(io::Error::other("injected fsync failure (path)"));
            }
            if self.fsync_fail_at == Some(self.fsync_count) {
                // One-shot: disarm so a retry could succeed.
                self.fsync_fail_at = None;
                return Err(io::Error::other("injected fsync failure (nth)"));
            }
            // The fsync succeeded: record the file's current length as the
            // durable extent so a later `crash()` truncates it back to exactly
            // the bytes forced to stable storage (Requirement 7.2). A `sync_dir`
            // targets a directory, which has no `files` entry, so this is a
            // no-op for directory fsyncs.
            if let Some(len) = self.files.get(path).map(|b| b.len() as u64) {
                self.synced_len.insert(path.to_path_buf(), len);
            }
            Ok(())
        }
    }

    impl FileSystem for MemFileSystem {
        type File = MemFile;
        type Lock = MemDirLock;

        fn create_dir_all(&self, path: &Path) -> io::Result<()> {
            let mut state = self.lock();
            if state.create_dir_fails {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "injected create_dir_all failure",
                ));
            }
            // Record the directory and all of its ancestors.
            let mut cur = Some(path);
            while let Some(dir) = cur {
                state.dirs.insert(dir.to_path_buf());
                cur = dir.parent();
            }
            Ok(())
        }

        fn read_dir(&self, path: &Path) -> io::Result<Vec<PathBuf>> {
            let state = self.lock();
            if !state.dirs.contains(path) {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    "read_dir: no such directory",
                ));
            }
            // Return files whose immediate parent is `path`.
            let entries = state
                .files
                .keys()
                .filter(|p| p.parent() == Some(path))
                .cloned()
                .collect();
            Ok(entries)
        }

        fn exists(&self, path: &Path) -> bool {
            let state = self.lock();
            state.files.contains_key(path) || state.dirs.contains(path)
        }

        fn open_read(&self, path: &Path) -> io::Result<Self::File> {
            let state = self.lock();
            if state.files.contains_key(path) {
                Ok(MemFile {
                    state: Arc::clone(&self.state),
                    path: path.to_path_buf(),
                })
            } else {
                Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    "open_read: no such file",
                ))
            }
        }

        fn open_read_write(&self, path: &Path) -> io::Result<Self::File> {
            let mut state = self.lock();
            state.files.entry(path.to_path_buf()).or_default();
            Ok(MemFile {
                state: Arc::clone(&self.state),
                path: path.to_path_buf(),
            })
        }

        fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
            let mut state = self.lock();
            match state.files.remove(from) {
                Some(bytes) => {
                    state.files.insert(to.to_path_buf(), bytes);
                    Ok(())
                }
                None => Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    "rename: no such file",
                )),
            }
        }

        fn remove_file(&self, path: &Path) -> io::Result<()> {
            let mut state = self.lock();
            if state.files.remove(path).is_some() {
                Ok(())
            } else {
                Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    "remove_file: no such file",
                ))
            }
        }

        fn sync_dir(&self, path: &Path) -> io::Result<()> {
            self.lock().tick_fsync(path)
        }

        fn lock_exclusive(&self, dir: &Path) -> io::Result<Self::Lock> {
            let mut state = self.lock();
            let lock_path = dir.join(LOCK_FILE_NAME);
            if state.locks.contains(&lock_path) {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "lock_exclusive: directory already locked",
                ));
            }
            state.locks.insert(lock_path.clone());
            Ok(MemDirLock {
                state: Arc::clone(&self.state),
                path: lock_path,
            })
        }
    }

    /// RAII guard releasing a [`MemFileSystem`] directory lock on drop.
    #[cfg(feature = "sim")]
    #[derive(Debug)]
    pub struct MemDirLock {
        state: Arc<Mutex<MemState>>,
        path: PathBuf,
    }

    /// RAII guard releasing a [`MemFileSystem`] directory lock on drop.
    #[cfg(not(feature = "sim"))]
    #[derive(Debug)]
    pub(crate) struct MemDirLock {
        state: Arc<Mutex<MemState>>,
        path: PathBuf,
    }

    impl Drop for MemDirLock {
        fn drop(&mut self) {
            if let Ok(mut state) = self.state.lock() {
                state.locks.remove(&self.path);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::fault::MemFileSystem;
    use super::{FileSystem, RealFileSystem, WalFile, LOCK_FILE_NAME};
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};

    // --- helpers -----------------------------------------------------------

    /// A unique temporary directory path (not yet created) for real-FS tests.
    fn unique_temp_dir(tag: &str) -> PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock before unix epoch")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "vela-wal-fs-{tag}-{}-{nanos}-{n}",
            std::process::id()
        ))
    }

    /// Run `body` against a freshly created real-FS temp dir, cleaning it up
    /// afterwards regardless of outcome.
    fn with_real_dir(tag: &str, body: impl FnOnce(&RealFileSystem, &Path)) {
        let fs = RealFileSystem::new();
        let dir = unique_temp_dir(tag);
        fs.create_dir_all(&dir).expect("create temp dir");
        body(&fs, &dir);
        let _ = std::fs::remove_dir_all(&dir);
    }

    // --- RealFileSystem: basic I/O round-trip ------------------------------

    #[test]
    fn real_write_read_round_trip_and_size() {
        with_real_dir("rw", |fs, dir| {
            let path = dir.join("data.bin");
            let file = fs.open_read_write(&path).expect("open rw");
            file.write_at(0, b"hello world").expect("write");
            file.sync_all().expect("fsync");

            assert_eq!(file.size().expect("size"), 11);

            let mut buf = [0u8; 5];
            file.read_exact_at(6, &mut buf).expect("read_exact_at");
            assert_eq!(&buf, b"world");

            // Positional overwrite of an interior region.
            file.write_at(0, b"HELLO").expect("overwrite");
            let mut all = [0u8; 11];
            file.read_exact_at(0, &mut all).expect("read all");
            assert_eq!(&all, b"HELLO world");
        });
    }

    #[test]
    fn real_set_len_truncates_then_reopen_sees_short_file() {
        with_real_dir("trunc", |fs, dir| {
            let path = dir.join("seg.wal");
            {
                let file = fs.open_read_write(&path).expect("open");
                file.write_at(0, &[1, 2, 3, 4, 5, 6, 7, 8]).expect("write");
                file.sync_all().expect("fsync");
                file.set_len(3).expect("set_len");
            }
            // Reopening the same path observes the truncated length.
            let reopened = fs.open_read_write(&path).expect("reopen");
            assert_eq!(reopened.size().expect("size"), 3);
        });
    }

    #[test]
    fn real_open_read_missing_file_is_not_found() {
        with_real_dir("missing", |fs, dir| {
            let err = fs
                .open_read(&dir.join("nope.wal"))
                .expect_err("missing file should error");
            assert_eq!(err.kind(), std::io::ErrorKind::NotFound);
        });
    }

    #[test]
    fn real_read_dir_lists_created_files() {
        with_real_dir("list", |fs, dir| {
            for name in ["a.wal", "b.wal"] {
                fs.open_read_write(&dir.join(name)).expect("create");
            }
            let mut names: Vec<String> = fs
                .read_dir(dir)
                .expect("read_dir")
                .into_iter()
                .filter_map(|p| p.file_name().map(|n| n.to_string_lossy().into_owned()))
                .collect();
            names.sort();
            assert_eq!(names, vec!["a.wal".to_string(), "b.wal".to_string()]);
        });
    }

    #[test]
    fn real_rename_and_remove() {
        with_real_dir("mv", |fs, dir| {
            let from = dir.join("from.wal");
            let to = dir.join("to.wal");
            let file = fs.open_read_write(&from).expect("create");
            file.write_at(0, b"x").expect("write");
            drop(file);

            fs.rename(&from, &to).expect("rename");
            assert!(!fs.exists(&from));
            assert!(fs.exists(&to));

            fs.remove_file(&to).expect("remove");
            assert!(!fs.exists(&to));
        });
    }

    #[test]
    fn real_sync_dir_succeeds_for_existing_dir() {
        with_real_dir("dirsync", |fs, dir| {
            fs.open_read_write(&dir.join("seg.wal")).expect("create");
            fs.sync_dir(dir).expect("sync_dir on existing dir");
        });
    }

    // --- RealFileSystem: exclusive directory lock --------------------------

    #[test]
    fn real_lock_is_exclusive_and_released_on_drop() {
        with_real_dir("lock", |fs, dir| {
            let lock = fs.lock_exclusive(dir).expect("first lock");
            // A second acquisition while the first is held fails: the advisory
            // lock is held on a distinct open descriptor, so the non-blocking
            // attempt is refused with WouldBlock (not a hang).
            let err = fs.lock_exclusive(dir).expect_err("second lock should fail");
            assert_eq!(err.kind(), std::io::ErrorKind::WouldBlock);

            // Releasing the guard (closing its descriptor) lets a later
            // acquisition succeed.
            drop(lock);
            let _again = fs.lock_exclusive(dir).expect("relock after release");
        });
    }

    #[test]
    fn real_lock_tolerates_a_stale_lock_file_from_a_crash() {
        // Regression: an OS advisory lock is released by the kernel when its
        // holder exits, so a `.wal.lock` file left behind by a process that
        // crashed (or was SIGKILLed) without running its destructor must NOT
        // block a restart. A create-new sentinel would wrongly reject this.
        with_real_dir("stale-lock", |fs, dir| {
            // Simulate the leftover: the lock file exists on disk but no live
            // process holds an advisory lock on it.
            let stale = dir.join(LOCK_FILE_NAME);
            std::fs::write(&stale, b"").expect("create a stale lock file");
            assert!(fs.exists(&stale), "the stale lock file is present");

            // Acquiring the lock succeeds despite the pre-existing file.
            let lock = fs
                .lock_exclusive(dir)
                .expect("a stale lock file must not block acquisition");
            // And it is genuinely held: a concurrent attempt is still refused.
            let err = fs
                .lock_exclusive(dir)
                .expect_err("the lock is held after reclaiming a stale file");
            assert_eq!(err.kind(), std::io::ErrorKind::WouldBlock);
            drop(lock);
        });
    }

    // --- MemFileSystem: I/O round-trip & persistence across reopen ---------

    #[test]
    fn mem_write_read_round_trip() {
        let fs = MemFileSystem::new();
        let dir = PathBuf::from("/wal");
        fs.create_dir_all(&dir).expect("mkdir");
        let path = dir.join("seg.wal");

        let file = fs.open_read_write(&path).expect("open");
        file.write_at(0, b"durable").expect("write");
        assert_eq!(file.size().expect("size"), 7);

        let mut buf = [0u8; 7];
        file.read_exact_at(0, &mut buf).expect("read");
        assert_eq!(&buf, b"durable");
    }

    #[test]
    fn mem_bytes_persist_across_reopen_via_shared_store() {
        // The mem FS models disk: a new handle (or a clone of the FS) sees
        // bytes written by an earlier handle — the "reopen after crash" shape.
        let fs = MemFileSystem::new();
        let dir = PathBuf::from("/wal");
        fs.create_dir_all(&dir).expect("mkdir");
        let path = dir.join("seg.wal");

        {
            let writer = fs.open_read_write(&path).expect("open writer");
            writer.write_at(0, &[9, 8, 7]).expect("write");
        }
        let reopened = fs.clone();
        let reader = reopened.open_read(&path).expect("open reader");
        let mut buf = [0u8; 3];
        reader.read_exact_at(0, &mut buf).expect("read");
        assert_eq!(buf, [9, 8, 7]);
    }

    // --- MemFileSystem: torn write -----------------------------------------

    #[test]
    fn mem_tear_last_write_drops_the_tail() {
        let fs = MemFileSystem::new();
        let dir = PathBuf::from("/wal");
        fs.create_dir_all(&dir).expect("mkdir");
        let path = dir.join("seg.wal");

        let file = fs.open_read_write(&path).expect("open");
        file.write_at(0, &[0u8; 4]).expect("first write");
        // Simulate a second write whose final 3 bytes never reach disk.
        file.write_at(4, &[1, 2, 3, 4, 5]).expect("second write");
        fs.tear_last_write(3);

        // The first write survives; only the torn tail of the last write is
        // lost (9 bytes written, 3 dropped → 6 remain).
        assert_eq!(fs.file_size(&path), Some(6));
        assert_eq!(fs.file_bytes(&path), Some(vec![0, 0, 0, 0, 1, 2]));
    }

    #[test]
    fn mem_truncate_file_drops_named_tail() {
        let fs = MemFileSystem::new();
        let dir = PathBuf::from("/wal");
        fs.create_dir_all(&dir).expect("mkdir");
        let path = dir.join("seg.wal");

        let file = fs.open_read_write(&path).expect("open");
        file.write_at(0, &[10, 20, 30, 40]).expect("write");
        fs.truncate_file(&path, 2);
        assert_eq!(fs.file_bytes(&path), Some(vec![10, 20]));
    }

    // --- MemFileSystem: fsync failure --------------------------------------

    #[test]
    fn mem_arm_fsync_failure_at_fails_only_that_call() {
        let fs = MemFileSystem::new();
        let dir = PathBuf::from("/wal");
        fs.create_dir_all(&dir).expect("mkdir");
        let file = fs.open_read_write(&dir.join("seg.wal")).expect("open");

        fs.arm_fsync_failure_at(2);
        file.sync_all().expect("first fsync succeeds");
        let err = file.sync_all().expect_err("second fsync fails");
        assert_eq!(err.kind(), std::io::ErrorKind::Other);
        // One-shot: the third fsync succeeds again.
        file.sync_all().expect("third fsync succeeds");
    }

    #[test]
    fn mem_arm_fsync_failure_for_path_fails_that_files_fsync() {
        let fs = MemFileSystem::new();
        let dir = PathBuf::from("/wal");
        fs.create_dir_all(&dir).expect("mkdir");
        let seg = dir.join("seg.wal");
        let manifest = dir.join("wal.manifest");
        let seg_file = fs.open_read_write(&seg).expect("open seg");
        let man_file = fs.open_read_write(&manifest).expect("open manifest");

        fs.arm_fsync_failure_for(&manifest);
        // The segment fsync is unaffected; the manifest fsync fails.
        seg_file.sync_all().expect("segment fsync ok");
        assert!(man_file.sync_all().is_err(), "manifest fsync should fail");
    }

    // --- MemFileSystem: locked / missing / uncreatable directory -----------

    #[test]
    fn mem_lock_exclusive_is_exclusive_and_released_on_drop() {
        let fs = MemFileSystem::new();
        let dir = PathBuf::from("/wal");
        fs.create_dir_all(&dir).expect("mkdir");

        let lock = fs.lock_exclusive(&dir).expect("first lock");
        assert_eq!(
            fs.lock_exclusive(&dir)
                .expect_err("second lock fails")
                .kind(),
            std::io::ErrorKind::AlreadyExists,
        );
        drop(lock);
        let _again = fs.lock_exclusive(&dir).expect("relock after release");
    }

    #[test]
    fn mem_hold_lock_simulates_another_holder() {
        let fs = MemFileSystem::new();
        let dir = PathBuf::from("/wal");
        fs.create_dir_all(&dir).expect("mkdir");

        // A different process already holds the lock.
        fs.hold_lock(&dir);
        assert_eq!(
            fs.lock_exclusive(&dir)
                .expect_err("locked dir refuses")
                .kind(),
            std::io::ErrorKind::AlreadyExists,
        );
    }

    #[test]
    fn mem_arm_create_dir_failure_blocks_creation() {
        let fs = MemFileSystem::new();
        fs.arm_create_dir_failure();
        let err = fs
            .create_dir_all(Path::new("/wal"))
            .expect_err("create should fail");
        assert_eq!(err.kind(), std::io::ErrorKind::PermissionDenied);
    }

    #[test]
    fn mem_read_dir_missing_directory_is_not_found() {
        let fs = MemFileSystem::new();
        let err = fs
            .read_dir(Path::new("/absent"))
            .expect_err("missing dir should error");
        assert_eq!(err.kind(), std::io::ErrorKind::NotFound);
    }

    // --- MemFileSystem: injected read failure (fail-stop support) ----------

    #[test]
    fn mem_arm_read_failure_for_path_fails_reads() {
        let fs = MemFileSystem::new();
        let dir = PathBuf::from("/wal");
        fs.create_dir_all(&dir).expect("mkdir");
        let path = dir.join("seg.wal");
        let file = fs.open_read_write(&path).expect("open");
        file.write_at(0, b"payload").expect("write");

        fs.arm_read_failure_for(&path);
        let mut buf = [0u8; 7];
        assert!(
            file.read_at(0, &mut buf).is_err(),
            "armed read failure should surface as an I/O error",
        );
    }

    #[test]
    fn mem_arm_write_failure_for_path_fails_writes() {
        let fs = MemFileSystem::new();
        let dir = PathBuf::from("/wal");
        fs.create_dir_all(&dir).expect("mkdir");
        let path = dir.join("seg.wal");
        let file = fs.open_read_write(&path).expect("open");

        fs.arm_write_failure_for(&path);
        // An armed write failure surfaces through the result type rather than
        // panicking or silently succeeding (Requirement 7.4).
        assert!(
            file.write_at(0, b"payload").is_err(),
            "armed write failure should surface as an I/O error",
        );
        // The failed write left no bytes behind (no silent partial success).
        assert_eq!(fs.file_size(&path), Some(0));
    }

    // --- MemFileSystem: crash durability boundary --------------------------

    #[test]
    fn mem_crash_retains_fsynced_bytes_and_drops_unsynced_tail() {
        // The crash-durability boundary (Requirement 7.2): a crash keeps exactly
        // the bytes forced to stable storage and discards the un-fsynced tail.
        let fs = MemFileSystem::new();
        let dir = PathBuf::from("/wal");
        fs.create_dir_all(&dir).expect("mkdir");
        let path = dir.join("seg.wal");

        let file = fs.open_read_write(&path).expect("open");
        file.write_at(0, &[1, 2, 3, 4]).expect("durable write");
        file.sync_all()
            .expect("fsync forces 4 bytes to stable storage");
        // Append a tail that is never fsynced.
        file.write_at(4, &[5, 6, 7, 8])
            .expect("unsynced tail write");
        assert_eq!(
            fs.file_size(&path),
            Some(8),
            "all 8 bytes present pre-crash"
        );

        fs.crash();

        // Only the fsynced prefix survives; the un-fsynced tail is gone.
        assert_eq!(fs.file_size(&path), Some(4));
        assert_eq!(fs.file_bytes(&path), Some(vec![1, 2, 3, 4]));
    }

    #[test]
    fn mem_crash_truncates_never_fsynced_file_to_zero() {
        // A file that was never fsynced has no durable extent, so a crash drops
        // all of its bytes while leaving the (now empty) file in place.
        let fs = MemFileSystem::new();
        let dir = PathBuf::from("/wal");
        fs.create_dir_all(&dir).expect("mkdir");
        let path = dir.join("seg.wal");

        let file = fs.open_read_write(&path).expect("open");
        file.write_at(0, &[9, 9, 9, 9, 9]).expect("write, no fsync");
        assert_eq!(fs.file_size(&path), Some(5));

        fs.crash();

        assert_eq!(fs.file_bytes(&path), Some(Vec::new()));
    }

    #[test]
    fn mem_crash_retains_all_bytes_when_everything_was_fsynced() {
        // When the last fsync forced the file's full length to stable storage,
        // a crash retains every byte unchanged.
        let fs = MemFileSystem::new();
        let dir = PathBuf::from("/wal");
        fs.create_dir_all(&dir).expect("mkdir");
        let path = dir.join("seg.wal");

        let file = fs.open_read_write(&path).expect("open");
        file.write_at(0, &[1, 2, 3, 4, 5, 6]).expect("write");
        file.sync_data().expect("fsync forces all 6 bytes");

        fs.crash();

        assert_eq!(fs.file_size(&path), Some(6));
        assert_eq!(fs.file_bytes(&path), Some(vec![1, 2, 3, 4, 5, 6]));
    }

    #[test]
    fn mem_crash_durable_extent_advances_with_each_fsync() {
        // Each successful fsync advances the durable extent to the file's
        // current length, so a later crash keeps the most recently synced
        // prefix — not the first one.
        let fs = MemFileSystem::new();
        let dir = PathBuf::from("/wal");
        fs.create_dir_all(&dir).expect("mkdir");
        let path = dir.join("seg.wal");

        let file = fs.open_read_write(&path).expect("open");
        file.write_at(0, &[1, 2, 3, 4]).expect("first write");
        file.sync_all().expect("fsync at 4 bytes");
        file.write_at(4, &[5, 6, 7, 8]).expect("second write");
        file.sync_all().expect("fsync at 8 bytes");
        // A third, never-fsynced append.
        file.write_at(8, &[9, 10]).expect("unsynced tail");

        fs.crash();

        // The durable extent moved to 8 with the second fsync; only the final
        // un-fsynced 2 bytes are lost.
        assert_eq!(fs.file_size(&path), Some(8));
        assert_eq!(fs.file_bytes(&path), Some(vec![1, 2, 3, 4, 5, 6, 7, 8]));
    }

    #[test]
    fn mem_crash_applies_per_file_durable_extents() {
        // The boundary is tracked per file: each file is truncated to its own
        // last-fsynced length independently.
        let fs = MemFileSystem::new();
        let dir = PathBuf::from("/wal");
        fs.create_dir_all(&dir).expect("mkdir");
        let synced = dir.join("synced.wal");
        let dirty = dir.join("dirty.wal");

        let synced_file = fs.open_read_write(&synced).expect("open synced");
        synced_file.write_at(0, &[1, 2, 3, 4, 5, 6]).expect("write");
        synced_file.sync_all().expect("fsync 6 bytes");
        synced_file.write_at(6, &[7, 8]).expect("unsynced tail");

        let dirty_file = fs.open_read_write(&dirty).expect("open dirty");
        dirty_file
            .write_at(0, &[42, 42, 42])
            .expect("write, never fsync");

        fs.crash();

        // The fsynced file keeps its durable 6-byte prefix.
        assert_eq!(fs.file_bytes(&synced), Some(vec![1, 2, 3, 4, 5, 6]));
        // The never-fsynced file is emptied.
        assert_eq!(fs.file_bytes(&dirty), Some(Vec::new()));
    }

    #[test]
    fn mem_crash_clears_last_write_marker() {
        // A crash clears the volatile last-write marker, so a torn-tail fault
        // armed afterwards cannot reference a write that did not survive.
        let fs = MemFileSystem::new();
        let dir = PathBuf::from("/wal");
        fs.create_dir_all(&dir).expect("mkdir");
        let path = dir.join("seg.wal");

        let file = fs.open_read_write(&path).expect("open");
        file.write_at(0, &[1, 2, 3, 4]).expect("durable write");
        file.sync_all().expect("fsync");
        file.write_at(4, &[5, 6, 7, 8]).expect("unsynced tail");

        fs.crash();
        assert_eq!(fs.file_bytes(&path), Some(vec![1, 2, 3, 4]));

        // With the marker cleared, tearing the last write is a no-op rather than
        // truncating the surviving durable prefix.
        fs.tear_last_write(2);
        assert_eq!(fs.file_bytes(&path), Some(vec![1, 2, 3, 4]));
    }
}
