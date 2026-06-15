//! WAL configuration and sync policy.
//!
//! Defines [`WalConfig`] (the Data_Directory plus the optional Segment_Size and
//! Sync_Policy) and [`SyncPolicy`] (`Always`, `Periodic { interval_ms }`,
//! `Never`), with the defaults fixed by Requirement 11 (64 MiB segments,
//! `Always` sync, a 1000 ms periodic interval) and validation that rejects a
//! zero Segment_Size or a zero `Periodic` interval with
//! [`crate::LogError::Config`].
//!
//! Beyond plain validation, [`WalConfig::prepare`] performs the filesystem side
//! of opening a log against the [`FileSystem`] seam: it validates the
//! configuration, creates the Data_Directory, and acquires the exclusive
//! directory lock, in that order, so that an invalid configuration never
//! touches the filesystem and an already-held directory is left unmodified
//! (Requirements 11.4, 11.5, 11.7, 11.8). `DurableWal::open` (task 8) calls it
//! and keeps the returned lock guard alive for the lifetime of the log.

use std::path::PathBuf;

use super::fs::FileSystem;
use crate::LogError;

/// Default Segment_Size of 67108864 bytes (64 MiB), applied when the
/// configuration omits one (Requirement 11.2).
pub const DEFAULT_SEGMENT_SIZE: u64 = 64 * 1024 * 1024;

/// Default `Periodic` flush interval of 1000 milliseconds, applied when the
/// `Periodic` policy is selected without an explicit interval (Requirement
/// 11.6).
pub const DEFAULT_PERIODIC_INTERVAL_MS: u64 = 1000;

/// The durability policy controlling when buffered Record_Frames are forced to
/// stable storage (Requirement 4).
///
/// Only [`Always`](SyncPolicy::Always) delivers the persist-before-acknowledge
/// guarantee consensus requires (Requirement 4.7); it is therefore the
/// [`Default`]. [`Periodic`](SyncPolicy::Periodic) and [`Never`](SyncPolicy::Never)
/// are permitted only for logs that do not back consensus.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SyncPolicy {
    /// Force buffered Record_Frames (and the covering Frame_Metadata) to stable
    /// storage before each mutating operation returns. The only consensus-safe
    /// policy, and the default (Requirements 4.1, 4.7, 11.3).
    #[default]
    Always,
    /// Force buffered Record_Frames at most `interval_ms` apart while the log is
    /// actively servicing operations (Requirement 4.2). A zero interval is
    /// invalid (Requirement 11.7).
    Periodic {
        /// The maximum wall-clock gap, in milliseconds, between consecutive
        /// forces during active operation.
        interval_ms: u64,
    },
    /// Write Record_Frames to the operating system without ever forcing them to
    /// stable storage (Requirement 4.3).
    Never,
}

impl SyncPolicy {
    /// The `Periodic` policy with the default interval (Requirement 11.6).
    ///
    /// Use this where the configuration selects `Periodic` but omits the
    /// interval, rather than constructing `Periodic { interval_ms: 0 }` (which
    /// is rejected by validation).
    pub fn periodic_default() -> Self {
        SyncPolicy::Periodic {
            interval_ms: DEFAULT_PERIODIC_INTERVAL_MS,
        }
    }
}

/// Configuration for opening a [`DurableWal`](super::DurableWal).
///
/// `data_dir` is required; `segment_size` and `sync_policy` carry the
/// Requirement 11 defaults when constructed via [`WalConfig::new`] and can be
/// overridden with the builder methods (Requirement 11.1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalConfig {
    /// The Data_Directory in which this partition log stores its Segment files.
    pub data_dir: PathBuf,
    /// The maximum size, in bytes, of a single Segment before rollover.
    /// Defaults to [`DEFAULT_SEGMENT_SIZE`]; must be non-zero (Requirement 11.5).
    pub segment_size: u64,
    /// When buffered Record_Frames are forced to stable storage. Defaults to
    /// [`SyncPolicy::Always`].
    pub sync_policy: SyncPolicy,
}

impl WalConfig {
    /// Construct a configuration for `data_dir` with the default Segment_Size
    /// (64 MiB) and the default Sync_Policy (`Always`) (Requirements 11.2,
    /// 11.3).
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        Self {
            data_dir: data_dir.into(),
            segment_size: DEFAULT_SEGMENT_SIZE,
            sync_policy: SyncPolicy::default(),
        }
    }

    /// Override the Segment_Size (builder style).
    pub fn with_segment_size(mut self, segment_size: u64) -> Self {
        self.segment_size = segment_size;
        self
    }

    /// Override the Sync_Policy (builder style).
    pub fn with_sync_policy(mut self, sync_policy: SyncPolicy) -> Self {
        self.sync_policy = sync_policy;
        self
    }

    /// Validate the configuration without touching the filesystem.
    ///
    /// Rejects a zero Segment_Size (Requirement 11.5) and a `Periodic` policy
    /// with a zero interval (Requirement 11.7) with [`LogError::Config`], whose
    /// message describes what was invalid. All other configurations are
    /// accepted.
    pub fn validate(&self) -> Result<(), LogError> {
        if self.segment_size == 0 {
            return Err(LogError::Config {
                detail: "segment_size must be greater than zero".to_string(),
            });
        }
        if let SyncPolicy::Periodic { interval_ms: 0 } = self.sync_policy {
            return Err(LogError::Config {
                detail: "periodic sync interval_ms must be greater than zero".to_string(),
            });
        }
        Ok(())
    }

    /// Validate, create the Data_Directory, and acquire its exclusive lock.
    ///
    /// This is the filesystem half of `open`, kept generic over the
    /// [`FileSystem`] seam so it is callable with the real filesystem in
    /// production and the in-memory fault filesystem in tests. The ordering is
    /// deliberate (Requirement 11):
    ///
    /// 1. **Validate first** so an invalid configuration neither creates a
    ///    directory nor acquires a lock — there is no partial initialization
    ///    (Requirements 11.5, 11.7).
    /// 2. **Create the directory** (a no-op if it already exists); a failure
    ///    here surfaces as [`LogError::Io`] with no partial init (Requirement
    ///    11.4).
    /// 3. **Acquire the exclusive lock** last; the lock sentinel lives inside
    ///    the directory, so it can only be taken once the directory exists. If
    ///    the lock is already held — by another live instance, whose directory
    ///    therefore already exists, making step 2 a no-op — this fails with
    ///    [`LogError::Io`] and the directory is left unmodified (Requirement
    ///    11.8).
    ///
    /// On success the returned `F::Lock` guard holds the directory lock; the
    /// caller must keep it alive for the lifetime of the open log (dropping it
    /// releases the lock).
    pub(crate) fn prepare<F: FileSystem>(&self, fs: &F) -> Result<F::Lock, LogError> {
        self.validate()?;

        fs.create_dir_all(&self.data_dir)
            .map_err(|source| LogError::Io {
                op: "create data directory",
                source,
            })?;

        let lock = fs
            .lock_exclusive(&self.data_dir)
            .map_err(|source| LogError::Io {
                op: "lock data directory",
                source,
            })?;

        Ok(lock)
    }
}

#[cfg(test)]
mod tests {
    use super::super::fs::fault::MemFileSystem;
    use super::super::fs::FileSystem;
    use super::*;
    use std::path::Path;

    // --- defaults (R11.2, R11.3, R11.6) ------------------------------------

    #[test]
    fn new_applies_segment_size_and_sync_policy_defaults() {
        let cfg = WalConfig::new("/wal");
        assert_eq!(cfg.segment_size, DEFAULT_SEGMENT_SIZE);
        assert_eq!(cfg.segment_size, 67_108_864); // 64 MiB, spelled out (R11.2).
        assert_eq!(cfg.sync_policy, SyncPolicy::Always);
    }

    #[test]
    fn sync_policy_default_is_always() {
        assert_eq!(SyncPolicy::default(), SyncPolicy::Always);
    }

    #[test]
    fn periodic_default_uses_1000_ms() {
        assert_eq!(
            SyncPolicy::periodic_default(),
            SyncPolicy::Periodic { interval_ms: 1000 }
        );
        assert_eq!(DEFAULT_PERIODIC_INTERVAL_MS, 1000);
    }

    #[test]
    fn builders_override_defaults() {
        let cfg = WalConfig::new("/wal")
            .with_segment_size(4096)
            .with_sync_policy(SyncPolicy::Never);
        assert_eq!(cfg.segment_size, 4096);
        assert_eq!(cfg.sync_policy, SyncPolicy::Never);
    }

    // --- validation (R11.5, R11.7) -----------------------------------------

    #[test]
    fn validate_accepts_default_config() {
        assert!(WalConfig::new("/wal").validate().is_ok());
    }

    #[test]
    fn validate_accepts_nonzero_periodic_interval() {
        let cfg = WalConfig::new("/wal").with_sync_policy(SyncPolicy::periodic_default());
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn validate_rejects_zero_segment_size() {
        let cfg = WalConfig::new("/wal").with_segment_size(0);
        assert!(matches!(cfg.validate(), Err(LogError::Config { .. })));
    }

    #[test]
    fn validate_rejects_zero_periodic_interval() {
        let cfg = WalConfig::new("/wal").with_sync_policy(SyncPolicy::Periodic { interval_ms: 0 });
        assert!(matches!(cfg.validate(), Err(LogError::Config { .. })));
    }

    // --- prepare: validation is enforced with no partial init (R11.5, R11.7)

    #[test]
    fn prepare_rejects_invalid_config_without_creating_dir() {
        let fs = MemFileSystem::new();
        let cfg = WalConfig::new("/wal").with_segment_size(0);

        let err = cfg.prepare(&fs).unwrap_err();
        assert!(matches!(err, LogError::Config { .. }));
        // No partial initialization: the directory was never created.
        assert!(!fs.exists(Path::new("/wal")));
    }

    #[test]
    fn prepare_rejects_zero_periodic_interval_without_touching_fs() {
        let fs = MemFileSystem::new();
        let cfg = WalConfig::new("/wal").with_sync_policy(SyncPolicy::Periodic { interval_ms: 0 });

        let err = cfg.prepare(&fs).unwrap_err();
        assert!(matches!(err, LogError::Config { .. }));
        assert!(!fs.exists(Path::new("/wal")));
    }

    // --- prepare: uncreatable directory (R11.4) ----------------------------

    #[test]
    fn prepare_maps_uncreatable_dir_to_io_with_no_partial_init() {
        let fs = MemFileSystem::new();
        fs.arm_create_dir_failure();
        let cfg = WalConfig::new("/wal");

        let err = cfg.prepare(&fs).unwrap_err();
        assert!(matches!(
            err,
            LogError::Io { op, .. } if op == "create data directory"
        ));
        // The failed create left no directory behind.
        assert!(!fs.exists(Path::new("/wal")));
    }

    // --- prepare: exclusive lock lifecycle (R11.8) -------------------------

    #[test]
    fn prepare_succeeds_then_holds_an_exclusive_lock() {
        let fs = MemFileSystem::new();
        let dir = PathBuf::from("/wal");

        let lock = WalConfig::new(&dir)
            .prepare(&fs)
            .expect("first prepare should succeed");
        assert!(fs.exists(&dir));

        // A second open of the same directory is refused while the lock is held.
        let err = WalConfig::new(&dir).prepare(&fs).unwrap_err();
        assert!(matches!(
            err,
            LogError::Io { op, .. } if op == "lock data directory"
        ));

        // Releasing the guard frees the lock, so a later open succeeds again.
        drop(lock);
        WalConfig::new(&dir)
            .prepare(&fs)
            .expect("prepare after unlock should succeed");
    }

    #[test]
    fn prepare_fails_io_when_dir_lock_already_held_and_leaves_dir_unmodified() {
        let fs = MemFileSystem::new();
        let dir = PathBuf::from("/wal");
        // Model another live instance: the directory exists and its lock is held.
        fs.create_dir_all(&dir).unwrap();
        fs.hold_lock(&dir);

        let err = WalConfig::new(&dir).prepare(&fs).unwrap_err();
        assert!(matches!(
            err,
            LogError::Io { op, .. } if op == "lock data directory"
        ));
        // The directory was not modified: no segment files were created.
        assert!(fs.read_dir(&dir).unwrap().is_empty());
    }
}
