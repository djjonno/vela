//! Feature-gate note and check for the deterministic-simulation surface.
//!
//! `vela_log::sim` is gated behind the non-default `sim` Cargo feature
//! (Requirements 3.2, 7.1). With the feature **off** — the production default —
//! the module does not exist: the WAL `FileSystem`/`WalFile` seam traits and the
//! in-memory `FaultFileSystem` stay crate-private and no simulation-only item is
//! exposed, so production builds are byte-for-byte unchanged. That absence is
//! enforced at compile time by the `#[cfg(feature = "sim")]` gate in `lib.rs`
//! and `wal/mod.rs`; a test cannot *reference* `vela_log::sim` without the
//! feature, because the path simply is not there to name.
//!
//! With the feature **on**, the surface is reachable and usable. The single
//! gated test below proves that by naming each re-exported item and asserting
//! the fault filesystem implements the WAL `FileSystem` seam. Run it with:
//!
//! ```sh
//! cargo test -p vela-log --features sim
//! ```

/// With the `sim` feature enabled, `vela_log::sim` exposes the WAL filesystem
/// seam (`FileSystem`/`WalFile`) and the in-memory `FaultFileSystem`, and the
/// fault filesystem satisfies the `FileSystem` bound the harness injects through
/// `DurableWal::open_with`.
#[cfg(feature = "sim")]
#[test]
fn sim_surface_is_exposed_and_usable_with_feature_on() {
    use vela_log::sim::{FaultFileSystem, FileSystem, WalFile};

    // Naming the seam traits in bounds proves they are public under `sim`.
    fn assert_file_system<F: FileSystem>() {}
    fn assert_wal_file<W: WalFile>() {}

    // The injectable fault filesystem implements the `FileSystem` seam, and its
    // associated handle implements `WalFile` — exactly what the simulation
    // harness relies on to drive `DurableWal` over a deterministic disk.
    assert_file_system::<FaultFileSystem>();
    assert_wal_file::<<FaultFileSystem as FileSystem>::File>();
}
