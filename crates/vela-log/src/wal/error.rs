//! WAL-internal error helpers over [`crate::LogError`].
//!
//! Convenience constructors and mappings (e.g. wrapping a [`std::io::Error`]
//! into [`crate::LogError::Io`] with the in-progress operation name) live here.
//!
//! Implemented in a later task (Requirement 10).
