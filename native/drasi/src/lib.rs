//! Versioned native host for Temporal Watch.

#![deny(unnameable_test_items)]

mod checkpoint;
pub mod credentials;
mod ffi;
mod plugins;
pub mod protocol;
mod query;
pub mod runtime;
mod source;

pub use ffi::*;
