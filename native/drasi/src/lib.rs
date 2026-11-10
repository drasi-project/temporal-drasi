//! Versioned native host for Temporal Watch.

#![deny(unnameable_test_items)]

mod checkpoint;
pub mod credentials;
mod ffi;
mod owned_query;
mod plugins;
pub mod protocol;
mod query;
pub mod runtime;
mod source;

pub use ffi::*;
#[doc(hidden)]
pub use plugins::worker::plugin_worker_main;
