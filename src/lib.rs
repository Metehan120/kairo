//! An async task runtime with configurable worker scheduling and timers.
//!
//! Use [`thread::Runtime`] for a configured scheduler, or the functions in
//! [`thread`] for the shared global scheduler. [`time::sleep`] provides a
//! timer future that also works with other Rust executors.
//!
//! ```
//! use std::time::Duration;
//! use kairo::{thread::Runtime, time::sleep};
//!
//! let runtime = Runtime::new().thread_count(2).build();
//! let task = runtime.spawn(async {
//!     sleep(Duration::from_millis(1)).await;
//!     42
//! });
//! assert_eq!(runtime.block_on(task).unwrap(), 42);
//! ```

mod task;
mod threading;
pub mod time;
/// Runtimes, task spawning, and join handles.
pub mod thread {
    pub use crate::threading::runtime::*;
    pub use crate::threading::spawn::*;
    pub use crate::threading::thread_join::*;
}
mod scheduler;
