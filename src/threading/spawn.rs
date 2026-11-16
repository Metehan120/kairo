use crate::{
    scheduler::GENERAL_SCHEDULER,
    threading::{
        inner::{block_on_inner, spawn_inner},
        thread_join::JoinHandle,
    },
};

/// Schedules a future with an empty name on the lazily initialized global
/// scheduler. Its worker count is determined by `num_cpus::get()`.
///
/// Await the returned handle to obtain the result or a join error.
///
/// ```
/// let handle = kairo::thread::spawn(async { 42 });
/// assert_eq!(kairo::thread::block_on(handle).unwrap(), 42);
/// ```
pub fn spawn<F>(future: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    spawn_with_name("", future)
}

/// Schedules a future on the global scheduler with a copied task name.
///
/// The name is available through the join handle's thread; it does not name
/// an OS thread.
pub fn spawn_with_name<F>(name: &str, future: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    spawn_inner(future, name, &GENERAL_SCHEDULER)
}

/// Polls a future to completion on the calling thread.
///
/// The thread is parked when the future is pending and unparked by its waker.
/// The future is not submitted to the global scheduler, and panics during
/// polling propagate to the caller.
pub fn block_on<F>(future: F) -> F::Output
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    block_on_inner(future)
}
