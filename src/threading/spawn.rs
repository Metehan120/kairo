use crate::{
    thread::Runtime,
    threading::{
        inner::{block_on_inner, spawn_inner},
        thread_join::JoinHandle,
    },
};

/// Schedules a future with an empty name on the provided runtime.
///
/// Await the returned handle to obtain the result or a join error.
///
/// ```
/// let runtime = kairo::thread::Runtime::new().build();
/// let handle = kairo::thread::spawn(&runtime, async { 42 });
/// assert_eq!(kairo::thread::block_on(handle).unwrap(), 42);
/// ```
pub fn spawn<F>(runtime: &Runtime, future: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    spawn_with_name("", runtime, future)
}

/// Schedules a future on the provided runtime with a copied task name.
///
/// The name is available through the join handle's thread; it does not name
/// an OS thread.
pub fn spawn_with_name<F>(name: &str, runtime: &Runtime, future: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    spawn_inner(future, name, &runtime.scheduler)
}

/// Polls a future to completion on the calling thread.
///
/// The thread is parked when the future is pending and unparked by its waker.
/// The future is not submitted to a runtime's scheduler, and panics during
/// polling propagate to the caller.
pub fn block_on<F>(future: F) -> F::Output
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    block_on_inner(future)
}
