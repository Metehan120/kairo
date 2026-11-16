use crate::{
    scheduler::Scheduler,
    threading::{
        inner::{block_on_inner, spawn_inner},
        thread_join::JoinHandle,
    },
};

pub use crate::scheduler::WakePolicy;

/// Configures a [`Runtime`] before starting its worker threads.
pub struct RuntimeBuilder {
    thread_count: usize,
    wake_policy: WakePolicy,
}

/// A handle to a scheduler used to run spawned futures on worker threads.
///
/// Clones share the same scheduler. [`Runtime::block_on`] instead polls its
/// future on the calling thread.
///
/// Dropping a runtime handle does not necessarily stop its workers immediately:
/// spawned tasks and their handles can retain scheduler references. When the
/// last scheduler reference is released, shutdown wakes and joins its workers
/// (except the current worker, if shutdown happens there).
#[derive(Debug, Clone)]
pub struct Runtime {
    scheduler: Scheduler,
}

impl RuntimeBuilder {
    /// Sets the number of scheduler worker threads.
    ///
    /// Zero is accepted, but spawning on the resulting runtime panics because
    /// no worker is available.
    pub fn thread_count(self, count: usize) -> RuntimeBuilder {
        RuntimeBuilder {
            thread_count: count,
            ..self
        }
    }

    /// Sets how the scheduler breaks ties when choosing a worker to wake.
    pub fn wake_policy(self, policy: WakePolicy) -> RuntimeBuilder {
        RuntimeBuilder {
            wake_policy: policy,
            ..self
        }
    }

    /// Starts the configured workers and returns a runtime.
    ///
    /// Each call creates a separate scheduler.
    pub fn build(&self) -> Runtime {
        let scheduler = Scheduler::with_wake_policy(self.thread_count, self.wake_policy);
        Runtime {
            scheduler: scheduler,
        }
    }
}

impl Runtime {
    /// Returns a builder using `num_cpus::get()` workers and the default
    /// [`WakePolicy::PreferFirst`] policy.
    ///
    /// ```
    /// let runtime = kairo::thread::Runtime::new().thread_count(2).build();
    /// let handle = runtime.spawn(async { 42 });
    /// assert_eq!(runtime.block_on(handle).unwrap(), 42);
    /// ```
    pub fn new() -> RuntimeBuilder {
        RuntimeBuilder {
            thread_count: num_cpus::get(),
            wake_policy: WakePolicy::default(),
        }
    }

    /// Polls a future to completion on the calling thread, parking that thread
    /// when the future is pending until it is woken.
    ///
    /// This does not submit the future to this runtime's scheduler. Panics
    /// from polling the future propagate to the caller. Calling this on a
    /// worker can deadlock if the awaited work needs that worker to progress.
    pub fn block_on<F>(&self, future: F) -> F::Output
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        block_on_inner(future)
    }

    /// Schedules a future on this runtime with an empty name.
    ///
    /// The returned handle can be awaited for the result or used to request
    /// cancellation. Panics if the scheduler has no workers.
    pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.spawn_with_name("", future)
    }

    /// Schedules a future on this runtime with the given task name.
    ///
    /// The name is copied and can be retrieved through the join handle's
    /// thread. It does not name an OS thread. Panics if there are no workers.
    pub fn spawn_with_name<F>(&self, name: &str, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        spawn_inner(future, name, &self.scheduler)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_defaults_to_prefer_first_and_accepts_round_robin() {
        assert_eq!(Runtime::new().wake_policy, WakePolicy::PreferFirst);
        assert_eq!(
            Runtime::new()
                .wake_policy(WakePolicy::RoundRobin)
                .wake_policy,
            WakePolicy::RoundRobin
        );
    }
}
