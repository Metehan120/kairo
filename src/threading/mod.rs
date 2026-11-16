pub mod inner;
pub mod runtime;
pub mod spawn;

/// Handles for observing and controlling spawned tasks.
pub mod thread_join {
    use std::{
        pin::Pin,
        sync::Arc,
        task::{Context, Poll, Waker},
    };

    use spin::mutex::Mutex;

    use crate::task::Task;

    /// A reference to a spawned task, not an operating-system thread.
    pub struct Thread {
        pub(crate) task: Arc<dyn Task>,
    }

    pub(crate) struct JoinState<T> {
        pub result: Option<Result<T, JoinError>>,
        pub waker: Option<Waker>,
    }

    impl Thread {
        /// Notifies the task that it may make progress.
        ///
        /// A task already queued, completed, or being cancelled is not queued
        /// again. A notification during polling is retained for a later poll.
        pub fn wake(&self) {
            self.task.clone().wake_task();
        }

        /// Returns the task's name, or an empty string if none was supplied.
        pub fn name(&self) -> &str {
            self.task.get_name()
        }
    }

    /// A future that yields the result of a spawned task.
    ///
    /// Dropping the handle does not request cancellation. The result is
    /// consumed when the handle resolves; use [`Self::cancel`] to request
    /// cancellation explicitly.
    pub struct JoinHandle<T> {
        pub(crate) state: Arc<Mutex<JoinState<T>>>,
        pub(crate) thread: Arc<Thread>,
    }

    /// The reason a spawned task did not return a value.
    #[derive(Debug)]
    pub enum JoinError {
        /// The task's future was dropped before producing a result.
        Cancelled,
        /// Polling the task's future panicked; contains the panic payload.
        Panic(Box<dyn std::any::Any + Send + 'static>),
    }

    impl<T> Future for JoinHandle<T> {
        type Output = Result<T, JoinError>;
        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            let mut state = self.state.lock();

            if let Some(result) = state.result.take() {
                Poll::Ready(result)
            } else {
                state.waker = Some(cx.waker().clone());
                Poll::Pending
            }
        }
    }

    impl<T> JoinHandle<T> {
        /// Requests cancellation, returning `true` if the request was accepted.
        ///
        /// A task being polled is cancelled after that poll returns pending;
        /// if it completes during the poll, its result can still win. Returns
        /// `false` if the task has completed or cancellation was already requested.
        pub fn cancel(&self) -> bool {
            self.thread.task.clone().cancel_task()
        }
        /// Returns a shared reference to the underlying task's [`Thread`].
        pub fn as_thread(&self) -> Arc<Thread> {
            self.thread.clone()
        }
    }
}
