use crate::{
    scheduler::{Scheduler, SchedulerPublic},
    task::new_task,
    threading::thread_join::{JoinError, JoinHandle, JoinState},
};
use spin::mutex::Mutex;
use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, Wake, Waker},
    thread,
};

struct CancellationGuard<T> {
    state: Arc<Mutex<JoinState<T>>>,
    armed: bool,
}

impl<T> Drop for CancellationGuard<T> {
    fn drop(&mut self) {
        if self.armed {
            let waker = {
                let mut state = self.state.lock();
                state.result = Some(Err(JoinError::Cancelled));
                state.waker.take()
            };
            if let Some(waker) = waker {
                waker.wake();
            }
        }
    }
}

pub fn spawn_inner<F>(future: F, name: &str, scheduler: &Scheduler) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let state = Arc::new(Mutex::new(JoinState {
        result: None,
        waker: None,
    }));

    let task_state = Arc::clone(&state);
    let guard = CancellationGuard {
        state: Arc::clone(&state),
        armed: true,
    };
    let task = new_task(
        async move {
            let mut guard = guard;
            let result = CatchPanic(future).await.map_err(JoinError::Panic);
            let waker = {
                let mut state = task_state.lock();
                state.result = Some(result);
                guard.armed = false;
                state.waker.take()
            };
            if let Some(waker) = waker {
                waker.wake();
            }
        },
        name.to_string(),
        scheduler,
    );
    scheduler
        .schedule(task.clone())
        .expect("no scheduler worker available");
    JoinHandle {
        state,
        thread: Arc::new(crate::thread::Thread { task }),
    }
}

struct CatchPanic<F>(F);

impl<F: Future> Future for CatchPanic<F> {
    type Output = Result<F::Output, Box<dyn std::any::Any + Send>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // The inner future stays pinned for as long as the wrapper is pinned.
        let future = unsafe { self.map_unchecked_mut(|this| &mut this.0) };
        match catch_unwind(AssertUnwindSafe(|| future.poll(cx))) {
            Ok(Poll::Ready(output)) => Poll::Ready(Ok(output)),
            Ok(Poll::Pending) => Poll::Pending,
            Err(payload) => Poll::Ready(Err(payload)),
        }
    }
}

struct ThreadWaker(thread::Thread);

impl Wake for ThreadWaker {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

pub fn block_on_inner<F: Future>(future: F) -> F::Output {
    let waker = Waker::from(Arc::new(ThreadWaker(thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = std::pin::pin!(future);

    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => thread::park(),
        }
    }
}
