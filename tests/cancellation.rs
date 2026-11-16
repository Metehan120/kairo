use std::{
    future::{Future, poll_fn},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    task::Poll,
    time::Duration,
};

use kairo::thread::Runtime;

const TIMEOUT: Duration = Duration::from_secs(5);

// Await on a separate executor so a broken join cannot hang the test thread.
fn await_with_timeout<F>(future: F) -> F::Output
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let executor = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("build join executor");
        let _ = tx.send(executor.block_on(future));
    });
    rx.recv_timeout(TIMEOUT).expect("join timed out")
}

struct DropSignal {
    dropped: mpsc::Sender<bool>,
    polling: Arc<AtomicBool>,
}

impl Drop for DropSignal {
    fn drop(&mut self) {
        let _ = self.dropped.send(self.polling.load(Ordering::SeqCst));
    }
}

#[test]
fn cancel_idle_pending_future() {
    let runtime = Runtime::new().thread_count(1).build();
    let (polled_tx, polled_rx) = mpsc::channel();
    let (dropped_tx, dropped_rx) = mpsc::channel();
    let guard = DropSignal {
        dropped: dropped_tx,
        polling: Arc::new(AtomicBool::new(false)),
    };
    let handle = runtime.spawn(async move {
        let _guard = guard;
        poll_fn(move |_| {
            let _ = polled_tx.send(());
            Poll::<()>::Pending
        })
        .await
    });

    polled_rx
        .recv_timeout(TIMEOUT)
        .expect("task was not polled");
    // A subsequent task on the sole worker proves the pending poll returned.
    assert!(await_with_timeout(runtime.spawn(async {})).is_ok());
    assert!(handle.cancel(), "first cancellation should succeed");
    assert!(!handle.cancel(), "repeated cancellation should not succeed");
    assert!(
        await_with_timeout(handle).is_err(),
        "join should report cancellation"
    );
    assert!(
        !dropped_rx
            .recv_timeout(TIMEOUT)
            .expect("future was not dropped")
    );
}

#[test]
fn cancel_while_poll_is_running_defers_drop_until_poll_returns() {
    let runtime = Runtime::new().thread_count(1).build();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let (dropped_tx, dropped_rx) = mpsc::channel();
    let polling = Arc::new(AtomicBool::new(false));
    let guard = DropSignal {
        dropped: dropped_tx,
        polling: Arc::clone(&polling),
    };
    let handle = runtime.spawn(async move {
        let _guard = guard;
        poll_fn(move |_| {
            polling.store(true, Ordering::SeqCst);
            let _ = entered_tx.send(());
            let _ = release_rx.recv_timeout(TIMEOUT);
            polling.store(false, Ordering::SeqCst);
            Poll::<()>::Pending
        })
        .await
    });

    entered_rx
        .recv_timeout(TIMEOUT)
        .expect("poll did not start");
    let (cancel_tx, cancel_rx) = mpsc::channel();
    std::thread::spawn(move || {
        let cancelled = handle.cancel();
        let _ = cancel_tx.send((cancelled, handle));
    });
    let (cancelled, handle) = cancel_rx
        .recv_timeout(TIMEOUT / 2)
        .expect("cancel blocked while poll was running");
    assert!(cancelled);
    assert!(
        matches!(dropped_rx.try_recv(), Err(mpsc::TryRecvError::Empty)),
        "future dropped before poll returned"
    );
    release_tx.send(()).expect("poll exited before release");
    assert!(
        await_with_timeout(handle).is_err(),
        "join should report cancellation"
    );
    assert!(
        !dropped_rx
            .recv_timeout(TIMEOUT)
            .expect("future was not dropped"),
        "future dropped inside poll"
    );
}

#[test]
fn cancel_queued_task_before_it_is_polled() {
    let runtime = Runtime::new().thread_count(1).build();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let blocker = runtime.spawn(async move {
        let _ = entered_tx.send(());
        let _ = release_rx.recv_timeout(TIMEOUT);
    });
    entered_rx
        .recv_timeout(TIMEOUT)
        .expect("worker did not start blocker");

    let (dropped_tx, dropped_rx) = mpsc::channel();
    let polled = Arc::new(AtomicBool::new(false));
    let was_polled = Arc::clone(&polled);
    let guard = DropSignal {
        dropped: dropped_tx,
        polling: Arc::new(AtomicBool::new(false)),
    };
    let handle = runtime.spawn(async move {
        let _guard = guard;
        was_polled.store(true, Ordering::SeqCst);
    });
    assert!(handle.cancel());
    release_tx.send(()).expect("worker exited before release");
    assert!(
        await_with_timeout(handle).is_err(),
        "queued task should be cancelled"
    );
    assert!(await_with_timeout(blocker).is_ok());
    assert!(
        !polled.load(Ordering::SeqCst),
        "cancelled queued task was polled"
    );
    dropped_rx
        .recv_timeout(TIMEOUT)
        .expect("queued future was not dropped");
}

#[test]
fn successful_task_returns_ok() {
    let runtime = Runtime::new().thread_count(1).build();
    let handle = runtime.spawn(async { 42 });
    assert!(matches!(await_with_timeout(handle), Ok(42)));
}
