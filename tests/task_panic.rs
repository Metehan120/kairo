use std::{sync::mpsc, task::Poll, time::Duration};
use kairo::thread::{JoinError, Runtime};

#[test]
fn panicking_task_reports_payload_and_worker_survives() {
    let runtime = Runtime::new().thread_count(1).build();
    let handle = runtime.spawn(async {
        let mut first = true;
        std::future::poll_fn(move |cx| {
            if first {
                first = false;
                cx.waker().wake_by_ref();
                Poll::<()>::Pending
            } else {
                panic!("task panic payload")
            }
        })
        .await
    });
    let (tx, rx) = mpsc::channel();
    runtime.spawn(async move {
        let _ = tx.send(handle.await);
    });
    let error = rx
        .recv_timeout(Duration::from_secs(5))
        .expect("join timed out")
        .unwrap_err();
    match error {
        JoinError::Panic(payload) => {
            assert_eq!(payload.downcast_ref::<&str>(), Some(&"task panic payload"))
        }
        JoinError::Cancelled => panic!("panic was reported as cancellation"),
    }

    let (tx, rx) = mpsc::channel();
    runtime.spawn(async move {
        tx.send(42).unwrap();
    });
    assert_eq!(
        rx.recv_timeout(Duration::from_secs(5))
            .expect("worker stopped"),
        42
    );
}
