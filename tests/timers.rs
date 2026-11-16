use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
    task::{Context, Poll, Wake, Waker},
    time::{Duration, Instant},
};
use kairo::{thread::Runtime, time::sleep};

#[test]
fn sleep_wakes_kairo_tasks_no_earlier_than_deadline() {
    let runtime = Runtime::new().thread_count(1).build();
    let (tx, rx) = mpsc::channel();
    let start = Instant::now();
    runtime.spawn(async move {
        sleep(Duration::from_millis(35)).await;
        tx.send(start.elapsed()).unwrap();
    });
    assert!(rx.recv_timeout(Duration::from_secs(5)).unwrap() >= Duration::from_millis(35));
}

#[test]
fn earlier_timer_interrupts_longer_wait() {
    let runtime = Runtime::new().thread_count(1).build();
    let (tx, rx) = mpsc::channel();
    let (ready_tx, ready_rx) = mpsc::channel();
    let long_task = runtime.spawn(async move {
        let mut long = Box::pin(sleep(Duration::from_secs(10)));
        std::future::poll_fn(|cx| {
            assert!(long.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        ready_tx.send(()).unwrap();
        std::future::pending::<()>().await;
    });
    ready_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    runtime.spawn(async move {
        sleep(Duration::from_millis(10)).await;
        tx.send(()).unwrap();
    });
    rx.recv_timeout(Duration::from_secs(2))
        .expect("earlier timer did not wake");
    assert!(long_task.cancel());
}

struct Counter(AtomicUsize);
impl Wake for Counter {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn repoll_replaces_waker_and_drop_releases_it() {
    let mut timer = Box::pin(sleep(Duration::from_secs(60)));
    let first = Arc::new(Counter(AtomicUsize::new(0)));
    let second = Arc::new(Counter(AtomicUsize::new(0)));
    let first_waker = Waker::from(first.clone());
    let second_waker = Waker::from(second.clone());
    assert!(
        Pin::as_mut(&mut timer)
            .poll(&mut Context::from_waker(&first_waker))
            .is_pending()
    );
    assert!(
        Pin::as_mut(&mut timer)
            .poll(&mut Context::from_waker(&second_waker))
            .is_pending()
    );
    assert_eq!(Arc::strong_count(&first), 2);
    assert_eq!(Arc::strong_count(&second), 3);
    drop(timer);
    assert_eq!(Arc::strong_count(&second), 2);
}

#[test]
fn zero_duration_is_ready() {
    let waker = Waker::noop();
    let mut timer = Box::pin(sleep(Duration::ZERO));
    assert!(
        timer
            .as_mut()
            .poll(&mut Context::from_waker(waker))
            .is_ready()
    );
}
