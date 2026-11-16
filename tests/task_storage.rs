use std::{
    cell::Cell,
    future::Future,
    marker::PhantomPinned,
    mem::size_of,
    pin::Pin,
    sync::mpsc::{self, Receiver, Sender},
    task::{Context, Poll},
    time::Duration,
};

use kairo::thread::Runtime;

const TIMEOUT: Duration = Duration::from_secs(10);
const POLLS: usize = 8;

struct WakeFuture<const N: usize> {
    payload: [u8; N],
    polls: Cell<usize>,
    first_address: Cell<usize>,
    stable: Cell<bool>,
    dropped: Sender<()>,
    _pin: PhantomPinned,
}

struct Output {
    polls: usize,
    stable: bool,
    checksum: usize,
    dropped: Sender<()>,
}

impl Drop for Output {
    fn drop(&mut self) {
        let _ = self.dropped.send(());
    }
}

impl<const N: usize> Future for WakeFuture<N> {
    type Output = Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.as_ref().get_ref();
        let address = this as *const Self as usize;
        let polls = this.polls.get() + 1;
        if polls == 1 {
            this.first_address.set(address);
        } else if this.first_address.get() != address {
            this.stable.set(false);
        }
        this.polls.set(polls);
        if polls < POLLS {
            cx.waker().wake_by_ref();
            Poll::Pending
        } else {
            Poll::Ready(Output {
                polls,
                stable: this.stable.get(),
                checksum: this.payload.iter().map(|&value| usize::from(value)).sum(),
                dropped: this.dropped.clone(),
            })
        }
    }
}

impl<const N: usize> Drop for WakeFuture<N> {
    fn drop(&mut self) {
        let _ = self.dropped.send(());
    }
}

fn receive<T>(receiver: &Receiver<T>) -> T {
    receiver
        .recv_timeout(TIMEOUT)
        .expect("task did not complete in time")
}

fn exercise_storage<const N: usize>() {
    let runtime = Runtime::new().thread_count(2).build();
    let (drop_tx, drop_rx) = mpsc::channel();
    let future = WakeFuture {
        payload: [7; N],
        polls: Cell::new(0),
        first_address: Cell::new(0),
        stable: Cell::new(true),
        dropped: drop_tx,
        _pin: PhantomPinned,
    };
    let handle = runtime.spawn(future);
    let (result_tx, result_rx) = mpsc::channel();
    runtime.spawn(async move {
        let _ = result_tx.send(handle.await);
    });

    let output = receive(&result_rx).unwrap();
    assert_eq!(output.polls, POLLS);
    assert!(output.stable, "pinned future moved between polls");
    assert_eq!(output.checksum, 7 * N);
    receive(&drop_rx); // The completed future is dropped independently of its output.
    assert!(drop_rx.try_recv().is_err(), "output dropped prematurely");
    drop(output);
    receive(&drop_rx);
    assert!(drop_rx.try_recv().is_err(), "unexpected extra drop");
}

#[test]
fn inline_future_self_wakes_without_moving() {
    assert!(size_of::<WakeFuture<16>>() < 2048);
    exercise_storage::<16>();
}

#[test]
fn large_future_self_wakes_and_preserves_output_and_drops() {
    assert!(size_of::<WakeFuture<4096>>() > 2048);
    exercise_storage::<4096>();
}
