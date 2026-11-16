//! Timer futures that work with Kairo or any executor using Rust wakers.

use std::{
    collections::BTreeMap,
    future::Future,
    pin::Pin,
    sync::{Condvar, LazyLock, Mutex},
    task::{Context, Poll, Waker},
    thread,
    time::{Duration, Instant},
};

type TimerKey = (Instant, u64);

struct Timers {
    next_id: u64,
    entries: BTreeMap<TimerKey, Waker>,
}

struct TimerService {
    timers: Mutex<Timers>,
    changed: Condvar,
}

static TIMER: LazyLock<TimerService> = LazyLock::new(|| {
    let service = TimerService {
        timers: Mutex::new(Timers {
            next_id: 0,
            entries: BTreeMap::new(),
        }),
        changed: Condvar::new(),
    };
    thread::Builder::new()
        .name("kairo-timer".into())
        .spawn(|| TIMER.run())
        .expect("failed to start timer thread");
    service
});

impl TimerService {
    fn run(&self) {
        let mut timers = self.timers.lock().unwrap();
        loop {
            match timers.entries.first_key_value() {
                None => timers = self.changed.wait(timers).unwrap(),
                Some((&(deadline, _), _)) => {
                    let now = Instant::now();
                    if deadline > now {
                        let (guard, _) = self.changed.wait_timeout(timers, deadline - now).unwrap();
                        timers = guard;
                    } else {
                        let (_, waker) = timers.entries.pop_first().unwrap();
                        drop(timers);
                        let _ = std::panic::catch_unwind(|| waker.wake());
                        timers = self.timers.lock().unwrap();
                    }
                }
            }
        }
    }
}

/// A future that completes once its deadline has passed.
///
/// Dropping it removes its registered timer, if any. It does not block the
/// thread polling it and can be used with any executor that supports wakers.
#[must_use = "futures do nothing unless polled"]
pub struct Sleep {
    deadline: Instant,
    key: Option<TimerKey>,
}

/// Creates a future that completes after `duration` has elapsed.
///
/// The deadline is measured from the call to `sleep`, not the first poll.
/// Timers share a lazily started background thread and may complete later than
/// their deadline depending on scheduling. A zero-duration sleep is ready on
/// its first poll.
///
/// # Panics
///
/// Panics if the deadline cannot be represented by [`Instant`]. The first
/// pending timer also panics if the timer thread cannot be started.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
/// use kairo::{thread::block_on, time::sleep};
///
/// block_on(async { sleep(Duration::from_millis(1)).await });
/// ```
pub fn sleep(duration: Duration) -> Sleep {
    Sleep {
        deadline: Instant::now()
            .checked_add(duration)
            .expect("timer deadline overflow"),
        key: None,
    }
}

impl Future for Sleep {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if Instant::now() >= self.deadline {
            if let Some(key) = self.key.take() {
                let removed = TIMER.timers.lock().unwrap().entries.remove(&key);
                drop(removed);
            }
            return Poll::Ready(());
        }

        let waker = cx.waker().clone();
        let mut timers = TIMER.timers.lock().unwrap();
        if Instant::now() >= self.deadline {
            let removed = self.key.take().and_then(|key| timers.entries.remove(&key));
            drop(timers);
            drop(removed);
            return Poll::Ready(());
        }
        let key = match self.key {
            Some(key) => key,
            None => {
                let id = timers.next_id;
                timers.next_id = id.checked_add(1).expect("timer ID overflow");
                let key = (self.deadline, id);
                self.key = Some(key);
                key
            }
        };
        let previous = timers.entries.insert(key, waker);
        drop(timers);
        drop(previous);
        TIMER.changed.notify_one();
        Poll::Pending
    }
}

impl Drop for Sleep {
    fn drop(&mut self) {
        if let Some(key) = self.key.take() {
            let removed = TIMER.timers.lock().unwrap().entries.remove(&key);
            drop(removed);
            TIMER.changed.notify_one();
        }
    }
}
