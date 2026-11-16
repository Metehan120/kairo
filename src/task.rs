use std::{
    cell::UnsafeCell,
    pin::Pin,
    sync::{
        Arc,
        atomic::{
            AtomicU16,
            Ordering::{self},
        },
    },
    task::{Context, Poll, Wake, Waker},
};

use crate::scheduler::{Scheduler, SchedulerPublic, States};

const BOX_THRESHOLD: usize = 2048;

pub struct TaskHeader {
    pub state: AtomicU16,
    pub scheduler: Scheduler,
}

pub trait Task: Send + Sync {
    fn header(&self) -> &TaskHeader;
    fn get_name(&self) -> &str;
    fn poll_task(self: Arc<Self>);
    fn wake_task(self: Arc<Self>);
    fn cancel_task(self: Arc<Self>) -> bool;
}

struct TaskStorage<F> {
    header: TaskHeader,
    future: UnsafeCell<Option<F>>,
    name: String,
}

unsafe impl<F: Send> Sync for TaskStorage<F> {}

pub fn new_task<F>(future: F, name: String, scheduler: &Scheduler) -> Arc<dyn Task>
where
    F: Future<Output = ()> + Send + 'static,
{
    if std::mem::size_of::<F>() > BOX_THRESHOLD {
        allocate_task(Box::pin(future), name, scheduler)
    } else {
        allocate_task(future, name, scheduler)
    }
}

fn allocate_task<F>(future: F, name: String, scheduler: &Scheduler) -> Arc<dyn Task>
where
    F: Future<Output = ()> + Send + 'static,
{
    Arc::new(TaskStorage {
        header: TaskHeader {
            state: AtomicU16::new(States::Queued as u16),
            scheduler: scheduler.clone(),
        },
        future: UnsafeCell::new(Some(future)),
        name,
    })
}

impl<F> Task for TaskStorage<F>
where
    F: Future<Output = ()> + Send + 'static,
{
    fn header(&self) -> &TaskHeader {
        &self.header
    }

    fn get_name(&self) -> &str {
        &self.name
    }

    fn poll_task(self: Arc<Self>) {
        let waker = Waker::from(Arc::clone(&self));
        let mut cx = Context::from_waker(&waker);
        let future = unsafe { Pin::new_unchecked(&mut *self.future.get()) }
            .as_pin_mut()
            .expect("idle task must have a future");

        match future.poll(&mut cx) {
            Poll::Ready(()) => self
                .header
                .state
                .store(States::Completed as u16, Ordering::Release),
            Poll::Pending => self.notify_running(),
        }
    }

    fn wake_task(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn cancel_task(self: Arc<Self>) -> bool {
        self.cancel()
    }
}

impl<F> TaskStorage<F>
where
    F: Future<Output = ()> + Send + 'static,
{
    fn schedule(self: &Arc<Self>) {
        let task: Arc<dyn Task> = self.clone();
        self.header
            .scheduler
            .schedule(task)
            .expect("no scheduler worker available");
    }

    fn notify_running(self: &Arc<Self>) {
        loop {
            let state = self.header.state.load(Ordering::Acquire);

            let next = if state == States::Idle as u16 {
                States::Running
            } else if state == States::Notified as u16 {
                States::Queued
            } else if state == States::Cancelling as u16 {
                self.drop_future();
                self.header
                    .state
                    .store(States::Cancelled as u16, Ordering::Release);
                return;
            } else {
                unreachable!("task must be idle when it becomes running");
            };

            if self
                .header
                .state
                .compare_exchange(state, next as u16, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                if state == States::Notified as u16 {
                    self.schedule();
                }
                return;
            }
        }
    }

    fn drop_future(&self) {
        // # Safety:
        // Only the worker after polling, or a canceller that claimed a running/queued
        // task, may access the future. Pin::set drops it without moving it.
        unsafe { Pin::new_unchecked(&mut *self.future.get()) }.set(None);
    }

    fn cancel(self: &Arc<Self>) -> bool {
        loop {
            let state = self.header.state.load(Ordering::Acquire);
            let idle = state == States::Idle as u16 || state == States::Notified as u16;
            let running = state == States::Running as u16 || state == States::Queued as u16;
            if !idle && !running {
                return false;
            }

            let next = if idle {
                States::Cancelling
            } else {
                States::Cancelled
            };

            if self
                .header
                .state
                .compare_exchange(state, next as u16, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                if running {
                    self.drop_future();
                }
                return true;
            }
        }
    }
}

impl<F> Wake for TaskStorage<F>
where
    F: Future<Output = ()> + Send + 'static,
{
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        loop {
            let state = self.header.state.load(Ordering::Acquire);
            let (next, schedule) = match state {
                s if s == States::Running as u16 => (States::Queued as u16, true),
                s if s == States::Idle as u16 => (States::Notified as u16, false),
                s if s == States::Queued as u16
                    || s == States::Notified as u16
                    || s == States::Completed as u16
                    || s == States::Cancelling as u16
                    || s == States::Cancelled as u16 =>
                {
                    return;
                }
                _ => unreachable!(),
            };
            if self
                .header
                .state
                .compare_exchange(state, next, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                if schedule {
                    self.schedule();
                }
                return;
            }
        }
    }
}
