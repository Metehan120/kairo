use std::{
    sync::{
        Arc, Barrier, LazyLock, OnceLock,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread::{JoinHandle, Thread, park},
};

use crossbeam::deque::{Injector, Stealer, Worker};
use thiserror::Error;

use crate::task::Task;

#[derive(Debug)]
#[repr(u16)]
pub enum States {
    Idle,
    Running,
    Notified,
    Completed,
    Queued,
    Cancelling,
    Cancelled,
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("No worker exist")]
    NoWorker,
    #[error("Too many retries")]
    ExcessiveRetries,
    #[error("List is empty")]
    Empty,
}

pub trait SchedulerPublic {
    fn schedule(&self, task: Arc<dyn Task>) -> Result<(), Error>;
}

#[derive(Debug)]
pub struct GlobalCoordination {
    global_queue: Injector<Arc<dyn Task>>,
}

#[derive(Debug)]
pub struct ThreadWorker {
    thread: Arc<Thread>,
    queue: Stealer<Arc<dyn Task>>,
}

/// Selects how ties between the shortest worker queues are broken when the
/// scheduler chooses a worker to wake.
///
/// Tasks are placed in a shared queue; this policy selects a worker, not a
/// task's destination. A selected worker is unparked only if its local queue
/// is empty. The default is [`WakePolicy::PreferFirst`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum WakePolicy {
    /// Prefer the first worker when queue lengths are equal.
    #[default]
    PreferFirst,
    /// Rotate the starting worker on each selection, breaking ties in that
    /// circular order.
    RoundRobin,
}

#[derive(Debug)]
pub struct SchedulerInner {
    coordination: Arc<GlobalCoordination>,
    workers: Arc<Vec<ThreadWorker>>,
    wake_policy: WakePolicy,
    next_worker: AtomicUsize,
    should_exit: Arc<AtomicBool>,
    handles: Vec<JoinHandle<()>>,
}

#[derive(Debug, Clone)]
pub struct Scheduler {
    scheduler: Arc<SchedulerInner>,
}

impl Scheduler {
    pub fn new(threads: usize) -> Scheduler {
        Self::with_wake_policy(threads, WakePolicy::default())
    }

    pub fn with_wake_policy(threads: usize, wake_policy: WakePolicy) -> Scheduler {
        let should_exit = Arc::new(AtomicBool::new(false));
        let (coordination, workers, handles) = Self::spawn_threads(threads, should_exit.clone());
        Scheduler {
            scheduler: Arc::new(SchedulerInner {
                coordination,
                workers,
                wake_policy,
                next_worker: AtomicUsize::new(0),
                should_exit,
                handles,
            }),
        }
    }

    fn spawn_threads(
        count: usize,
        should_exit: Arc<AtomicBool>,
    ) -> (
        Arc<GlobalCoordination>,
        Arc<Vec<ThreadWorker>>,
        Vec<JoinHandle<()>>,
    ) {
        let coordination = Arc::new(GlobalCoordination {
            global_queue: Injector::new(),
        });
        let ready = Arc::new(Barrier::new(count + 1));
        let shared_workers = Arc::new(OnceLock::new());
        let mut workers = Vec::with_capacity(count);
        let mut handles = Vec::with_capacity(count);

        for _ in 0..count {
            let queue = Worker::new_lifo();
            let stealer = queue.stealer();
            let global = Arc::clone(&coordination);
            let ready = Arc::clone(&ready);
            let shared_workers = Arc::clone(&shared_workers);
            let exit_clone = should_exit.clone();
            let handle = std::thread::spawn(move || {
                ready.wait();
                let workers = Arc::clone(shared_workers.get().expect("workers initialized"));
                Self::worker_loop(exit_clone, queue, global, workers)
            });

            workers.push(ThreadWorker {
                thread: Arc::new(handle.thread().clone()),
                queue: stealer,
            });
            handles.push(handle);
        }

        let workers = Arc::new(workers);
        assert!(shared_workers.set(Arc::clone(&workers)).is_ok());
        ready.wait();
        (coordination, workers, handles)
    }

    fn worker_loop(
        should_exit: Arc<AtomicBool>,
        worker_queue: Worker<Arc<dyn Task>>,
        global: Arc<GlobalCoordination>,
        workers: Arc<Vec<ThreadWorker>>,
    ) {
        loop {
            let exiting: bool = should_exit.load(Ordering::Acquire);
            let is_empty = worker_queue.is_empty();
            if exiting && is_empty && global.global_queue.is_empty() {
                break;
            }
            if !is_empty && let Some(task) = worker_queue.pop() {
                if task
                    .header()
                    .state
                    .compare_exchange(
                        States::Queued as u16,
                        States::Running as u16,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .is_err()
                {
                    continue;
                }
                task.poll_task();
            } else {
                let mut maybe_park = true;
                match Self::get_from_global_queue(&worker_queue, &global, 4) {
                    Ok(_) => {
                        continue;
                    }
                    Err(Error::ExcessiveRetries) => maybe_park = false,
                    _ => {}
                };
                if let Ok(_) = Self::steal(&worker_queue, workers.clone(), 3) {
                    continue;
                }
                if maybe_park && !exiting {
                    park();
                }
            }
        }
    }

    fn steal(
        main_worker: &Worker<Arc<dyn Task>>,
        workers: Arc<Vec<ThreadWorker>>,
        limit: usize,
    ) -> Result<(), Error> {
        let Some(thread) = workers.iter().max_by_key(|x| x.queue.len()) else {
            return Err(Error::NoWorker);
        };
        let batch: usize = (thread.queue.len() / 2).clamp(1, limit);
        for _ in 0..5 {
            match thread.queue.steal_batch_with_limit(main_worker, batch) {
                crossbeam::deque::Steal::Empty => return Err(Error::Empty),
                crossbeam::deque::Steal::Retry => continue,
                crossbeam::deque::Steal::Success(_) => return Ok(()),
            }
        }
        Err(Error::ExcessiveRetries)
    }

    fn get_from_global_queue(
        dest: &Worker<Arc<dyn Task>>,
        global: &GlobalCoordination,
        limit: usize,
    ) -> Result<(), Error> {
        let mut i = 0;
        let batch: usize = (global.global_queue.len() / 2).clamp(1, limit);
        loop {
            match global.global_queue.steal_batch_with_limit(dest, batch) {
                crossbeam::deque::Steal::Empty => return Err(Error::Empty),
                crossbeam::deque::Steal::Success(_) => break,
                crossbeam::deque::Steal::Retry => {
                    if i == 100 {
                        return Err(Error::ExcessiveRetries);
                    }
                }
            }
            i += 1;
        }
        Ok(())
    }
}

impl SchedulerInner {
    fn select_worker(&self) -> Option<&ThreadWorker> {
        let workers = &self.workers;
        if workers.is_empty() {
            return None;
        }
        let start = match self.wake_policy {
            WakePolicy::PreferFirst => 0,
            WakePolicy::RoundRobin => {
                self.next_worker.fetch_add(1, Ordering::Relaxed) % workers.len()
            }
        };
        workers[start..]
            .iter()
            .chain(workers[..start].iter())
            .min_by_key(|worker| worker.queue.len())
    }
}

impl SchedulerPublic for Scheduler {
    fn schedule(&self, task: Arc<dyn Task>) -> Result<(), Error> {
        self.scheduler.coordination.global_queue.push(task);
        let Some(thread) = self.scheduler.select_worker() else {
            return Err(Error::NoWorker);
        };
        if thread.queue.len() == 0 {
            thread.thread.unpark();
        }
        Ok(())
    }
}

impl Drop for SchedulerInner {
    fn drop(&mut self) {
        self.should_exit.store(true, Ordering::Release);

        for worker in self.workers.iter() {
            worker.thread.unpark();
        }
        let current = std::thread::current().id();
        for handle in self.handles.drain(..) {
            if handle.thread().id() != current {
                let _ = handle.join();
            }
        }
    }
}

pub static GENERAL_SCHEDULER: LazyLock<Scheduler> = LazyLock::new(|| {
    let thread_count = num_cpus::get();
    Scheduler::new(thread_count)
});
