use std::{
    future::poll_fn,
    sync::{Arc, mpsc},
    task::Poll,
    time::{Duration, Instant},
};
use kairo::thread::Runtime;

// Run alone: cargo test --release --test extreme_threads -- --ignored --nocapture
#[test]
#[ignore = "manual oversubscription stress test"]
fn oversubscribed_workers_complete_concurrent_spawns_and_wakes() {
    const SPAWNERS: usize = 8;
    const TASKS_PER_SPAWNER: usize = 256;
    const WAKES: usize = 8;
    const TIMEOUT: Duration = Duration::from_secs(60);

    let cpus = std::thread::available_parallelism().unwrap().get();
    let workers = std::env::var("KAIRO_STRESS_WORKERS")
        .ok()
        .map(|value| {
            value
                .parse::<usize>()
                .expect("KAIRO_STRESS_WORKERS must be a number")
        })
        .unwrap_or_else(|| cpus.saturating_mul(4).clamp(32, 256));
    assert!(
        (1..=512).contains(&workers),
        "worker count must be between 1 and 512"
    );
    println!(
        "{cpus} available CPUs, {workers} workers, {} tasks, {WAKES} wakes per task",
        SPAWNERS * TASKS_PER_SPAWNER
    );
    let start = Instant::now();
    let runtime = Arc::new(Runtime::new().thread_count(workers).build());
    let (handles_tx, handles_rx) = mpsc::channel();
    let mut spawners = Vec::new();
    for producer in 0..SPAWNERS {
        let runtime = Arc::clone(&runtime);
        let handles_tx = handles_tx.clone();
        spawners.push(std::thread::spawn(move || {
            for index in 0..TASKS_PER_SPAWNER {
                let id = producer * TASKS_PER_SPAWNER + index;
                let handle = runtime.spawn(async move {
                    let mut remaining = WAKES;
                    poll_fn(move |cx| {
                        if remaining == 0 {
                            Poll::Ready(id)
                        } else {
                            remaining -= 1;
                            cx.waker().wake_by_ref();
                            Poll::Pending
                        }
                    })
                    .await
                });
                handles_tx.send(handle).unwrap();
            }
        }));
    }
    drop(handles_tx);
    for spawner in spawners {
        spawner.join().expect("spawner panicked");
    }
    let handles: Vec<_> = handles_rx.into_iter().collect();
    assert_eq!(handles.len(), SPAWNERS * TASKS_PER_SPAWNER);
    let (done_tx, done_rx) = mpsc::channel();
    runtime.spawn(async move {
        let mut results = Vec::with_capacity(handles.len());
        for handle in handles {
            results.push(handle.await);
        }
        let _ = done_tx.send(results);
    });
    let results = done_rx
        .recv_timeout(TIMEOUT)
        .expect("tasks or joins did not finish");
    let mut seen = vec![false; SPAWNERS * TASKS_PER_SPAWNER];
    for result in results {
        let id = result.expect("task failed");
        assert!(
            id < seen.len() && !seen[id],
            "invalid or duplicate task ID {id}"
        );
        seen[id] = true;
    }
    assert!(seen.iter().all(|&done| done));
    println!("all tasks and joins completed in {:?}", start.elapsed());
}
