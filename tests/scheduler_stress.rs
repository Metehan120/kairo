use std::{
    future::poll_fn,
    sync::{Arc, mpsc},
    task::Poll,
    time::{Duration, Instant},
};

use kairo::thread::Runtime;

const TIMEOUT: Duration = Duration::from_secs(30);

#[test]
fn concurrent_spawners_complete_every_task() {
    const SPAWNERS: usize = 4;
    const TASKS_PER_SPAWNER: usize = 500;
    let runtime = Arc::new(Runtime::new().thread_count(4).build());
    let (sender, receiver) = mpsc::channel();

    for spawner in 0..SPAWNERS {
        let runtime = Arc::clone(&runtime);
        let sender = sender.clone();
        std::thread::spawn(move || {
            for task in 0..TASKS_PER_SPAWNER {
                let sender = sender.clone();
                runtime.spawn(async move {
                    let _ = sender.send(spawner * TASKS_PER_SPAWNER + task);
                });
            }
        });
    }
    drop(sender);

    let deadline = Instant::now() + TIMEOUT;
    let mut seen = vec![false; SPAWNERS * TASKS_PER_SPAWNER];
    for _ in 0..seen.len() {
        let id = receiver
            .recv_timeout(deadline.saturating_duration_since(Instant::now()))
            .expect("concurrently spawned tasks did not complete in time");
        assert!(!seen[id], "task {id} completed more than once");
        seen[id] = true;
    }
    assert!(seen.into_iter().all(|completed| completed));
}

#[test]
fn repeated_self_wakes_and_join_handles_complete() {
    const TASKS: usize = 256;
    const WAKES: usize = 100;
    let runtime = Runtime::new().thread_count(4).build();
    let (sender, receiver) = mpsc::channel();
    let mut handles = Vec::new();

    for id in 0..TASKS {
        handles.push(runtime.spawn(async move {
            let mut remaining = WAKES;
            poll_fn(move |cx| {
                if remaining == 0 {
                    Poll::Ready(id)
                } else {
                    remaining -= 1;
                    cx.waker().wake_by_ref();
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            })
            .await
        }));
    }
    runtime.spawn(async move {
        let mut results = Vec::new();
        for handle in handles {
            results.push(handle.await.unwrap());
        }
        let _ = sender.send(results);
    });

    let results = receiver
        .recv_timeout(TIMEOUT)
        .expect("self-waking tasks or their join handles timed out");
    assert_eq!(results, (0..TASKS).collect::<Vec<_>>());
}

#[test]
fn bounded_channel_contention_completes_all_messages() {
    const PRODUCERS: usize = 8;
    const CONSUMERS: usize = 8;
    const PER_PRODUCER: usize = 500;
    let runtime = Runtime::new().thread_count(4).build();
    let (completion_tx, completion_rx) = mpsc::channel();
    let (sender, receiver) = async_channel::bounded::<usize>(2);
    let mut producers = Vec::new();
    let mut consumers = Vec::new();

    for _ in 0..CONSUMERS {
        let receiver = receiver.clone();
        consumers.push(runtime.spawn(async move {
            let mut values = Vec::new();
            while let Ok(value) = receiver.recv().await {
                values.push(value);
            }
            values
        }));
    }
    drop(receiver);
    for producer in 0..PRODUCERS {
        let sender = sender.clone();
        producers.push(runtime.spawn(async move {
            for item in 0..PER_PRODUCER {
                if sender.send(producer * PER_PRODUCER + item).await.is_err() {
                    return false;
                }
            }
            true
        }));
    }
    drop(sender);
    runtime.spawn(async move {
        let mut all_sent = true;
        for producer in producers {
            all_sent &= producer.await.unwrap();
        }
        let mut values = Vec::new();
        for consumer in consumers {
            values.extend(consumer.await.unwrap());
        }
        let _ = completion_tx.send((all_sent, values));
    });

    let (all_sent, mut values) = completion_rx
        .recv_timeout(TIMEOUT)
        .expect("contended channel workload timed out");
    assert!(all_sent, "a producer lost its receiver");
    values.sort_unstable();
    assert_eq!(values, (0..PRODUCERS * PER_PRODUCER).collect::<Vec<_>>());
}
