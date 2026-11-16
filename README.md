# Kairo

Kairo is an experimental Rust async runtime I built to learn how futures, wakers, task scheduling, work stealing, and timers fit together.

**Please don't use Kairo in your applications.** It is a learning project, not a production-ready runtime; its APIs and behavior may change, and it may contain bugs. For real applications, choose a mature, maintained runtime instead. Feel free to explore the code and experiment with it locally.

## Features

- A multi-threaded scheduler with a shared queue and per-worker work stealing.
- Configurable worker count and worker wake policy.
- Task spawning, named tasks, join handles, and cancellation requests.
- Panic reporting through join handles for panics while polling a task.
- A `sleep` future backed by a shared timer thread.
- `block_on` for running a future on the calling thread.

## Trying it out

```rust
use std::time::Duration;
use kairo::{thread::Runtime, time::sleep};

fn main() {
    let runtime = Runtime::new().thread_count(4).build();

    let task = runtime.spawn(async {
        sleep(Duration::from_millis(100)).await;
        "hello from Kairo"
    });

    println!("{}", runtime.block_on(task).unwrap());
}
```

Tasks are spawned on an explicit runtime; there is no global scheduler. You can also use the free functions `kairo::thread::spawn(&runtime, future)` and `kairo::thread::spawn_with_name(name, &runtime, future)`.

Both `runtime.block_on(future)` and `kairo::thread::block_on(future)` poll the provided future on the calling thread; spawned tasks run on the runtime's workers. Avoid calling `block_on` on a worker if the awaited work needs that worker to make progress.

### Wake policies

By default, `WakePolicy::PreferFirst` favors the first worker when the shortest local queues are tied. `RoundRobin` rotates the starting point for ties:

```rust
use kairo::thread::{Runtime, WakePolicy};

let runtime = Runtime::new()
    .thread_count(4)
    .wake_policy(WakePolicy::RoundRobin)
    .build();
```

The policy chooses a worker to wake; tasks are still placed in a shared queue.

## Build and test

```sh
cargo build
cargo test
cargo doc --open --no-deps
```

Kairo currently uses Rust edition 2024. For API details, see the generated documentation.

## Project status

Kairo is a place to experiment and learn, not a runtime recommended for applications. Dropping a runtime handle does not necessarily stop its workers immediately: tasks and join handles can retain scheduler references. Networking and filesystem drivers are not currently provided by the public API; executor-independent libraries can supply their own I/O infrastructure.
