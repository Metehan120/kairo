//! Experimental task dashboard. Run with `cargo run --features gui-examples --example task_dashboard`.

use std::{
    future::{Future, poll_fn},
    pin::Pin,
    sync::{Arc, Mutex, mpsc},
    time::Duration,
};

use eframe::egui;
use kairo::thread::{JoinHandle, Runtime};

const MAX_TASKS: usize = 32;

enum Event {
    Progress(u64, u32),
    Finished(u64, Result<u64, String>),
}

struct TaskRow {
    id: u64,
    name: String,
    steps: u32,
    completed: u32,
    status: String,
    finished: bool,
    cancel_requested: bool,
    worker: Arc<Mutex<JoinHandle<u64>>>,
    observer: JoinHandle<()>,
}

impl TaskRow {
    fn cancel(&mut self) {
        // The observer only holds this lock for one nonblocking Future::poll.
        if self.worker.lock().unwrap().cancel() {
            self.cancel_requested = true;
            self.status = "Cancellation requested (completion may still win)".into();
        }
    }
}

struct Dashboard {
    runtime: Runtime,
    sender: mpsc::Sender<Event>,
    receiver: mpsc::Receiver<Event>,
    tasks: Vec<TaskRow>,
    next_id: u64,
    name: String,
    steps: u32,
    delay_ms: u64,
}

impl Dashboard {
    fn new(runtime: Runtime) -> Self {
        let (sender, receiver) = mpsc::channel();
        Self {
            runtime,
            sender,
            receiver,
            tasks: Vec::new(),
            next_id: 1,
            name: "Example task".into(),
            steps: 20,
            delay_ms: 100,
        }
    }

    fn start(&mut self, ctx: &egui::Context) {
        if self.tasks.len() >= MAX_TASKS {
            return;
        }
        let id = self.next_id;
        self.next_id += 1;
        let name: String = self.name.trim().chars().take(48).collect();
        let name = if name.is_empty() {
            format!("Task {id}")
        } else {
            name
        };
        let steps = self.steps.clamp(1, 100);
        let delay = Duration::from_millis(self.delay_ms.clamp(10, 1_000));
        let sender = self.sender.clone();
        let repaint = ctx.clone();
        let worker = self.runtime.spawn_with_name(&name, async move {
            let mut sum = 0_u64;
            for step in 1..=steps {
                kairo::time::sleep(delay).await;
                sum += u64::from(step);
                if sender.send(Event::Progress(id, step)).is_err() {
                    return sum;
                }
                repaint.request_repaint();
            }
            sum
        });
        let worker = Arc::new(Mutex::new(worker));
        let observed = worker.clone();
        let sender = self.sender.clone();
        let repaint = ctx.clone();
        let observer = self.runtime.spawn(async move {
            // Thread has no cancel method. Sharing the actual JoinHandle retains
            // cancellation without blocking the GUI to await a task's result.
            let result = poll_fn(|cx| Pin::new(&mut *observed.lock().unwrap()).poll(cx))
                .await
                .map_err(|error| format!("{error:?}"));
            let _ = sender.send(Event::Finished(id, result));
            repaint.request_repaint();
        });
        self.tasks.push(TaskRow {
            id,
            name,
            steps,
            completed: 0,
            status: "Running".into(),
            finished: false,
            cancel_requested: false,
            worker,
            observer,
        });
    }

    fn receive_events(&mut self, ctx: &egui::Context) {
        // Limit per-frame work even if several workers report at once.
        for _ in 0..256 {
            let Ok(event) = self.receiver.try_recv() else {
                return;
            };
            let id = match &event {
                Event::Progress(id, _) | Event::Finished(id, _) => *id,
            };
            let Some(task) = self.tasks.iter_mut().find(|task| task.id == id) else {
                continue;
            };
            match event {
                Event::Progress(_, completed) => task.completed = completed,
                Event::Finished(_, result) => {
                    task.finished = true;
                    task.status = match result {
                        Ok(sum) => format!("Joined successfully: sum = {sum}"),
                        Err(error) => format!("Join error: {error}"),
                    };
                }
            }
        }
        ctx.request_repaint();
    }
}

impl eframe::App for Dashboard {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.receive_events(ctx);
        egui::CentralPanel::default().show(ctx, |ui| {
            ui.heading("Kairo task dashboard");
            ui.label("Experimental runtime demo • four workers • no GUI-thread joins");
            ui.label("Workers sum 1..=steps. An async observer awaits each join result, including cancellation errors.");
            ui.separator();
            ui.horizontal(|ui| {
                ui.label("Name");
                ui.add(egui::TextEdit::singleline(&mut self.name).char_limit(48));
            });
            ui.add(egui::Slider::new(&mut self.steps, 1..=100).text("Steps"));
            ui.add(egui::Slider::new(&mut self.delay_ms, 10..=1_000).text("Milliseconds per step"));
            ui.horizontal(|ui| {
                if ui.add_enabled(self.tasks.len() < MAX_TASKS, egui::Button::new("Spawn task")).clicked() {
                    self.start(ctx);
                }
                if ui.button("Cancel running").clicked() {
                    for task in &mut self.tasks {
                        if !task.finished && !task.cancel_requested { task.cancel(); }
                    }
                }
                if ui.button("Clear finished").clicked() {
                    self.tasks.retain(|task| !task.finished);
                }
            });
            ui.label(format!("{} / {MAX_TASKS} rows (clear finished tasks to make room)", self.tasks.len()));
            egui::ScrollArea::vertical().show(ui, |ui| {
                for task in &mut self.tasks {
                    ui.push_id(task.id, |ui| {
                        ui.group(|ui| {
                            ui.horizontal(|ui| {
                                ui.strong(format!("#{} {}", task.id, task.name));
                                if ui.add_enabled(!task.finished && !task.cancel_requested, egui::Button::new("Cancel")).clicked() {
                                    task.cancel();
                                }
                            });
                            ui.add(egui::ProgressBar::new(task.completed as f32 / task.steps as f32)
                                .text(format!("{} / {} steps", task.completed, task.steps)));
                            ui.label(&task.status);
                        });
                    });
                }
            });
        });
    }
}

impl Drop for Dashboard {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.worker.lock().unwrap().cancel();
            task.observer.cancel();
        }
    }
}

fn main() -> eframe::Result {
    // Keep the final runtime owner outside the app so scheduler teardown does
    // not happen inside a GUI callback. No task is synchronously joined by UI code.
    let runtime = Runtime::new().thread_count(4).build();
    let app = Dashboard::new(runtime.clone());
    eframe::run_native(
        "Kairo — task dashboard",
        eframe::NativeOptions::default(),
        Box::new(move |_cc| Ok(Box::new(app))),
    )
}
