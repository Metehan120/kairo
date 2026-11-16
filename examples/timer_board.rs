//! Experimental countdown board. Run with `cargo run --features gui-examples --example timer_board`.

use std::{
    sync::mpsc,
    time::{Duration, Instant},
};

use eframe::egui;
use kairo::thread::{JoinHandle, Runtime};

const MAX_TIMERS: usize = 32;

struct Event {
    id: u64,
    remaining: Duration,
}

struct TimerRow {
    id: u64,
    name: String,
    duration: Duration,
    remaining: Duration,
    state: TimerState,
    handle: JoinHandle<()>,
}

#[derive(PartialEq)]
enum TimerState {
    Running,
    Cancelled,
    Finished,
}

struct TimerBoard {
    runtime: Runtime,
    sender: mpsc::Sender<Event>,
    receiver: mpsc::Receiver<Event>,
    timers: Vec<TimerRow>,
    next_id: u64,
    name: String,
    seconds: u64,
}

fn progress(duration: Duration, remaining: Duration) -> f32 {
    if duration.is_zero() {
        return 1.0;
    }
    (1.0 - remaining.as_secs_f64() / duration.as_secs_f64()).clamp(0.0, 1.0) as f32
}

impl TimerBoard {
    fn new(runtime: Runtime) -> Self {
        let (sender, receiver) = mpsc::channel();
        Self {
            runtime,
            sender,
            receiver,
            timers: Vec::new(),
            next_id: 1,
            name: "Countdown".into(),
            seconds: 10,
        }
    }

    fn start(&mut self, ctx: &egui::Context) {
        if self.timers.len() >= MAX_TIMERS {
            return;
        }
        let id = self.next_id;
        self.next_id += 1;
        let name: String = self.name.trim().chars().take(48).collect();
        let name = if name.is_empty() {
            format!("Timer {id}")
        } else {
            name
        };
        let duration = Duration::from_secs(self.seconds.clamp(1, 3_600));
        let sender = self.sender.clone();
        let repaint = ctx.clone();
        let handle = self.runtime.spawn_with_name(&name, async move {
            let started = Instant::now();
            let mut last_bucket = 0;
            loop {
                let remaining = duration.saturating_sub(started.elapsed());
                let bucket = (progress(duration, remaining) * 100.0) as u32;
                // Coalesce updates to at most 100 progress events plus completion
                // per timer, even while the window is minimized or not painting.
                if bucket > last_bucket || remaining.is_zero() {
                    if sender.send(Event { id, remaining }).is_err() {
                        return;
                    }
                    repaint.request_repaint();
                    last_bucket = bucket;
                }
                if remaining.is_zero() {
                    return;
                }
                kairo::time::sleep(remaining.min(Duration::from_millis(100))).await;
            }
        });
        self.timers.push(TimerRow {
            id,
            name,
            duration,
            remaining: duration,
            state: TimerState::Running,
            handle,
        });
    }

    fn receive_events(&mut self, ctx: &egui::Context) {
        for _ in 0..256 {
            let Ok(event) = self.receiver.try_recv() else {
                return;
            };
            if let Some(timer) = self.timers.iter_mut().find(|timer| timer.id == event.id) {
                // A queued tick must never revive a cancelled timer.
                if timer.state == TimerState::Running {
                    timer.remaining = event.remaining;
                    if event.remaining.is_zero() {
                        timer.state = TimerState::Finished;
                    }
                }
            }
        }
        ctx.request_repaint();
    }
}

impl eframe::App for TimerBoard {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.receive_events(ctx);
        egui::CentralPanel::default().show(ctx, |ui| {
            ui.heading("Kairo timer board");
            ui.label("Experimental runtime demo • independent async countdowns on four workers");
            ui.label("Elapsed time drives progress; timer wakeups are not real-time guarantees. Display updates are coalesced to 1% steps.");
            ui.separator();
            ui.horizontal(|ui| {
                ui.label("Label");
                ui.add(egui::TextEdit::singleline(&mut self.name).char_limit(48));
            });
            ui.add(egui::Slider::new(&mut self.seconds, 1..=3_600).text("Seconds"));
            ui.horizontal(|ui| {
                if ui.add_enabled(self.timers.len() < MAX_TIMERS, egui::Button::new("Add countdown")).clicked() {
                    self.start(ctx);
                }
                if ui.button("Cancel all").clicked() {
                    for timer in &mut self.timers {
                        if timer.state == TimerState::Running {
                            timer.handle.cancel();
                            timer.state = TimerState::Cancelled;
                        }
                    }
                }
                if ui.button("Clear inactive").clicked() {
                    self.timers.retain(|timer| timer.state == TimerState::Running);
                }
            });
            ui.label(format!("{} / {MAX_TIMERS} rows (clear inactive timers to make room)", self.timers.len()));
            ui.label("Cancel stops tracking immediately, even if task completion wins the cancellation race.");
            egui::ScrollArea::vertical().show(ui, |ui| {
                for timer in &mut self.timers {
                    ui.push_id(timer.id, |ui| {
                        ui.group(|ui| {
                            ui.horizontal(|ui| {
                                ui.strong(format!("#{} {}", timer.id, timer.name));
                                if ui.add_enabled(timer.state == TimerState::Running, egui::Button::new("Cancel")).clicked() {
                                    timer.handle.cancel();
                                    timer.state = TimerState::Cancelled;
                                }
                            });
                            ui.add(egui::ProgressBar::new(progress(timer.duration, timer.remaining))
                                .text(format!("{:.1}s remaining / {}s", timer.remaining.as_secs_f64(), timer.duration.as_secs())));
                            ui.label(match timer.state {
                                TimerState::Running => "Running",
                                TimerState::Cancelled => "Cancelled — tracking stopped",
                                TimerState::Finished => "Finished!",
                            });
                        });
                    });
                }
            });
        });
    }
}

impl Drop for TimerBoard {
    fn drop(&mut self) {
        for timer in &self.timers {
            timer.handle.cancel();
        }
    }
}

fn main() -> eframe::Result {
    // Retain a runtime owner outside the GUI so final scheduler teardown occurs
    // after run_native returns, not in an App callback.
    let runtime = Runtime::new().thread_count(4).build();
    let app = TimerBoard::new(runtime.clone());
    eframe::run_native(
        "Kairo — timer board",
        eframe::NativeOptions::default(),
        Box::new(move |_cc| Ok(Box::new(app))),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn progress_is_bounded_and_handles_zero_duration() {
        let duration = Duration::from_secs(10);
        assert_eq!(progress(duration, duration), 0.0);
        assert_eq!(progress(duration, Duration::from_secs(5)), 0.5);
        assert_eq!(progress(duration, Duration::ZERO), 1.0);
        assert_eq!(progress(duration, Duration::from_secs(20)), 0.0);
        assert_eq!(progress(Duration::ZERO, Duration::ZERO), 1.0);
    }
}
