//! Concurrent local search with eframe/egui 0.31.
//! Run with `cargo run --features gui-examples --example concurrent_search`.

use std::{
    sync::{Arc, mpsc},
    time::Duration,
};

use eframe::egui;
use kairo::thread::{JoinHandle, Runtime};

const CHUNK_SIZE: usize = 8;
const SOURCES: [&str; 4] = ["Articles", "Documentation", "Notes", "Examples"];

struct SearchEvent {
    source: usize,
    scanned: usize,
    matches: Vec<String>,
}

fn search_chunk(rows: &[String], normalized_query: &str) -> Vec<String> {
    rows.iter()
        .filter(|row| row.to_lowercase().contains(normalized_query))
        .cloned()
        .collect()
}

fn local_datasets(rows_per_dataset: usize) -> Arc<Vec<Vec<String>>> {
    let topics = [
        "Rust async futures and cooperative tasks",
        "Kairo timers and cancellation",
        "Concurrent search across local datasets",
        "Queues, workers, and work stealing",
        "Building responsive graphical interfaces",
        "Channels and message passing",
    ];
    Arc::new(
        SOURCES
            .iter()
            .enumerate()
            .map(|(source, name)| {
                (0..rows_per_dataset)
                    .map(|row| {
                        format!(
                            "{name} #{:03}: {}",
                            row + 1,
                            topics[(row + source) % topics.len()]
                        )
                    })
                    .collect()
            })
            .collect(),
    )
}

struct SearchApp {
    runtime: Runtime,
    datasets: Arc<Vec<Vec<String>>>,
    query: String,
    rows_per_dataset: usize,
    query_limit: usize,
    result_limit: usize,
    receiver: Option<mpsc::Receiver<SearchEvent>>,
    tasks: Vec<JoinHandle<()>>,
    scanned: [usize; 4],
    match_counts: [usize; 4],
    results: Vec<(usize, String)>,
    status: String,
    searched_query: String,
}

impl SearchApp {
    fn new() -> Self {
        Self {
            runtime: Runtime::new().thread_count(4).build(),
            datasets: local_datasets(240),
            query: "kairo".into(),
            rows_per_dataset: 240,
            query_limit: 96,
            result_limit: 250,
            receiver: None,
            tasks: Vec::new(),
            scanned: [0; 4],
            match_counts: [0; 4],
            results: Vec::new(),
            status: "Ready — search four local, in-memory datasets.".into(),
            searched_query: String::new(),
        }
    }

    fn cancel_tasks(&mut self) {
        // Disconnect first: a poll already in flight cannot publish into a new run.
        self.receiver = None;
        for task in self.tasks.drain(..) {
            task.cancel();
        }
    }

    fn start(&mut self, ctx: &egui::Context) {
        let query = self.query.trim().to_lowercase();
        if query.is_empty() {
            return;
        }
        self.cancel_tasks();
        if self
            .datasets
            .iter()
            .any(|rows| rows.len() != self.rows_per_dataset)
        {
            self.datasets = local_datasets(self.rows_per_dataset);
        }
        self.scanned = [0; 4];
        self.match_counts = [0; 4];
        self.results.clear();
        self.searched_query = self.query.trim().to_owned();
        self.status = "Searching… results appear as each chunk finishes.".into();
        // A new receiver is the run identity. No event from a previous run is read.
        let (sender, receiver) = mpsc::channel();
        self.receiver = Some(receiver);
        for source in 0..SOURCES.len() {
            let sender = sender.clone();
            let ctx = ctx.clone();
            let datasets = Arc::clone(&self.datasets);
            let query = query.clone();
            let name = format!("search-{}", SOURCES[source]);
            self.tasks
                .push(self.runtime.spawn_with_name(&name, async move {
                    for (chunk_index, rows) in datasets[source].chunks(CHUNK_SIZE).enumerate() {
                        // A real timer yields between small CPU chunks, bounding each poll.
                        // Different delays make independent completion easy to see.
                        kairo::time::sleep(Duration::from_millis(25 + source as u64 * 12)).await;
                        let event = SearchEvent {
                            source,
                            scanned: ((chunk_index + 1) * CHUNK_SIZE).min(datasets[source].len()),
                            matches: search_chunk(rows, &query),
                        };
                        if sender.send(event).is_err() {
                            return;
                        }
                        ctx.request_repaint();
                    }
                }));
        }
        // Input controls bound dataset sizes, so messages and retained matches
        // remain bounded even when the window is not painting.
    }

    fn receive(&mut self, ctx: &egui::Context) {
        let Some(receiver) = &self.receiver else {
            return;
        };
        let mut disconnected = false;
        // Never block the UI or spend an unbounded frame draining messages.
        for _ in 0..64 {
            match receiver.try_recv() {
                Ok(event) => {
                    self.scanned[event.source] = event.scanned;
                    self.match_counts[event.source] += event.matches.len();

                    self.results
                        .extend(event.matches.into_iter().map(|text| (event.source, text)));
                }
                Err(mpsc::TryRecvError::Empty) => break,
                Err(mpsc::TryRecvError::Disconnected) => {
                    disconnected = true;
                    break;
                }
            }
        }
        if self
            .scanned
            .iter()
            .zip(self.datasets.iter())
            .all(|(&n, rows)| n == rows.len())
        {
            self.status = "Complete.".into();
            self.cancel_tasks();
        } else if disconnected {
            self.status = "Search stopped before all datasets completed.".into();
            self.cancel_tasks();
        } else {
            // Also covers messages remaining after the per-frame drain budget.
            ctx.request_repaint_after(Duration::from_millis(33));
        }
    }
}

impl Drop for SearchApp {
    fn drop(&mut self) {
        self.cancel_tasks();
    }
}

impl eframe::App for SearchApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.receive(ctx);
        egui::CentralPanel::default().show(ctx, |ui| {
            ui.heading("Concurrent local search");
            ui.label("Four named Kairo tasks • four runtime workers • GUI on the main thread");
            ui.label("Synthetic in-memory data only; no network or filesystem access.");
            ui.separator();
            ui.collapsing("Search limits", |ui| {
                ui.add(egui::Slider::new(&mut self.rows_per_dataset, 1..=10_000).text("Rows per dataset (next search)"));
                ui.add(egui::Slider::new(&mut self.query_limit, 1..=1_024).text("Query character cap"));
                if let Some((byte_index, _)) = self.query.char_indices().nth(self.query_limit) {
                    self.query.truncate(byte_index);
                }
                let total: usize = self.datasets.iter().map(Vec::len).sum();
                ui.add(egui::Slider::new(&mut self.result_limit, 1..=total).text("Visible result cap"));
                ui.small("All matches are retained for this search. Change the visible cap at any time; other changes apply to the next search.");
            });
            ui.horizontal(|ui| {
                ui.label("Query:");
                let edit = ui.add(egui::TextEdit::singleline(&mut self.query)
                    .char_limit(self.query_limit).desired_width(300.0));
                let enter = edit.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter));
                let valid = !self.query.trim().is_empty();
                if (ui.add_enabled(valid, egui::Button::new("Search / restart")).clicked() || enter) && valid {
                    self.start(ctx);
                }
                if ui.add_enabled(self.receiver.is_some(), egui::Button::new("Cancel")).clicked() {
                    self.cancel_tasks();
                    self.status = "Cancelled. Partial results retained.".into();
                }
            });
            ui.label("Case-insensitive substring match. Editing does not change an active search.");
            ui.label(&self.status);
            if !self.searched_query.is_empty() {
                ui.label(format!("Results for: {:?}", self.searched_query));
            }
            let scanned: usize = self.scanned.iter().sum();
            let total: usize = self.datasets.iter().map(Vec::len).sum();
            ui.add(egui::ProgressBar::new(scanned as f32 / total as f32)
                .text(format!("{scanned} / {total} records scanned")));
            egui::Grid::new("source-progress").striped(true).show(ui, |ui| {
                for (source, name) in SOURCES.iter().enumerate() {
                    ui.label(*name);
                    let rows = self.datasets[source].len();
                    ui.add(egui::ProgressBar::new(self.scanned[source] as f32 / rows as f32)
                        .desired_width(180.0).text(format!("{} / {rows}", self.scanned[source])));
                    ui.label(format!("{} matches", self.match_counts[source]));
                    ui.end_row();
                }
            });
            ui.separator();
            let matches: usize = self.match_counts.iter().sum();
            ui.label(format!("{matches} matches received • displaying {} (cap {}) • arrival order", self.results.len().min(self.result_limit), self.result_limit));
            // Only lay out on-screen rows, even when the display cap is large.
            let visible = self.results.len().min(self.result_limit);
            let row_height = ui.text_style_height(&egui::TextStyle::Body);
            egui::ScrollArea::vertical().auto_shrink([false, false]).show_rows(ui, row_height, visible, |ui, range| {
                for (source, text) in &self.results[range] {
                    ui.horizontal(|ui| {
                        ui.strong(SOURCES[*source]);
                        ui.add(egui::Label::new(text).truncate());
                    });
                }
            });
        });
    }
}

fn main() -> eframe::Result {
    eframe::run_native(
        "Kairo — concurrent local search",
        eframe::NativeOptions::default(),
        Box::new(|_cc| Ok(Box::new(SearchApp::new()))),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_case_insensitively_with_normalized_query() {
        let rows = vec![
            "Kairo timers".into(),
            "RUST futures".into(),
            "kairo tasks".into(),
        ];
        assert_eq!(
            search_chunk(&rows, "kairo"),
            vec!["Kairo timers", "kairo tasks"]
        );
        assert!(search_chunk(&rows, "absent").is_empty());
    }

    #[test]
    fn dataset_sizes_and_batches_follow_configuration() {
        for size in [1, 7, 8, 9, 240, 10_000] {
            let datasets = local_datasets(size);
            assert_eq!(datasets.len(), SOURCES.len());
            assert!(datasets.iter().all(|rows| rows.len() == size));
            assert_eq!(
                datasets
                    .iter()
                    .map(|rows| rows.chunks(CHUNK_SIZE).count())
                    .sum::<usize>(),
                SOURCES.len() * size.div_ceil(CHUNK_SIZE)
            );
        }
    }
}
