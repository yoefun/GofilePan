use eframe::egui;
use gofilepan_core::{
    parse_batch_lines, DownloadConfig, DownloadEvent, DownloadManager, DownloadPlan,
    DownloadRequest,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::mpsc,
    time::Duration,
};
use tokio::runtime::Runtime;

fn main() -> eframe::Result<()> {
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default().with_inner_size([980.0, 720.0]),
        ..Default::default()
    };
    eframe::run_native(
        "GofilePan",
        options,
        Box::new(|_cc| Box::new(GofilePanApp::new())),
    )
}

struct GofilePanApp {
    settings: AppSettings,
    url_input: String,
    password_input: String,
    status: String,
    discovered_plan: Option<DownloadPlan>,
    selected_files: HashSet<usize>,
    progresses: HashMap<usize, FileProgress>,
    logs: Vec<String>,
    runtime: Runtime,
    tx: mpsc::Sender<GuiMessage>,
    rx: mpsc::Receiver<GuiMessage>,
    active_manager: Option<DownloadManager>,
}

impl GofilePanApp {
    fn new() -> Self {
        let (tx, rx) = mpsc::channel();
        let settings = AppSettings::load();
        Self {
            url_input: String::new(),
            password_input: String::new(),
            status: "Ready".to_string(),
            discovered_plan: None,
            selected_files: HashSet::new(),
            progresses: HashMap::new(),
            logs: Vec::new(),
            runtime: Runtime::new().expect("create tokio runtime"),
            tx,
            rx,
            active_manager: None,
            settings,
        }
    }

    fn config(&self) -> DownloadConfig {
        let mut config = DownloadConfig::from_env();
        config.download_dir = PathBuf::from(self.settings.download_dir.trim());
        config.token = empty_to_none(&self.settings.token);
        config.max_concurrent = self.settings.max_concurrent.max(1);
        config.retries = self.settings.retries.max(1);
        config.timeout = Duration::from_secs_f64(self.settings.timeout_seconds.max(1.0));
        config.chunk_size = self.settings.chunk_size.max(1);
        if !self.settings.user_agent.trim().is_empty() {
            config.user_agent = self.settings.user_agent.clone();
        }
        config
    }

    fn discover_single(&mut self) {
        self.discovered_plan = None;
        self.selected_files.clear();
        self.progresses.clear();
        let request =
            DownloadRequest::new(self.url_input.trim(), empty_to_none(&self.password_input));
        let tx = self.tx.clone();
        let config = self.config();
        self.status = "Discovering files...".to_string();

        self.runtime.spawn(async move {
            let result = async {
                let manager = DownloadManager::new(config)?;
                let plan = manager.discover(request).await?;
                Ok::<_, anyhow::Error>(plan)
            }
            .await;
            let _ = tx.send(GuiMessage::DiscoveryFinished(
                result.map_err(|error| error.to_string()),
            ));
        });
    }

    fn start_download(&mut self) {
        self.progresses.clear();
        self.logs.clear();
        self.settings.save();
        let config = self.config();
        let password = empty_to_none(&self.password_input);
        let input = self.url_input.clone();
        let existing_plan = self.discovered_plan.clone();
        let selected_files = self.selected_files.clone();
        let tx = self.tx.clone();
        let manager = match DownloadManager::new(config) {
            Ok(manager) => manager,
            Err(error) => {
                self.status = "Failed".to_string();
                self.logs.push(error.to_string());
                return;
            }
        };
        self.active_manager = Some(manager.clone());
        self.status = "Downloading...".to_string();

        self.runtime.spawn(async move {
            let result = async {
                forward_events(manager.subscribe(), tx.clone());

                if let Some(plan) = existing_plan {
                    let mut request = plan.request.clone();
                    request.selected_files = Some(selected_files);
                    let plan = manager.discover(request).await?;
                    manager.download(plan).await?;
                } else if looks_like_batch(&input) {
                    for request in parse_batch_lines(&input, password.as_deref()) {
                        let plan = manager.discover(request).await?;
                        manager.download(plan).await?;
                    }
                } else {
                    let request = DownloadRequest::new(input.trim(), password);
                    let plan = manager.discover(request).await?;
                    manager.download(plan).await?;
                }

                Ok::<_, anyhow::Error>(())
            }
            .await;

            let _ = tx.send(GuiMessage::DownloadFinished(
                result.map_err(|error| error.to_string()),
            ));
        });
    }

    fn cancel_download(&mut self) {
        if let Some(manager) = &self.active_manager {
            manager.cancel();
        }
        self.status = "Cancellation requested".to_string();
    }

    fn drain_messages(&mut self) {
        while let Ok(message) = self.rx.try_recv() {
            match message {
                GuiMessage::DiscoveryFinished(Ok(plan)) => {
                    self.status = format!("Discovered {} files", plan.files.len());
                    self.selected_files = plan.files.iter().map(|file| file.index).collect();
                    self.discovered_plan = Some(plan);
                }
                GuiMessage::DiscoveryFinished(Err(error)) => {
                    self.status = "Discovery failed".to_string();
                    self.logs.push(error);
                }
                GuiMessage::DownloadEvent(event) => self.apply_event(event),
                GuiMessage::DownloadFinished(Ok(())) => {
                    self.status = "Done".to_string();
                    self.active_manager = None;
                }
                GuiMessage::DownloadFinished(Err(error)) => {
                    self.status = "Failed".to_string();
                    self.active_manager = None;
                    self.logs.push(error);
                }
            }
        }
    }

    fn apply_event(&mut self, event: DownloadEvent) {
        match event {
            DownloadEvent::FileDiscovered { index, path } => {
                self.logs
                    .push(format!("[{index}] discovered {}", path.display()));
            }
            DownloadEvent::Started { index, path } => {
                self.progresses.entry(index).or_default().path = path.display().to_string();
            }
            DownloadEvent::Progress {
                index,
                path,
                downloaded,
                total,
            } => {
                let entry = self.progresses.entry(index).or_default();
                entry.path = path.display().to_string();
                entry.downloaded = downloaded;
                entry.total = total;
            }
            DownloadEvent::Completed { index, path, bytes } => {
                let entry = self.progresses.entry(index).or_default();
                entry.path = path.display().to_string();
                entry.downloaded = bytes;
                entry.total = bytes;
                entry.done = true;
                self.logs.push(format!("[{index}] done {}", path.display()));
            }
            DownloadEvent::Skipped { index, path } => {
                let entry = self.progresses.entry(index).or_default();
                entry.path = path.display().to_string();
                entry.done = true;
                self.logs
                    .push(format!("[{index}] skipped {}", path.display()));
            }
            DownloadEvent::Retry {
                index,
                message,
                attempt,
            } => {
                self.logs.push(format!(
                    "retry {attempt}{}: {message}",
                    index
                        .map(|value| format!(" for [{value}]"))
                        .unwrap_or_default()
                ));
            }
            DownloadEvent::Failed { index, message } => {
                self.logs.push(format!(
                    "failed{}: {message}",
                    index.map(|value| format!(" [{value}]")).unwrap_or_default()
                ));
            }
            DownloadEvent::Cancelled => {
                self.logs.push("cancelled".to_string());
            }
        }
    }
}

impl eframe::App for GofilePanApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.drain_messages();

        egui::TopBottomPanel::top("top").show(ctx, |ui| {
            ui.horizontal(|ui| {
                ui.heading("GofilePan");
                ui.separator();
                ui.label(&self.status);
            });
        });

        egui::SidePanel::left("settings")
            .resizable(false)
            .default_width(310.0)
            .show(ctx, |ui| {
                ui.heading("Settings");
                ui.label("Download directory");
                ui.horizontal(|ui| {
                    ui.text_edit_singleline(&mut self.settings.download_dir);
                    if ui.button("...").clicked() {
                        if let Some(path) = rfd::FileDialog::new().pick_folder() {
                            self.settings.download_dir = path.display().to_string();
                        }
                    }
                });
                ui.label("Token");
                ui.text_edit_singleline(&mut self.settings.token);
                ui.label("User agent");
                ui.text_edit_singleline(&mut self.settings.user_agent);
                ui.add(
                    egui::Slider::new(&mut self.settings.max_concurrent, 1..=32).text("Concurrent"),
                );
                ui.add(egui::Slider::new(&mut self.settings.retries, 1..=20).text("Retries"));
                ui.add(
                    egui::Slider::new(&mut self.settings.timeout_seconds, 1.0..=120.0)
                        .text("Timeout"),
                );
                ui.add(
                    egui::DragValue::new(&mut self.settings.chunk_size)
                        .speed(1024.0)
                        .prefix("Chunk "),
                );

                ui.separator();
                if ui.button("Save Settings").clicked() {
                    self.settings.save();
                    self.status = "Settings saved".to_string();
                }
            });

        egui::CentralPanel::default().show(ctx, |ui| {
            ui.label("URL or batch lines");
            ui.add(
                egui::TextEdit::multiline(&mut self.url_input)
                    .desired_rows(5)
                    .desired_width(f32::INFINITY),
            );
            ui.horizontal(|ui| {
                ui.label("Password");
                ui.text_edit_singleline(&mut self.password_input);
            });
            ui.horizontal(|ui| {
                if ui.button("Discover").clicked() {
                    self.discover_single();
                }
                if ui.button("Start").clicked() {
                    self.start_download();
                }
                if ui.button("Cancel").clicked() {
                    self.cancel_download();
                }
            });

            if let Some(plan) = &self.discovered_plan {
                ui.separator();
                ui.heading("Files");
                egui::ScrollArea::vertical()
                    .max_height(180.0)
                    .show(ui, |ui| {
                        for file in &plan.files {
                            let mut selected = self.selected_files.contains(&file.index);
                            if ui
                                .checkbox(
                                    &mut selected,
                                    format!("[{}] {}", file.index, file.destination().display()),
                                )
                                .changed()
                            {
                                if selected {
                                    self.selected_files.insert(file.index);
                                } else {
                                    self.selected_files.remove(&file.index);
                                }
                            }
                        }
                    });
            }

            ui.separator();
            ui.heading("Progress");
            egui::ScrollArea::vertical()
                .max_height(200.0)
                .show(ui, |ui| {
                    let mut rows: Vec<_> = self.progresses.iter().collect();
                    rows.sort_by_key(|(index, _)| **index);
                    for (index, progress) in rows {
                        let pct = if progress.total == 0 {
                            0.0
                        } else {
                            progress.downloaded as f32 / progress.total as f32
                        };
                        ui.label(format!("[{index}] {}", progress.path));
                        ui.add(egui::ProgressBar::new(pct).show_percentage());
                    }
                });

            ui.separator();
            ui.heading("Log");
            egui::ScrollArea::vertical().show(ui, |ui| {
                for line in self.logs.iter().rev().take(100) {
                    ui.label(line);
                }
            });
        });

        ctx.request_repaint_after(Duration::from_millis(100));
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AppSettings {
    download_dir: String,
    token: String,
    max_concurrent: usize,
    retries: usize,
    timeout_seconds: f64,
    chunk_size: usize,
    user_agent: String,
}

impl Default for AppSettings {
    fn default() -> Self {
        let core = DownloadConfig::from_env();
        Self {
            download_dir: core.download_dir.display().to_string(),
            token: core.token.unwrap_or_default(),
            max_concurrent: core.max_concurrent,
            retries: core.retries,
            timeout_seconds: core.timeout.as_secs_f64(),
            chunk_size: core.chunk_size,
            user_agent: core.user_agent,
        }
    }
}

impl AppSettings {
    fn load() -> Self {
        let Some(path) = config_path() else {
            return Self::default();
        };
        std::fs::read_to_string(path)
            .ok()
            .and_then(|text| toml::from_str(&text).ok())
            .unwrap_or_default()
    }

    fn save(&self) {
        let Some(path) = config_path() else {
            return;
        };
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        if let Ok(text) = toml::to_string_pretty(self) {
            let _ = std::fs::write(path, text);
        }
    }
}

#[derive(Debug, Clone, Default)]
struct FileProgress {
    path: String,
    downloaded: u64,
    total: u64,
    done: bool,
}

enum GuiMessage {
    DiscoveryFinished(Result<DownloadPlan, String>),
    DownloadEvent(DownloadEvent),
    DownloadFinished(Result<(), String>),
}

fn forward_events(
    mut events: tokio::sync::broadcast::Receiver<DownloadEvent>,
    tx: mpsc::Sender<GuiMessage>,
) {
    tokio::spawn(async move {
        while let Ok(event) = events.recv().await {
            let _ = tx.send(GuiMessage::DownloadEvent(event));
        }
    });
}

fn config_path() -> Option<PathBuf> {
    dirs::config_dir().map(|dir| dir.join("GofilePan").join("config.toml"))
}

fn empty_to_none(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn looks_like_batch(input: &str) -> bool {
    input.lines().filter(|line| !line.trim().is_empty()).count() > 1
}
