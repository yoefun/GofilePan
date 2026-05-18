#![cfg_attr(target_os = "windows", windows_subsystem = "windows")]

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

const ACCENT: egui::Color32 = egui::Color32::from_rgb(0x35, 0xd3, 0xff);
const ACCENT_SOFT: egui::Color32 = egui::Color32::from_rgb(0x16, 0x2b, 0x3c);
const PANEL_BG: egui::Color32 = egui::Color32::from_rgb(0x0f, 0x14, 0x1d);
const SURFACE_BG: egui::Color32 = egui::Color32::from_rgb(0x13, 0x1a, 0x25);
const BORDER: egui::Color32 = egui::Color32::from_rgb(0x2b, 0x3d, 0x54);
const TEXT: egui::Color32 = egui::Color32::from_rgb(0xe2, 0xec, 0xf7);
const MUTED_TEXT: egui::Color32 = egui::Color32::from_rgb(0xa4, 0xb2, 0xc5);
const SUCCESS: egui::Color32 = egui::Color32::from_rgb(0x63, 0xe6, 0xbe);
const WARNING: egui::Color32 = egui::Color32::from_rgb(0xff, 0xb0, 0x57);
const DANGER: egui::Color32 = egui::Color32::from_rgb(0xff, 0x6c, 0x6c);
const PANEL_MIN_HEIGHT: f32 = 180.0;
const LABEL_COL_WIDTH: f32 = 118.0;
const CONTROL_WIDTH: f32 = 218.0;
const BUTTON_WIDTH: f32 = 104.0;
const BUTTON_HEIGHT: f32 = 32.0;
const FIELD_HEIGHT: f32 = 28.0;

fn main() -> eframe::Result<()> {
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default().with_inner_size([980.0, 720.0]),
        ..Default::default()
    };
    eframe::run_native(
        "GofilePan",
        options,
        Box::new(|cc| Box::new(GofilePanApp::new(cc))),
    )
}

struct GofilePanApp {
    settings: AppSettings,
    url_input: String,
    password_input: String,
    status: AppStatus,
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
    fn new(cc: &eframe::CreationContext<'_>) -> Self {
        apply_tech_theme(&cc.egui_ctx);
        let (tx, rx) = mpsc::channel();
        let settings = AppSettings::load();
        Self {
            url_input: String::new(),
            password_input: String::new(),
            status: AppStatus::Ready,
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
        self.status = AppStatus::Discovering;

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
                self.status = AppStatus::Failed;
                self.logs.push(error.to_string());
                return;
            }
        };
        self.active_manager = Some(manager.clone());
        self.status = AppStatus::Downloading;

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
        self.status = AppStatus::CancellationRequested;
    }

    fn drain_messages(&mut self) {
        while let Ok(message) = self.rx.try_recv() {
            match message {
                GuiMessage::DiscoveryFinished(Ok(plan)) => {
                    self.status = AppStatus::Discovered(plan.files.len());
                    self.selected_files = plan.files.iter().map(|file| file.index).collect();
                    self.discovered_plan = Some(plan);
                }
                GuiMessage::DiscoveryFinished(Err(error)) => {
                    self.status = AppStatus::DiscoveryFailed;
                    self.logs.push(error);
                }
                GuiMessage::DownloadEvent(event) => self.apply_event(event),
                GuiMessage::DownloadFinished(Ok(())) => {
                    self.status = AppStatus::Done;
                    self.active_manager = None;
                }
                GuiMessage::DownloadFinished(Err(error)) => {
                    self.status = AppStatus::Failed;
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

        egui::TopBottomPanel::top("top")
            .frame(section_frame(0.0))
            .show(ctx, |ui| {
                ui.horizontal(|ui| {
                    ui.add_space(4.0);
                    ui.label(
                        egui::RichText::new("GofilePan")
                            .strong()
                            .color(TEXT)
                            .size(20.0),
                    );
                    ui.separator();
                    ui.label(egui::RichText::new(self.t(TextKey::Subtitle)).color(MUTED_TEXT));
                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                        status_badge(ui, &self.status_label());
                    });
                });
            });

        egui::SidePanel::left("settings")
            .resizable(false)
            .default_width(310.0)
            .frame(section_frame(0.0))
            .show(ctx, |ui| {
                let settings_title = self.t(TextKey::Settings);
                let download_directory_label = self.t(TextKey::DownloadDirectory);
                let token_label = self.t(TextKey::Token);
                let user_agent_label = self.t(TextKey::UserAgent);
                let concurrent_label = self.t(TextKey::Concurrent);
                let retries_label = self.t(TextKey::Retries);
                let timeout_label = self.t(TextKey::Timeout);
                let chunk_size_label = self.t(TextKey::ChunkSize);
                let chunk_prefix = self.t(TextKey::ChunkPrefix);
                let language_label = self.t(TextKey::Language);
                let language_system = self.t(TextKey::LanguageSystem);
                let language_chinese = self.t(TextKey::LanguageChinese);
                let language_english = self.t(TextKey::LanguageEnglish);
                let save_settings_label = self.t(TextKey::SaveSettings);

                panel_block(ui, settings_title, |ui| {
                    settings_grid(ui, |ui| {
                        ui.label(download_directory_label);
                        ui.horizontal(|ui| {
                            ui.add_sized(
                                [CONTROL_WIDTH, FIELD_HEIGHT],
                                egui::TextEdit::singleline(&mut self.settings.download_dir),
                            );
                            if ui
                                .add_sized([42.0, FIELD_HEIGHT], egui::Button::new("..."))
                                .clicked()
                            {
                                if let Some(path) = rfd::FileDialog::new().pick_folder() {
                                    self.settings.download_dir = path.display().to_string();
                                }
                            }
                        });
                        ui.end_row();

                        ui.label(token_label);
                        ui.add_sized(
                            [CONTROL_WIDTH, FIELD_HEIGHT],
                            egui::TextEdit::singleline(&mut self.settings.token),
                        );
                        ui.end_row();

                        ui.label(user_agent_label);
                        ui.add_sized(
                            [CONTROL_WIDTH, FIELD_HEIGHT],
                            egui::TextEdit::singleline(&mut self.settings.user_agent),
                        );
                        ui.end_row();

                        ui.label(concurrent_label);
                        ui.add_sized(
                            [CONTROL_WIDTH, FIELD_HEIGHT],
                            egui::Slider::new(&mut self.settings.max_concurrent, 1..=32)
                                .text(concurrent_label),
                        );
                        ui.end_row();

                        ui.label(retries_label);
                        ui.add_sized(
                            [CONTROL_WIDTH, FIELD_HEIGHT],
                            egui::Slider::new(&mut self.settings.retries, 1..=20)
                                .text(retries_label),
                        );
                        ui.end_row();

                        ui.label(timeout_label);
                        ui.add_sized(
                            [CONTROL_WIDTH, FIELD_HEIGHT],
                            egui::Slider::new(&mut self.settings.timeout_seconds, 1.0..=120.0)
                                .text(timeout_label),
                        );
                        ui.end_row();

                        ui.label(chunk_size_label);
                        ui.add_sized(
                            [CONTROL_WIDTH, FIELD_HEIGHT],
                            egui::DragValue::new(&mut self.settings.chunk_size)
                                .speed(1024.0)
                                .prefix(chunk_prefix),
                        );
                        ui.end_row();

                        ui.label(language_label);
                        egui::ComboBox::from_id_source("language-select")
                            .selected_text(self.settings.language.display_name())
                            .width(CONTROL_WIDTH)
                            .show_ui(ui, |ui| {
                                ui.selectable_value(
                                    &mut self.settings.language,
                                    LanguageChoice::System,
                                    language_system,
                                );
                                ui.selectable_value(
                                    &mut self.settings.language,
                                    LanguageChoice::Chinese,
                                    language_chinese,
                                );
                                ui.selectable_value(
                                    &mut self.settings.language,
                                    LanguageChoice::English,
                                    language_english,
                                );
                            });
                        ui.end_row();

                        ui.label("");
                        if ui
                            .add_sized(
                                [CONTROL_WIDTH, BUTTON_HEIGHT],
                                egui::Button::new(save_settings_label),
                            )
                            .clicked()
                        {
                            self.settings.save();
                            self.status = AppStatus::SettingsSaved;
                        }
                        ui.end_row();
                    });
                });
            });

        egui::CentralPanel::default()
            .frame(section_frame(0.0))
            .show(ctx, |ui| {
                let mission_title = self.t(TextKey::MissionControl);
                let url_label = self.t(TextKey::UrlOrBatch);
                let password_label = self.t(TextKey::Password);
                let discover_label = self.t(TextKey::Discover);
                let start_label = self.t(TextKey::Start);
                let cancel_label = self.t(TextKey::Cancel);
                let files_title = self.t(TextKey::Files);
                let selected_files_hint = self.t(TextKey::SelectedFilesHint);
                let no_files_label = self.t(TextKey::NoFiles);
                let progress_title = self.t(TextKey::Progress);
                let no_progress_label = self.t(TextKey::NoProgress);
                let log_title = self.t(TextKey::Log);
                let no_log_label = self.t(TextKey::NoLog);

                panel_block(ui, mission_title, |ui| {
                    ui.label(url_label);
                    ui.add(
                        egui::TextEdit::multiline(&mut self.url_input)
                            .desired_rows(6)
                            .desired_width(f32::INFINITY),
                    );
                    ui.horizontal(|ui| {
                        ui.add_sized(
                            [LABEL_COL_WIDTH, FIELD_HEIGHT],
                            egui::Label::new(password_label),
                        );
                        ui.add_sized(
                            [CONTROL_WIDTH, FIELD_HEIGHT],
                            egui::TextEdit::singleline(&mut self.password_input),
                        );
                    });
                    ui.horizontal(|ui| {
                        if ui
                            .add_sized(
                                [BUTTON_WIDTH, BUTTON_HEIGHT],
                                egui::Button::new(discover_label),
                            )
                            .clicked()
                        {
                            self.discover_single();
                        }
                        if ui
                            .add_sized(
                                [BUTTON_WIDTH, BUTTON_HEIGHT],
                                egui::Button::new(start_label)
                                    .fill(ACCENT_SOFT)
                                    .stroke(egui::Stroke::new(1.0, ACCENT)),
                            )
                            .clicked()
                        {
                            self.start_download();
                        }
                        if ui
                            .add_sized(
                                [BUTTON_WIDTH, BUTTON_HEIGHT],
                                egui::Button::new(cancel_label),
                            )
                            .clicked()
                        {
                            self.cancel_download();
                        }
                    });
                });

                if let Some(plan) = &self.discovered_plan {
                    panel_block(ui, files_title, |ui| {
                        egui::ScrollArea::vertical()
                            .max_height(PANEL_MIN_HEIGHT)
                            .show(ui, |ui| {
                                ui.label(selected_files_hint);
                                for file in &plan.files {
                                    let mut selected = self.selected_files.contains(&file.index);
                                    if ui
                                        .checkbox(
                                            &mut selected,
                                            format!(
                                                "[{}] {}",
                                                file.index,
                                                file.destination().display()
                                            ),
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
                    });
                } else {
                    panel_block(ui, files_title, |ui| {
                        empty_state(ui, no_files_label);
                    });
                }

                panel_block(ui, progress_title, |ui| {
                    egui::ScrollArea::vertical()
                        .max_height(PANEL_MIN_HEIGHT)
                        .show(ui, |ui| {
                            let mut rows: Vec<_> = self.progresses.iter().collect();
                            rows.sort_by_key(|(index, _)| **index);
                            if rows.is_empty() {
                                empty_state(ui, no_progress_label);
                            }
                            for (index, progress) in rows {
                                progress_row(ui, *index, progress, self);
                            }
                        });
                });

                panel_block(ui, log_title, |ui| {
                    egui::ScrollArea::vertical().show(ui, |ui| {
                        if self.logs.is_empty() {
                            empty_state(ui, no_log_label);
                        } else {
                            for line in self.logs.iter().rev().take(100) {
                                ui.label(line);
                            }
                        }
                    });
                });
            });

        ctx.request_repaint_after(Duration::from_millis(100));
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
struct AppSettings {
    download_dir: String,
    token: String,
    max_concurrent: usize,
    retries: usize,
    timeout_seconds: f64,
    chunk_size: usize,
    user_agent: String,
    language: LanguageChoice,
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
            language: LanguageChoice::System,
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

#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum LanguageChoice {
    System,
    Chinese,
    English,
}

impl Default for LanguageChoice {
    fn default() -> Self {
        Self::System
    }
}

impl LanguageChoice {
    fn resolved(self) -> UiLanguage {
        match self {
            LanguageChoice::Chinese => UiLanguage::Chinese,
            LanguageChoice::English => UiLanguage::English,
            LanguageChoice::System => detect_language(),
        }
    }

    fn display_name(self) -> &'static str {
        match self {
            LanguageChoice::System => "System / 系统",
            LanguageChoice::Chinese => "中文",
            LanguageChoice::English => "English",
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum UiLanguage {
    Chinese,
    English,
}

fn detect_language() -> UiLanguage {
    let locale = std::env::var("LC_ALL")
        .or_else(|_| std::env::var("LANG"))
        .or_else(|_| std::env::var("LANGUAGE"))
        .unwrap_or_default()
        .to_lowercase();
    if locale.contains("zh") {
        UiLanguage::Chinese
    } else {
        UiLanguage::English
    }
}

#[derive(Debug, Clone)]
enum AppStatus {
    Ready,
    Discovering,
    Discovered(usize),
    Downloading,
    Done,
    Failed,
    SettingsSaved,
    DiscoveryFailed,
    CancellationRequested,
}

#[derive(Debug, Copy, Clone)]
enum ProgressState {
    Pending,
    Active,
    Done,
}

impl FileProgress {
    fn state(&self) -> ProgressState {
        if self.done {
            ProgressState::Done
        } else if self.total > 0 {
            ProgressState::Active
        } else {
            ProgressState::Pending
        }
    }
}

#[derive(Debug, Copy, Clone)]
enum TextKey {
    Subtitle,
    Settings,
    DownloadDirectory,
    Token,
    UserAgent,
    Concurrent,
    Retries,
    Timeout,
    ChunkSize,
    ChunkPrefix,
    Language,
    LanguageSystem,
    LanguageChinese,
    LanguageEnglish,
    SaveSettings,
    MissionControl,
    UrlOrBatch,
    Password,
    Discover,
    Start,
    Cancel,
    Files,
    Progress,
    Log,
    SelectedFilesHint,
    NoFiles,
    NoProgress,
    NoLog,
    Pending,
    Downloading,
    Completed,
}

impl GofilePanApp {
    fn ui_language(&self) -> UiLanguage {
        self.settings.language.resolved()
    }

    fn t(&self, key: TextKey) -> &'static str {
        match self.ui_language() {
            UiLanguage::Chinese => match key {
                TextKey::Subtitle => "数据传输控制台",
                TextKey::Settings => "设置",
                TextKey::DownloadDirectory => "下载目录",
                TextKey::Token => "令牌",
                TextKey::UserAgent => "User Agent",
                TextKey::Concurrent => "并发数",
                TextKey::Retries => "重试次数",
                TextKey::Timeout => "超时",
                TextKey::ChunkSize => "分块大小",
                TextKey::ChunkPrefix => "块 ",
                TextKey::Language => "语言",
                TextKey::LanguageSystem => "系统",
                TextKey::LanguageChinese => "中文",
                TextKey::LanguageEnglish => "English",
                TextKey::SaveSettings => "保存设置",
                TextKey::MissionControl => "任务控制",
                TextKey::UrlOrBatch => "链接或批量行",
                TextKey::Password => "密码",
                TextKey::Discover => "探测",
                TextKey::Start => "开始",
                TextKey::Cancel => "取消",
                TextKey::Files => "文件",
                TextKey::Progress => "进度",
                TextKey::Log => "日志",
                TextKey::SelectedFilesHint => "可勾选需要下载的文件",
                TextKey::NoFiles => "尚未探测到文件",
                TextKey::NoProgress => "暂无下载进度",
                TextKey::NoLog => "暂无日志",
                TextKey::Pending => "等待中",
                TextKey::Downloading => "下载中",
                TextKey::Completed => "已完成",
            },
            UiLanguage::English => match key {
                TextKey::Subtitle => "DATA TRANSFER CONSOLE",
                TextKey::Settings => "Settings",
                TextKey::DownloadDirectory => "Download directory",
                TextKey::Token => "Token",
                TextKey::UserAgent => "User agent",
                TextKey::Concurrent => "Concurrent",
                TextKey::Retries => "Retries",
                TextKey::Timeout => "Timeout",
                TextKey::ChunkSize => "Chunk size",
                TextKey::ChunkPrefix => "Chunk ",
                TextKey::Language => "Language",
                TextKey::LanguageSystem => "System",
                TextKey::LanguageChinese => "Chinese",
                TextKey::LanguageEnglish => "English",
                TextKey::SaveSettings => "Save Settings",
                TextKey::MissionControl => "Mission Control",
                TextKey::UrlOrBatch => "URL or batch lines",
                TextKey::Password => "Password",
                TextKey::Discover => "Discover",
                TextKey::Start => "Start",
                TextKey::Cancel => "Cancel",
                TextKey::Files => "Files",
                TextKey::Progress => "Progress",
                TextKey::Log => "Log",
                TextKey::SelectedFilesHint => "Select the files you want to download",
                TextKey::NoFiles => "No files discovered yet",
                TextKey::NoProgress => "No active downloads yet",
                TextKey::NoLog => "No logs yet",
                TextKey::Pending => "Pending",
                TextKey::Downloading => "Downloading",
                TextKey::Completed => "Completed",
            },
        }
    }

    fn status_label(&self) -> String {
        match self.ui_language() {
            UiLanguage::Chinese => match &self.status {
                AppStatus::Ready => "就绪".to_string(),
                AppStatus::Discovering => "正在探测文件...".to_string(),
                AppStatus::Discovered(count) => format!("已探测到 {count} 个文件"),
                AppStatus::Downloading => "正在下载...".to_string(),
                AppStatus::Done => "完成".to_string(),
                AppStatus::Failed => "失败".to_string(),
                AppStatus::SettingsSaved => "设置已保存".to_string(),
                AppStatus::DiscoveryFailed => "探测失败".to_string(),
                AppStatus::CancellationRequested => "已请求取消".to_string(),
            },
            UiLanguage::English => match &self.status {
                AppStatus::Ready => "Ready".to_string(),
                AppStatus::Discovering => "Discovering files...".to_string(),
                AppStatus::Discovered(count) => format!("Discovered {count} files"),
                AppStatus::Downloading => "Downloading...".to_string(),
                AppStatus::Done => "Done".to_string(),
                AppStatus::Failed => "Failed".to_string(),
                AppStatus::SettingsSaved => "Settings saved".to_string(),
                AppStatus::DiscoveryFailed => "Discovery failed".to_string(),
                AppStatus::CancellationRequested => "Cancellation requested".to_string(),
            },
        }
    }
}

fn empty_state(ui: &mut egui::Ui, message: &str) {
    ui.vertical_centered(|ui| {
        ui.add_space(10.0);
        ui.label(egui::RichText::new(message).color(MUTED_TEXT));
        ui.add_space(10.0);
    });
}

fn settings_grid(ui: &mut egui::Ui, add_contents: impl FnOnce(&mut egui::Ui)) {
    egui::Grid::new("settings-grid")
        .num_columns(2)
        .spacing(egui::vec2(10.0, 10.0))
        .show(ui, |ui| add_contents(ui));
}

fn progress_row(ui: &mut egui::Ui, index: usize, progress: &FileProgress, app: &GofilePanApp) {
    let state = progress.state();
    let (label, color, fraction, animate) = match state {
        ProgressState::Pending => (app.t(TextKey::Pending), BORDER, 0.0, false),
        ProgressState::Active => {
            let fraction = if progress.total == 0 {
                0.0
            } else {
                progress.downloaded as f32 / progress.total as f32
            };
            (app.t(TextKey::Downloading), ACCENT, fraction, true)
        }
        ProgressState::Done => (app.t(TextKey::Completed), SUCCESS, 1.0, false),
    };

    ui.group(|ui| {
        ui.horizontal(|ui| {
            ui.label(
                egui::RichText::new(format!("[{index}] {}", progress.path))
                    .strong()
                    .color(TEXT),
            );
            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                status_chip(ui, label, color);
            });
        });
        ui.add_space(4.0);
        ui.add(
            egui::ProgressBar::new(fraction)
                .fill(color)
                .animate(animate)
                .rounding(egui::Rounding::same(6.0))
                .desired_width(f32::INFINITY)
                .show_percentage(),
        );
    });
    ui.add_space(8.0);
}

fn status_chip(ui: &mut egui::Ui, status: &str, color: egui::Color32) {
    egui::Frame::none()
        .fill(color.linear_multiply(0.16))
        .stroke(egui::Stroke::new(1.0, color))
        .rounding(egui::Rounding::same(999.0))
        .inner_margin(egui::Margin::symmetric(12.0, 5.0))
        .show(ui, |ui| {
            ui.label(egui::RichText::new(status).color(color).strong());
        });
}

fn apply_tech_theme(ctx: &egui::Context) {
    let mut visuals = egui::Visuals::dark();
    visuals.override_text_color = Some(TEXT);
    visuals.widgets.noninteractive.bg_fill = PANEL_BG;
    visuals.widgets.noninteractive.weak_bg_fill = SURFACE_BG;
    visuals.widgets.noninteractive.bg_stroke = egui::Stroke::new(1.0, BORDER);
    visuals.widgets.noninteractive.fg_stroke.color = MUTED_TEXT;
    visuals.widgets.inactive.bg_fill = SURFACE_BG;
    visuals.widgets.inactive.bg_stroke = egui::Stroke::new(1.0, BORDER);
    visuals.widgets.inactive.fg_stroke.color = TEXT;
    visuals.widgets.hovered.bg_fill = egui::Color32::from_rgb(0x1a, 0x2a, 0x38);
    visuals.widgets.hovered.bg_stroke = egui::Stroke::new(1.0, ACCENT);
    visuals.widgets.hovered.fg_stroke.color = TEXT;
    visuals.widgets.active.bg_fill = egui::Color32::from_rgb(0x1d, 0x46, 0x58);
    visuals.widgets.active.bg_stroke = egui::Stroke::new(1.0, ACCENT);
    visuals.widgets.active.fg_stroke.color = TEXT;
    visuals.widgets.open.bg_fill = visuals.widgets.hovered.bg_fill;
    visuals.widgets.open.bg_stroke = visuals.widgets.hovered.bg_stroke;
    visuals.selection.bg_fill = egui::Color32::from_rgba_unmultiplied(0x35, 0xd3, 0xff, 60);
    visuals.selection.stroke = egui::Stroke::new(1.0, ACCENT);
    visuals.hyperlink_color = ACCENT;
    visuals.faint_bg_color = egui::Color32::from_rgb(0x12, 0x18, 0x21);
    visuals.extreme_bg_color = egui::Color32::from_rgb(0x08, 0x0c, 0x12);
    visuals.code_bg_color = egui::Color32::from_rgb(0x12, 0x18, 0x24);
    visuals.warn_fg_color = WARNING;
    visuals.error_fg_color = DANGER;
    visuals.window_rounding = egui::Rounding::same(10.0);
    visuals.window_fill = PANEL_BG;
    visuals.window_stroke = egui::Stroke::new(1.0, BORDER);
    visuals.panel_fill = PANEL_BG;
    visuals.popup_shadow = egui::epaint::Shadow::NONE;
    visuals.button_frame = true;
    visuals.slider_trailing_fill = true;
    visuals.handle_shape = egui::style::HandleShape::Rect { aspect_ratio: 0.35 };

    let mut style = (*ctx.style()).clone();
    style.visuals = visuals;
    style.spacing.item_spacing = egui::vec2(10.0, 8.0);
    style.spacing.button_padding = egui::vec2(12.0, 7.0);
    style.spacing.menu_margin = egui::Margin::same(6.0);
    style.spacing.window_margin = egui::Margin::same(10.0);
    ctx.set_style(style);
}

fn section_frame(top_margin: f32) -> egui::Frame {
    egui::Frame::none()
        .fill(PANEL_BG)
        .stroke(egui::Stroke::new(1.0, BORDER))
        .rounding(egui::Rounding::same(0.0))
        .inner_margin(egui::Margin {
            left: 12.0,
            right: 12.0,
            top: top_margin,
            bottom: 12.0,
        })
}

fn panel_block(ui: &mut egui::Ui, title: &str, add_contents: impl FnOnce(&mut egui::Ui)) {
    egui::Frame::group(ui.style())
        .fill(SURFACE_BG)
        .stroke(egui::Stroke::new(1.0, BORDER))
        .rounding(egui::Rounding::same(8.0))
        .inner_margin(egui::Margin::same(12.0))
        .show(ui, |ui| {
            ui.horizontal(|ui| {
                ui.label(egui::RichText::new(title).strong().color(ACCENT).size(15.0));
                ui.add_space(8.0);
                ui.separator();
            });
            ui.add_space(8.0);
            add_contents(ui);
        });
    ui.add_space(10.0);
}

fn status_badge(ui: &mut egui::Ui, status: &str) {
    status_chip(ui, status, ACCENT);
}
