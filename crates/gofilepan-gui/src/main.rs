#![cfg_attr(target_os = "windows", windows_subsystem = "windows")]

use anyhow::Context;
use gofilepan_core::{
    parse_batch_lines, DownloadConfig, DownloadEvent, DownloadManager, DownloadPlan,
    DownloadRequest,
};
use serde::{Deserialize, Serialize};
use slint::{Color, Model, ModelRc, SharedString, Timer, TimerMode, VecModel};
use std::{collections::HashSet, path::PathBuf, rc::Rc, sync::mpsc, time::Duration};
use tokio::runtime::Runtime;

slint::include_modules!();

const ACCENT: Color = Color::from_rgb_u8(0x35, 0xd3, 0xff);
const SUCCESS: Color = Color::from_rgb_u8(0x63, 0xe6, 0xbe);
const WARNING: Color = Color::from_rgb_u8(0xff, 0xb0, 0x57);
const DANGER: Color = Color::from_rgb_u8(0xff, 0x6c, 0x6c);
const PENDING: Color = Color::from_rgb_u8(0x5c, 0x6f, 0x82);
const MAX_LOGS: usize = 200;

fn main() -> anyhow::Result<()> {
    let window = MainWindow::new()?;
    let state = Rc::new(std::cell::RefCell::new(AppState::new()?));

    {
        let mut state = state.borrow_mut();
        state.bind_models(&window);
        state.load_settings_into_window(&window);
        state.refresh_language(&window);
        state.refresh_status(&window);
        state.sync_counts(&window);
    }

    install_callbacks(&window, state.clone());

    let timer = Timer::default();
    let window_weak = window.as_weak();
    let state_for_timer = state.clone();
    timer.start(TimerMode::Repeated, Duration::from_millis(75), move || {
        if let Some(window) = window_weak.upgrade() {
            state_for_timer.borrow_mut().drain_messages(&window);
        }
    });

    window.run()?;
    Ok(())
}

fn install_callbacks(window: &MainWindow, state: Rc<std::cell::RefCell<AppState>>) {
    let discover_state = state.clone();
    let discover_weak = window.as_weak();
    window.on_request_discover(move || {
        if let Some(window) = discover_weak.upgrade() {
            discover_state.borrow_mut().request_discover(&window);
        }
    });

    let start_state = state.clone();
    let start_weak = window.as_weak();
    window.on_request_start(move || {
        if let Some(window) = start_weak.upgrade() {
            start_state.borrow_mut().request_start(&window);
        }
    });

    let cancel_state = state.clone();
    let cancel_weak = window.as_weak();
    window.on_request_cancel(move || {
        if let Some(window) = cancel_weak.upgrade() {
            cancel_state.borrow_mut().request_cancel(&window);
        }
    });

    let save_state = state.clone();
    let save_weak = window.as_weak();
    window.on_request_save_settings(move || {
        if let Some(window) = save_weak.upgrade() {
            save_state.borrow_mut().request_save_settings(&window);
        }
    });

    let pick_state = state.clone();
    let pick_weak = window.as_weak();
    window.on_request_pick_folder(move || {
        if let Some(window) = pick_weak.upgrade() {
            pick_state.borrow_mut().request_pick_folder(&window);
        }
    });

    let clear_state = state.clone();
    let clear_weak = window.as_weak();
    window.on_request_clear_logs(move || {
        if let Some(window) = clear_weak.upgrade() {
            clear_state.borrow_mut().clear_logs(&window);
        }
    });

    let all_state = state.clone();
    let all_weak = window.as_weak();
    window.on_request_select_all(move || {
        if let Some(window) = all_weak.upgrade() {
            all_state.borrow_mut().set_all_selected(&window, true);
        }
    });

    let none_state = state.clone();
    let none_weak = window.as_weak();
    window.on_request_select_none(move || {
        if let Some(window) = none_weak.upgrade() {
            none_state.borrow_mut().set_all_selected(&window, false);
        }
    });

    let file_state = state.clone();
    let file_weak = window.as_weak();
    window.on_file_toggled(move |index, checked| {
        if let Some(window) = file_weak.upgrade() {
            file_state
                .borrow_mut()
                .toggle_file(&window, index as usize, checked);
        }
    });

    let lang_state = state;
    let lang_weak = window.as_weak();
    window.on_language_changed(move |_| {
        if let Some(window) = lang_weak.upgrade() {
            lang_state.borrow_mut().apply_language_from_window(&window);
        }
    });
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

struct AppState {
    settings: AppSettings,
    status: AppStatus,
    runtime: Runtime,
    tx: mpsc::Sender<GuiMessage>,
    rx: mpsc::Receiver<GuiMessage>,
    active_manager: Option<DownloadManager>,
    discovered_plan: Option<DownloadPlan>,
    selected_files: HashSet<usize>,
    files: Rc<VecModel<FileItem>>,
    progresses: Rc<VecModel<ProgressItem>>,
    logs: Rc<VecModel<SharedString>>,
}

impl AppState {
    fn new() -> anyhow::Result<Self> {
        let settings = AppSettings::load();
        let (tx, rx) = mpsc::channel();
        Ok(Self {
            settings,
            status: AppStatus::Ready,
            runtime: Runtime::new().context("create tokio runtime")?,
            tx,
            rx,
            active_manager: None,
            discovered_plan: None,
            selected_files: HashSet::new(),
            files: Rc::new(VecModel::default()),
            progresses: Rc::new(VecModel::default()),
            logs: Rc::new(VecModel::default()),
        })
    }

    fn bind_models(&mut self, window: &MainWindow) {
        window.set_files(ModelRc::from(self.files.clone()));
        window.set_progresses(ModelRc::from(self.progresses.clone()));
        window.set_logs(ModelRc::from(self.logs.clone()));
    }

    fn load_settings_into_window(&self, window: &MainWindow) {
        window.set_download_dir(shared(self.settings.download_dir.clone()));
        window.set_token_input(shared(self.settings.token.clone()));
        window.set_max_concurrent(self.settings.max_concurrent.clamp(1, 32) as i32);
        window.set_retries(self.settings.retries.clamp(1, 20) as i32);
        window.set_timeout_seconds_input(format_number(self.settings.timeout_seconds));
        window.set_chunk_size(self.settings.chunk_size.clamp(1024, i32::MAX as usize) as i32);
        window.set_user_agent_input(shared(self.settings.user_agent.clone()));
        window.set_language_index(language_index(self.settings.language));
        window.set_selected_count(0);
        window.set_files_count(0);
        window.set_progress_count(0);
        window.set_logs_count(0);
        window.set_busy(false);
    }

    fn refresh_language(&self, window: &MainWindow) {
        let chinese = matches!(self.settings.language.resolved(), UiLanguage::Chinese);
        window.set_chinese(chinese);
    }

    fn refresh_status(&self, window: &MainWindow) {
        window.set_busy(matches!(
            self.status,
            AppStatus::Discovering | AppStatus::Downloading | AppStatus::CancellationRequested
        ));
        window.set_status_text(shared(status_text(&self.status, self.is_chinese())));
        window.set_status_color(status_color(&self.status));
    }

    fn sync_counts(&self, window: &MainWindow) {
        window.set_files_count(self.files.row_count() as i32);
        window.set_progress_count(self.progresses.row_count() as i32);
        window.set_logs_count(self.logs.row_count() as i32);
        window.set_selected_count(self.selected_files.len() as i32);
    }

    fn is_chinese(&self) -> bool {
        matches!(self.settings.language.resolved(), UiLanguage::Chinese)
    }

    fn collect_settings_from_window(&self, window: &MainWindow) -> AppSettings {
        let current_language = language_choice_from_index(window.get_language_index());
        let download_dir = window.get_download_dir().trim().to_string();
        let token = window.get_token_input().trim().to_string();
        let user_agent = window.get_user_agent_input().trim().to_string();
        let timeout_seconds = window
            .get_timeout_seconds_input()
            .trim()
            .parse::<f64>()
            .ok()
            .filter(|value| *value >= 1.0)
            .unwrap_or(self.settings.timeout_seconds);

        AppSettings {
            download_dir: if download_dir.is_empty() {
                self.settings.download_dir.clone()
            } else {
                download_dir
            },
            token,
            max_concurrent: window.get_max_concurrent().max(1) as usize,
            retries: window.get_retries().max(1) as usize,
            timeout_seconds,
            chunk_size: window.get_chunk_size().max(1024) as usize,
            user_agent: if user_agent.is_empty() {
                self.settings.user_agent.clone()
            } else {
                user_agent
            },
            language: current_language,
        }
    }

    fn build_config(&self, window: &MainWindow) -> DownloadConfig {
        let settings = self.collect_settings_from_window(window);
        let mut config = DownloadConfig::from_env();
        config.download_dir = PathBuf::from(settings.download_dir.trim());
        config.token = empty_to_none(&settings.token);
        config.max_concurrent = settings.max_concurrent.max(1);
        config.retries = settings.retries.max(1);
        config.timeout = Duration::from_secs_f64(settings.timeout_seconds.max(1.0));
        config.chunk_size = settings.chunk_size.max(1);
        if !settings.user_agent.trim().is_empty() {
            config.user_agent = settings.user_agent;
        }
        config
    }

    fn request_discover(&mut self, window: &MainWindow) {
        if self.get_busy(window) {
            return;
        }

        self.discovered_plan = None;
        self.selected_files.clear();
        self.files.clear();
        self.progresses.clear();
        self.sync_counts(window);
        self.set_status(window, AppStatus::Discovering);

        let request = DownloadRequest::new(
            window.get_url_input().trim(),
            empty_to_none(&window.get_password_input()),
        );
        let tx = self.tx.clone();
        let config = self.build_config(window);

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

    fn request_start(&mut self, window: &MainWindow) {
        if self.get_busy(window) {
            return;
        }

        self.settings = self.collect_settings_from_window(window);
        self.settings.save();
        let config = self.build_config(window);
        let password = empty_to_none(&window.get_password_input());
        let input = window.get_url_input().to_string();
        let existing_plan = self.discovered_plan.clone();
        let selected_files = self.selected_files.clone();
        self.discovered_plan = None;
        self.selected_files.clear();
        self.files.clear();
        self.progresses.clear();
        self.sync_counts(window);
        self.set_status(window, AppStatus::Downloading);
        self.logs.clear();
        let tx = self.tx.clone();
        let manager = match DownloadManager::new(config) {
            Ok(manager) => manager,
            Err(error) => {
                self.append_log(window, error.to_string().into());
                self.set_status(window, AppStatus::Failed);
                return;
            }
        };

        self.active_manager = Some(manager.clone());

        self.runtime.spawn(async move {
            let result = async {
                forward_events(manager.subscribe(), tx.clone());

                if let Some(plan) = existing_plan {
                    let mut request = plan.request.clone();
                    request.selected_files = Some(selected_files);
                    let plan = manager.discover(request).await?;
                    let _ = tx.send(GuiMessage::DiscoveryFinished(Ok(plan.clone())));
                    manager.download(plan).await?;
                } else if looks_like_batch(&input) {
                    for request in parse_batch_lines(&input, password.as_deref()) {
                        let plan = manager.discover(request).await?;
                        let _ = tx.send(GuiMessage::DiscoveryFinished(Ok(plan.clone())));
                        manager.download(plan).await?;
                    }
                } else {
                    let request = DownloadRequest::new(input.trim(), password);
                    let plan = manager.discover(request).await?;
                    let _ = tx.send(GuiMessage::DiscoveryFinished(Ok(plan.clone())));
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

    fn request_cancel(&mut self, window: &MainWindow) {
        if let Some(manager) = &self.active_manager {
            manager.cancel();
        }
        self.set_status(window, AppStatus::CancellationRequested);
    }

    fn request_save_settings(&mut self, window: &MainWindow) {
        self.settings = self.collect_settings_from_window(window);
        self.settings.save();
        self.refresh_language(window);
        self.refresh_status(window);
        let saved_message = self.translate("设置已保存", "Settings saved");
        self.append_log(window, saved_message);
        self.set_status(window, AppStatus::SettingsSaved);
    }

    fn request_pick_folder(&mut self, window: &MainWindow) {
        if let Some(path) = rfd::FileDialog::new().pick_folder() {
            window.set_download_dir(shared(path.display().to_string()));
        }
    }

    fn clear_logs(&mut self, window: &MainWindow) {
        self.logs.clear();
        self.sync_counts(window);
    }

    fn set_all_selected(&mut self, window: &MainWindow, checked: bool) {
        self.selected_files.clear();
        for row in 0..self.files.row_count() {
            if let Some(mut item) = self.files.row_data(row) {
                item.checked = checked;
                self.files.set_row_data(row, item.clone());
                if checked {
                    self.selected_files.insert(item.index as usize);
                }
            }
        }
        self.sync_counts(window);
    }

    fn toggle_file(&mut self, window: &MainWindow, index: usize, checked: bool) {
        if let Some(mut item) = self.files.row_data(index) {
            item.checked = checked;
            self.files.set_row_data(index, item.clone());
            if checked {
                self.selected_files.insert(index);
            } else {
                self.selected_files.remove(&index);
            }
        }
        self.sync_counts(window);
    }

    fn apply_language_from_window(&mut self, window: &MainWindow) {
        self.settings.language = language_choice_from_index(window.get_language_index());
        self.refresh_language(window);
        self.relabel_progress_rows();
        self.refresh_status(window);
    }

    fn drain_messages(&mut self, window: &MainWindow) {
        while let Ok(message) = self.rx.try_recv() {
            match message {
                GuiMessage::DiscoveryFinished(Ok(plan)) => {
                    self.active_manager = None;
                    self.set_plan(window, plan);
                    self.set_status(window, AppStatus::Discovered(self.files.row_count()));
                }
                GuiMessage::DiscoveryFinished(Err(error)) => {
                    self.active_manager = None;
                    self.set_status(window, AppStatus::DiscoveryFailed);
                    self.append_log(window, shared(error));
                }
                GuiMessage::DownloadEvent(event) => self.apply_event(window, event),
                GuiMessage::DownloadFinished(Ok(())) => {
                    self.active_manager = None;
                    self.set_status(window, AppStatus::Done);
                }
                GuiMessage::DownloadFinished(Err(error)) => {
                    self.active_manager = None;
                    if matches!(self.status, AppStatus::CancellationRequested)
                        && error.to_lowercase().contains("cancel")
                    {
                        self.set_status(window, AppStatus::Cancelled);
                    } else {
                        self.set_status(window, AppStatus::Failed);
                    }
                    self.append_log(window, shared(error));
                }
            }
        }
    }

    fn apply_event(&mut self, window: &MainWindow, event: DownloadEvent) {
        match event {
            DownloadEvent::FileDiscovered { index, path } => {
                self.append_log(window, shared(format!("[{index}] {}", path.display())));
            }
            DownloadEvent::Started { index, path } => {
                self.set_status(window, AppStatus::Downloading);
                self.update_progress_row(index, path, ProgressKind::Downloading, 0, 0, 0.0);
            }
            DownloadEvent::Progress {
                index,
                path,
                downloaded,
                total,
            } => {
                self.update_progress_row(
                    index,
                    path,
                    ProgressKind::Downloading,
                    downloaded,
                    total,
                    fraction(downloaded, total),
                );
            }
            DownloadEvent::Completed { index, path, bytes } => {
                let done_message = self.translate("完成", "done");
                self.update_progress_row(index, path, ProgressKind::Completed, bytes, bytes, 1.0);
                self.append_log(window, shared(format!("[{index}] {done_message}")));
            }
            DownloadEvent::Skipped { index, path } => {
                let skipped_message = self.translate("跳过", "skipped");
                self.update_progress_row(index, path.clone(), ProgressKind::Skipped, 0, 0, 1.0);
                self.append_log(window, shared(format!("[{index}] {skipped_message}")));
            }
            DownloadEvent::Retry {
                index,
                message,
                attempt,
            } => {
                let retry_message = self.translate("重试", "retry");
                let prefix = index
                    .map(|value| format!(" for [{value}]"))
                    .unwrap_or_default();
                self.append_log(
                    window,
                    shared(format!("{retry_message} {attempt}{prefix}: {message}")),
                );
            }
            DownloadEvent::Failed { index, message } => {
                let failed_message = self.translate("失败", "failed");
                if let Some(index) = index {
                    self.update_progress_state(
                        index,
                        ProgressKind::Failed,
                        failed_message.clone(),
                        DANGER,
                    );
                }
                let suffix = index.map(|value| format!(" [{value}]")).unwrap_or_default();
                self.append_log(
                    window,
                    shared(format!("{failed_message}{suffix}: {message}")),
                );
            }
            DownloadEvent::Cancelled => {
                let cancelled_message = self.translate("已取消", "cancelled");
                self.append_log(window, cancelled_message);
            }
        }
    }

    fn set_plan(&mut self, window: &MainWindow, plan: DownloadPlan) {
        self.discovered_plan = Some(plan.clone());
        let selected_files: HashSet<usize> = plan
            .request
            .selected_files
            .clone()
            .unwrap_or_else(|| plan.files.iter().map(|file| file.index).collect());
        self.selected_files = selected_files.clone();
        self.files.set_vec(
            plan.files
                .iter()
                .map(|file| FileItem {
                    index: file.index as i32,
                    title: shared(file.destination().display().to_string()),
                    checked: selected_files.contains(&file.index),
                })
                .collect::<Vec<_>>(),
        );
        self.progresses.set_vec(
            plan.files
                .iter()
                .map(|file| ProgressItem {
                    index: file.index as i32,
                    title: shared(file.destination().display().to_string()),
                    kind: ProgressKind::Pending as i32,
                    status: self.translate("等待中", "Pending"),
                    detail: SharedString::from(""),
                    fraction: 0.0,
                    accent: PENDING,
                })
                .collect::<Vec<_>>(),
        );
        self.sync_counts(window);
    }

    fn relabel_progress_rows(&mut self) {
        for row in 0..self.progresses.row_count() {
            if let Some(mut item) = self.progresses.row_data(row) {
                let kind = match item.kind {
                    1 => ProgressKind::Downloading,
                    2 => ProgressKind::Completed,
                    3 => ProgressKind::Skipped,
                    4 => ProgressKind::Failed,
                    _ => ProgressKind::Pending,
                };
                item.status = self.progress_status(kind);
                item.accent = self.progress_accent(kind);
                self.progresses.set_row_data(row, item);
            }
        }
    }

    fn update_progress_row(
        &mut self,
        index: usize,
        path: std::path::PathBuf,
        kind: ProgressKind,
        downloaded: u64,
        total: u64,
        fraction_value: f32,
    ) {
        let status = self.progress_status(kind);
        let accent = self.progress_accent(kind);
        let detail = progress_detail(downloaded, total);
        if let Some(mut row) = self.progresses.row_data(index) {
            row.index = index as i32;
            row.title = shared(path.display().to_string());
            row.kind = kind as i32;
            row.status = status;
            row.detail = detail;
            row.fraction = fraction_value;
            row.accent = accent;
            self.progresses.set_row_data(index, row);
        }
    }

    fn update_progress_state(
        &mut self,
        index: usize,
        kind: ProgressKind,
        status: SharedString,
        accent: Color,
    ) {
        if let Some(mut row) = self.progresses.row_data(index) {
            row.kind = kind as i32;
            row.status = status;
            row.accent = accent;
            self.progresses.set_row_data(index, row);
        }
    }

    fn progress_status(&self, kind: ProgressKind) -> SharedString {
        match kind {
            ProgressKind::Pending => self.translate("等待中", "Pending"),
            ProgressKind::Downloading => self.translate("下载中", "Downloading"),
            ProgressKind::Completed => self.translate("已完成", "Completed"),
            ProgressKind::Skipped => self.translate("已跳过", "Skipped"),
            ProgressKind::Failed => self.translate("失败", "Failed"),
        }
    }

    fn progress_accent(&self, kind: ProgressKind) -> Color {
        match kind {
            ProgressKind::Pending => PENDING,
            ProgressKind::Downloading => ACCENT,
            ProgressKind::Completed => SUCCESS,
            ProgressKind::Skipped => WARNING,
            ProgressKind::Failed => DANGER,
        }
    }

    fn set_status(&mut self, window: &MainWindow, status: AppStatus) {
        self.status = status;
        self.refresh_status(window);
    }

    fn get_busy(&self, window: &MainWindow) -> bool {
        window.get_busy()
    }

    fn translate(&self, zh: &str, en: &str) -> SharedString {
        if self.is_chinese() {
            shared(zh)
        } else {
            shared(en)
        }
    }

    fn append_log(&mut self, window: &MainWindow, message: SharedString) {
        if self.logs.row_count() >= MAX_LOGS {
            let _ = self.logs.remove(0);
        }
        self.logs.push(message);
        self.sync_counts(window);
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
    Cancelled,
}

#[derive(Debug, Copy, Clone)]
#[repr(i32)]
enum ProgressKind {
    Pending = 0,
    Downloading = 1,
    Completed = 2,
    Skipped = 3,
    Failed = 4,
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
}

#[derive(Debug, Copy, Clone)]
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

fn status_text(status: &AppStatus, chinese: bool) -> SharedString {
    match (status, chinese) {
        (AppStatus::Ready, true) => shared("就绪"),
        (AppStatus::Ready, false) => shared("Ready"),
        (AppStatus::Discovering, true) => shared("正在探测文件..."),
        (AppStatus::Discovering, false) => shared("Discovering files..."),
        (AppStatus::Discovered(count), true) => shared(format!("已探测到 {count} 个文件")),
        (AppStatus::Discovered(count), false) => shared(format!("Discovered {count} files")),
        (AppStatus::Downloading, true) => shared("正在下载..."),
        (AppStatus::Downloading, false) => shared("Downloading..."),
        (AppStatus::Done, true) => shared("完成"),
        (AppStatus::Done, false) => shared("Done"),
        (AppStatus::Failed, true) => shared("失败"),
        (AppStatus::Failed, false) => shared("Failed"),
        (AppStatus::SettingsSaved, true) => shared("设置已保存"),
        (AppStatus::SettingsSaved, false) => shared("Settings saved"),
        (AppStatus::DiscoveryFailed, true) => shared("探测失败"),
        (AppStatus::DiscoveryFailed, false) => shared("Discovery failed"),
        (AppStatus::CancellationRequested, true) => shared("已请求取消"),
        (AppStatus::CancellationRequested, false) => shared("Cancellation requested"),
        (AppStatus::Cancelled, true) => shared("已取消"),
        (AppStatus::Cancelled, false) => shared("Cancelled"),
    }
}

fn status_color(status: &AppStatus) -> Color {
    match status {
        AppStatus::Ready | AppStatus::Discovered(_) | AppStatus::Downloading => ACCENT,
        AppStatus::Done | AppStatus::SettingsSaved => SUCCESS,
        AppStatus::DiscoveryFailed | AppStatus::Failed => DANGER,
        AppStatus::CancellationRequested | AppStatus::Cancelled => WARNING,
        AppStatus::Discovering => ACCENT,
    }
}

fn language_index(choice: LanguageChoice) -> i32 {
    match choice {
        LanguageChoice::System => 0,
        LanguageChoice::Chinese => 1,
        LanguageChoice::English => 2,
    }
}

fn language_choice_from_index(index: i32) -> LanguageChoice {
    match index {
        1 => LanguageChoice::Chinese,
        2 => LanguageChoice::English,
        _ => LanguageChoice::System,
    }
}

fn shared(value: impl Into<String>) -> SharedString {
    SharedString::from(value.into())
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

fn format_number(value: f64) -> SharedString {
    if (value - value.trunc()).abs() < f64::EPSILON {
        shared(format!("{value:.0}"))
    } else {
        shared(format!("{value}"))
    }
}

fn fraction(downloaded: u64, total: u64) -> f32 {
    if total == 0 {
        0.0
    } else {
        (downloaded as f32 / total as f32).clamp(0.0, 1.0)
    }
}

fn progress_detail(downloaded: u64, total: u64) -> SharedString {
    if total == 0 {
        shared(format_bytes(downloaded))
    } else {
        shared(format!(
            "{} / {}",
            format_bytes(downloaded),
            format_bytes(total)
        ))
    }
}

fn format_bytes(bytes: u64) -> String {
    const KB: f64 = 1024.0;
    const MB: f64 = KB * 1024.0;
    const GB: f64 = MB * 1024.0;

    let value = bytes as f64;
    if value < KB {
        format!("{bytes} B")
    } else if value < MB {
        format!("{:.1} KB", value / KB)
    } else if value < GB {
        format!("{:.1} MB", value / MB)
    } else {
        format!("{:.1} GB", value / GB)
    }
}
