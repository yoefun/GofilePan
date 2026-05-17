use async_recursion::async_recursion;
use futures_util::{stream::FuturesUnordered, StreamExt};
use reqwest::{
    header::{
        HeaderMap, HeaderValue, ACCEPT, ACCEPT_ENCODING, CONNECTION, COOKIE, ORIGIN, RANGE,
        REFERER, USER_AGENT,
    },
    Client, StatusCode,
};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use thiserror::Error;
use tokio::{
    fs,
    io::AsyncWriteExt,
    sync::{broadcast, Mutex, Semaphore},
    time::sleep,
};
use url::Url;

#[derive(Debug, Error)]
pub enum DownloadError {
    #[error("invalid Gofile URL: {0}")]
    InvalidUrl(String),
    #[error("Gofile API returned an error for {url}: {message}")]
    Api { url: String, message: String },
    #[error("password protected link or invalid password")]
    PasswordRequired,
    #[error("account creation failed")]
    AccountCreationFailed,
    #[error("download response did not include a verifiable file size")]
    MissingFileSize,
    #[error("download failed with status code {0}")]
    BadStatus(StatusCode),
    #[error("request failed: {0}")]
    Request(#[from] reqwest::Error),
    #[error("I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("task cancelled")]
    Cancelled,
}

pub type Result<T> = std::result::Result<T, DownloadError>;

#[derive(Debug, Clone)]
pub struct DownloadConfig {
    pub download_dir: PathBuf,
    pub token: Option<String>,
    pub max_concurrent: usize,
    pub retries: usize,
    pub timeout: Duration,
    pub chunk_size: usize,
    pub user_agent: String,
    pub anonymous_fallback: bool,
}

impl Default for DownloadConfig {
    fn default() -> Self {
        Self {
            download_dir: std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
            token: None,
            max_concurrent: 5,
            retries: 5,
            timeout: Duration::from_secs_f64(15.0),
            chunk_size: 2_097_152,
            user_agent: "Mozilla/5.0".to_string(),
            anonymous_fallback: true,
        }
    }
}

impl DownloadConfig {
    pub fn from_env() -> Self {
        let mut config = Self::default();
        if let Ok(value) = std::env::var("GF_DOWNLOAD_DIR") {
            config.download_dir = PathBuf::from(value);
        }
        if let Ok(value) = std::env::var("GF_TOKEN") {
            if !value.trim().is_empty() {
                config.token = Some(value);
            }
        }
        if let Ok(value) = std::env::var("GF_MAX_CONCURRENT_DOWNLOADS") {
            config.max_concurrent = value.parse().unwrap_or(config.max_concurrent);
        }
        if let Ok(value) = std::env::var("GF_MAX_RETRIES") {
            config.retries = value.parse().unwrap_or(config.retries);
        }
        if let Ok(value) = std::env::var("GF_TIMEOUT") {
            config.timeout = Duration::from_secs_f64(value.parse().unwrap_or(15.0));
        }
        if let Ok(value) = std::env::var("GF_CHUNK_SIZE") {
            config.chunk_size = value.parse().unwrap_or(config.chunk_size);
        }
        if let Ok(value) = std::env::var("GF_USERAGENT") {
            if !value.trim().is_empty() {
                config.user_agent = value;
            }
        }
        config
    }
}

#[derive(Debug, Clone)]
pub struct DownloadRequest {
    pub url: String,
    pub password: Option<String>,
    pub selected_files: Option<HashSet<usize>>,
}

impl DownloadRequest {
    pub fn new(url: impl Into<String>, password: Option<String>) -> Self {
        Self {
            url: url.into(),
            password,
            selected_files: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct DownloadPlan {
    pub request: DownloadRequest,
    pub content_id: String,
    pub root_dir: PathBuf,
    pub files: Vec<DownloadFile>,
}

#[derive(Debug, Clone)]
pub struct DownloadFile {
    pub index: usize,
    pub path: PathBuf,
    pub filename: String,
    pub url: String,
}

impl DownloadFile {
    pub fn destination(&self) -> PathBuf {
        self.path.join(&self.filename)
    }
}

#[derive(Debug, Clone)]
pub enum DownloadEvent {
    FileDiscovered {
        index: usize,
        path: PathBuf,
    },
    Started {
        index: usize,
        path: PathBuf,
    },
    Progress {
        index: usize,
        path: PathBuf,
        downloaded: u64,
        total: u64,
    },
    Completed {
        index: usize,
        path: PathBuf,
        bytes: u64,
    },
    Skipped {
        index: usize,
        path: PathBuf,
    },
    Retry {
        index: Option<usize>,
        message: String,
        attempt: usize,
    },
    Failed {
        index: Option<usize>,
        message: String,
    },
    Cancelled,
}

#[derive(Clone)]
pub struct DownloadManager {
    config: DownloadConfig,
    client: Client,
    account_token: Arc<Mutex<Option<String>>>,
    cancelled: Arc<AtomicBool>,
    events: broadcast::Sender<DownloadEvent>,
}

impl DownloadManager {
    pub fn new(config: DownloadConfig) -> Result<Self> {
        let mut headers = HeaderMap::new();
        headers.insert(ACCEPT_ENCODING, HeaderValue::from_static("gzip"));
        headers.insert(CONNECTION, HeaderValue::from_static("keep-alive"));
        headers.insert(ACCEPT, HeaderValue::from_static("*/*"));
        headers.insert(ORIGIN, HeaderValue::from_static("https://gofile.io"));
        headers.insert(REFERER, HeaderValue::from_static("https://gofile.io/"));
        headers.insert(
            USER_AGENT,
            HeaderValue::from_str(&config.user_agent)
                .unwrap_or_else(|_| HeaderValue::from_static("Mozilla/5.0")),
        );

        let client = Client::builder()
            .default_headers(headers)
            .cookie_store(true)
            .timeout(config.timeout)
            .build()?;
        let (events, _) = broadcast::channel(512);
        Ok(Self {
            config,
            client,
            account_token: Arc::new(Mutex::new(None)),
            cancelled: Arc::new(AtomicBool::new(false)),
            events,
        })
    }

    pub fn subscribe(&self) -> broadcast::Receiver<DownloadEvent> {
        self.events.subscribe()
    }

    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
        let _ = self.events.send(DownloadEvent::Cancelled);
    }

    pub fn reset_cancelled(&self) {
        self.cancelled.store(false, Ordering::SeqCst);
    }

    pub async fn discover(&self, request: DownloadRequest) -> Result<DownloadPlan> {
        self.ensure_account_token().await?;
        let content_id = parse_content_id(&request.url)?;
        let password_hash = request.password.as_deref().map(hash_password);
        let root_dir = self.config.download_dir.join(&content_id);
        let mut files = Vec::new();
        let mut pathing_count = HashMap::new();
        let node = self
            .fetch_content(&content_id, password_hash.as_deref())
            .await?;

        self.collect_files(
            &root_dir,
            &content_id,
            &node,
            password_hash.as_deref(),
            &mut pathing_count,
            &mut files,
        )
        .await?;

        for file in &files {
            let _ = self.events.send(DownloadEvent::FileDiscovered {
                index: file.index,
                path: file.destination(),
            });
        }

        Ok(DownloadPlan {
            request,
            content_id,
            root_dir,
            files,
        })
    }

    pub async fn download(&self, plan: DownloadPlan) -> Result<()> {
        let semaphore = Arc::new(Semaphore::new(self.config.max_concurrent.max(1)));
        let mut tasks = FuturesUnordered::new();
        let selected = plan.request.selected_files.clone();

        for file in plan.files {
            if let Some(selected) = selected.as_ref() {
                if !selected.contains(&file.index) {
                    continue;
                }
            }

            let permit = semaphore
                .clone()
                .acquire_owned()
                .await
                .expect("semaphore closed");
            let manager = self.clone();
            tasks.push(tokio::spawn(async move {
                let _permit = permit;
                manager.download_one(file).await
            }));
        }

        while let Some(result) = tasks.next().await {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(DownloadError::Cancelled)) => return Err(DownloadError::Cancelled),
                Ok(Err(error)) => {
                    let _ = self.events.send(DownloadEvent::Failed {
                        index: None,
                        message: error.to_string(),
                    });
                }
                Err(error) => {
                    let _ = self.events.send(DownloadEvent::Failed {
                        index: None,
                        message: error.to_string(),
                    });
                }
            }
        }

        Ok(())
    }

    async fn ensure_account_token(&self) -> Result<String> {
        if let Some(token) = self.account_token.lock().await.clone() {
            return Ok(token);
        }

        if let Some(token) = self.config.token.clone() {
            self.apply_account_token(&token).await;
            return Ok(token);
        }

        if !self.config.anonymous_fallback {
            return Err(DownloadError::AccountCreationFailed);
        }

        let wt = generate_website_token(&self.config.user_agent, "");
        let mut last_error = None;

        for attempt in 1..=self.config.retries.max(1) {
            let response = self
                .client
                .post("https://api.gofile.io/accounts")
                .header("X-Website-Token", &wt)
                .header("X-BL", "en-US")
                .send()
                .await;

            match response {
                Ok(response) => {
                    let body: AccountResponse = response.json().await?;
                    if body.status == "ok" {
                        if let Some(token) = body.data.and_then(|data| data.token) {
                            self.apply_account_token(&token).await;
                            return Ok(token);
                        }
                    }
                    last_error = Some(DownloadError::AccountCreationFailed);
                }
                Err(error) => {
                    last_error = Some(DownloadError::Request(error));
                    let _ = self.events.send(DownloadEvent::Retry {
                        index: None,
                        message: "account creation timed out or failed".to_string(),
                        attempt,
                    });
                    sleep(retry_delay(attempt)).await;
                }
            }
        }

        Err(last_error.unwrap_or(DownloadError::AccountCreationFailed))
    }

    async fn apply_account_token(&self, token: &str) {
        *self.account_token.lock().await = Some(token.to_string());
    }

    #[async_recursion]
    async fn fetch_content(
        &self,
        content_id: &str,
        password_hash: Option<&str>,
    ) -> Result<ContentNode> {
        let account_token = self.ensure_account_token().await?;
        let wt = generate_website_token(&self.config.user_agent, &account_token);
        let mut url = format!(
            "https://api.gofile.io/contents/{content_id}?cache=true&sortField=createTime&sortDirection=1"
        );
        if let Some(password_hash) = password_hash {
            url.push_str("&password=");
            url.push_str(password_hash);
        }

        let mut last_error = None;
        for attempt in 1..=self.config.retries.max(1) {
            let response = self
                .client
                .get(&url)
                .bearer_auth(&account_token)
                .header(COOKIE, format!("accountToken={account_token}"))
                .header("X-Website-Token", &wt)
                .header("X-BL", "en-US")
                .send()
                .await;

            match response {
                Ok(response) => {
                    let body: ContentResponse = response.json().await?;
                    if body.status != "ok" {
                        return Err(DownloadError::Api {
                            url,
                            message: body
                                .message
                                .unwrap_or_else(|| "status was not ok".to_string()),
                        });
                    }
                    let data = body.data.ok_or_else(|| DownloadError::Api {
                        url: url.clone(),
                        message: "missing data".to_string(),
                    })?;
                    if data.password.is_some()
                        && data.password_status.as_deref() != Some("passwordOk")
                    {
                        return Err(DownloadError::PasswordRequired);
                    }
                    return Ok(data);
                }
                Err(error) => {
                    last_error = Some(DownloadError::Request(error));
                    let _ = self.events.send(DownloadEvent::Retry {
                        index: None,
                        message: format!("failed to fetch content {content_id}"),
                        attempt,
                    });
                    sleep(retry_delay(attempt)).await;
                }
            }
        }

        Err(last_error.unwrap_or_else(|| DownloadError::Api {
            url,
            message: "request failed".to_string(),
        }))
    }

    #[async_recursion]
    async fn collect_files(
        &self,
        parent_dir: &Path,
        root_content_id: &str,
        node: &ContentNode,
        password_hash: Option<&str>,
        pathing_count: &mut HashMap<PathBuf, usize>,
        files: &mut Vec<DownloadFile>,
    ) -> Result<()> {
        if node.content_type != "folder" {
            let filepath = resolve_naming_collision(pathing_count, parent_dir, &node.name, false);
            let url = node.link.clone().ok_or_else(|| DownloadError::Api {
                url: node.id.clone(),
                message: "file node missing download link".to_string(),
            })?;
            files.push(DownloadFile {
                index: files.len(),
                path: filepath.parent().unwrap_or(parent_dir).to_path_buf(),
                filename: filepath
                    .file_name()
                    .unwrap_or_default()
                    .to_string_lossy()
                    .to_string(),
                url,
            });
            return Ok(());
        }

        let mut absolute_path =
            resolve_naming_collision(pathing_count, parent_dir, &node.name, true);
        if parent_dir.file_name().and_then(|name| name.to_str()) == Some(root_content_id) {
            absolute_path = parent_dir.to_path_buf();
        }

        let mut children: Vec<_> = node
            .children
            .as_ref()
            .map(|children| children.values().cloned().collect())
            .unwrap_or_default();
        children.sort_by(|a, b| a.name.cmp(&b.name));

        for child in children {
            if child.content_type == "folder" {
                let child_node = self.fetch_content(&child.id, password_hash).await?;
                self.collect_files(
                    &absolute_path,
                    root_content_id,
                    &child_node,
                    password_hash,
                    pathing_count,
                    files,
                )
                .await?;
            } else {
                let filepath =
                    resolve_naming_collision(pathing_count, &absolute_path, &child.name, false);
                let url = child.link.ok_or_else(|| DownloadError::Api {
                    url: child.id.clone(),
                    message: "file node missing download link".to_string(),
                })?;
                files.push(DownloadFile {
                    index: files.len(),
                    path: filepath.parent().unwrap_or(&absolute_path).to_path_buf(),
                    filename: filepath
                        .file_name()
                        .unwrap_or_default()
                        .to_string_lossy()
                        .to_string(),
                    url,
                });
            }
        }
        Ok(())
    }

    async fn download_one(&self, file: DownloadFile) -> Result<()> {
        if self.cancelled.load(Ordering::SeqCst) {
            return Err(DownloadError::Cancelled);
        }

        fs::create_dir_all(&file.path).await?;
        let destination = file.destination();
        if let Ok(metadata) = fs::metadata(&destination).await {
            if metadata.len() > 0 {
                let _ = self.events.send(DownloadEvent::Skipped {
                    index: file.index,
                    path: destination,
                });
                return Ok(());
            }
        }

        let part_file = destination.with_file_name(format!(
            "{}.part",
            destination
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
        ));

        let mut last_error = None;
        for attempt in 1..=self.config.retries.max(1) {
            if self.cancelled.load(Ordering::SeqCst) {
                return Err(DownloadError::Cancelled);
            }

            match self.download_attempt(&file, &destination, &part_file).await {
                Ok(()) => return Ok(()),
                Err(DownloadError::Cancelled) => return Err(DownloadError::Cancelled),
                Err(error) => {
                    last_error = Some(error);
                    let _ = self.events.send(DownloadEvent::Retry {
                        index: Some(file.index),
                        message: format!("retrying {}", file.filename),
                        attempt,
                    });
                    sleep(retry_delay(attempt)).await;
                }
            }
        }

        let error = last_error.unwrap_or(DownloadError::MissingFileSize);
        let _ = self.events.send(DownloadEvent::Failed {
            index: Some(file.index),
            message: error.to_string(),
        });
        Err(error)
    }

    async fn download_attempt(
        &self,
        file: &DownloadFile,
        destination: &Path,
        part_file: &Path,
    ) -> Result<()> {
        let part_size = fs::metadata(part_file).await.map(|m| m.len()).unwrap_or(0);
        let mut request = self.client.get(&file.url);
        if let Some(account_token) = self.account_token.lock().await.clone() {
            request = request
                .bearer_auth(&account_token)
                .header(COOKIE, format!("accountToken={account_token}"));
        }
        if part_size > 0 {
            request = request.header(RANGE, format!("bytes={part_size}-"));
        }

        let response = request.send().await?;
        let status = response.status();
        if !is_valid_download_status(status, part_size) {
            return Err(DownloadError::BadStatus(status));
        }

        let total_size = extract_total_size(response.headers(), part_size)
            .ok_or(DownloadError::MissingFileSize)?;
        let _ = self.events.send(DownloadEvent::Started {
            index: file.index,
            path: destination.to_path_buf(),
        });

        let mut output = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(part_file)
            .await?;
        let mut downloaded = part_size;
        let mut stream = response.bytes_stream();
        let mut buffered = Vec::with_capacity(self.config.chunk_size);

        while let Some(chunk) = stream.next().await {
            if self.cancelled.load(Ordering::SeqCst) {
                return Err(DownloadError::Cancelled);
            }

            let chunk = chunk?;
            buffered.extend_from_slice(&chunk);
            while buffered.len() >= self.config.chunk_size {
                let tail = buffered.split_off(self.config.chunk_size);
                output.write_all(&buffered).await?;
                downloaded += buffered.len() as u64;
                buffered = tail;
                let _ = self.events.send(DownloadEvent::Progress {
                    index: file.index,
                    path: destination.to_path_buf(),
                    downloaded,
                    total: total_size,
                });
            }
        }
        if !buffered.is_empty() {
            output.write_all(&buffered).await?;
            downloaded += buffered.len() as u64;
            let _ = self.events.send(DownloadEvent::Progress {
                index: file.index,
                path: destination.to_path_buf(),
                downloaded,
                total: total_size,
            });
        }
        output.flush().await?;

        let actual_size = fs::metadata(part_file).await?.len();
        if actual_size == total_size {
            fs::rename(part_file, destination).await?;
            let _ = self.events.send(DownloadEvent::Completed {
                index: file.index,
                path: destination.to_path_buf(),
                bytes: actual_size,
            });
            Ok(())
        } else {
            Err(DownloadError::MissingFileSize)
        }
    }
}

#[derive(Debug, Deserialize)]
struct AccountResponse {
    status: String,
    data: Option<AccountData>,
}

#[derive(Debug, Deserialize)]
struct AccountData {
    token: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ContentResponse {
    status: String,
    data: Option<ContentNode>,
    message: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct ContentNode {
    id: String,
    name: String,
    #[serde(rename = "type")]
    content_type: String,
    link: Option<String>,
    children: Option<HashMap<String, ContentNode>>,
    password: Option<serde_json::Value>,
    #[serde(rename = "passwordStatus")]
    password_status: Option<String>,
}

pub fn parse_content_id(input: &str) -> Result<String> {
    let url = Url::parse(input).map_err(|_| DownloadError::InvalidUrl(input.to_string()))?;
    let mut segments = url
        .path_segments()
        .ok_or_else(|| DownloadError::InvalidUrl(input.to_string()))?;

    while let Some(segment) = segments.next() {
        if segment == "d" {
            if let Some(id) = segments.next() {
                if !id.trim().is_empty() {
                    return Ok(id.to_string());
                }
            }
        }
    }

    Err(DownloadError::InvalidUrl(input.to_string()))
}

pub fn parse_batch_lines(text: &str, global_password: Option<&str>) -> Vec<DownloadRequest> {
    text.lines()
        .filter_map(|line| {
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                return None;
            }

            let mut parts = trimmed.split_whitespace();
            let url = parts.next()?.to_string();
            let password = global_password
                .map(str::to_string)
                .or_else(|| parts.next().map(str::to_string));
            Some(DownloadRequest::new(url, password))
        })
        .collect()
}

pub fn hash_password(password: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(password.as_bytes());
    hex::encode(hasher.finalize())
}

pub fn generate_website_token(user_agent: &str, account_token: &str) -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    generate_website_token_at(user_agent, account_token, now)
}

pub fn generate_website_token_at(
    user_agent: &str,
    account_token: &str,
    unix_seconds: u64,
) -> String {
    let time_slot = unix_seconds / 14_400;
    let raw = format!("{user_agent}::en-US::{account_token}::{time_slot}::5d4f7g8sd45fsd");
    hash_password(&raw)
}

pub fn resolve_naming_collision(
    pathing_count: &mut HashMap<PathBuf, usize>,
    parent_dir: &Path,
    child_name: &str,
    is_dir: bool,
) -> PathBuf {
    let filepath = parent_dir.join(child_name);
    let count = pathing_count.entry(filepath.clone()).or_insert(0);
    let current = *count;
    *count += 1;

    if current == 0 {
        return filepath;
    }

    if is_dir {
        return parent_dir.join(format!("{child_name}({current})"));
    }

    let path = Path::new(child_name);
    let stem = path.file_stem().unwrap_or_default().to_string_lossy();
    let extension = path.extension().map(|ext| ext.to_string_lossy());
    let filename = match extension {
        Some(extension) if !extension.is_empty() => format!("{stem}({current}).{extension}"),
        _ => format!("{stem}({current})"),
    };
    parent_dir.join(filename)
}

pub fn is_valid_download_status(status: StatusCode, part_size: u64) -> bool {
    if matches!(
        status,
        StatusCode::FORBIDDEN
            | StatusCode::NOT_FOUND
            | StatusCode::METHOD_NOT_ALLOWED
            | StatusCode::INTERNAL_SERVER_ERROR
    ) {
        return false;
    }

    if part_size == 0 {
        status == StatusCode::OK || status == StatusCode::PARTIAL_CONTENT
    } else {
        status == StatusCode::PARTIAL_CONTENT
    }
}

pub fn extract_total_size(headers: &HeaderMap, part_size: u64) -> Option<u64> {
    if part_size == 0 {
        return headers
            .get("Content-Length")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse().ok());
    }

    headers
        .get("Content-Range")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.rsplit('/').next())
        .and_then(|value| value.parse().ok())
}

fn retry_delay(attempt: usize) -> Duration {
    Duration::from_millis((attempt as u64).min(5) * 250)
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::header::{HeaderMap, HeaderValue};

    #[test]
    fn parses_content_id() {
        assert_eq!(
            parse_content_id("https://gofile.io/d/abc123").unwrap(),
            "abc123"
        );
        assert!(parse_content_id("https://gofile.io/not/abc123").is_err());
    }

    #[test]
    fn parses_batch_lines_with_password_precedence() {
        let text = "https://gofile.io/d/a one\n\n# comment\nhttps://gofile.io/d/b\n";
        let requests = parse_batch_lines(text, None);
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].password.as_deref(), Some("one"));
        assert_eq!(requests[1].password, None);

        let requests = parse_batch_lines(text, Some("global"));
        assert_eq!(requests[0].password.as_deref(), Some("global"));
        assert_eq!(requests[1].password.as_deref(), Some("global"));
    }

    #[test]
    fn hashes_password() {
        assert_eq!(
            hash_password("password"),
            "5e884898da28047151d0e56f8dc6292773603d0d6aabbdd62a11ef721d1542d8"
        );
    }

    #[test]
    fn website_token_matches_script_formula() {
        assert_eq!(
            generate_website_token_at("Mozilla/5.0", "token", 14_400),
            hash_password("Mozilla/5.0::en-US::token::1::5d4f7g8sd45fsd")
        );
    }

    #[test]
    fn resolves_name_collisions() {
        let mut seen = HashMap::new();
        let root = Path::new("root");
        assert_eq!(
            resolve_naming_collision(&mut seen, root, "a.txt", false),
            root.join("a.txt")
        );
        assert_eq!(
            resolve_naming_collision(&mut seen, root, "a.txt", false),
            root.join("a(1).txt")
        );
        assert_eq!(
            resolve_naming_collision(&mut seen, root, "dir", true),
            root.join("dir")
        );
        assert_eq!(
            resolve_naming_collision(&mut seen, root, "dir", true),
            root.join("dir(1)")
        );
    }

    #[test]
    fn extracts_total_size_from_headers() {
        let mut headers = HeaderMap::new();
        headers.insert("Content-Length", HeaderValue::from_static("42"));
        assert_eq!(extract_total_size(&headers, 0), Some(42));

        let mut headers = HeaderMap::new();
        headers.insert("Content-Range", HeaderValue::from_static("bytes 10-41/42"));
        assert_eq!(extract_total_size(&headers, 10), Some(42));
    }
}
