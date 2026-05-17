use anyhow::Context;
use clap::Parser;
use gofilepan_core::{
    parse_batch_lines, DownloadConfig, DownloadEvent, DownloadManager, DownloadPlan,
    DownloadRequest,
};
use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};
use tokio::task::JoinHandle;

#[derive(Debug, Parser)]
#[command(name = "gofilepan", version, about = "Download files from Gofile")]
struct Args {
    /// A Gofile URL or a text file containing one URL per line.
    url_or_file: String,
    /// Optional password for the URL, or global password for every URL in a text file.
    password: Option<String>,
    /// Directory where files will be downloaded.
    #[arg(long)]
    output: Option<PathBuf>,
    /// Gofile account token. Defaults to GF_TOKEN when present.
    #[arg(long)]
    token: Option<String>,
    /// Select files interactively before downloading a single URL.
    #[arg(long)]
    interactive: bool,
    /// Maximum number of concurrent file downloads.
    #[arg(long)]
    max_concurrent: Option<usize>,
    /// Number of retries for API and file requests.
    #[arg(long)]
    retries: Option<usize>,
    /// Request timeout in seconds.
    #[arg(long)]
    timeout: Option<f64>,
    /// Number of bytes read per HTTP chunk.
    #[arg(long)]
    chunk_size: Option<usize>,
    /// Browser user agent sent to Gofile.
    #[arg(long)]
    user_agent: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let config = build_config(&args);
    let manager = DownloadManager::new(config)?;
    let event_task = print_events(manager.subscribe());
    let cancel_manager = manager.clone();

    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            eprintln!("\nStopping, please wait...");
            cancel_manager.cancel();
        }
    });

    println!("Starting, please wait...");
    let requests = load_requests(&args)
        .await
        .with_context(|| format!("failed to read {}", args.url_or_file))?;
    let interactive =
        args.interactive || std::env::var("GF_INTERACTIVE").ok().as_deref() == Some("1");

    for request in requests {
        let mut plan = manager.discover(request).await?;
        if interactive && !is_existing_file(&args.url_or_file).await {
            apply_interactive_selection(&mut plan)?;
        }
        manager.download(plan).await?;
    }

    event_task.abort();
    Ok(())
}

fn build_config(args: &Args) -> DownloadConfig {
    let mut config = DownloadConfig::from_env();
    if let Some(output) = args.output.clone() {
        config.download_dir = output;
    }
    if let Some(token) = args.token.clone() {
        config.token = Some(token);
    }
    if let Some(max_concurrent) = args.max_concurrent {
        config.max_concurrent = max_concurrent;
    }
    if let Some(retries) = args.retries {
        config.retries = retries;
    }
    if let Some(timeout) = args.timeout {
        config.timeout = Duration::from_secs_f64(timeout);
    }
    if let Some(chunk_size) = args.chunk_size {
        config.chunk_size = chunk_size;
    }
    if let Some(user_agent) = args.user_agent.clone() {
        config.user_agent = user_agent;
    }
    config
}

async fn load_requests(args: &Args) -> anyhow::Result<Vec<DownloadRequest>> {
    if is_existing_file(&args.url_or_file).await {
        let text = tokio::fs::read_to_string(&args.url_or_file).await?;
        Ok(parse_batch_lines(&text, args.password.as_deref()))
    } else {
        Ok(vec![DownloadRequest::new(
            args.url_or_file.clone(),
            args.password.clone(),
        )])
    }
}

async fn is_existing_file(path: impl AsRef<Path>) -> bool {
    tokio::fs::metadata(path)
        .await
        .map(|metadata| metadata.is_file())
        .unwrap_or(false)
}

fn apply_interactive_selection(plan: &mut DownloadPlan) -> anyhow::Result<()> {
    if plan.files.is_empty() {
        println!("No files found.");
        return Ok(());
    }

    for file in &plan.files {
        println!("[{}] -> {}", file.index, file.destination().display());
    }

    println!("Files to download (Ex: 1 3 7) | leave empty to download them all");
    print!(":: ");
    use std::io::Write;
    std::io::stdout().flush()?;

    let mut input = String::new();
    std::io::stdin().read_line(&mut input)?;
    let had_selection_input = input.split_whitespace().next().is_some();
    let available: HashSet<_> = plan.files.iter().map(|file| file.index).collect();
    let selected: HashSet<_> = input
        .split_whitespace()
        .filter_map(|value| value.parse::<usize>().ok())
        .filter(|index| available.contains(index))
        .collect();

    if had_selection_input {
        if selected.is_empty() {
            println!("Nothing done.");
        }
        plan.request.selected_files = Some(selected);
    }

    Ok(())
}

fn print_events(mut events: tokio::sync::broadcast::Receiver<DownloadEvent>) -> JoinHandle<()> {
    tokio::spawn(async move {
        let started = Instant::now();
        while let Ok(event) = events.recv().await {
            match event {
                DownloadEvent::FileDiscovered { index, path } => {
                    println!("[{index}] discovered {}", path.display());
                }
                DownloadEvent::Started { index, path } => {
                    println!("[{index}] downloading {}", path.display());
                }
                DownloadEvent::Progress {
                    index,
                    path,
                    downloaded,
                    total,
                } => {
                    let progress = if total == 0 {
                        0.0
                    } else {
                        downloaded as f64 / total as f64 * 100.0
                    };
                    let rate = downloaded as f64 / started.elapsed().as_secs_f64().max(0.1);
                    print!(
                        "\r[{index}] {}: {downloaded} of {total} ({progress:.1}%) {}",
                        path.file_name().unwrap_or_default().to_string_lossy(),
                        format_rate(rate)
                    );
                    use std::io::Write;
                    let mut stdout = std::io::stdout();
                    let _ = stdout.flush();
                }
                DownloadEvent::Completed { index, path, bytes } => {
                    println!("\r[{index}] done {} ({bytes} bytes)", path.display());
                }
                DownloadEvent::Skipped { index, path } => {
                    println!("[{index}] skipped existing {}", path.display());
                }
                DownloadEvent::Retry {
                    index,
                    message,
                    attempt,
                } => {
                    println!(
                        "\nretry {attempt}{}: {message}",
                        index
                            .map(|value| format!(" for [{value}]"))
                            .unwrap_or_default()
                    );
                }
                DownloadEvent::Failed { index, message } => {
                    eprintln!(
                        "failed{}: {message}",
                        index.map(|value| format!(" [{value}]")).unwrap_or_default()
                    );
                }
                DownloadEvent::Cancelled => {
                    eprintln!("cancelled");
                }
            }
        }
    })
}

fn format_rate(bytes_per_second: f64) -> String {
    if bytes_per_second < 1024.0 {
        format!("{bytes_per_second:.1} B/s")
    } else if bytes_per_second < 1024.0 * 1024.0 {
        format!("{:.1} KB/s", bytes_per_second / 1024.0)
    } else if bytes_per_second < 1024.0 * 1024.0 * 1024.0 {
        format!("{:.1} MB/s", bytes_per_second / 1024.0 / 1024.0)
    } else {
        format!("{:.1} GB/s", bytes_per_second / 1024.0 / 1024.0 / 1024.0)
    }
}
