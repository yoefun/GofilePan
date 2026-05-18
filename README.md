# GofilePan

[![CI](https://github.com/yoefun/GofilePan/actions/workflows/ci.yml/badge.svg)](https://github.com/yoefun/GofilePan/actions/workflows/ci.yml)
[![Release](https://github.com/yoefun/GofilePan/actions/workflows/release.yml/badge.svg)](https://github.com/yoefun/GofilePan/actions/workflows/release.yml)

GofilePan is a Rust downloader for [Gofile](https://gofile.io) with both a command-line interface and a desktop GUI. It rewrites the download behavior of [`ltsdw/gofile-downloader`](https://github.com/ltsdw/gofile-downloader) around a shared Rust core so CLI and GUI users get the same URL parsing, authentication, recursive folder discovery, retries, resume support, and progress events.

> Gofile's public API is currently beta. GofilePan keeps the API integration isolated in `gofilepan-core` so token and request-header behavior can be updated in one place if Gofile changes its API.

## Features

- CLI and GUI built on one shared download engine.
- Single URL downloads: `https://gofile.io/d/<content-id>`.
- Batch downloads from a text file, with optional per-link passwords.
- Password-protected content support.
- Recursive folder discovery and local directory creation.
- Concurrent downloads with configurable worker count.
- Retry and timeout handling.
- Resume support through `.part` files and HTTP `Range` requests.
- Existing non-empty files are skipped.
- File selection in CLI interactive mode and GUI discovery mode.
- Compatible `GF_*` environment variables from `gofile-downloader.py`.

## Project Layout

```text
crates/
  gofilepan-core/  Shared Gofile API, planning, download, retry, resume, and event logic
  gofilepan-cli/   Command-line application
  gofilepan-gui/   Slint desktop application
```

## Requirements

- Rust stable toolchain
- Git
- A network connection to Gofile

Install Rust with [rustup](https://rustup.rs/) if `cargo` is not already available:

```powershell
rustup default stable
```

Linux GUI builds may require common desktop packages for Slint's native backend, such as X11, Wayland, OpenGL, and fontconfig development libraries.

## CLI Usage

Run from source:

```powershell
cargo run -p gofilepan-cli -- https://gofile.io/d/contentid
cargo run -p gofilepan-cli -- https://gofile.io/d/contentid password
cargo run -p gofilepan-cli -- urls.txt
```

Build a release binary:

```powershell
cargo build --release -p gofilepan-cli
```

The binary is written to:

```text
target/release/gofilepan.exe
```

On Linux and macOS the binary name is `gofilepan`.

### Batch File Format

One URL per line:

```text
https://gofile.io/d/contentid1
https://gofile.io/d/contentid2
https://gofile.io/d/contentid3
```

Per-link passwords are supported by adding a password after the URL:

```text
https://gofile.io/d/contentid1 password1
https://gofile.io/d/contentid2
https://gofile.io/d/contentid3 password3
```

If a positional password is passed together with a batch file, that password is used for every URL:

```powershell
cargo run -p gofilepan-cli -- urls.txt shared-password
```

### CLI Options

```text
gofilepan <url-or-file> [password]

Options:
  --output <DIR>             Download directory
  --token <TOKEN>            Gofile account token
  --interactive              Select files before downloading a single URL
  --max-concurrent <N>       Maximum concurrent file downloads
  --retries <N>              Number of retries for API and file requests
  --timeout <SECONDS>        Request timeout in seconds
  --chunk-size <BYTES>       Number of bytes written per progress chunk
  --user-agent <VALUE>       Browser user agent sent to Gofile
```

### Environment Variables

GofilePan accepts the same environment variable names as the Python downloader:

| Variable | Description | Default |
| --- | --- | --- |
| `GF_DOWNLOAD_DIR` | Download directory | Current working directory |
| `GF_INTERACTIVE` | Set to `1` to enable CLI file selection | Disabled |
| `GF_TOKEN` | Existing Gofile account token | Temporary account fallback |
| `GF_MAX_CONCURRENT_DOWNLOADS` | Concurrent file downloads | `5` |
| `GF_MAX_RETRIES` | Retry count | `5` |
| `GF_TIMEOUT` | Request timeout in seconds | `15.0` |
| `GF_CHUNK_SIZE` | Progress/write chunk size in bytes | `2097152` |
| `GF_USERAGENT` | User-Agent header | `Mozilla/5.0` |

Explicit CLI flags take precedence over environment variables.

## GUI Usage

Run from source:

```powershell
cargo run -p gofilepan-gui
```

Build a release binary:

```powershell
cargo build --release -p gofilepan-gui
```

The GUI lets you:

- Paste a single URL or multiple batch lines.
- Set password, token, output directory, concurrency, retries, timeout, chunk size, and user agent.
- Discover files before downloading.
- Select individual files for a single discovered link.
- Start and cancel downloads.
- View per-file progress and recent logs.
- Toggle advanced settings from a compact side panel so the main flow stays clean.

The Slint UI keeps the core workflow visible and folds low-frequency settings into the advanced panel to reduce visual noise.

Settings are saved to the platform config directory, for example:

```text
%APPDATA%\GofilePan\config.toml
```

## Authentication

GofilePan uses the same compatibility strategy as `gofile-downloader.py`:

1. Use `--token`, GUI token, or `GF_TOKEN` when provided.
2. Otherwise, attempt to create a temporary Gofile account.
3. Send the Gofile website token headers used by the web client integration.

If Gofile changes the beta API or account flow, update the isolated logic in `gofilepan-core`.

## Development

Format, lint, and test:

```powershell
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

Build everything:

```powershell
cargo build --workspace
```

Build release binaries:

```powershell
cargo build --release --workspace
```

## CI/CD

GitHub Actions configuration lives in `.github/workflows`:

- `ci.yml` runs on pushes and pull requests. It checks formatting, runs Clippy, runs tests, and builds the workspace on Windows, Linux, and macOS.
- `release.yml` runs when a tag matching `v*` is pushed. It builds release binaries for Windows, Linux, and macOS, packages them, and uploads them to a GitHub Release.

Create a release:

```powershell
git tag v0.1.0
git push origin v0.1.0
```

## Contributing

Issues and pull requests are welcome. Please keep changes focused and run the development checks before opening a PR.

For download behavior changes, prefer adding tests in `gofilepan-core` so CLI and GUI behavior stays consistent.

## License

This project is distributed under the GPL-3.0-or-later license, matching the upstream downloader lineage. See [LICENSE](LICENSE).
