# Changelog

All notable changes to Little Brother v3 will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- **Quiescence Snapshots**: Lightweight timer-based alternative to live idle detection with configurable interval
- **Monitor Configuration**: New `monitors.context_snapshot` config section with granular control over live idle detection and quiescence
- **CLI Improvements**: Enhanced `lb3 monitors status` command now non-invasive (shows configuration only, no monitor instantiation)
- **Probe Exit Codes**: `lb3 probe context` now returns proper exit codes (0=success, 1=failure) with better isolation and cleanup

### Changed
- **Context Snapshot Configuration**: Moved from `heartbeat.poll_intervals.context_idle_gap` to `monitors.context_snapshot.idle_gap` with backwards compatibility
- **Monitors Status Command**: Now purely configuration-based inspection without starting monitors or supervisor

### Deprecated

### Removed
- **Context Snapshot Runtime**: Completely removed from default supervisor and `lb3 run` for improved stability
  - `monitors.context_snapshot.enabled: false` is now enforced at supervisor level
  - Default runs produce zero context_snapshot events
  - `lb3 probe context` and `lb3 spool generate context_snapshot` still work for testing
  - Optional quiescence timer available with `monitors.context_snapshot.quiescence.enabled: true`
- **Quiescence Default**: Quiescence snapshots disabled by default (`enabled: false`, `interval: "300s"`)
### Fixed
### Security

## [3.0.0] - 2025-09-09

### Added
- **Core Monitors**: Active window, context snapshot, keyboard, mouse, file, and browser monitoring with CDP plugin support
- **NDJSON.gz Spooler**: High-performance atomic append-only spooler with automatic rollover and compression
- **Import System**: Idempotent importer with duplicate detection, batch processing, and crash recovery
- **SQLite WAL Database**: Full schema with apps, windows, files, URLs, and events tables plus optimized indexes
- **CLI Commands**: `run`, `status`, `spool flush`, `db check`, `config show/set`, `version` commands
- **Logging System**: Per-run timestamped log files with configurable levels and rotation
- **Recovery System**: Gzip salvage capabilities for truncated .part files with comprehensive recovery reporting
- **Configuration**: YAML-based config with CLI override support and XDG directory compliance
- **Event Bus**: Centralized event distribution system for monitor coordination

### Changed
- **File Monitor**: Now uses public Database API instead of private attributes for better encapsulation
- **Monitor Discovery**: Importer now filters to known monitors only (`active_window`, `context_snapshot`, `keyboard`, `mouse`, `browser`, `file`, `heartbeat`)
- **Status Accuracy**: "Pending imports" count now excludes `.error` and `.part` files and unknown monitor directories

### Fixed
- **Logging Levels**: File-level import errors now log as WARN instead of ERROR (expected for truncated recovery files)
- **Recovery Logging**: Truncated files with no recoverable lines now log single-line WARN messages without stack traces
- **Status Command**: Fixed "Pending imports: 0 files" display when no importable files remain

### Security
- **Privacy-First Hashing**: Purpose-scoped hashing system prevents cross-correlation of sensitive data
- **Salt Persistence**: Cryptographically secure salt generation and storage for consistent hashing
- **Strict Guardrails**: No plaintext keylogging - only metadata like key press counts and timing
- **Data Minimization**: File paths, window titles, and URLs stored only as hashes with metadata

---

## Links

- [3.0.0]: https://github.com/your-org/little-brother-v3/releases/tag/v3.0.0
