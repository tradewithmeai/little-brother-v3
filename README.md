# Little Brother v3

![Release v3.0.0](https://img.shields.io/badge/release-v3.0.0-blue)

**Privacy-preserving, Windows-first monitoring daemon and CLI for developers and system administrators.**

Little Brother v3 is a local-only system monitoring solution that captures application usage, window focus events, file access patterns, and optional browser activity while prioritizing privacy through comprehensive data hashing and safe storage practices.

## What's New in v3.0.0

🎉 **First stable release!** See the [CHANGELOG.md](CHANGELOG.md) for complete release notes including all monitors, spooler/import system, SQLite WAL database, CLI commands, recovery system, and privacy-first security features.

## Objectives & Non-Goals

### ✅ What Little Brother v3 Does
- **Windows-first**: Optimized for Windows 10/11 with native Win32 API integration
- **Local-only**: All data stays on your machine, no network transmission
- **Privacy-safe**: No plaintext titles, paths, or URLs stored - only salted hashes
- **Lightweight monitoring**: Captures usage patterns without being a keylogger
- **Optional browser CDP plugin**: Advanced browser monitoring when explicitly enabled
- **Fallback minimal signals**: Graceful degradation when permissions are limited

### ❌ What Little Brother v3 Does NOT Do
- **No global text keylogging**: We enforce guardrails against capturing keystrokes as text
- **No cloud sync**: Data never leaves your machine
- **No real-time surveillance**: Designed for post-analysis and personal insights
- **No credential capture**: Actively avoids collecting sensitive information

## Safety Guardrails

Little Brother v3 implements multiple layers of privacy protection:

- **Hashing + Salt**: All sensitive strings (window titles, file paths, URLs) are hashed with a random salt before storage
- **No Global Text Keylogging**: Keyboard monitoring only captures aggregate statistics (timing, burst detection) - never actual keystrokes
- **Local Storage Only**: Data is stored locally in `./lb_data/` using NDJSON.gz compressed files and SQLite with WAL mode
- **Configurable Limits**: Built-in rate limiting and resource constraints
- **Safe Defaults**: Conservative monitoring settings that can be explicitly expanded if needed

## Quickstart (Windows)

### Prerequisites
- **Python 3.12** or later
- **pip** package installer
- **Optional**: Chrome/Edge with remote debugging port for advanced browser monitoring

### Installation
```bash
# Clone or download the repository
git clone <repo-url>
cd little-brother-v3

# Install in development mode
pip install -e .
```

### Verifying Your Install
```bash
# Check version
lb3 version

# Test basic functionality (5-second dry run)
lb3 run --dry-run --duration 5

# Check system status
lb3 status
```

### First Run
```bash
# Check configuration
lb3 config show

# Start monitoring (press Ctrl+C to stop)
lb3 run

# Check status in another terminal
lb3 status
```

### Optional: Enable Browser CDP
For advanced browser monitoring (tab titles, URLs), enable Chrome DevTools Protocol:

1. **Set debugging port**: Add `--remote-debugging-port=9222` to your Chrome/Edge startup
2. **Update config**: `lb3 config set browser.cdp_enabled true`
3. **Restart browser** with the debugging port enabled
4. **Restart monitoring**: `lb3 run`

### Inspect Your Data
```bash
# Flush pending spool files
lb3 spool flush

# Check database status
lb3 db check

# View current monitoring status
lb3 status

# Show version information
lb3 version
```

## Data Model Overview

Little Brother v3 uses a structured data model with these core tables:

### Core Tables
- **sessions**: Monitoring sessions with start/end times
- **apps**: Application executables (hashed paths)
- **windows**: Application windows (hashed titles)
- **files**: File system objects (hashed paths)
- **urls**: Web URLs (hashed, domain extracted)

### Event Types
- **Active Window**: Focus changes between applications/windows
- **Keyboard**: Aggregate timing statistics (no keystroke content)
- **Mouse**: Movement, clicks, scroll patterns
- **File Watch**: File system access events
- **Browser**: Tab changes, navigation (when CDP enabled)
- **Context Snapshot**: Periodic system state captures

### Event Fields
All events include:
- `session_id`: Links to monitoring session
- `ts_utc`: UTC timestamp (milliseconds)
- `monitor`: Source monitor name
- `action`: Event type (focus, blur, create, etc.)
- `attrs`: Monitor-specific attributes (all sensitive data hashed)

### Hashing Purposes
- **Window Titles**: `title_hash` for privacy while enabling aggregation
- **File Paths**: `path_hash` for usage analysis without exposing file structure
- **URLs**: `url_hash` and `domain_hash` for web activity patterns
- **App Paths**: `exe_path_hash` for application identification

## Privacy Note

**What IS Collected:**
- Application focus timing and patterns
- Aggregate keyboard/mouse activity statistics
- File access events (hashed paths only)
- Window management events (hashed titles)
- Browser navigation (hashed URLs, when CDP enabled)

**What IS NOT Collected:**
- Actual keystrokes or typed text
- Plaintext file paths, window titles, or URLs
- Screenshot or screen recording data
- Network traffic content
- System passwords or credentials

All data remains **local** in the `./lb_data/` directory. The monitoring system includes an **idempotent importer** that can safely re-process data without duplicates.

## Troubleshooting

### Common Issues

**Permissions (pynput/watchdog)**
- Run as regular user (administrator not required)
- Some antivirus software may flag input monitoring - add exclusions for lb3
- On Windows, UAC prompts are normal for input monitoring setup

**Antivirus Exclusions**
Add these to your antivirus exclusions:
- Python executable path: `C:\Users\<username>\AppData\Local\Programs\Python\`
- Project directory: `<path>\little-brother-v3\`
- Data directory: `<path>\little-brother-v3\lb_data\`

**Enabling CDP (Browser Monitoring)**
- Ensure browser is started with `--remote-debugging-port=9222`
- Check firewall isn't blocking localhost:9222
- Verify browser process has debugging port open: `netstat -an | findstr 9222`

**Data Issues**
- Logs location: `./lb_data/logs/lb3.log`
- Recovery sweep behavior: Temporary files are automatically recovered on startup
- Database corruption: WAL mode provides resilience, check `lb3 db check`

### CI Notes
- **Windows job required**: Core functionality requires Windows APIs
- **Ubuntu advisory**: Basic functionality works, Windows-specific features skipped
- **Linux compatibility**: File watching and basic monitoring supported

## Roadmap

**Short-term improvements:**
- File monitor polish and performance optimization
- Browser plugin feature parity with other monitors
- Enhanced configuration validation and user experience
- Better error handling and recovery mechanisms

**Future considerations:**
- Optional Rust sidecar for performance-critical monitoring paths
- Additional browser engines support (Firefox, Safari)
- Cross-platform compatibility improvements
- Advanced analytics and reporting features

## Development

### Running Tests
```bash
# Run all tests
pytest

# Run specific test categories
pytest tests/unit/          # Unit tests
pytest tests/integration/   # Integration tests (slower)

# Run with coverage
pytest --cov=lb3
```

### Code Quality
```bash
# Linting
ruff check .

# Type checking
mypy .

# Formatting
ruff format .
```

### Contributing
1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests and quality checks pass
5. Submit a pull request

## License

MIT License - see [LICENSE](LICENSE) for details.

---

**Little Brother v3** - Monitor responsibly, protect privacy, gain insights.
