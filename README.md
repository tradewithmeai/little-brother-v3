# Little Brother v3

A Windows-first system monitoring daemon and CLI tool for comprehensive activity tracking.

## Overview

Little Brother v3 provides real-time monitoring of system activity including:
- Active window tracking
- Keyboard and mouse activity
- File system changes
- Browser activity
- Context snapshots

## Architecture

- **Core**: Main application logic, configuration, and utilities
- **Monitors**: Pluggable monitoring modules for different system aspects
- **Plugins**: Extensible plugin system for additional functionality
- **Storage**: Local SQLite database with spooling support

## Data Storage

Data is stored locally in the `./lb_data/` directory:
- `./lb_data/local.db` - SQLite database for persistent storage
- `./lb_data/spool/<monitor>/` - Temporary spool directories for each monitor

## Installation

```bash
pip install -e .
```

## Usage

### CLI Mode
```bash
# Via module
python -m lb3

# Via installed command
lb3
```

### Daemon Mode
```bash
lb3 daemon start
lb3 daemon stop
lb3 daemon status
```

## Development

Install development dependencies:
```bash
pip install -e ".[dev]"
```

Run tests:
```bash
pytest
```

## Requirements

- Python 3.9+
- Windows (primary target)
- Administrative privileges (for some monitoring features)

## License

MIT License - see LICENSE file for details.
