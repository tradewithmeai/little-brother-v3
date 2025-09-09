"""Crash recovery for Little Brother v3 spool files."""

import gzip
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

from .logging_setup import get_logger

logger = get_logger("recovery")


@dataclass
class SalvageStats:
    """Statistics from salvaging a corrupted file."""
    original_path: Path
    recovered_path: Optional[Path]
    error_path: Optional[Path]
    lines_total: int
    lines_salvaged: int
    lines_corrupted: int
    success: bool
    error_message: Optional[str] = None


@dataclass 
class RecoveryReport:
    """Report of recovery operations across monitors."""
    monitors_processed: List[str]
    temp_files_found: int
    temp_files_recovered: int
    temp_files_failed: int
    total_lines_salvaged: int
    salvage_stats: List[SalvageStats]
    
    def summary_line(self) -> str:
        """Get a single-line summary for logging."""
        if self.temp_files_found == 0:
            return "Recovery sweep: no temp files found"
        
        return (f"Recovery sweep: {self.temp_files_recovered}/{self.temp_files_found} temp files recovered, "
                f"{self.total_lines_salvaged} lines salvaged")


def salvage_plain_ndjson(temp_path: Path) -> SalvageStats:
    """Salvage a plain NDJSON .part file by truncating to last complete line.
    
    Args:
        temp_path: Path to .part file with plain NDJSON content
        
    Returns:
        SalvageStats with results of salvage operation
    """
    logger.debug(f"Salvaging plain NDJSON file: {temp_path}")
    
    try:
        # Read the entire file
        content = temp_path.read_text(encoding='utf-8')
        lines = content.splitlines()
        
        # Find last complete JSON line
        valid_lines = []
        corrupted_count = 0
        
        for i, line in enumerate(lines):
            if not line.strip():
                continue  # Skip empty lines
                
            try:
                # Try to parse as JSON to validate
                json.loads(line)
                valid_lines.append(line)
            except json.JSONDecodeError:
                logger.debug(f"Corrupted line {i+1} in {temp_path}: {line[:100]}...")
                corrupted_count += 1
                # Stop at first corruption - assume everything after is bad
                break
        
        if not valid_lines:
            return SalvageStats(
                original_path=temp_path,
                recovered_path=None,
                error_path=None,
                lines_total=len(lines),
                lines_salvaged=0,
                lines_corrupted=len(lines),
                success=False,
                error_message="No valid JSON lines found"
            )
        
        # Generate recovered filename  
        stem = temp_path.stem.replace('.part', '')
        if corrupted_count > 0:
            recovered_path = temp_path.parent / f"{stem}_recovered.ndjson.gz"
        else:
            recovered_path = temp_path.parent / f"{stem}.ndjson.gz"
        
        # Write valid lines to compressed file
        with gzip.open(recovered_path, 'wt', encoding='utf-8') as f:
            for line in valid_lines:
                f.write(line + '\n')
            # Flush and fsync within the same file handle
            f.flush()
            try:
                os.fsync(f.fileno())
            except (OSError, AttributeError) as e:
                # fsync may not work on all file types/systems
                logger.debug(f"fsync failed for {recovered_path}: {e}")
        
        # Fsync directory (best effort)
        try:
            dir_fd = os.open(str(temp_path.parent), os.O_RDONLY)
            os.fsync(dir_fd)
            os.close(dir_fd)
        except (OSError, AttributeError):
            pass  # Directory fsync not supported on all systems
        
        # Create error sidecar if there was corruption
        error_path = None
        if corrupted_count > 0:
            error_path = temp_path.parent / f"{temp_path.name}.error"
            error_msg = f"Salvaged {len(valid_lines)} valid lines, {corrupted_count} corrupted lines discarded"
            error_path.write_text(error_msg, encoding='utf-8')
        
        # Remove temp file
        temp_path.unlink()
        
        logger.info(f"Successfully salvaged {temp_path} -> {recovered_path} ({len(valid_lines)} lines)")
        
        return SalvageStats(
            original_path=temp_path,
            recovered_path=recovered_path,
            error_path=error_path,
            lines_total=len(lines),
            lines_salvaged=len(valid_lines),
            lines_corrupted=corrupted_count,
            success=True
        )
        
    except Exception as e:
        logger.error(f"Failed to salvage {temp_path}: {e}")
        return SalvageStats(
            original_path=temp_path,
            recovered_path=None,
            error_path=None,
            lines_total=0,
            lines_salvaged=0,
            lines_corrupted=0,
            success=False,
            error_message=str(e)
        )


def salvage_gzipped_ndjson(temp_path: Path) -> SalvageStats:
    """Salvage a gzipped NDJSON .part file with tolerant decompression.
    
    Args:
        temp_path: Path to .ndjson.gz.part file
        
    Returns:
        SalvageStats with results of salvage operation
    """
    import zlib
    
    logger.debug(f"Salvaging gzipped NDJSON file: {temp_path}")
    
    try:
        # Read the raw bytes
        raw_content = temp_path.read_bytes()
        bytes_read = len(raw_content)
        
        # Tolerant decompression using zlib
        decompressor = zlib.decompressobj(wbits=16+zlib.MAX_WBITS)  # 16+ for gzip format
        text_buffer = ""
        chunk_size = 64 * 1024  # 64 KiB chunks
        error_reason = None
        
        # Process in chunks
        for i in range(0, len(raw_content), chunk_size):
            chunk = raw_content[i:i+chunk_size]
            try:
                decompressed = decompressor.decompress(chunk)
                text_buffer += decompressed.decode('utf-8', errors='replace')
            except (zlib.error, UnicodeDecodeError) as e:
                error_reason = f"truncated gzip; {str(e)}"
                logger.debug(f"Decompression stopped at byte {i}: {e}")
                break
        
        # Try to get any remaining data
        try:
            if not error_reason:
                remaining = decompressor.flush()
                text_buffer += remaining.decode('utf-8', errors='replace')
        except (zlib.error, UnicodeDecodeError) as e:
            error_reason = error_reason or "truncated gzip; CRC missing"
            logger.debug(f"Final flush failed: {e}")
        
        # Keep only complete lines (discard final partial line)
        lines = text_buffer.splitlines()
        if text_buffer and not text_buffer.endswith('\n'):
            # Remove incomplete last line
            if lines:
                lines = lines[:-1]
        
        # Validate and collect good JSON lines
        valid_lines = []
        corrupted_count = 0
        
        for line_num, line in enumerate(lines, 1):
            line = line.strip()
            if not line:
                continue
                
            try:
                json.loads(line)
                valid_lines.append(line)
            except json.JSONDecodeError:
                logger.debug(f"Invalid JSON at line {line_num}: {line[:100]}...")
                corrupted_count += 1
        
        if not valid_lines:
            # Create error sidecar
            error_path = temp_path.parent / f"{temp_path.name}.error"
            error_msg = f"No valid lines salvaged from {bytes_read} bytes; {error_reason or 'all lines corrupted'}"
            error_path.write_text(error_msg, encoding='utf-8')
            
            # Remove temp file
            temp_path.unlink()
            
            return SalvageStats(
                original_path=temp_path,
                recovered_path=None,
                error_path=error_path,
                lines_total=len(lines),
                lines_salvaged=0,
                lines_corrupted=len(lines),
                success=False,
                error_message=error_reason or "No valid JSON lines could be salvaged"
            )
        
        # Generate recovered filename
        stem = temp_path.stem.replace('.ndjson.gz.part', '').replace('.part', '')
        if stem.endswith('.ndjson.gz'):
            stem = stem[:-10]  # Remove .ndjson.gz if present
        recovered_path = temp_path.parent / f"{stem}_recovered.ndjson.gz"
        
        # Write salvaged lines to new compressed file
        with gzip.open(recovered_path, 'wt', encoding='utf-8') as f:
            for line in valid_lines:
                f.write(line + '\n')
            # Flush and fsync within the same file handle
            f.flush()
            try:
                os.fsync(f.fileno())
            except (OSError, AttributeError) as e:
                # fsync may not work on all file types/systems
                logger.debug(f"fsync failed for {recovered_path}: {e}")
        
        # Fsync directory (best effort)
        try:
            dir_fd = os.open(str(temp_path.parent), os.O_RDONLY)
            os.fsync(dir_fd)
            os.close(dir_fd)
        except (OSError, AttributeError):
            pass
        
        # Create error sidecar with salvage summary
        error_path = temp_path.parent / f"{temp_path.name}.error"
        error_msg = f"bytes_read={bytes_read}, lines_salvaged={len(valid_lines)}, reason=\"{error_reason or 'complete file'}\""
        if corrupted_count > 0:
            error_msg += f", invalid_json_lines={corrupted_count}"
        error_path.write_text(error_msg, encoding='utf-8')
        
        # Remove temp file
        temp_path.unlink()
        
        logger.info(f"Successfully salvaged {temp_path} -> {recovered_path} ({len(valid_lines)} lines)")
        
        return SalvageStats(
            original_path=temp_path,
            recovered_path=recovered_path, 
            error_path=error_path,
            lines_total=len(lines),
            lines_salvaged=len(valid_lines),
            lines_corrupted=corrupted_count,
            success=True
        )
        
    except Exception as e:
        logger.error(f"Failed to salvage {temp_path}: {e}")
        return SalvageStats(
            original_path=temp_path,
            recovered_path=None,
            error_path=None,
            lines_total=0,
            lines_salvaged=0,
            lines_corrupted=0,
            success=False,
            error_message=str(e)
        )


def recover_monitor_temp_files(monitor_dir: Path) -> List[SalvageStats]:
    """Recover all temp files for a specific monitor.
    
    Args:
        monitor_dir: Directory containing monitor spool files
        
    Returns:
        List of SalvageStats for each temp file found
    """
    if not monitor_dir.exists():
        return []
    
    logger.debug(f"Scanning for temp files in {monitor_dir}")
    
    results = []
    
    # Find all .part files
    part_files = list(monitor_dir.glob("*.part"))
    
    for part_file in part_files:
        logger.info(f"Recovering temp file: {part_file}")
        
        # Determine salvage method based on filename
        if part_file.name.endswith('.ndjson.gz.part'):
            # Gzipped NDJSON temp file
            stats = salvage_gzipped_ndjson(part_file)
        else:
            # Assume plain NDJSON temp file
            stats = salvage_plain_ndjson(part_file)
        
        results.append(stats)
    
    return results


def recover_all_temp_files(spool_base_dir: Path, monitors: Optional[List[str]] = None) -> RecoveryReport:
    """Recover temp files across all or specified monitors.
    
    Args:
        spool_base_dir: Base spool directory containing monitor subdirs
        monitors: Optional list of monitor names to process. If None, processes all.
        
    Returns:
        RecoveryReport with overall results
    """
    logger.info(f"Starting recovery sweep in {spool_base_dir}")
    
    if not spool_base_dir.exists():
        logger.warning(f"Spool directory does not exist: {spool_base_dir}")
        return RecoveryReport(
            monitors_processed=[],
            temp_files_found=0,
            temp_files_recovered=0,
            temp_files_failed=0,
            total_lines_salvaged=0,
            salvage_stats=[]
        )
    
    # Get monitor directories to process
    if monitors:
        monitor_dirs = [spool_base_dir / monitor for monitor in monitors if (spool_base_dir / monitor).exists()]
    else:
        monitor_dirs = [d for d in spool_base_dir.iterdir() if d.is_dir() and not d.name.startswith('.')]
    
    all_stats = []
    monitors_processed = []
    
    for monitor_dir in monitor_dirs:
        monitor_name = monitor_dir.name
        monitors_processed.append(monitor_name)
        
        monitor_stats = recover_monitor_temp_files(monitor_dir)
        all_stats.extend(monitor_stats)
        
        if monitor_stats:
            recovered = sum(1 for s in monitor_stats if s.success)
            failed = len(monitor_stats) - recovered
            lines = sum(s.lines_salvaged for s in monitor_stats)
            logger.info(f"Monitor {monitor_name}: recovered {recovered}/{len(monitor_stats)} temp files, {lines} lines salvaged")
    
    # Calculate totals
    temp_files_found = len(all_stats)
    temp_files_recovered = sum(1 for s in all_stats if s.success)
    temp_files_failed = temp_files_found - temp_files_recovered
    total_lines_salvaged = sum(s.lines_salvaged for s in all_stats)
    
    report = RecoveryReport(
        monitors_processed=monitors_processed,
        temp_files_found=temp_files_found,
        temp_files_recovered=temp_files_recovered,
        temp_files_failed=temp_files_failed,
        total_lines_salvaged=total_lines_salvaged,
        salvage_stats=all_stats
    )
    
    logger.info(report.summary_line())
    
    return report