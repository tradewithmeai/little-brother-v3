"""Orchestration module for chaining hourly and daily AI pipeline."""

import time

from ..database import Database
from . import lock, reconcile, summarise, summarise_days, timeutils
from .advice import (
    get_daily_advice,
    get_hourly_advice,
    upsert_daily_advice,
    upsert_hourly_advice,
)
from .digest import (
    ensure_digests_dir,
    render_daily_digest,
    render_hourly_digest,
    upsert_digest_record,
    write_json,
    write_text,
)


def tick_once(
    db: Database,
    now_utc_ms: int,
    backfill_hours: int = 6,
    grace_minutes: int = 5,
    idle_mode: str = "simple",
    do_daily: bool = False,
    run_id: str = None,
) -> dict:
    """Execute one-shot orchestration of hourly and daily AI pipeline.

    Args:
        db: Database connection
        now_utc_ms: Current time in UTC milliseconds
        backfill_hours: Hours to backfill from now
        grace_minutes: Minutes to skip for incomplete hours
        idle_mode: Idle detection mode ("simple" or "session-gap")
        do_daily: Force daily processing regardless of time

    Returns:
        Dictionary with processing counters
    """
    # Initialize counters
    counters = {
        "hours_examined": 0,
        "hour_inserts": 0,
        "hour_updates": 0,
        "hour_advice_created": 0,
        "hour_advice_updated": 0,
        "hour_reports": 0,
        "hour_digests": 0,
        "days_processed": 0,
        "day_updates": 0,
        "day_advice_created": 0,
        "day_advice_updated": 0,
        "day_reports": 0,
        "day_digests": 0,
        "skipped_open_hours": 0,
    }

    # Calculate window and grace period
    since_utc_ms = now_utc_ms - (backfill_hours * 3600000)
    grace_ms = grace_minutes * 60000

    # Compute closed hour windows
    hour_windows = timeutils.iter_hours(since_utc_ms, now_utc_ms)
    closed_windows = []

    for hstart, hend in hour_windows:
        counters["hours_examined"] += 1
        # Skip if hour is not closed with grace period
        if now_utc_ms < hend + grace_ms:
            counters["skipped_open_hours"] += 1
            continue
        closed_windows.append((hstart, hend))

    if not closed_windows and not do_daily:
        return counters

    # Acquire advisory lock
    ttl_seconds = (backfill_hours * 60 + grace_minutes + 5) * 60
    lock_result = lock.acquire_lock(db, "tick", ttl_seconds)
    if not lock_result["success"]:
        raise RuntimeError(
            f"Failed to acquire tick lock: {lock_result.get('reason', 'unknown')}"
        )

    # Prepare common variables
    current_ms = int(time.time() * 1000)
    digest_run_id = run_id or "tick-orchestration"

    try:
        # Process hourly pipeline if we have closed windows
        if closed_windows:
            # Get overall window bounds
            window_start = closed_windows[0][0]
            window_end = closed_windows[-1][1]

            # 1. Hourly summarise for the window
            summarise_result = summarise.summarise_hours(
                db, window_start, window_end, grace_minutes, digest_run_id, 1, idle_mode
            )
            counters["hour_inserts"] += summarise_result.get("inserts", 0)
            counters["hour_updates"] += summarise_result.get("updates", 0)

            # 2. Reconcile hours for the same window
            mismatches = reconcile.find_hour_mismatches(
                db, window_start, window_end, grace_minutes
            )
            if mismatches:
                reconcile.recompute_hours(
                    db,
                    mismatches,
                    digest_run_id,
                    computed_by_version=1,
                    idle_mode=idle_mode,
                )

            # 3. For each closed hour: advice -> digest
            for hstart, hend in closed_windows:
                # Generate hourly advice
                advice_data = get_hourly_advice(db, hstart, hend, digest_run_id)
                for advice_item in advice_data:
                    result = upsert_hourly_advice(
                        db,
                        hstart,
                        advice_item["rule_key"],
                        advice_item["rule_version"],
                        advice_item["severity"],
                        advice_item["score"],
                        advice_item["advice_text"],
                        advice_item["input_hash_hex"],
                        advice_item["evidence_json"],
                        advice_item["reason_json"],
                        digest_run_id,
                    )
                    if result["action"] == "inserted":
                        counters["hour_advice_created"] += 1
                    elif result["action"] == "updated":
                        counters["hour_advice_updated"] += 1

                # Generate hourly digest
                digest_data = render_hourly_digest(db, hstart, hend)

                # Write digest files
                digests_dir = ensure_digests_dir()
                dt = time.gmtime(hstart // 1000)
                year_dir = digests_dir / f"{dt.tm_year:04d}"
                month_dir = year_dir / f"{dt.tm_mon:02d}"
                day_dir = month_dir / f"{dt.tm_mday:02d}"

                # Generate unique digest ID and file paths
                import uuid

                digest_id = str(uuid.uuid4())
                hash_short = (
                    digest_data["hour_hash"][:8]
                    if digest_data["hour_hash"]
                    else "00000000"
                )

                txt_filename = f"hourly-digest-{hstart}-{hash_short}.txt"
                json_filename = f"hourly-digest-{hstart}-{hash_short}.json"

                txt_path = day_dir / txt_filename
                json_path = day_dir / json_filename

                # Write files and record in database
                txt_sha256 = write_text(txt_path, digest_data["txt"])
                json_sha256 = write_json(json_path, digest_data["json"])

                # Record digests in database

                txt_result = upsert_digest_record(
                    db,
                    f"{digest_id}-txt",
                    "hourly_digest",
                    hstart,
                    hend,
                    "txt",
                    str(txt_path.relative_to(digests_dir.parent)),
                    txt_sha256,
                    current_ms,
                    digest_run_id,
                    digest_data["hour_hash"],
                )

                json_result = upsert_digest_record(
                    db,
                    f"{digest_id}-json",
                    "hourly_digest",
                    hstart,
                    hend,
                    "json",
                    str(json_path.relative_to(digests_dir.parent)),
                    json_sha256,
                    current_ms,
                    digest_run_id,
                    digest_data["hour_hash"],
                )

                if txt_result["action"] in ["inserted", "updated"] or json_result[
                    "action"
                ] in [
                    "inserted",
                    "updated",
                ]:
                    counters["hour_digests"] += 1

        # Determine if we should do daily processing
        should_do_daily = do_daily
        if not should_do_daily:
            # Check if now is between 00:05Z and 01:00Z
            now_seconds = (now_utc_ms // 1000) % 86400  # Seconds since midnight UTC
            should_do_daily = 300 <= now_seconds < 3600  # 00:05Z to 01:00Z

        if should_do_daily:
            counters["days_processed"] += 1

            # Calculate yesterday's day boundaries
            day_start_sec = (now_utc_ms // 1000) // 86400 * 86400
            yesterday_start_ms = (day_start_sec - 86400) * 1000

            # a) Finalise yesterday - run hourly then daily summarisation
            yesterday_end_ms = yesterday_start_ms + 86400000

            # Run hourly summarisation for the whole day
            hour_result = summarise.summarise_hours(
                db,
                yesterday_start_ms,
                yesterday_end_ms,
                grace_minutes=5,
                run_id=digest_run_id,
                computed_by_version=1,
                idle_mode=idle_mode,
            )
            counters["hour_inserts"] += hour_result.get("inserts", 0)
            counters["hour_updates"] += hour_result.get("updates", 0)

            # Run daily summarisation
            day_result = summarise_days.summarise_days(
                db,
                yesterday_start_ms,
                yesterday_end_ms,
                digest_run_id,
                computed_by_version=1,
            )
            counters["day_updates"] += day_result.get("inserts", 0) + day_result.get(
                "updates", 0
            )

            # b) Reconcile that day
            day_mismatches = reconcile.find_day_mismatches(db, [yesterday_start_ms])
            if day_mismatches:
                reconcile.recompute_days(
                    db, day_mismatches, digest_run_id, computed_by_version=1
                )

            # c) Daily advice -> digest

            # Generate daily advice
            daily_advice_data = get_daily_advice(db, yesterday_start_ms, digest_run_id)
            for advice_item in daily_advice_data:
                result = upsert_daily_advice(
                    db,
                    yesterday_start_ms,
                    advice_item["rule_key"],
                    advice_item["rule_version"],
                    advice_item["severity"],
                    advice_item["score"],
                    advice_item["advice_text"],
                    advice_item["input_hash_hex"],
                    advice_item["evidence_json"],
                    advice_item["reason_json"],
                    digest_run_id,
                )
                if result["action"] == "inserted":
                    counters["day_advice_created"] += 1
                elif result["action"] == "updated":
                    counters["day_advice_updated"] += 1

            # Generate daily digest
            daily_digest_data = render_daily_digest(db, yesterday_start_ms)

            # Write daily digest files
            dt = time.gmtime(yesterday_start_ms // 1000)
            year_dir = digests_dir / f"{dt.tm_year:04d}"
            month_dir = year_dir / f"{dt.tm_mon:02d}"
            day_dir = month_dir / f"{dt.tm_mday:02d}"

            # Generate unique digest ID and file paths
            daily_digest_id = str(uuid.uuid4())
            daily_hash_short = (
                daily_digest_data["day_hash"][:8]
                if daily_digest_data["day_hash"]
                else "00000000"
            )

            daily_txt_filename = (
                f"daily-digest-{yesterday_start_ms}-{daily_hash_short}.txt"
            )
            daily_json_filename = (
                f"daily-digest-{yesterday_start_ms}-{daily_hash_short}.json"
            )

            daily_txt_path = day_dir / daily_txt_filename
            daily_json_path = day_dir / daily_json_filename

            # Write files and record in database
            daily_txt_sha256 = write_text(daily_txt_path, daily_digest_data["txt"])
            daily_json_sha256 = write_json(daily_json_path, daily_digest_data["json"])

            # Record daily digests in database
            daily_txt_result = upsert_digest_record(
                db,
                f"{daily_digest_id}-txt",
                "daily_digest",
                yesterday_start_ms,
                yesterday_start_ms + 86400000,
                "txt",
                str(daily_txt_path.relative_to(digests_dir.parent)),
                daily_txt_sha256,
                current_ms,
                digest_run_id,
                daily_digest_data["day_hash"],
            )

            daily_json_result = upsert_digest_record(
                db,
                f"{daily_digest_id}-json",
                "daily_digest",
                yesterday_start_ms,
                yesterday_start_ms + 86400000,
                "json",
                str(daily_json_path.relative_to(digests_dir.parent)),
                daily_json_sha256,
                current_ms,
                digest_run_id,
                daily_digest_data["day_hash"],
            )

            if daily_txt_result["action"] in [
                "inserted",
                "updated",
            ] or daily_json_result["action"] in ["inserted", "updated"]:
                counters["day_digests"] += 1

    finally:
        # Release advisory lock
        lock.release_lock(db, "tick", lock_result["owner_token"])

    return counters
