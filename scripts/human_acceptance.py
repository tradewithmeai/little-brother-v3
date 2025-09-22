# scripts/human_acceptance.py
# Runs lb3 end-to-end on Windows: starts daemon, simulates user activity, shuts down cleanly,
# flushes/imports, prints event counts. Exits 0 on success, 1 otherwise.

import os
import signal
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

# Optional CPU sampling (requires psutil; falls back gracefully)
try:
    import psutil  # type: ignore
except Exception:  # pragma: no cover
    psutil = None

LB3_DB = Path("lb_data") / "local.db"


def run_cmd(args):
    return subprocess.run(args, capture_output=True, text=True, shell=False)


def start_lb3():
    # Start lb3 in its own process group so we can send CTRL_BREAK_EVENT for a graceful stop.
    return subprocess.Popen(
        ["python", "-m", "lb3", "run"],
        creationflags=subprocess.CREATE_NEW_PROCESS_GROUP,  # Windows-only
    )


def graceful_stop(proc: subprocess.Popen):
    try:
        # Send Ctrl+Break to the process group (more reliable than Ctrl+C across processes)
        os.kill(proc.pid, signal.CTRL_BREAK_EVENT)  # type: ignore[attr-defined]
    except Exception:
        proc.terminate()
    try:
        proc.wait(timeout=20)
    except subprocess.TimeoutExpired:
        proc.kill()


def simulate_activity(duration_s: int = 75):
    """
    Generates:
      - foreground switches (Alt+Tab)
      - typing bursts
      - mouse moves/clicks/scrolls
      - file create → rename → delete on Desktop
    """
    from pynput.keyboard import Controller as Kbd  # type: ignore
    from pynput.keyboard import Key
    from pynput.mouse import Controller as Mouse  # type: ignore

    # Launch two simple targets to switch between
    np = subprocess.Popen(["notepad.exe"])
    ex = subprocess.Popen(["explorer.exe", str(Path.home())])
    time.sleep(2.0)

    kbd, mouse = Kbd(), Mouse()
    desktop = Path.home() / "Desktop"
    desktop.mkdir(exist_ok=True)

    t0 = time.time()
    next_file_op = t0 + 3.0
    cpu_samples = []

    while time.time() - t0 < duration_s:
        # small typing burst
        for ch in ("h", "i", " ", "t", "h", "e", "r", "e"):
            kbd.press(ch)
            kbd.release(ch)
            time.sleep(0.02)

        # mouse square + scroll + clicks
        x, y = mouse.position
        for dx, dy in [(20, 0), (0, 20), (-20, 0), (0, -20)]:
            mouse.position = (x + dx, y + dy)
            time.sleep(0.01)
        mouse.scroll(0, 1)
        mouse.press(1)
        mouse.release(1)  # left click

        # toggle foreground app (Alt+Tab) a couple times
        kbd.press(Key.alt)
        kbd.press(Key.tab)
        kbd.release(Key.tab)
        kbd.release(Key.alt)
        time.sleep(0.2)
        kbd.press(Key.alt)
        kbd.press(Key.tab)
        kbd.release(Key.tab)
        kbd.release(Key.alt)

        # periodic file ops to hit the file monitor
        now = time.time()
        if now >= next_file_op:
            p = desktop / "lb3_test.txt"
            with open(p, "w", encoding="utf-8") as f:
                f.write("lb3 test\n")
            time.sleep(0.1)
            p2 = desktop / "lb3_test_renamed.txt"
            try:
                p.rename(p2)
            except FileNotFoundError:
                pass
            time.sleep(0.1)
            try:
                p2.unlink()
            except FileNotFoundError:
                pass
            next_file_op = now + 6.0

        if psutil:
            cpu_samples.append(psutil.cpu_percent(interval=0.1))
        else:
            time.sleep(0.1)

    # try to close apps we opened
    for p in (np, ex):
        try:
            p.terminate()
        except Exception:
            pass

    return max(cpu_samples) if cpu_samples else None


def flush_and_status():
    print("==> Flushing spool into DB...")
    print(run_cmd(["lb3", "spool", "flush"]).stdout.strip())
    print("==> Status:")
    print(run_cmd(["lb3", "status"]).stdout.strip())


def print_counts(db_path: Path) -> int:
    con = sqlite3.connect(str(db_path))
    try:
        cur = con.cursor()
        rows = list(
            cur.execute(
                "select monitor, count(*) from events group by monitor order by 2 desc"
            )
        )
    finally:
        con.close()
    print("==> Event counts by monitor:")
    total = 0
    for m, c in rows:
        print(f"{m}\t{c}")
        total += int(c)
    return total


def main():
    # Ensure config and data dirs exist / get created on first run
    print(run_cmd(["python", "-m", "lb3", "config", "path"]).stdout.strip())

    print("==> Starting lb3 run ...")
    proc = start_lb3()
    try:
        max_cpu = simulate_activity(75)
    finally:
        print("==> Stopping lb3 gracefully ...")
        graceful_stop(proc)

    flush_and_status()

    if not LB3_DB.exists():
        print("ERROR: DB not found at", LB3_DB)
        sys.exit(2)

    total = print_counts(LB3_DB)
    if total < 50:
        print(f"FAIL: only {total} events ingested (< 50 required)")
        sys.exit(1)

    if max_cpu is not None:
        print(f"Max CPU% observed (approx): {max_cpu:.1f}")

    print("PASS: human-acceptance criteria satisfied.")
    sys.exit(0)


if __name__ == "__main__":
    if sys.platform != "win32":
        print("This script is Windows-only.")
        sys.exit(3)
    main()
