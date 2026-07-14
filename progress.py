from __future__ import annotations

from scanner import ScanState, ScanStatus


def format_bytes(num_bytes: int) -> str:
    """Human-readable byte size, e.g. 1536 -> '1.50 KB'."""
    value = float(num_bytes)
    for unit in ("B", "KB", "MB", "GB", "TB", "PB"):
        if value < 1024.0 or unit == "PB":
            if unit == "B":
                return f"{int(value)} {unit}"
            return f"{value:.2f} {unit}"
        value /= 1024.0
    return f"{value:.2f} PB"  # unreachable in practice, keeps mypy/pylint happy


def format_duration(seconds: float) -> str:
    """Human-readable duration, e.g. 3725 -> '1h 2m 5s'."""
    if seconds < 0 or seconds != seconds:  # NaN guard
        return "unknown"
    seconds = int(seconds)
    days, seconds = divmod(seconds, 86400)
    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)

    parts = []
    if days:
        parts.append(f"{days}d")
    if hours:
        parts.append(f"{hours}h")
    if minutes:
        parts.append(f"{minutes}m")
    if seconds or not parts:
        parts.append(f"{seconds}s")
    return " ".join(parts)


def format_speed(messages_per_second: float) -> str:
    if messages_per_second <= 0:
        return "0 items/s"
    if messages_per_second >= 1:
        return f"{messages_per_second:.2f} items/s"
    # Slower than 1/s: express as seconds per item instead, more readable.
    return f"1 item / {format_duration(1 / messages_per_second)}"


_STATE_EMOJI = {
    ScanState.IDLE: "\u23f8",       # pause symbol used loosely for idle too
    ScanState.RUNNING: "\u25b6",    # play
    ScanState.PAUSED: "\u23f8",     # pause
    ScanState.STOPPING: "\u23f9",   # stop
}


def build_status_message(status: ScanStatus) -> str:
    """
    Render the full /status response text. Kept deliberately plain
    (no Markdown table gymnastics) so it renders identically whether
    parse_mode is left default or set to Markdown/HTML by the caller.
    """
    lines: list[str] = []
    lines.append(f"Scanner status: {status.state.value.upper()}")

    if status.current_chat_id is not None:
        title = status.current_title or str(status.current_chat_id)
        lines.append(f"Current channel: {title} ({status.current_chat_id})")
        lines.append(
            f"This channel: {status.channel_scanned} scanned, "
            f"{status.channel_duplicates} duplicates"
        )
    else:
        lines.append("Current channel: none (idle)")

    lines.append(
        f"Total videos scanned: {status.total_scanned}\n"
        f"Total duplicates removed: {status.total_duplicates}"
    )
    lines.append(f"Speed: {format_speed(status.messages_per_second)}")

    if status.eta_seconds is not None:
        lines.append(f"ETA (current channel): {format_duration(status.eta_seconds)}")
    else:
        lines.append("ETA (current channel): unknown")

    lines.append(f"Channels queued: {status.queue_size}")
    lines.append(f"Database size: {format_bytes(status.database_size_bytes)}")

    return "\n".join(lines)


def build_stats_message(stats: dict, database_size_bytes: int, unique_hash_count: int) -> str:
    scanned = stats.get("scanned", 0)
    duplicates = stats.get("duplicates", 0)
    dedup_rate = (duplicates / scanned * 100.0) if scanned else 0.0

    lines = [
        "Global statistics",
        f"Videos scanned (all time): {scanned}",
        f"Duplicates removed (all time): {duplicates}",
        f"Unique files stored: {unique_hash_count}",
        f"Duplicate rate: {dedup_rate:.2f}%",
        f"Database size: {format_bytes(database_size_bytes)}",
    ]
    return "\n".join(lines)
