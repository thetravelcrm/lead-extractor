"""
sse/event_stream.py
-------------------
Server-Sent Events (SSE) infrastructure.

Each scraping job gets its own in-memory Queue.  The Flask `/stream/<job_id>`
route calls `event_generator(job_id)` which yields SSE-formatted lines until
the job emits a "done" or "error" event (or a 10-minute safety timeout fires).

Usage from the pipeline thread:
    from sse.event_stream import emit
    emit(job_id, "info",    "Visiting site 3/40 ...")
    emit(job_id, "success", "Found 2 emails",  data={"current": 3, "total": 40})
    emit(job_id, "warn",    "CAPTCHA detected, skipping page")
    emit(job_id, "error",   "Job failed: network unreachable")
    emit(job_id, "done",    "Complete: 28 leads saved", data={"count": 28})
"""

import json
import queue
import time
from datetime import datetime
from typing import Optional

# -----------------------------------------------------------------------
# Global job registry
# -----------------------------------------------------------------------
_job_queues: dict[str, queue.Queue] = {}
_job_cancelled: dict[str, bool] = {}


def create_job_queue(job_id: str) -> queue.Queue:
    """Create and register a new event queue for the given job."""
    q: queue.Queue = queue.Queue(maxsize=500)
    _job_queues[job_id] = q
    _job_cancelled[job_id] = False
    return q


def cancel_job(job_id: str) -> None:
    """Signal the pipeline thread that it should stop."""
    _job_cancelled[job_id] = True


def is_cancelled(job_id: str) -> bool:
    """Returns True if the job has been asked to stop."""
    return _job_cancelled.get(job_id, False)


def cleanup_job(job_id: str) -> None:
    """Remove job state after the SSE stream closes."""
    _job_queues.pop(job_id, None)
    _job_cancelled.pop(job_id, None)


# -----------------------------------------------------------------------
# Emitting events (called from the pipeline background thread)
# -----------------------------------------------------------------------

def emit(
    job_id: str,
    level: str,
    message: str,
    data: Optional[dict] = None,
) -> None:
    """
    Put a structured event onto the job's SSE queue.

    Parameters
    ----------
    job_id  : str  — job identifier
    level   : str  — one of: info | success | warn | error | progress | done
    message : str  — human-readable log message
    data    : dict — optional extra payload (e.g. {"current": 5, "total": 50})
    """
    q = _job_queues.get(job_id)
    if q is None:
        return  # job already cleaned up

    payload: dict = {
        "level":   level,
        "message": message,
        "ts":      datetime.utcnow().strftime("%H:%M:%S"),
    }
    if data:
        payload["data"] = data

    try:
        q.put_nowait(payload)
    except queue.Full:
        pass  # drop the event rather than blocking the pipeline thread


# -----------------------------------------------------------------------
# SSE generator (consumed by Flask route)
# -----------------------------------------------------------------------

def format_sse(payload: dict) -> str:
    """Format a dict as an SSE data line."""
    return f"data: {json.dumps(payload)}\n\n"


def event_generator(job_id: str):
    """
    Generator that yields SSE-formatted strings.

    Exits when:
    - A "done" or "error" event is received from the pipeline
    - 60 minutes have elapsed (safety timeout for long-running jobs)
    - The job queue no longer exists
    """
    q = _job_queues.get(job_id)
    if q is None:
        yield format_sse({"level": "error", "message": "Unknown job ID", "ts": ""})
        return

    deadline = time.time() + 3600  # 60 minutes

    while time.time() < deadline:
        try:
            payload = q.get(timeout=1.0)
        except queue.Empty:
            # Send a heartbeat comment so the connection doesn't time out
            yield ": heartbeat\n\n"
            continue

        yield format_sse(payload)

        if payload.get("level") in ("done", "error"):
            break

    cleanup_job(job_id)
