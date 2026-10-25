"""Shared configuration and atomic state helpers for the RabbitMQ lab."""

from contextlib import contextmanager
import fcntl
import json
from pathlib import Path


QUEUE_NAME = "orders"
RUNTIME_DIR = Path(__file__).parent / "runtime"
PROCESSED_FILE = RUNTIME_DIR / "processed_event_ids.txt"
FAILURE_FILE = RUNTIME_DIR / "failure_claims.txt"


def ensure_runtime_dir():
    RUNTIME_DIR.mkdir(exist_ok=True)


@contextmanager
def locked_file(path):
    ensure_runtime_dir()
    with path.open("a+", encoding="utf-8") as file:
        fcntl.flock(file.fileno(), fcntl.LOCK_EX)
        try:
            file.seek(0)
            yield file
        finally:
            fcntl.flock(file.fileno(), fcntl.LOCK_UN)


def already_processed(event_id):
    with locked_file(PROCESSED_FILE) as file:
        return event_id in {line.strip() for line in file if line.strip()}


def mark_processed(event_id):
    with locked_file(PROCESSED_FILE) as file:
        file.seek(0)
        processed = {line.strip() for line in file if line.strip()}
        if event_id in processed:
            return False
        file.seek(0, 2)
        file.write(f"{event_id}\n")
        file.flush()
        return True


def claim_failure(event_id):
    """Return True exactly once across all worker processes for an event."""
    with locked_file(FAILURE_FILE) as file:
        claimed = {line.strip() for line in file if line.strip()}
        if event_id in claimed:
            return False
        file.seek(0, 2)
        file.write(f"{event_id}\n")
        file.flush()
        return True


def decode_message(body):
    message = json.loads(body.decode("utf-8"))
    required = {"event_id", "order_id", "item_id", "quantity"}
    missing = required - message.keys()
    if missing:
        raise ValueError(f"missing fields: {sorted(missing)}")
    return message
