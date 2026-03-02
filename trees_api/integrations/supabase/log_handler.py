"""Supabase logging handler for 3DTrees API."""

import logging
import threading
import time
from typing import Optional, Set

from supabase import Client

IMPORTANT_KEYWORDS = [
    "workflow",
    "invocation",
    "invoked",
    "dataset",
    "job",
    "sync",
    "completed",
    "created",
    "failed",
    "error",
    "success",
    "uploaded",
    "imported",
    "started",
    "finished",
    "history",
    "collection",
]
MAX_LOGS_PER_MINUTE = 100
DEDUP_WINDOW_SECONDS = 60


class SupabaseLogHandler(logging.Handler):
    def __init__(
        self,
        supabase_client: Client,
        source: str = "unknown",
        min_level: int = logging.INFO,
    ):
        super().__init__(level=min_level)
        self.client = supabase_client
        self.source = source
        self._lock = threading.Lock()
        self._log_count = 0
        self._minute_start = time.time()
        self._recent_messages: Set[str] = set()
        self._last_cleanup = time.time()

    def _is_important(self, message: str) -> bool:
        msg_lower = message.lower()
        return any(keyword in msg_lower for keyword in IMPORTANT_KEYWORDS)

    def _check_rate_limit(self) -> bool:
        current_time = time.time()
        if current_time - self._minute_start >= 60:
            self._log_count = 0
            self._minute_start = current_time
        if self._log_count >= MAX_LOGS_PER_MINUTE:
            return False
        self._log_count += 1
        return True

    def _check_dedup(self, message: str) -> bool:
        current_time = time.time()
        if current_time - self._last_cleanup >= DEDUP_WINDOW_SECONDS:
            self._recent_messages.clear()
            self._last_cleanup = current_time
        if message in self._recent_messages:
            return False
        self._recent_messages.add(message)
        return True

    def emit(self, record: logging.LogRecord):
        try:
            message = record.getMessage()
            if record.levelno < logging.WARNING and not self._is_important(message):
                return
            with self._lock:
                if not self._check_rate_limit():
                    return
                if not self._check_dedup(f"{record.levelname}:{message}"):
                    return
            payload = {
                "level": record.levelname.lower(),
                "message": message[:500],
                "operation": f"{self.source}:{record.name}",
                "metadata": {
                    "source": self.source,
                    "logger": record.name,
                    "filename": record.filename,
                    "lineno": record.lineno,
                    "funcName": record.funcName,
                },
            }
            if record.exc_info:
                payload["error_details"] = self.format(record)
            self.client.table("logs").insert(payload).execute()
        except Exception:
            pass


def setup_supabase_logging(
    supabase_client: Client,
    source: str = "unknown",
    logger_names: Optional[list] = None,
) -> SupabaseLogHandler:
    handler = SupabaseLogHandler(supabase_client, source=source)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s\n%(exc_info)s")
    handler.setFormatter(formatter)
    if logger_names:
        for name in logger_names:
            logging.getLogger(name).addHandler(handler)
    else:
        logging.getLogger().addHandler(handler)
    return handler


__all__ = ["SupabaseLogHandler", "setup_supabase_logging"]

