"""
Supabase logging handler for 3DTrees API.

This module provides a custom logging handler that writes important log messages
to the Supabase `logs` table, allowing centralized log access without SSH.

Usage:
    from trees_api.supabase_log_handler import setup_supabase_logging
    
    # After creating supabase_client:
    setup_supabase_logging(supabase_client.client, source="api")
"""

import logging
import time
import threading
from typing import Optional, Set
from supabase import Client

# Keywords that indicate an important log message worth storing
IMPORTANT_KEYWORDS = [
    "workflow", "invocation", "invoked",
    "dataset", "job", "sync",
    "completed", "created", "failed",
    "error", "success", "uploaded",
    "imported", "started", "finished",
    "history", "collection",
]

# Maximum logs per minute to prevent runaway logging
MAX_LOGS_PER_MINUTE = 100

# Deduplication window in seconds
DEDUP_WINDOW_SECONDS = 60


class SupabaseLogHandler(logging.Handler):
    """
    Custom logging handler that writes to Supabase `logs` table.
    
    Features:
    - Keyword-based filtering (only logs important messages)
    - Always logs WARNING and above
    - Rate limiting (max 100 logs/minute)
    - Message deduplication (skip identical messages within 60s)
    """
    
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
        """Check if message contains important keywords."""
        msg_lower = message.lower()
        return any(kw in msg_lower for kw in IMPORTANT_KEYWORDS)
    
    def _check_rate_limit(self) -> bool:
        """Check if we're within rate limit. Returns True if OK to log."""
        current_time = time.time()
        
        # Reset counter every minute
        if current_time - self._minute_start >= 60:
            self._log_count = 0
            self._minute_start = current_time
        
        if self._log_count >= MAX_LOGS_PER_MINUTE:
            return False
        
        self._log_count += 1
        return True
    
    def _check_dedup(self, message: str) -> bool:
        """Check if message is duplicate. Returns True if OK to log."""
        current_time = time.time()
        
        # Cleanup old messages every minute
        if current_time - self._last_cleanup >= DEDUP_WINDOW_SECONDS:
            self._recent_messages.clear()
            self._last_cleanup = current_time
        
        # Create a hash of level + message for dedup
        if message in self._recent_messages:
            return False
        
        self._recent_messages.add(message)
        return True
    
    def emit(self, record: logging.LogRecord):
        """Emit a log record to Supabase if it passes filters."""
        try:
            message = record.getMessage()
            
            # Always log WARNING and above, otherwise check keywords
            if record.levelno < logging.WARNING:
                if not self._is_important(message):
                    return
            
            with self._lock:
                # Check rate limit
                if not self._check_rate_limit():
                    return
                
                # Check deduplication
                if not self._check_dedup(f"{record.levelname}:{message}"):
                    return
            
            # Build the log payload
            payload = {
                "level": record.levelname.lower(),
                "message": message[:500],  # Truncate long messages
                "operation": f"{self.source}:{record.name}",
                "metadata": {
                    "source": self.source,
                    "logger": record.name,
                    "filename": record.filename,
                    "lineno": record.lineno,
                    "funcName": record.funcName,
                },
            }
            
            # Add error details if present
            if record.exc_info:
                payload["error_details"] = self.format(record)
            
            # Insert to Supabase (fire and forget)
            self.client.table("logs").insert(payload).execute()
            
        except Exception:
            # Never crash the application due to logging failure
            pass


def setup_supabase_logging(
    supabase_client: Client,
    source: str = "unknown",
    logger_names: Optional[list] = None,
) -> SupabaseLogHandler:
    """
    Set up Supabase logging for the application.
    
    Args:
        supabase_client: The Supabase client instance
        source: Identifier for log source ("api", "status_pooler", etc.)
        logger_names: Optional list of specific logger names to attach to.
                     If None, attaches to root logger (catches all).
    
    Returns:
        The created SupabaseLogHandler instance
    
    Example:
        # In API server startup:
        handler = setup_supabase_logging(supabase_client.client, source="api")
        
        # In status pooler:
        handler = setup_supabase_logging(supabase_client.client, source="status_pooler")
    """
    handler = SupabaseLogHandler(supabase_client, source=source)
    
    # Set a formatter for error details
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s\n%(exc_info)s'
    )
    handler.setFormatter(formatter)
    
    if logger_names:
        # Attach to specific loggers
        for name in logger_names:
            logging.getLogger(name).addHandler(handler)
    else:
        # Attach to root logger (catches all)
        logging.getLogger().addHandler(handler)
    
    return handler
