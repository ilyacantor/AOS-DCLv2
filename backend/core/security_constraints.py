"""
Security Constraints - Build-Time and Runtime Enforcement

This module provides constraints that prevent Zero-Trust violations:
1. Block payload.body writes to disk
2. Block payload serialization to logs
3. Enforce metadata-only storage

Architecture: Self-Healing Mesh & Zero-Trust Vision
- DCL is "The Brain" - Metadata-Only
- These constraints are enforced at build/runtime
"""

import functools
import logging
import os
import sys
from typing import Any, Callable, Dict, Optional, Set

logger = logging.getLogger("dcl.security")


class ZeroTrustViolation(Exception):
    """Raised when Zero-Trust security policy is violated."""
    pass


BLOCKED_WRITE_PATTERNS = {
    "payload",
    "body",
    "data",
    "raw_data",
    "content",
    "record_data",
    "customer_data",
    "pii",
    "ssn",
    "password",
    "secret",
    "credit_card",
}


def enforce_metadata_only(func: Callable) -> Callable:
    """
    Decorator that enforces metadata-only policy on function arguments.
    
    If any argument contains blocked patterns (payload, body, etc.),
    raises ZeroTrustViolation.
    
    Usage:
        @enforce_metadata_only
        def write_to_disk(data: dict):
            # Will fail if data contains 'payload' or 'body' keys
            ...
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        for i, arg in enumerate(args):
            if isinstance(arg, dict):
                _check_dict_for_violations(arg, f"arg[{i}]", func.__name__)
        
        for key, value in kwargs.items():
            if isinstance(value, dict):
                _check_dict_for_violations(value, f"kwarg[{key}]", func.__name__)
        
        return func(*args, **kwargs)
    
    return wrapper


def _check_dict_for_violations(data: Dict[str, Any], path: str, func_name: str) -> None:
    """Recursively check a dictionary for Zero-Trust violations."""
    for key, value in data.items():
        lower_key = key.lower()
        if lower_key in BLOCKED_WRITE_PATTERNS:
            raise ZeroTrustViolation(
                f"Zero-Trust Violation in {func_name}: "
                f"Attempted to write blocked key '{key}' at {path}. "
                f"DCL is metadata-only. Raw payload data must not be stored."
            )
        
        if isinstance(value, dict):
            _check_dict_for_violations(value, f"{path}.{key}", func_name)
        elif isinstance(value, list):
            for i, item in enumerate(value):
                if isinstance(item, dict):
                    _check_dict_for_violations(item, f"{path}.{key}[{i}]", func_name)


class SecureLogger:
    """
    A wrapper around logging that redacts sensitive data.
    
    Automatically strips payload/body data before logging.
    """
    
    def __init__(self, name: str):
        self._logger = logging.getLogger(name)
    
    def _redact(self, message: Any) -> str:
        """Redact sensitive patterns from log messages."""
        msg_str = str(message)
        
        for pattern in BLOCKED_WRITE_PATTERNS:
            if f'"{pattern}"' in msg_str.lower() or f"'{pattern}'" in msg_str.lower():
                logger.warning(f"SecureLogger: Redacting potential payload data from log")
                return "[REDACTED - Payload data stripped from log]"
        
        return msg_str
    
    def info(self, message: Any, *args, **kwargs):
        self._logger.info(self._redact(message), *args, **kwargs)
    
    def warning(self, message: Any, *args, **kwargs):
        self._logger.warning(self._redact(message), *args, **kwargs)
    
    def error(self, message: Any, *args, **kwargs):
        self._logger.error(self._redact(message), *args, **kwargs)
    
    def debug(self, message: Any, *args, **kwargs):
        self._logger.debug(self._redact(message), *args, **kwargs)


def validate_no_disk_payload_writes():
    """
    Build-time validation that scans code for payload disk writes.
    
    This function can be called during CI/CD to detect violations.
    Returns list of files with potential violations.
    """
    violations = []
    backend_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    dangerous_patterns = [
        "open(",
        "write(",
        ".dump(",
        "to_json(",
        "to_pickle(",
        "serialize(",
    ]
    
    payload_patterns = [
        "payload",
        "body",
        "raw_data",
    ]
    
    for root, dirs, files in os.walk(backend_path):
        if "__pycache__" in root or ".git" in root:
            continue
        
        for file in files:
            if not file.endswith(".py"):
                continue
            
            if file in ("security_constraints.py", "metadata_buffer.py"):
                continue
            
            filepath = os.path.join(root, file)
            try:
                with open(filepath, "r", encoding="utf-8") as f:
                    content = f.read()
                    lines = content.split("\n")
                
                for i, line in enumerate(lines, 1):
                    line_lower = line.lower()
                    
                    has_dangerous = any(p in line_lower for p in dangerous_patterns)
                    has_payload = any(p in line_lower for p in payload_patterns)
                    
                    if has_dangerous and has_payload:
                        if not line.strip().startswith("#"):
                            violations.append({
                                "file": filepath,
                                "line": i,
                                "content": line.strip()[:100],
                            })
            except Exception as e:
                logger.warning(f"Could not scan {filepath}: {e}")
    
    return violations


def assert_metadata_only_mode():
    """
    Runtime assertion that DCL is in metadata-only mode.
    
    Call this at startup to verify configuration.
    """
    env_mode = os.environ.get("DCL_MODE", "metadata_only")
    
    if env_mode != "metadata_only":
        raise ZeroTrustViolation(
            f"DCL must run in 'metadata_only' mode. "
            f"Current mode: {env_mode}. "
            f"Set DCL_MODE=metadata_only in environment."
        )
    
    logger.info("Zero-Trust: DCL running in metadata-only mode")


class MetadataOnlyDict(dict):
    """
    A dictionary subclass that refuses to store payload data.
    
    Use this for any data that will be persisted.
    """
    
    def __setitem__(self, key: str, value: Any):
        if isinstance(key, str) and key.lower() in BLOCKED_WRITE_PATTERNS:
            raise ZeroTrustViolation(
                f"Cannot store blocked key '{key}' in MetadataOnlyDict. "
                f"DCL is metadata-only."
            )
        super().__setitem__(key, value)
    
    def update(self, *args, **kwargs):
        if args:
            other = args[0]
            if isinstance(other, dict):
                for key in other:
                    if isinstance(key, str) and key.lower() in BLOCKED_WRITE_PATTERNS:
                        raise ZeroTrustViolation(
                            f"Cannot store blocked key '{key}' in MetadataOnlyDict. "
                            f"DCL is metadata-only."
                        )
        
        for key in kwargs:
            if key.lower() in BLOCKED_WRITE_PATTERNS:
                raise ZeroTrustViolation(
                    f"Cannot store blocked key '{key}' in MetadataOnlyDict. "
                    f"DCL is metadata-only."
                )
        
        super().update(*args, **kwargs)
