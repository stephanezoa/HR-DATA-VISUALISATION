from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler

from .config import LOG_DIR, ensure_runtime_dirs


SERVICE_LOG_FILES = {
    "app": "app.log",
    "routes": "routes.log",
    "storage": "storage.log",
    "analysis": "analysis.log",
    "jobs": "jobs.log",
    "mail": "mail.log",
}


def get_logger(service: str) -> logging.Logger:
    return logging.getLogger(f"hr.{service}")


def setup_logging(level: int = logging.INFO) -> None:
    ensure_runtime_dirs()
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")

    for service, filename in SERVICE_LOG_FILES.items():
        logger = get_logger(service)
        logger.setLevel(level)
        logger.propagate = False

        log_path = LOG_DIR / filename
        log_path.touch(exist_ok=True)

        if any(
            isinstance(handler, RotatingFileHandler) and getattr(handler, "baseFilename", "") == str(log_path)
            for handler in logger.handlers
        ):
            continue

        handler = RotatingFileHandler(
            log_path,
            maxBytes=1_500_000,
            backupCount=5,
            encoding="utf-8",
        )
        handler.setLevel(level)
        handler.setFormatter(formatter)
        logger.addHandler(handler)
