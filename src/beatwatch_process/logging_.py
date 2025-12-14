import os
import sys
from pathlib import Path

from loguru import logger


def setup_logging(level: str = "INFO", logfile: str = "__logs/analysis.log"):
    logger.remove()
    _ = logger.add(
        sys.stderr,
        level=level,
        enqueue=True,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
        + "<level>{level: <8}</level> | "
        + "<cyan>{name}</cyan>:<cyan>{function}</cyan> — {message}",
    )
    if logfile:
        Path(logfile).parent.mkdir(parents=True, exist_ok=True)
        _ = logger.add(
            logfile, level=level, rotation="50 MB", retention=10, enqueue=True
        )
    logger.info(
        "Logging started:\nPython environment information:"
        + f"\n - Executable: {sys.executable}"
        + f"\n - Version: {sys.version}"
        + f"\n - Working directory: {os.getcwd()}"
    )
    return logger
