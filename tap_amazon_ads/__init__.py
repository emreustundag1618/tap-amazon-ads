"""Tap for TapAmazonAds."""

import logging
import os
import sys
from pathlib import Path

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_amazon_ads.tap import TapTapAmazonAds

__all__ = ["TapTapAmazonAds"]

# Create logs directory if it doesn't exist
log_dir = Path("logs")
log_dir.mkdir(exist_ok=True)

# Configure logging to both console and file
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stderr),  # Log to console
        logging.FileHandler(log_dir / "tap_amazon_ads.log"),  # Log to file
    ],
)

# Get logger for this module
logger = logging.getLogger(__name__)
logger.info("Logging initialized. Log file: %s", (log_dir / "tap_amazon_ads.log").absolute())

# Silence noisy loggers
for log_name in ["requests", "urllib3", "singer", "boto3", "botocore"]:
    logging.getLogger(log_name).setLevel(logging.WARNING)
