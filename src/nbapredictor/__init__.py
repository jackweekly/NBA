"""NBA predictor package."""

from .nbadb_sync import bootstrap_from_kaggle, update_raw_data

__all__ = ["bootstrap_from_kaggle", "update_raw_data"]
