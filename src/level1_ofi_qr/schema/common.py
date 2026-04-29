"""Shared schema fields and validation helpers for normalized market data."""

from __future__ import annotations

from typing import Final, Iterable

import pandas as pd
from pandas.api.types import is_datetime64_any_dtype

EVENT_TIME: Final[str] = "event_time"
SYMBOL: Final[str] = "symbol"
SOURCE: Final[str] = "source"

COMMON_COLUMNS: Final[tuple[str, ...]] = (
    EVENT_TIME,
    SYMBOL,
    SOURCE,
)


class SchemaValidationError(ValueError):
    """Raised when a frame does not satisfy the project schema contract."""


def validate_common_frame(
    frame: pd.DataFrame,
    *,
    required_columns: Iterable[str],
) -> pd.DataFrame:
    """Validate the shared part of a normalized market-data frame."""

    required_columns = tuple(required_columns)
    missing_columns = [column for column in required_columns if column not in frame.columns]
    if missing_columns:
        missing_list = ", ".join(missing_columns)
        raise SchemaValidationError(f"Missing required columns: {missing_list}")

    if not is_datetime64_any_dtype(frame[EVENT_TIME]):
        raise SchemaValidationError(
            f"Column '{EVENT_TIME}' must use a datetime dtype before schema validation."
        )

    if frame[EVENT_TIME].isna().any():
        raise SchemaValidationError(f"Column '{EVENT_TIME}' contains null timestamps.")

    for column in (SYMBOL, SOURCE):
        string_values = frame[column].astype("string")
        if string_values.isna().any() or (string_values.str.strip() == "").any():
            raise SchemaValidationError(f"Column '{column}' contains null or empty values.")

    return frame
