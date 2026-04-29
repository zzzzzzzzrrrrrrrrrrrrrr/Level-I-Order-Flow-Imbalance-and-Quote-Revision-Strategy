"""Schema definitions for normalized Level-I quote data."""

from __future__ import annotations

from typing import Final

import pandas as pd

from .common import (
    COMMON_COLUMNS,
    SchemaValidationError,
    validate_common_frame,
)

BID: Final[str] = "bid"
ASK: Final[str] = "ask"
BID_SIZE: Final[str] = "bid_size"
ASK_SIZE: Final[str] = "ask_size"
QUOTE_EXCHANGE: Final[str] = "quote_exchange"
ASK_EXCHANGE: Final[str] = "ask_exchange"
BID_EXCHANGE: Final[str] = "bid_exchange"
NATBBO_INDICATOR: Final[str] = "natbbo_indicator"
QUOTE_CONDITION: Final[str] = "quote_condition"
QUOTE_SEQUENCE_NUMBER: Final[str] = "quote_sequence_number"

QUOTE_COLUMNS: Final[tuple[str, ...]] = (
    *COMMON_COLUMNS,
    QUOTE_EXCHANGE,
    BID_EXCHANGE,
    ASK_EXCHANGE,
    QUOTE_CONDITION,
    QUOTE_SEQUENCE_NUMBER,
    NATBBO_INDICATOR,
    BID,
    ASK,
    BID_SIZE,
    ASK_SIZE,
)


def validate_quote_frame(quotes: pd.DataFrame) -> pd.DataFrame:
    """Validate a normalized Level-I quote frame."""

    validate_common_frame(quotes, required_columns=QUOTE_COLUMNS)

    for column in (QUOTE_EXCHANGE, BID_EXCHANGE, ASK_EXCHANGE):
        exchange_values = quotes[column].astype("string")
        if exchange_values.isna().any() or (exchange_values.str.strip() == "").any():
            raise SchemaValidationError(f"Quote rows must populate {column}.")

    required_audit_columns = [QUOTE_SEQUENCE_NUMBER]
    missing_audit_mask = quotes.loc[:, required_audit_columns].isna()
    if missing_audit_mask.any(axis=1).any():
        missing_counts = missing_audit_mask.sum()
        missing_counts = missing_counts[missing_counts > 0]

        raise SchemaValidationError(
            "Quote rows must populate quote_sequence_number. "
            f"Missing counts: {missing_counts.to_dict()}"
        )

    required_value_columns = [BID, ASK, BID_SIZE, ASK_SIZE]
    missing_mask = quotes.loc[:, required_value_columns].isna()
    if missing_mask.any(axis=1).any():
        missing_counts = missing_mask.sum()
        missing_counts = missing_counts[missing_counts > 0]

        raise SchemaValidationError(
            "Quote rows must populate bid, ask, bid_size, and ask_size. "
            f"Missing counts: {missing_counts.to_dict()}"
        )

    return quotes
