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
ASK_EXCHANGE: Final[str] = "ask_exchange"
BID_EXCHANGE: Final[str] = "bid_exchange"
NBBO_QUOTE_CONDITION: Final[str] = "nbbo_quote_condition"

QUOTE_COLUMNS: Final[tuple[str, ...]] = (
    *COMMON_COLUMNS,
    BID_EXCHANGE,
    ASK_EXCHANGE,
    NBBO_QUOTE_CONDITION,
    BID,
    ASK,
    BID_SIZE,
    ASK_SIZE,
)


def validate_quote_frame(quotes: pd.DataFrame) -> pd.DataFrame:
    """Validate a normalized Level-I quote frame."""

    validate_common_frame(quotes, required_columns=QUOTE_COLUMNS)

    exchange_columns = [BID_EXCHANGE, ASK_EXCHANGE]
    missing_exchange_mask = quotes.loc[:, exchange_columns].isna()
    if missing_exchange_mask.any(axis=1).any():
        missing_counts = missing_exchange_mask.sum()
        missing_counts = missing_counts[missing_counts > 0]
        raise SchemaValidationError(
            "Quote rows must populate bid_exchange and ask_exchange. "
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
