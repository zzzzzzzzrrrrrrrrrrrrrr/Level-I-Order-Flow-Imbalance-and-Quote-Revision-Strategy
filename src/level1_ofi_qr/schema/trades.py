"""Schema definitions for normalized trade data."""

from __future__ import annotations

from typing import Final

import pandas as pd

from .common import (
    COMMON_COLUMNS,
    SchemaValidationError,
    validate_common_frame,
)

TRADE_PRICE: Final[str] = "trade_price"
TRADE_SIZE: Final[str] = "trade_size"
TRADE_EXCHANGE: Final[str] = "trade_exchange"
SALE_CONDITION: Final[str] = "sale_condition"
TRADE_CORRECTION: Final[str] = "trade_correction"
TRADE_ID: Final[str] = "trade_id"
TRADE_SOURCE: Final[str] = "trade_source"
TRADE_SEQUENCE_NUMBER: Final[str] = "trade_sequence_number"

TRADE_COLUMNS: Final[tuple[str, ...]] = (
    *COMMON_COLUMNS,
    TRADE_EXCHANGE,
    SALE_CONDITION,
    TRADE_CORRECTION,
    TRADE_ID,
    TRADE_SOURCE,
    TRADE_SEQUENCE_NUMBER,
    TRADE_PRICE,
    TRADE_SIZE,
)


def validate_trade_frame(trades: pd.DataFrame) -> pd.DataFrame:
    """Validate a normalized trade frame."""

    validate_common_frame(trades, required_columns=TRADE_COLUMNS)

    trade_exchange_values = trades[TRADE_EXCHANGE].astype("string")
    if trade_exchange_values.isna().any() or (trade_exchange_values.str.strip() == "").any():
        raise SchemaValidationError("Trade rows must populate trade_exchange.")

    required_value_columns = [
        TRADE_PRICE,
        TRADE_SIZE,
        TRADE_CORRECTION,
        TRADE_ID,
        TRADE_SOURCE,
        TRADE_SEQUENCE_NUMBER,
    ]
    missing_mask = trades.loc[:, required_value_columns].isna()
    if missing_mask.any(axis=1).any():
        missing_counts = missing_mask.sum()
        missing_counts = missing_counts[missing_counts > 0]

        raise SchemaValidationError(
            "Trade rows must populate trade_price, trade_size, trade_correction, trade_id, "
            "trade_source, and trade_sequence_number. "
            f"Missing counts: {missing_counts.to_dict()}"
        )

    return trades
