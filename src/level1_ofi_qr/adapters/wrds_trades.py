"""Normalization adapter for WRDS TAQ trade data."""

from __future__ import annotations

import pandas as pd

from ..schema import TRADE_COLUMNS, validate_trade_frame
from ..utils import DataSliceConfig
from .wrds_common import normalize_wrds_frame


def normalize_wrds_trades(
    raw_trades: pd.DataFrame,
    *,
    config: DataSliceConfig,
) -> pd.DataFrame:
    """Normalize WRDS trade rows into the project trade schema."""

    normalized = normalize_wrds_frame(
        raw_trades,
        mapping=config.data_contract.trade_mapping,
        output_columns=TRADE_COLUMNS,
        market_timezone=config.time_range.timezone,
    )
    return validate_trade_frame(normalized)
