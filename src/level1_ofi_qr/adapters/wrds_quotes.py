"""Normalization adapter for WRDS TAQ quote data."""

from __future__ import annotations

import pandas as pd

from ..schema import QUOTE_COLUMNS, validate_quote_frame
from ..utils import DataSliceConfig
from .wrds_common import normalize_wrds_frame


def normalize_wrds_quotes(
    raw_quotes: pd.DataFrame,
    *,
    config: DataSliceConfig,
) -> pd.DataFrame:
    """Normalize WRDS quote rows into the project quote schema."""

    normalized = normalize_wrds_frame(
        raw_quotes,
        mapping=config.data_contract.quote_mapping,
        output_columns=QUOTE_COLUMNS,
        market_timezone=config.time_range.timezone,
    )
    return validate_quote_frame(normalized)
