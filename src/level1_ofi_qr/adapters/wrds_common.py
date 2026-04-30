"""Shared helpers for WRDS quote and trade normalization."""

from __future__ import annotations

from typing import Any, Mapping

import pandas as pd

from ..schema import SOURCE

WRDS_EVENT_TIME_EXPRESSION = "date + time_m"
WRDS_SYMBOL_EXPRESSION = "sym_root + sym_suffix"
WrdsMappingRule = str | dict[str, Any] | None


class WrdsNormalizationError(ValueError):
    """Raised when WRDS data cannot be normalized into the project schema."""


def normalize_wrds_frame(
    raw_frame: pd.DataFrame,
    *,
    mapping: Mapping[str, WrdsMappingRule],
    output_columns: tuple[str, ...],
    market_timezone: str,
) -> pd.DataFrame:
    """Materialize a normalized frame from WRDS-style raw columns."""

    normalized_columns: dict[str, pd.Series] = {}
    for target_column in output_columns:
        if target_column not in mapping:
            raise WrdsNormalizationError(
                f"Config mapping is missing a rule for normalized column '{target_column}'."
            )

        mapping_value = mapping[target_column]
        normalized_columns[target_column] = materialize_wrds_mapped_series(
            raw_frame,
            target_column=target_column,
            mapping_value=mapping_value,
            market_timezone=market_timezone,
        )

    return pd.DataFrame(normalized_columns, index=raw_frame.index)


def materialize_wrds_mapped_series(
    raw_frame: pd.DataFrame,
    *,
    target_column: str,
    mapping_value: WrdsMappingRule,
    market_timezone: str,
) -> pd.Series:
    """Materialize one normalized column from a WRDS mapping expression."""

    if mapping_value is None:
        return pd.Series(pd.NA, index=raw_frame.index, dtype="string")

    if isinstance(mapping_value, dict):
        return materialize_wrds_structured_rule(
            raw_frame,
            target_column=target_column,
            mapping_rule=mapping_value,
            market_timezone=market_timezone,
        )

    mapping_value = mapping_value.strip()
    if target_column == SOURCE:
        return pd.Series(mapping_value, index=raw_frame.index, dtype="string")

    if mapping_value == WRDS_EVENT_TIME_EXPRESSION:
        return build_wrds_event_time(raw_frame, market_timezone=market_timezone)

    if mapping_value == WRDS_SYMBOL_EXPRESSION:
        return build_wrds_symbol(raw_frame)

    if mapping_value not in raw_frame.columns:
        raise WrdsNormalizationError(
            f"WRDS raw frame does not contain expected source column '{mapping_value}' "
            f"for normalized field '{target_column}'."
        )

    return raw_frame[mapping_value].copy()


def materialize_wrds_structured_rule(
    raw_frame: pd.DataFrame,
    *,
    target_column: str,
    mapping_rule: Mapping[str, Any],
    market_timezone: str,
) -> pd.Series:
    """Materialize one normalized column from a structured WRDS mapping rule."""

    if "literal" in mapping_rule:
        return pd.Series(str(mapping_rule["literal"]), index=raw_frame.index, dtype="string")

    expression = str(mapping_rule.get("expr", "")).strip()
    if expression == "combine_date_time":
        return build_wrds_event_time(
            raw_frame,
            market_timezone=str(mapping_rule.get("timezone", market_timezone)),
            date_column=str(mapping_rule.get("date_col", "date")),
            time_column=str(mapping_rule.get("time_col", "time_m")),
        )

    if expression == "concat_symbol":
        empty_suffix_policy = str(mapping_rule.get("empty_suffix_policy", "omit"))
        if empty_suffix_policy != "omit":
            raise WrdsNormalizationError(
                f"Unsupported empty_suffix_policy for '{target_column}': {empty_suffix_policy!r}."
            )
        return build_wrds_symbol(
            raw_frame,
            root_column=str(mapping_rule.get("root_col", "sym_root")),
            suffix_column=str(mapping_rule.get("suffix_col", "sym_suffix")),
        )

    raise WrdsNormalizationError(
        f"Unsupported WRDS mapping rule for normalized field '{target_column}': {mapping_rule!r}."
    )


def build_wrds_event_time(
    raw_frame: pd.DataFrame,
    *,
    market_timezone: str,
    date_column: str = "date",
    time_column: str = "time_m",
) -> pd.Series:
    """Build a timezone-aware event timestamp from WRDS date and time columns."""

    require_raw_columns(raw_frame, [date_column, time_column])

    date_values = raw_frame[date_column]
    time_values = raw_frame[time_column]
    if date_values.isna().any() or time_values.isna().any():
        raise WrdsNormalizationError(
            f"WRDS event-time columns '{date_column}' and '{time_column}' must not contain nulls."
        )

    timestamp_strings = date_values.astype("string") + " " + time_values.astype("string")
    timestamps = pd.to_datetime(timestamp_strings, errors="coerce", format="mixed")
    if timestamps.isna().any():
        invalid_count = int(timestamps.isna().sum())
        raise WrdsNormalizationError(
            f"Failed to parse {invalid_count} WRDS event timestamps from "
            f"'{date_column} + {time_column}'."
        )

    return timestamps.dt.tz_localize(market_timezone)


def build_wrds_symbol(
    raw_frame: pd.DataFrame,
    *,
    root_column: str = "sym_root",
    suffix_column: str = "sym_suffix",
    suffix_separator: str = ".",
) -> pd.Series:
    """Build a normalized symbol from WRDS root and suffix columns."""

    require_raw_columns(raw_frame, [root_column, suffix_column])

    symbol_root = raw_frame[root_column].astype("string")
    if symbol_root.isna().any() or (symbol_root.str.strip() == "").any():
        raise WrdsNormalizationError(
            f"WRDS symbol root column '{root_column}' must not contain null or empty values."
        )

    normalized_root = symbol_root.str.strip()
    normalized_suffix = raw_frame[suffix_column].fillna("").astype("string").str.strip()
    has_suffix = normalized_suffix != ""

    combined = normalized_root.where(~has_suffix, normalized_root + suffix_separator + normalized_suffix)
    return combined.astype("string")


def require_raw_columns(raw_frame: pd.DataFrame, required_columns: list[str]) -> None:
    """Raise when a WRDS raw frame is missing source columns needed for normalization."""

    missing_columns = [column for column in required_columns if column not in raw_frame.columns]
    if missing_columns:
        missing_list = ", ".join(missing_columns)
        raise WrdsNormalizationError(f"WRDS raw frame is missing required source columns: {missing_list}")
