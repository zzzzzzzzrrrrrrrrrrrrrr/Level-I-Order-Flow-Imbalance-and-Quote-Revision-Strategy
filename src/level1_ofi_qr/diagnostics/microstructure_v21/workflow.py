"""Workflow for microstructure v2.1 passive/hybrid diagnostics."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict, dataclass
import json
from itertools import product
from pathlib import Path

import numpy as np
import pandas as pd

from ...alignment import TRADING_DATE
from ...features.quotes import (
    MIDQUOTE,
    QUOTE_REVISION_BPS,
    QUOTED_SPREAD,
)
from ...models import COST_AWARE_LINEAR_SCORE_STRATEGY, MODEL_SCORE_COLUMN
from ...schema import ASK, ASK_SIZE, BID, BID_SIZE, EVENT_TIME, SYMBOL
from ...utils import DataSliceConfig
from .candidate_pool import (
    attach_quote_state,
    candidate_pool_mask,
    edge_threshold_mask,
    microprice,
    spread_thresholds,
)
from .cancellation import first_cancellation
from .config import MicrostructureV21Config, MicrostructureV21Variant
from .execution_selector import select_execution_mode
from .metrics import summarize_orders
from .passive_fill import (
    MarketArrays,
    find_passive_fill,
    market_entry_price,
    market_exit_price,
    passive_exit_price,
    quote_index_at_or_after,
    quote_index_at_or_before,
    to_ns,
)


class MicrostructureV21WorkflowError(ValueError):
    """Raised when v2.1 diagnostics cannot be built."""


@dataclass(frozen=True)
class MicrostructureV21OutputPaths:
    """Output paths for v2.1 diagnostics."""

    candidate_events_csv_path: Path
    orders_csv_path: Path
    variant_daily_metrics_csv_path: Path
    variant_summary_csv_path: Path
    validation_selection_csv_path: Path
    selected_test_metrics_csv_path: Path
    manifest_path: Path


@dataclass(frozen=True)
class MicrostructureV21BuildResult:
    """Result tables and paths for v2.1 diagnostics."""

    candidate_events: pd.DataFrame
    orders: pd.DataFrame
    variant_daily_metrics: pd.DataFrame
    variant_summary: pd.DataFrame
    validation_selection: pd.DataFrame
    selected_test_metrics: pd.DataFrame
    paths: MicrostructureV21OutputPaths


def build_microstructure_v21_diagnostics(
    config: DataSliceConfig,
    *,
    processed_dir: str | Path | None = None,
    output_dir: str | Path | None = None,
    diagnostics_config: MicrostructureV21Config = MicrostructureV21Config(),
) -> MicrostructureV21BuildResult:
    """Build independent v2.1 diagnostics without changing v1/v2.0 outputs."""

    _validate_config(diagnostics_config)
    root = Path(processed_dir or config.storage["processed_dir"]) / config.slice_name
    prediction_path = root / f"{config.slice_name}_{COST_AWARE_LINEAR_SCORE_STRATEGY}_predictions.csv"
    quote_path = root / f"{config.slice_name}_quote_features_v1.csv"
    trade_path = root / f"{config.slice_name}_trades_signed_v1.csv"
    for path in (prediction_path, quote_path, trade_path):
        if not path.exists():
            raise MicrostructureV21WorkflowError(f"Required input is missing: {path}")

    quote_state = _read_quote_state(quote_path)
    candidate_events = _read_prediction_candidate_events(prediction_path)
    candidate_events = attach_quote_state(candidate_events, quote_state)
    candidate_events = _finalize_candidate_events(
        candidate_events,
        tick_size=diagnostics_config.tick_size,
    )
    market_by_group = _build_market_arrays(quote_state, _read_trade_state(trade_path))
    variants = _variant_grid(diagnostics_config)
    orders = _evaluate_variants(
        candidate_events,
        variants=variants,
        market_by_group=market_by_group,
        diagnostics_config=diagnostics_config,
    )
    variant_daily_metrics = summarize_orders(
        orders,
        group_columns=(
            "variant_id",
            "candidate_pool",
            "edge_threshold",
            "microprice_usage",
            "ttl",
            "queue_haircut",
            "execution_variant",
            TRADING_DATE,
        ),
    )
    variant_summary = summarize_orders(
        orders,
        group_columns=(
            "variant_id",
            "candidate_pool",
            "edge_threshold",
            "microprice_usage",
            "ttl",
            "queue_haircut",
            "execution_variant",
        ),
    )
    validation_selection = _select_variants_chronologically(
        variant_daily_metrics,
        diagnostics_config=diagnostics_config,
    )
    selected_test_metrics = _selected_test_metrics(variant_daily_metrics, validation_selection)
    paths = _write_outputs(
        config,
        candidate_events=candidate_events,
        orders=orders,
        variant_daily_metrics=variant_daily_metrics,
        variant_summary=variant_summary,
        validation_selection=validation_selection,
        selected_test_metrics=selected_test_metrics,
        diagnostics_config=diagnostics_config,
        inputs={
            "prediction_path": prediction_path,
            "quote_path": quote_path,
            "trade_path": trade_path,
        },
        output_dir=output_dir or processed_dir,
    )
    return MicrostructureV21BuildResult(
        candidate_events=candidate_events,
        orders=orders,
        variant_daily_metrics=variant_daily_metrics,
        variant_summary=variant_summary,
        validation_selection=validation_selection,
        selected_test_metrics=selected_test_metrics,
        paths=paths,
    )


def _read_prediction_candidate_events(path: Path) -> pd.DataFrame:
    usecols = [
        EVENT_TIME,
        SYMBOL,
        TRADING_DATE,
        "fold_id",
        "test_date",
        "selected_threshold",
        MODEL_SCORE_COLUMN,
        "cost_aware_estimated_cost_bps",
        "signal_midquote",
        "signal_quoted_spread",
    ]
    frames = []
    previous_side: dict[tuple[str, str], int] = {}
    for chunk in pd.read_csv(path, usecols=usecols, chunksize=500_000):
        chunk[EVENT_TIME] = pd.to_datetime(chunk[EVENT_TIME], format="mixed")
        chunk = chunk.sort_values([SYMBOL, TRADING_DATE, EVENT_TIME], kind="mergesort")
        score = pd.to_numeric(chunk[MODEL_SCORE_COLUMN], errors="coerce")
        finite_score = score.where(np.isfinite(score), 0.0)
        cost = pd.to_numeric(chunk["cost_aware_estimated_cost_bps"], errors="coerce").fillna(0.0)
        edge = finite_score.abs() - cost
        side = np.sign(finite_score).astype(int)
        chunk["predicted_edge_bps"] = finite_score.abs()
        chunk["expected_cost_bps"] = cost
        chunk["tradable_edge_bps"] = edge
        chunk["side"] = side
        chunk["desired_side"] = chunk["side"].where(edge > 0, 0)
        masks = []
        for key, group in chunk.groupby([SYMBOL, TRADING_DATE], sort=False):
            previous = previous_side.get((str(key[0]), str(key[1])), 0)
            desired = group["desired_side"]
            shifted = desired.shift(fill_value=previous)
            mask = (desired != 0) & (desired != shifted)
            masks.append(mask)
            previous_side[(str(key[0]), str(key[1]))] = int(desired.iloc[-1])
        if masks:
            chunk_mask = pd.concat(masks).sort_index()
            selected = chunk.loc[chunk_mask].copy()
            if not selected.empty:
                frames.append(selected)
    if not frames:
        return pd.DataFrame()
    result = pd.concat(frames, ignore_index=True)
    result["signal_id"] = [f"v21_signal_{index:08d}" for index in range(1, len(result) + 1)]
    result["selected_threshold_numeric"] = pd.to_numeric(
        result["selected_threshold"],
        errors="coerce",
    )
    return result


def _read_quote_state(path: Path) -> pd.DataFrame:
    usecols = [
        EVENT_TIME,
        SYMBOL,
        TRADING_DATE,
        BID,
        ASK,
        BID_SIZE,
        ASK_SIZE,
        MIDQUOTE,
        QUOTED_SPREAD,
        QUOTE_REVISION_BPS,
    ]
    rows = pd.read_csv(path, usecols=usecols)
    rows[EVENT_TIME] = pd.to_datetime(rows[EVENT_TIME], format="mixed")
    rows["microprice"] = microprice(rows[BID], rows[ASK], rows[BID_SIZE], rows[ASK_SIZE])
    rows["microprice_gap_bps"] = (rows["microprice"] - rows[MIDQUOTE]) / rows[MIDQUOTE] * 10000.0
    return rows.sort_values([SYMBOL, TRADING_DATE, EVENT_TIME], kind="mergesort")


def _read_trade_state(path: Path) -> pd.DataFrame:
    usecols = [EVENT_TIME, SYMBOL, TRADING_DATE, "trade_price"]
    rows = pd.read_csv(path, usecols=usecols)
    rows[EVENT_TIME] = pd.to_datetime(rows[EVENT_TIME], format="mixed")
    return rows.sort_values([SYMBOL, TRADING_DATE, EVENT_TIME], kind="mergesort")


def _finalize_candidate_events(rows: pd.DataFrame, *, tick_size: float) -> pd.DataFrame:
    result = rows.copy()
    result["midquote"] = pd.to_numeric(result[MIDQUOTE], errors="coerce")
    result["quoted_spread"] = pd.to_numeric(result[QUOTED_SPREAD], errors="coerce")
    result["bid_size"] = pd.to_numeric(result[BID_SIZE], errors="coerce")
    result["ask_size"] = pd.to_numeric(result[ASK_SIZE], errors="coerce")
    result["displayed_depth"] = result["bid_size"] + result["ask_size"]
    result["microprice_gap"] = result["microprice"] - result["midquote"]
    result["microprice_aligned"] = result["side"] * result["microprice_gap_bps"] > 0
    result["one_tick_spread"] = result["quoted_spread"] <= tick_size + 1e-12
    result["candidate_signal"] = True
    return result.dropna(subset=["midquote", "quoted_spread", BID, ASK]).reset_index(drop=True)


def _build_market_arrays(
    quote_state: pd.DataFrame,
    trade_state: pd.DataFrame,
) -> dict[tuple[str, str], MarketArrays]:
    result: dict[tuple[str, str], MarketArrays] = {}
    trade_groups = {
        (str(key[0]), str(key[1])): group
        for key, group in trade_state.groupby([SYMBOL, TRADING_DATE], sort=False)
    }
    for key, quotes in quote_state.groupby([SYMBOL, TRADING_DATE], sort=False):
        normalized_key = (str(key[0]), str(key[1]))
        trades = trade_groups.get(normalized_key, pd.DataFrame(columns=[EVENT_TIME, "trade_price"]))
        result[normalized_key] = MarketArrays(
            quote_time_ns=quotes[EVENT_TIME].astype("int64").to_numpy(),
            bid=pd.to_numeric(quotes[BID], errors="coerce").to_numpy(dtype="float64"),
            ask=pd.to_numeric(quotes[ASK], errors="coerce").to_numpy(dtype="float64"),
            midquote=pd.to_numeric(quotes[MIDQUOTE], errors="coerce").to_numpy(dtype="float64"),
            microprice_gap_bps=pd.to_numeric(
                quotes["microprice_gap_bps"],
                errors="coerce",
            ).to_numpy(dtype="float64"),
            quote_revision_bps=pd.to_numeric(
                quotes[QUOTE_REVISION_BPS],
                errors="coerce",
            ).fillna(0.0).to_numpy(dtype="float64"),
            quoted_spread=pd.to_numeric(
                quotes[QUOTED_SPREAD],
                errors="coerce",
            ).to_numpy(dtype="float64"),
            trade_time_ns=trades[EVENT_TIME].astype("int64").to_numpy(),
            trade_price=pd.to_numeric(
                trades["trade_price"],
                errors="coerce",
            ).to_numpy(dtype="float64"),
        )
    return result


def _variant_grid(config: MicrostructureV21Config) -> tuple[MicrostructureV21Variant, ...]:
    return tuple(
        MicrostructureV21Variant(*values)
        for values in product(
            config.candidate_pools,
            config.edge_thresholds,
            config.microprice_usages,
            config.ttl_values,
            config.queue_haircuts,
            config.execution_variants,
        )
    )


def _evaluate_variants(
    candidates: pd.DataFrame,
    *,
    variants: tuple[MicrostructureV21Variant, ...],
    market_by_group: dict[tuple[str, str], MarketArrays],
    diagnostics_config: MicrostructureV21Config,
) -> pd.DataFrame:
    rows = []
    if candidates.empty:
        return pd.DataFrame()
    candidates = candidates.reset_index(drop=True)
    global_thresholds = spread_thresholds(candidates)
    pool_masks = {
        pool: candidate_pool_mask(
            candidates,
            candidate_pool=pool,
            thresholds=global_thresholds,
            tick_size=diagnostics_config.tick_size,
            min_depth=diagnostics_config.min_depth,
        )
        .fillna(False)
        .to_numpy(dtype=bool)
        for pool in diagnostics_config.candidate_pools
    }
    edge_masks = {
        edge: edge_threshold_mask(candidates, edge_threshold=edge)
        .fillna(False)
        .to_numpy(dtype=bool)
        for edge in diagnostics_config.edge_thresholds
    }
    combo_masks: dict[tuple[str, str], np.ndarray] = {}
    variants_by_simulation: dict[
        tuple[str, str, str, str],
        list[MicrostructureV21Variant],
    ] = defaultdict(list)
    for variant in variants:
        key = (
            variant.microprice_usage,
            variant.ttl,
            variant.queue_haircut,
            variant.execution_variant,
        )
        variants_by_simulation[key].append(variant)

    for (
        microprice_usage,
        ttl,
        queue_haircut,
        execution_variant,
    ), simulation_variants in variants_by_simulation.items():
        union_mask = np.zeros(len(candidates), dtype=bool)
        for variant in simulation_variants:
            combo_key = (variant.candidate_pool, variant.edge_threshold)
            if combo_key not in combo_masks:
                combo_masks[combo_key] = (
                    pool_masks[variant.candidate_pool] & edge_masks[variant.edge_threshold]
                )
            union_mask |= combo_masks[combo_key]
        if not union_mask.any():
            continue
        ttl_ns = int(pd.Timedelta(ttl).value)
        use_microprice_cancel = (
            microprice_usage in {"cancellation_only", "leaning_or_adverse_selection_score"}
            or "cancel_on_microprice_flip" in execution_variant
        )
        simulation_variant = MicrostructureV21Variant(
            candidate_pool="simulation",
            edge_threshold="simulation",
            microprice_usage=microprice_usage,
            ttl=ttl,
            queue_haircut=queue_haircut,
            execution_variant=execution_variant,
        )
        subset = candidates.loc[union_mask].copy()
        subset["candidate_index"] = np.flatnonzero(union_mask)
        for row in subset.itertuples(index=False):
            row_dict = row._asdict()
            candidate_index = int(row_dict.pop("candidate_index"))
            mode = select_execution_mode(
                pd.Series(row_dict),
                edge_threshold_passed=True,
                microprice_usage=microprice_usage,
                adverse_selection_buffer_bps=diagnostics_config.adverse_selection_buffer_bps,
                safety_margin_bps=diagnostics_config.market_safety_margin_bps,
            )
            if mode == "no_trade":
                continue
            order = _simulate_order(
                row_dict,
                variant=simulation_variant,
                mode=mode,
                ttl_ns=ttl_ns,
                use_microprice_cancel=use_microprice_cancel,
                market_by_group=market_by_group,
                diagnostics_config=diagnostics_config,
            )
            if order is not None:
                for variant in simulation_variants:
                    if not combo_masks[(variant.candidate_pool, variant.edge_threshold)][
                        candidate_index
                    ]:
                        continue
                    expanded = order.copy()
                    expanded.update(
                        {
                            "variant_id": variant.variant_id,
                            "candidate_pool": variant.candidate_pool,
                            "edge_threshold": variant.edge_threshold,
                            "microprice_usage": variant.microprice_usage,
                            "ttl": variant.ttl,
                            "queue_haircut": variant.queue_haircut,
                            "execution_variant": variant.execution_variant,
                        }
                    )
                    rows.append(expanded)
    return pd.DataFrame(rows)


def _simulate_order(
    row: dict[str, object],
    *,
    variant: MicrostructureV21Variant,
    mode: str,
    ttl_ns: int,
    use_microprice_cancel: bool,
    market_by_group: dict[tuple[str, str], MarketArrays],
    diagnostics_config: MicrostructureV21Config,
) -> dict[str, object] | None:
    key = (str(row[SYMBOL]), str(row[TRADING_DATE]))
    market = market_by_group.get(key)
    if market is None:
        return None
    side = int(row["side"])
    submission_ns = to_ns(row[EVENT_TIME])
    quote_index = quote_index_at_or_before(submission_ns, market)
    if quote_index is None:
        return None
    entry_mid = float(market.midquote[quote_index])
    entry_spread = float(market.quoted_spread[quote_index])
    cancel = first_cancellation(
        side=side,
        submission_time_ns=submission_ns,
        ttl_ns=ttl_ns,
        entry_spread=entry_spread,
        market=market,
        use_microprice_cancel=use_microprice_cancel,
        tick_size=diagnostics_config.tick_size,
    )
    if mode == "market_entry":
        entry_price = market_entry_price(side=side, quote_index=quote_index, market=market)
        fill_time_ns = submission_ns
        fill_evidence = "market_entry"
        filled = True
    else:
        limit_price = float(market.bid[quote_index] if side > 0 else market.ask[quote_index])
        fill = find_passive_fill(
            side=side,
            submission_time_ns=submission_ns,
            limit_price=limit_price,
            cancel_time_ns=cancel.cancel_time_ns,
            market=market,
            queue_haircut=variant.queue_haircut,
            tick_size=diagnostics_config.tick_size,
        )
        filled = fill.filled
        fill_time_ns = fill.fill_time_ns
        entry_price = float(fill.fill_price) if fill.fill_price is not None else None
        fill_evidence = fill.fill_evidence
    base = _base_order(row, variant=variant, mode=mode)
    base.update(
        {
            "submitted": True,
            "filled": filled,
            "submission_time_ns": submission_ns,
            "cancel_time_ns": cancel.cancel_time_ns,
            "cancel_reason": cancel.cancel_reason,
            "entry_price": entry_price,
            "entry_midquote": entry_mid,
            "fill_time_ns": fill_time_ns,
            "fill_evidence": fill_evidence,
        }
    )
    if not filled or fill_time_ns is None or entry_price is None:
        base.update(_unfilled_fields(side, submission_ns, ttl_ns, entry_mid, market))
        return base
    base.update(
        _filled_fields(
            side=side,
            fill_time_ns=fill_time_ns,
            entry_price=entry_price,
            entry_mid=entry_mid,
            ttl_ns=ttl_ns,
            market=market,
            execution_variant=variant.execution_variant,
            queue_haircut=variant.queue_haircut,
            tick_size=diagnostics_config.tick_size,
            post_fill_horizons=diagnostics_config.post_fill_horizons,
        )
    )
    return base


def _base_order(
    row: dict[str, object],
    *,
    variant: MicrostructureV21Variant,
    mode: str,
) -> dict[str, object]:
    return {
        "variant_id": variant.variant_id,
        "candidate_pool": variant.candidate_pool,
        "edge_threshold": variant.edge_threshold,
        "microprice_usage": variant.microprice_usage,
        "ttl": variant.ttl,
        "queue_haircut": variant.queue_haircut,
        "execution_variant": variant.execution_variant,
        "signal_id": row["signal_id"],
        EVENT_TIME: row[EVENT_TIME],
        SYMBOL: row[SYMBOL],
        TRADING_DATE: row[TRADING_DATE],
        "fold_id": row.get("fold_id"),
        "side": row["side"],
        "candidate_signal": True,
        "execution_mode": mode,
        "predicted_edge_bps": row["predicted_edge_bps"],
        "expected_cost_bps": row["expected_cost_bps"],
        "tradable_edge_bps": row["tradable_edge_bps"],
        "microprice_gap_bps": row["microprice_gap_bps"],
        "quoted_spread": row["quoted_spread"],
    }


def _unfilled_fields(
    side: int,
    submission_ns: int,
    ttl_ns: int,
    entry_mid: float,
    market: MarketArrays,
) -> dict[str, object]:
    exit_index = quote_index_at_or_after(submission_ns + ttl_ns, market)
    if exit_index is None:
        opportunity = 0.0
    else:
        opportunity = max(side * (float(market.midquote[exit_index]) - entry_mid), 0.0)
    return {
        "exit_time_ns": None,
        "exit_price": None,
        "exit_midquote": None,
        "gross_pnl": 0.0,
        "cost": 0.0,
        "net_pnl": 0.0,
        "realized_spread_bps": None,
        "adverse_selection_bps": None,
        "unfilled_opportunity_cost": opportunity,
        **{f"post_fill_mid_move_{label}_bps": None for label in ("100ms", "500ms", "1s", "5s")},
    }


def _filled_fields(
    *,
    side: int,
    fill_time_ns: int,
    entry_price: float,
    entry_mid: float,
    ttl_ns: int,
    market: MarketArrays,
    execution_variant: str,
    queue_haircut: str,
    tick_size: float,
    post_fill_horizons: tuple[str, ...],
) -> dict[str, object]:
    fill_index = quote_index_at_or_before(fill_time_ns, market)
    if fill_index is None:
        fill_index = 0
    exit_time_ns = fill_time_ns + ttl_ns
    exit_index = quote_index_at_or_after(exit_time_ns, market)
    if exit_index is None:
        exit_index = len(market.quote_time_ns) - 1
    passive_exit = execution_variant != "passive_entry_market_exit"
    if passive_exit:
        limit_price = passive_exit_price(side=side, quote_index=fill_index, market=market)
        exit_fill = find_passive_fill(
            side=-side,
            submission_time_ns=fill_time_ns,
            limit_price=limit_price,
            cancel_time_ns=int(market.quote_time_ns[exit_index]),
            market=market,
            queue_haircut=queue_haircut,
            tick_size=tick_size,
        )
        if exit_fill.filled and exit_fill.fill_time_ns is not None and exit_fill.fill_price is not None:
            exit_price = float(exit_fill.fill_price)
            exit_time_ns = exit_fill.fill_time_ns
            exit_index = quote_index_at_or_before(exit_time_ns, market) or exit_index
        else:
            exit_price = market_exit_price(side=side, quote_index=exit_index, market=market)
    else:
        exit_price = market_exit_price(side=side, quote_index=exit_index, market=market)
    exit_mid = float(market.midquote[exit_index])
    gross = side * (exit_mid - entry_mid)
    net = side * (float(exit_price) - float(entry_price))
    fields = {
        "exit_time_ns": exit_time_ns,
        "exit_price": float(exit_price),
        "exit_midquote": exit_mid,
        "gross_pnl": gross,
        "cost": gross - net,
        "net_pnl": net,
        "realized_spread_bps": side * (entry_mid - float(entry_price)) / entry_mid * 10000.0,
        "unfilled_opportunity_cost": 0.0,
    }
    for label in post_fill_horizons:
        horizon_index = quote_index_at_or_after(fill_time_ns + int(pd.Timedelta(label).value), market)
        if horizon_index is None:
            move = None
        else:
            move = side * (float(market.midquote[horizon_index]) - entry_mid) / entry_mid * 10000.0
        fields[f"post_fill_mid_move_{label}_bps"] = move
    fields["adverse_selection_bps"] = (
        None
        if fields.get("post_fill_mid_move_500ms_bps") is None
        else -float(fields["post_fill_mid_move_500ms_bps"])
    )
    return fields


def _select_variants_chronologically(
    daily_metrics: pd.DataFrame,
    *,
    diagnostics_config: MicrostructureV21Config,
) -> pd.DataFrame:
    if daily_metrics.empty:
        return pd.DataFrame()
    dates = sorted(str(value) for value in daily_metrics[TRADING_DATE].dropna().unique())
    rows = []
    for test_date_index in range(diagnostics_config.validation_min_dates, len(dates)):
        test_date = dates[test_date_index]
        validation_dates = dates[:test_date_index]
        validation = daily_metrics.loc[daily_metrics[TRADING_DATE].astype(str).isin(validation_dates)]
        if validation.empty:
            continue
        grouped = validation.groupby("variant_id", sort=False)[
            diagnostics_config.validation_objective
        ].mean()
        selected_variant = str(grouped.idxmax())
        rows.append(
            {
                "test_date": test_date,
                "selected_variant_id": selected_variant,
                "validation_dates": ";".join(validation_dates),
                "validation_objective": diagnostics_config.validation_objective,
                "validation_objective_value": float(grouped.loc[selected_variant]),
                "test_used_for_selection": False,
            }
        )
    return pd.DataFrame(rows)


def _selected_test_metrics(
    daily_metrics: pd.DataFrame,
    selection: pd.DataFrame,
) -> pd.DataFrame:
    if daily_metrics.empty or selection.empty:
        return pd.DataFrame()
    rows = []
    for _, selected in selection.iterrows():
        match = daily_metrics.loc[
            (daily_metrics[TRADING_DATE].astype(str) == str(selected["test_date"]))
            & (daily_metrics["variant_id"] == selected["selected_variant_id"])
        ]
        if match.empty:
            continue
        row = match.iloc[0].to_dict()
        row.update(selected.to_dict())
        rows.append(row)
    return pd.DataFrame(rows)


def _write_outputs(
    config: DataSliceConfig,
    *,
    candidate_events: pd.DataFrame,
    orders: pd.DataFrame,
    variant_daily_metrics: pd.DataFrame,
    variant_summary: pd.DataFrame,
    validation_selection: pd.DataFrame,
    selected_test_metrics: pd.DataFrame,
    diagnostics_config: MicrostructureV21Config,
    inputs: dict[str, Path],
    output_dir: str | Path | None,
) -> MicrostructureV21OutputPaths:
    output_root = Path(output_dir or config.storage["processed_dir"]) / config.slice_name
    output_root.mkdir(parents=True, exist_ok=True)
    stem = f"{config.slice_name}_microstructure_v21"
    paths = MicrostructureV21OutputPaths(
        candidate_events_csv_path=output_root / f"{stem}_candidate_events.csv",
        orders_csv_path=output_root / f"{stem}_orders.csv",
        variant_daily_metrics_csv_path=output_root / f"{stem}_variant_daily_metrics.csv",
        variant_summary_csv_path=output_root / f"{stem}_variant_summary.csv",
        validation_selection_csv_path=output_root / f"{stem}_validation_selection.csv",
        selected_test_metrics_csv_path=output_root / f"{stem}_selected_test_metrics.csv",
        manifest_path=output_root / f"{stem}_manifest.json",
    )
    candidate_events.to_csv(paths.candidate_events_csv_path, index=False)
    orders.to_csv(paths.orders_csv_path, index=False)
    variant_daily_metrics.to_csv(paths.variant_daily_metrics_csv_path, index=False)
    variant_summary.to_csv(paths.variant_summary_csv_path, index=False)
    validation_selection.to_csv(paths.validation_selection_csv_path, index=False)
    selected_test_metrics.to_csv(paths.selected_test_metrics_csv_path, index=False)
    manifest = {
        "slice_name": config.slice_name,
        "diagnostic_version": "microstructure_v21",
        "diagnostic_only": True,
        "v1_outputs_modified": False,
        "v20_outputs_modified": False,
        "test_used_for_selection": False,
        "inputs": {key: str(value) for key, value in inputs.items()},
        "outputs": {key: str(value) for key, value in asdict(paths).items()},
        "config": asdict(diagnostics_config),
        "row_counts": {
            "candidate_events": len(candidate_events),
            "orders": len(orders),
            "variant_daily_metrics": len(variant_daily_metrics),
            "variant_summary": len(variant_summary),
            "validation_selection": len(validation_selection),
            "selected_test_metrics": len(selected_test_metrics),
        },
    }
    paths.manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return paths


def _validate_config(config: MicrostructureV21Config) -> None:
    if config.validation_min_dates < 1:
        raise MicrostructureV21WorkflowError("validation_min_dates must be positive.")
    if config.tick_size <= 0:
        raise MicrostructureV21WorkflowError("tick_size must be positive.")
    for ttl in config.ttl_values:
        pd.Timedelta(ttl)
