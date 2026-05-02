"""Configuration objects for microstructure v2.1 diagnostics."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final


DEFAULT_CANDIDATE_POOLS: Final[tuple[str, ...]] = (
    "spread_q1",
    "spread_q1_or_q2",
    "one_tick_spread",
    "one_tick_spread_with_min_depth",
)
DEFAULT_EDGE_THRESHOLDS: Final[tuple[str, ...]] = (
    "edge_gt_0",
    "edge_gt_0p25",
    "edge_gt_0p50",
    "existing_threshold",
)
DEFAULT_MICROPRICE_USAGES: Final[tuple[str, ...]] = (
    "entry_gate",
    "cancellation_only",
    "leaning_or_adverse_selection_score",
)
DEFAULT_TTLS: Final[tuple[str, ...]] = ("100ms", "500ms", "1s", "5s")
DEFAULT_QUEUE_HAIRCUTS: Final[tuple[str, ...]] = ("conservative", "base", "optimistic")
DEFAULT_EXECUTION_VARIANTS: Final[tuple[str, ...]] = (
    "passive_entry_market_exit",
    "passive_entry_passive_first_exit_with_timeout",
    "passive_entry_cancel_on_microprice_flip_passive_first_exit",
)


@dataclass(frozen=True)
class MicrostructureV21Config:
    """Config for v2.1 frequency-expansion passive/hybrid diagnostics."""

    candidate_pools: tuple[str, ...] = DEFAULT_CANDIDATE_POOLS
    edge_thresholds: tuple[str, ...] = DEFAULT_EDGE_THRESHOLDS
    microprice_usages: tuple[str, ...] = DEFAULT_MICROPRICE_USAGES
    ttl_values: tuple[str, ...] = DEFAULT_TTLS
    queue_haircuts: tuple[str, ...] = DEFAULT_QUEUE_HAIRCUTS
    execution_variants: tuple[str, ...] = DEFAULT_EXECUTION_VARIANTS
    tick_size: float = 0.01
    min_depth: float = 200.0
    market_safety_margin_bps: float = 0.0
    adverse_selection_buffer_bps: float = 0.5
    validation_min_dates: int = 2
    validation_objective: str = "net_pnl_per_submitted_order"
    post_fill_horizons: tuple[str, ...] = ("100ms", "500ms", "1s", "5s")


@dataclass(frozen=True)
class MicrostructureV21Variant:
    """One diagnostic variant in the v2.1 grid."""

    candidate_pool: str
    edge_threshold: str
    microprice_usage: str
    ttl: str
    queue_haircut: str
    execution_variant: str

    @property
    def variant_id(self) -> str:
        return "|".join(
            (
                self.candidate_pool,
                self.edge_threshold,
                self.microprice_usage,
                self.ttl,
                self.queue_haircut,
                self.execution_variant,
            )
        )
