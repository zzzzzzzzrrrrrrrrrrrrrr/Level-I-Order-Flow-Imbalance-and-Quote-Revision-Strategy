"""Passive-order cancellation rules for microstructure v2.1."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np

from .passive_fill import MarketArrays


@dataclass(frozen=True)
class CancellationResult:
    """Cancellation time and reason."""

    cancel_time_ns: int
    cancel_reason: str


def first_cancellation(
    *,
    side: int,
    submission_time_ns: int,
    ttl_ns: int,
    entry_spread: float,
    market: MarketArrays,
    use_microprice_cancel: bool,
    tick_size: float,
) -> CancellationResult:
    """Find first cancellation trigger strictly after submission."""

    expiry = submission_time_ns + ttl_ns
    quote_start = np.searchsorted(market.quote_time_ns, submission_time_ns, side="right")
    quote_end = np.searchsorted(market.quote_time_ns, expiry, side="right")
    candidates: list[tuple[int, str]] = [(expiry, "ttl_expired")]
    if quote_start >= quote_end:
        return CancellationResult(expiry, "ttl_expired")

    if use_microprice_cancel:
        microprice = market.microprice_gap_bps[quote_start:quote_end]
        flips = np.flatnonzero(side * microprice <= 0)
        if len(flips):
            candidates.append(
                (int(market.quote_time_ns[quote_start + flips[0]]), "microprice_flip")
            )

    qr = market.quote_revision_bps[quote_start:quote_end]
    qr_flips = np.flatnonzero(side * qr < 0)
    if len(qr_flips):
        candidates.append((int(market.quote_time_ns[quote_start + qr_flips[0]]), "qr_flip"))

    spread = market.quoted_spread[quote_start:quote_end]
    spread_widens = np.flatnonzero(spread > max(entry_spread + tick_size, entry_spread * 1.5))
    if len(spread_widens):
        candidates.append(
            (int(market.quote_time_ns[quote_start + spread_widens[0]]), "spread_widen")
        )

    qr_abs = np.abs(qr)
    if len(qr_abs):
        threshold = max(float(np.nanmedian(qr_abs)) * 5.0, 1.0)
        vol_spikes = np.flatnonzero(qr_abs > threshold)
        if len(vol_spikes):
            candidates.append(
                (int(market.quote_time_ns[quote_start + vol_spikes[0]]), "volatility_spike")
            )

    cancel_time, reason = min(candidates, key=lambda item: item[0])
    return CancellationResult(cancel_time, reason)
