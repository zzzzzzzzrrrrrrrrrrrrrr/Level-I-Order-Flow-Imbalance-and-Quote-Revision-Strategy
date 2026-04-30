"""Reporting, tables, and figure generation."""

from .pnl import (
    PNL_REPORTING_POLICY_NOTE,
    PnLComparisonResult,
    PnLReportingError,
    StrategyLedgerSpec,
    build_pnl_comparison,
    render_equity_svg,
    write_pnl_comparison,
)

__all__ = [
    "PNL_REPORTING_POLICY_NOTE",
    "PnLComparisonResult",
    "PnLReportingError",
    "StrategyLedgerSpec",
    "build_pnl_comparison",
    "render_equity_svg",
    "write_pnl_comparison",
]
