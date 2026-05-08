from __future__ import annotations

from pathlib import Path

import pytest

from level1_ofi_qr.orchestration import (
    ExperimentConfigError,
    build_experiment_plan,
    load_experiment_run_config,
)
from level1_ofi_qr.orchestration.experiment import select_plan_steps

PROJECT_ROOT = Path(__file__).resolve().parents[2]
PHASE1_EXPERIMENT_CONFIG = (
    PROJECT_ROOT
    / "configs"
    / "experiments"
    / "v22_symbol_screen_phase1_by_liquidity_regime_same_20d.yaml"
)
PHASE1_DATA_CONFIG = (
    PROJECT_ROOT
    / "configs"
    / "data"
    / "phase1_wrds_12_symbols_20260313_20260410.yaml"
)


def test_load_phase1_experiment_orchestration_config() -> None:
    config = load_experiment_run_config(PHASE1_EXPERIMENT_CONFIG)

    assert config.name == "v22_symbol_screen_phase1_by_liquidity_regime_same_20d"
    assert config.default_data_config_path == PHASE1_DATA_CONFIG
    assert config.stages[0].name == "extract_wrds"
    assert config.stages[0].config_path == PHASE1_DATA_CONFIG
    assert config.stages[-1].name == "run_symbol_screen_v22"
    assert config.stages[-1].config_path == PHASE1_EXPERIMENT_CONFIG

    disabled = [stage for stage in config.stages if not stage.enabled]
    assert [stage.name for stage in disabled] == ["run_microstructure_v21"]


def test_build_experiment_plan_resolves_existing_scripts() -> None:
    config = load_experiment_run_config(PHASE1_EXPERIMENT_CONFIG)
    plan = build_experiment_plan(config, python_executable="python")

    assert plan[0].command == (
        "python",
        str(PROJECT_ROOT / "scripts" / "extract_wrds.py"),
        str(PHASE1_DATA_CONFIG),
    )
    assert plan[-1].command == (
        "python",
        str(PROJECT_ROOT / "scripts" / "run_symbol_screen_v22.py"),
        str(PHASE1_EXPERIMENT_CONFIG),
    )
    assert plan[-2].name == "run_microstructure_v21"
    assert plan[-2].enabled is False


def test_select_plan_steps_supports_stage_slices() -> None:
    config = load_experiment_run_config(PHASE1_EXPERIMENT_CONFIG)
    plan = build_experiment_plan(config, python_executable="python")

    selected = select_plan_steps(
        plan,
        from_stage="build_labels",
        to_stage="run_cost_aware_linear_score",
    )

    assert selected[0].name == "build_labels"
    assert selected[-1].name == "run_cost_aware_linear_score"
    assert "extract_wrds" not in {step.name for step in selected}


def test_select_plan_steps_rejects_unknown_boundary() -> None:
    config = load_experiment_run_config(PHASE1_EXPERIMENT_CONFIG)
    plan = build_experiment_plan(config, python_executable="python")

    with pytest.raises(ExperimentConfigError, match="Unknown --from-stage"):
        select_plan_steps(plan, from_stage="not_a_stage")
