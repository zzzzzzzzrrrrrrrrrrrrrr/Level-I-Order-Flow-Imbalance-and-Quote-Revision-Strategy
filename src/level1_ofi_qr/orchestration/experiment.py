"""Config-driven experiment planning for existing pipeline scripts."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import os
from pathlib import Path
import subprocess
import sys
from typing import Any, Iterable

import yaml


DATA_CONFIG_STAGES: dict[str, str] = {
    "extract_wrds": "scripts/extract_wrds.py",
    "build_dataset": "scripts/build_dataset.py",
    "build_quote_features": "scripts/build_quote_features.py",
    "align_trades": "scripts/align_trades.py",
    "sign_trades": "scripts/sign_trades.py",
    "build_signed_flow_features": "scripts/build_signed_flow_features.py",
    "build_labels": "scripts/build_labels.py",
    "build_signals": "scripts/build_signals.py",
    "run_walk_forward": "scripts/run_walk_forward.py",
    "run_threshold_selection": "scripts/run_threshold_selection.py",
    "run_cost_model": "scripts/run_cost_model.py",
    "run_execution_accounting": "scripts/run_execution_accounting.py",
    "run_target_position_accounting": "scripts/run_target_position_accounting.py",
    "run_parameter_sensitivity": "scripts/run_parameter_sensitivity.py",
    "run_tvt_parameter_selection": "scripts/run_tvt_parameter_selection.py",
    "run_backtest": "scripts/run_backtest.py",
    "run_model_training": "scripts/run_model_training.py",
    "plot_pnl": "scripts/plot_pnl.py",
    "run_cost_aware_linear_score": "scripts/run_cost_aware_linear_score.py",
    "run_microstructure_v21": "scripts/run_microstructure_v21_diagnostics.py",
}

EXPERIMENT_CONFIG_STAGES: dict[str, str] = {
    "run_symbol_screen_v22": "scripts/run_symbol_screen_v22.py",
}

STAGE_SCRIPTS: dict[str, str] = {
    **DATA_CONFIG_STAGES,
    **EXPERIMENT_CONFIG_STAGES,
}


class ExperimentConfigError(ValueError):
    """Raised when an experiment orchestration config is invalid."""


@dataclass(frozen=True)
class PipelineStageConfig:
    """One configured pipeline stage."""

    name: str
    config_path: Path
    args: tuple[str, ...] = ()
    enabled: bool = True


@dataclass(frozen=True)
class ExperimentRunConfig:
    """Parsed experiment orchestration config."""

    path: Path
    project_root: Path
    name: str
    default_data_config_path: Path | None
    stages: tuple[PipelineStageConfig, ...]


@dataclass(frozen=True)
class ExperimentPlanStep:
    """Resolved command for one experiment stage."""

    index: int
    name: str
    script_path: Path
    config_path: Path
    command: tuple[str, ...]
    enabled: bool

    def as_dict(self) -> dict[str, object]:
        """Return a JSON-serializable representation."""

        result = asdict(self)
        for key in ("script_path", "config_path"):
            result[key] = str(result[key])
        return result


def load_experiment_run_config(config_path: str | Path) -> ExperimentRunConfig:
    """Load an experiment YAML with an optional `pipeline` section.

    The orchestration layer is intentionally thin: it validates stage names,
    resolves config paths, and builds commands that call existing scripts. It
    does not change feature, label, signal, cost, or diagnostic schemas.
    """

    path = Path(config_path)
    project_root = _find_project_root(path)
    raw_config = _load_yaml_mapping(path)
    name = _experiment_name(raw_config, path=path)
    default_data_config = _default_data_config(raw_config, project_root=project_root)
    stages = _pipeline_stages(
        raw_config,
        config_path=path,
        project_root=project_root,
        default_data_config=default_data_config,
    )
    return ExperimentRunConfig(
        path=path,
        project_root=project_root,
        name=name,
        default_data_config_path=default_data_config,
        stages=stages,
    )


def build_experiment_plan(
    config: ExperimentRunConfig,
    *,
    python_executable: str | Path | None = None,
) -> tuple[ExperimentPlanStep, ...]:
    """Resolve configured stages into executable commands."""

    executable = str(python_executable or sys.executable)
    steps: list[ExperimentPlanStep] = []
    for index, stage in enumerate(config.stages, start=1):
        script_path = config.project_root / STAGE_SCRIPTS[stage.name]
        if not script_path.exists():
            raise ExperimentConfigError(f"Stage script does not exist: {script_path}")
        command = (
            executable,
            str(script_path),
            str(stage.config_path),
            *stage.args,
        )
        steps.append(
            ExperimentPlanStep(
                index=index,
                name=stage.name,
                script_path=script_path,
                config_path=stage.config_path,
                command=command,
                enabled=stage.enabled,
            )
        )
    return tuple(steps)


def select_plan_steps(
    steps: Iterable[ExperimentPlanStep],
    *,
    only_stage: str | None = None,
    from_stage: str | None = None,
    to_stage: str | None = None,
) -> tuple[ExperimentPlanStep, ...]:
    """Filter plan steps by stage names while preserving order."""

    selected = tuple(steps)
    if only_stage:
        return tuple(step for step in selected if step.name == only_stage)

    if from_stage:
        start_indexes = [step.index for step in selected if step.name == from_stage]
        if not start_indexes:
            raise ExperimentConfigError(f"Unknown --from-stage value: {from_stage}")
        selected = tuple(step for step in selected if step.index >= start_indexes[0])

    if to_stage:
        end_indexes = [step.index for step in selected if step.name == to_stage]
        if not end_indexes:
            raise ExperimentConfigError(f"Unknown --to-stage value: {to_stage}")
        selected = tuple(step for step in selected if step.index <= end_indexes[-1])

    return selected


def run_plan_steps(steps: Iterable[ExperimentPlanStep], *, cwd: str | Path) -> None:
    """Run enabled plan steps in order."""

    root = Path(cwd)
    env = _subprocess_env(project_root=root)
    for step in steps:
        if not step.enabled:
            continue
        subprocess.run(step.command, cwd=root, env=env, check=True)


def _load_yaml_mapping(path: Path) -> dict[str, object]:
    with path.open("r", encoding="utf-8") as handle:
        loaded = yaml.safe_load(handle)
    if not isinstance(loaded, dict):
        raise ExperimentConfigError(f"Experiment config must be a YAML mapping: {path}")
    return loaded


def _experiment_name(raw_config: dict[str, object], *, path: Path) -> str:
    experiment = raw_config.get("experiment", {})
    if isinstance(experiment, dict) and experiment.get("name"):
        return str(experiment["name"])
    if raw_config.get("screen_name"):
        return str(raw_config["screen_name"])
    return path.stem


def _default_data_config(
    raw_config: dict[str, object],
    *,
    project_root: Path,
) -> Path | None:
    pipeline = raw_config.get("pipeline", {})
    if isinstance(pipeline, dict) and pipeline.get("default_data_config"):
        return _resolve_project_path(pipeline["default_data_config"], project_root=project_root)

    data = raw_config.get("data", {})
    if isinstance(data, dict) and data.get("config"):
        return _resolve_project_path(data["config"], project_root=project_root)

    return None


def _pipeline_stages(
    raw_config: dict[str, object],
    *,
    config_path: Path,
    project_root: Path,
    default_data_config: Path | None,
) -> tuple[PipelineStageConfig, ...]:
    pipeline = raw_config.get("pipeline", {})
    if not isinstance(pipeline, dict) or "stages" not in pipeline:
        return (
            PipelineStageConfig(
                name="run_symbol_screen_v22",
                config_path=config_path,
                args=(),
                enabled=True,
            ),
        )

    raw_stages = pipeline["stages"]
    if not isinstance(raw_stages, list) or not raw_stages:
        raise ExperimentConfigError("pipeline.stages must be a non-empty list.")

    stages = []
    for raw_stage in raw_stages:
        stages.append(
            _parse_stage(
                raw_stage,
                experiment_config_path=config_path,
                project_root=project_root,
                default_data_config=default_data_config,
            )
        )
    return tuple(stages)


def _parse_stage(
    raw_stage: object,
    *,
    experiment_config_path: Path,
    project_root: Path,
    default_data_config: Path | None,
) -> PipelineStageConfig:
    if isinstance(raw_stage, str):
        name = raw_stage
        raw_config_path = None
        raw_args: object = []
        enabled = True
    elif isinstance(raw_stage, dict):
        name = str(raw_stage.get("name", ""))
        raw_config_path = (
            raw_stage.get("config")
            or raw_stage.get("data_config")
            or raw_stage.get("experiment_config")
        )
        raw_args = raw_stage.get("args", [])
        enabled = bool(raw_stage.get("enabled", True))
    else:
        raise ExperimentConfigError(f"Unsupported pipeline stage entry: {raw_stage!r}")

    if name not in STAGE_SCRIPTS:
        allowed = ", ".join(sorted(STAGE_SCRIPTS))
        raise ExperimentConfigError(f"Unknown pipeline stage '{name}'. Allowed: {allowed}.")

    args = _parse_args(raw_args, stage_name=name)
    config_path = _stage_config_path(
        name,
        raw_config_path=raw_config_path,
        experiment_config_path=experiment_config_path,
        project_root=project_root,
        default_data_config=default_data_config,
    )
    if not config_path.exists():
        raise ExperimentConfigError(f"Stage config does not exist for {name}: {config_path}")

    return PipelineStageConfig(
        name=name,
        config_path=config_path,
        args=args,
        enabled=enabled,
    )


def _stage_config_path(
    name: str,
    *,
    raw_config_path: object,
    experiment_config_path: Path,
    project_root: Path,
    default_data_config: Path | None,
) -> Path:
    if raw_config_path:
        return _resolve_project_path(raw_config_path, project_root=project_root)
    if name in EXPERIMENT_CONFIG_STAGES:
        return experiment_config_path
    if default_data_config is None:
        raise ExperimentConfigError(
            f"Stage '{name}' requires a data config. Set data.config, "
            "pipeline.default_data_config, or stage.config."
        )
    return default_data_config


def _parse_args(raw_args: object, *, stage_name: str) -> tuple[str, ...]:
    if raw_args is None:
        return ()
    if isinstance(raw_args, str):
        raise ExperimentConfigError(
            f"Stage '{stage_name}' args must be a YAML list, not a shell string."
        )
    if not isinstance(raw_args, list):
        raise ExperimentConfigError(f"Stage '{stage_name}' args must be a YAML list.")
    return tuple(str(value) for value in raw_args)


def _resolve_project_path(value: object, *, project_root: Path) -> Path:
    path = Path(str(value))
    if path.is_absolute():
        return path
    return project_root / path


def _find_project_root(path: Path) -> Path:
    start = path.resolve().parent
    for candidate in (start, *start.parents):
        if (candidate / "pyproject.toml").exists():
            return candidate
    raise ExperimentConfigError(f"Could not find project root for config: {path}")


def _subprocess_env(*, project_root: Path) -> dict[str, str]:
    env = os.environ.copy()
    src_root = str(project_root / "src")
    current = env.get("PYTHONPATH")
    if current:
        env["PYTHONPATH"] = src_root + os.pathsep + current
    else:
        env["PYTHONPATH"] = src_root
    return env
