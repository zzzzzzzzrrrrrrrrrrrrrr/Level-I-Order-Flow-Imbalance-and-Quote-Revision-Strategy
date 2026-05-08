"""Command-line entry point for config-driven experiment orchestration."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from level1_ofi_qr.orchestration.experiment import (
    build_experiment_plan,
    load_experiment_run_config,
    run_plan_steps,
    select_plan_steps,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Build or execute a YAML-driven experiment plan. The default mode "
            "prints the resolved commands without running them."
        )
    )
    parser.add_argument("config", help="Path to an experiment YAML config.")
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Run enabled stages. Omit this flag to print a dry-run plan.",
    )
    parser.add_argument(
        "--python-executable",
        default="D:\\python_library_envs\\VHFT_lab\\python.exe",
        help="Python executable used in resolved stage commands.",
    )
    parser.add_argument("--only-stage", help="Plan or execute only one named stage.")
    parser.add_argument("--from-stage", help="Start at this named stage.")
    parser.add_argument("--to-stage", help="Stop after this named stage.")
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print the dry-run plan as JSON.",
    )
    args = parser.parse_args()

    config = load_experiment_run_config(args.config)
    plan = build_experiment_plan(config, python_executable=args.python_executable)
    selected = select_plan_steps(
        plan,
        only_stage=args.only_stage,
        from_stage=args.from_stage,
        to_stage=args.to_stage,
    )

    if args.json:
        print(json.dumps([step.as_dict() for step in selected], indent=2))
    else:
        _print_plan(config.name, selected)

    if args.execute:
        run_plan_steps(selected, cwd=Path(config.project_root))


def _print_plan(experiment_name: str, steps: tuple[object, ...]) -> None:
    print(f"experiment={experiment_name}")
    print(f"stage_count={len(steps)}")
    for step in steps:
        status = "enabled" if step.enabled else "disabled"
        print(f"[{step.index}] {step.name} ({status})")
        print(f"  config={step.config_path}")
        print(f"  command={_format_command(step.command)}")


def _format_command(command: tuple[str, ...]) -> str:
    return " ".join(_quote_if_needed(part) for part in command)


def _quote_if_needed(value: str) -> str:
    if not value or any(character.isspace() for character in value):
        return f'"{value}"'
    return value


if __name__ == "__main__":
    main()
