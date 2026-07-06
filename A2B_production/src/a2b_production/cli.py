from __future__ import annotations

import argparse
import sys

from .clean import clean_runtime
from .config import load_config
from .daily_aggregator import run_daily_aggregator
from .dashboard import run_dashboard
from .inference import run_inference
from .kafka_utils import ensure_topics, wait_for_kafka
from .lakehouse_sink import run_lakehouse_sink
from .producer import run_producer


def build_parser() -> argparse.ArgumentParser:
    """Build the command-line parser for the production A2B commands."""
    parser = argparse.ArgumentParser(prog="a2b-prod")
    parser.add_argument("--config", default=None, help="Path to YAML config")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("create-topics")
    sub.add_parser("produce-weather")
    sub.add_parser("run-inference")
    sub.add_parser("run-daily-aggregator")
    sub.add_parser("run-lakehouse-sink")
    sub.add_parser("run-dashboard")
    sub.add_parser("clean-runtime")
    return parser


def main(argv: list[str] | None = None) -> int:
    """Dispatch the requested CLI command and return its process exit code."""
    args = build_parser().parse_args(argv)
    cfg = load_config(args.config)
    if args.command == "create-topics":
        wait_for_kafka(cfg)
        ensure_topics(cfg)
        print("topics ready")
        return 0
    if args.command == "produce-weather":
        run_producer(cfg)
        return 0
    if args.command == "run-inference":
        run_inference(cfg)
        return 0
    if args.command == "run-daily-aggregator":
        run_daily_aggregator(cfg)
        return 0
    if args.command == "run-lakehouse-sink":
        run_lakehouse_sink(cfg)
        return 0
    if args.command == "run-dashboard":
        run_dashboard(cfg)
        return 0
    if args.command == "clean-runtime":
        clean_runtime(cfg)
        return 0
    raise AssertionError(args.command)


if __name__ == "__main__":
    sys.exit(main())
