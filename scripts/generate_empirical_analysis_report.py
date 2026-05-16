from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from export.empirical_report import write_empirical_analysis_report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate empirical market time structure report.")
    parser.add_argument("--output-dir", required=True, help="Research output directory.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    report_path = write_empirical_analysis_report(output_dir=args.output_dir)
    print(report_path)


if __name__ == "__main__":
    main()
