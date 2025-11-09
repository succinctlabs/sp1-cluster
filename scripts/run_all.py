#!/usr/bin/env python3
"""
Run benchmarks for all input files in an S3 directory.

Usage:
    uv run scripts/run_all.py --s3-path <s3_path> [--bucket <bucket>] [additional bench args]

The S3 directory should have this structure:
    s3://bucket/s3_path/
        program.bin
        input/
            param1.bin
            param2.bin
            ...
"""

import argparse
import subprocess
import sys
from pathlib import Path


def list_s3_files(s3_path: str) -> list[str]:
    """List all .bin files in the input subdirectory."""
    s3_input_path = f"s3://sp1-testing-suite/{s3_path}/input/"

    try:
        result = subprocess.run(
            ["aws", "s3", "ls", s3_input_path],
            capture_output=True,
            text=True,
            check=True
        )
    except subprocess.CalledProcessError as e:
        print(f"Error listing S3 files: {e.stderr}", file=sys.stderr)
        sys.exit(1)

    # Parse the output to get filenames
    # Output format: "2024-01-01 12:00:00      12345 filename.bin"
    files = []
    for line in result.stdout.strip().split('\n'):
        if not line:
            continue
        parts = line.split()
        if len(parts) >= 4:
            filename = parts[3]
            if filename.endswith('.bin'):
                # Remove the .bin extension to get the param name
                param = filename[:-4]
                files.append(param)

    return files


def run_bench(s3_path: str, param: str, extra_args: list[str]):
    """Run the bench command for a specific param."""
    # Build the command
    cmd = [
        "cargo", "run", "--bin", "sp1-cluster-cli", "--release", "--",
        "bench", "s3",
        s3_path,
        "--param", param,
    ]
    cmd.extend(extra_args)

    print(f"\n{'='*80}")
    print(f"Running benchmark for param: {param}")
    print(f"Command: {' '.join(cmd)}")
    print(f"{'='*80}\n")

    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error running benchmark for {param}: {e}", file=sys.stderr)
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="Run benchmarks for all input files in an S3 directory"
    )
    parser.add_argument(
        "--s3-path",
        required=True,
        help="S3 path to the program directory (e.g., 'path/to/program' for s3://bucket/path/to/program/)"
    )

    # Parse known args to allow passing through additional args to bench command
    args, extra_args = parser.parse_known_args()

    # List all input files
    print(f"Listing input files from s3://sp1-testing-suite/{args.s3_path}/input/...")
    params = list_s3_files(args.s3_path)

    if not params:
        print("No input files found!", file=sys.stderr)
        sys.exit(1)

    print(f"Found {len(params)} input files: {', '.join(params)}\n")

    # Run benchmark for each param
    for param in params:
        run_bench(args.s3_path, param, extra_args)

    print(f"\n{'='*80}")
    print(f"All benchmarks completed successfully!")
    print(f"{'='*80}")


if __name__ == "__main__":
    main()
