#!/usr/bin/env python3
"""Fail if any tracked text file contains an em dash or en dash.

A hard convention of this repo: no em dashes anywhere in code, docstrings,
comments, READMEs, or commit messages. This hook enforces it. Use commas,
colons, parentheses, or restructure the sentence instead.

Run by pre-commit against the staged files. Exits non-zero on the first
violation and prints the offending file, line, and column.
"""

from __future__ import annotations

import sys

# U+2014 EM DASH and U+2013 EN DASH. Both are banned. Built with chr() on
# purpose, so this file itself contains no literal dash for the check to trip on.
FORBIDDEN = {chr(0x2014): "em dash", chr(0x2013): "en dash"}


def check_file(path: str) -> list[str]:
    problems: list[str] = []
    try:
        with open(path, encoding="utf-8") as handle:
            for lineno, line in enumerate(handle, start=1):
                for char, name in FORBIDDEN.items():
                    col = line.find(char)
                    if col != -1:
                        problems.append(f"{path}:{lineno}:{col + 1}: found {name} ({char!r})")
    except (UnicodeDecodeError, FileNotFoundError):
        # Binary or missing files are not our concern here.
        return []
    return problems


def main(argv: list[str]) -> int:
    all_problems: list[str] = []
    for path in argv[1:]:
        all_problems.extend(check_file(path))

    if all_problems:
        print("Em or en dash found. Replace with commas, colons, or parentheses:")
        for problem in all_problems:
            print(f"  {problem}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
