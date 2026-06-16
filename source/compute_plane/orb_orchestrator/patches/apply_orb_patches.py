#!/usr/bin/env python3
"""Apply the 4 source patches that make orb-py 1.6.2's DynamoDB backend work.

orb-py 1.6.2 ships a DynamoDB storage backend that does not function out of the
box. Each patch below is a minimal, targeted fix; all four are required for the
create/status/terminate loop to work against real DynamoDB + EC2. See
docs/ORB_DYNAMODB_PATCHES.md for the full diagnosis.

The script locates the installed `orb` package and edits it in place. It is
idempotent: re-running detects already-applied patches and skips them. It fails
loudly if any anchor string is missing (e.g. a future orb-py version moved the
code), so a broken build never ships silently.
"""

from __future__ import annotations

import importlib.util
import os
import sys
from pathlib import Path


def _orb_root() -> Path:
    """Locate the installed `orb` package directory.

    Container build: orb is pip-installed into the interpreter, found via importlib.
    Zip build: orb is pip-installed into a target/staging dir; pass that dir as the first
    CLI arg (or via ORB_PACKAGE_DIR) so we patch the copy that gets zipped, not the build
    host's interpreter copy.
    """
    override = (sys.argv[1] if len(sys.argv) > 1 else "") or os.environ.get("ORB_PACKAGE_DIR", "")
    if override:
        root = Path(override)
        # Accept either the dir containing `orb/` or the `orb/` package dir itself.
        if (root / "orb").is_dir():
            root = root / "orb"
        if not root.is_dir():
            raise SystemExit(f"ERROR: ORB package dir not found at {override}")
        return root

    spec = importlib.util.find_spec("orb")
    if spec is None or not spec.submodule_search_locations:
        raise SystemExit("ERROR: orb package not found; pip install orb-py first")
    return Path(list(spec.submodule_search_locations)[0])


def _patch(path: Path, old: str, new: str, *, label: str) -> None:
    text = path.read_text()
    if new in text:
        print(f"  [skip] {label}: already applied")
        return
    if old not in text:
        raise SystemExit(
            f"ERROR: {label}: anchor not found in {path}.\n"
            "orb-py internals changed; review the patch before shipping."
        )
    path.write_text(text.replace(old, new, 1))
    print(f"  [ok]   {label}")


def main() -> int:
    root = _orb_root()
    print(f"Patching orb at {root}")

    # Patch 1 — allow 'dynamodb' as a core storage strategy.
    # Without this the SDK's get_typed(AppConfig) hard-rejects
    # storage.strategy=dynamodb even though the dynamodb backend exists.
    _patch(
        root / "config/schemas/storage_schema.py",
        'valid_strategies = ["json", "sql"]',
        'valid_strategies = ["json", "sql", "dynamodb"]',
        label="1/4 storage_schema valid_strategies",
    )

    # Patch 2 — DynamoDB unit-of-work passes a raw boto3 client where the
    # client manager expects an ORB wrapper with .get_client(); passing None
    # makes the manager build its own client correctly.
    reg = root / "providers/aws/storage/registration.py"
    reg_text = reg.read_text()
    if "aws_client=aws_client," in reg_text:
        reg.write_text(reg_text.replace("aws_client=aws_client,", "aws_client=None,"))
        print("  [ok]   2/4 registration aws_client=None")
    elif "aws_client=None," in reg_text:
        print("  [skip] 2/4 registration: already applied")
    else:
        raise SystemExit(f"ERROR: 2/4: anchor not found in {reg}")

    # Patch 3 — bool must be handled before int (bool subclasses int), else
    # Decimal(str(True)) raises ConversionSyntax on write.
    _patch(
        root / "providers/aws/storage/components/dynamodb_converter.py",
        (
            "        # Handle numeric types - convert to Decimal for DynamoDB\n"
            "        if isinstance(value, (int, float)):\n"
            "            return Decimal(str(value))\n"
            "\n"
            "        # Handle boolean\n"
            "        if isinstance(value, bool):\n"
            "            return value\n"
        ),
        (
            "        # Handle boolean (must precede int: bool is a subclass of int)\n"
            "        if isinstance(value, bool):\n"
            "            return value\n"
            "\n"
            "        # Handle numeric types - convert to Decimal for DynamoDB\n"
            "        if isinstance(value, (int, float)):\n"
            "            return Decimal(str(value))\n"
        ),
        label="3/4 converter bool-before-int",
    )

    # Patch 4 — do not auto-parse ISO strings to datetime on read; the domain
    # layer calls fromisoformat() itself and chokes on an already-parsed
    # datetime ("argument must be str").
    _patch(
        root / "providers/aws/storage/components/dynamodb_converter.py",
        (
            "            with suppress(ValueError, TypeError):\n"
            '                if "T" in value and ("Z" in value or "+" in value or value.endswith("00")):\n'
            '                    return datetime.fromisoformat(value.replace("Z", "+00:00"))\n'
            "            return value"
        ),
        (
            "            # NOTE: return ISO strings as-is; domain layer parses them itself.\n"
            "            return value"
        ),
        label="4/4 converter no datetime auto-parse",
    )

    print("All ORB DynamoDB patches applied.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
