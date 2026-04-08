#!/usr/bin/env python3
"""
Auto-increment version number on each commit.
Usage: Run this script before each commit to bump the version.

Version format: MAJOR.MINOR
- MINOR increments on each commit
- MAJOR increments manually when needed

Example:
  python3 bump_version.py          # Bumps 2.5 → 2.6
  python3 bump_version.py --major  # Bumps 2.5 → 3.0
"""

import os
import sys
from pathlib import Path

VERSION_FILE = Path(__file__).parent / "VERSION"
APP_FILE = Path(__file__).parent / "app.py"


def read_version():
    """Read current version from VERSION file."""
    if not VERSION_FILE.exists():
        return "1.0"
    return VERSION_FILE.read_text().strip()


def write_version(version):
    """Write new version to VERSION file."""
    VERSION_FILE.write_text(version + "\n")


def update_app_version(version):
    """Update APP_VERSION in app.py."""
    if not APP_FILE.exists():
        return
    
    content = APP_FILE.read_text()
    
    # Update the APP_VERSION line
    import re
    new_content = re.sub(
        r'APP_VERSION\s*=\s*["\']V?[\d.]+["\']',
        f'APP_VERSION = "V{version}"',
        content
    )
    
    if new_content != content:
        APP_FILE.write_text(new_content)
        print(f"✅ Updated app.py: APP_VERSION = \"V{version}\"")


def bump_version(bump_major=False):
    """Increment version number."""
    current = read_version()
    major, minor = map(int, current.split("."))
    
    if bump_major:
        major += 1
        minor = 0
    else:
        minor += 1
    
    new_version = f"{major}.{minor}"
    
    write_version(new_version)
    update_app_version(new_version)
    
    print(f"📦 Version bumped: {current} → {new_version}")
    return new_version


if __name__ == "__main__":
    bump_major = "--major" in sys.argv
    bump_version(bump_major)
