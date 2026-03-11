#!/usr/bin/env python3
import argparse
import subprocess
import sys
from pathlib import Path
import tomllib
import re

PROJECTS_DIR = Path("projects")


def run(cmd, capture=False):
    result = subprocess.run(cmd, check=True, text=True, capture_output=capture)
    return result.stdout.strip() if capture else None


def ensure_clean_git():
    status = run(["git", "status", "--porcelain"], capture=True)
    if status:
        print("❌ Working tree not clean. Commit or stash changes first.")
        sys.exit(1)


def find_projects():
    return [p for p in PROJECTS_DIR.iterdir() if (p / "pyproject.toml").exists()]


def resolve_project(name: str):
    if name.startswith("aer-"):
        return PROJECTS_DIR / name

    for p in find_projects():
        if p.name.endswith(name):
            return p

    raise ValueError(f"Project {name} not found")


def get_current_version(pyproject_path: Path):
    data = tomllib.loads(pyproject_path.read_text())
    return data["project"]["version"]


def get_next_version(project_path: Path, tag_format: str):
    tmp_config = project_path / "release_tmp.toml"

    tmp_config.write_text(
        f"""
[tool.semantic_release]
version_toml = ["{project_path}/pyproject.toml:project.version"]
tag_format = "{tag_format}"
commit_parser = "angular"
"""
    )

    try:
        result = run(
            [
                "uv",
                "run",
                "semantic-release",
                "-c",
                str(tmp_config),
                "version",
                "--print",
            ],
            capture=True,
        )

        if not result:
            return None

        return result.splitlines()[-1]

    finally:
        tmp_config.unlink()


def update_version(pyproject: Path, version: str):
    text = pyproject.read_text()

    text = re.sub(
        r'version\s*=\s*"[0-9A-Za-z\.\-]+"',
        f'version = "{version}"',
        text,
    )

    pyproject.write_text(text)


def release_project(project_path: Path, push: bool):
    name = project_path.name
    pyproject = project_path / "pyproject.toml"

    tag_format = f"{name}-v{{version}}"

    current = get_current_version(pyproject)
    next_version = get_next_version(project_path, tag_format)

    if not next_version or next_version == current:
        print(f"ℹ️  {name}: no release needed")
        return

    print(f"🚀 Releasing {name} {current} → {next_version}")

    update_version(pyproject, next_version)

    commit_msg = f"chore(release): {name} v{next_version}"

    run(["git", "add", str(pyproject)])
    run(["git", "commit", "-m", commit_msg])

    tag = f"{name}-v{next_version}"
    run(["git", "tag", "-a", tag, "-m", commit_msg])

    if push:
        run(["git", "push", "origin", tag])

    print(f"✅ Tagged {tag}")


def detect_changed_projects():
    diff = run(["git", "diff", "--name-only", "origin/main"], capture=True)
    files = diff.splitlines()

    projects = set()

    for f in files:
        for p in find_projects():
            if str(p) in f:
                projects.add(p)

    return list(projects)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("project", nargs="?")
    parser.add_argument("--all", action="store_true")
    parser.add_argument("--changed", action="store_true")
    parser.add_argument("--no-push", action="store_true")

    args = parser.parse_args()

    ensure_clean_git()

    push = not args.no_push

    if args.all:
        targets = find_projects()

    elif args.changed:
        targets = detect_changed_projects()

    elif args.project:
        targets = [resolve_project(args.project)]

    else:
        parser.error("Specify project, --all, or --changed")

    for project in targets:
        release_project(project, push)


if __name__ == "__main__":
    main()
