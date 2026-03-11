# PyPI Publishing Guide

This template is designed to work with GitHub Actions for automated publishing to PyPI. The process is **tag-triggered**: when you create a tag like `aer-xyz-v1.0.0`, the CI will build and upload that specific package.

---

## 1. Set up PyPI Trusted Publishing

The most secure way to publish is via [PyPI Trusted Publishing](https://docs.pypi.org/trusted-publishers/).

1.  Go to your PyPI project (or your account settings).
2.  Add a **GitHub Publisher**.
3.  Set the **Environment** to `pypi`.
4.  Specify the **GitHub Repository** and the **Workflow name** (`release.yml`).

## 2. GitHub Release Action

Create the file `.github/workflows/release.yml` with the following content:

```yaml
name: Publish Plugin to PyPI

on:
  push:
    tags:
      - "aer-*-v*" # Triggers on tags like aer-search-earthaccess-v1.0.0

jobs:
  publish:
    name: Build & Publish
    runs-on: ubuntu-latest
    permissions:
      id-token: write # Required for PyPI Trusted Publishing
      contents: write # Required for GitHub Releases

    environment: pypi

    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0 # Get all tags for correct versioning (if needed)

      - name: Install uv
        uses: astral-sh/setup-uv@v2
        with:
          enable-cache: true

      - name: Extract Project Name
        id: meta
        run: |
          # Extract prefix (e.g. aer-search-earthaccess) from aer-search-earthaccess-v1.0.0
          TAG=${GITHUB_REF#refs/tags/}
          PROJECT=$(echo $TAG | sed -E 's/-v[0-9].*$//')
          echo "project=$PROJECT" >> $GITHUB_OUTPUT
          echo "Building project: $PROJECT"

      - name: Build Package
        run: |
          # Change directory to the specific Polylith project
          cd "projects/${{ steps.meta.outputs.project }}"
          
          # Build using uv (uses hatchling + hatch-polylith-bricks)
          uv build --out-dir dist/

      - name: Publish to PyPI
        uses: pypa/gh-action-pypi-publish@release/v1
        with:
          packages-dir: "projects/${{ steps.meta.outputs.project }}/dist/"

      - name: Create GitHub Release
        uses: softprops/action-gh-release@v2
        with:
          files: "projects/${{ steps.meta.outputs.project }}/dist/*"
          generate_release_notes: true
```

## 3. Triggering a Release

Use the `release.py` script or the AI skill as described in the [README](README.md#releasing-plugins).

1.  Draft your code changes.
2.  Commit via Conventional Commits.
3.  Execute `.agents/scripts/release.py <project-name>` or ask you agent to run new-release skill
4.  The script will push the tag, and the GitHub Action above will handle the rest!