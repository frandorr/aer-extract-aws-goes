---
name: new-release
description: Automates the release process for a given Polylith project by updating the version, committing, tagging, and pushing changes using semantic-release.
---

# New Release Skill

Use this skill when the user asks to "release", "tag", or "create a new version" for a specific project or plugin.

## Workflow

1.  **Identify the Project**:  **Do not ask for a version**. The version is automatically calculated based on conventional commits using `python-semantic-release`.
2.  **Run the Release Script**: Execute the `.agents/scripts/release.py` script.

### Example Execution

```bash
python3 .agents/scripts/release.py <project_name>
```

### Dry Run

If you want to verify the actions first, use the `--dry-run` flag:

```bash
python3 .agents/scripts/release.py <project_name> --dry-run
```

## Constraints

-   Only run this for projects located in the `projects/` directory.
-   Ensure the user is aware that this will perform `git commit`, `git tag`, and `git push` by default.
