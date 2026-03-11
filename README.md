# 🚀 aer Plugin Template

Welcome to the **aer plugin template**! This repository is your starting point for building high-performance, modular plugins for the `aer` ecosystem. 

Powered by the [Polylith architecture](https://davidvujic.github.io/python-polylith-docs/setup/) and `uv` for lightning-fast dependency management, this template ensures your plugin is scalable, maintainable, and ready for production.

---

## ⚡ Quick Start

If you are new here, **start with the setup script**. It will bootstrap your environment, name your project, and create your first component automatically.

```bash
chmod +x setup.sh
./setup.sh
```

<details>
<summary><b>🔍 What does the setup script do?</b></summary>

The `setup.sh` script automates the tedious parts of starting a new Polylith plugin:
1.  **Validation**: Ensures your project name follows the `aer-` prefix rule.
2.  **Environment**: Installs `uv` (if missing) and sets up the workspace dependencies.
3.  **Scaffolding**: Creates your first **Component** (for code) and **Project** (for packaging).
4.  **Configuration**: Generates a pre-configured `pyproject.toml` with standard entry points so `aer-core` can immediately find your plugin.
5.  **Git Hooks**: Installs `prek` to ensure high-quality commits from day one.
</details>

> [!IMPORTANT]
> The setup script is mandatory for new project initialization. It ensures consistent naming conventions (`aer-` prefix) and registers your plugin entry points correctly.

---

## 🏗️ Architecture Overview

This project uses a **Polylith** structure. Instead of a messy monolith, code is organized into:

*   **Components**: Pure logic and functionality (found in `components/`).
*   **Projects**: Deployable artifacts like PyPI packages (found in `projects/`).
*   **Bases**: Public APIs or entry points (CLI, FastAPI, etc.).

### Why Polylith?
It allows you to share code between different projects within the same workspace perfectly, while keeping your deployments lean.

---

## 🛠️ Development Workflow

Once you've run the setup script, here is how you work with your new plugin:

### 1. Project Info
See the state of your workspace, which components are used by which projects:
```bash
uv run poly info
```

### 2. Adding Dependencies
Use `uv` to manage your environment:
```bash
uv add requests          # Add to the workspace
uv add --group dev pytest  # Add development tools
```

### 3. Running Tests
Tests are enabled globally. Run them with:
```bash
uv run pytest
```

---

## 📦 Creating More Components (Advanced)

If your plugin grows and you need to split logic into more components, you can use the Polylith CLI:

```bash
# Create a new component
uv run poly create component --name my_new_feature

# Create a new project (if you want to ship a separate package)
uv run poly create project --name aer-my-other-package
```

---

## 🚀 Releasing Plugins

Releases are automated using [Conventional Commits](https://www.conventionalcommits.org/) and `python-semantic-release`.

1.  **Commit with prefixes**: Use `feat:`, `fix:`, or `chore:` in your commit messages.
2.  **Run the release script**:
    ```bash
    # Release a specific project
    python3 .agents/scripts/release.py aer-my-plugin

    # Release all changed projects
    python3 .agents/scripts/release.py --changed
    ```

### 🤖 Agentic Workflow
If you are working with **Antigravity** or another AI assistant, you can simply say:
> "Release my plugin" or "Create a new release for aer-search-earthaccess"

The assistant will use the `new-release` skill to handle the versioning, tagging, and pushing for you.

---

## 📜 License

This template is licensed under the [MIT License](LICENSE).