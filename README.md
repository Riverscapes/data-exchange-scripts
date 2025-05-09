# Data Exchange Scripts (pydex)

**EXPERIMENTAL**: This repository contains the Python module `pydex` for connecting to the Riverscapes Data Exchange API. It also includes a collection of scripts that use `pydex` classes to perform useful tasks.

## Project Overview

This project is designed to simplify interaction with the Riverscapes GraphQL API. It uses modern Python packaging standards, including a `pyproject.toml` file for configuration and dependency management.

## Using UV for Environment Management

This project uses [uv](https://github.com/astral-sh/uv) to manage Python virtual environments and dependencies. `uv` is an alternative to tools like `pipenv` and `poetry`.

### Prerequisites

1. Install `uv` by following the [installation instructions](https://github.com/astral-sh/uv#installation) for your operating system.
2. Ensure you have Python 3.9 or higher installed.

### Setting Up the Project

To set up the project, follow these steps:

```bash
# Clone the repository
git clone https://github.com/Riverscapes/data-exchange-scripts.git
cd data-exchange-scripts

# Sync the environment using uv
uv sync
```

This will create a `.venv` folder in the root of the repository with the correct Python environment and dependencies installed.

### Using the Virtual Environment in VSCode

1. Open the repository in VSCode.
2. If the `.venv` environment is not automatically detected, reload the window or restart VSCode.
3. Select the Python interpreter located at `.venv/bin/python` (on macOS/Linux) or `.venv\Scripts\python.exe` (on Windows).

## Running Scripts

The best way to run a script is going to be using the "Run and Debug" feature in VSCode. This will ensure that the correct virtual environment is activated and that the script runs in the correct context.

Click that button and select the dropdown item that best fits. If you're just trying to run a file without a launch item you can use `🚀 Python: Run/Debug Current File (with .env)`. This will run the script and set you up with a server environment context (production or staging). 

Running scripts this way will also allow you to drop breakpoints in your code and debug it.

## Optional Dependencies

This project includes optional dependencies for geospatial functionality. To install these dependencies, run:

```bash
uv sync --extra geo
```

This will install packages like `gdal` and `shapely`. Note that `gdal` may require additional system-level dependencies. On macOS, you can install `gdal` using Homebrew:

```bash
brew install gdal
```

## Codespace Instructions

1. Open the codespace "Riverscapes API Codespace."
2. In VSCode, load the `RiverscapesAPI.code-workspace` workspace.
3. Ensure the appropriate Python version is selected (e.g., `3.12.9 ('.venv')`).

**NOTE: THE CODESPACE WILL NOT WORK WITH SCRIPTS THAT REQUIRE GDAL FOR NOW (think the project merging etc.)**

## Best Practices

- **Dependency Management**: Use `uv sync` to ensure your environment is always up-to-date with the dependencies specified in `pyproject.toml`.


## Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository.
2. Create a new branch for your feature or bugfix.
3. Submit a pull request with a clear description of your changes.

## License

This project is licensed under the MIT License. See the `LICENSE` file for details.