# Riverscapes

A description of your project.

## Using UV

When you clone this repo you can get it set up quickly using `uv`. Uv is a tool that helps manage python virtual environments and dependencies. It is an alternative to `pipenv` and `poetry`.

Make sure you have `uv` installed. You can install it with `pip`:

```bash
pip install uv
```

Then you can set up the project with the following commands:

```bash
# In the root of the repository
uv sync
```

Now in VSCode you should have a `.venv` folder in the root with the correct python environment set up. If you don't see it immediately try reloading the window or restarting VSCode. You're looking for a version of python at the path: `.venv/bin/python`.