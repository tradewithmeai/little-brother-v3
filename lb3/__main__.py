"""Allow running lb3 as a module with python -m lb3."""

from .cli import app

if __name__ == "__main__":
    app()
