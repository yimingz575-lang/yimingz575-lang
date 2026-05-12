from pathlib import Path

from src.ui.app import create_app


def main() -> None:
    project_root = Path(__file__).resolve().parent
    app = create_app(project_root)
    app.run(host="127.0.0.1", port=8050, debug=True)


if __name__ == "__main__":
    main()