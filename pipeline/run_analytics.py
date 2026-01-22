import logging
from pathlib import Path

from pipeline.analytics import run_all


def _setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-5s %(name)s: %(message)s",
    )


def main() -> int:
    _setup_logging()
    root = Path(__file__).resolve().parents[1]
    db_path = root / "data" / "output" / "warehouse.db"
    export_dir = root / "data" / "output" / "exports"

    if not db_path.exists():
        raise FileNotFoundError(f"Warehouse DB not found: {db_path}. Run the pipeline first.")

    results = run_all(db_path, export_dir)

    # Print previews + exported paths
    for r in results:
        print("\n" + r.name)
        if r.df.empty:
            print("(no rows)")
        else:
            print(r.df.head(20).to_string(index=False))
        logging.getLogger(__name__).info("exported: %s", r.csv_path)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
