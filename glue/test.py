from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

works_path = BASE_DIR / "clean_data" / "data" / "works.parquet"

print(works_path)