from pathlib import Path
import os

DATA_DIR = Path(os.getenv("BD2_DATA_DIR", "") or Path(__file__).resolve().parents[1] / "runtime" / "files")
DATA_DIR.mkdir(parents=True, exist_ok=True)
