from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]

CHECKPOINT_DIR = (
    PROJECT_ROOT
    / "data"
    / "checkpoints"
    / "traffic_analytics"
)


def get_checkpoint_path():
    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
    return str(CHECKPOINT_DIR)