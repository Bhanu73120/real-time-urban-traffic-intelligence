import json
from pathlib import Path


def load_roads():
    project_root = Path(__file__).resolve().parents[2]
    road_file = project_root / "data" / "reference" / "roads.json"

    with open(road_file, "r", encoding="utf-8") as file:
        roads = json.load(file)

    return roads