from cdragon import coordinates
import json
from pathlib import Path


def main():
    # Create directory if it doesn't exist
    DIR_COORDINATES = Path("data/cdragon/coordinates/")
    Path(DIR_COORDINATES).mkdir(parents=True, exist_ok=True)

    with open("data/cdragon/coordinates/buildings.json", "w") as f:
        json.dump(coordinates.get_building_xy(), f, indent=4)
    
    with open("data/cdragon/coordinates/camps.json", "w") as f:
        json.dump(coordinates.get_camp_xy(), f, indent=4)
