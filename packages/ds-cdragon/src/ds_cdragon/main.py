from cdragon import coordinates
import json
from pathlib import Path


def main():
    # Create directory if it doesn't exist
    DIR_COORDINATES = Path("data/cdragon/coordinates/")
    Path(DIR_COORDINATES).mkdir(parents=True, exist_ok=True)

    building_coordinates = coordinates.get_building_xy()
    camp_coordinates = coordinates.get_camp_xy()
    camp_coordinates["Dragon"] = coordinates.mirror(
        center=building_coordinates["Locator_Map_Center"],
        mirror_from=camp_coordinates["Baron"],
    )

    with open("data/cdragon/coordinates/buildings.json", "w") as f:
        json.dump(building_coordinates, f, indent=4)
    
    with open("data/cdragon/coordinates/camps.json", "w") as f:
        json.dump(camp_coordinates, f, indent=4)
