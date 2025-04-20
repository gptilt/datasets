import json
import requests


BUILDINGS = {
    "Locator_Map_Center",
    # Turrets Fountain
    "Turret_OrderTurretShrine",
    "Turret_ChaosTurretShrine",
    # Nexus & Nexus Turrets
    # Blue
    "Turret_OrderNexus",
    "Turret_T1_C_01",
    "Turret_T1_C_02",
    # Red
    "Turret_ChaosNexus",
    "Turret_T2_C_01",
    "Turret_T2_C_02",
    # Inhibitors & Inhibitor Turrets
    # Blue
    "Barracks_T1_L1",
    "Turret_T1_C_06",
    "Barracks_T1_C1",
    "Turret_T1_C_03",
    "Barracks_T1_R1",
    "Turret_T1_C_07",
    # Red
    "Barracks_T2_L1",
    "Turret_T2_L_01",
    "Barracks_T2_C1",
    "Turret_T2_C_03",
    "Barracks_T2_R1",
    "Turret_T2_R_01",
    # Turrets Top Inner
    # Blue
    "Turret_T1_L_02"
    # Red
    "Turret_T2_L_02"
    # Turrets Top Outer
    # Blue
    "Turret_T1_L_03",
    # Red
    "Turret_T2_L_03",
    # Turrets Mid Inner
    # Blue
    "Turret_T1_C_04",
    # Red
    "Turret_T2_C_04",
    # Turrets Mid Outer
    # Blue
    "Turret_T1_C_05",
    # Red
    "Turret_T2_C_05",
    # Turrets Bot Inner
    # Blue
    "Turret_T1_R_02",
    # Red
    "Turret_T2_R_02",
    # Turrets Bot Outer
    # Blue
    "Turret_T1_R_03",
    # Red
    "Turret_T2_R_03",
}


def get_building_xy():
    response = requests.get('https://raw.communitydragon.org/latest/game/data/maps/mapgeometry/map11/base.materials.bin.json')
    materials = json.loads(response.text)
    map_objects = materials["Maps/MapGeometry/Map11/Chunks/SRX_MapObjects"]['items']
    
    return {
        obj["name"]: (obj['transform'][-1][0], obj['transform'][-1][2])
        for obj in map_objects.values()
        if "name" in obj and obj["name"] in BUILDINGS
    }


def get_camp_xy():
    response = requests.get('https://raw.communitydragon.org/15.8/game/data/maps/mapgeometry/map11/a22.materials.bin.json')
    materials = json.loads(response.text)
    map_objects = materials["{0395a48a}"]['items']
    
    return {
        obj["definition"]["CampName"]: (obj['transform'][-1][0], obj['transform'][-1][2])
        for obj in map_objects.values()
        if "definition" in obj and "CampName" in obj["definition"]
    }


def mirror(center, mirror_from):
    """
    Mirrors the coordinates of a point across a center point.
    """
    return (
        2 * center[0] - mirror_from[0],
        2 * center[1] - mirror_from[1]
    )
