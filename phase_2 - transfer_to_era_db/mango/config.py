# START OF FILE config.py

from pathlib import Path
import tomllib

path = Path(__file__).parent / "recipe.toml"

with open(path, mode="rb") as fb:
    rules = tomllib.load(fb)

#server = rules["server"]
# Add this line to load database configuration
database = rules["database"] 
column2type = rules["mapping"]
properties = rules["properties"]
relations = rules["relations"]
delimited_fields = rules["delimited_fields"]

# Defines rules for dynamic entity typing based on value prefixes.
# The key is the prefix, and the value is the entity type to assign.


prefix_map = {
    "p_": "person",
    "w_": "work",
    "ex_": "expression",
    "m_": "manifestation",
    "m_vol_":"manifestation",
    "i_": "item",
    "PO_": "physical_object",
    "PO_PAG_":"physical_object",
    "PAG_": "page",
    "VO_": "visual_object",
    "e_": "event", 
    "ac_": "abstract_character",
    "inst_": "institution",
    "loc_": "place",
    

}