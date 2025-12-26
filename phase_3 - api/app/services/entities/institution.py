# app/services/entities/institution.py
from typing import List, Dict, Any, Tuple
from app.services.common import extract_human_readable_id, calculate_richness

def process_institutions(raw_institutions: List[Dict[str, Any]], context: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    unique_institutions_map = {}
    institution_details_cache = {}

    id2projects = context["id2projects"]
    institution_to_name = context["institution_to_name"]
    id2label = context["id2label"] # Fallback
    institution_to_place = context["institution_to_place"]
    entity_to_hypotheses_details = context["entity_to_hypotheses_details"]
    institution_to_roles = context["institution_to_roles"] # --- MODIFICATION 2 START ---

    for inst in raw_institutions:
        inst_id = inst["_id"]
        
        # --- FILTER: Must have institution_has_name ---
        if inst_id not in institution_to_name:
            continue

        inst_name = institution_to_name[inst_id]
        
        inst_place = institution_to_place.get(inst_id)
        projects = id2projects.get(inst_id, ["Unknown Project"])
        roles = list(institution_to_roles.get(inst_id, {}).keys()) # --- MODIFICATION 2 START ---

        card = {
            "name": inst_name,
            "place": inst_place,
            "projects": projects
        }

        inst_data = {
            "institution_id": inst_id,
            "human_readable_id": extract_human_readable_id(inst, "inst_"),
            "institution_name": inst_name,
            "projects": projects,
            "place": inst_place,
            "hypotheses": entity_to_hypotheses_details.get(inst_id, []),
            "institution_name_normalized": {inst_name.lower()},
            "institution_place_normalized": {inst_place["place_name"].lower()} if inst_place else set(),
            "roles": roles, # --- MODIFICATION 2 START ---
            "roles_normalized": {role.lower() for role in roles}, # --- MODIFICATION 2 START ---
            "card": card
        }

        # --- DEDUPLICATION LOGIC ---
        # Key is now (Name, HumanReadableID)
        unique_key = (inst_name, inst_data["human_readable_id"])

        if unique_key in unique_institutions_map:
            existing = unique_institutions_map[unique_key]
            if calculate_richness(inst_data) > calculate_richness(existing):
                unique_institutions_map[unique_key] = inst_data
                institution_details_cache[inst_id] = {
                    "institution_name": inst_name, 
                    "relationships": [], 
                    "roles_with_entities": {}, 
                    "projects": projects
                }
        else:
            unique_institutions_map[unique_key] = inst_data
            institution_details_cache[inst_id] = {
                "institution_name": inst_name, 
                "relationships": [], 
                "roles_with_entities": {}, 
                "projects": projects
            }

    return list(unique_institutions_map.values()), institution_details_cache