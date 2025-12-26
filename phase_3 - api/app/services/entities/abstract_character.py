# app/services/entities/abstract_character.py
from typing import List, Dict, Any, Tuple
from app.services.common import extract_human_readable_id, calculate_richness

def process_abstract_characters(raw_acs: List[Dict[str, Any]], context: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    unique_ac_map = {}
    ac_details_cache = {}

    id2projects = context["id2projects"]
    ac_to_name = context["ac_to_name"]
    ac_to_aliases = context.get("ac_to_aliases", {})
    ac_is_mentioned_by = context.get("ac_is_mentioned_by", {})
    id_to_collection = context["id_to_collection"]
    entity_to_hypotheses_details = context["entity_to_hypotheses_details"]

    for ac in raw_acs:
        ac_id = ac["_id"]
        
        if ac_id not in ac_to_name:
            continue

        ac_name = ac_to_name[ac_id]
        projects = id2projects.get(ac_id, ["Unknown Project"])
        aliases = ac_to_aliases.get(ac_id, [])
        
        search_terms = {ac_name.lower()} | {alias.lower() for alias in aliases}
        
        mentioned_by_ids = ac_is_mentioned_by.get(ac_id, [])
        mentioned_by_entity_types = {id_to_collection.get(m_id) for m_id in mentioned_by_ids if id_to_collection.get(m_id)}

        card = {
            "ac_name": ac_name,
            "projects": projects
        }
        
        ac_data = {
            "abstract_character_id": ac_id, 
            "human_readable_id": extract_human_readable_id(ac, "ac_"),
            "ac_name": ac_name, 
            "projects": projects,
            "hypotheses": entity_to_hypotheses_details.get(ac_id, []),
            "ac_name_normalized": search_terms,
            "mentioned_by_entity_types": mentioned_by_entity_types,
            "card": card
        }

        unique_key = (ac_name, ac_data["human_readable_id"])

        if unique_key in unique_ac_map:
            existing = unique_ac_map[unique_key]
            if calculate_richness(ac_data) > calculate_richness(existing):
                unique_ac_map[unique_key] = ac_data
        else:
            unique_ac_map[unique_key] = ac_data

        # Always create a details cache entry for every active AC with a name
        ac_details_cache[ac_id] = {
            "ac_name": ac_name, 
            "relationships": [], 
            "projects": projects
        }

    return list(unique_ac_map.values()), ac_details_cache