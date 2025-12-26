# app/services/entities/place.py
from typing import List, Dict, Any, Tuple
from app.services.common import extract_human_readable_id

def process_places(raw_places: List[Dict[str, Any]], context: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    # Places are mostly used as lookups, but we need details cache
    # Note: The user requested filtering for Place cards based on place_has_name
    # Since places are not usually returned in the main search list in this app structure (they return empty list in original code),
    # I will keep the return list empty but ensure the cache is populated correctly based on the filter.
    
    place_details_cache = {}
    id2projects = context["id2projects"]
    place_to_name = context["place_to_name"]
    id2label = context["id2label"]

    for place in raw_places:
        p_id = place["_id"]
        
        # --- FILTER: Must have place_has_name ---
        if p_id not in place_to_name:
            continue
            
        place_name = place_to_name[p_id]

        place_details_cache[p_id] = {
            "place_name": place_name, 
            "human_readable_id": extract_human_readable_id(place, "loc_"),
            "relationships": [], 
            "projects": id2projects.get(p_id, ["Unknown Project"])
        }
    
    return [], place_details_cache