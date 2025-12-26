# app/services/entities/event.py
from typing import List, Dict, Any, Tuple
from app.services.common import extract_human_readable_id, parse_date_to_range, calculate_richness

def process_events(raw_events: List[Dict[str, Any]], context: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    unique_events_map = {}
    event_details_cache = {}

    id2projects = context["id2projects"]
    event_to_name = context["event_to_name"]
    id2label = context["id2label"] # Fallback
    event_to_date = context["event_to_date"]
    event_to_place = context["event_to_place"]
    entity_to_hypotheses_details = context["entity_to_hypotheses_details"]

    for event in raw_events:
        event_id = event["_id"]
        
        # --- FILTER: Must have event_has_name ---
        if event_id not in event_to_name:
            continue

        event_name = event_to_name[event_id]
        
        event_date = event_to_date.get(event_id)
        event_place = event_to_place.get(event_id)
        projects = id2projects.get(event_id, ["Unknown Project"])
        
        start_year, end_year = parse_date_to_range(event_date)
        
        card = {
            "event_name": event_name,
            "date": event_date,
            "place": event_place,
            "projects": projects
        }
        
        event_data = {
            "event_id": event_id,
            "human_readable_id": extract_human_readable_id(event, "e_"),
            "event_name": event_name,
            "projects": projects,
            "date": event_date,
            "place": event_place,
            "start_year": start_year,
            "end_year": end_year,
            "hypotheses": entity_to_hypotheses_details.get(event_id, []),
            "event_name_normalized": {event_name.lower()},
            "card": card
        }

        # --- DEDUPLICATION LOGIC ---
        # Key is now (Name, HumanReadableID)
        unique_key = (event_name, event_data["human_readable_id"])

        if unique_key in unique_events_map:
            existing = unique_events_map[unique_key]
            if calculate_richness(event_data) > calculate_richness(existing):
                unique_events_map[unique_key] = event_data
                event_details_cache[event_id] = {
                    "event_name": event_name, 
                    "relationships": [], 
                    "projects": projects
                }
        else:
            unique_events_map[unique_key] = event_data
            event_details_cache[event_id] = {
                "event_name": event_name, 
                "relationships": [], 
                "projects": projects
            }

    return list(unique_events_map.values()), event_details_cache