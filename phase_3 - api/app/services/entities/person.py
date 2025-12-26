# app/services/entities/person.py
import re
from typing import List, Dict, Any, Tuple
from app.services.common import extract_human_readable_id, calculate_richness
from app.config import YEAR_RE

def process_persons(raw_persons: List[Dict[str, Any]], context: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    unique_persons_map = {}
    person_details_cache = {}
    
    id2projects = context["id2projects"]
    person_to_name = context["person_to_name"]
    id2label = context["id2label"] # Fallback for name
    person_to_aliases = context["person_to_aliases"]
    person_to_roles = context["person_to_roles"]
    person_to_gender = context["person_to_gender"]
    person_to_birth_date = context["person_to_birth_date"]
    person_to_birth_date_notes = context["person_to_birth_date_notes"]
    person_to_death_date = context["person_to_death_date"]
    person_to_death_date_notes = context["person_to_death_date_notes"]
    entity_to_hypotheses_details = context["entity_to_hypotheses_details"]
    
    # Updated regex to recognize English (bc, b.c.) and Italian (a.C.) abbreviations
    bc_pattern = re.compile(r'\b(bc|b\.c\.|a\.C\.)\b', re.IGNORECASE)

    for person in raw_persons:
        person_id = person["_id"]
        
        # --- FILTER: Must have person_has_name ---
        if person_id not in person_to_name:
            continue

        person_name = person_to_name[person_id]
        
        aliases = person_to_aliases.get(person_id, [])
        search_terms = {person_name.lower()} | {alias.lower() for alias in aliases}
        roles = list(person_to_roles.get(person_id, {}).keys())
        gender = person_to_gender.get(person_id, "")
        
        birth_date_str = person_to_birth_date.get(person_id)
        death_date_str = person_to_death_date.get(person_id)
        birth_year = int(match.group(0)) if birth_date_str and (match := YEAR_RE.search(birth_date_str)) else None
        death_year = int(match.group(0)) if death_date_str and (match := YEAR_RE.search(death_date_str)) else None

        birth_date_notes = person_to_birth_date_notes.get(person_id, "")
        death_date_notes = person_to_death_date_notes.get(person_id, "")

        birth_era = "BC" if bc_pattern.search(birth_date_notes) else "AD"
        death_era = "BC" if bc_pattern.search(death_date_notes) else "AD"

        if birth_year is not None and birth_era == "BC":
            birth_year = -birth_year
        if death_year is not None and death_era == "BC":
            death_year = -death_year

        projects = id2projects.get(person_id, ["Unknown Project"])

        current_person_data = {
            "person_id": person_id, 
            "human_readable_id": extract_human_readable_id(person, "p_"),
            "person_name": person_name, 
            "projects": projects, 
            "hypotheses": entity_to_hypotheses_details.get(person_id, []), 
            "person_name_normalized": search_terms, 
            "roles": roles, 
            "roles_normalized": {role.lower() for role in roles}, 
            "gender": gender, 
            "gender_normalized": {gender.lower()} if gender else set(),
            "birth_year": birth_year,
            "death_year": death_year,
            "birth_era": birth_era,
            "death_era": death_era,
            "card": {
                "name": person_name, 
                "birth_date": person_to_birth_date.get(person_id), 
                "birth_date_notes": person_to_birth_date_notes.get(person_id), 
                "death_date": person_to_death_date.get(person_id), 
                "death_date_notes": person_to_death_date_notes.get(person_id), 
                "projects": projects
            }
        }
        
        # --- DEDUPLICATION LOGIC ---
        # Key is now (Name, HumanReadableID)
        unique_key = (person_name, current_person_data["human_readable_id"])

        if unique_key in unique_persons_map:
            existing = unique_persons_map[unique_key]
            if calculate_richness(current_person_data) > calculate_richness(existing):
                unique_persons_map[unique_key] = current_person_data
                person_details_cache[person_id] = {
                    "person_name": person_name, 
                    "relationships": [], 
                    "roles_with_entities": {}, 
                    "projects": projects
                }
        else:
            unique_persons_map[unique_key] = current_person_data
            person_details_cache[person_id] = {
                "person_name": person_name, 
                "relationships": [], 
                "roles_with_entities": {}, 
                "projects": projects
            }

    return list(unique_persons_map.values()), person_details_cache