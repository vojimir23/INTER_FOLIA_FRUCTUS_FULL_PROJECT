# app/services/entities/work.py
from typing import List, Dict, Any, Tuple
from app.services.common import extract_human_readable_id, calculate_richness

def process_works(raw_works: List[Dict[str, Any]], context: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    unique_works_map = {} # Key: (Title, HumanReadableID), Value: Work Data
    work_details_cache = {}
    
    id2projects = context["id2projects"]
    work_to_expression_ids = context["work_to_expression_ids"]
    work_to_author_ids = context["work_to_author_ids"]
    id_to_collection = context["id_to_collection"]
    person_to_name = context["person_to_name"]
    institution_to_name = context["institution_to_name"]
    id2label = context["id2label"]
    work_to_classification = context["work_to_classification"]
    work_to_uniform_title = context["work_to_uniform_title"]
    entity_to_hypotheses_details = context["entity_to_hypotheses_details"]
    person_to_aliases = context["person_to_aliases"]

    # Helpers for author details
    person_to_birth_date = context["person_to_birth_date"]
    person_to_birth_date_notes = context["person_to_birth_date_notes"]
    person_to_death_date = context["person_to_death_date"]
    person_to_death_date_notes = context["person_to_death_date_notes"]

    for work in raw_works:
        work_id = work["_id"]
        
        # --- FILTER: Must have work_has_uniform_title ---
        if work_id not in work_to_uniform_title:
            continue

        work_title = work_to_uniform_title[work_id]

        # Build Expressions List for Search Object
        expressions_list = []
        for exp_id in sorted(list(set(work_to_expression_ids.get(work_id, [])))):
            expressions_list.append({
                "expression_id": exp_id, 
                "expression_title": id2label.get(exp_id, "Unknown Expression")
            })

        # Build Authors List
        authors_list = []
        for author_id in sorted(list(set(work_to_author_ids.get(work_id, [])))):
            coll = id_to_collection.get(author_id)
            if coll == "person":
                authors_list.append({
                    "id": author_id,
                    "name": person_to_name.get(author_id, id2label.get(author_id, "Unknown Author")),
                    "type": "person",
                    "birth_date": person_to_birth_date.get(author_id),
                    "birth_date_notes": person_to_birth_date_notes.get(author_id),
                    "death_date": person_to_death_date.get(author_id),
                    "death_date_notes": person_to_death_date_notes.get(author_id)
                })
            elif coll == "institution":
                authors_list.append({
                    "id": author_id,
                    "name": institution_to_name.get(author_id, id2label.get(author_id, "Unknown Institution")),
                    "type": "institution"
                })
        
        authors_list = sorted(authors_list, key=lambda x: x['name'])
        author_display_names = [author['name'] for author in authors_list]
        
        # Search Terms
        author_search_terms = set(author_display_names)
        for author_id in work_to_author_ids.get(work_id, []):
            if id_to_collection.get(author_id) == "person": 
                author_search_terms.update(person_to_aliases.get(author_id, []))
        
        classifications = sorted(list(set(work_to_classification.get(work_id, []))))
        projects = id2projects.get(work_id, ["Unknown Project"])
        
        card = {
            "title": work_title, 
            "authors": authors_list, 
            "classifications": ", ".join(classifications) if classifications else "<em>None</em>", 
            "projects": projects
        }

        work_data = {
            "work_id": work_id, 
            "human_readable_id": extract_human_readable_id(work, "w_"),
            "work_title": work_title, 
            "projects": projects, 
            "expressions": expressions_list, 
            "authors": authors_list, 
            "classifications": classifications, 
            "hypotheses": entity_to_hypotheses_details.get(work_id, []), 
            "author_search_terms": sorted(list(author_search_terms)), 
            "classifications_normalized": {c.lower() for c in classifications}, 
            "author_search_terms_normalized": {t.lower() for t in author_search_terms}, 
            "work_title_normalized": {work_title.lower()} if work_title else set(), 
            "card": card
        }
        
        # --- DEDUPLICATION LOGIC ---
        # Key is now (Title, HumanReadableID)
        unique_key = (work_title, work_data["human_readable_id"])

        if unique_key in unique_works_map:
            existing_work = unique_works_map[unique_key]
            if calculate_richness(work_data) > calculate_richness(existing_work):
                unique_works_map[unique_key] = work_data
                # Update cache pointer to the richer ID
                work_details_cache[work_id] = {
                    "work_title": work_title, 
                    "relationships": [], 
                    "projects": projects
                }
        else:
            unique_works_map[unique_key] = work_data
            work_details_cache[work_id] = {
                "work_title": work_title, 
                "relationships": [], 
                "projects": projects
            }

    return list(unique_works_map.values()), work_details_cache