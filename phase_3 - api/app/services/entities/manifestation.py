# app/services/entities/manifestation.py
from typing import List, Dict, Any, Tuple
from app.services.common import parse_date_to_range, extract_human_readable_id, calculate_richness

def process_manifestations(raw_manifestations: List[Dict[str, Any]], context: Dict[str, Any], works_data_map: Dict[str, Any], expressions_data_map: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, Any], Dict[str, Any]]:
    unique_manifestations_map = {}
    unique_volumes_map = {}
    manifestation_details_cache = {}
    manifestation_volume_details_cache = {}

    id2projects = context["id2projects"]
    id2label = context["id2label"]
    id_to_collection = context["id_to_collection"]
    volume_ids = context["volume_ids"]
    
    # Maps
    volume_to_parent_manifestation_id = context["volume_to_parent_manifestation_id"]
    manifestation_to_expression = context["manifestation_to_expression"]
    volume_to_short_title = context["volume_to_short_title"]
    volume_to_number_of_volumes = context["volume_to_number_of_volumes"]
    volume_to_volume_title = context["volume_to_volume_title"]
    volume_to_date = context["volume_to_date"]
    volume_to_publication_date_range = context["volume_to_publication_date_range"]
    volume_to_item_ids = context["volume_to_item_ids"]
    volume_to_place = context["volume_to_place"]
    volume_to_publishers = context["volume_to_publishers"]
    
    entity_to_hypotheses_details = context["entity_to_hypotheses_details"]
    
    manifestation_to_place = context["manifestation_to_place"]
    manifestation_to_date = context["manifestation_to_date"]
    manifestation_to_publication_date_range = context["manifestation_to_publication_date_range"]
    manifestation_to_publishers = context["manifestation_to_publishers"]
    manifestation_to_editors = context["manifestation_to_editors"]
    manifestation_to_correctors = context["manifestation_to_correctors"]
    manifestation_to_sponsors = context["manifestation_to_sponsors"]
    manifestation_to_short_title = context["manifestation_to_short_title"]
    manifestation_to_item_ids = context["manifestation_to_item_ids"]
    manifestation_to_volume_ids = context["manifestation_to_volume_ids"]

    person_to_name = context["person_to_name"]
    institution_to_name = context["institution_to_name"]
    person_to_birth_date = context["person_to_birth_date"]
    person_to_birth_date_notes = context["person_to_birth_date_notes"]
    person_to_death_date = context["person_to_death_date"]
    person_to_death_date_notes = context["person_to_death_date_notes"]
    institution_to_founding_date = context["institution_to_founding_date"]
    institution_to_dissolution_date = context["institution_to_dissolution_date"]

    # Create a lookup map for raw documents to access descriptions for human_readable_id
    id_to_raw_doc = {doc["_id"]: doc for doc in raw_manifestations}

    def format_contributors(ids):
        res = []
        for cid in ids:
            coll = id_to_collection.get(cid)
            if coll == "person":
                res.append({
                    "id": cid, "name": person_to_name.get(cid, id2label.get(cid, "Unknown")), "type": "person",
                    "birth_date": person_to_birth_date.get(cid), "birth_date_notes": person_to_birth_date_notes.get(cid),
                    "death_date": person_to_death_date.get(cid), "death_date_notes": person_to_death_date_notes.get(cid)
                })
            elif coll == "institution":
                res.append({
                    "id": cid, "name": institution_to_name.get(cid, id2label.get(cid, "Unknown")), "type": "institution",
                    "founding_date": institution_to_founding_date.get(cid), "dissolution_date": institution_to_dissolution_date.get(cid)
                })
        return sorted(res, key=lambda x: x['name'])

    for man in raw_manifestations:
        man_id = man["_id"]
        
        # --- VOLUME PROCESSING ---
        if man_id in volume_ids:
            # Filter: Volume must have short title (assuming same rule as manifestation)
            if man_id not in volume_to_short_title:
                continue

            parent_manifestation_id = volume_to_parent_manifestation_id.get(man_id)
            parent_expression_id = manifestation_to_expression.get(parent_manifestation_id)
            parent_expression_details = expressions_data_map.get(parent_expression_id)
            
            work_id = None
            authors = []
            projects = id2projects.get(man_id, ["Unknown Project"])

            if parent_expression_details:
                work_id = parent_expression_details.get("work_id")
                if work_id and works_data_map.get(work_id):
                    authors = works_data_map[work_id].get("authors", [])
                if not id2projects.get(man_id):
                    projects = parent_expression_details.get("projects", ["Unknown Project"])

            # Fetch Volume Specific Data
            publication_place = volume_to_place.get(man_id)
            publication_date = volume_to_date.get(man_id)
            publication_date_range = volume_to_publication_date_range.get(man_id)
            
            publishers = format_contributors(volume_to_publishers.get(man_id, []))
            # Note: Editors, Correctors, Sponsors for volumes are stored in the main manifestation maps 
            # in relations.py, keyed by the volume ID.
            editors = format_contributors(manifestation_to_editors.get(man_id, []))
            correctors = format_contributors(manifestation_to_correctors.get(man_id, []))
            sponsors = format_contributors(manifestation_to_sponsors.get(man_id, []))

    
            human_readable_id = extract_human_readable_id(man, "m_vol_")
            volume_number = volume_to_number_of_volumes.get(man_id)
            formatted_volume_number = None
            if volume_number:
                if human_readable_id and human_readable_id.startswith("m_vol_"):
                    formatted_volume_number = f"Vol. {volume_number}"
                else:
                    formatted_volume_number = volume_number

            title_parts = [
                volume_to_short_title.get(man_id), 
                formatted_volume_number, 
                volume_to_volume_title.get(man_id), 
                publication_date
            ]
     
            composite_title = ", ".join(filter(None, title_parts))
            
            card = {
                "title": composite_title, 
                "authors": authors,
                "publishers": publishers,
                "place": publication_place,
                "date": publication_date_range or publication_date or "<em>None</em>",
                "projects": projects
            }

            vol_data = {
                "manifestation_volume_id": man_id, 
                "human_readable_id": human_readable_id,
                "manifestation_volume_title": composite_title, 
                "parent_manifestation_id": parent_manifestation_id, 
                "expression_id": parent_expression_id, 
                "work_id": work_id, 
                "projects": projects, 
                "items": [{"item_id": i, "item_label": id2label.get(i, "Unknown Item")} for i in volume_to_item_ids.get(man_id, [])], 
                "hypotheses": entity_to_hypotheses_details.get(man_id, []), 
                "authors": authors,
                "publishers": publishers,
                "editors": editors,
                "correctors": correctors,
                "sponsors": sponsors,
                "publication_place": publication_place,
                "publication_date": publication_date,
                "publication_date_range": publication_date_range,
                "card": card
            }
            
            # Deduplication for Volumes
            # Key is now (Title, HumanReadableID)
            unique_key = (composite_title, vol_data["human_readable_id"])

            if unique_key in unique_volumes_map:
                existing = unique_volumes_map[unique_key]
                if calculate_richness(vol_data) > calculate_richness(existing):
                    unique_volumes_map[unique_key] = vol_data
                    manifestation_volume_details_cache[man_id] = {
                        "manifestation_volume_title": composite_title, 
                        "relationships": [], 
                        "projects": projects
                    }
            else:
                unique_volumes_map[unique_key] = vol_data
                manifestation_volume_details_cache[man_id] = {
                    "manifestation_volume_title": composite_title, 
                    "relationships": [], 
                    "projects": projects
                }
            
            id_to_collection[man_id] = "manifestation_volume"
            continue

        # --- MANIFESTATION PROCESSING ---
        
        # --- FILTER: Must have manifestation_has_short_title ---
        if man_id not in manifestation_to_short_title:
            continue

        parent_expression_id = manifestation_to_expression.get(man_id)
        parent_expression_details = expressions_data_map.get(parent_expression_id)
        
        # Defaults
        expression_title = "Unknown Expression"
        work_id = None
        work_title = "Unknown Work"
        authors = []
        author_search_terms_normalized = set()
        classifications = []
        classifications_normalized = set()
        type_of_expression = []
        type_of_expression_normalized = set()
        language = []
        language_normalized = set()
        work_title_normalized = set()
        projects = id2projects.get(man_id, ["Unknown Project"])
        
        # --- NEW: Expression Roles Defaults ---
        translators = []
        expression_editors = []
        scriptwriters = []
        compositors = []
        reviewers = []
        other_secondary_roles = []

        if parent_expression_details:
            expression_title = parent_expression_details["expression_title"]
            work_id = parent_expression_details["work_id"]
            work_title = parent_expression_details["work_title"]
            authors = parent_expression_details.get("authors", [])
            author_search_terms_normalized = parent_expression_details.get("author_search_terms_normalized", set())
            classifications = parent_expression_details.get("classifications", [])
            classifications_normalized = parent_expression_details.get("classifications_normalized", set())
            type_of_expression = parent_expression_details.get("type_of_expression", [])
            type_of_expression_normalized = parent_expression_details.get("type_of_expression_normalized", set())
            language = parent_expression_details.get("language", [])
            language_normalized = parent_expression_details.get("language_normalized", set())
            work_title_normalized = parent_expression_details.get("work_title_normalized", set())
            
            # --- NEW: Inherit Expression Roles ---
            translators = parent_expression_details.get("translators", [])
            expression_editors = parent_expression_details.get("expression_editors", [])
            scriptwriters = parent_expression_details.get("scriptwriters", [])
            compositors = parent_expression_details.get("compositors", [])
            reviewers = parent_expression_details.get("reviewers", [])
            other_secondary_roles = parent_expression_details.get("other_secondary_roles", [])

            if not id2projects.get(man_id):
                projects = parent_expression_details.get("projects", ["Unknown Project"])

        publication_place = manifestation_to_place.get(man_id)
        publication_date = manifestation_to_date.get(man_id)
        start_year, end_year = parse_date_to_range(publication_date)

        publication_place_normalized_set = set()
        if publication_place and publication_place.get("place_name"):
            place_name = publication_place["place_name"]
            if place_name.lower().startswith(('http://', 'https://', 'www.')):
                publication_place_normalized_set = {"web"}
            else:
                publication_place_normalized_set = {place_name.lower()}

        publishers = format_contributors(manifestation_to_publishers.get(man_id, []))
        editors = format_contributors(manifestation_to_editors.get(man_id, []))
        correctors = format_contributors(manifestation_to_correctors.get(man_id, []))
        sponsors = format_contributors(manifestation_to_sponsors.get(man_id, []))
        
        all_manifestation_roles = publishers + editors + correctors + sponsors
        all_people_and_institutions_normalized_manif = {p['name'].lower() for p in all_manifestation_roles}

        title_parts = [manifestation_to_short_title.get(man_id)]
        if man_id in manifestation_to_volume_ids:
            if pr := manifestation_to_publication_date_range.get(man_id): title_parts.append(pr)
        elif publication_date: title_parts.append(publication_date)
        composite_title = ", ".join(filter(None, title_parts))
        
        card = {
            "title": composite_title, 
            "authors": authors, 
            "publishers": publishers, 
            "place": publication_place, 
            "projects": projects
        }

        # Prepare volumes list with human_readable_id lookup AND rich data
        volumes_data = []
        for v_id in manifestation_to_volume_ids.get(man_id, []):
            v_entry = {"volume_id": v_id}
            # Lookup volume document to extract human_readable_id
            if v_doc := id_to_raw_doc.get(v_id):
                if h_id := extract_human_readable_id(v_doc, "m_vol_"):
                    v_entry["human_readable_id"] = h_id
            
            # --- NEW: Add rich data for the volume nested inside manifestation ---
            v_entry["publishers"] = format_contributors(volume_to_publishers.get(v_id, []))
            v_entry["place"] = volume_to_place.get(v_id)
            v_entry["date"] = volume_to_date.get(v_id)
            
            volumes_data.append(v_entry)

        man_data = {
            "manifestation_id": man_id, 
            "human_readable_id": extract_human_readable_id(man, "m_"),
            "manifestation_title": composite_title, 
            "expression_id": parent_expression_id, 
            "expression_title": expression_title, 
            "work_id": work_id, 
            "work_title": work_title, 
            "projects": projects, 
            "publication_place": publication_place, 
            "publication_date": publication_date, 
            "publication_date_range": manifestation_to_publication_date_range.get(man_id), 
            "publication_start_year": start_year, 
            "publication_end_year": end_year,
            "items": [{"item_id": i, "item_label": id2label.get(i, "Unknown Item")} for i in manifestation_to_item_ids.get(man_id, [])], 
            "volumes": volumes_data, 
            "publisher": publishers, 
            "editor": editors, 
            "corrector": correctors, 
            "sponsor": sponsors, 
            "authors": authors, 
            "author_search_terms_normalized": author_search_terms_normalized,
            "hypotheses": entity_to_hypotheses_details.get(man_id, []), 
            "classifications": classifications, 
            "type_of_expression": type_of_expression, 
            "language": language, 
            # --- NEW: Inherited Expression Roles ---
            "translators": translators,
            "expression_editors": expression_editors,
            "scriptwriters": scriptwriters,
            "compositors": compositors,
            "reviewers": reviewers,
            "other_secondary_roles": other_secondary_roles,
            # ---------------------------------------
            "publication_place_normalized": publication_place_normalized_set,
            "publisher_normalized": {p['name'].lower() for p in publishers}, 
            "editor_normalized": {e['name'].lower() for e in editors}, 
            "corrector_normalized": {c['name'].lower() for c in correctors}, 
            "sponsor_normalized": {s['name'].lower() for s in sponsors}, 
            "all_people_and_institutions_normalized": all_people_and_institutions_normalized_manif,
            "classifications_normalized": classifications_normalized, 
            "type_of_expression_normalized": type_of_expression_normalized, 
            "language_normalized": language_normalized, 
            "work_title_normalized": work_title_normalized, 
            "card": card
        }

        # --- DEDUPLICATION LOGIC ---
        # Key is now (Title, HumanReadableID)
        unique_key = (composite_title, man_data["human_readable_id"])

        if unique_key in unique_manifestations_map:
            existing = unique_manifestations_map[unique_key]
            if calculate_richness(man_data) > calculate_richness(existing):
                unique_manifestations_map[unique_key] = man_data
                manifestation_details_cache[man_id] = {
                    "manifestation_title": composite_title, 
                    "relationships": [], 
                    "projects": projects
                }
        else:
            unique_manifestations_map[unique_key] = man_data
            manifestation_details_cache[man_id] = {
                "manifestation_title": composite_title, 
                "relationships": [], 
                "projects": projects
            }

    return list(unique_manifestations_map.values()), list(unique_volumes_map.values()), manifestation_details_cache, manifestation_volume_details_cache