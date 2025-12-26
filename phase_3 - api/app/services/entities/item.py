# app/services/entities/item.py
from typing import List, Dict, Any, Tuple
from app.services.common import extract_human_readable_id, calculate_richness

# --- Mapping dictionaries are removed, as data is pre-translated ---

def process_items(raw_items: List[Dict[str, Any]], context: Dict[str, Any], manifestations_data_map: Dict[str, Any], works_data_map: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    unique_items_map = {}
    item_details_cache = {}

    id2projects = context["id2projects"]
    id2label = context["id2label"]
    item_to_manifestation = context["item_to_manifestation"]
    item_to_page_ids = context["item_to_page_ids"]
    page_to_visual_object_ids = context["page_to_visual_object_ids"]
    vo_to_type = context["vo_to_type"]
    item_to_physical_object_ids = context["item_to_physical_object_ids"]
    po_to_type = context["po_to_type"]
    
    # VO Roles (Now containing IDs, not names)
    vo_to_owners = context["vo_to_owners"]
    vo_to_inscribers = context["vo_to_inscribers"]
    vo_to_senders = context["vo_to_senders"]
    vo_to_recipients = context["vo_to_recipients"]
    
    vo_to_name = context["vo_to_name"]
    item_to_owner = context["item_to_owner"] # Now containing IDs
    item_to_shelf_mark = context["item_to_shelf_mark"]
    volume_to_item_ids = context["volume_to_item_ids"]
    volume_to_number_of_volumes = context["volume_to_number_of_volumes"]
    item_to_preservation_status = context["item_to_preservation_status"]
    item_to_material = context["item_to_material"]
    item_to_type = context["item_to_type"]
    entity_to_hypotheses_details = context["entity_to_hypotheses_details"]
    page_to_physical_object_ids = context["page_to_physical_object_ids"]
    page_to_digital_representation = context.get("page_to_digital_representation", {})
    page_to_name = context["page_to_name"]
    manifestation_to_short_title = context["manifestation_to_short_title"]
    page_to_sorting_number = context["page_to_sorting_number"]

    # Name Lookups
    person_to_name = context["person_to_name"]
    institution_to_name = context["institution_to_name"]

    def resolve_names(ids: List[str]) -> List[str]:
        """Helper to resolve a list of IDs to names."""
        names = []
        for oid in ids:
            name = person_to_name.get(oid) or institution_to_name.get(oid) or id2label.get(oid, "Unknown")
            names.append(name)
        return sorted(list(set(names)))

    for item in raw_items:
        item_id = item["_id"]
        
        parent_manifestation_id = item_to_manifestation.get(item_id)
        
        # --- FILTER: Parent Manifestation must have manifestation_has_short_title ---
        if not parent_manifestation_id or parent_manifestation_id not in manifestation_to_short_title:
            continue

        parent_manifestation_details = manifestations_data_map.get(parent_manifestation_id)
        
        # Defaults
        manifestation_title = "Unknown Manifestation"
        expression_id = None
        expression_title = "Unknown Expression"
        work_id = None
        work_title = "Unknown Work"
        authors_from_work = []
        projects = id2projects.get(item_id, ["Unknown Project"])
        classifications = []
        type_of_expression = []
        language = []
        publication_place = None
        publication_date = None
        publication_date_range = None
        publication_start_year = None
        publication_end_year = None
        classifications_normalized = set()
        type_of_expression_normalized = set()
        language_normalized = set()
        publication_place_normalized = set()
        work_title_normalized = set()
        author_search_terms_normalized = set()

        if parent_manifestation_details:
            manifestation_title = parent_manifestation_details["manifestation_title"]
            expression_id = parent_manifestation_details["expression_id"]
            expression_title = parent_manifestation_details["expression_title"]
            work_id = parent_manifestation_details["work_id"]
            work_title = parent_manifestation_details["work_title"]
            authors_from_work = parent_manifestation_details.get("authors", [])
            if not id2projects.get(item_id):
                projects = parent_manifestation_details.get("projects", ["Unknown Project"])
            
            classifications = parent_manifestation_details.get("classifications", [])
            type_of_expression = parent_manifestation_details.get("type_of_expression", [])
            language = parent_manifestation_details.get("language", [])
            publication_place = parent_manifestation_details.get("publication_place")
            publication_date = parent_manifestation_details.get("publication_date")
            publication_date_range = parent_manifestation_details.get("publication_date_range")
            publication_start_year = parent_manifestation_details.get("publication_start_year")
            publication_end_year = parent_manifestation_details.get("publication_end_year")
            
            classifications_normalized = parent_manifestation_details.get("classifications_normalized", set())
            type_of_expression_normalized = parent_manifestation_details.get("type_of_expression_normalized", set())
            language_normalized = parent_manifestation_details.get("language_normalized", set())
            publication_place_normalized = parent_manifestation_details.get("publication_place_normalized", set())
            work_title_normalized = parent_manifestation_details.get("work_title_normalized", set())
            author_search_terms_normalized = parent_manifestation_details.get("author_search_terms_normalized", set())

        pages_with_details, all_vo_owners, all_vo_inscribers, all_vo_senders, all_vo_recipients = [], set(), set(), set(), set()
        excluded_vo_types, annotated_pages_count, annotation_type_counts = {"watermark", "drawing", "decoration"}, 0, {}
        
        all_vo_types = set()
        for p_id in item_to_page_ids.get(item_id, []):
            for vo_id in page_to_visual_object_ids.get(p_id, []):
                all_vo_types.update(vo_to_type.get(vo_id, []))

        all_po_types = set()
        for po_id in item_to_physical_object_ids.get(item_id, []):
            all_po_types.update(po_to_type.get(po_id, []))

        # --- DIGITALIZATION CHECK ---
        item_has_digital_representation = False

        for p_id in item_to_page_ids.get(item_id, []):
            has_scan = p_id in page_to_digital_representation
            if has_scan:
                item_has_digital_representation = True

            vo_ids_on_page = page_to_visual_object_ids.get(p_id, [])
            if vo_ids_on_page and any(t.lower() not in excluded_vo_types for vo_id in vo_ids_on_page for t in vo_to_type.get(vo_id, [""])):
                annotated_pages_count += 1
            visual_objects_on_page = []
            for vo_id in vo_ids_on_page:
                for vo_type in vo_to_type.get(vo_id, []): annotation_type_counts[vo_type] = annotation_type_counts.get(vo_type, 0) + 1
                
                # Resolve IDs to Names here
                owners = resolve_names(vo_to_owners.get(vo_id, []))
                inscribers = resolve_names(vo_to_inscribers.get(vo_id, []))
                senders = resolve_names(vo_to_senders.get(vo_id, []))
                recipients = resolve_names(vo_to_recipients.get(vo_id, []))
                
                all_vo_owners.update(owners); all_vo_inscribers.update(inscribers); all_vo_senders.update(senders); all_vo_recipients.update(recipients)
                visual_objects_on_page.append({"vo_id": vo_id, "vo_name": vo_to_name.get(vo_id, id2label.get(vo_id, "Unknown Visual Object")), "owners": owners, "inscribers": inscribers, "senders": senders, "recipients": recipients})
            
            sorting_number = page_to_sorting_number.get(p_id, None)
            pages_with_details.append({
                "page_id": p_id, 
                "page_number_sorting": sorting_number,
                "page_label": page_to_name.get(p_id, id2label.get(p_id, "Unknown Page")),
                "digital_page": page_to_digital_representation.get(p_id, None),
                "visual_objects": visual_objects_on_page,
                "has_visual_objects": len(visual_objects_on_page) > 0,
                "has_physical_objects": p_id in page_to_physical_object_ids,
                "has_digital_representation": has_scan
            })
        
        # Sort pages by the sorting number, handling None values by placing them at the end.
        pages_with_details.sort(key=lambda p: p['page_number_sorting'] if p['page_number_sorting'] is not None else float('inf'))
        
        # Resolve Item Owners
        item_owners = resolve_names(item_to_owner.get(item_id, []))

        all_item_roles = all_vo_owners | all_vo_inscribers | all_vo_senders | all_vo_recipients
        all_item_roles.update(item_owners) 
        all_people_and_institutions_normalized_item = {p.lower() for p in all_item_roles}

        title_parts = [manifestation_title, item_to_shelf_mark.get(item_id, id2label.get(item_id, "Unknown Item"))]
        
        volume_number = None
        for vol_id in volume_to_item_ids:
            if item_id in volume_to_item_ids[vol_id]:
                volume_number = volume_to_number_of_volumes.get(vol_id)
                break
        if volume_number:
            title_parts.insert(1, f"Vol. {volume_number}")

        po_info = f"{len(item_to_physical_object_ids.get(item_id, []))} physical object(s)"
        if all_po_types:
            po_info += f": {', '.join(sorted(list(all_po_types)))}"

        card = {
            "title": ", ".join(filter(None, title_parts)), 
            "authors": authors_from_work, 
            "date": publication_date_range or publication_date or "<em>None</em>", 
            "physical_object_info": po_info, 
            "annotated_pages_info": f"{annotated_pages_count} of annotated page(s)", 
            "projects": projects
        }

        # --- Values are now pre-translated. No mapping needed. ---
        preservation_status = sorted(list(set(item_to_preservation_status.get(item_id, []))))
        material = sorted(list(set(item_to_material.get(item_id, []))))
        type_of_item = sorted(list(set(item_to_type.get(item_id, []))))

        item_data = {
            "item_id": item_id, 
            "human_readable_id": extract_human_readable_id(item, "i_"),
            "item_label": item_to_shelf_mark.get(item_id, id2label.get(item_id, "Unknown Item")), 
            "manifestation_id": parent_manifestation_id, 
            "manifestation_title": manifestation_title, 
            "expression_id": expression_id, 
            "expression_title": expression_title, 
            "work_id": work_id, 
            "work_title": work_title, 
            "projects": projects, 
            "authors": authors_from_work, 
            "physical_object_count": len(item_to_physical_object_ids.get(item_id, [])), 
            "annotated_pages_count": annotated_pages_count, 
            "annotation_type_counts": annotation_type_counts, 
            "preservation_status": preservation_status, 
            "owner": item_owners, 
            "material": material, 
            "type_of_item": type_of_item, 
            "pages": pages_with_details, 
            "has_digital_representation": item_has_digital_representation, # Added for filtering
            "hypotheses": entity_to_hypotheses_details.get(item_id, []), 
            "visual_object_owners": sorted(list(all_vo_owners)), 
            "visual_object_inscribers": sorted(list(all_vo_inscribers)), 
            "visual_object_senders": sorted(list(all_vo_senders)), 
            "visual_object_recipients": sorted(list(all_vo_recipients)), 
            "classifications": classifications, 
            "type_of_expression": type_of_expression, 
            "language": language, 
            "publication_place": publication_place, 
            "publication_date": publication_date, 
            "publication_date_range": publication_date_range, 
            "publication_start_year": publication_start_year, 
            "publication_end_year": publication_end_year, 
            "preservation_status_normalized": {s.lower() for s in preservation_status}, 
            "owner_normalized": {o.lower() for o in item_owners}, 
            "material_normalized": {m.lower() for m in material}, 
            "type_of_item_normalized": {t.lower() for t in type_of_item}, 
            "visual_object_owners_normalized": {o.lower() for o in all_vo_owners}, 
            "visual_object_inscribers_normalized": {i.lower() for i in all_vo_inscribers}, 
            "visual_object_senders_normalized": {s.lower() for s in all_vo_senders}, 
            "visual_object_recipients_normalized": {r.lower() for r in all_vo_recipients}, 
            "all_people_and_institutions_normalized": all_people_and_institutions_normalized_item, 
            "classifications_normalized": classifications_normalized, 
            "type_of_expression_normalized": type_of_expression_normalized, 
            "language_normalized": language_normalized, 
            "publication_place_normalized": publication_place_normalized, 
            "work_title_normalized": work_title_normalized, 
            "author_search_terms_normalized": author_search_terms_normalized,
            "type_of_visual_object_normalized": {t.lower() for t in all_vo_types},
            "type_of_physical_object_normalized": {t.lower() for t in all_po_types},
            "card": card
        }

        # --- DEDUPLICATION LOGIC ---
        # Key is now (Title, HumanReadableID)
        item_title = card["title"]
        unique_key = (item_title, item_data["human_readable_id"])

        if unique_key in unique_items_map:
            existing = unique_items_map[unique_key]
            if calculate_richness(item_data) > calculate_richness(existing):
                unique_items_map[unique_key] = item_data
                item_details_cache[item_id] = {
                    "item_label": item_to_shelf_mark.get(item_id, id2label.get(item_id, "Unknown Item")), 
                    "relationships": [], 
                    "pages": pages_with_details, 
                    "projects": projects
                }
        else:
            unique_items_map[unique_key] = item_data
            item_details_cache[item_id] = {
                "item_label": item_to_shelf_mark.get(item_id, id2label.get(item_id, "Unknown Item")), 
                "relationships": [], 
                "pages": pages_with_details, 
                "projects": projects
            }

    return list(unique_items_map.values()), item_details_cache