# app/services/entities/physical_object.py
from typing import List, Dict, Any, Tuple
from app.services.common import get_contributor_details, extract_human_readable_id, parse_date_to_range, calculate_richness
from collections import defaultdict

def process_physical_objects(raw_pos: List[Dict[str, Any]], context: Dict[str, Any], items_data_map: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    unique_po_map = {}
    physical_object_details_cache = {}

    # --- CONTEXT & MAPS ---
    id2projects = context["id2projects"]
    physical_object_to_name = context["physical_object_to_name"]
    id2label = context["id2label"]
    po_to_type = context["po_to_type"]
    person_to_roles = context["person_to_roles"]
    entity_to_hypotheses_details = context["entity_to_hypotheses_details"]
    
    # Maps for card logic
    item_to_physical_object_ids = context["item_to_physical_object_ids"]
    page_to_physical_object_ids = context.get("page_to_physical_object_ids", {})
    page_to_item_map = context.get("page_to_item_map", {})
    page_to_name = context.get("page_to_name", {})
    physical_object_to_description = context.get("physical_object_to_description", {})
    
    # This map is crucial for PO_PAG_PO_ logic: finding the PO that contains the Page
    page_to_po_page_parent_map = context.get("page_to_po_page_parent_map", {}) 

    # --- NEW MAPS FOR FILTERS ---
    po_to_place = context["po_to_place"]
    po_to_date = context["po_to_date"]
    po_to_insertion_type = context["po_to_insertion_type"] # Newly added
    page_to_visual_object_ids = context["page_to_visual_object_ids"]
    vo_to_owners_rel = context["vo_to_owners"]
    vo_to_inscribers_rel = context["vo_to_inscribers"]
    vo_to_senders_rel = context["vo_to_senders"]
    vo_to_recipients_rel = context["vo_to_recipients"]
    person_to_name = context["person_to_name"]
    institution_to_name = context["institution_to_name"]
    # --------------------------
    
    page_to_digital_representation = context.get("page_to_digital_representation", {})

    # --- REVERSE MAPS ---
    # Map PO -> Item (Directly linked)
    po_to_item_map = {po_id: item_id for item_id, po_ids in item_to_physical_object_ids.items() for po_id in po_ids}
    # Map PO -> Page (Where PO is inside a Page)
    po_to_page_map = {po_id: page_id for page_id, po_ids in page_to_physical_object_ids.items() for po_id in po_ids}

    po_to_creators = defaultdict(list)
    po_to_owners = defaultdict(list)
    for person_id, roles in person_to_roles.items():
        for po_id in roles.get("Creator of PO", []):
            po_to_creators[po_id].append(person_id)
        for po_id in roles.get("Owner of PO", []):
            po_to_owners[po_id].append(person_id)

    def resolve_names(ids: List[str]) -> List[str]:
        names = []
        for oid in ids:
            name = person_to_name.get(oid) or institution_to_name.get(oid) or id2label.get(oid, "Unknown")
            names.append(name)
        return sorted(list(set(names)))

    for po in raw_pos:
        po_id = po["_id"]
        
        if po_id not in po_to_type:
            continue

        projects = id2projects.get(po_id, ["Unknown Project"])
        
        # --- INHERITANCE LOGIC ---
        parent_item_id = po_to_item_map.get(po_id)
        parent_item_details = items_data_map.get(parent_item_id) if parent_item_id else None

        # Inherit projects if unknown
        if projects == ["Unknown Project"] and parent_item_details:
            projects = parent_item_details.get("projects", ["Unknown Project"])

        # Defaults for inherited data
        inherited_fields = {
            "authors": [], "classifications": [], "type_of_expression": [], "language": [],
            "publication_place": None, "publication_date": None, "publication_date_range": None,
            "publication_start_year": None, "publication_end_year": None, "owner": [],
            "work_title": None, "work_title_normalized": set(), "author_search_terms_normalized": set(),
            "classifications_normalized": set(), "type_of_expression_normalized": set(),
            "language_normalized": set(), "publication_place_normalized": set(), "owner_normalized": set()
        }

        if parent_item_details:
            for key in inherited_fields:
                if key in parent_item_details:
                    inherited_fields[key] = parent_item_details[key]
        
        # --- VISUAL OBJECT ROLE AGGREGATION ---
        all_vo_owners, all_vo_inscribers, all_vo_senders, all_vo_recipients = set(), set(), set(), set()
        parent_page_id = po_to_page_map.get(po_id)
        if parent_page_id:
            vo_ids_on_page = page_to_visual_object_ids.get(parent_page_id, [])
            for vo_id in vo_ids_on_page:
                all_vo_owners.update(resolve_names(vo_to_owners_rel.get(vo_id, [])))
                all_vo_inscribers.update(resolve_names(vo_to_inscribers_rel.get(vo_id, [])))
                all_vo_senders.update(resolve_names(vo_to_senders_rel.get(vo_id, [])))
                all_vo_recipients.update(resolve_names(vo_to_recipients_rel.get(vo_id, [])))

        po_name = physical_object_to_name.get(po_id, id2label.get(po_id, "Unknown PO"))
        types = sorted(list(set(po_to_type.get(po_id, []))))
        type_str = ", ".join(types)
        po_title = f"{type_str} - {po_name}"

        creators = sorted([d for d in [get_contributor_details(c_id, context) for c_id in po_to_creators.get(po_id, [])] if d], key=lambda x: x['name'])
        owners = sorted([d for d in [get_contributor_details(o_id, context) for o_id in po_to_owners.get(po_id, [])] if d], key=lambda x: x['name'])

        # --- NEW FILTER DATA ---
        place = po_to_place.get(po_id)
        date_str = po_to_date.get(po_id)
        start_year, end_year = parse_date_to_range(date_str)
        insertion_types = sorted(list(set(po_to_insertion_type.get(po_id, []))))

        has_digital_representation = False
        if parent_page_id:
            has_digital_representation = parent_page_id in page_to_digital_representation
        elif parent_item_details:
            has_digital_representation = parent_item_details.get("has_digital_representation", False)

        # --- CARD LOGIC ---
        human_readable_id = extract_human_readable_id(po, ("PO_", "PO_PAG_"))
        
        card = {
            "title": po_title,
            "type_of_physical_object": type_str,
            "projects": projects,
            "human_readable_id": human_readable_id # THIS IS THE FIX
        }

        if human_readable_id:
            if human_readable_id.startswith("PO_PAG_PO_"):
                if parent_page_id:
                    card["Page name:"] = page_to_name.get(parent_page_id, "Unknown Page")
                    grandparent_po_id = page_to_po_page_parent_map.get(parent_page_id)
                    if grandparent_po_id:
                        gp_name = physical_object_to_name.get(grandparent_po_id, "Unknown PO")
                        gp_types = ", ".join(po_to_type.get(grandparent_po_id, []))
                        card["Contenuto in:"] = f"{gp_types} - {gp_name}"
                        item_id = po_to_item_map.get(grandparent_po_id)
                        if item_id and (item_details := items_data_map.get(item_id)):
                            card["Item:"] = item_details.get("card", {}).get("title", "Unknown Item")
                    else:
                        item_id = page_to_item_map.get(parent_page_id)
                        if item_id and (item_details := items_data_map.get(item_id)):
                            card["Item:"] = item_details.get("card", {}).get("title", "Unknown Item")
            elif human_readable_id.startswith("PO_PAG_"):
                if parent_page_id:
                    card["Page name:"] = page_to_name.get(parent_page_id, "Unknown Page")
                    parent_item_id_from_page = page_to_item_map.get(parent_page_id)
                    if not parent_item_id_from_page:
                        parent_po_id = page_to_po_page_parent_map.get(parent_page_id)
                        if parent_po_id:
                            parent_item_id_from_page = po_to_item_map.get(parent_po_id)
                    if parent_item_id_from_page and (item_details := items_data_map.get(parent_item_id_from_page)):
                        card["Item:"] = item_details.get("card", {}).get("title", "Unknown Item")
            elif human_readable_id.startswith("PO_IND_"):
                description = physical_object_to_description.get(po_id)
                if description:
                    card["Descrizione:"] = description
            elif human_readable_id.startswith("PO_"):
                if parent_item_id and (item_details := items_data_map.get(parent_item_id)):
                    card["Item:"] = item_details.get("card", {}).get("title", "Unknown Item")

        po_data = {
            "physical_object_id": po_id, 
            "human_readable_id": human_readable_id,
            "physical_object_name": po_title, 
            "projects": projects,
            "type_of_physical_object": types,
            "creators": creators,
            "owners": owners,
            "hypotheses": entity_to_hypotheses_details.get(po_id, []),
            
            # --- NEW & INHERITED FIELDS FOR FILTERING ---
            "place": place,
            "date": date_str,
            "start_year": start_year,
            "end_year": end_year,
            "insertion_type": insertion_types,
            "has_digital_representation": has_digital_representation,
            **inherited_fields,
            "visual_object_owners": sorted(list(all_vo_owners)),
            "visual_object_inscribers": sorted(list(all_vo_inscribers)),
            "visual_object_senders": sorted(list(all_vo_senders)),
            "visual_object_recipients": sorted(list(all_vo_recipients)),
            # --------------------------------------------

            # --- NORMALIZED FIELDS ---
            "type_of_physical_object_normalized": {t.lower() for t in types},
            "creators_normalized": {c['name'].lower() for c in creators},
            "owners_normalized": {o['name'].lower() for o in owners},
            "all_people_and_institutions_normalized": {p['name'].lower() for p in creators + owners},
            "physical_object_place_normalized": {place["place_name"].lower()} if place else set(),
            "insertion_type_normalized": {t.lower() for t in insertion_types},
            "visual_object_owners_normalized": {o.lower() for o in all_vo_owners},
            "visual_object_inscribers_normalized": {i.lower() for i in all_vo_inscribers},
            "visual_object_senders_normalized": {s.lower() for s in all_vo_senders},
            "visual_object_recipients_normalized": {r.lower() for r in all_vo_recipients},
            # -------------------------
            
            "card": card
        }

        unique_key = (po_title, po_data["human_readable_id"])

        if unique_key in unique_po_map:
            existing = unique_po_map[unique_key]
            if calculate_richness(po_data) > calculate_richness(existing):
                unique_po_map[unique_key] = po_data
                physical_object_details_cache[po_id] = {
                    "po_name": po_title,
                    "relationships": [], 
                    "projects": projects
                }
        else:
            unique_po_map[unique_key] = po_data
            physical_object_details_cache[po_id] = {
                "po_name": po_title,
                "relationships": [], 
                "projects": projects
            }

    return list(unique_po_map.values()), physical_object_details_cache