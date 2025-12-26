# app/services/entities/page.py
from typing import List, Dict, Any, Tuple
from app.services.common import extract_human_readable_id

def process_pages(raw_pages: List[Dict[str, Any]], context: Dict[str, Any], items_data_map: Dict[str, Any], manifestations_data_map: Dict[str, Any], manifestation_volumes_data_map: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    unique_pages_map = {}
    page_details_cache = {}

    id2projects = context["id2projects"]
    page_to_name = context["page_to_name"]
    id2label = context["id2label"]
    page_to_visual_object_ids = context["page_to_visual_object_ids"]
    page_to_physical_object_ids = context["page_to_physical_object_ids"]
    vo_to_name = context["vo_to_name"]
    physical_object_to_name = context["physical_object_to_name"]
    page_to_sorting_number = context["page_to_sorting_number"]
    page_to_digital_representation = context.get("page_to_digital_representation", {})
    
    # Context for Physical Object Types (Goal 1)
    po_to_type = context["po_to_type"]
    
    # Reverse map for project lookup and Parent lookup
    item_to_page_ids = context["item_to_page_ids"]
    page_to_item_map = {page_id: item_id for item_id, page_ids in item_to_page_ids.items() for page_id in page_ids}
    
    # Get maps for direct manifestation/volume connections
    page_to_manifestation_map = context.get("page_to_manifestation_map", {})
    page_to_manifestation_volume_map = context.get("page_to_manifestation_volume_map", {})

    # --- Build Physical Object -> Item Map ---
    # This allows us to find the Item if the Page is only connected via a Physical Object
    item_to_physical_object_ids = context.get("item_to_physical_object_ids", {})
    po_to_item_map = {}
    for item_id, po_ids in item_to_physical_object_ids.items():
        for po_id in po_ids:
            po_to_item_map[po_id] = item_id
            
    # --- Build Page -> Parent PO Map ---
    page_to_po_page_parent_map = context.get("page_to_po_page_parent_map", {})

    for page in raw_pages:
        page_id = page["_id"]
        
        # --- GOAL 1: Filter & Title Logic ---
        # Only process pages that have a name defined
        raw_name = page_to_name.get(page_id)
        if not raw_name:
            continue
        
        # Title format: "Pagina " + page_has_name
        # human_readable_id is intentionally excluded from the title string
        page_title = f"Pagina {raw_name}"
        digital_page = page_to_digital_representation.get(page_id, None)
        
        # Look up parent IDs
        parent_item_id = page_to_item_map.get(page_id)
        parent_manifestation_id = page_to_manifestation_map.get(page_id)
        parent_volume_id = page_to_manifestation_volume_map.get(page_id)

        # --- GOAL 2: Indirect Item Connection via Physical Object ---
        # If no direct Item parent, check if Page contains a PO that belongs to an Item
        if not parent_item_id:
            po_ids_on_page = page_to_physical_object_ids.get(page_id, [])
            for po_id in po_ids_on_page:
                if found_item_id := po_to_item_map.get(po_id):
                    parent_item_id = found_item_id
                    break # Stop once we find the parent Item

        # 1. Try to get project from the Page itself
        projects = id2projects.get(page_id, ["Unknown Project"])
        
        # 2. If unknown, try to inherit from parent (Item, Volume, or Manifestation)
        if projects == ["Unknown Project"]:
            if parent_item_id and (parent_item_details := items_data_map.get(parent_item_id)):
                projects = parent_item_details.get("projects", ["Unknown Project"])
            elif parent_volume_id and (parent_vol_details := manifestation_volumes_data_map.get(parent_volume_id)):
                 projects = parent_vol_details.get("projects", ["Unknown Project"])
            elif parent_manifestation_id and (parent_man_details := manifestations_data_map.get(parent_manifestation_id)):
                projects = parent_man_details.get("projects", ["Unknown Project"])
        
        human_readable_id = extract_human_readable_id(page, "PAG_")
        
        # Create the card for frontend display
        card = {
            "title": page_title,
            "projects": projects
        }

        # --- NEW: Add "Unità materiale" (Physical Object Type) to Card ---
        po_types_on_page = set()
        for po_id in page_to_physical_object_ids.get(page_id, []):
            po_types_on_page.update(po_to_type.get(po_id, []))
        
        if po_types_on_page:
            card["Unità materiale"] = ", ".join(sorted(list(po_types_on_page)))

        # --- Add specific card context based on human_readable_id ---
        if human_readable_id:
            if human_readable_id.startswith("PAG_PO_PAG_PO"):
                # Find the PO that is the PARENT of the page
                parent_po_id = page_to_po_page_parent_map.get(page_id)
                if parent_po_id:
                    parent_po_name = physical_object_to_name.get(parent_po_id, "Unknown PO")
                    parent_po_types = ", ".join(po_to_type.get(parent_po_id, []))
                    card["Contenuto in:"] = f"{parent_po_types} - {parent_po_name}"

                    # Find the Item via this parent PO
                    parent_item_id = po_to_item_map.get(parent_po_id)
                    if parent_item_id and (item_details := items_data_map.get(parent_item_id)):
                        card["Item:"] = item_details.get("card", {}).get("title", "Unknown Item")
            elif human_readable_id.startswith("PAG_PO_PAG_") or human_readable_id.startswith("PAG_PO_"):
                parent_page_id = page_id # The current page is the context
                
                # Find the PO that is the PARENT of the page
                parent_po_id = page_to_po_page_parent_map.get(parent_page_id)
                if parent_po_id:
                    parent_po_name = physical_object_to_name.get(parent_po_id, "Unknown PO")
                    parent_po_types = ", ".join(po_to_type.get(parent_po_id, []))
                    card["Contenuto in:"] = f"{parent_po_types} - {parent_po_name}"

                # Find the Item via the page's direct link or its parent PO
                item_id_found = page_to_item_map.get(parent_page_id) or po_to_item_map.get(parent_po_id)
                if item_id_found and (item_details := items_data_map.get(item_id_found)):
                    card["Item:"] = item_details.get("card", {}).get("title", "Unknown Item")
            
            # Fallback for other types (Manifestation/Volume/Item)
            else:
                if parent_item_id:
                    if parent_item_details := items_data_map.get(parent_item_id):
                        item_title_str = parent_item_details.get("card", {}).get("title") or parent_item_details.get("item_label")
                        if item_title_str:
                            card["Item"] = item_title_str
                elif parent_volume_id:
                    if parent_vol_details := manifestation_volumes_data_map.get(parent_volume_id):
                        vol_title_str = parent_vol_details.get("card", {}).get("title") or parent_vol_details.get("manifestation_volume_title")
                        if vol_title_str:
                            card["Manifestation"] = vol_title_str
                        
                        publishers = parent_vol_details.get("publishers", [])
                        if publishers:
                            publisher_names = [p.get("name", "Unknown") for p in publishers]
                            card["Publisher"] = ", ".join(publisher_names)
                elif parent_manifestation_id:
                    if parent_man_details := manifestations_data_map.get(parent_manifestation_id):
                        man_title_str = parent_man_details.get("card", {}).get("title") or parent_man_details.get("manifestation_title")
                        if man_title_str:
                            card["Manifestation"] = man_title_str
                        
                        publishers = parent_man_details.get("publisher", [])
                        if publishers:
                            publisher_names = [p.get("name", "Unknown") for p in publishers]
                            card["Publisher"] = ", ".join(publisher_names)
  

        sorting_number = page_to_sorting_number.get(page_id, None)

        page_data = {
            "page_id": page_id, 
            "page_number_sorting": sorting_number,
            "human_readable_id": human_readable_id,
            "page_title": page_title,
            "digital_page": digital_page,
            "projects": projects,
            "card": card
        }

        # --- DEDUPLICATION LOGIC ---
        # Pages are unique by ID. 
        # If IDs are different but titles are same (e.g. "Pagina 1r" in two different books), both are shown.
        # If IDs are same, they are treated as one entity.
        unique_pages_map[page_id] = page_data

        page_details_cache[page_id] = {
            "page_label": page_title,
            "digital_page": digital_page,
            "page_number_sorting": sorting_number,
            "visual_objects": [{"vo_id": vo_id, "vo_name": vo_to_name.get(vo_id, id2label.get(vo_id, "Unknown VO"))} for vo_id in page_to_visual_object_ids.get(page_id, [])], 
            "physical_objects": [{"po_id": po_id, "po_name": physical_object_to_name.get(po_id, id2label.get(po_id, "Unknown PO"))} for po_id in page_to_physical_object_ids.get(page_id, [])], 
            "relationships": [], 
            "projects": projects
        }

    return list(unique_pages_map.values()), page_details_cache