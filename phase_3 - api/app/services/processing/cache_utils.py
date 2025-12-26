from typing import Dict, Any, Optional

def get_entity_details_minimal(
    entity_id: str,
    id_to_collection: Dict[str, str],
    id2label: Dict[str, str],
    context: Dict[str, Any],
    data_maps: Dict[str, Dict[str, Any]],
    id2projects: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Helper to get minimal entity details for relationship targets.
    """
    if not entity_id: 
        return {"type": "unknown", "label": "Unknown", "card": {}}
    
    coll = id_to_collection.get(entity_id)
    label = id2label.get(entity_id, "Unknown")
    card = {"hypotheses": context["entity_to_hypotheses_details"].get(entity_id, [])}
    entity_type = "other"

    # Unpack data maps for easier access
    works_data_map = data_maps.get("works", {})
    expressions_data_map = data_maps.get("expressions", {})
    manifestations_data_map = data_maps.get("manifestations", {})
    manifestation_volumes_data_map = data_maps.get("manifestation_volumes", {})
    items_data_map = data_maps.get("items", {})
    physical_objects_data_map = data_maps.get("physical_objects", {})
    pages_data_map = data_maps.get("pages", {})
    visual_objects_data_map = data_maps.get("visual_objects", {})

    # Map collection to type and basic card data
    if coll == "person": 
        entity_type = "person"
        label = context["person_to_name"].get(entity_id, label)
    elif coll == "work": 
        entity_type = "work"
        label = works_data_map.get(entity_id, {}).get("work_title", label)
    elif coll == "expression": 
        entity_type = "expression"
        label = expressions_data_map.get(entity_id, {}).get("expression_title", label)
    elif coll == "manifestation": 
        entity_type = "manifestation"
        label = manifestations_data_map.get(entity_id, {}).get("manifestation_title", label)
    elif coll == "manifestation_volume": 
        entity_type = "manifestation_volume"
        label = manifestation_volumes_data_map.get(entity_id, {}).get("manifestation_volume_title", label)
    elif coll == "item": 
        entity_type = "item"
        label = items_data_map.get(entity_id, {}).get("item_label", label)
    elif coll == "institution": 
        entity_type = "institution"
        label = context["institution_to_name"].get(entity_id, label)
    elif coll == "event": 
        entity_type = "event"
        label = context["event_to_name"].get(entity_id, label)
    elif coll == "hypothesis": 
        entity_type = "hypothesis"
    elif coll == "abstract_character": 
        entity_type = "abstract_character"
        label = context["ac_to_name"].get(entity_id, label)
    elif coll == "visual_object": 
        entity_type = "visual_object"
        label = context["vo_to_name"].get(entity_id, label)
    elif coll == "place": 
        entity_type = "place"
        label = context["place_to_name"].get(entity_id, label)
    elif coll == "page": 
        entity_type = "page"
        label = context["page_to_name"].get(entity_id, label)
        digital_page = context.get("page_to_digital_representation", {}).get(entity_id, None)
        if digital_page:
            card["digital_page"] = digital_page
    elif coll == "physical_object": 
        entity_type = "physical_object"
        label = context["physical_object_to_name"].get(entity_id, label)

    # Populate card data from cache if available
    if coll == "work" and entity_id in works_data_map: 
        card.update(works_data_map[entity_id]["card"])
    elif coll == "expression" and entity_id in expressions_data_map: 
        card.update(expressions_data_map[entity_id]["card"])
    elif coll == "manifestation" and entity_id in manifestations_data_map: 
        card.update(manifestations_data_map[entity_id]["card"])
    elif coll == "manifestation_volume" and entity_id in manifestation_volumes_data_map: 
        card.update(manifestation_volumes_data_map[entity_id]["card"])
    elif coll == "item" and entity_id in items_data_map: 
        card.update(items_data_map[entity_id]["card"])
    elif coll == "physical_object" and entity_id in physical_objects_data_map: 
        card.update(physical_objects_data_map[entity_id]["card"])
    elif coll == "page" and entity_id in pages_data_map: 
        card.update(pages_data_map[entity_id]["card"])
    elif coll == "visual_object" and entity_id in visual_objects_data_map:
        card.update(visual_objects_data_map[entity_id]["card"])
    elif coll == "person": 
        card.update({
            "name": label, 
            "birth_date": context["person_to_birth_date"].get(entity_id), 
            "death_date": context["person_to_death_date"].get(entity_id),
            "projects": id2projects.get(entity_id)
        })
    
    return {"id": entity_id, "type": entity_type, "label": label, "card": card}

def add_relationship(entity_cache: Dict[str, Any], rel_data: Dict[str, Any]) -> None:
    """
    Adds a relationship to the entity cache with deduplication logic.
    """
    if not entity_cache: 
        return
    
    # Determine the ID of the related entity for the NEW relationship
    new_related_id = rel_data.get("target_id") or rel_data.get("source_id")
    new_group = rel_data.get("group")
    new_type = rel_data.get("type")
    
    for existing in entity_cache["relationships"]:
        existing_group = existing.get("group")
        
        # Determine the ID of the related entity for the EXISTING relationship
        existing_related_id = existing.get("target_id") or existing.get("source_id")
        
        # 1. Strict deduplication for structural groups (parent/child)
        if new_group in ["parent", "child"] and existing_group == new_group:
            if new_related_id and existing_related_id and new_related_id == existing_related_id:
                return # Skip adding duplicate structural link

        # 2. General deduplication for other groups
        if existing_group == new_group:
            if new_related_id == existing_related_id and existing.get("type") == new_type:
                return

    entity_cache["relationships"].append(rel_data)