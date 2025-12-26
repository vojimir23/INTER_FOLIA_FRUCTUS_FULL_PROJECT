import asyncio
from typing import Dict, Any, List
from motor.motor_asyncio import AsyncIOMotorDatabase
from collections import defaultdict

from app.logging_setup import logger
from app.store import store
from app.config import PERSON_RELATIONSHIPS
from app.services.common import extract_label
from app.services.relations import process_relations
from app.database import fetch_collection 

from app.services.translations import (
    PHYSICAL_OBJECT_TYPE_MAP,
    ITEM_PRESERVATION_MAP,
    ITEM_MATERIAL_MAP,
    ITEM_TYPE_MAP,
    EXPRESSION_TYPE_MAP,
    PHYSICAL_OBJECT_INSERTION_TYPE_MAP,
    VISUAL_OBJECT_TYPE_MAP,
    VISUAL_OBJECT_INSTRUMENT_MAP,
    VISUAL_OBJECT_COLOUR_MAP,
    VISUAL_OBJECT_TRANSCRIPTION_QUALITY_MAP,
    VISUAL_OBJECT_FUNCTION_MAP
)
from app.services.languages import LANGUAGE_MAP


#Import only the 'process' functions for entities
from app.services.entities.work import process_works
from app.services.entities.expression import process_expressions
from app.services.entities.manifestation import process_manifestations
from app.services.entities.item import process_items
from app.services.entities.visual_object import process_visual_objects
from app.services.entities.person import process_persons
from app.services.entities.institution import process_institutions
from app.services.entities.event import process_events
from app.services.entities.place import process_places
from app.services.entities.page import process_pages
from app.services.entities.physical_object import process_physical_objects
from app.services.entities.abstract_character import process_abstract_characters
from app.services.entities.hypothesis import process_hypotheses

# Import New Processing Modules
from app.services.processing.cache_utils import get_entity_details_minimal, add_relationship
from app.services.processing.ancestor_injector import apply_all_ancestors
from app.services.processing.graph_preparer import prepare_graph_data
from app.services.processing.filter_builder import generate_frontend_filters

# --- NEW MAPPING FOR VISUAL OBJECT DETAILS ---
VISUAL_OBJECT_RELATION_MAP = {
    "page_contains_visual_object": "Contained in Page",
    "visual_object_owned_by": "Owner",
    "visual_object_owned_by_person": "Owner",
    "visual_object_owned_by_institution": "Owner",
    "visual_object_inscribed_by": "Inscriber",
    "visual_object_inscribed_by_person": "Inscriber",
    "visual_object_sent_by": "Sender",
    "visual_object_sent_by_person": "Sender",
    "visual_object_received_by": "Recipient",
    "visual_object_received_by_person": "Recipient"
}

async def load_and_process_data(db: AsyncIOMotorDatabase) -> None:
    """
    Handles the entire process of fetching, processing, and caching the data
    directly from the MongoDB database.
    """
    logger.info("--- Starting Data Loading and Processing from MongoDB ---")
    store.mark_loading()
    new_cache: Dict[str, Any] = {}

    try:
        # Create a master translation lookup ---
        # Combine all dictionaries into a single map with lowercase keys for standardization.
        # The order matters if keys overlap; later dictionaries will overwrite earlier ones.
        MASTER_TRANSLATION_MAP = {k.lower(): v for k, v in {
            **PHYSICAL_OBJECT_TYPE_MAP,
            **ITEM_PRESERVATION_MAP,
            **ITEM_MATERIAL_MAP,
            **ITEM_TYPE_MAP,
            **EXPRESSION_TYPE_MAP,
            **PHYSICAL_OBJECT_INSERTION_TYPE_MAP,
            **VISUAL_OBJECT_TYPE_MAP,
            **VISUAL_OBJECT_INSTRUMENT_MAP,
            **VISUAL_OBJECT_COLOUR_MAP,
            **VISUAL_OBJECT_TRANSCRIPTION_QUALITY_MAP,
            **VISUAL_OBJECT_FUNCTION_MAP,
            **LANGUAGE_MAP  # Language codes are also treated as labels to be translated
        }.items()}
 

        # 1. FETCH HELPER COLLECTIONS
        logger.info("Fetching helper collections (types, users)...")
        types_cursor = db.types.find({"active": True})
        users_cursor = db.users.find({})
        types_list = await types_cursor.to_list(length=None)
        users_list = await users_cursor.to_list(length=None)
        
        user_id_to_username = {str(u["_id"]): u.get("username", "Unknown Project") for u in users_list}
        logger.info("Helper collections fetched.")

        # 2. FETCH ENTITIES (: Using generic fetcher)
        logger.info("Fetching entities from dedicated collections...")
        raw_works = await fetch_collection(db, "works")
        raw_expressions = await fetch_collection(db, "expressions")
        raw_manifestations = await fetch_collection(db, "manifestations")
        raw_items = await fetch_collection(db, "items")
        raw_persons = await fetch_collection(db, "persons")
        raw_vos = await fetch_collection(db, "visual_objects")
        raw_institutions = await fetch_collection(db, "institutions")
        raw_events = await fetch_collection(db, "events")
        raw_places = await fetch_collection(db, "places")
        raw_pages = await fetch_collection(db, "pages")
        raw_pos = await fetch_collection(db, "physical_objects")
        raw_acs = await fetch_collection(db, "abstract_characters")
        raw_hypotheses = await fetch_collection(db, "hypotheses")
        logger.info("Entities fetched.")

        # 3. BUILD GLOBAL LOOKUPS (Labels, Projects, Collections)
        id2label: Dict[str, str] = {}
        id2projects: Dict[str, List[str]] = defaultdict(list)
        id_to_collection: Dict[str, str] = {}

        all_raw_entities = [
            (raw_works, "work"), (raw_expressions, "expression"), (raw_manifestations, "manifestation"),
            (raw_items, "item"), (raw_persons, "person"), (raw_vos, "visual_object"),
            (raw_institutions, "institution"), (raw_events, "event"), (raw_places, "place"),
            (raw_pages, "page"), (raw_pos, "physical_object"), (raw_acs, "abstract_character"),
            (raw_hypotheses, "hypothesis"),
            (types_list, "type") # Also process the 'types' collection for labels
        ]

        for entity_list, entity_type in all_raw_entities:
            for it in entity_list:
                original_id = str(it["_id"])
                raw_label = extract_label(it)
                id_to_collection[original_id] = entity_type
                
                # Standardize and translate label on load ---
                # Look up the lowercased raw label in our master map.
                # If found, use the translation; otherwise, use the original raw label.
                id2label[original_id] = MASTER_TRANSLATION_MAP.get(raw_label.lower(), raw_label)
                # --- MODIFICATION END ---
                
                # Project logic (only for main entities, not types)
                if entity_type != "type":
                    user_ids = it.get("associatedUsers")
                    if user_ids:
                        projects = {user_id_to_username.get(str(user_id), "Unknown Project") for user_id in user_ids}
                        id2projects[original_id] = sorted(list(projects))
                    elif creation_user := it.get("creationUser"):
                        project_name = user_id_to_username.get(str(creation_user), "Unknown Project")
                        id2projects[original_id] = [project_name]
                    else:
                        id2projects[original_id] = ["Unknown Project"]

        # 4. FETCH AND PROCESS RELATIONS
        logger.info("Fetching and processing relations...")
        relations_cursor = db.relations.find({"active": True})
        rel_all_raw = await relations_cursor.to_list(length=None)
        rel_all = [{"_id": str(r["_id"]), "entity1": str(r.get("entity1")), "entity2": str(r.get("entity2")), "relationType": str(r.get("relationType"))} for r in rel_all_raw if r.get("entity1") and r.get("entity2")]
        
        rtypes_cursor = db.relationtypes.find({"active": True})
        rtypes_res_raw = await rtypes_cursor.to_list(length=None)
        rtypes = {str(r["_id"]): {"_id": str(r["_id"]), "name": r.get("name")} for r in rtypes_res_raw}
        
        id2rtype_name = {rid: r.get("name", "unknown_relation") for rid, r in rtypes.items()}
        
        # Map for finding the Parent PO of a Page (used in PO processing)
        page_to_po_page_parent_map = {r['entity1']: r['entity2'] for r in rel_all if id2rtype_name.get(r['relationType']) == 'page_contains_physical_object_page'}

        # Build Context Map (Relations Processing)
        # This now operates on the pre-translated id2label map
        context = process_relations(rel_all, rtypes, id2label, id_to_collection)
        context["id2projects"] = id2projects # Add projects to context
        context["page_to_po_page_parent_map"] = page_to_po_page_parent_map # Add for PO processor

        # 5. PROCESS ENTITIES (Order matters for dependencies)
        logger.info("Processing entity structures...")
        
        # Independent or Base Entities
        persons_data, person_details_cache = process_persons(raw_persons, context)
        institutions_data, institution_details_cache = process_institutions(raw_institutions, context)
        events_data, event_details_cache = process_events(raw_events, context)
        _, place_details_cache = process_places(raw_places, context)
        abstract_characters_data, ac_details_cache = process_abstract_characters(raw_acs, context)
        _, hypothesis_details_cache = process_hypotheses(raw_hypotheses, context)

        # Hierarchical Entities
        works_data, work_details_cache = process_works(raw_works, context)
        works_data_map = {w['work_id']: w for w in works_data}

        expressions_data, expression_details_cache = process_expressions(raw_expressions, context, works_data_map)
        expressions_data_map = {e['expression_id']: e for e in expressions_data}

        manifestations_data, manifestation_volumes_data, manifestation_details_cache, manifestation_volume_details_cache = process_manifestations(raw_manifestations, context, works_data_map, expressions_data_map)
        manifestations_data_map = {m['manifestation_id']: m for m in manifestations_data}
        manifestation_volumes_data_map = {v['manifestation_volume_id']: v for v in manifestation_volumes_data}

        items_data, item_details_cache = process_items(raw_items, context, manifestations_data_map, works_data_map)
        items_data_map = {i['item_id']: i for i in items_data}

        visual_objects_data, visual_object_details_cache = process_visual_objects(raw_vos, context, items_data_map, manifestations_data_map, manifestation_volumes_data_map)
        visual_objects_data_map = {vo['visual_object_id']: vo for vo in visual_objects_data}
        
        pages_data, page_details_cache = process_pages(raw_pages, context, items_data_map, manifestations_data_map, manifestation_volumes_data_map)
        pages_data_map = {p['page_id']: p for p in pages_data}
        
        physical_objects_data, physical_object_details_cache = process_physical_objects(raw_pos, context, items_data_map)
        physical_objects_data_map = {po['physical_object_id']: po for po in physical_objects_data}

        # Bundle data maps for helpers
        data_maps = {
            "works": works_data_map, "expressions": expressions_data_map, "manifestations": manifestations_data_map,
            "manifestation_volumes": manifestation_volumes_data_map, "items": items_data_map,
            "physical_objects": physical_objects_data_map, "pages": pages_data_map,
            "visual_objects": visual_objects_data_map,
            # Lists for iteration
            "works_list": works_data, "expressions_list": expressions_data, "manifestations_list": manifestations_data,
            "manifestation_volumes_list": manifestation_volumes_data, "items_list": items_data,
            "physical_objects_list": physical_objects_data, "pages_list": pages_data,
            "persons_list": persons_data, "institutions_list": institutions_data, "events_list": events_data,
            "visual_objects_list": visual_objects_data, "abstract_characters_list": abstract_characters_data
        }

        # 6. FINALIZE DETAILS CACHE (Relationships & Transitive Logic)
        logger.info("Finalizing details cache with relationships...")
        
        all_caches = {
            **person_details_cache, **ac_details_cache, **event_details_cache, 
            **work_details_cache, **hypothesis_details_cache, **expression_details_cache, 
            **visual_object_details_cache, **manifestation_details_cache, 
            **manifestation_volume_details_cache, **place_details_cache, 
            **item_details_cache, **institution_details_cache, **page_details_cache, 
            **physical_object_details_cache
        }

        # Populate relationships in cache
        attribute_relations = {
            "item_has_shelf_mark", "item_has_preservation_status", "item_has_material", "item_has_type", "item_has_dimensions", 
            "item_has_copy_number", "item_has_surviving_pages", "item_has_physical_description", "manifestation_has_external_id", 
            "manifestation_has_print_run", "manifestation_has_edition_number", "manifestation_has_number_of_volumes", 
            "manifestation_has_format", "manifestation_has_collation_formula", "manifestation_has_number_of_pages", 
            "manifestation_has_introduction_pages", "manifestation_has_external_digitization", "manifestation_has_pages",
            "expression_has_incipit", "expression_has_explicit", "expression_has_completeness"
        }
        
        parent_child_map = {
            "is_expression_of_work": ("expression", "work"), 
            "is_manifestation_of_expression": ("manifestation", "expression"), 
            "is_item_of_manifestation": ("item", "manifestation"), 
            "manifestation_has_volume": ("manifestation", "manifestation_volume"), 
            "item_has_manifestation_volume": ("item", "manifestation_volume"), 
            "item_contains_physical_object": ("item", "physical_object")
        }
        
        relation_to_role_map = {
            "work_authored_by": "Author", "expression_has_translator": "Translator", "expression_has_editor": "Editor",
            "expression_has_scriptwriter": "Scriptwriter", "expression_has_compositor": "Compositor", "expression_has_reviewer": "Reviewer",
            "expression_has_other_secondary_role": "Other secondary role", "manifestation_published_by": "Publisher", 
            "manifestation_volume_published_by": "Publisher", "manifestation_edited_by": "Editor", "manifestation_volume_edited_by": "Editor", 
            "manifestation_sponsored_by": "Sponsor", "manifestation_volume_sponsored_by": "Sponsor", "manifestation_corrected_by": "Corrector", 
            "manifestation_volume_corrected_by": "Corrector", "item_owned_by": "Owner of item", "visual_object_owned_by": "Owner",
            "visual_object_owned_by_person": "Owner", "visual_object_inscribed_by": "Inscriber", "visual_object_inscribed_by_person": "Inscriber", 
            "visual_object_sent_by": "Sender", "visual_object_sent_by_person": "Sender", "visual_object_received_by": "Recipient",
            "visual_object_received_by_person": "Recipient", "physical_object_created_by": "Creator of PO", 
            "physical_object_owned_by": "Owner of PO",
            "person_member_of_institution": "Member"
        }

        for r in rel_all:
            e1, e2, rtype_id = r["entity1"], r["entity2"], r["relationType"]
            if not e1 or not e2: continue
            rel_name = id2rtype_name.get(rtype_id, "unknown_relation")
            
         
            # Populate roles_with_entities for Person and Institution
            if rel_name in relation_to_role_map:
                if id_to_collection.get(e2) == "person":
                    if (person_cache := person_details_cache.get(e2)):
                        person_cache["roles_with_entities"].setdefault(relation_to_role_map[rel_name], []).append(
                            get_entity_details_minimal(e1, id_to_collection, id2label, context, data_maps, id2projects)
                        )
                elif id_to_collection.get(e2) == "institution":
                    if (institution_cache := institution_details_cache.get(e2)):
                        institution_cache["roles_with_entities"].setdefault(relation_to_role_map[rel_name], []).append(
                            get_entity_details_minimal(e1, id_to_collection, id2label, context, data_maps, id2projects)
                        )
            # ---  Add reverse role for person_member_of_institution ---
            if rel_name == "person_member_of_institution":
                person_id, institution_id = e1, e2
                if id_to_collection.get(person_id) == "person":
                    if (person_cache := person_details_cache.get(person_id)):
                        details = get_entity_details_minimal(institution_id, id_to_collection, id2label, context, data_maps, id2projects)
                        person_cache["roles_with_entities"].setdefault("Member of", []).append(details)

            if rel_name in VISUAL_OBJECT_RELATION_MAP:
                label = VISUAL_OBJECT_RELATION_MAP[rel_name]
                vo_id, other_entity_id = None, None

                if rel_name == "page_contains_visual_object":
                    # This is an incoming relationship to the VO (e2) from the Page (e1)
                    vo_id = e2
                    other_entity_id = e1
                elif id_to_collection.get(e1) == "visual_object":
                    # This is an outgoing relationship from the VO (e1) to another entity (e2)
                    vo_id = e1
                    other_entity_id = e2

                if vo_id and other_entity_id:
                    # Ensure the cache for this VO exists
                    if (vo_cache := visual_object_details_cache.get(vo_id)):
                        # Get the details of the other entity to display in the list
                        details = get_entity_details_minimal(other_entity_id, id_to_collection, id2label, context, data_maps, id2projects)
                        # Add the details to the correct group, creating the list if it doesn't exist
                        vo_cache["grouped_relationships"].setdefault(label, []).append(details)
         

            if rel_name in ["item_has_page", "page_from_manifestation", "page_from_manifestation_volume"]: continue

            if rel_name in attribute_relations:
                if e1_cache := all_caches.get(e1):
                    add_relationship(e1_cache, {"type": rel_name, "direction": "outgoing", "group": "other", "target_id": e2, "target_type": "literal", "target_label": id2label.get(e2, ""), "target_card": {}})
                continue

            group = "mention" if "is_mentioning" in rel_name or "is_mentioned_by" in rel_name else "other"
            
            if rel_name in PERSON_RELATIONSHIPS:
                outgoing_label, incoming_label = PERSON_RELATIONSHIPS[rel_name]
                if e1_cache := all_caches.get(e1):
                    details = get_entity_details_minimal(e2, id_to_collection, id2label, context, data_maps, id2projects)
                    add_relationship(e1_cache, {"type": outgoing_label, "direction": "outgoing", "group": "personal", "target_id": e2, "target_type": details["type"], "target_label": details["label"], "target_card": details.get("card", {})})
                if e2_cache := all_caches.get(e2):
                    details = get_entity_details_minimal(e1, id_to_collection, id2label, context, data_maps, id2projects)
                    add_relationship(e2_cache, {"type": incoming_label, "direction": "incoming", "group": "personal", "source_id": e1, "source_type": details["type"], "source_label": details["label"], "source_card": details.get("card", {})})
                continue

            # Refactored Page/PO/VO relationship logic
            if rel_name in ["page_contains_visual_object", "page_contains_physical_object", "page_contains_physical_object_page"]:
                page_id, other_entity_id = e1, e2
                
                # This block handles POs specifically
                if rel_name in ["page_contains_physical_object", "page_contains_physical_object_page"]:
                    po_id = other_entity_id
                    
                    # Page (e1) gets a link TO the PO (e2)
                    if page_cache := all_caches.get(page_id):
                        details = get_entity_details_minimal(po_id, id_to_collection, id2label, context, data_maps, id2projects)
                        group_for_page = "parent" if rel_name == "page_contains_physical_object_page" else "child"
                        add_relationship(page_cache, {"type": rel_name, "direction": "outgoing", "group": group_for_page, "target_id": po_id, "target_type": details["type"], "target_label": details["label"], "target_card": details.get("card", {})})

                    # PO (e2) gets a link FROM the Page (e1)
                    if po_cache := all_caches.get(po_id):
                        po_hr_id = physical_objects_data_map.get(po_id, {}).get('human_readable_id', '')
                        group_for_po = "parent"
                        if rel_name == "page_contains_physical_object_page":
                            group_for_po = "child"
                        if po_hr_id and (po_hr_id.startswith("PO_PAG_PO_") or po_hr_id.startswith("PO_PAG_")):
                            group_for_po = "child"

                        details = get_entity_details_minimal(page_id, id_to_collection, id2label, context, data_maps, id2projects)
                        add_relationship(po_cache, {"type": rel_name, "direction": "incoming", "group": group_for_po, "source_id": page_id, "source_type": details["type"], "source_label": details["label"], "source_card": details.get("card", {})})

                # This block handles VOs
                elif rel_name == "page_contains_visual_object":
                    vo_id = other_entity_id
                    
                    # 1. Standard Page Logic
                    if page_cache := all_caches.get(page_id):
                        details = get_entity_details_minimal(vo_id, id_to_collection, id2label, context, data_maps, id2projects)
                        add_relationship(page_cache, {"type": rel_name, "direction": "outgoing", "group": "child", "target_id": vo_id, "target_type": details["type"], "target_label": details["label"], "target_card": details.get("card", {})})
                    
                    # 2. Special PO Logic (User Request: POs acting as Pages)
                    if po_cache := all_caches.get(page_id):
                        details = get_entity_details_minimal(vo_id, id_to_collection, id2label, context, data_maps, id2projects)
                        add_relationship(po_cache, {"type": rel_name, "direction": "outgoing", "group": "child", "target_id": vo_id, "target_type": details["type"], "target_label": details["label"], "target_card": details.get("card", {})})

                    # VO (e2) gets a 'parent' link TO the Page (e1)
                    if vo_cache := all_caches.get(vo_id):
                        details = get_entity_details_minimal(page_id, id_to_collection, id2label, context, data_maps, id2projects)
                        add_relationship(vo_cache, {"type": rel_name, "direction": "incoming", "group": "parent", "source_id": page_id, "source_type": details["type"], "source_label": details["label"], "source_card": details.get("card", {})})
                
                continue 

            if rel_name in parent_child_map:
                if rel_name in ["manifestation_has_volume", "item_contains_physical_object"]:
                    if e1_cache := all_caches.get(e1):
                        details = get_entity_details_minimal(e2, id_to_collection, id2label, context, data_maps, id2projects)
                        add_relationship(e1_cache, {"type": rel_name, "direction": "outgoing", "group": "child", "target_id": e2, "target_type": details["type"], "target_label": details["label"], "target_card": details.get("card", {})})
                    if e2_cache := all_caches.get(e2):
                        details = get_entity_details_minimal(e1, id_to_collection, id2label, context, data_maps, id2projects)
                        add_relationship(e2_cache, {"type": rel_name, "direction": "incoming", "group": "parent", "source_id": e1, "source_type": details["type"], "source_label": details["label"], "source_card": details.get("card", {})})
                else:
                    if e1_cache := all_caches.get(e1):
                        details = get_entity_details_minimal(e2, id_to_collection, id2label, context, data_maps, id2projects)
                        add_relationship(e1_cache, {"type": rel_name, "direction": "outgoing", "group": "parent", "target_id": e2, "target_type": details["type"], "target_label": details["label"], "target_card": details.get("card", {})})
                    if e2_cache := all_caches.get(e2):
                        details = get_entity_details_minimal(e1, id_to_collection, id2label, context, data_maps, id2projects)
                        add_relationship(e2_cache, {"type": rel_name, "direction": "incoming", "group": "child", "source_id": e1, "source_type": details["type"], "source_label": details["label"], "source_card": details.get("card", {})})
                continue

            if e1_cache := all_caches.get(e1):
                details = get_entity_details_minimal(e2, id_to_collection, id2label, context, data_maps, id2projects)
                add_relationship(e1_cache, {"type": rel_name, "direction": "outgoing", "group": group, "target_id": e2, "target_type": details["type"], "target_label": details["label"], "target_card": details.get("card", {})})
            if e2_cache := all_caches.get(e2):
                details = get_entity_details_minimal(e1, id_to_collection, id2label, context, data_maps, id2projects)
                add_relationship(e2_cache, {"type": rel_name, "direction": "incoming", "group": group, "source_id": e1, "source_type": details["type"], "source_label": details["label"], "source_card": details.get("card", {})})

        # 6.5 & 7. INJECT HIERARCHICAL ANCESTORS
        details_caches = {
            "expression": expression_details_cache,
            "manifestation": manifestation_details_cache,
            "manifestation_volume": manifestation_volume_details_cache,
            "item": item_details_cache,
            "physical_object": physical_object_details_cache,
            "page": page_details_cache,
            "visual_object": visual_object_details_cache
        }
        apply_all_ancestors(data_maps, details_caches, context)

        # 8. PREPARE GRAPH DATA
        all_relations_for_graph, id_to_details_for_graph = prepare_graph_data(data_maps, rel_all, id2label, id2rtype_name, context)

        page_to_item_map = {page_id: item_id for item_id, page_ids in context["item_to_page_ids"].items() for page_id in page_ids}

        new_cache.update({
            "all_relations": all_relations_for_graph, "id_to_details_map": id_to_details_for_graph, "page_to_item_map": page_to_item_map,
            "works": works_data, "expressions": expressions_data, "manifestations": manifestations_data,
            "items": items_data, "persons": persons_data, "visual_objects": visual_objects_data,
            "institutions": institutions_data, "events": events_data, "physical_objects": physical_objects_data,
            "pages": pages_data, "abstract_characters": abstract_characters_data,
            "person_details": person_details_cache, "work_details": work_details_cache,
            "ac_details": ac_details_cache, "event_details": event_details_cache, "hypothesis_details": hypothesis_details_cache,
            "expression_details": expression_details_cache, "visual_object_details": visual_object_details_cache,
            "manifestation_details": manifestation_details_cache, "manifestation_volume_details": manifestation_volume_details_cache,
            "place_details": place_details_cache, "item_details": item_details_cache, "institution_details": institution_details_cache,
            "page_details": page_details_cache, "physical_object_details": physical_object_details_cache
        })

        # 9. BUILD FILTER OPTIONS
        logger.info("Building project-specific filter options...")
        filter_options_by_project = generate_frontend_filters(data_maps, id2projects, context, relation_to_role_map)
        
        new_cache["filter_options"] = filter_options_by_project
        
        store.swap_cache(new_cache)
        logger.info("--- Data Loading and Processing Finished ---")

    except Exception as e:
        logger.critical(f"A critical error occurred during data loading: {e}", exc_info=True)