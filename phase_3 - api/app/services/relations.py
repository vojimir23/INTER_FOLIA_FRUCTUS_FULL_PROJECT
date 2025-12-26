# app/services/relations.py
from typing import List, Dict, Any
from collections import defaultdict
from app.services.common import translate_date_notes

def process_relations(rel_all: List[Dict[str, Any]], rtypes: Dict[str, Any], id2label: Dict[str, str], id_to_collection: Dict[str, str]) -> Dict[str, Any]:
    """
    Iterates through all relations and builds lookup dictionaries (Context) 
    to be used by entity processors. Now relies on a pre-translated id2label map.
    """
    ctx = defaultdict(dict)
    ctx["id2label"] = id2label
    ctx["id_to_collection"] = id_to_collection
    
    # Initialize list-based lookups
    list_keys = [
        "person_to_aliases", "work_to_classification", "work_to_expression_ids", 
        "expression_to_translators", "expression_to_editors_exp", "expression_to_scriptwriters", 
        "expression_to_compositors", "expression_to_reviewers", "expression_to_other_roles",
        "expression_to_responsibility", "expression_to_language", "expression_to_type", 
        "expression_to_medium", "expression_to_manifestation_ids", "work_is_mentioning", 
        "work_is_mentioned_by", "manifestation_to_item_ids", "volume_to_item_ids",
        "manifestation_to_publishers", "manifestation_to_editors", "manifestation_to_correctors", 
        "manifestation_to_sponsors", "manifestation_to_volume_title", "volume_to_publishers",
        "item_to_preservation_status", "item_to_owner", "item_to_material", "item_to_type",
        "item_to_page_ids", "item_to_physical_object_ids", "page_to_visual_object_ids",
        "page_to_physical_object_ids", "vo_to_owners", "vo_to_inscribers", "vo_to_senders",
        "vo_to_recipients", "vo_to_type", "po_to_type", "person_to_created_hypotheses",
        "entity_to_hypotheses_details", "work_to_author_ids", "manifestation_to_volume_ids",
        "vo_to_function", "vo_to_language", "vo_to_instrument", "vo_to_colour",

        "vo_to_transcription_quality", "po_to_insertion_type",
        "ac_to_aliases", "ac_is_mentioning", "ac_is_mentioned_by"
    ]
    for k in list_keys:
        ctx[k] = defaultdict(list)

    # Store the target IDs first, resolve to final names later
    ctx["hypothesis_to_about_ids"] = defaultdict(list)


    # Initialize set-based lookups
    ctx["volume_ids"] = set()
    ctx["person_to_roles"] = {} # Nested dict
    ctx["institution_to_roles"] = defaultdict(lambda: defaultdict(list)) # --- MODIFICATION START ---

    for r in rel_all:
        rname = rtypes.get(r["relationType"], {}).get("name", "")
        e1, e2 = r["entity1"], r["entity2"]
        if not e1 or not e2: continue

        # --- WORK RELATIONS ---
        if rname == "work_has_uniform_title": ctx["work_to_uniform_title"][e1] = id2label.get(e2, "Unknown Title")
        elif rname == "work_has_classification": ctx["work_to_classification"][e1].append(id2label.get(e2, "Unknown Classification"))
        elif rname == "work_authored_by": 
            ctx["work_to_author_ids"][e1].append(e2)
            if id_to_collection.get(e2) == "person":
                ctx["person_to_roles"].setdefault(e2, {}).setdefault("Autore dell’opera", []).append(e1)
            elif id_to_collection.get(e2) == "institution":
                ctx["institution_to_roles"][e2]["Autore dell’opera"].append(e1)
        elif rname == "work_is_mentioning": ctx["work_is_mentioning"][e1].append(e2)
        elif rname == "work_is_mentioned_by": ctx["work_is_mentioned_by"][e1].append(e2)

        # --- EXPRESSION RELATIONS ---
        elif rname == "is_expression_of_work": 
            ctx["work_to_expression_ids"][e2].append(e1)
            ctx["expression_to_work"][e1] = e2
        elif rname == "expression_has_translator": 
            ctx["expression_to_translators"][e1].append(e2)
            if id_to_collection.get(e2) == "person": ctx["person_to_roles"].setdefault(e2, {}).setdefault("Traduttore", []).append(e1)
            elif id_to_collection.get(e2) == "institution": ctx["institution_to_roles"][e2]["Traduttore"].append(e1)
        elif rname == "expression_has_editor": 
            ctx["expression_to_editors_exp"][e1].append(e2)
            if id_to_collection.get(e2) == "person": ctx["person_to_roles"].setdefault(e2, {}).setdefault("Curatore", []).append(e1)
            elif id_to_collection.get(e2) == "institution": ctx["institution_to_roles"][e2]["Curatore"].append(e1)
        elif rname == "expression_has_scriptwriter": 
            ctx["expression_to_scriptwriters"][e1].append(e2)
            if id_to_collection.get(e2) == "person": ctx["person_to_roles"].setdefault(e2, {}).setdefault("Sceneggiatore", []).append(e1)
            elif id_to_collection.get(e2) == "institution": ctx["institution_to_roles"][e2]["Sceneggiatore"].append(e1)
        elif rname == "expression_has_compositor": 
            ctx["expression_to_compositors"][e1].append(e2)
            if id_to_collection.get(e2) == "person": ctx["person_to_roles"].setdefault(e2, {}).setdefault("Compositore", []).append(e1)
            elif id_to_collection.get(e2) == "institution": ctx["institution_to_roles"][e2]["Compositore"].append(e1)
        elif rname == "expression_has_reviewer": 
            ctx["expression_to_reviewers"][e1].append(e2)
            if id_to_collection.get(e2) == "person": ctx["person_to_roles"].setdefault(e2, {}).setdefault("Recensore", []).append(e1)
            elif id_to_collection.get(e2) == "institution": ctx["institution_to_roles"][e2]["Recensore"].append(e1)
        elif rname == "expression_has_other_secondary_role": 
            ctx["expression_to_other_roles"][e1].append(e2)
            if id_to_collection.get(e2) == "person": ctx["person_to_roles"].setdefault(e2, {}).setdefault("Altro ruolo secondario", []).append(e1)
            elif id_to_collection.get(e2) == "institution": ctx["institution_to_roles"][e2]["Altro ruolo secondario"].append(e1)
        elif rname == "expression_has_responsibility_statement": ctx["expression_to_responsibility"][e1].append(id2label.get(e2, "Unknown"))
        elif rname == "expression_has_language": ctx["expression_to_language"][e1].append(id2label.get(e2, "Unknown"))
        elif rname == "expression_has_type": ctx["expression_to_type"][e1].append(id2label.get(e2, "Unknown Type"))
        elif rname == "expression_has_medium": ctx["expression_to_medium"][e1].append(id2label.get(e2, "Unknown Medium"))
        elif rname == "expression_has_number": ctx["expression_to_number"][e1] = id2label.get(e2, "")

        # --- MANIFESTATION & VOLUME RELATIONS ---
        elif rname == "is_manifestation_of_expression": 
            ctx["expression_to_manifestation_ids"][e2].append(e1)
            ctx["manifestation_to_expression"][e1] = e2
        elif rname == "manifestation_has_volume": 
            ctx["manifestation_to_volume_ids"][e1].append(e2)
            ctx["volume_to_parent_manifestation_id"][e2] = e1
            ctx["volume_ids"].add(e2)
        elif rname == "manifestation_published_in_place": ctx["manifestation_to_place"][e1] = {"place_id": e2, "place_name": ctx.get("place_to_name", {}).get(e2, id2label.get(e2, "Unknown Place"))} # Note: place_to_name populated in pass 1 below
        elif rname == "manifestation_has_publication_date":
            raw_date = id2label.get(e2, "Unknown Date")
            ctx["manifestation_to_date"][e1] = translate_date_notes(raw_date)
        elif rname == "manifestation_has_publication_date_range": ctx["manifestation_to_publication_date_range"][e1] = id2label.get(e2, "Unknown Date Range")
        elif rname == "manifestation_has_number_of_volumes": ctx["manifestation_to_number_of_volumes"][e1] = id2label.get(e2, "Unknown")
        elif rname == "manifestation_has_short_title": ctx["manifestation_to_short_title"][e1] = id2label.get(e2, "Unknown Short Title")
        
        # Volume specific attributes
        elif rname == "manifestation_volume_has_short_title": ctx["volume_to_short_title"][e1] = id2label.get(e2, "Unknown Short Title")
        elif rname == "manifestation_volume_has_volume_title": ctx["volume_to_volume_title"][e1] = id2label.get(e2, "Unknown Volume Title")
        elif rname == "manifestation_volume_has_number_of_volumes": ctx["volume_to_number_of_volumes"][e1] = id2label.get(e2, "Unknown")
        elif rname == "manifestation_volume_has_publication_date":
            raw_date = id2label.get(e2, "Unknown Date")
            ctx["volume_to_date"][e1] = translate_date_notes(raw_date)
        elif rname == "manifestation_volume_has_publication_date_range": ctx["volume_to_publication_date_range"][e1] = id2label.get(e2, "Unknown Date Range")
        elif rname == "manifestation_volume_published_in_place": ctx["volume_to_place"][e1] = {"place_id": e2, "place_name": id2label.get(e2, "Unknown Place")}
        
        # Manifestation Roles
        elif rname in ["manifestation_published_by", "manifestation_volume_published_by"]:
            target_map = ctx["manifestation_to_publishers"] if "volume" not in rname else ctx["volume_to_publishers"]
            target_map[e1].append(e2)
            if id_to_collection.get(e2) == "person": ctx["person_to_roles"].setdefault(e2, {}).setdefault("Editore", []).append(e1)
            elif id_to_collection.get(e2) == "institution": ctx["institution_to_roles"][e2]["Editore"].append(e1)
        elif rname in ["manifestation_edited_by", "manifestation_volume_edited_by"]:
            target_map = ctx["manifestation_to_editors"]
            target_map[e1].append(e2)
            if id_to_collection.get(e2) == "person": ctx["person_to_roles"].setdefault(e2, {}).setdefault("Curatore", []).append(e1)
            elif id_to_collection.get(e2) == "institution": ctx["institution_to_roles"][e2]["Curatore"].append(e1)
        elif rname in ["manifestation_corrected_by", "manifestation_volume_corrected_by"]:
            target_map = ctx["manifestation_to_correctors"]
            target_map[e1].append(e2)
            if id_to_collection.get(e2) == "person": ctx["person_to_roles"].setdefault(e2, {}).setdefault("Correttore", []).append(e1)
            elif id_to_collection.get(e2) == "institution": ctx["institution_to_roles"][e2]["Correttore"].append(e1)
        elif rname in ["manifestation_sponsored_by", "manifestation_volume_sponsored_by"]:
            target_map = ctx["manifestation_to_sponsors"]
            target_map[e1].append(e2)
            if id_to_collection.get(e2) == "person": ctx["person_to_roles"].setdefault(e2, {}).setdefault("Finanziatore", []).append(e1)
            elif id_to_collection.get(e2) == "institution": ctx["institution_to_roles"][e2]["Finanziatore"].append(e1)

        # --- ITEM RELATIONS ---
        elif rname == "is_item_of_manifestation": 
            ctx["manifestation_to_item_ids"][e2].append(e1)
            ctx["item_to_manifestation"][e1] = e2

        elif rname == "item_has_manifestation_volume": 
            # Link item to its volume
            ctx["volume_to_item_ids"][e2].append(e1)
            # CRITICAL: Also link the item to the volume's PARENT manifestation
            parent_manifestation_id = ctx["volume_to_parent_manifestation_id"].get(e2)
            if parent_manifestation_id:
                ctx["item_to_manifestation"][e1] = parent_manifestation_id
     
        elif rname == "item_has_shelf_mark": ctx["item_to_shelf_mark"][e1] = id2label.get(e2, "Unknown Shelf Mark")
        elif rname == "item_has_preservation_status": ctx["item_to_preservation_status"][e1].append(id2label.get(e2, "Unknown Status"))
        elif rname == "item_has_material": ctx["item_to_material"][e1].append(id2label.get(e2, "Unknown Material"))
        elif rname == "item_has_type": ctx["item_to_type"][e1].append(id2label.get(e2, "Unknown Type"))
        elif rname == "item_has_page": ctx["item_to_page_ids"][e1].append(e2)
        elif rname == "item_contains_physical_object": ctx["item_to_physical_object_ids"][e1].append(e2)
        elif rname == "item_owned_by":
            ctx["item_to_owner"][e1].append(e2)
            if id_to_collection.get(e2) == "person":
                ctx["person_to_roles"].setdefault(e2, {}).setdefault("Owner of item", []).append(e1)
            elif id_to_collection.get(e2) == "institution":
                ctx["institution_to_roles"][e2]["Owner of item"].append(e1)

        # --- PAGE & VO RELATIONS ---
        elif rname == "page_has_name": ctx["page_to_name"][e1] = id2label.get(e2, "Unknown Page Name")
        elif rname == "page_sorting":
            try:
                ctx["page_to_sorting_number"][e1] = int(id2label.get(e2, "0"))
            except (ValueError, TypeError):
                ctx["page_to_sorting_number"][e1] = None
        elif rname == "page_from_manifestation": ctx["page_to_manifestation_map"][e1] = e2
        elif rname == "page_from_manifestation_volume": ctx["page_to_manifestation_volume_map"][e1] = e2
        elif rname == "page_contains_visual_object": ctx["page_to_visual_object_ids"][e1].append(e2)
        elif rname in ["page_contains_physical_object", "page_contains_physical_object_page"]: 
            ctx["page_to_physical_object_ids"][e1].append(e2)
        elif rname == "page_has_digital_representation": ctx["page_to_digital_representation"][e1] = id2label.get(e2, None)
        elif rname == "visual_object_has_name": ctx["vo_to_name"][e1] = id2label.get(e2, "Unknown VO Name")
        elif rname == "visual_object_has_transcription": ctx["vo_to_transcription"][e1] = id2label.get(e2, "")
        elif rname == "visual_object_has_type": ctx["vo_to_type"][e1].append(id2label.get(e2, "Unknown Type"))
        # --- NEW RELATIONS FOR VISUAL OBJECT ---
        elif rname == "visual_object_has_function": ctx["vo_to_function"][e1].append(id2label.get(e2, "Unknown Function"))
        elif rname == "visual_object_has_language": ctx["vo_to_language"][e1].append(id2label.get(e2, "Unknown Language"))
        elif rname == "visual_object_has_instrument": ctx["vo_to_instrument"][e1].append(id2label.get(e2, "Unknown Instrument"))
        elif rname == "visual_object_has_colour": ctx["vo_to_colour"][e1].append(id2label.get(e2, "Unknown Colour"))
        elif rname == "visual_object_has_transcription_quality": ctx["vo_to_transcription_quality"][e1].append(id2label.get(e2, "Unknown Quality"))
        # ---------------------------------------
        
        # VO Roles
        elif rname in ["visual_object_owned_by", "visual_object_owned_by_person", "visual_object_owned_by_institution"]:
            ctx["vo_to_owners"][e1].append(e2)
            if id_to_collection.get(e2) == "person": ctx["person_to_roles"].setdefault(e2, {}).setdefault("Possessore precedente", []).append(e1)
            elif id_to_collection.get(e2) == "institution": ctx["institution_to_roles"][e2]["Possessore precedente"].append(e1)
        elif rname in ["visual_object_inscribed_by", "visual_object_inscribed_by_person"]:
            ctx["vo_to_inscribers"][e1].append(e2)
            if id_to_collection.get(e2) == "person": ctx["person_to_roles"].setdefault(e2, {}).setdefault("Annotatore", []).append(e1)
            elif id_to_collection.get(e2) == "institution": ctx["institution_to_roles"][e2]["Annotatore"].append(e1)
        elif rname in ["visual_object_sent_by", "visual_object_sent_by_person"]:
            ctx["vo_to_senders"][e1].append(e2)
            if id_to_collection.get(e2) == "person": ctx["person_to_roles"].setdefault(e2, {}).setdefault("Dedicatore", []).append(e1)
            elif id_to_collection.get(e2) == "institution": ctx["institution_to_roles"][e2]["Dedicatore"].append(e1)
        elif rname in ["visual_object_received_by", "visual_object_received_by_person"]:
            ctx["vo_to_recipients"][e1].append(e2)
            if id_to_collection.get(e2) == "person": ctx["person_to_roles"].setdefault(e2, {}).setdefault("Dedicatario", []).append(e1)
            elif id_to_collection.get(e2) == "institution": ctx["institution_to_roles"][e2]["Dedicatario"].append(e1)

        # --- PERSON RELATIONS ---
        elif rname == "person_has_name": ctx["person_to_name"][e1] = id2label.get(e2, "Unknown Name")
        elif rname == "person_has_alias": ctx["person_to_aliases"][e1].append(id2label.get(e2, "Unknown Alias"))
        elif rname == "person_has_birth_date": ctx["person_to_birth_date"][e1] = id2label.get(e2, "")
        elif rname == "person_has_birth_date_notes":
            raw_note = id2label.get(e2, "")
            ctx["person_to_birth_date_notes"][e1] = translate_date_notes(raw_note)
        elif rname == "person_has_death_date": ctx["person_to_death_date"][e1] = id2label.get(e2, "")
        elif rname == "person_has_death_date_notes":
            raw_note = id2label.get(e2, "")
            ctx["person_to_death_date_notes"][e1] = translate_date_notes(raw_note)
        elif rname == "person_has_gender": ctx["person_to_gender"][e1] = id2label.get(e2, "")

        elif rname == "person_member_of_institution":
            ctx["person_to_roles"].setdefault(e1, {}).setdefault("Member of", []).append(e2)
            ctx["institution_to_roles"][e2]["Membro"].append(e1)


        # --- INSTITUTION RELATIONS ---
        elif rname == "institution_has_name": ctx["institution_to_name"][e1] = id2label.get(e2, "Unknown Institution")
        elif rname == "institution_has_founding_date": ctx["institution_to_founding_date"][e1] = id2label.get(e2, "")
        elif rname == "institution_has_dissolution_date": ctx["institution_to_dissolution_date"][e1] = id2label.get(e2, "")
        elif rname == "institution_located_at_place": ctx["institution_to_place"][e1] = {"place_id": e2, "place_name": id2label.get(e2, "Unknown Place")}

        # --- PLACE RELATIONS ---
        elif rname == "place_has_name": ctx["place_to_name"][e1] = id2label.get(e2, "Unknown Place")

        # --- EVENT RELATIONS ---
        elif rname == "event_has_name": ctx["event_to_name"][e1] = id2label.get(e2, "Unknown Event")
        elif rname == "event_has_date": ctx["event_to_date"][e1] = id2label.get(e2, "Unknown Date")
        elif rname == "event_occurred_at_place": ctx["event_to_place"][e1] = {"place_id": e2, "place_name": id2label.get(e2, "Unknown Place")}

        # --- PHYSICAL OBJECT RELATIONS ---
        elif rname == "physical_object_has_name": ctx["physical_object_to_name"][e1] = id2label.get(e2, "Unknown Physical Object Name")
        elif rname == "physical_object_has_description": ctx["physical_object_to_description"][e1] = id2label.get(e2, "")
        
      
   
        elif rname == "physical_object_has_type":
            ctx["po_to_type"][e1].append(id2label.get(e2, "Unknown Type"))
     
        
        # --- NEW PO RELATIONS ---
        elif rname == "physical_object_located_at_place": ctx["po_to_place"][e1] = {"place_id": e2, "place_name": id2label.get(e2, "Unknown Place")}
        elif rname == "physical_object_has_date": ctx["po_to_date"][e1] = id2label.get(e2, "Unknown Date")
        elif rname == "physical_object_has_insertion_type": ctx["po_to_insertion_type"][e1].append(id2label.get(e2, "Unknown Type"))
        # ------------------------

        elif rname == "physical_object_created_by":
            if id_to_collection.get(e2) == "person": ctx["person_to_roles"].setdefault(e2, {}).setdefault("Creatore dell’unità materiale", []).append(e1)
            elif id_to_collection.get(e2) == "institution": ctx["institution_to_roles"][e2]["Creatore dell’unità materiale"].append(e1)
        elif rname == "physical_object_owned_by":
            if id_to_collection.get(e2) == "person": ctx["person_to_roles"].setdefault(e2, {}).setdefault("Possessore dell’unità materiale", []).append(e1)
            elif id_to_collection.get(e2) == "institution": ctx["institution_to_roles"][e2]["Possessore dell’unità materiale"].append(e1)

        # --- ABSTRACT CHARACTER RELATIONS ---
        elif rname == "abstract_character_has_name": ctx["ac_to_name"][e1] = id2label.get(e2, "Unknown AC Name")
        elif rname == "abstract_character_has_alias": ctx["ac_to_aliases"][e1].append(id2label.get(e2, "Unknown Alias"))
        elif rname == "abstract_character_is_mentioning": ctx["ac_is_mentioning"][e1].append(e2)
        elif rname == "abstract_character_is_mentioned_by": ctx["ac_is_mentioned_by"][e1].append(e2)

        # --- HYPOTHESIS RELATIONS ---
        elif rname == "hypothesis_created_by_person": 
            ctx["hypothesis_to_creator_name"][e1] = e2 
            ctx["person_to_created_hypotheses"][e2].append(e1)
        
   
        elif rname == "is_hypothesis_about":
            ctx["hypothesis_to_about_ids"][e1].append(e2)
      
        
        if rname.endswith("_has_hypothesis"):
            ctx["entity_to_hypotheses_details"][e1].append({
                "hypothesis_id": e2, 
                "hypothesis_title": id2label.get(e2, "Unknown Hypothesis"), 
                "creator_name": "Unknown Creator" 
            })


    # Create a map of resolved "about" labels for each hypothesis using the now-populated context maps
    resolved_hypothesis_about = defaultdict(list)
    for hypo_id, about_ids in ctx["hypothesis_to_about_ids"].items():
        for about_id in about_ids:
            coll = id_to_collection.get(about_id)
            label = id2label.get(about_id, "Unknown") # Start with fallback
            
            if coll == "person":
                label = ctx["person_to_name"].get(about_id, label)
            elif coll == "work":
                label = ctx["work_to_uniform_title"].get(about_id, label)
            elif coll == "item":
                label = ctx["item_to_shelf_mark"].get(about_id, label)
            elif coll == "institution":
                label = ctx["institution_to_name"].get(about_id, label)
            elif coll == "event":
                label = ctx["event_to_name"].get(about_id, label)
            elif coll == "abstract_character":
                label = ctx["ac_to_name"].get(about_id, label)
            elif coll == "visual_object":
                label = ctx["vo_to_name"].get(about_id, label)
            elif coll == "physical_object":
                label = ctx["physical_object_to_name"].get(about_id, label)
            elif coll == "place":
                label = ctx["place_to_name"].get(about_id, label)
            
            resolved_hypothesis_about[hypo_id].append(label)

    # Fix up hypothesis creator names and "about" labels in the entity details
    for entity_id, hypos in ctx["entity_to_hypotheses_details"].items():
        for hypo in hypos:
            creator_id = ctx["hypothesis_to_creator_name"].get(hypo["hypothesis_id"])
            if creator_id:
                hypo["creator_name"] = ctx["person_to_name"].get(creator_id, id2label.get(creator_id, "Unknown Creator"))
            
            about_labels = resolved_hypothesis_about.get(hypo["hypothesis_id"], [])
            if about_labels:
                hypo["hypothesis_about"] = "; ".join(sorted(list(set(about_labels))))


    # Fix up place names in manifestation/institution/event/volume maps now that place_to_name is populated
    for m_id, place_data in ctx["manifestation_to_place"].items():
        place_data["place_name"] = ctx["place_to_name"].get(place_data["place_id"], place_data["place_name"])
    for v_id, place_data in ctx["volume_to_place"].items():
        place_data["place_name"] = ctx["place_to_name"].get(place_data["place_id"], place_data["place_name"])
    for i_id, place_data in ctx["institution_to_place"].items():
        place_data["place_name"] = ctx["place_to_name"].get(place_data["place_id"], place_data["place_name"])
    for e_id, place_data in ctx["event_to_place"].items():
        place_data["place_name"] = ctx["place_to_name"].get(place_data["place_id"], place_data["place_name"])
  
    for po_id, place_data in ctx["po_to_place"].items():
        place_data["place_name"] = ctx["place_to_name"].get(place_data["place_id"], place_data["place_name"])


    return ctx