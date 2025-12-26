# app/services/entities/visual_object.py
from typing import List, Dict, Any, Tuple
from app.services.common import extract_human_readable_id, normalize_language, calculate_richness
from app.textual_manipulation import apply_confusables, strip_diacritics

def process_visual_objects(
    raw_vos: List[Dict[str, Any]],
    context: Dict[str, Any],
    items_data_map: Dict[str, Any],
    manifestations_data_map: Dict[str, Any],
    manifestation_volumes_data_map: Dict[str, Any]
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    unique_vos_map = {}
    visual_object_details_cache = {}

    id2label = context["id2label"]
    id2projects = context["id2projects"]
    vo_to_transcription = context["vo_to_transcription"]
    vo_to_name = context["vo_to_name"]
    entity_to_hypotheses_details = context["entity_to_hypotheses_details"]

    # New context maps
    vo_to_type = context["vo_to_type"]
    vo_to_function = context["vo_to_function"]
    vo_to_language = context["vo_to_language"]
    vo_to_instrument = context["vo_to_instrument"]
    vo_to_colour = context["vo_to_colour"]
    vo_to_transcription_quality = context["vo_to_transcription_quality"] # Newly added

    # Role maps
    vo_to_owners = context["vo_to_owners"]
    vo_to_inscribers = context["vo_to_inscribers"]
    vo_to_senders = context["vo_to_senders"]
    vo_to_recipients = context["vo_to_recipients"]

    # Name lookups for roles
    person_to_name = context["person_to_name"]
    institution_to_name = context["institution_to_name"]


    page_to_digital_representation = context.get("page_to_digital_representation", {})


    # Build VO to Item map
    item_to_page_ids = context["item_to_page_ids"]
    page_to_visual_object_ids = context["page_to_visual_object_ids"]
    vo_to_item_map = {}
    for item_id, page_ids in item_to_page_ids.items():
        for page_id in page_ids:
            for vo_id in page_to_visual_object_ids.get(page_id, []):
                vo_to_item_map[vo_id] = item_id

    # Build VO to Page map
    vo_to_page_map = {}
    for page_id, vo_ids in page_to_visual_object_ids.items():
        for vo_id in vo_ids:
            vo_to_page_map[vo_id] = page_id


    # --- NEW: Build VO counter per page ---
    vo_counters = {}
    for page_id, vo_ids_on_page in page_to_visual_object_ids.items():
        # Sort VOs by their ID before assigning numbers for consistency
        sorted_vo_ids = sorted(vo_ids_on_page)
        for i, vo_id in enumerate(sorted_vo_ids):
            vo_counters[vo_id] = i + 1

    # --- NEW: Context maps for card building ---
    page_to_name = context.get("page_to_name", {})
    page_to_po_page_parent_map = context.get("page_to_po_page_parent_map", {})
    po_to_type = context.get("po_to_type", {})
    page_to_manifestation_map = context.get("page_to_manifestation_map", {})
    page_to_manifestation_volume_map = context.get("page_to_manifestation_volume_map", {})

    def resolve_names(ids: List[str]) -> List[str]:
        names = []
        for oid in ids:
            name = person_to_name.get(oid) or institution_to_name.get(oid) or id2label.get(oid, "Unknown")
            names.append(name)
        return sorted(list(set(names)))

    for vo in raw_vos:
        vo_id = vo["_id"]

        human_readable_id = extract_human_readable_id(vo, "VO_")
        if not human_readable_id:
            continue

        # --- Get parent page info ---
        parent_page_id = vo_to_page_map.get(vo_id)
        page_title = None
        digital_page = None

        if parent_page_id:
            page_name = page_to_name.get(parent_page_id)
            if page_name:
                page_title = f"Pagina {page_name}"
            digital_page = page_to_digital_representation.get(parent_page_id, None)


        # --- INHERIT ANCESTOR DATA ---
        parent_item_id = vo_to_item_map.get(vo_id)
        parent_item_details = items_data_map.get(parent_item_id) if parent_item_id else None

        # Defaults for inherited data
        inherited_fields = {
            "work_title": None, "authors": [], "classifications": [], "type_of_expression": [],
            "language": [], "publication_place": None, "publication_start_year": None,
            "publication_end_year": None, "work_title_normalized": set(), "author_search_terms_normalized": set(),
            "classifications_normalized": set(), "type_of_expression_normalized": set(),
            "language_normalized": set(), "publication_place_normalized": set()
        }

        # ---  1a: Inherit parent Item's owner ---
        item_owner = []

        if parent_item_details:
            inherited_fields["work_title"] = parent_item_details.get("work_title")
            inherited_fields["authors"] = parent_item_details.get("authors", [])
            inherited_fields["classifications"] = parent_item_details.get("classifications", [])
            inherited_fields["type_of_expression"] = parent_item_details.get("type_of_expression", [])
            inherited_fields["language"] = parent_item_details.get("language", [])
            inherited_fields["publication_place"] = parent_item_details.get("publication_place")
            inherited_fields["publication_start_year"] = parent_item_details.get("publication_start_year")
            inherited_fields["publication_end_year"] = parent_item_details.get("publication_end_year")
            inherited_fields["work_title_normalized"] = parent_item_details.get("work_title_normalized", set())
            inherited_fields["author_search_terms_normalized"] = parent_item_details.get("author_search_terms_normalized", set())
            inherited_fields["classifications_normalized"] = parent_item_details.get("classifications_normalized", set())
            inherited_fields["type_of_expression_normalized"] = parent_item_details.get("type_of_expression_normalized", set())
            inherited_fields["language_normalized"] = parent_item_details.get("language_normalized", set())
            inherited_fields["publication_place_normalized"] = parent_item_details.get("publication_place_normalized", set())
            item_owner = parent_item_details.get("owner", [])

        projects = id2projects.get(vo_id, ["Unknown Project"])
        if projects == ["Unknown Project"] and parent_item_details:
            projects = parent_item_details.get("projects", ["Unknown Project"])

        transcription = vo_to_transcription.get(vo_id, "")
        original_vo_name = vo_to_name.get(vo_id, id2label.get(vo_id, "Unknown Visual Object"))

        # ---  Construct new title ---
        counter = vo_counters.get(vo_id)
        if counter:
            vo_name = f"Unità visuale {counter} - {human_readable_id}"
        else:
            vo_name = original_vo_name # Fallback for VOs not on a page

        # ---Construct new card ---
        card = {
            "title": vo_name,
            "projects": projects,
            "human_readable_id": human_readable_id
        }

        if human_readable_id.startswith("VO_PAG_PO_"):
            if parent_page_id:
                card["Page:"] = page_to_name.get(parent_page_id, "Unknown Page")

                # Find the PO that is the parent of the page
                parent_po_id = page_to_po_page_parent_map.get(parent_page_id)
                if parent_po_id:
                    po_types = po_to_type.get(parent_po_id, [])
                    if po_types:
                        card["Unità materiale:"] = ", ".join(sorted(list(set(po_types))))

            if parent_item_details:
                card["Esemplare:"] = parent_item_details.get("card", {}).get("title", "Unknown Item")

            card["Transcription:"] = (transcription[:100] + '...') if len(transcription) > 100 else transcription

        elif human_readable_id.startswith("VO_PAG_M_") or human_readable_id.startswith("VO_PAG_M_VOL_"):
            if parent_page_id:
                card["Page:"] = page_to_name.get(parent_page_id, "Unknown Page")
                parent_volume_id = page_to_manifestation_volume_map.get(parent_page_id)
                parent_manifestation_id = page_to_manifestation_map.get(parent_page_id)

                if parent_volume_id and (vol_details := manifestation_volumes_data_map.get(parent_volume_id)):
                    card["Manifestazione:"] = vol_details.get("manifestation_volume_title", "Unknown Volume")
                elif parent_manifestation_id and (man_details := manifestations_data_map.get(parent_manifestation_id)):
                    card["Manifestazione:"] = man_details.get("manifestation_title", "Unknown Manifestation")

            card["Transcription:"] = (transcription[:100] + '...') if len(transcription) > 100 else transcription

        elif human_readable_id.startswith("VO_PAG_"):
            if parent_page_id:
                card["Page:"] = page_to_name.get(parent_page_id, "Unknown Page")

            if parent_item_details:
                card["Esemplare:"] = parent_item_details.get("card", {}).get("title", "Unknown Item")

            card["Transcription:"] = (transcription[:100] + '...') if len(transcription) > 100 else transcription
        else:
            # Fallback for other VOs if any
            card["transcription_snippet"] = (transcription[:100] + '...') if len(transcription) > 100 else transcription


        transcription_lower = transcription.lower()
        transcription_confusable = apply_confusables(transcription)
        transcription_normalized = strip_diacritics(transcription_confusable.lower())
        transcription_normalized_cs = strip_diacritics(transcription_confusable)

        # Get new fields
        types = sorted(list(set(vo_to_type.get(vo_id, []))))
        functions = sorted(list(set(vo_to_function.get(vo_id, []))))
        raw_languages = sorted(list(set(vo_to_language.get(vo_id, []))))
        languages = [normalize_language(l) for l in raw_languages]
        instruments = sorted(list(set(vo_to_instrument.get(vo_id, []))))
        colours = sorted(list(set(vo_to_colour.get(vo_id, []))))
        transcription_qualities = sorted(list(set(vo_to_transcription_quality.get(vo_id, [])))) # Newly added

        # Get roles
        owners = resolve_names(vo_to_owners.get(vo_id, []))
        inscribers = resolve_names(vo_to_inscribers.get(vo_id, []))
        senders = resolve_names(vo_to_senders.get(vo_id, []))
        recipients = resolve_names(vo_to_recipients.get(vo_id, []))

        # --- Digitalization Logic ---
        has_digital_representation = parent_page_id in page_to_digital_representation if parent_page_id else False
        # --- END Digitalization Logic ---

        # ---  1b: Create a comprehensive set of all related people/institutions ---
        all_people_and_institutions = set(owners) | set(inscribers) | set(senders) | set(recipients) | set(item_owner)
        all_people_and_institutions.update([author['name'] for author in inherited_fields.get("authors", [])])

        vo_data = {
            "visual_object_id": vo_id,
            "page_title": page_title,
            "digital_page": digital_page,
            "human_readable_id": human_readable_id,
            "visual_object_name": vo_name, # Use new name
            "projects": projects,
            "hypotheses": entity_to_hypotheses_details.get(vo_id, []),
            "card": card, # Use new card

            # New fields
            "type_of_visual_object": types,
            "visual_object_function": functions,
            "visual_object_language": languages,
            "visual_object_instrument": instruments,
            "visual_object_colour": colours,
            "transcription_quality": transcription_qualities, # Newly added
            "visual_object_owners": owners,
            "visual_object_inscribers": inscribers,
            "visual_object_senders": senders,
            "visual_object_recipients": recipients,
            "has_digital_representation": has_digital_representation, # MODIFICATION

            # Inherited fields
            **inherited_fields,

            # --- NEW FIELDS FOR FILTERING ---
            "item_owner": item_owner,
            "all_people_and_institutions": sorted(list(all_people_and_institutions)),

            # Normalized new fields
            "type_of_visual_object_normalized": {t.lower() for t in types},
            "visual_object_function_normalized": {f.lower() for f in functions},
            "visual_object_language_normalized": {l.lower() for l in languages},
            "visual_object_instrument_normalized": {i.lower() for i in instruments},
            "visual_object_colour_normalized": {c.lower() for c in colours},
            "transcription_quality_normalized": {q.lower() for q in transcription_qualities}, # Newly added
            "visual_object_owners_normalized": {o.lower() for o in owners},
            "visual_object_inscribers_normalized": {i.lower() for i in inscribers},
            "visual_object_senders_normalized": {s.lower() for s in senders},
            "visual_object_recipients_normalized": {r.lower() for r in recipients},

            # --- NEW NORMALIZED FIELDS FOR FILTERING ---
            "item_owner_normalized": {o.lower() for o in item_owner},
            "all_people_and_institutions_normalized": {p.lower() for p in all_people_and_institutions},

            # Transcription fields
            "transcription_original": transcription,
            "transcription_lower": transcription_lower,
            "transcription_normalized": transcription_normalized,
            "transcription_normalized_cs": transcription_normalized_cs,
            "transcription_original_words": set(transcription.split()),
            "transcription_lower_words": set(transcription_lower.split()),
            "transcription_normalized_words": set(transcription_normalized.split()),
            "transcription_normalized_cs_words": set(transcription_normalized_cs.split()),
            "transcription_original_tokens": transcription.split(),
            "transcription_lower_tokens": transcription_lower.split(),
            "transcription_normalized_tokens": transcription_normalized.split(),
            "transcription_normalized_cs_tokens": transcription_normalized_cs.split(),
        }

        # --- DEDUPLICATION LOGIC ---
        # Key is now (Name, HumanReadableID)
        unique_key = (vo_name, vo_data["human_readable_id"])

        if unique_key in unique_vos_map:
            existing = unique_vos_map[unique_key]
            if calculate_richness(vo_data) > calculate_richness(existing):
                unique_vos_map[unique_key] = vo_data
                visual_object_details_cache[vo_id] = {
                    "vo_name": vo_name,
                    "page_title": page_title,
                    "digital_page": digital_page,
                    "relationships": [],
                    "grouped_relationships": {},
                    "projects": projects
                }
        else:
            unique_vos_map[unique_key] = vo_data
            visual_object_details_cache[vo_id] = {
                "vo_name": vo_name,
                "page_title": page_title,
                "digital_page": digital_page,
                "relationships": [],
                "grouped_relationships": {},
                "projects": projects
            }

    return list(unique_vos_map.values()), visual_object_details_cache