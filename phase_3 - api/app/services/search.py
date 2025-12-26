import re
from typing import Dict, Any, List, Set, Optional

from app.textual_manipulation import apply_confusables, strip_diacritics
from app.models import SearchQuery, EntityType, FilterableField, OrderByField, Logic, Operator, FilterRule, GraphSearchQuery, GraphType, Era, ProximityQuery, ProximityLogic, ProximityOperator
from app.store import store

# --- DETAILS LOOKUP

entity_to_cache_map = {
    "person": "person_details",
    "work": "work_details",
    "hypothesis": "hypothesis_details",
    "expression": "expression_details",
    "abstract_character": "ac_details",
    "event": "event_details", 
    "visual_object": "visual_object_details",
    "manifestation": "manifestation_details",
    "manifestation_volume": "manifestation_volume_details",
    "place": "place_details",
    "item": "item_details",
    "page": "page_details",
    "institution": "institution_details",
    "physical_object": "physical_object_details",
}

def get_entity_details(entity: str, entity_id: str) -> Dict[str, Any]:
    """
    Retrieves details for a given entity type and its ID from the cache.
    Raises KeyError if not found.
    """
    if not store.is_ready:
        raise RuntimeError("Data is still being loaded")

    cache_key = entity_to_cache_map.get(entity)
    if not cache_key:
        raise KeyError(f"Entity type '{entity}' not found")

    details_cache = store.cache.get(cache_key, {})
    details = details_cache.get(entity_id)

    if not details:
        entity_name = entity.replace('_', ' ').title()
        raise KeyError(f"{entity_name} with ID '{entity_id}' not found")

    return details

# --- GRAPH SEARCH LOGIC ---
def run_graph_search(query: GraphSearchQuery) -> Dict[str, List[Dict[str, Any]]]:
    """
    Constructs a graph of nodes and edges based on the specified graph type and filters.
    """

    if query.graph_type == GraphType.GENERAL:
        return _run_general_graph_search(query)
    elif query.graph_type == GraphType.MENTIONS:
        return _run_mentions_graph_search(query)
    elif query.graph_type == GraphType.PERSON_AUTHORSHIP_OWNERSHIP:
        return _run_person_authorship_ownership_graph_search(query)
    else:
        return {"nodes": [], "edges": []}


def _run_person_authorship_ownership_graph_search(query: GraphSearchQuery) -> Dict[str, List[Dict[str, Any]]]:
    """
    Handles the logic for the 'person_authorship_ownership' graph type.
    """
    nodes: Dict[str, Dict[str, Any]] = {}
    edges: List[Dict[str, Any]] = []
    
    all_relations = store.cache.get("all_relations", [])
    id_to_details_map = store.cache.get("id_to_details_map", {})
    filters = query.person_authorship_ownership_filters


    # Normalize incoming entity types from the frontend (e.g., "visual object" -> "visual_object")
    normalized_entity_types = {et.lower().replace(' ', '_') for et in filters.entity_types}
    entity_types_to_include = normalized_entity_types
    entity_types_to_include.add("person") # Ensure person is always included


    # 1. Get the set of person IDs to filter by.
    person_ids_to_filter = set()
    # If person_names filter is present and not empty, filter by those names.
    if filters.person_names:
        persons_cache = store.cache.get("persons", [])
        person_names_set = set(filters.person_names)
        for person in persons_cache:
            if person.get("person_name") in person_names_set:
                person_ids_to_filter.add(person.get("person_id"))
    # If person_names is not provided or is empty, we consider all persons.
    # In this case, person_ids_to_filter remains empty, and we'll check against it later.

    query_projects = set(query.projects) if query.projects else set()
    relationships_to_include = set(filters.relationships)

    for rel in all_relations:
        rel_name = rel.get("name")
        # Filter by relation type
        if rel_name not in relationships_to_include:
            continue

        source_id, target_id = rel.get("source_id"), rel.get("target_id")
        if not source_id or not target_id:
            continue

        source_details = id_to_details_map.get(source_id, {})
        target_details = id_to_details_map.get(target_id, {})
        source_type = source_details.get("type")
        target_type = target_details.get("type")

        # 2. Check if the relation involves a person and an allowed entity type.
        is_relevant_relation = False
        person_id_in_relation = None
        
        if source_type == "person" and target_type in entity_types_to_include:
            is_relevant_relation = True
            person_id_in_relation = source_id
        elif target_type == "person" and source_type in entity_types_to_include:
            is_relevant_relation = True
            person_id_in_relation = target_id
        
        if not is_relevant_relation:
            continue

        # 3. If a person name filter is active, check if the person in the relation matches.
        if person_ids_to_filter and person_id_in_relation not in person_ids_to_filter:
            continue
            
        # 4. Project filtering
        if query_projects:
            source_projects = set(source_details.get("projects", []))
            target_projects = set(target_details.get("projects", []))
            # The relation is included if at least one of the entities belongs to the queried projects.
            if not source_projects.intersection(query_projects) and not target_projects.intersection(query_projects):
                continue

        # 5. Add nodes and edges if all checks pass.
        if source_id not in nodes:
            nodes[source_id] = {"id": source_id, "label": source_details.get("label", ""), "title": source_details.get("title", ""), "entity_type": source_type, "projects": source_details.get("projects", [])}
        if target_id not in nodes:
            nodes[target_id] = {"id": target_id, "label": target_details.get("label", ""), "title": target_details.get("title", ""), "entity_type": target_type, "projects": target_details.get("projects", [])}
        
        edges.append({"source": source_id, "target": target_id, "type": rel_name, "direction": "outgoing"})
        edges.append({"source": target_id, "target": source_id, "type": rel_name, "direction": "incoming"})

    return {"nodes": list(nodes.values()), "edges": edges}

# Removed _run_person_centric_graph_search function

def _run_general_graph_search(query: GraphSearchQuery) -> Dict[str, List[Dict[str, Any]]]:
    """
    Handles the logic for the 'general' graph type.
    """
    nodes: Dict[str, Dict[str, Any]] = {}
    edges: List[Dict[str, Any]] = []
    
    all_relations = store.cache.get("all_relations", [])
    id_to_details_map = store.cache.get("id_to_details_map", {})
    
    # Add this block near the top of the function
    vo_to_page_map: Dict[str, str] = {}
    for rel in all_relations:
        if rel.get("name") == "page_contains_visual_object":
            page_id, vo_id = rel.get("source_id"), rel.get("target_id")
            if page_id and vo_id:
                vo_to_page_map[vo_id] = page_id
    
    entity_types_to_include = set(query.general_filters.entity_types)
    if "manifestation" in entity_types_to_include:
        entity_types_to_include.add("manifestation_volume")

    special_vo_rels = {
        "visual_object_inscribed_by",
        "visual_object_sent_by",
        "visual_object_received_by",
        "visual_object_owned_by"
    }
    
    relationships_to_filter = set(query.general_filters.relationships)
    query_projects = set(query.projects) if query.projects else set()

    # --- Removed work_titles filter logic ---
    for rel in all_relations:
        rel_name = rel.get("name")
        source_id, target_id = rel.get("source_id"), rel.get("target_id")
        
        if not source_id or not target_id:
            continue

        source_details = id_to_details_map.get(source_id, {})
        target_details = id_to_details_map.get(target_id, {})
        source_type = source_details.get("type")
        target_type = target_details.get("type")

        is_special_vo_rel = rel_name in special_vo_rels and rel_name in relationships_to_filter
        
        
        if is_special_vo_rel:
            # This relationship connects a VO (source) to a Person/Institution (target)
            if source_type == "visual_object" and target_type in ["person", "institution"]:
                # Find the Page that contains this Visual Object
                page_id = vo_to_page_map.get(source_id)
                
                # If the VO is on a Page and the user has filtered for the target's type (Person/Institution)
                if page_id and target_type in entity_types_to_include:
                    page_details = id_to_details_map.get(page_id, {})
                    
                    # Project filtering
                    if query_projects:
                        page_projects = set(page_details.get("projects", []))
                        target_projects = set(target_details.get("projects", []))
                        if not page_projects.intersection(query_projects) and not target_projects.intersection(query_projects):
                            continue

                    # Add the PAGE node (instead of the VO node)
                    if page_id not in nodes:
                        nodes[page_id] = {"id": page_id, "label": page_details.get("label", ""), "title": page_details.get("title", ""), "entity_type": "page", "projects": page_details.get("projects", [])}
                    
                    # Add the Person/Institution node
                    if target_id not in nodes:
                        nodes[target_id] = {"id": target_id, "label": target_details.get("label", ""), "title": target_details.get("title", ""), "entity_type": target_type, "projects": target_details.get("projects", [])}
                    
                    # Create the edge between the PAGE and the Person/Institution
                    edges.append({"source": page_id, "target": target_id, "type": rel_name, "direction": "outgoing"})
                    edges.append({"source": target_id, "target": page_id, "type": rel_name, "direction": "incoming"})

            # After handling this special case, always skip to the next relation
            continue

        if rel_name not in relationships_to_filter:
            continue

        if source_type in ["page", "visual_object"] or target_type in ["page", "visual_object"]:
            continue
        
        if source_type not in entity_types_to_include or target_type not in entity_types_to_include:
            continue

        if query_projects:
            source_projects = set(source_details.get("projects", []))
            target_projects = set(target_details.get("projects", []))
            if not source_projects.intersection(query_projects) and not target_projects.intersection(query_projects):
                continue

        if source_id not in nodes:
            nodes[source_id] = {"id": source_id, "label": source_details.get("label", ""), "title": source_details.get("title", ""), "entity_type": source_type, "projects": source_details.get("projects", [])}
        if target_id not in nodes:
            nodes[target_id] = {"id": target_id, "label": target_details.get("label", ""), "title": target_details.get("title", ""), "entity_type": target_type, "projects": target_details.get("projects", [])}
            
        edges.append({"source": source_id, "target": target_id, "type": rel_name, "direction": "outgoing"})
        edges.append({"source": target_id, "target": source_id, "type": rel_name, "direction": "incoming"})
    # --- MODIFICATION END ---

    return {"nodes": list(nodes.values()), "edges": edges}

def _run_mentions_graph_search(query: GraphSearchQuery) -> Dict[str, List[Dict[str, Any]]]:
    """
    Handles the logic for the 'mentions' graph type.
    """
    nodes: Dict[str, Dict[str, Any]] = {}
    edges: List[Dict[str, Any]] = []
    
    all_relations = store.cache.get("all_relations", [])
    id_to_details_map = store.cache.get("id_to_details_map", {})
    filters = query.mentions_filters
    query_projects = set(query.projects) if query.projects else set()

    relation_suffixes = []
    if "Mentioning" in filters.mention_directions:
        relation_suffixes.append("_is_mentioning")
    if "Mentioned by" in filters.mention_directions:
        relation_suffixes.append("_is_mentioned_by")

    mention_relations = [
        rel for rel in all_relations
        if any(rel.get("name", "").endswith(suffix) for suffix in relation_suffixes)
    ]

    #  Removed work_titles filter logic ---
    relevant_relations = mention_relations

    for rel in relevant_relations:

        source_id, target_id = rel.get("source_id"), rel.get("target_id")
        if not source_id or not target_id:
            continue

        source_details = id_to_details_map.get(source_id, {})
        target_details = id_to_details_map.get(target_id, {})
        source_type = source_details.get("type")
        target_type = target_details.get("type")

        if source_type not in filters.entity_types or target_type not in filters.entity_types:
            continue
        
        if query_projects:
            source_projects = set(source_details.get("projects", []))
            target_projects = set(target_details.get("projects", []))
            if not source_projects.intersection(query_projects) and not target_projects.intersection(query_projects):
                continue

        if source_id not in nodes:
            nodes[source_id] = {"id": source_id, "label": source_details.get("label", ""), "title": source_details.get("title", ""), "entity_type": source_type, "projects": source_details.get("projects", [])}
        if target_id not in nodes:
            nodes[target_id] = {"id": target_id, "label": target_details.get("label", ""), "title": target_details.get("title", ""), "entity_type": target_type, "projects": target_details.get("projects", [])}
        
        edges.append({"source": source_id, "target": target_id, "type": rel.get("name"), "direction": "outgoing"})
        edges.append({"source": target_id, "target": source_id, "type": rel.get("name"), "direction": "incoming"})

    return {"nodes": list(nodes.values()), "edges": edges}


# --- SEARCH LOGIC ---

def _get_normalized_attr(item: Dict[str, Any], field_name: FilterableField) -> Set[str]:
    """
    Maps a filterable field from the API to its corresponding normalized
    data field in the cached item.
    """
    field_map = {
        # Work
        FilterableField.AUTHOR: "author_search_terms_normalized",
        FilterableField.CLASSIFICATION: "classifications_normalized",
        FilterableField.WORK_TITLE: "work_title_normalized",
        # Expression
        FilterableField.TYPE_OF_EXPRESSION: "type_of_expression_normalized",
        FilterableField.LANGUAGE: "language_normalized",
        FilterableField.TRANSLATOR: "translators_normalized",
        FilterableField.EXPRESSION_EDITOR: "expression_editors_normalized",
        FilterableField.SCRIPTWRITER: "scriptwriters_normalized",
        FilterableField.COMPOSITOR: "compositors_normalized",
        FilterableField.REVIEWER: "reviewers_normalized",
        FilterableField.OTHER_SECONDARY_ROLE: "other_secondary_roles_normalized",
        # Manifestation
        FilterableField.PLACE: "publication_place_normalized",
        FilterableField.PUBLISHER: "publisher_normalized",
        FilterableField.EDITOR: "editor_normalized",
        FilterableField.CORRECTOR: "corrector_normalized",
        FilterableField.SPONSOR: "sponsor_normalized",
        # Item
        FilterableField.PRESERVATION_STATUS: "preservation_status_normalized",
        FilterableField.OWNER: "owner_normalized",
        FilterableField.MATERIAL: "material_normalized",
        FilterableField.TYPE_OF_ITEM: "type_of_item_normalized",
        # Visual Object
        FilterableField.VISUAL_OBJECT_OWNER: "visual_object_owners_normalized",
        FilterableField.VISUAL_OBJECT_INSCRIBER: "visual_object_inscribers_normalized",
        FilterableField.VISUAL_OBJECT_SENDER: "visual_object_senders_normalized",
        FilterableField.VISUAL_OBJECT_RECIPIENT: "visual_object_recipients_normalized",
        FilterableField.TYPE_OF_VISUAL_OBJECT: "type_of_visual_object_normalized",
        FilterableField.VISUAL_OBJECT_FUNCTION: "visual_object_function_normalized",
        FilterableField.VISUAL_OBJECT_LANGUAGE: "visual_object_language_normalized",
        FilterableField.VISUAL_OBJECT_INSTRUMENT: "visual_object_instrument_normalized",
        FilterableField.VISUAL_OBJECT_COLOUR: "visual_object_colour_normalized",
        # Physical Object
        FilterableField.TYPE_OF_PHYSICAL_OBJECT: "type_of_physical_object_normalized",
        FilterableField.PHYSICAL_OBJECT_PLACE: "physical_object_place_normalized",
        # Person
        FilterableField.PERSON_NAME: "person_name_normalized",
        FilterableField.PERSON_ROLE: "roles_normalized",
        FilterableField.PERSON_GENDER: "gender_normalized",
        # Institution
        FilterableField.INSTITUTION_NAME: "institution_name_normalized",
        FilterableField.INSTITUTION_PLACE: "institution_place_normalized",
        FilterableField.INSTITUTION_ROLE: "roles_normalized", 
        # Event
        FilterableField.EVENT_NAME: "event_name_normalized",
        FilterableField.PERSON_OR_INSTITUTION: "all_people_and_institutions_normalized",
        # Digitalization
        FilterableField.DIGITALIZATION: "has_digital_representation",
        # Abstract Character
        FilterableField.ABSTRACT_CHARACTER_NAME: "ac_name_normalized"
    }
    return item.get(field_map.get(field_name), set())

def _check_text_match(rule: FilterRule, item: Dict[str, Any]) -> bool:
    """
    Handles text search logic based on case and diacritics sensitivity flags.
    """
    search_query = rule.values[0] if rule.values else ""
    if not search_query:
        return True

    # Determine which pre-processed data field and query version to use based on flags.
    if rule.diacritics_sensitive:
        # For sensitive search, do NOT normalize scripts (confusables).
        # Just handle case sensitivity.
        if rule.case_sensitive:
            text_to_search = item.get("transcription_original", "")
            words_to_search = item.get("transcription_original_words", set())
            query_to_use = search_query
        else:  # case-insensitive
            text_to_search = item.get("transcription_lower", "")
            words_to_search = item.get("transcription_lower_words", set())
            query_to_use = search_query.lower()
    else:
        # For insensitive search, normalize scripts (confusables) first.
        query_confusable = apply_confusables(search_query)
        if rule.case_sensitive:
            text_to_search = item.get("transcription_normalized_cs", "")
            words_to_search = item.get("transcription_normalized_cs_words", set())
            query_to_use = strip_diacritics(query_confusable)
        else:  # case-insensitive
            text_to_search = item.get("transcription_normalized", "")
            words_to_search = item.get("transcription_normalized_words", set())
            query_to_use = strip_diacritics(query_confusable.lower())

    # Perform the search based on the operator
    if rule.op == Operator.PHRASE:
        return query_to_use in text_to_search
    
    query_words = set(query_to_use.split())
    if rule.op == Operator.ALL_WORDS:
        return query_words.issubset(words_to_search)
    elif rule.op == Operator.ANY_WORD:
        return not query_words.isdisjoint(words_to_search)
    
    return False

def _check_proximity_match(query: ProximityQuery, item: Dict[str, Any]) -> bool:
    """
    Handles proximity text search logic.
    """
    if not query or not query.terms:
        return True

    # 1. Select the correct token list based on sensitivity flags
    if query.diacritics_sensitive:
        if query.case_sensitive:
            tokens = item.get("transcription_original_tokens", [])
        else:
            tokens = item.get("transcription_lower_tokens", [])
    else:
        if query.case_sensitive:
            tokens = item.get("transcription_normalized_cs_tokens", [])
        else:
            tokens = item.get("transcription_normalized_tokens", [])
    
    if not tokens:
        return False

    # 2. Normalize search terms
    normalized_terms = []
    for term in query.terms:
        text = term.text
        if not query.diacritics_sensitive:
            text = apply_confusables(text)
            text = strip_diacritics(text)
        if not query.case_sensitive:
            text = text.lower()
        normalized_terms.append(text)

    # 3. Helper to find term indices
    def find_term_indices(term_text: str, token_list: List[str]) -> List[int]:
        indices = []
        # Strip common punctuation for non-exact matches
        punctuation_to_strip = '.,;:!?'
        for i, token in enumerate(token_list):
            if query.exact_match:
                if token == term_text:
                    indices.append(i)
            else:
                # By default, it's a phrase/substring search within a token
                if term_text in token.rstrip(punctuation_to_strip):
                    indices.append(i)
        return indices

    # 4. Find all occurrences of the primary term
    primary_term_indices = find_term_indices(normalized_terms[0], tokens)
    if not primary_term_indices:
        return False

    # If only one term, the presence is enough
    if len(query.terms) == 1:
        return True

    # 5. Check proximity conditions for each primary term occurrence
    for p_idx in primary_term_indices:
        results = []
        # Check conditions for the 2nd and 3rd terms
        for i in range(1, len(query.terms)):
            term_indices = find_term_indices(normalized_terms[i], tokens)
            if not term_indices:
                results.append(False)
                continue

            op = query.terms[i].proximity
            found_match = False
            for t_idx in term_indices:
                if op == ProximityOperator.NEAR:
                    if abs(p_idx - t_idx) <= query.distance and p_idx != t_idx:
                        found_match = True
                        break
                elif op == ProximityOperator.BEFORE:
                    if 0 < (p_idx - t_idx) <= query.distance:
                        found_match = True
                        break
                elif op == ProximityOperator.AFTER:
                    if 0 < (t_idx - p_idx) <= query.distance:
                        found_match = True
                        break
            results.append(found_match)

        # 6. Combine results using the logic (AND/OR/NOT)
        # Initial result is from the second term (index 0 in `results` list)
        final_result = results[0]
        
        # If there's a third term, combine its result
        if len(query.terms) == 3:
            logic = query.terms[2].logic
            term3_result = results[1]
            if logic == ProximityLogic.AND:
                final_result = final_result and term3_result
            elif logic == ProximityLogic.OR:
                final_result = final_result or term3_result
            elif logic == ProximityLogic.NOT:
                final_result = final_result and not term3_result
        
        if final_result:
            return True # Found a valid combination, no need to check other primary term occurrences

    return False

def _rule_matches_item(rule: FilterRule, item: Dict[str, Any]) -> bool:
    """
    Checks if a single data item matches a filter rule.
    The logic for values within a rule is OR.
    """
    if rule.field == FilterableField.VISUAL_OBJECT_TRANSCRIPTION:
        if item.get("visual_object_id"):
            return _check_text_match(rule, item)
        return False
    
    if rule.field == FilterableField.PROXIMITY_TEXT_SEARCH:
        if item.get("visual_object_id"):
            return _check_proximity_match(rule.proximity_query, item)
        return False

    # --- UPDATED ENUM CHECK ---
    if rule.field == FilterableField.SEARCH_FOR_ROLES_IN_EXPRESSION:
        expression_role_map = {
            # "Author" removed from here as requested
            "Traduttore": item.get("translators", []),
            "Curatore": item.get("expression_editors", []),
            "Sceneggiatore": item.get("scriptwriters", []),
            "Compositore": item.get("compositors", []),
            "Recensore": item.get("reviewers", []),
            "Altro ruolo secondario": item.get("other_secondary_roles", [])
        }
        for role_name in rule.values:
            if expression_role_map.get(role_name):
                return True
        return False

    # --- UPDATED ENUM CHECK ---
    if rule.field == FilterableField.SEARCH_FOR_ROLES_IN_MANIFESTATION:
        manifestation_role_map = {
            "Editore": item.get("publisher", []),
            "Curatore": item.get("editor", []),
            "Correttore": item.get("corrector", []),
            "Finanziatore": item.get("sponsor", [])
        }
        for role_name in rule.values:
            if manifestation_role_map.get(role_name):
                return True
        return False

    if rule.field == FilterableField.ROLES_RELATED_TO_VISUAL_OBJECT:
        item_role_map = {
            "Possessore precedente": item.get("visual_object_owners", []),
            "Annotatore": item.get("visual_object_inscribers", []),
            "Dedicatore": item.get("visual_object_senders", []),
            "Dedicatario": item.get("visual_object_recipients", [])
        }
        for role_name in rule.values:
            if item_role_map.get(role_name):
                return True
        return False
    
    # --- FIX 3: Ensure this logic handles both persons and institutions ---
    if rule.field == FilterableField.ROLES_RELATED_TO_PHYSICAL_OBJECT:
        item_role_map = {
            "Possessore dell’unità materiale": item.get("owners", []),
            "Creatore dell’unità materiale": item.get("creators", [])
        }
        for role_name in rule.values:
            if item_role_map.get(role_name):
                return True
        return False
    
    # --- NEW: Digitalization Filter Logic ---
    if rule.field == FilterableField.DIGITALIZATION:
        if "cerca solo Item con scansioni online" in rule.values:
            return item.get("has_digital_representation", False)
        return False

    # --- NEW: Abstract Character Mentioned In Filter ---
    if rule.field == FilterableField.ABSTRACT_CHARACTER_MENTIONED_IN:
        mentioned_in_types = item.get("mentioned_by_entity_types", set())
        search_types = set(rule.values)
        return not mentioned_in_types.isdisjoint(search_types)

    # --- FIX 1a: Special handling for the "owner" filter on Visual Objects ---
    if rule.field == FilterableField.OWNER and "visual_object_id" in item:
        # For VOs, "owner" refers to the parent item's owner.
        attributes = item.get("item_owner_normalized", set())
    else:
        # Default behavior for all other cases.
        attributes = _get_normalized_attr(item, rule.field)


    for value in rule.values:
        if value == "__EMPTY__":
            if not attributes:
                return True
        elif rule.op == Operator.CONTAINS:
            if any(value.lower() in attr for attr in attributes):
                return True
        else:  # Operator.EQUALS
            if value.lower() in attributes:
                return True
    
    return False

def run_search(query: SearchQuery) -> Dict[str, Any]:
    """
    Runs the search using set-based logic and handles multi-project entities.
    """
    if not store.is_ready:
        raise RuntimeError("Data is still being loaded")

    entity_map = {
        EntityType.WORK: "works", EntityType.EXPRESSION: "expressions",
        EntityType.MANIFESTATION: "manifestations", EntityType.ITEM: "items",
        EntityType.PAGE: "pages",
        EntityType.PERSON: "persons", EntityType.VISUAL_OBJECT: "visual_objects",
        EntityType.PHYSICAL_OBJECT: "physical_objects", EntityType.INSTITUTION: "institutions", 
        EntityType.EVENT: "events", EntityType.ABSTRACT_CHARACTER: "abstract_characters",
    }
    source_data = store.cache.get(entity_map[query.entity], [])
    id_key = f"{query.entity.value}_id"

    if query.projects:
        query_projects_set = set(query.projects)
        data_to_filter = [
            d for d in source_data 
            if not query_projects_set.isdisjoint(d.get('projects', []))
        ]
    else:
        data_to_filter = source_data

    if not query.rules:
        total_count = len(data_to_filter)
        paginated_results = data_to_filter[query.offset : query.offset + query.limit]
        final_projected_results = _project_results(paginated_results, query)
        return {"count": total_count, "results": final_projected_results}

    manifestation_date_rules = [r for r in query.rules if r.field == FilterableField.PUBLICATION_DATE]
    person_date_rules = [r for r in query.rules if r.field in [FilterableField.PERSON_BIRTH_DATE, FilterableField.PERSON_DEATH_DATE]]
    po_date_rules = [r for r in query.rules if r.field == FilterableField.PHYSICAL_OBJECT_DATE]
    event_date_rules = [r for r in query.rules if r.field == FilterableField.EVENT_DATE]
    
    if query.entity == EntityType.PERSON:
        data_to_filter = [item for item in data_to_filter if _passes_person_date_rules(item, person_date_rules)]
    elif query.entity == EntityType.PHYSICAL_OBJECT:
        data_to_filter = [item for item in data_to_filter if _passes_po_date_rules(item, po_date_rules)]
    elif query.entity == EntityType.EVENT:
        data_to_filter = [item for item in data_to_filter if _passes_event_date_rules(item, event_date_rules)]
    else:
        data_to_filter = [item for item in data_to_filter if _passes_date_rules(item, manifestation_date_rules)]

    chainable_rules = [r for r in query.rules if r not in manifestation_date_rules and r not in person_date_rules and r not in po_date_rules and r not in event_date_rules]

    if not chainable_rules:
        matching_items = data_to_filter
    else:
        current_results_ids = {item[id_key] for item in data_to_filter if _rule_matches_item(chainable_rules[0], item)}

        for i in range(1, len(chainable_rules)):
            logic_to_apply = chainable_rules[i-1].logic
            current_rule = chainable_rules[i]

            rule_match_ids = {item[id_key] for item in data_to_filter if _rule_matches_item(current_rule, item)}

            if logic_to_apply == Logic.AND:
                current_results_ids.intersection_update(rule_match_ids)
            elif logic_to_apply == Logic.OR:
                current_results_ids.update(rule_match_ids)
            elif logic_to_apply == Logic.NOT:
                current_results_ids.difference_update(rule_match_ids)
        
        matching_items = [item for item in data_to_filter if item.get(id_key) in current_results_ids]

    final_results_with_search_terms = matching_items
    
    default_sort_key = {
        EntityType.WORK: OrderByField.WORK_TITLE, EntityType.EXPRESSION: OrderByField.EXPRESSION_TITLE,
        EntityType.MANIFESTATION: OrderByField.MANIFESTATION_TITLE, EntityType.ITEM: OrderByField.ITEM_LABEL,
        EntityType.PAGE: OrderByField.PAGE_TITLE,
        EntityType.PERSON: OrderByField.PERSON_NAME, EntityType.VISUAL_OBJECT: OrderByField.VISUAL_OBJECT_NAME,
        EntityType.PHYSICAL_OBJECT: OrderByField.PHYSICAL_OBJECT_NAME, EntityType.INSTITUTION: OrderByField.INSTITUTION_NAME, 
        EntityType.EVENT: OrderByField.EVENT_NAME,
        EntityType.ABSTRACT_CHARACTER: OrderByField.ABSTRACT_CHARACTER_NAME,
    }.get(query.entity, OrderByField.WORK_TITLE)
    sort_key = query.order_by.value if query.order_by else default_sort_key.value
    
    if query.entity == EntityType.ITEM:
        final_results_with_search_terms.sort(key=lambda x: (x['card']['title'] is None, x['card']['title']))
    else:
        final_results_with_search_terms.sort(key=lambda x: (x.get(sort_key) is None, x.get(sort_key)))


    total_count = len(final_results_with_search_terms)
    paginated_results = final_results_with_search_terms[query.offset : query.offset + query.limit]
    final_projected_results = _project_results(paginated_results, query)

    return {"count": total_count, "results": final_projected_results}


#  Helper functions for search logic (some are unchanged)

def _passes_date_rules(item: Dict[str, Any], date_rules: List[FilterRule]) -> bool:
    """
    Checks if an item's date range overlaps with the search date range.
    """
    if not date_rules:
        return True

    item_start = item.get("publication_start_year")
    item_end = item.get("publication_end_year") or item_start

    if item_start is None:
        return False

    gte_rule = next((r for r in date_rules if r.logic == Logic.GTE), None)
    lte_rule = next((r for r in date_rules if r.logic == Logic.LTE), None)

    search_start_str = gte_rule.values[0] if gte_rule and gte_rule.values else None
    search_end_str = lte_rule.values[0] if lte_rule and lte_rule.values else None

    search_start = _parse_year_input(search_start_str, is_start_of_range=True)
    search_end = _parse_year_input(search_end_str, is_start_of_range=False)

    starts_before_search_ends = (search_end is None) or (item_start <= search_end)
    ends_after_search_starts = (search_start is None) or (item_end >= search_start)

    return starts_before_search_ends and ends_after_search_starts

def _passes_event_date_rules(item: Dict[str, Any], date_rules: List[FilterRule]) -> bool:
    """
    Checks if an event's date range overlaps with the search date range.
    """
    if not date_rules:
        return True

    item_start = item.get("start_year")
    item_end = item.get("end_year") or item_start

    if item_start is None:
        return False

    gte_rule = next((r for r in date_rules if r.logic == Logic.GTE), None)
    lte_rule = next((r for r in date_rules if r.logic == Logic.LTE), None)

    search_start_str = gte_rule.values[0] if gte_rule and gte_rule.values else None
    search_end_str = lte_rule.values[0] if lte_rule and lte_rule.values else None

    search_start = _parse_year_input(search_start_str, is_start_of_range=True)
    search_end = _parse_year_input(search_end_str, is_start_of_range=False)
    
    if gte_rule and search_start is not None and gte_rule.era == Era.BC:
        search_start = -search_start
    if lte_rule and search_end is not None and lte_rule.era == Era.BC:
        search_end = -search_end

    starts_before_search_ends = (search_end is None) or (item_start <= search_end)
    ends_after_search_starts = (search_start is None) or (item_end >= search_start)

    return starts_before_search_ends and ends_after_search_starts

def _passes_po_date_rules(item: Dict[str, Any], date_rules: List[FilterRule]) -> bool:
    """
    Checks if a physical object's date range is strictly contained within the search date range.
    """
    if not date_rules:
        return True

    item_start = item.get("start_year")
    item_end = item.get("end_year") or item_start

    # If the item has no date range, it cannot match a date rule.
    if item_start is None:
        return False

    gte_rule = next((r for r in date_rules if r.logic == Logic.GTE), None)
    lte_rule = next((r for r in date_rules if r.logic == Logic.LTE), None)

    search_start_str = gte_rule.values[0] if gte_rule and gte_rule.values else None
    search_end_str = lte_rule.values[0] if lte_rule and lte_rule.values else None

    search_start = _parse_year_input(search_start_str, is_start_of_range=True)
    search_end = _parse_year_input(search_end_str, is_start_of_range=False)

    # Strict containment check:
    # The item's start year must be greater than or equal to the search start year.
    starts_after_search_starts = (search_start is None) or (item_start >= search_start)
    
    # The item's end year must be less than or equal to the search end year.
    ends_before_search_ends = (search_end is None) or (item_end <= search_end)

    return starts_after_search_starts and ends_before_search_ends

def _roman_to_int(s: str) -> Optional[int]:
    """Converts a Roman numeral string to an integer."""
    s = s.upper()
    roman_map = {'I': 1, 'V': 5, 'X': 10, 'L': 50, 'C': 100, 'D': 500, 'M': 1000}
    if not all(c in roman_map for c in s):
        return None
    
    result = 0
    for i in range(len(s)):
        if i > 0 and roman_map[s[i]] > roman_map[s[i-1]]:
            result += roman_map[s[i]] - 2 * roman_map[s[i-1]]
        else:
            result += roman_map[s[i]]
    return result

def _parse_year_input(value_str: str, is_start_of_range: bool) -> Optional[int]:
    """
    Parses a string that could be a year, a Roman numeral, or a century.
    """
    if not value_str:
        return None
    
    value_str = value_str.strip().lower()

    try:
        return int(value_str)
    except ValueError:
        pass

    century_match = re.match(r'^(.*?)(?:st|nd|rd|th)?(?:\s*century)?$', value_str)
    if century_match:
        century_val_str = century_match.group(1).strip()
        century_num = None
        
        try:
            century_num = int(century_val_str)
        except ValueError:
            century_num = _roman_to_int(century_val_str)

        if century_num is not None and 0 < century_num < 40:
            if is_start_of_range:
                return (century_num - 1) * 100 + 1
            else:
                return century_num * 100
    
    roman_year = _roman_to_int(value_str)
    if roman_year is not None:
        return roman_year

    return None

def _passes_person_date_rules(item: Dict[str, Any], date_rules: List[FilterRule]) -> bool:
    """
    MODIFIED: Checks if a person's entire lifespan is contained within the search date range.
    """
    if not date_rules:
        return True

    birth_year = item.get("birth_year")
    death_year = item.get("death_year")

    if birth_year is None or death_year is None:
        return False

    from_rule = next((r for r in date_rules if r.field == FilterableField.PERSON_BIRTH_DATE and r.logic == Logic.GTE), None)
    to_rule = next((r for r in date_rules if r.field == FilterableField.PERSON_DEATH_DATE and r.logic == Logic.LTE), None)

    start_year_str = from_rule.values[0] if from_rule and from_rule.values else None
    end_year_str = to_rule.values[0] if to_rule and to_rule.values else None

    start_year = _parse_year_input(start_year_str, is_start_of_range=True)
    end_year = _parse_year_input(end_year_str, is_start_of_range=False)

    if from_rule and start_year is not None and from_rule.era == Era.BC:
        start_year = -start_year
    if to_rule and end_year is not None and to_rule.era == Era.BC:
        end_year = -end_year
    
    if start_year is not None and birth_year < start_year:
        return False

    if end_year is not None and death_year > end_year:
        return False

    return True

def _project_results(results: List[Dict[str, Any]], query: SearchQuery) -> List[Dict[str, Any]]:
    work_fields_to_remove = {'author_search_terms', 'author_search_terms_normalized', 'classifications_normalized', 'work_title_normalized'}
    expression_fields_to_remove = {
        'author_search_terms', 'author_search_terms_normalized', 'classifications_normalized', 
        'type_of_expression_normalized', 'language_normalized', 'role_of_person_or_institution_normalized', 
        'work_title_normalized', 'translators', 'expression_editors', 'scriptwriters', 'compositors', 
        'reviewers', 'other_secondary_roles', 'translators_normalized', 'expression_editors_normalized', 
        'scriptwriters_normalized', 'compositors_normalized', 'reviewers_normalized', 'other_secondary_roles_normalized',
        'all_people_and_institutions_normalized', 'secondary_authors'
    }
    manifestation_fields_to_remove = {
        'publication_start_year', 'publication_end_year', 'publication_place_normalized', 'publisher_normalized', 'editor_normalized',
        'corrector_normalized', 'sponsor_normalized', 'classifications_normalized',
        'type_of_expression_normalized', 'language_normalized', 'work_title_normalized',
        'all_people_and_institutions_normalized', 'author_search_terms_normalized', 'authors',
        # ---  Clean up inherited expression roles from output ---
        'translators', 'expression_editors', 'scriptwriters', 'compositors', 'reviewers', 'other_secondary_roles'
    }
    item_fields_to_remove = {
        'publication_start_year', 'publication_end_year', 'visual_object_owners', 'visual_object_inscribers', 'visual_object_senders',
        'visual_object_recipients', 'preservation_status_normalized', 'owner_normalized', 'material_normalized',
        'type_of_item_normalized', 'visual_object_owners_normalized', 'visual_object_inscribers_normalized',
        'visual_object_senders_normalized', 'visual_object_recipients_normalized', 'classifications_normalized',
        'type_of_expression_normalized', 'language_normalized', 'publication_place_normalized', 'work_title_normalized',
        'all_people_and_institutions_normalized', 'type_of_visual_object_normalized', 'type_of_physical_object_normalized',
        # Keep 'has_digital_representation' if needed for frontend logic, or remove if purely internal
    }
    person_fields_to_remove = {'person_name_normalized', 'roles_normalized', 'gender_normalized', 'birth_year', 'death_year', 'birth_era', 'death_era'}
    visual_object_fields_to_remove = {
        'transcription_original', 'transcription_lower', 'transcription_normalized',
        'transcription_normalized_cs', 'transcription_original_words', 'transcription_lower_words',
        'transcription_normalized_words', 'transcription_normalized_cs_words',
        'transcription_original_tokens', 'transcription_lower_tokens',
        'transcription_normalized_tokens', 'transcription_normalized_cs_tokens',
        # Remove normalized fields to clean up the response
        'type_of_visual_object_normalized', 'visual_object_function_normalized',
        'visual_object_language_normalized', 'visual_object_instrument_normalized',
        'visual_object_colour_normalized', 'visual_object_owners_normalized',
        'visual_object_inscribers_normalized', 'visual_object_senders_normalized',
        'visual_object_recipients_normalized', 'work_title_normalized',
        'author_search_terms_normalized', 'classifications_normalized',
        'type_of_expression_normalized', 'language_normalized',
        'publication_place_normalized',
        # --- NEW: Remove new internal fields from response ---
        'item_owner_normalized', 'all_people_and_institutions_normalized',
        'all_people_and_institutions'
    }
    physical_object_fields_to_remove = {
        'type_of_physical_object_normalized', 'creators_normalized', 'owners_normalized', 
        'all_people_and_institutions_normalized', 'physical_object_place_normalized',
        'start_year', 'end_year', 'author_search_terms_normalized', 'classifications_normalized',
        'type_of_expression_normalized', 'language_normalized', 'publication_place_normalized',
        'owner_normalized', 'work_title_normalized', 'visual_object_owners_normalized',
        'visual_object_inscribers_normalized', 'visual_object_senders_normalized',
        'visual_object_recipients_normalized'
    }
    institution_fields_to_remove = {'institution_name_normalized', 'institution_place_normalized', 'roles_normalized'} # --- MODIFICATION 2 START ---
    event_fields_to_remove = {'event_name_normalized', 'start_year', 'end_year'}
    page_fields_to_remove = set()
    ac_fields_to_remove = {'ac_name_normalized', 'mentioned_by_entity_types'}
    
    entity_field_removal_map = {
        EntityType.WORK: work_fields_to_remove, EntityType.EXPRESSION: expression_fields_to_remove,
        EntityType.MANIFESTATION: manifestation_fields_to_remove, EntityType.ITEM: item_fields_to_remove,
        EntityType.PAGE: page_fields_to_remove,
        EntityType.PERSON: person_fields_to_remove, EntityType.VISUAL_OBJECT: visual_object_fields_to_remove,
        EntityType.PHYSICAL_OBJECT: physical_object_fields_to_remove, EntityType.INSTITUTION: institution_fields_to_remove, 
        EntityType.EVENT: event_fields_to_remove,
        EntityType.ABSTRACT_CHARACTER: ac_fields_to_remove,
    }

    fields_to_remove = entity_field_removal_map.get(query.entity, set())
    clean_results = [{k: v for k, v in item.items() if k not in fields_to_remove} for item in results]

    if query.summary and not query.fields:
        return clean_results 
    elif query.fields:
        return clean_results
        
    return clean_results