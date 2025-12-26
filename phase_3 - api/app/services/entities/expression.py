# app/services/entities/expression.py
from typing import List, Dict, Any, Tuple
from app.services.common import get_contributor_details, extract_human_readable_id, normalize_language, calculate_richness

def process_expressions(raw_expressions: List[Dict[str, Any]], context: Dict[str, Any], works_data_map: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    unique_expressions_map = {}
    expression_details_cache = {}
    
    id2projects = context["id2projects"]
    id2label = context["id2label"]
    expression_to_work = context["expression_to_work"]
    expression_to_type = context["expression_to_type"]
    expression_to_number = context["expression_to_number"]
    work_to_uniform_title = context["work_to_uniform_title"]
    
    # Role maps
    expression_to_translators = context["expression_to_translators"]
    expression_to_editors_exp = context["expression_to_editors_exp"]
    expression_to_scriptwriters = context["expression_to_scriptwriters"]
    expression_to_compositors = context["expression_to_compositors"]
    expression_to_reviewers = context["expression_to_reviewers"]
    expression_to_other_roles = context["expression_to_other_roles"]
    
    expression_to_language = context["expression_to_language"]
    expression_to_responsibility = context["expression_to_responsibility"]
    expression_to_medium = context["expression_to_medium"]
    entity_to_hypotheses_details = context["entity_to_hypotheses_details"]

    # --- Translation map is removed, as data is pre-translated ---

    for exp in raw_expressions:
        expression_id = exp["_id"]
        
        parent_work_id = expression_to_work.get(expression_id)
        
        # --- FILTER: Parent Work must have work_has_uniform_title ---
        if not parent_work_id or parent_work_id not in work_to_uniform_title:
            continue

        # --- Types are now pre-translated from the context map ---
        types_of_expression = sorted(list(set(expression_to_type.get(expression_id, []))))

        # Build Composite Title
        work_title = work_to_uniform_title[parent_work_id]
        type_str = ", ".join(types_of_expression)
        number = expression_to_number.get(expression_id)
        parts = [p for p in [work_title, type_str, number] if p]
        composite_title = " - ".join(parts) if parts else id2label.get(expression_id, "Unknown Expression")

        parent_work_details = works_data_map.get(parent_work_id)
        
        # Default values
        work_authors = []
        work_classifications = []
        work_classifications_normalized = set()
        work_author_search_terms = []
        work_author_search_terms_normalized = set()
        work_title_normalized = set()
        projects = id2projects.get(expression_id, ["Unknown Project"])

        if parent_work_details:
            work_authors = parent_work_details["authors"]
            work_classifications = parent_work_details.get("classifications", [])
            work_classifications_normalized = parent_work_details.get("classifications_normalized", set())
            work_author_search_terms = parent_work_details.get("author_search_terms", [])
            work_author_search_terms_normalized = parent_work_details.get("author_search_terms_normalized", set())
            work_title_normalized = parent_work_details.get("work_title_normalized", set())
            if not id2projects.get(expression_id):
                projects = parent_work_details.get("projects", ["Unknown Project"])

        # Contributors
        translators = sorted([d for d in [get_contributor_details(e_id, context) for e_id in expression_to_translators.get(expression_id, [])] if d], key=lambda x: x['name'])
        editors_exp = sorted([d for d in [get_contributor_details(e_id, context) for e_id in expression_to_editors_exp.get(expression_id, [])] if d], key=lambda x: x['name'])
        scriptwriters = sorted([d for d in [get_contributor_details(e_id, context) for e_id in expression_to_scriptwriters.get(expression_id, [])] if d], key=lambda x: x['name'])
        compositors = sorted([d for d in [get_contributor_details(e_id, context) for e_id in expression_to_compositors.get(expression_id, [])] if d], key=lambda x: x['name'])
        reviewers = sorted([d for d in [get_contributor_details(e_id, context) for e_id in expression_to_reviewers.get(expression_id, [])] if d], key=lambda x: x['name'])
        other_roles = sorted([d for d in [get_contributor_details(e_id, context) for e_id in expression_to_other_roles.get(expression_id, [])] if d], key=lambda x: x['name'])
        
        all_contributors = translators + editors_exp + scriptwriters + compositors + reviewers + other_roles
        all_people_and_institutions_normalized = {p['name'].lower() for p in all_contributors}
        all_people_and_institutions_normalized.update({author['name'].lower() for author in work_authors})

        secondary_authors = sorted(all_contributors, key=lambda x: x['name'])

        # --- Languages are now pre-translated from the context map ---
        # The normalize_language function is still useful if any codes slip through.
        raw_languages = sorted(list(set(expression_to_language.get(expression_id, []))))
        languages = [normalize_language(l) for l in raw_languages]
        
        responsibilities = sorted(list(set(expression_to_responsibility.get(expression_id, []))))
        
        card = {
            "title": composite_title, 
            "primary_authors": work_authors, 
            "secondary_authors": secondary_authors,
            "responsibility": ", ".join(responsibilities), 
            "language": ", ".join(languages), 
            "projects": projects
        }

        expression_data = {
            "expression_id": expression_id, 
            "human_readable_id": extract_human_readable_id(exp, "ex_"),
            "expression_title": composite_title, 
            "work_id": parent_work_id, 
            "work_title": work_title, 
            "projects": projects, 
            "authors": work_authors, 
            "type_of_expression": types_of_expression, 
            "language": languages, 
            "medium": sorted(list(set(expression_to_medium.get(expression_id, [])))), 
            "role_of_person_or_institution": responsibilities, 
            "hypotheses": entity_to_hypotheses_details.get(expression_id, []),
            "classifications": work_classifications, 
            "author_search_terms": work_author_search_terms,
            "translators": translators,
            "expression_editors": editors_exp,
            "scriptwriters": scriptwriters,
            "compositors": compositors,
            "reviewers": reviewers,
            "other_secondary_roles": other_roles,
            "secondary_authors": secondary_authors,
            "translators_normalized": {p['name'].lower() for p in translators},
            "expression_editors_normalized": {p['name'].lower() for p in editors_exp},
            "scriptwriters_normalized": {p['name'].lower() for p in scriptwriters},
            "compositors_normalized": {p['name'].lower() for p in compositors},
            "reviewers_normalized": {p['name'].lower() for p in reviewers},
            "other_secondary_roles_normalized": {p['name'].lower() for p in other_roles},
            "all_people_and_institutions_normalized": all_people_and_institutions_normalized,
            "type_of_expression_normalized": {t.lower() for t in types_of_expression}, 
            "language_normalized": {l.lower() for l in languages}, 
            "role_of_person_or_institution_normalized": {r.lower() for r in responsibilities}, 
            "classifications_normalized": work_classifications_normalized, 
            "author_search_terms_normalized": work_author_search_terms_normalized, 
            "work_title_normalized": work_title_normalized, 
            "card": card
        }

        # --- DEDUPLICATION LOGIC ---
        # Key is now (Title, HumanReadableID)
        unique_key = (composite_title, expression_data["human_readable_id"])

        if unique_key in unique_expressions_map:
            existing = unique_expressions_map[unique_key]
            if calculate_richness(expression_data) > calculate_richness(existing):
                unique_expressions_map[unique_key] = expression_data
                expression_details_cache[expression_id] = {
                    "expression_title": composite_title, 
                    "relationships": [], 
                    "projects": projects
                }
        else:
            unique_expressions_map[unique_key] = expression_data
            expression_details_cache[expression_id] = {
                "expression_title": composite_title, 
                "relationships": [], 
                "projects": projects
            }

    return list(unique_expressions_map.values()), expression_details_cache