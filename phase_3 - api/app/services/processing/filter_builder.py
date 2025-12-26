from typing import Dict, Any, List
from collections import defaultdict
from app.services.common import normalize_language

def generate_frontend_filters(
    data_maps: Dict[str, Dict[str, Any]],
    id2projects: Dict[str, List[str]],
    context: Dict[str, Any],
    relation_to_role_map: Dict[str, str]
) -> Dict[str, Any]:
    """
    Builds project-specific filter options (Step 9 logic).
    """
    filter_options_by_project = {"__ALL__": defaultdict(set)}

    def add_to_filters(projects, key, values):
        """
        Helper function to add values to the filter sets for the correct projects.
        Now standardizes capitalization for all string values EXCEPT for project names.
        """
        if not projects: projects = ["Unknown Project"]
        value_set = set()
        
        # Process list or set of values
        if isinstance(values, (list, set)):
            for v in values:
                if v:
                    if isinstance(v, str):
                        # Filter out internal IDs
                        if v.startswith("p_") or v.startswith("inst_"):
                            continue
                        
                        # --- Apply title() case only to non-project filters ---
                        if key == 'projects':
                            value_set.add(v)  # Use project name as-is
                        else:
                            value_set.add(v.title()) # Standardize other filters
                    else:
                        # Add non-string values as-is
                        value_set.add(v)
        # Process single value
        elif values:
            if isinstance(values, str):
                # Filter out internal IDs
                if not (values.startswith("p_") or values.startswith("inst_")):
                    # --- FIX: Apply title() case only to non-project filters ---
                    if key == 'projects':
                        value_set.add(values) # Use project name as-is
                    else:
                        value_set.add(values.title()) # Standardize other filters
            else:
                # Add non-string values as-is
                value_set.add(values)

        if not value_set: return
        
        # Add the values to the project-specific and global filters
        for project in projects:
            # Use project names directly from the database without changing capitalization
            standardized_project = project 
            if standardized_project not in filter_options_by_project:
                filter_options_by_project[standardized_project] = defaultdict(set)
            filter_options_by_project[standardized_project][key].update(value_set)
        filter_options_by_project["__ALL__"][key].update(value_set)


    all_project_names = set()
    for project_list in id2projects.values(): all_project_names.update(project_list)
    # ---  Ensure project names are used as-is from the database ---
    add_to_filters(["__ALL__"], 'projects', list(all_project_names))

    # Populate filters from entities
    for work in data_maps["works_list"]:
        p = work['projects']
        standardized_projects = p
        add_to_filters(standardized_projects, 'classifications', work['classifications'])
        add_to_filters(standardized_projects, 'authors', work['author_search_terms'])
        add_to_filters(standardized_projects, 'work_titles', work['work_title'])

    for exp in data_maps["expressions_list"]:
        p = exp['projects']
        standardized_projects = p
        add_to_filters(standardized_projects, 'types_of_expression', exp['type_of_expression'])
        
        langs = [normalize_language(l) for l in exp['language']]
        add_to_filters(standardized_projects, 'languages', langs)
        
        add_to_filters(standardized_projects, 'roles_of_person_or_institution', exp['role_of_person_or_institution'])
        all_exp_people = {person['name'] for person in exp.get('authors', [])}
        all_exp_people.update({person['name'] for person in exp.get('secondary_authors', [])})
        add_to_filters(standardized_projects, 'all_people_and_institutions', all_exp_people)

    for man in data_maps["manifestations_list"]:
        p = man['projects']
        standardized_projects = p
        if man.get('publication_place'):
            place_name = man['publication_place']['place_name']
            if place_name.lower().startswith(('http://', 'https://', 'www.')): add_to_filters(standardized_projects, 'places', 'Web')
            else: add_to_filters(standardized_projects, 'places', place_name)
        all_man_people = {person['name'] for person in man.get('publisher', [])}
        all_man_people.update({person['name'] for person in man.get('editor', [])})
        all_man_people.update({person['name'] for person in man.get('corrector', [])})
        all_man_people.update({person['name'] for person in man.get('sponsor', [])})
        add_to_filters(standardized_projects, 'all_people_and_institutions', all_man_people)

    for item in data_maps["items_list"]:
        p = item['projects']
        standardized_projects = p
        add_to_filters(standardized_projects, 'preservation_statuses', item['preservation_status'])
        add_to_filters(standardized_projects, 'owners', item['owner'])
        add_to_filters(standardized_projects, 'materials', item['material'])
        add_to_filters(standardized_projects, 'types_of_item', item['type_of_item'])
        add_to_filters(standardized_projects, 'types_of_visual_object', item['type_of_visual_object_normalized'])
        add_to_filters(standardized_projects, 'types_of_physical_object', item['type_of_physical_object_normalized'])
        
        if item.get('has_digital_representation'):
            add_to_filters(standardized_projects, 'digitalization', 'cerca solo Item con scansioni online')
        
        all_item_people = set(item.get('owner', []))
        all_item_people.update(item.get('visual_object_owners', []))
        all_item_people.update(item.get('visual_object_inscribers', []))
        all_item_people.update(item.get('visual_object_senders', []))
        all_item_people.update(item.get('visual_object_recipients', []))
        add_to_filters(standardized_projects, 'all_people_and_institutions', all_item_people)

    # ---  Populate filters from Visual Objects ---
    for vo in data_maps["visual_objects_list"]:
        p = vo['projects']
        standardized_projects = p
        # New VO-specific filters
        add_to_filters(standardized_projects, 'types_of_visual_object', vo['type_of_visual_object'])
        add_to_filters(standardized_projects, 'visual_object_functions', vo['visual_object_function'])
        add_to_filters(standardized_projects, 'visual_object_languages', vo['visual_object_language'])
        add_to_filters(standardized_projects, 'visual_object_instruments', vo['visual_object_instrument'])
        add_to_filters(standardized_projects, 'visual_object_colours', vo['visual_object_colour'])
        
        # Inherited filters
        add_to_filters(standardized_projects, 'work_titles', vo.get('work_title'))
        add_to_filters(standardized_projects, 'authors', [author['name'] for author in vo.get('authors', [])])
        add_to_filters(standardized_projects, 'classifications', vo.get('classifications'))
        add_to_filters(standardized_projects, 'types_of_expression', vo.get('type_of_expression'))
        add_to_filters(standardized_projects, 'languages', vo.get('language')) # This is language of expression
        if vo.get('publication_place'):
            place_name = vo['publication_place']['place_name']
            if place_name.lower().startswith(('http://', 'https://', 'www.')): add_to_filters(standardized_projects, 'places', 'Web')
            else: add_to_filters(standardized_projects, 'places', place_name)
            
        # ---  1b: Populate the Person/Institution filter for VOs ---
        add_to_filters(standardized_projects, 'all_people_and_institutions', vo.get('all_people_and_institutions', []))
        add_to_filters(standardized_projects, 'owners', vo.get('item_owner', []))


    for person in data_maps["persons_list"]:
        p = person['projects']
        standardized_projects = p
        add_to_filters(standardized_projects, 'all_people', person['person_name'])
        add_to_filters(standardized_projects, 'all_people', context["person_to_aliases"].get(person['person_id'], []))
        add_to_filters(standardized_projects, 'all_people_and_institutions', person['person_name'])
        add_to_filters(standardized_projects, 'all_people_and_institutions', context["person_to_aliases"].get(person['person_id'], []))

    for inst in data_maps["institutions_list"]:
        p = inst['projects']
        standardized_projects = p
        add_to_filters(standardized_projects, 'institution_names', inst['institution_name'])
        if inst.get('place'): add_to_filters(standardized_projects, 'institution_places', inst['place']['place_name'])
        add_to_filters(standardized_projects, 'all_people_and_institutions', inst['institution_name'])

    for event in data_maps["events_list"]:
        p = event['projects']
        standardized_projects = p
        add_to_filters(standardized_projects, 'event_names', event['event_name'])
    
    for po in data_maps["physical_objects_list"]:
        p = po['projects']
        standardized_projects = p
        # PO-specific filters
        add_to_filters(standardized_projects, 'types_of_physical_object', po['type_of_physical_object'])
        if po.get('place'):
            add_to_filters(standardized_projects, 'physical_object_places', po['place']['place_name'])
        
        # Inherited filters
        add_to_filters(standardized_projects, 'owners', po.get('owner', []))
        add_to_filters(standardized_projects, 'work_titles', po.get('work_title'))
        add_to_filters(standardized_projects, 'authors', [author['name'] for author in po.get('authors', [])])
        add_to_filters(standardized_projects, 'classifications', po.get('classifications'))
        add_to_filters(standardized_projects, 'types_of_expression', po.get('type_of_expression'))
        add_to_filters(standardized_projects, 'languages', po.get('language'))
        if po.get('publication_place'):
            place_name = po['publication_place']['place_name']
            if place_name.lower().startswith(('http://', 'https://', 'www.')): add_to_filters(standardized_projects, 'places', 'Web')
            else: add_to_filters(standardized_projects, 'places', place_name)

        # Aggregated people/institutions
        all_po_people = {person['name'] for person in po.get('creators', [])}
        all_po_people.update({person['name'] for person in po.get('owners', [])})
        all_po_people.update(po.get('owner', [])) # from inherited item
        all_po_people.update(po.get('visual_object_owners', []))
        all_po_people.update(po.get('visual_object_inscribers', []))
        all_po_people.update(po.get('visual_object_senders', []))
        all_po_people.update(po.get('visual_object_recipients', []))
        add_to_filters(standardized_projects, 'all_people_and_institutions', all_po_people)

    # --- NEW: Populate filters from Abstract Characters ---
    for ac in data_maps["abstract_characters_list"]:
        p = ac['projects']
        standardized_projects = p
        # For "Name of character:" filter
        ac_aliases = context.get("ac_to_aliases", {}).get(ac['abstract_character_id'], [])
        all_names = [ac['ac_name']] + ac_aliases
        add_to_filters(standardized_projects, 'abstract_character_names', all_names)
    
    for page in data_maps["pages_list"]:
        p = page['projects']
        standardized_projects = p
        # No specific filters for pages yet, but iterating ensures project association if we add filters later

    static_roles = {
        "search_for_roles_in_expression": ["Traduttore", "Curatore", "Sceneggiatore", "Compositore", "Recensore", "Altro ruolo secondario"], 
        "search_for_roles_in_manifestation": ["Editore", "Curatore", "Correttore", "Finanziatore"],
        "visual_object_roles": ["Possessore precedente", "Annotatore", "Dedicatore", "Dedicatario"],
        "physical_object_roles": ["Possessore dell’unità materiale", "Creatore dell’unità materiale"],
        "person_roles": list(set(relation_to_role_map.values())),
 
        "institution_roles": [
            "Autore dell’opera", "Traduttore", "Curatore", "Sceneggiatore", "Compositore", "Recensore", 
            "Altro ruolo secondario", "Curatore", "Finanziatore", "Correttore", "Owner of item", 
            "Possessore precedente", "Annotatore", "Dedicatore", "Dedicatario", "Creatore dell’unità materiale", "Possessore dell’unità materiale", "Membro"
        ],
 
        # For "Where is it mentioned:" filter
        "abstract_character_mentioned_in": [
            "work", "expression", "manifestation", "manifestation_volume", "item", 
            "page", "visual_object", "person", "physical_object", "institution", "event"
        ]
    }
    for project_key in filter_options_by_project:
        for role_key, role_list in static_roles.items():
            filter_options_by_project[project_key][role_key].update(role_list)

    # Sort filters
    for project, options in filter_options_by_project.items():
        for key, value_set in options.items():
            if key == 'places' or key == 'physical_object_places':
                sorted_list = sorted(list(value_set - {'Web'}), key=str.lower)
                if 'Web' in value_set: sorted_list.insert(0, 'Web')
                filter_options_by_project[project][key] = sorted_list
            else:
                filter_options_by_project[project][key] = sorted(list(value_set), key=str.lower)
    
    return filter_options_by_project