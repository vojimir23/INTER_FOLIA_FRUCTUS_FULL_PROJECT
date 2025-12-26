from typing import Dict, Any, List, Tuple

def prepare_graph_data(
    data_maps: Dict[str, Dict[str, Any]],
    rel_all: List[Dict[str, Any]],
    id2label: Dict[str, str],
    id2rtype_name: Dict[str, str],
    context: Dict[str, Any]
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Prepares graph data (nodes and edges) (Step 8 logic).
    """
    all_relations_for_graph = [{"name": id2rtype_name.get(r["relationType"], "unknown_relation"), "source_id": r["entity1"], "target_id": r["entity2"]} for r in rel_all]
    id_to_details_for_graph = {}
    
    # Helper to populate graph details
    def populate_graph_details(data_list, type_name, id_key, title_key):
        for item in data_list:
            item_id = item[id_key]
            title = item.get(title_key)
            if type_name == 'item': title = item['card']['title']
            id_to_details_for_graph[item_id] = {"id": item_id, "type": type_name, "label": id2label.get(item_id, ""), "title": title, "projects": item.get("projects", ["Unknown Project"])}

    populate_graph_details(data_maps["works_list"], "work", "work_id", "work_title")
    populate_graph_details(data_maps["expressions_list"], "expression", "expression_id", "expression_title")
    populate_graph_details(data_maps["manifestations_list"], "manifestation", "manifestation_id", "manifestation_title")
    populate_graph_details(data_maps["manifestation_volumes_list"], "manifestation_volume", "manifestation_volume_id", "manifestation_volume_title")
    populate_graph_details(data_maps["items_list"], "item", "item_id", "item_label")
    populate_graph_details(data_maps["pages_list"], "page", "page_id", "page_title")
    populate_graph_details(data_maps["persons_list"], "person", "person_id", "person_name")
    populate_graph_details(data_maps["institutions_list"], "institution", "institution_id", "institution_name")
    populate_graph_details(data_maps["physical_objects_list"], "physical_object", "physical_object_id", "physical_object_name")
    populate_graph_details(data_maps["visual_objects_list"], "visual_object", "visual_object_id", "visual_object_name")
    populate_graph_details(data_maps["events_list"], "event", "event_id", "event_name")
    populate_graph_details(data_maps["abstract_characters_list"], "abstract_character", "abstract_character_id", "ac_name")

    return all_relations_for_graph, id_to_details_for_graph