# START OF FILE __main__.py

import json
import pandas as pd
import asyncio
import unicodedata # <--- ADDED: Essential for handling accents correctly
from collections import defaultdict
from pathlib import Path
from alive_progress import alive_bar
from bson import ObjectId

from mango.db_handler import MongoDirect as mp
from mango.cli import args
from mango.spoon import process_field
from mango.config import  relations, column2type, properties, delimited_fields, prefix_map, database

BATCH_SIZE = 1000

def load_json_cache(filename, default_factory):
    if filename.exists():
        with open(filename, "r") as infile:
            try:
                loaded_data = json.load(infile)
                cache = defaultdict(default_factory)
                
                is_nested_defaultdict = isinstance(default_factory(), defaultdict)

                for key, value in loaded_data.items():
                    if is_nested_defaultdict and isinstance(value, dict):
                        inner_dd = default_factory()
                        inner_dd.update(value)
                        cache[key] = inner_dd
                    else:
                        cache[key] = value
                
                print(f"Successfully loaded cache from {filename.name}")
                return cache
            except json.JSONDecodeError:
                print(f"Warning: {filename.name} is empty or corrupted. Starting with a new cache.")
    return defaultdict(default_factory)

def normalize_value(v):
    """
    Standardizes values to strings, handles floats/ints, removes extra whitespace,
    and crucially applies Unicode Normalization (NFC) to fix accent matching issues.
    """
    if pd.isnull(v) or v == "":
        return None

    val_str = ""
    
    if isinstance(v, (int, float)):
        if v == int(v):
            val_str = str(int(v))
        else:
            val_str = str(v)
    else:
        # Handle strings and potential numpy types
        try:
            clean_v = str(v).replace('\n', ' ').replace('\r', ' ').replace('\t', ' ').strip()
            # Try converting to float to catch numbers stored as text
            f = float(clean_v)
            val_str = str(int(f)) if f.is_integer() else str(f)
        except (ValueError, TypeError):
            val_str = str(v).replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')

    # Final strip and Unicode Normalization (NFC)
    # This ensures 'moïse' (composed) matches 'moïse' (decomposed)
    return unicodedata.normalize('NFC', val_str).strip()


def get_dynamic_entity_type(column_name, value, default_mapping):
    str_value = str(value)
    
    # Define the set of columns that are dynamically typed based on value prefixes.
    dynamically_typed_columns = {
        "PERSON_CHARACTER_ID_A", 
        "PERSON_CHARACTER_ID_B", 
        "HYPOTHESIS_ABOUT_ID(S)", 
        "MENTIONING_ID", 
        "MENTIONED_ID", 
        "AUTHOR_WORK_ID",
        "OTHER_SECONDARY_ROLE_ID",
        "TRANSLATOR_ID",
        "EDITOR_ID",
        "SCRIPTWRITER_ID",
        "COMPOSITOR_ID",
        "REVIEWER_ID",
        "PUBLISHER_MANIFESTATION_ID",
        "EDITOR_MANIFESTATION_ID",
        "CORRECTOR_MANIFESTATION_ID",
        "SPONSOR_MANIFESTATION_ID",
        "OWNER_OF_ITEM_ID",
        "OWNERSHIP_OF_VISUAL_ID",
        "INSCRIBER_VISUAL_ID",
        "SENDER_VISUAL_ID",
        "RECIPIENT_VISUAL_ID",
        "OWNERSHIP_OF_PHYSICAL_OBJECT_ID",
        "CREATOR_OF_PHYSICAL_OBJECT_ID",
    }
    
    # Check if the column is in the dynamic set or has a specific suffix.
    if column_name.endswith(("_MENTIONING", "_MENTIONED_BY", "_HYPOTHESIS_OF")) or \
       column_name in dynamically_typed_columns:
        # Make the check case-insensitive to handle prefixes like "P_" or "p_"
        str_value_lower = str_value.lower()
        for prefix, entity_type in prefix_map.items():
            if str_value_lower.startswith(prefix.lower()):
                return entity_type
    return default_mapping.get(column_name)

def collect_mapped_entities(data):
    entities = {}
    print("Collecting unique entities from statically-mapped columns...")
    with alive_bar(len(column2type)) as bar:
        for column_name, entity_type in column2type.items():
            if column_name not in data.columns:
                bar()
                continue

            unique_records = data[[column_name] + list(properties.get(column_name, {}))].drop_duplicates()

            for record in unique_records.to_dict('records'):
                item = record.pop(column_name)
                if pd.isnull(item):
                    continue
                
                delimiter = delimited_fields.get(column_name)
                processed_items = process_field(item, delimiter=delimiter)

                for single_item in processed_items:
                    single_item = normalize_value(single_item)
                    if not single_item:
                        continue
                    
                    entity_key = (entity_type, single_item)
                    if entity_key not in entities:
                        params = {
                            f_name: normalize_value(value)
                            for p_name, f_name in properties.get(column_name, {}).items()
                            if (value := record.get(p_name)) and pd.notnull(value)
                        }
                        entities[entity_key] = {"type": entity_type, "name": single_item, "params": params}
            bar()
    return entities

def collect_dynamic_relation_entities(data):
    entities = {}
    print("Discovering and collecting entities from dynamic relation columns...")
    
    relation_cols = {r['entity1'] for r in relations} | {r['entity2'] for r in relations}
    mapped_cols = set(column2type.keys())
    dynamic_cols = [col for col in relation_cols if col not in mapped_cols and col in data.columns]

    if not dynamic_cols:
        print("No dynamic relation columns found to process.")
        return {}

    with alive_bar(len(data), title="Scanning dynamic columns") as bar:
        for _, row in data.iterrows():
            for col_name in dynamic_cols:
                if pd.notnull(row.get(col_name)):
                    items = process_field(row[col_name], delimiter=delimited_fields.get(col_name))
                    for item in items:
                        item = normalize_value(item)
                        if not item: continue
                        
                        entity_type = get_dynamic_entity_type(col_name, item, column2type)
                        if entity_type:
                            entity_key = (entity_type, item)
                            if entity_key not in entities:
                                entities[entity_key] = {"type": entity_type, "name": item, "params": {}}
            bar()
            
    return entities

async def main():
    folder = Path.cwd() / "output"
    folder.mkdir(exist_ok=True)
    entities_filename = folder / "entities.json"
    relations_filename = folder / "relations.json"

    logged_entities = load_json_cache(entities_filename, dict)
    logs = load_json_cache(relations_filename, lambda: defaultdict(list))

    xls = pd.ExcelFile(args.path)
    data = pd.concat([xls.parse(sheet_name) for sheet_name in xls.sheet_names], ignore_index=True)

    mg = mp(database["connection_string"], database["database_name"], args.user)
    await mg.authenticate() 

    print("Populating server-side caches before starting...")
    await asyncio.gather(
        mg.get_active_entities(),
        mg.get_relationTypes(),
        mg.get_relations()
    )
    print("Caches populated. Starting main processing.")

    mapped_entities = collect_mapped_entities(data)
    dynamic_entities = collect_dynamic_relation_entities(data)

    all_entities = {**dynamic_entities, **mapped_entities}
    
    entities_to_process = [entity_data for entity_data in all_entities.values()]
    
    print(f"\nCollected {len(entities_to_process)} unique entities from the source file.")
    print("The script will now check each entity against the database and skip any duplicates.")

    if entities_to_process:
        with alive_bar(len(entities_to_process), title="Processing entities...") as bar:
            for i in range(0, len(entities_to_process), BATCH_SIZE):
                batch = entities_to_process[i:i + BATCH_SIZE]
                tasks = [mg.merge_entity(e["type"], e["name"], params=e["params"]) for e in batch]
                results = await asyncio.gather(*tasks)
                
                for entity_data, entity_id in zip(batch, results):
                    if entity_id:
                        entity_type = entity_data["type"]
                        name = entity_data["name"]
                        str_entity_id = str(entity_id)
                        if entity_type not in logged_entities:
                            logged_entities[entity_type] = {}
                        logged_entities[entity_type][name] = str_entity_id
                    bar()

    with open(entities_filename, "w") as outfile:
        json.dump(dict(logged_entities), outfile, indent=4)
    print("Entity processing complete. Cache saved.")

    relations_to_create = []
    
    print("Collecting statically-defined relations from source file...")
    for _, row in data.iterrows():
        for r in relations:
            relation_name, col1, col2 = r["name"], r["entity1"], r["entity2"]
            if pd.notnull(row.get(col1)) and pd.notnull(row.get(col2)):
                entities1 = process_field(row[col1], delimiter=delimited_fields.get(col1))
                entities2 = process_field(row[col2], delimiter=delimited_fields.get(col2))

                entities1 = [normalize_value(e) for e in entities1]
                entities2 = [normalize_value(e) for e in entities2]

                for e1 in entities1:
                    for e2 in entities2:
                        if not e1 or not e2: continue

                        e1_display_type = get_dynamic_entity_type(col1, e1, column2type)
                        e2_display_type = get_dynamic_entity_type(col2, e2, column2type)

                        if not e1_display_type or not e2_display_type:
                            continue
                        
                        # This lookup was failing because of accent mismatch. 
                        # Now that normalize_value uses NFC, this should work.
                        entity1_id_str = logged_entities.get(e1_display_type, {}).get(e1)
                        entity2_id_str = logged_entities.get(e2_display_type, {}).get(e2)

                        if entity1_id_str and entity2_id_str:
                            entity1_id = ObjectId(entity1_id_str)
                            entity2_id = ObjectId(entity2_id_str)
                            rel_tuple = (
                                relation_name,
                                mg.active_entities[e1_display_type],
                                mg.active_entities[e2_display_type],
                                entity1_id,
                                entity2_id
                            )
                            relations_to_create.append(rel_tuple)

    print("Collecting dynamically-named relationships from source file...")
    dynamic_rel_cols = ["PERSON_CHARACTER_ID_A", "RELATIONSHIP", "PERSON_CHARACTER_ID_B"]
    if all(c in data.columns for c in dynamic_rel_cols):
        for _, row in data.iterrows():
            if all(pd.notnull(row.get(c)) for c in dynamic_rel_cols):
                
                relation_name = normalize_value(row["RELATIONSHIP"])
                col1, col2 = "PERSON_CHARACTER_ID_A", "PERSON_CHARACTER_ID_B"

                entities1 = process_field(row[col1], delimiter=delimited_fields.get(col1))
                entities2 = process_field(row[col2], delimiter=delimited_fields.get(col2))

                entities1 = [normalize_value(e) for e in entities1]
                entities2 = [normalize_value(e) for e in entities2]

                for e1 in entities1:
                    for e2 in entities2:
                        if not e1 or not e2: continue

                        e1_display_type = get_dynamic_entity_type(col1, e1, column2type)
                        e2_display_type = get_dynamic_entity_type(col2, e2, column2type)

                        if not e1_display_type or not e2_display_type:
                            print(f"Warning: Could not determine type for dynamic relation between '{e1}' and '{e2}'. Skipping.")
                            continue
                        
                        entity1_id_str = logged_entities.get(e1_display_type, {}).get(e1)
                        entity2_id_str = logged_entities.get(e2_display_type, {}).get(e2)

                        if entity1_id_str and entity2_id_str:
                            entity1_id = ObjectId(entity1_id_str)
                            entity2_id = ObjectId(entity2_id_str)
                            rel_tuple = (
                                relation_name,
                                mg.active_entities[e1_display_type],
                                mg.active_entities[e2_display_type],
                                entity1_id,
                                entity2_id
                            )
                            relations_to_create.append(rel_tuple)
    else:
        print("Dynamic relationship columns (e.g., PERSON_CHARACTER_ID_A) not found. Skipping.")

    unique_relations = sorted(list(set(relations_to_create)), key=lambda x: (x[0], str(x[3]), str(x[4])))
    print(f"Found {len(unique_relations)} unique relations to process.")

    if unique_relations:
        with alive_bar(len(unique_relations), title="Processing relations...") as bar:
            for i in range(0, len(unique_relations), BATCH_SIZE):
                batch = unique_relations[i:i + BATCH_SIZE]
                tasks = [mg.merge_relation(*r) for r in batch]
                results = await asyncio.gather(*tasks)

                for relation_data, relation_id in zip(batch, results):
                    if relation_id:
                        relation_name = relation_data[0]
                        src_type = relation_data[1]
                        str_relation_id = str(relation_id)
                        if str_relation_id not in logs[relation_name][src_type]:
                            logs[relation_name][src_type].append(str_relation_id)
                    bar()

    with open(relations_filename, "w") as outfile:
        json.dump(logs, outfile, indent=4)
    print("Relation processing complete. Cache saved.")


if __name__ == "__main__":
    asyncio.run(main())