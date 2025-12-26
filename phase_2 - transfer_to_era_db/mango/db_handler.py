# START OF FILE db_handler.py

import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from bson import ObjectId
from datetime import datetime, timezone
from collections import defaultdict # Import defaultdict

class MongoDirect:
    """
    Handles direct interaction with the MongoDB database.
    This version includes a lock to prevent race conditions when creating relation types.
    """
    def __init__(self, connection_string, database_name, username):
        self.client = AsyncIOMotorClient(connection_string)
        self.db = self.client[database_name]
        self.username = username
        self.user_id = None
        
        # FIX: A dictionary of locks to prevent race conditions.
        # Each key will be a unique relation type, and the value will be a lock.
        self.relation_type_locks = defaultdict(asyncio.Lock)
        
        self.collection_map = {
            "persons": self.db.persons,
            "places": self.db.places,
            "institutions": self.db.institutions,
            "event": self.db.events,
            "appellations": self.db.appellations,
            "attachments": self.db.attachments,
            "groups": self.db.groups,
            "sources": self.db.sources,
            "visual_object":self.db.visual_objects,
         "work": self.db.works,
         "expression": self.db.expressions,
         "manifestation": self.db.manifestations,
         "item": self.db.items,
         "page": self.db.pages,
         "physical_object": self.db.physical_objects,
         "abstract_character": self.db.abstract_characters,
         "hypothesis": self.db.hypotheses,
         "relationship": self.db.relationships



        }

        self.default_collection = self.db.entities

        self.predefined = {
            "group": "name",
            "appellation": "name",
            "source": "title",
            "attachment": "label",
            "event": "title"
        }

    async def _log_audit(self, reference_id, path, value, op_type="post"):
        if not self.user_id:
            print("Warning: Cannot create audit log without a user context.")
            return

        audit_doc = {
            "referenceId": reference_id,
            "user": self.user_id,
            "timestamp": datetime.now(timezone.utc),
            "path": path,
            "value": value,
            "type": op_type,
            "__v": 0
        }
        try:
            await self.db.audits.insert_one(audit_doc)
        except Exception as e:
            print(f"Warning: Failed to create audit log for {reference_id}. Error: {e}")

    async def _set_user_context(self):
        user_doc = await self.db.users.find_one({"username": self.username})
        if not user_doc:
            raise ValueError(f"User '{self.username}' not found in the 'users' collection. Please provide a valid username.")
        self.user_id = user_doc['_id']
        print(f"Operating on behalf of user: {self.username} (ID: {self.user_id})")

    async def authenticate(self, user=None, password=None):
        await self.db.command('ping')
        await self._set_user_context()
        print("Successfully connected to MongoDB and set user context.")

    async def get_active_entities(self):
        cursor = self.db.types.find({"active": True})
        self.active_entities = {
            e["displayName"].lower(): e["name"]
            async for e in cursor
        }

    async def get_relationTypes(self):
        cursor = self.db.relationtypes.find({})
        self.relation_types = {
            (r["name"], str(r["type"]), str(r["relationType"])): r["_id"]
            async for r in cursor if r.get("type") and r.get("relationType")
        }

    async def get_relations(self):
        cursor = self.db.relations.find({})
        self.relations = {
            str(rel["entity1"]) + str(rel["entity2"]) + str(rel["relationType"]): rel["_id"]
            async for rel in cursor if all(k in rel for k in ["entity1", "entity2", "relationType"])
        }

    async def get_entity_id(self, entity_type, params_dict):
        collection = self.collection_map.get(entity_type, self.default_collection)
        doc = await collection.find_one(params_dict)
        return doc['_id'] if doc else None

    async def merge_entity(self, display_name, entity_name, params=None):
        if not hasattr(self, "active_entities"):
            await self.get_active_entities()

        params = params or {}
        display_name_lower = display_name.lower()
        entity_type = self.active_entities.get(display_name_lower)

        if not entity_type:
            return None

        if entity_type == 'persons':
            user_doc = await self.db.users.find_one({"username": entity_name})
            if user_doc:
                return user_doc['_id']

        collection = self.collection_map.get(entity_type, self.default_collection)
        field_name = self.predefined.get(display_name_lower, "description")
        
        query_params = params.copy()
        query_params[field_name] = entity_name

        existing_doc = await collection.find_one(query_params)
        
        if existing_doc:
            # Entity exists. Add the current user to the list of associated users if not already present.
            if self.user_id not in existing_doc.get("associatedUsers", []):
                await collection.update_one(
                    {"_id": existing_doc["_id"]},
                    {
                        "$addToSet": {"associatedUsers": self.user_id},
                        "$set": {"updateUser": self.user_id, "latestUpdateTimestamp": datetime.now(timezone.utc)}
                    }
                )
                await self._log_audit(existing_doc["_id"], "associatedUsers", str(self.user_id), op_type="update")
            return existing_doc["_id"]

        # If the entity does not exist, create it with the current user as the creator and associated user.
        now = datetime.now(timezone.utc)
        new_doc = {
            "active": True, "creationUser": self.user_id, "updateUser": self.user_id,
            "creationTimestamp": now, "latestUpdateTimestamp": now,
            "namespace": "interfolia", "__v": 0, 
            "associatedUsers": [self.user_id], # Initialize with the creator's ID
            **query_params
        }
        
        if entity_type == "persons":
            new_doc["measures"] = []
        elif entity_type in ["institutions", "events"] or collection == self.default_collection:
            new_doc["relations"] = []
        
        type_doc = await self.db.types.find_one({"name": entity_type})
        if type_doc:
            new_doc["type"] = type_doc["_id"]

        result = await collection.insert_one(new_doc)
        inserted_id = result.inserted_id
        await self._log_audit(inserted_id, field_name, entity_name, op_type="post")
        return inserted_id

    async def merge_relation(self, relation_name, src_type, trg_type, entity1_id, entity2_id):
        if not hasattr(self, "relation_types"):
            await self.get_relationTypes()

        src_type_doc = await self.db.types.find_one({"name": src_type})
        trg_type_doc = await self.db.types.find_one({"name": trg_type})

        if not src_type_doc or not trg_type_doc:
            return None
        
        src_type_id, trg_type_id = src_type_doc["_id"], trg_type_doc["_id"]

        rel_type_key = (relation_name, str(src_type_id), str(trg_type_id))
        
        # FIX: Use a lock to ensure only one task can create a new relation type at a time.
        async with self.relation_type_locks[rel_type_key]:
            relation_type_id = self.relation_types.get(rel_type_key)

            if not relation_type_id:
                # This block is now protected. Only one task can execute it for a given key.
                rel_type_doc = {
                    "active": True, "name": relation_name, "type": src_type_id,
                    "relationType": trg_type_id, "creationUser": self.user_id,
                    "updateUser": self.user_id, "namespace": "interfolia", "__v": 0
                }
                result = await self.db.relationtypes.insert_one(rel_type_doc)
                relation_type_id = result.inserted_id
                await self._log_audit(relation_type_id, "name", relation_name, op_type="post")
                # Update the shared cache so other waiting tasks can see the new ID.
                self.relation_types[rel_type_key] = relation_type_id

        if not hasattr(self, "relations"):
            await self.get_relations()
            
        rel_key = str(entity1_id) + str(entity2_id) + str(relation_type_id)
        if rel_key in self.relations:
            return self.relations[rel_key]

        relation_doc = {
            "active": True, "entity1": entity1_id, "relationType": relation_type_id,
            "entity2": entity2_id, "__v": 0
        }
        
        result = await self.db.relations.insert_one(relation_doc)
        relation_id = result.inserted_id
        await self._log_audit(relation_id, "active", True, op_type="post")
        # Update the shared cache for relations as well.
        self.relations[rel_key] = relation_id
        return relation_id