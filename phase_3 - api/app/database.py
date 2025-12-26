# app/database.py
from typing import List, Dict, Any
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from app.config import MONGO_URI, MONGO_DB_NAME

class Database:
    client: AsyncIOMotorClient = None

db = Database()

async def connect_to_mongo():
    """Establishes the connection to the MongoDB database."""
    print("Connecting to MongoDB...")
    db.client = AsyncIOMotorClient(MONGO_URI)
    print("MongoDB connection established.")

async def close_mongo_connection():
    """Closes the connection to the MongoDB database."""
    print("Closing MongoDB connection...")
    db.client.close()
    print("MongoDB connection closed.")

def get_database() -> AsyncIOMotorDatabase:
    """Returns the database client instance."""
    if db.client is None:
        raise RuntimeError("Database is not connected. Call connect_to_mongo() first.")
    return db.client[MONGO_DB_NAME]

async def fetch_collection(db: AsyncIOMotorDatabase, collection_name: str) -> List[Dict[str, Any]]:
    """
    A generic function to fetch all active documents from a collection
    and convert relevant IDs to strings.
    """
    cursor = db[collection_name].find({"active": True})
    docs = await cursor.to_list(length=None)
    for doc in docs:
        doc["_id"] = str(doc["_id"])
        if 'creationUser' in doc and doc.get('creationUser'):
            doc['creationUser'] = str(doc.get('creationUser'))
        if 'associatedUsers' in doc and doc.get('associatedUsers'):
            doc['associatedUsers'] = [str(user_id) for user_id in doc.get('associatedUsers', [])]
    return docs