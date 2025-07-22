from pymongo import MongoClient
import json

def mongodb_connect(connection_string="mongodb://localhost:27017", db="gramira",collection="summary"):
    try:
        client = MongoClient(connection_string)
        db = client[db]
        collection = db[collection]
        print("Connected to MongoDB:", db.list_collection_names())
        return client, db, collection
    except Exception as e:
        print(f"Error connecting to MongoDB: {e}")
        return None, None, None

def load_json(file):
    with open(file, "r", encoding="utf-8") as f:
        return json.load(f)

def save_json(file, data):
    with open(file, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
