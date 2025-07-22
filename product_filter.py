from utils import mongodb_connect
import requests, csv, os
import json

# This program filters and fetches the desired products from collection 'summary'
connection_string = "mongodb://localhost:27017"
failed_products_file = "failed_products.json"

def filter_data(collection, field, values):
    pipeline = [
    {"$match": {field : {"$in": values }}},
    {"$project": {"product_id": 1, "current_url": 1}},
    {"$group": {"_id": "$product_id", "url": {"$addToSet": "$current_url"}}}
    ]
    results = list(collection.aggregate(pipeline))
    return results


def filter_products():
    with open(failed_products_file, "r", encoding="utf-8") as f:
        data = json.load(f)
        list_pid = [item["product_id"] for item in data]

    client, db, collection = mongodb_connect(connection_string)
    # Filter documents on selected "collection"
    doc = filter_data(collection, "product_id", list_pid)
    print(doc[0])
    path = "fixed_products.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(doc, f, ensure_ascii=False, indent=2)

filter_products()

