#map ip to location

import IP2Location
import os
from utils import mongodb_connect

connection_string = "mongodb://localhost:27017"
BIN_db = "IP-COUNTRY-REGION-CITY.BIN"


def get_ips(collection):
    try:
        pipeline = [{"$group":{"_id":"$ip"}}]
        ips_cursor = collection.aggregate(pipeline, allowDiskUse=True)
        ips = [doc["_id"] for doc in ips_cursor]
        print(f"Found {len(ips)} unique IPs.")
        return ips
    except Exception as e:
        print(f"Error fetching unique IPs: {e}")
        return []

def get_ips_from_location(collection):
    try:
        pipeline = [{"$group": {"_id": "$IP"}}]
        cursor = collection.aggregate(pipeline, allowDiskUse=True)
        return set(doc["_id"] for doc in cursor)
    except Exception as e:
        print(f"Error: {e}")
        return set()


def ip2location(ip_list, location_db):
    db = IP2Location.IP2Location(location_db)
    mapped_ip = []
    for ip in ip_list:
        try:
            record = db.get_all(ip)
            mapped_ip.append({ "IP":ip, "Country:":record.country_long, 
                "Region":record.region, "City":record.city})
        except Exception as e:
            print(f"Failed to lookup IP {ip}: {e}")
            mapped_ip.append({
                    "IP": ip,
                    "Country": None,
                    "Region": None,
                    "City": None
                 })
    return mapped_ip

def insert_data(col, doc_list, batch_size=1000):
    try:
        for i in range(0, len(doc_list), batch_size):
            batch = doc_list[i:i+batch_size]
            col.insert_many(batch , ordered=False)
            print(f"Inserted batch {i // batch_size + 1} of {len(batch)} records")
    except Exception as e:
        print(f"Error inserting into MongoDB: {e}")


def process_ip_locations():
# 1. Connect to MongoDB
    client, db, collection = mongodb_connect(connection_string)
    geo_collection = db["location"]

# 2. Read unique IPs from main collection
    ip_list = get_ips(collection)

# 3. Use ip2location to get location data
    #ip_location = ip2location(ip_list, BIN_db)
    #print(ip_location[0])
    inserted = set(get_ips_from_location(geo_collection))
    remaining = [ip for ip in ip_list if ip not in inserted]
    print(f"{len(remaining)} IPs remaining to be inserted.")

# 4. Store results in new collection Or xu
   # geo_collection.create_index("IP", unique=True)
    BATCH_SIZE = 1000
    for i in range(0, len(remaining), BATCH_SIZE):
        batch_ips = remaining[i:i+BATCH_SIZE]
        ip_location = ip2location(batch_ips, BIN_db)
        insert_data(geo_collection, ip_location)
        print(f"Inserted batch {i//BATCH_SIZE + 1}")
# Close the connection
    client.close()
    print("MongoDB connection closed.") 

process_ip_locations()

