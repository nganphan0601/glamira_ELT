from pymongo import MongoClient
from faker import Faker
import random


fake = Faker()
client = MongoClient("mongodb://localhost:27017/")
db = client["test_db"]
collection = db["people"]

# Clear existing data 
collection.delete_many({})

# Function to generate one fake person
def generate_fake_person():
    return {
        "name": fake.name(),
        "email": fake.email(),
        "address": fake.address(),
        "age": random.randint(18, 80),
        "signup_date": fake.date_time_this_decade(),
        "bio": fake.text(),
        "is_active": random.choice([True, False])
    }

# Generate and insert multiple fake documents
def insert_fake_data(n=1000):
    data = [generate_fake_person() for _ in range(n)]
    collection.insert_many(data)
    print(f"Inserted {n} fake documents into MongoDB.")


insert_fake_data(1000)
