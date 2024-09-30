from dotenv import load_dotenv, find_dotenv
import os
import pprint
from pymongo import MongoClient
import certifi
load_dotenv(find_dotenv())

password = os.environ.get("MONGODB_PWD")

connection_string = f"mongodb+srv://onur:{password}@newsportal.vs1n6.mongodb.net/?retryWrites=true&w=majority&appName=NewsPortal"

client = MongoClient(connection_string,tlsCAFile=certifi.where())

dbs = client.list_database_names()
print(dbs)

tutorial_db = client.tutorial
collections = tutorial_db.list_collection_names()
print(collections)

def insert_test_doc():
    collection = tutorial_db.tests
    test_document = {
        "name": "Onur",
        "type": "Test",
    }
    result = collection.insert_one(test_document)
    print(result.inserted_id)


production_db = client.production
person_collection = production_db.persons

def create_docs():
    first_names = ["Onur", "John", "Jane", "Alice", "Bob"]
    last_names = ["Smith", "Doe", "Johnson", "Brown", "Davis"]
    ages = [25, 30, 35, 40, 45]
    docs = []
    for first_name, last_name, age in zip(first_names,last_names,ages):
        person = {
            "first_name": first_name,
            "last_name": last_name,
            "age": age
        }
        docs.append(person)
    person_collection.insert_many(docs)

printer = pprint.PrettyPrinter()

def find_all_people():
    people = person_collection.find()
    for person in people:
        printer.pprint(person)

def find_onur():
    onur = person_collection.find_one({"first_name": "Onur"})
    printer.pprint(onur)

def count_all_people():
    count = person_collection.count_documents(filter = {})
    print(f"Number of people: {count}")

def get_person_by_id(person_id):
    from bson.objectid import ObjectId
    if ObjectId.is_valid(person_id):
        _id = ObjectId(person_id)
        person = person_collection.find_one({"_id": _id })
        printer.pprint(person)
    else:
        print("Invalid ID")

def get_age_range(min_age, max_age):
    query = {
            "$and": [
                {"age": {"$gte": min_age}},
                {"age": {"$lte": max_age}}
            ]  
        }
    people = person_collection.find(query).sort("age")
    for person in people:
        printer.pprint(person)

def project_columns():
    query = {}
    columns = {
        "_id": 0,
        "first_name": 1,
        "last_name": 1
    }
    people = person_collection.find(query, columns)
    for person in people:
        printer.pprint(person)

def update_person_by_id(person_id):
    from bson.objectid import ObjectId
    if ObjectId.is_valid(person_id):
        _id = ObjectId(person_id)
        """ all_updates = {
                "$set": {"new_field": True},
                "$inc": {"age": 1},
                "$rename": {"first_name": "first", "last_name" : "last"}
            } """
        all_updates = {"$unset": {"new_field": ""}}
        person_collection.update_one({"_id": _id}, all_updates)
    else:
        print("Invalid ID")

def replace_one(person_id):
    from bson.objectid import ObjectId
    if ObjectId.is_valid(person_id):
        _id = ObjectId(person_id)
        new_person = {
            "first_name": "new first name",
            "last_name": "new last name",
            "age": 100
        }
        person_collection.replace_one({"_id": _id}, new_person)
    else:
        print("Invalid ID")

def delete_doc_by_id(person_id):
    from bson.objectid import ObjectId
    if ObjectId.is_valid(person_id):
        _id = ObjectId(person_id)
        person_collection.delete_one({"_id": _id})
    else:
        print("Invalid ID")
    

delete_doc_by_id("66fa7ddadcba2afc8dc901d5")