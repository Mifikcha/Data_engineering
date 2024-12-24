import pickle
import json
from pymongo import MongoClient

def load_data_from_pkl(file_path):
    with open(file_path, 'rb') as file:
        data = pickle.load(file)
    return data

def load_data_from_json(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)
    return data

def connect_to_mongo(db_name, collection_name):
    client = MongoClient('mongodb://localhost:27017/')
    db = client[db_name]
    collection = db[collection_name]
    return collection

def insert_data_into_mongo(collection, data):
    collection.insert_many(data)

def serialize_mongo_data(data):
    for record in data:
        if '_id' in record:
            record['_id'] = str(record['_id'])
    return data

def salary_stats(collection):
    pipeline = [
        {
            "$group": {
                "_id": None,
                "min_salary": {"$min": "$salary"},
                "avg_salary": {"$avg": "$salary"},
                "max_salary": {"$max": "$salary"}
            }
        }
    ]
    return list(collection.aggregate(pipeline))

def count_by_profession(collection):
    pipeline = [
        {
            "$group": {
                "_id": "$profession",
                "count": {"$sum": 1}
            }
        }
    ]
    return list(collection.aggregate(pipeline))

def salary_by_city(collection):
    pipeline = [
        {
            "$group": {
                "_id": "$city",
                "min_salary": {"$min": "$salary"},
                "avg_salary": {"$avg": "$salary"},
                "max_salary": {"$max": "$salary"}
            }
        }
    ]
    return list(collection.aggregate(pipeline))

def salary_by_profession(collection):
    pipeline = [
        {
            "$group": {
                "_id": "$profession",
                "min_salary": {"$min": "$salary"},
                "avg_salary": {"$avg": "$salary"},
                "max_salary": {"$max": "$salary"}
            }
        }
    ]
    return list(collection.aggregate(pipeline))

def age_stats_by_city(collection):
    pipeline = [
        {
            "$group": {
                "_id": "$city",
                "min_age": {"$min": "$age"},
                "avg_age": {"$avg": "$age"},
                "max_age": {"$max": "$age"}
            }
        }
    ]
    return list(collection.aggregate(pipeline))

def age_stats_by_profession(collection):
    pipeline = [
        {
            "$group": {
                "_id": "$profession",
                "min_age": {"$min": "$age"},
                "avg_age": {"$avg": "$age"},
                "max_age": {"$max": "$age"}
            }
        }
    ]
    return list(collection.aggregate(pipeline))

def max_salary_min_age(collection):
    pipeline = [
        {
            "$sort": {"age": 1, "salary": -1}
        },
        {"$limit": 1}
    ]
    return list(collection.aggregate(pipeline))

def min_salary_max_age(collection):
    pipeline = [
        {
            "$sort": {"age": -1, "salary": 1}
        },
        {"$limit": 1}
    ]
    return list(collection.aggregate(pipeline))

def age_stats_salary_above_50k(collection):
    pipeline = [
        {
            "$match": {"salary": {"$gt": 50000}}
        },
        {
            "$group": {
                "_id": "$city",
                "min_age": {"$min": "$age"},
                "avg_age": {"$avg": "$age"},
                "max_age": {"$max": "$age"}
            }
        },
        {
            "$sort": {"avg_age": -1}
        }
    ]
    return list(collection.aggregate(pipeline))

def custom_query(collection):
    pipeline = [
        {
            "$match": {"age": {"$gt": 18, "$lt": 25}}
        },
        {
            "$group": {
                "_id": "$city",
                "min_salary": {"$min": "$salary"},
                "avg_salary": {"$avg": "$salary"},
                "max_salary": {"$max": "$salary"}
            }
        }
    ]
    return list(collection.aggregate(pipeline))

def save_data_to_json(data, file_name):
    with open(file_name, 'w', encoding='utf-8') as json_file:
        json.dump(data, json_file, ensure_ascii=False, indent=4)

def main():
    pkl_data = load_data_from_pkl('task_1_item.pkl')
    json_data = load_data_from_json('task_2_item.json')
    collection = connect_to_mongo('employee_db', 'employees')
    insert_data_into_mongo(collection, pkl_data)
    insert_data_into_mongo(collection, json_data)

    results = {}
    results["salary_stats"] = serialize_mongo_data(salary_stats(collection))
    results["count_by_profession"] = serialize_mongo_data(count_by_profession(collection))
    results["salary_by_city"] = serialize_mongo_data(salary_by_city(collection))
    results["salary_by_profession"] = serialize_mongo_data(salary_by_profession(collection))
    results["age_stats_by_city"] = serialize_mongo_data(age_stats_by_city(collection))
    results["age_stats_by_profession"] = serialize_mongo_data(age_stats_by_profession(collection))
    results["max_salary_min_age"] = serialize_mongo_data(max_salary_min_age(collection))
    results["min_salary_max_age"] = serialize_mongo_data(min_salary_max_age(collection))
    results["age_stats_salary_above_50k"] = serialize_mongo_data(age_stats_salary_above_50k(collection))
    results["custom_query"] = serialize_mongo_data(custom_query(collection))

    save_data_to_json(results, 'output_data_task_2.json')
    print("Результаты успешно сохранены в 'output_data.json'.")

if __name__ == '__main__':
    main()
