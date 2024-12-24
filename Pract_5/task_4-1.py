import pickle
import pandas as pd
from pymongo import MongoClient
import json

def load_data_from_csv(file_path):
    df = pd.read_csv(file_path)
    data = df.to_dict(orient='records')
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

def query_1_new(collection):
    result = collection.find().sort('Study_Hours_Per_Day', -1).limit(10)
    return serialize_mongo_data(list(result))

def query_2_new(collection):
    result = collection.find({'GPA': {'$gt': 3.0}}).sort('Study_Hours_Per_Day', -1).limit(15)
    return serialize_mongo_data(list(result))

def query_3_new(collection):
    result = collection.find({
        'Stress_Level': 'High',
        'Physical_Activity_Hours_Per_Day': {'$gt': 4}
    }).sort('Study_Hours_Per_Day', 1).limit(10)
    return serialize_mongo_data(list(result))

def query_4_new(collection):
    filter_query = {'Study_Hours_Per_Day': {'$gte': 5, '$lte': 7}}
    count = collection.count_documents(filter_query)
    return count

def query_5_new(collection):
    result = collection.find({
        'Stress_Level': 'High',
        'GPA': {'$gt': 4.0}
    }).sort('Study_Hours_Per_Day', 1).limit(10)
    return serialize_mongo_data(list(result))

def write_all_to_json(file_name, data):
    with open(file_name, 'w', encoding='utf-8') as json_file:
        json.dump(data, json_file, ensure_ascii=False, indent=4)

def main():
    data = load_data_from_csv('student_lifestyle_dataset.csv')
    collection = connect_to_mongo('student_db', 'students')
    insert_data_into_mongo(collection, data)

    results = {}
    print("Запрос 1: Переход по учебным часам (по убыванию) и вывод первых 10 записей")
    result_1 = query_1_new(collection)
    results["query_1"] = result_1

    print("\nЗапрос 2: GPA > 3.0 и по учебным часам (по убыванию) первых 15 записей")
    result_2 = query_2_new(collection)
    results["query_2"] = result_2

    print("\nЗапрос 3: Студенты с высоким уровнем стресса и физической активностью > 4 часов")
    result_3 = query_3_new(collection)
    results["query_3"] = result_3

    print("\nЗапрос 4: Количество студентов с часами учебы между 5 и 7")
    count_4 = query_4_new(collection)
    results["query_4_count"] = count_4

    print("\nЗапрос 5: Студенты с высоким уровнем стресса и GPA > 4.0, отсортированные по учебным часам")
    result_5 = query_5_new(collection)
    results["query_5"] = result_5

    write_all_to_json('task_4-1.json', results)

if __name__ == '__main__':
    main()
