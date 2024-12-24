import pickle
from pymongo import MongoClient
import json

def load_data_from_pkl(file_path):
    with open(file_path, 'rb') as file:
        data = pickle.load(file)
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

def query_1(collection):
    result = collection.find().sort('salary', -1).limit(10)
    return serialize_mongo_data(list(result))

def query_2(collection):
    result = collection.find({'age': {'$lt': 30}}).sort('salary', -1).limit(15)
    return serialize_mongo_data(list(result))

def query_3(collection, city, professions):
    result = collection.find({
        'city': city,
        'profession': {'$in': professions}
    }).sort('age', 1).limit(10)
    return serialize_mongo_data(list(result))

def query_4(collection, age_range, year_range, salary_range_1, salary_range_2):
    filter_query = {
        'age': {'$gte': age_range[0], '$lte': age_range[1]},
        'year': {'$gte': year_range[0], '$lte': year_range[1]},
        '$or': [
            {'salary': {'$gt': salary_range_1[0], '$lte': salary_range_1[1]}},
            {'salary': {'$gt': salary_range_2[0], '$lt': salary_range_2[1]}}
        ]
    }
    count = collection.count_documents(filter_query)
    return count

def write_all_to_json(file_name, data):
    with open(file_name, 'w', encoding='utf-8') as json_file:
        json.dump(data, json_file, ensure_ascii=False, indent=4)

def main():
    data = load_data_from_pkl('task_1_item.pkl')
    collection = connect_to_mongo('employee_db', 'employees')
    insert_data_into_mongo(collection, data)
    
    results = {}

    print("Запрос 1: Переход по зарплатам (по убыванию) и вывод первых 10 записей")
    result_1 = query_1(collection)
    results["query_1"] = result_1

    print("\nЗапрос 2: Возраст меньше 30 и по зарплатам (по убыванию) первых 15 записей")
    result_2 = query_2(collection)
    results["query_2"] = result_2

    print("\nЗапрос 3: Записи из города 'New York' и профессий ['Engineer', 'Manager', 'Developer'], по возрасту (по возрастанию) первых 10 записей")
    result_3 = query_3(collection, 'New York', ['Engineer', 'Manager', 'Developer'])
    results["query_3"] = result_3

    print("\nЗапрос 4: Количество записей с age в диапазоне [25, 40], year в [2019, 2022], salary в диапазоне [50000, 75000] или [125000, 150000]")
    count_4 = query_4(collection, [25, 40], [2019, 2022], [50000, 75000], [125000, 150000])
    results["query_4_count"] = count_4

    write_all_to_json('all_query_results_task_1.json', results)

if __name__ == '__main__':
    main()
