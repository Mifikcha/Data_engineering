import pickle
import json
import random
from pymongo import MongoClient
import csv

def load_data_from_file(file_path):
    data = []
    with open(file_path, newline='', encoding='utf-8') as file:
        reader = csv.DictReader(file, delimiter=';')
        for row in reader:
            row['salary'] = int(row['salary'])
            row['age'] = int(row['age'])
            data.append(row)
    return data

def connect_to_mongo(db_name, collection_name):
    client = MongoClient('mongodb://localhost:27017/')
    db = client[db_name]
    collection = db[collection_name]
    return collection

def serialize_mongo_data(data):
    for record in data:
        if '_id' in record:
            record['_id'] = str(record['_id'])
    return data

def delete_by_salary(collection):
    collection.delete_many({"salary": {"$lt": 25000, "$gt": 175000}})

def increment_age(collection):
    collection.update_many({}, {"$inc": {"age": 1}})

def increase_salary_for_professions(collection, professions):
    collection.update_many(
        {"job": {"$in": professions}},
        {"$mul": {"salary": 1.05}}
    )

def increase_salary_for_cities(collection, cities):
    collection.update_many(
        {"city": {"$in": cities}},
        {"$mul": {"salary": 1.07}}
    )

def increase_salary_for_complex_predicate(collection, city, professions, min_age, max_age):
    collection.update_many(
        {"city": city, "job": {"$in": professions}, "age": {"$gte": min_age, "$lte": max_age}},
        {"$mul": {"salary": 1.10}}
    )

def delete_by_custom_predicate(collection, predicate):
    collection.delete_many(predicate)

def insert_data_into_mongo(collection, data):
    collection.insert_many(data)

def main():
    file_path = 'task_3_item.csv'
    new_data = load_data_from_file(file_path)
    collection = connect_to_mongo('employee_db', 'employees')
    insert_data_into_mongo(collection, new_data)

    print("Удаление документов с зарплатой < 25000 или > 175000...")
    delete_by_salary(collection)

    print("Увеличение возраста всех документов на 1...")
    increment_age(collection)

    professions = ['Программист', 'Оператор call-центра', 'Продавец']
    cities = ['Малага', 'Барселона', 'Санкт-Петербург']

    print("Поднятие зарплаты на 5% для профессий...")
    increase_salary_for_professions(collection, professions)

    print("Поднятие зарплаты на 7% для городов...")
    increase_salary_for_cities(collection, cities)

    city = 'Малага'
    professions_complex = ['Программист', 'Учитель']
    min_age = 30
    max_age = 50

    print("Поднятие зарплаты на 10% для сложного предиката...")
    increase_salary_for_complex_predicate(collection, city, professions_complex, min_age, max_age)

    custom_predicate = {"salary": {"$lt": 30000}}
    print("Удаление по произвольному предикату...")
    delete_by_custom_predicate(collection, custom_predicate)

    print("Операции завершены успешно.")

if __name__ == '__main__':
    main()
