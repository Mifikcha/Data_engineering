import pandas as pd
import json
from pymongo import MongoClient

def load_data_from_xlsx(file_path):
    data = pd.read_excel(file_path)
    data = data.to_dict(orient='records')
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

def delete_by_custom_predicate(collection, predicate):
    collection.delete_many(predicate)

def increase_marks(collection):
    collection.update_many(
        {}, 
        {"$mul": {"Marks": 1.10}}
    )

def save_data_to_json(data, file_path):
    with open(file_path, 'w', encoding='utf-8') as file:
        json.dump(data, file, ensure_ascii=False, indent=4)

def count_students_above_50(collection):
    pipeline = [
        {
            "$match": {"Marks": {"$gt": 50}}
        },
        {
            "$count": "students_above_50"
        }
    ]
    result = list(collection.aggregate(pipeline))
    return result

def average_marks_by_courses(collection):
    pipeline = [
        {
            "$group": {
                "_id": "$number_courses",
                "avg_marks": {"$avg": "$Marks"}
            }
        },
        {
            "$sort": {"_id": 1}
        }
    ]
    result = list(collection.aggregate(pipeline))
    return result

def main():
    file_path = 'Student_Marks.xlsx'
    new_data = load_data_from_xlsx(file_path)

    collection = connect_to_mongo('education_db', 'students')

    collection.insert_many(new_data)

    print("Увеличение 'Marks' на 10%...")
    increase_marks(collection)

    custom_predicate = {"Marks": {"$lt": 10}}

    print("Удаление записей с Marks < 10...")
    delete_by_custom_predicate(collection, custom_predicate)

    updated_data = list(collection.find())
    serialized_data = serialize_mongo_data(updated_data)
    save_data_to_json(serialized_data, 'task_4-3.json')

    print("Подсчет студентов с Marks > 50...")
    students_above_50 = count_students_above_50(collection)
    print(json.dumps(students_above_50, indent=4))

    print("Среднее значение Marks по количеству курсов...")
    avg_marks_by_courses = average_marks_by_courses(collection)
    print(json.dumps(avg_marks_by_courses, indent=4))

    print("Операции завершены успешно.")

if __name__ == '__main__':
    main()
