import json
from pymongo import MongoClient

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

def exam_score_stats(collection):
    pipeline = [
        {
            "$group": {
                "_id": None,
                "min_score": {"$min": "$Exam_Score"},
                "avg_score": {"$avg": "$Exam_Score"},
                "max_score": {"$max": "$Exam_Score"}
            }
        }
    ]
    return list(collection.aggregate(pipeline))

def attendance_by_motivation(collection):
    pipeline = [
        {
            "$group": {
                "_id": "$Motivation_Level",
                "avg_attendance": {"$avg": "$Attendance"}
            }
        }
    ]
    return list(collection.aggregate(pipeline))

def sleep_hours_by_school_type(collection):
    pipeline = [
        {
            "$group": {
                "_id": "$School_Type",
                "avg_sleep": {"$avg": "$Sleep_Hours"}
            }
        }
    ]
    return list(collection.aggregate(pipeline))

def physical_activity_by_income(collection):
    pipeline = [
        {
            "$group": {
                "_id": "$Family_Income",
                "avg_physical_activity": {"$avg": "$Physical_Activity"}
            }
        }
    ]
    return list(collection.aggregate(pipeline))

def exam_score_by_school_type(collection):
    pipeline = [
        {
            "$group": {
                "_id": "$School_Type",
                "avg_exam_score": {"$avg": "$Exam_Score"}
            }
        }
    ]
    return list(collection.aggregate(pipeline))

def write_all_to_json(file_name, data):
    with open(file_name, 'w', encoding='utf-8') as json_file:
        json.dump(data, json_file, ensure_ascii=False, indent=4)

def main():
    json_data = load_data_from_json('StudentPerformanceFactors.json')
    collection = connect_to_mongo('student_db', 'students')
    insert_data_into_mongo(collection, json_data)

    print("Статистика по Exam_Score:")
    result_1 = exam_score_stats(collection)
    print(json.dumps(serialize_mongo_data(result_1), indent=4))

    print("\nСтатистика по Attendance по уровням мотивации:")
    result_2 = attendance_by_motivation(collection)
    print(json.dumps(serialize_mongo_data(result_2), indent=4))

    print("\nСтатистика по Sleep_Hours по типу школы:")
    result_3 = sleep_hours_by_school_type(collection)
    print(json.dumps(serialize_mongo_data(result_3), indent=4))

    print("\nСтатистика по Physical_Activity по уровню семейного дохода:")
    result_4 = physical_activity_by_income(collection)
    print(json.dumps(serialize_mongo_data(result_4), indent=4))

    print("\nСтатистика по среднему баллу по типу школы:")
    result_5 = exam_score_by_school_type(collection)
    print(json.dumps(serialize_mongo_data(result_5), indent=4))

    write_all_to_json('task_4-2.json', {
        "exam_score_stats": result_1,
        "attendance_by_motivation": result_2,
        "sleep_hours_by_school_type": result_3,
        "physical_activity_by_income": result_4,
        "exam_score_by_school_type": result_5
    })

if __name__ == '__main__':
    main()
