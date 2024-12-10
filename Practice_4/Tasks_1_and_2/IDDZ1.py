import sqlite3
import json

db_path = 'tournaments.db' 
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS tournaments (
    id INTEGER PRIMARY KEY,
    name TEXT,
    city TEXT,
    begin TEXT,
    system TEXT,
    tours_count INTEGER,
    min_rating INTEGER,
    time_on_game INTEGER
)
""")

def parse_file(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    blocks = content.strip().split("=====\n")
    records = []
    for block in blocks:
        record = {}
        for line in block.split("\n"):
            if "::" in line:
                key, value = line.split("::", 1)
                record[key.strip()] = value.strip()
        records.append(record)
    return records

file_path = 'item.text'

records = parse_file(file_path)

cursor.executemany("""
INSERT INTO tournaments (id, name, city, begin, system, tours_count, min_rating, time_on_game)
VALUES (:id, :name, :city, :begin, :system, :tours_count, :min_rating, :time_on_game)
""", records)

conn.commit()

VAR = 5
LIMIT = VAR + 10

query_1 = f"""
SELECT * FROM tournaments
ORDER BY min_rating ASC
LIMIT {LIMIT}
"""
cursor.execute(query_1)
results_1 = cursor.fetchall()

columns = [desc[0] for desc in cursor.description]
json_output_1 = [dict(zip(columns, row)) for row in results_1]

with open('query1_output.json', 'w', encoding='utf-8') as f:
    json.dump(json_output_1, f, ensure_ascii=False, indent=4)

print("Запрос 1: Данные сохранены в файл query1_output.json")

query_2 = """
SELECT 
    SUM(min_rating) AS total,
    MIN(min_rating) AS minimum,
    MAX(min_rating) AS maximum,
    AVG(min_rating) AS average
FROM tournaments
"""
cursor.execute(query_2)
stats_2 = cursor.fetchone()
print(f"Запрос 2: Сумма: {stats_2[0]}, Минимум: {stats_2[1]}, Максимум: {stats_2[2]}, Среднее: {stats_2[3]:.2f}")

query_3 = """
SELECT system, COUNT(*) AS frequency
FROM tournaments
GROUP BY system
ORDER BY frequency DESC
"""
cursor.execute(query_3)
frequency_3 = cursor.fetchall()
print("Запрос 3: Частота встречаемости категорий:")
for row in frequency_3:
    print(f"System: {row[0]}, Frequency: {row[1]}")

query_4 = f"""
SELECT * FROM tournaments
WHERE tours_count > 10
ORDER BY tours_count DESC
LIMIT {LIMIT}
"""
cursor.execute(query_4)
results_4 = cursor.fetchall()

json_output_4 = [dict(zip(columns, row)) for row in results_4]

with open('query4_output.json', 'w', encoding='utf-8') as f:
    json.dump(json_output_4, f, ensure_ascii=False, indent=4)

print("Запрос 4: Данные сохранены в файл query4_output.json")
conn.close()
