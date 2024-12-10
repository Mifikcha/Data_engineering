import sqlite3
import msgpack

# Путь к базе данных
db_path = 'tournaments.db'
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Создание таблицы prizes
cursor.execute("""
CREATE TABLE IF NOT EXISTS prizes (
    name TEXT,
    place INTEGER,
    prise INTEGER,
    FOREIGN KEY (name) REFERENCES tournaments (name)
)
""")

# Функция для парсинга файлов MsgPack
def parse_prizes_msgpack(file_path):
    with open(file_path, 'rb') as f:
        data = msgpack.unpack(f, raw=False)
    return data

# Путь к файлу MsgPack с данными
prizes_file_path = 'subitem.msgpack'

# Чтение данных из файла MsgPack
prizes_records = parse_prizes_msgpack(prizes_file_path)

# Преобразование данных в список кортежей
prizes_data = [
    (record['name'], int(record['place']), int(record['prise']))
    for record in prizes_records
]

# Вставка данных в таблицу prizes
cursor.executemany("""
INSERT INTO prizes (name, place, prise)
VALUES (?, ?, ?)
""", prizes_data)

conn.commit()

# Проверка количества записей
cursor.execute("SELECT COUNT(*) FROM prizes")
print(f"Количество записей в таблице prizes: {cursor.fetchone()[0]}")

# 4. Запросы, использующие связь между таблицами

# Запрос 1: Суммарный призовой фонд по системе турнира
query_1 = """
SELECT t.system, SUM(p.prise) AS total_prise
FROM tournaments t
JOIN prizes p ON t.name = p.name
GROUP BY t.system
ORDER BY total_prise DESC
"""
cursor.execute(query_1)
print("Запрос 1: Суммарный призовой фонд по системе турнира")
for row in cursor.fetchall():
    print(f"System: {row[0]}, Total Prize: {row[1]}")

# Запрос 2: Лучшие результаты (место = 1) для турниров с min_rating > 2400
query_2 = """
SELECT t.name, t.min_rating, p.place, p.prise
FROM tournaments t
JOIN prizes p ON t.name = p.name
WHERE p.place = 1 AND t.min_rating > 2400
ORDER BY t.min_rating DESC
"""
cursor.execute(query_2)
print("\nЗапрос 2: Лучшие результаты для турниров с min_rating > 2400")
for row in cursor.fetchall():
    print(f"Name: {row[0]}, Min Rating: {row[1]}, Place: {row[2]}, Prize: {row[3]}")

# Запрос 3: Количество турниров с призовыми местами (топ-3) по годам
query_3 = """
SELECT t.begin, COUNT(DISTINCT t.name) AS tournament_count
FROM tournaments t
JOIN prizes p ON t.name = p.name
WHERE p.place <= 3
GROUP BY t.begin
ORDER BY t.begin
"""
cursor.execute(query_3)
print("\nЗапрос 3: Количество турниров с призовыми местами по годам")
for row in cursor.fetchall():
    print(f"Year: {row[0]}, Tournament Count: {row[1]}")

# Закрытие соединения
conn.close()
