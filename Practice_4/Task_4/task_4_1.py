import sqlite3
import json
import csv


def connect_to_db(filename):
    conn = sqlite3.connect(filename)
    conn.row_factory = sqlite3.Row
    return conn


def create_table(db):
    cur = db.cursor()
    query = '''
        CREATE TABLE IF NOT EXISTS products(
            id INTEGER PRIMARY KEY,
            name TEXT,
            price FLOAT,
            quantity INTEGER,
            category TEXT,
            fromCity TEXT,
            isAvailable BOOL,
            views INTEGER,
            version INTEGER DEFAULT 0
        )
    '''
    cur.execute(query)


def insert_into_db(db, data):
    cur = db.cursor()
    query = '''
            INSERT INTO products (name, price, quantity,
                                  category, fromCity, isAvailable, views)
            VALUES (:name, :price, :quantity,
                    :category, :fromCity, :isAvailable, :views)
            '''
    valid_data = [
        item for item in data
        if all(key in item and item[key] for key in ['name', 'price', 'quantity', 'category', 'fromCity', 'isAvailable', 'views'])
    ]
    cur.executemany(query, valid_data)
    db.commit()


def update_db(db, updates):
    for product in updates:
        cur = db.cursor()
        name = product['name']
        param = product.get('param', None)

        if product['method'] == 'remove':
            cur.execute('DELETE FROM products WHERE name = ?', [name])

        elif product['method'] == 'price_percent' and param is not None:
            cur.execute('''UPDATE products
                           SET price = ROUND(price * (1 + ?), 2),
                               version = version + 1
                           WHERE name = ?''', [float(param), name])

        elif product['method'] == 'price_abs' and param is not None:
            cur.execute('''UPDATE products
                           SET price = price + ?,
                               version = version + 1
                           WHERE name = ?''', [float(param), name])

        elif product['method'] in ['quantity_add', 'quantity_sub'] and param is not None:
            cur.execute('''UPDATE products
                           SET quantity = quantity + ?,
                               version = version + 1
                           WHERE name = ?''', [int(param), name])

        elif product['method'] == 'available':
            cur.execute('''UPDATE products
                           SET isAvailable = ?,
                               version = version + 1
                           WHERE name = ?''', [param == 'True', name])

        db.commit()


def select_from_db(db, query):
    cur = db.cursor()
    res = cur.execute(query)
    return [dict(row) for row in res.fetchall()]


def read_csv(filename):
    with open(filename, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file, delimiter=';')
        items = [
            {
                'name': row['name'],
                'price': float(row['price']),
                'quantity': int(row['quantity']),
                'category': row['category'],
                'fromCity': row['fromCity'],
                'isAvailable': row['isAvailable'] == 'True',
                'views': int(row['views'])
            }
            for row in reader
            if all(row[key] for key in ['name', 'price', 'quantity', 'category', 'fromCity', 'isAvailable', 'views'])
        ]
        return items


def read_json(filename):
    with open(filename, 'r', encoding='utf-8') as file:
        updates = json.load(file)
        return [
            upd for upd in updates
            if all(key in upd and upd[key] for key in ['name', 'method']) and
            (upd['method'] == 'remove' or 'param' in upd)
        ]


# Основной код
data_main = read_csv('_product_data.csv')
data_upd = read_json('_update_data.json')

# Подключение к базе данных
db = connect_to_db('fourth_task1.db')

# Создание таблицы (если она ещё не создана)
create_table(db)

# Загрузка данных в таблицу
insert_into_db(db, data_main)

# Обновление данных
update_db(db, data_upd)

# Топ-10 самых обновляемых товаров
query = 'SELECT * FROM products ORDER BY version DESC LIMIT 10'
res = select_from_db(db, query)
for el in res:
    print(el['name'] + ' из ' + el['fromCity'])

# Анализ цен товаров
query = '''
        SELECT
            category,
            ROUND(SUM(price), 2) as sum,
            ROUND(AVG(price), 2) as avg,
            ROUND(MIN(price), 2) as min,
            ROUND(MAX(price), 2) as max,
            COUNT(price) as count
        FROM products
        GROUP BY category
        '''
print(select_from_db(db, query))

# Анализ остатков товаров
query = '''
        SELECT
            category,
            ROUND(SUM(quantity), 2) as sum,
            ROUND(AVG(quantity), 2) as avg,
            ROUND(MIN(quantity), 2) as min,
            ROUND(MAX(quantity), 2) as max,
            COUNT(quantity) as count
        FROM products
        GROUP BY category
        '''
print(select_from_db(db, query))

# Топ-10 наиболее просматриваемых товаров из категории 'fruit'
query = '''
SELECT name
FROM products
WHERE category = 'fruit'
ORDER BY views DESC
LIMIT 10
'''
for el in select_from_db(db, query):
    print(el['name'])

db.close()
