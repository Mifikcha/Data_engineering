import json
import pickle

# Функция для обновления цены
def update_price(price, method, param):
    if method == "add":
        return price + param
    elif method == "sub":
        return price - param
    elif method == "percent+":
        return price * (1 + param)
    elif method == "percent-":
        return price * (1 - param)
    else:
        raise ValueError(f"Неизвестный метод: {method}")

# Чтение данных из PKL файла (данные о товарах)
with open('fourth_task_products.pkl', 'rb') as pkl_file:
    products = pickle.load(pkl_file)  # Список товаров с их ценами

# Чтение обновлений цен из JSON файла
with open('fourth_task_updates.json', 'r', encoding='utf-8') as json_file:
    price_updates = json.load(json_file)  # Список обновлений цен

# Применение изменений цен
for update in price_updates:
    for product in products:
        if product['name'] == update['name']:
            # Обновление цены
            product['price'] = update_price(product['price'], update['method'], update['param'])

# Сохранение обновленных данных обратно в формат PKL
with open('updated_products.pkl', 'wb') as pkl_file:
    pickle.dump(products, pkl_file)

print("Цены успешно обновлены и сохранены в файл updated_products.pkl.")




