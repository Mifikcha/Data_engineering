import os
import json
import statistics
from bs4 import BeautifulSoup
from collections import Counter


# Задание 1: Парсинг всех HTML файлов
def parse_html_files(directory_path, output_file="task_2_parsed_data_all.json"):
    def handle_file(path):
        with open(path, "r", encoding="utf-8") as file:
            html_content = file.read()

            # Создаем объект BeautifulSoup
            soup = BeautifulSoup(html_content, "html.parser")
            products = soup.find_all("div", attrs={'class': 'product-item'})
            items = []  # Список для хранения информации о продуктах

            # Парсим данные
            for product in products:
                item = {}
                try:
                    item['id'] = int(product.a['data-id'])
                    item['link'] = product.find_all('a')[1]['href']
                    item['img'] = product.img['src']
                    item['title'] = product.span.get_text().strip()
                    item['price'] = float(product.price.get_text().replace('₽', '').replace(" ", "").strip())
                    item['bonus'] = int(product.strong.get_text()
                                        .replace("+ начислим", "")
                                        .replace(" бонусов", "")
                                        .strip())

                    # Парсим дополнительные характеристики
                    properties = product.ul.find_all("li")
                    for prop in properties:
                        item[prop['type']] = prop.get_text().strip()

                    items.append(item)
                except Exception as e:
                    print(f"Ошибка при обработке товара: {e}")
            return items

    all_items = []
    for filename in os.listdir(directory_path):
        if filename.endswith(".html"):
            file_path = os.path.join(directory_path, filename)
            all_items.extend(handle_file(file_path))

    # Сохраняем данные в JSON
    with open(output_file, "w", encoding="utf-8") as json_file:
        json.dump(all_items, json_file, ensure_ascii=False, indent=4)

    print(f"Данные успешно сохранены в файл {output_file}")
    return output_file


# Задание 2: Операции с данными
def process_data(input_file="second_task_parsed_data_all.json"):
    with open(input_file, "r", encoding="utf-8") as json_file:
        data = json.load(json_file)

    print(f"Всего объектов: {len(data)}")
    print(f"Пример данных: {data[0]}")

    # Задача 2.1 - Сортировка по цене
    def sort_by_price(data, output_file="second_task_sorted_data.json"):
        sorted_data = sorted(data, key=lambda x: x['price'])
        with open(output_file, "w", encoding="utf-8") as json_file:
            json.dump(sorted_data, json_file, ensure_ascii=False, indent=4)
        print(f"Данные отсортированы по цене и сохранены в '{output_file}'")
        return sorted_data

    # Задача 2.2 - Фильтрация по бонусам
    def filter_by_bonus(data, bonus_threshold=100, output_file="second_task_filtered_data.json"):
        filtered_data = [item for item in data if item['bonus'] > bonus_threshold]
        with open(output_file, "w", encoding="utf-8") as json_file:
            json.dump(filtered_data, json_file, ensure_ascii=False, indent=4)
        print(f"Фильтрованные данные (бонусы > {bonus_threshold}) сохранены в '{output_file}'")
        return filtered_data

    # Задача 2.3 - Статистика по ценам
    def calculate_price_statistics(data, output_file="second_task_price_statistics.json"):
        prices = [item['price'] for item in data]
        stats = {
            "count": len(prices),
            "sum": sum(prices),
            "min": min(prices),
            "max": max(prices),
            "mean": statistics.mean(prices),
            "median": statistics.median(prices)
        }
        with open(output_file, "w", encoding="utf-8") as json_file:
            json.dump(stats, json_file, ensure_ascii=False, indent=4)
        print(f"Статистика цен сохранена в '{output_file}'")
        return stats

    # Задача 2.4 - Частота заголовков
    def calculate_title_frequency(data, output_file="second_task_titles.json"):
        titles = [item['title'] for item in data]
        title_frequency = Counter(titles)
        with open(output_file, "w", encoding="utf-8") as json_file:
            json.dump(title_frequency, json_file, ensure_ascii=False, indent=4)
        print(f"Частота заголовков сохранена в '{output_file}'")
        return title_frequency

    # Выполнение всех операций
    sorted_data = sort_by_price(data)
    filtered_data = filter_by_bonus(data)
    price_stats = calculate_price_statistics(data)
    title_frequency = calculate_title_frequency(data)

    return sorted_data, filtered_data, price_stats, title_frequency


# Задание 1: Парсим HTML файлы
directory_path = "./2"  # Указываем путь к папке с HTML файлами
parsed_file = parse_html_files(directory_path)

# Задание 2: Обрабатываем данные
process_data(parsed_file)