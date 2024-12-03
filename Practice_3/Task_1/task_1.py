import json
import os

from bs4 import BeautifulSoup


def parse_building_html(directory):
    data = []

    for filename in os.listdir(directory):
        if filename.endswith(".html"):
            filepath = os.path.join(directory, filename)

            with open(filepath, "r", encoding="utf-8") as file:
                content = file.read()

            soup = BeautifulSoup(content, "html.parser")

            # Извлечение данных
            city = (
                soup.find("span").text.split(":")[1].strip()
                if soup.find("span")
                else "N/A"
            )
            building = (
                soup.find("h1", class_="title").text.split(":")[1].strip()
                if soup.find("h1", class_="title")
                else "N/A"
            )
            address = (
                soup.find("p", class_="address-p")
                .text.strip()
                .replace("\n", "")
                if soup.find("p", class_="address-p")
                else "N/A"
            )
            floors = (
                soup.find("span", class_="floors").text.split(":")[1].strip()
                if soup.find("span", class_="floors")
                else "N/A"
            )
            year = (
                soup.find("span", class_="year").text.split("в ")[1].strip()
                if soup.find("span", class_="year")
                else "N/A"
            )
            parking = "нет" if "Парковка:нет" in content else "да"
            rating = (
                soup.find("div", class_="build-wrapper")
                .find_all("span")[-2]
                .text.split(":")[1]
                .strip()
                if soup.find("div", class_="build-wrapper")
                else "N/A"
            )
            views = (
                soup.find("div", class_="build-wrapper")
                .find_all("span")[-1]
                .text.split(":")[1]
                .strip()
                if soup.find("div", class_="build-wrapper")
                else "N/A"
            )

            # Сохраняем в список
            data.append(
                {
                    "city": city,
                    "building": building,
                    "address": address,
                    "floors": int(floors) if floors.isdigit() else None,
                    "year": int(year) if year.isdigit() else None,
                    "parking": parking,
                    "rating": (
                        float(rating)
                        if rating.replace(".", "", 1).isdigit()
                        else None
                    ),
                    "views": int(views) if views.isdigit() else None,
                }
            )

    return data


def save_json(data, filename):
    with open(filename, "w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=4)


def analyze_data(data):
    # Сортировка по количеству просмотров
    sorted_data = sorted(data, key=lambda x: x["views"], reverse=True)

    # Фильтрация
    filtered_data = [item for item in data if item["rating"] > 3.0]

    # Статистические характеристики для просмотров
    views = [item["views"] for item in data if item["views"] is not None]
    stats = {
        "sum": sum(views),
        "min": min(views),
        "max": max(views),
        "mean": sum(views) / len(views) if views else None,
        "count": len(views),
    }

    # Частота меток для городов
    city_counts = {}
    for item in data:
        city = item["city"]
        city_counts[city] = city_counts.get(city, 0) + 1

    # Сохранение результатов
    save_json(sorted_data, "sorted_data.json")
    save_json(filtered_data, "filtered_data.json")
    save_json(stats, "stats.json")
    save_json(city_counts, "city_counts.json")


# Основная программа
directory = "32/1/"  # Путь к папке с файлами
parsed_data = parse_building_html(directory)

# Сохраняем данные в JSON
save_json(parsed_data, "parsed_data.json")

# Выполняем анализ
analyze_data(parsed_data)
