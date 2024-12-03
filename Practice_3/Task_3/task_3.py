import os
import xml.etree.ElementTree as ET
import json
import pandas as pd
from collections import Counter

# Путь к папке с XML файлами
folder_path = "C:/Users/bezzz/OneDrive/Desktop/All/Acheba/Hope/Data_engineering/Practice_3/Task_3/3"
json_output = "C:/Users/bezzz/OneDrive/Desktop/All/Acheba/Hope/Data_engineering/Practice_3/Task_3/json_output.json"
filtered_output = "C:/Users/bezzz/OneDrive/Desktop/All/Acheba/Hope/Data_engineering/Practice_3/Task_3/filtred_data.json"

# Чтение и парсинг данных
data = []
for file_name in os.listdir(folder_path):
    if file_name.endswith('.xml'):
        file_path = os.path.join(folder_path, file_name)
        tree = ET.parse(file_path)
        root = tree.getroot()

        obj = {}
        for element in root:
            obj[element.tag] = element.text.replace('\n  ', '').replace('\n ', '')
        data.append(obj)

# Запись в JSON
with open(json_output, 'w', encoding='utf-8') as f:
    json.dump(data, f, ensure_ascii=False, indent=4)

# Анализ данных
df = pd.DataFrame(data)

# Сортировка по полю
sorted_df = df.sort_values(by="name")
print("Отсортированные данные:\n", sorted_df)

# Фильтрация по полю
filtered_df = df[df['constellation'] == 'Дева']
filtered_df.to_json(filtered_output, orient="records", force_ascii=False)
print("Фильтрованные данные сохранены.")

# Статистики числового поля
numerical_field = "radius"
if numerical_field in df:
    df[numerical_field] = pd.to_numeric(df[numerical_field], errors="coerce")
    stats = {
        "sum": df[numerical_field].sum(),
        "min": df[numerical_field].min(),
        "max": df[numerical_field].max(),
        "mean": df[numerical_field].mean()
    }
    print("Статистики:", stats)

# Частота текстового поля
text_field = "constellation"
if text_field in df:
    frequency = Counter(df[text_field])
    print("Частота меток:", frequency)

print(df['constellation'].unique())

