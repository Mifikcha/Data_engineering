import json
import os

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

# Путь к файлу
file_path = "32/ratings_Electronics/ratings_Electronics.csv"

# 1. Загрузка данных и предварительный анализ
# 1.1 Определение объема памяти файла на диске
file_size = os.path.getsize(file_path) # Изначально - байты
print(f"Размер файла на диске: {file_size / 1024 ** 2:.2f} МБ")

# Загрузка данных в DataFrame
data = pd.read_csv(
    file_path, names=["user_id", "product_id", "rating", "timestamp"]
)

# 1.2 Объем памяти набора данных в памяти
memory_usage_before = data.memory_usage(deep=True).sum()
print(
    "Объем памяти набора данных в памяти: "
    f"{memory_usage_before / 1024 ** 2:.2f} МБ"
)
'''
Параметр deep=True означает, что подсчет объема памяти для объектов с типом object или category
будет включать объем памяти, занимаемый самими объектами, а не только ссылками на них.

Метод .sum() вычисляет сумму всех значений в результате, возвращенном memory_usage.
То есть, складывается объем памяти, занимаемый всеми колонками и индексами DataFrame.
'''
# 2. Анализ памяти до оптимизации
data_memory_before = data.memory_usage(deep=True).sum() / (1024**2)
columns_info_before = [
    {
        "column": col,
        "dtype": data[col].dtype, # Определяет тип данных (dtype) текущей колонки, например int64, float64, object, или category.
        "memory_usage_mb": data[col].memory_usage(deep=True) / (1024**2),
        "unique_values": data[col].nunique(), # Считает количество уникальных значений в текущей колонке.
    }
    for col in data.columns
]

# Преобразование типов для JSON
for col in columns_info_before:
    col["dtype"] = str(col["dtype"])

# Сортировка по занимаемому объему памяти в обратном порядке
columns_info_before = sorted(
    columns_info_before, key=lambda x: x["memory_usage_mb"], reverse=True
)

# Сохранение статистики до оптимизации
with open("result/column_stats_before.json", "w", encoding="utf-8") as f:
    json.dump(columns_info_before, f, ensure_ascii=False, indent=4)

# 3. Преобразование object в категориальные
for col in data.select_dtypes(include="object"):
    if data[col].nunique() / len(data[col]) < 0.5:
        data[col] = data[col].astype("category")
'''
Это необходимо для того, чтобы преобразовать тип object в более экономный тип по памяти category
'''
# 4. Понижающее преобразование типов int и float
for col in data.select_dtypes(include="int"):
    data[col] = pd.to_numeric(data[col], downcast="integer")

for col in data.select_dtypes(include="float"):
    data[col] = pd.to_numeric(data[col], downcast="float")
'''
Преобразование типов данных int и float в датафрейм, опять же, более экономно по памяти
Преобразование автоматически подбирается достаточное для точности
'''
# 5. Анализ памяти после оптимизации
data_memory_after = data.memory_usage(deep=True).sum() / (1024**2)
columns_info_after = [
    {
        "column": col,
        "dtype": data[col].dtype,
        "memory_usage_mb": data[col].memory_usage(deep=True) / (1024**2),
        "unique_values": data[col].nunique(),
    }
    for col in data.columns
]

# Преобразование типов для JSON
for col in columns_info_after:
    col["dtype"] = str(col["dtype"])

# Сортировка по занимаемому объему памяти
columns_info_after = sorted(
    columns_info_after, key=lambda x: x["memory_usage_mb"], reverse=True
)

# Сохранение статистики после оптимизации
with open("result/column_stats_after.json", "w", encoding="utf-8") as f:
    json.dump(columns_info_after, f, ensure_ascii=False, indent=4)

# 6. Сравнение объема памяти
print(f"Память до оптимизации: {data_memory_before:.2f} МБ")
print(f"Память после оптимизации: {data_memory_after:.2f} МБ")

# 7. Сохранение выбранных колонок с чанками
columns_to_save = ["user_id", "product_id", "rating", "timestamp"]
chunk_size = 1000000

with pd.read_csv(
    file_path,
    names=columns_to_save,
    chunksize=chunk_size,
) as reader:
    for i, chunk in enumerate(reader):
        chunk = chunk[columns_to_save]
        chunk.to_csv(f"result/optimized_data_part_{i}.csv", index=False)

# Графики на основе оптимизированных данных
data_sample = data.sample(7000000)  #Случайная выборка для построения графиков

# График распределения рейтингов
plt.figure(figsize=(8, 6))
sns.histplot(data_sample["rating"], bins=10, kde=True)
plt.title("Распределение рейтингов")
plt.xlabel("Рейтинг")
plt.ylabel("Частота")
plt.savefig("result/rating_distribution.png")

# График корреляции
numerical_columns = data_sample.select_dtypes(include=["number"])
plt.figure(figsize=(8, 6))
sns.heatmap(numerical_columns.corr(), annot=True, cmap="coolwarm")
plt.title("Корреляция между числовыми колонками")
plt.savefig("result/correlation_heatmap.png")

# Столбчатый график популярных продуктов
popular_products = data_sample["product_id"].value_counts().head(10)
plt.figure(figsize=(10, 6))
popular_products.plot(kind="bar")
plt.title("Топ-10 популярных продуктов")
plt.xlabel("ID продукта")
plt.ylabel("Частота")
plt.savefig("result/popular_products.png")

# Линейный график среднего рейтинга по времени
data_sample["timestamp"] = pd.to_datetime(data_sample["timestamp"], unit="s")
time_rating = data_sample.groupby(data_sample["timestamp"].dt.date)[
    "rating"
].mean()
plt.figure(figsize=(10, 6))
time_rating.plot()
plt.title("Средний рейтинг по времени")
plt.xlabel("Дата")
plt.ylabel("Средний рейтинг")
plt.savefig("result/average_rating_over_time.png")

# Круговая диаграмма распределения оценок
rating_counts = data_sample["rating"].value_counts()
plt.figure(figsize=(8, 6))
rating_counts.plot(kind="pie", autopct="%1.1f%%")
plt.title("Распределение оценок")
plt.savefig("result/rating_pie_chart.png")

print("Анализ данных завершен. Результаты сохранены.")
