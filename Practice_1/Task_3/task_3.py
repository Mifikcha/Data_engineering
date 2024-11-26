import pandas as pd

def third_task(file_path):

    # Чтение данных из файла и преобразование в DataFrame
    df = pd.DataFrame(
        open(file_path).read().strip().split(), columns=["values"]
    )

    # Преобразование значений в числовой формат, обработка ошибок
    df["values"] = pd.to_numeric(df["values"], errors="coerce")

    # Заполнение пропусков средним значением соседних чисел
    for i in range(1, len(df) - 1):
        if pd.isna(df.loc[i, "values"]):
            left, right = df.loc[i - 1, "values"], df.loc[i + 1, "values"]
            if not pd.isna(left) and not pd.isna(right):
                df.loc[i, "values"] = (left + right) / 2

    # Фильтрация данных: четные значения больше 500
    filtered_df = df[(df["values"] > 500) & (df["values"] % 2 == 0)]

    # Запись результата в файл CSV
    filtered_df.to_csv(
        "filtered_numbers.csv", header=True, index=False
    )

    print("Результирующий файл сохранен как 'filtered_numbers.csv'.")

# Пример вызова функции
third_task("C:/Users/bezzz/OneDrive/Desktop/All/Acheba/Hope/Data_engineering/Practice_1/Task_3/third_task.txt")
