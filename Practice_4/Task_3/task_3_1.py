import pandas as pd
from sqlalchemy import create_engine

def load_and_process_files(csv_path):
    """Загружает данные из CSV и возвращает DataFrame."""
    data = pd.read_csv(csv_path)

    # Преобразование типов данных
    data['tempo'] = pd.to_numeric(data['tempo'], errors='coerce')
    data['duration_ms'] = pd.to_numeric(data['duration_ms'], errors='coerce')
    data['year'] = pd.to_numeric(data['year'], errors='coerce')

    # Сохраняем преобразованные данные для проверки
    data.to_csv('third_task_combined_data_processed.csv', index=False)
    print("\nДанные успешно обработаны и сохранены в 'third_task_combined_data_processed.csv'.")
    return data

def save_query_result_to_file(query, connection, output_file, as_json=False):
    """Выполняет запрос к базе данных и сохраняет результат в файл."""
    result = pd.read_sql(query, connection)
    if as_json:
        result.to_json(output_file, orient='records')
    else:
        result.to_csv(output_file, index=False)
    print(f"\nРезультат сохранен в '{output_file}'.")
    return result

def main(csv_path):
    """Основная функция выполнения программы."""
    # Шаг 1: Загрузка и обработка данных
    combined_data = load_and_process_files(csv_path)

    # Шаг 2: Создание базы данных
    engine = create_engine('sqlite:///third_task_music_data.db')
    table_name = 'music'
    combined_data.to_sql(table_name, engine, if_exists='replace', index=False)
    connection = engine.connect()

    # VAR для запросов
    VAR = 5

    # Шаг 3: Выполнение запросов

    # Запрос 1: Первые (VAR+10) строк, отсортированных по tempo
    query_1 = f"SELECT * FROM {table_name} ORDER BY tempo DESC LIMIT {VAR + 10}"
    save_query_result_to_file(query_1, connection, 'third_task_VAR+10_sorted_tempo.json', as_json=True)

    # Запрос 2: Сумма, минимум, максимум, среднее для tempo
    query_2 = f"""
    SELECT 
        SUM(tempo) AS total_tempo,
        MIN(tempo) AS min_tempo,
        MAX(tempo) AS max_tempo,
        AVG(tempo) AS avg_tempo
    FROM {table_name}
    """
    save_query_result_to_file(query_2, connection, 'third_task_tempo_stats.csv')

    # Запрос 3: Частота встречаемости жанров
    query_3 = f"""
    SELECT genre, COUNT(*) AS frequency
    FROM {table_name}
    GROUP BY genre
    ORDER BY frequency DESC
    """
    save_query_result_to_file(query_3, connection, 'third_task_genre_frequencies.csv')

    # Запрос 4: Первые (VAR+15) строки, отфильтрованные по году > 2010, отсортированные по tempo
    query_4 = f"""
    SELECT * FROM {table_name} 
    WHERE year > 2010 
    ORDER BY tempo DESC 
    LIMIT {VAR + 15}
    """
    save_query_result_to_file(query_4, connection, 'third_task_VAR+15_sorted_year_more_2010.json', as_json=True)

    print("\nВсе запросы выполнены и результаты сохранены.")

main('third_task_combined_data.csv')
