import re
from collections import Counter

def count_word_frequency(input_file, output_file, consonant_output_file):
    try:
        # Читаем содержимое файла
        with open(input_file, 'r', encoding='utf-8') as f:
            text = f.read()
        
        # Убираем пунктуацию и переводим текст в нижний регистр
        words = re.findall(r'\b\w+\b', text.lower())
        
        # Подсчитываем частоту слов
        word_counts = Counter(words)
        
        # Сортируем по частоте (убывание), затем по алфавиту
        sorted_word_counts = sorted(word_counts.items(), key=lambda x: (-x[1], x[0]))
        
        # Записываем результат в файл
        with open(output_file, 'w', encoding='utf-8') as f:
            for word, count in sorted_word_counts:
                f.write(f"{word}:{count}\n")
        
        print(f"Частота слов сохранена в файл: {output_file}")

        # Определение списка согласных букв английского алфавита
        consonants = 'bcdfghjklmnpqrstvwxyz'

        # Подсчитываем количество слов, начинающихся на согласную
        consonant_words_count = sum(1 for word in words if word[0] in consonants)
        
        # Общее количество слов
        total_words_count = len(words)

        # Доля слов, начинающихся на согласную
        consonant_words_ratio = consonant_words_count / total_words_count if total_words_count > 0 else 0

        # Записываем статистику в отдельный файл
        with open(consonant_output_file, 'w', encoding='utf-8') as f:
            f.write(f"Total words: {total_words_count}\n")
            f.write(f"Consonant words: {consonant_words_count}\n")
            f.write(f"Consonant words ratio: {consonant_words_ratio:.2f}\n")

        print(f"Статистика по согласным словам сохранена в файл: {consonant_output_file}")
    
    except FileNotFoundError:
        print(f"Файл {input_file} не найден.")
    except Exception as e:
        print(f"Ошибка: {e}")

# Укажите путь к вашему входному и выходным файлам
input_file = 'input.txt'  # Входной файл с текстом
output_file = 'output.txt'  # Результирующий файл для частотности слов
consonant_output_file = 'consonant_stats.txt'  # Результирующий файл для статистики по согласным словам

count_word_frequency('C:/Users/bezzz/OneDrive/Desktop/All/Acheba/Hope/Data_engineering/Practice_1/Task_1/first_task.txt', 'C:/Users/bezzz/OneDrive/Desktop/All/Acheba/Hope/Data_engineering/Practice_1/Task_1/output_file.txt', 'C:/Users/bezzz/OneDrive/Desktop/All/Acheba/Hope/Data_engineering/Practice_1/Task_1/consonant_output_file.txt')

    

