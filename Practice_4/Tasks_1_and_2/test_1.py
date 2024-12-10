import msgpack
from pprint import pprint  # Современный и удобный способ форматирования вывода

def read_msgpack(file_path):
    with open(file_path, 'rb') as f:
        data = msgpack.unpack(f, raw=False)  # raw=False для декодирования байтов в строки
    return data

# Пример использования
file_path = "item.msgpack"  # Путь к вашему файлу
data = read_msgpack(file_path)

# Просмотр структуры данных
pprint(data)
