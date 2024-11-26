import numpy as np
import os

# 1. Загрузка матрицы из файла second.npy
file_path = 'second_task.npy'
matrix = np.load(file_path)

# 2. Создание массивов
x = []
y = []
z = []

# 3. Проход по матрице и отбор значений, превышающих 562
for i in range(matrix.shape[0]):
    for j in range(matrix.shape[1]):
        if matrix[i][j] > 562:
            x.append(i)
            y.append(j)
            z.append(matrix[i, j])

# 4. Сохранение массивов в файл формата .npz
np.savez('second_task_uncompressed.npz', x=x, y=y, z=z)
np.savez_compressed('second_task_compressed.npz', x=x, y=y, z=z)

# 5. Сравнение размеров файлов
size_uncompressed = os.path.getsize('second_task_uncompressed.npz')
size_compressed = os.path.getsize('second_task_compressed.npz')

print(f"Размер несжатого файла: {size_uncompressed} байт")
print(f"Размер сжатого файла: {size_compressed} байт")
print(f"Разница в размерах: {size_uncompressed - size_compressed} байт")