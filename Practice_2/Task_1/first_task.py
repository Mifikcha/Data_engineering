import json

import numpy as np


def first_task(path):
    matrix = np.load(path)

    total_sum = int(matrix.sum())
    average = float(matrix.mean())

    # Главная диагональ
    main_diag = np.diagonal(matrix)
    sum_main_diag = int(main_diag.sum())
    average_main_diag = float(main_diag.mean())
    # Побочная диагональ
    side_diag = np.fliplr(matrix).diagonal()
    sum_side_diag = int(side_diag.sum())
    average_side_diag = float(side_diag.mean())

    max_value = int(matrix.max())
    min_value = int(matrix.min())

    result = {
        "sum": total_sum,
        "avr": average,
        "sumMD": sum_main_diag,
        "avrMD": average_main_diag,
        "sumSD": sum_side_diag,
        "avrSD": average_side_diag,
        "max": max_value,
        "min": min_value,
    }

    normalized_matrix = (matrix - min_value) / (max_value - min_value)

    np.save("normalized_matrix.npy", normalized_matrix)

    with open("first_task.json", "w") as json_file:
        json.dump(result, json_file, indent=4)

    print("Результаты сохранены в first_task.json")
    print("Нормализованная матрица сохранена в first_task.npy")


first_task("32/first_task.npy")
