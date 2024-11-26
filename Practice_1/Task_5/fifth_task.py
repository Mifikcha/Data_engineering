from bs4 import BeautifulSoup
import pandas as pd

# Загрузка HTML-файл
file_path = "C:/Users/bezzz/OneDrive/Desktop/All/Acheba/Hope/Data_engineering/Practice_1/Task_5/fifth_task.html"
with open(file_path, "r", encoding="utf-8") as file:
    soup = BeautifulSoup(file, "html.parser")

# Поиск таблицы
table = soup.find("table", {"id": "product-table"})

# Извлечение заголовков
headers = [header.text.strip() for header in table.find("thead").find_all("th")]

# Извлечение строк
rows = []
for row in table.find("tbody").find_all("tr"):
    cells = [cell.text.strip() for cell in row.find_all("td")]
    rows.append(cells)

# Создание DataFrame
df = pd.DataFrame(rows, columns=headers)

# Сохранение в CSV-файл
output_path = "C:/Users/bezzz/OneDrive/Desktop/All/Acheba/Hope/Data_engineering/Practice_1/Task_5/fifth_task.csv"
df.to_csv(output_path, index=False, encoding="utf-8-sig")

