import pandas as pd

# Считываем исходный TXT файл
input_file = "C:/Users/bezzz/OneDrive/Desktop/All/Acheba/Hope/Data_engineering/Practice_1/Task_4/fourth_task.txt"
output_file_results = "results.txt"
output_file_modified = "modified.csv"

# Чтение файла
data = pd.read_csv(input_file, sep=",")  # Если разделитель - запятая

if "description" in data.columns:
    data = data.drop(columns=["description"])

average_quantity = data["quantity"].mean()
max_quantity = data["quantity"].max()
min_rating = data["rating"].min()

filtered_data = data[data["quantity"] < 947]

with open(output_file_results, "w") as results_file:
    results_file.write(f"{average_quantity}\n")
    results_file.write(f"{max_quantity}\n")
    results_file.write(f"{min_rating}\n")

filtered_data.to_csv(output_file_modified, index=False)

