import requests
from bs4 import BeautifulSoup
import json

# Функция для загрузки страниц
def fetch_page(url):
    response = requests.get(url)
    response.raise_for_status()
    return response.text

# Парсинг каталога
def parse_catalog(catalog_url):
    html = fetch_page(catalog_url)
    soup = BeautifulSoup(html, 'html.parser')
    
    
    links = []
    for a_tag in soup.select('a[href*="/shop/product/"]'):
        links.append('https://tolyatti.mir-sveta.com' + a_tag['href'])
    
    return links

# Парсинг страницы объекта
def parse_object(object_url):
    html = fetch_page(object_url)
    soup = BeautifulSoup(html, 'html.parser')
    flux = ''
    color = ''
    
    for item in soup.find_all('div', class_='item'):
        if item.find('div', class_='label').text.strip() == 'Световой поток (Лм)':
            flux = item.find('div', class_='value').text.strip()
        if item.find('div', class_='label').text.strip() == 'Цвет арматуры (основания)':
            color = item.find('div', class_='value').text.strip()

    
    data = {
        'title': soup.find('h1').text.strip(),
        'price': soup.find('text', id ='chs11').text.strip(),
        'luminousFlux': float(flux),
        'color': color,
        
    }
    
    return data

# Процесс парсинга
catalog_url = "https://tolyatti.mir-sveta.com/shop/category/svyetilniki-dlya-doma/"
object_links = parse_catalog(catalog_url)
# print(object_links)

data_objects = []
for link in object_links[:10]:  # Ограничиваем до 10 объектов
    object_data = parse_object(link)
    data_objects.append(object_data)
# print(data_objects)

# Сохраняем данные в JSON
with open('objects.json', 'w', encoding='utf-8') as f:
    json.dump(data_objects, f, ensure_ascii=False, indent=4)


import pandas as pd

# Загрузка данных
data = pd.read_json('objects.json')

# Сортировка по полю
sorted_data = data.sort_values(by='price', ascending=False)
sorted_data.to_json('sorted_objects.json', orient='records', force_ascii=False, indent=4)

# Фильтрация
filtered_data = data[data['title'].str.contains('Торшер')]
filtered_data.to_json('filtered_objects.json', orient='records', force_ascii=False, indent=4)

# Статистика по числовому полю
rating_stats = {
    'mean': data['luminousFlux'].mean(),
    'median': data['luminousFlux'].median(),
    'min': data['luminousFlux'].min(),
    'max': data['luminousFlux'].max(),
}
print(rating_stats)

# Частотный анализ текстового поля
from collections import Counter
word_counts = Counter(" ".join(data['color']).split())
print(word_counts.most_common(10))
