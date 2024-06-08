import pandas
import pandas as pd
from alive_progress import alive_bar
import ast
from math import radians, sin, cos, sqrt, atan2
import math
from dateutil import parser
import warnings
warnings.filterwarnings("ignore")

streets = ['улица Ленина', 'улица Серышева','улица Ленинградская','улица Синельникова',
    'Уссурийский бульвар', 'улица Муравьева-Амурского', 'Амурский бульвар',
    'улица Карла Маркса', 'Матвеевское  шоссе','улица Гамарника','улица Шевченко',
    'улица Тургенева', 'улица Комсомольская', 'улица Истомина','улица Калинина',
    'улица Фрунзе','улица Запарина','улица Дзержинского','улица Волочаевская',
    'улица Шеронова', 'улица Гоголя', 'улица Пушкина','улица Ким Ю Чена',
    'улица Дикопольцева','улица Московская','улица улица Большая','улица Промышленная',
    'улица Выборгская','Чернореченское шоссе','улица Воронежская','улица Шелеста',
    'Студенческий переулок','Ленинградский переулок']


"""Определяем две ближайшие координаты"""
"""
road - это массив координат
"""
def find_closest_points(house, road):
    closest_points = [None, None]
    min_distances = [float('inf'), float('inf')]
    road1 = road.copy()
    for point in road1:
        distance = math.sqrt((point[0] - house[0])**2 + (point[1] - house[1])**2)
        if distance < min_distances[1]:
            if distance < min_distances[0]:
                min_distances[1] = min_distances[0]
                min_distances[0] = distance
                closest_points[1] = closest_points[0]
                closest_points[0] = point
            else:
                min_distances[1] = distance
                closest_points[1] = point

    return closest_points

def main():
    df_dtp = pd.read_csv('./files/csv/dtp_with_streets.csv', sep=';')
    df_speed = pd.read_csv('./files/csv/speed_with_streets.csv', sep=';')
    sorted_streets = sorted(streets)
    df_dtp = df_dtp[df_dtp["street_geo"].isin(sorted_streets)]
    df_speed = df_speed[df_speed["street_geo"].isin(sorted_streets)]
    ndexport = []
    column_names = []
    column_names.append(df_dtp.columns.tolist())
    column_names[0].append('datetime')
    column_names[0].append('max_speed')
    with alive_bar(len(df_dtp), force_tty=True, title='Concat DTP Speed') as bar:
        for index, row_dtp in df_dtp.iterrows():
            try:
                df_speed_filtered = df_speed[df_speed["street_geo"] == row_dtp[
                    "street_geo"]].copy()  # По образцу (экземпляру) улицы ДТП сортируем улицы скорости
                df = df_speed_filtered.apply(lambda row: [row['latitude'], row['longitude']],
                                             axis=1)  # Соединяем широту и долготу из разных столбцов в один
                ndarray = df.values
                house = ast.literal_eval(row_dtp['house'])  # Определяем координаты дома. Это нужно для метода.
                df_speed_filtered[
                    'latitude_longitude'] = df  # Доавляем в датасет  df_speed_filtered новый столбец с соединенными данными о широте и долготе
                # Определяем координаты ближайших точекна дороге. Это нужно для метода и от этих координат будет выполняться поиск скоростных координат в радиусе 50 метров.
                closest_points = find_closest_points(house, ndarray.tolist())
                """ Находим все точки df (скоростные точки) в радиусе 50 метров от  точки closest_points[0] (это 
                ближайшая точка на дороге, мы ее находим по ранее созданному методу, потому не вышло сделать сразу все в залданном радиусе)"""
                def distance(lat1, lon1, lat2, lon2):
                    R = 6371000  # радиус Земли в метрах
                    phi1 = radians(lat1)
                    phi2 = radians(lat2)
                    delta_phi = radians(lat2 - lat1)
                    delta_lambda = radians(lon2 - lon1)
                    a = sin(delta_phi / 2) * sin(delta_phi / 2) + cos(phi1) * cos(phi2) * sin(delta_lambda / 2) * sin(
                        delta_lambda / 2)
                    c = 2 * atan2(sqrt(a), sqrt(1 - a))
                    return R * c
                closest_point_lat, closest_point_lon = closest_points[0]
                radius = 50  # радиус в метрах
                points_within_radius = []
                for point in df:
                    point_lat, point_lon = point
                    dist = distance(closest_point_lat, closest_point_lon, point_lat, point_lon)
                    if dist <= radius:
                        points_within_radius.append(point)
                """ Сортируем даннные speed по полученным координатным точкам. Таким образом мы сужаем поле обработки используя фильтрацию"""
                # print(points_within_radius.info())
                sorted_streets_speed = sorted(
                    points_within_radius)  # Сортируем данные скорости по отобранным координатам. Обозначаем список сортировки
                df_speed_filtered1 = df_speed_filtered[
                    df_speed_filtered["latitude_longitude"].isin(sorted_streets_speed)]  # Сортируем Весь датафрейм
                row_dtp['datetime'] = parser.parse(row_dtp['Дата'])
                def find_nearest_month(target_month, months):
                    nearest_month = sorted(months, key=lambda x: min(abs(x - target_month), 12 - abs(x - target_month)))[:1]
                    return nearest_month
                df_speed_filtered1['datetime'] = df_speed_filtered1['date_time'].apply(lambda x: parser.parse(x))
                available_months = df_speed_filtered1['datetime'].dt.strftime('%m').unique().astype(int)
                nearest_month = find_nearest_month(row_dtp['datetime'].month, available_months)
                ndf = df_speed_filtered1[df_speed_filtered1['datetime'].dt.month == nearest_month.pop()][:10]
                max_speed = ndf['speed'].max()
                row_dtp['max_speed'] = max_speed
                ndexport.append(row_dtp.tolist())
            except Exception as err:
                print(index)
                print(err)
            bar()
    expdf = pandas.DataFrame(ndexport, columns=column_names)
    expdf.to_csv('files/csv/dtp_speed_concatenated.csv', index=False, sep=';', encoding="UTF-8-SIG")
