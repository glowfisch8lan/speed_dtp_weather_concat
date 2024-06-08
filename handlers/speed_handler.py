# Импортируем библиотеки NumPy, lxml (для работы с XML), Pandas, os, re (для работы с регулярными выражениями) и dateutil.parser.
import logging
import numpy as np
from alive_progress import alive_bar
from lxml import etree
import pandas as pd
import os
import re
# Путь к папке, содержащей файлы GPX
folder_path = 'files/gpx'
# Конечный файл для записи
output_file_path = 'files/csv/speed_handled.csv'
df_combined = pd.DataFrame()
file_list = os.listdir(folder_path)


def haversine_distance(lat1, lon1, lat2, lon2):
    """
    Calculate the great-circle distance between two points on the Earth given their latitudes and longitudes in degrees.

    Args:
        lat1 (float): Latitude of the first point in degrees.
        lon1 (float): Longitude of the first point in degrees.
        lat2 (float): Latitude of the second point in degrees.
        lon2 (float): Longitude of the second point in degrees.

    Returns:
        float: The distance between the two points in kilometers.
    """

    R = 6371
    lat1_rad = np.radians(lat1)
    lon1_rad = np.radians(lon1)
    lat2_rad = np.radians(lat2)
    lon2_rad = np.radians(lon2)
    delta_lat = lat2_rad - lat1_rad
    delta_lon = lon2_rad - lon1_rad
    a = (np.sin(delta_lat / 2) ** 2 + np.cos(lat1_rad) *
         np.cos(lat2_rad) * np.sin(delta_lon / 2) ** 2)
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
    distance = R * c
    return distance

def main():
    """
       Process files in the 'file' folder with GPX extension, extract relevant information,
       calculate distance, time difference, and speed, and merge the data into a combined DataFrame.
    """

    global df_combined
    with alive_bar(len(file_list), force_tty=True, title='Speed GPX to CSV') as bar:
        for file_name in file_list:
            if file_name.endswith('.gpx'):
                file_path = os.path.join(folder_path, file_name)
                try:
                    tree = etree.parse(file_path)
                    root = tree.getroot()
                    namespace = root.nsmap
                    tracks = []
                    for trk in root.iterfind('.//{%s}trk' % namespace[None]):
                        track = {}
                        track['latitude'] = []
                        track['longitude'] = []
                        track['elevation'] = []
                        track['time'] = []
                        for trkpt in trk.iterfind('.//{%s}trkpt' % namespace[None]):
                            track['latitude'].append(float(trkpt.get('lat')))
                            track['longitude'].append(float(trkpt.get('lon')))
                            elevation_element = trkpt.find('.//{%s}ele' % namespace[None])
                            if elevation_element is not None:
                                track['elevation'].append(float(elevation_element.text))
                            else:
                                track['elevation'].append(None)
                            time_element = trkpt.find('.//{%s}time' % namespace[None])
                            if time_element is not None:
                                time_element_text = time_element.text
                                try:
                                    regex_pattern = r'\b(20[01][0-9])\b'
                                    replaced_text = re.sub(regex_pattern, r'2022', time_element_text)
                                    track['time'].append(pd.to_datetime(replaced_text))
                                except Exception:
                                    track['time'].append(None)
                            else:
                                track['time'].append(None)
                        tracks.append(track)
                    if len(tracks) > 0:
                        df = pd.DataFrame(tracks[0])
                        df['date_time'] = df['time']  # Добавление столбца с датой и временем
                        df['distance'] = haversine_distance(df['latitude'].shift(-1), df['longitude'].shift(-1),
                                                            df['latitude'], df['longitude'])
                        df['time_diff'] = (df['time'] - df['time'].shift()).shift(-1)
                        df['time_diff'] = df['time_diff'].dt.total_seconds() / 3600
                        df['speed'] = df['distance'] / df['time_diff']
                        df_combined = pd.concat([df_combined, df[
                            ['latitude', 'longitude', 'elevation', 'time', 'date_time', 'distance', 'time_diff',
                             'speed']]], ignore_index=True)
                except Exception as e:
                    logging.error(f"Ошибка обработки файла {file_name}: {e}")
            bar()
    df_combined.to_csv(output_file_path, index=False)
    print("Обработка файлов GPX и запись в один файл завершена.")
