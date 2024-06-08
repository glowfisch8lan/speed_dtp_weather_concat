"""

Once the average speed with coordinates is obtained, it needs to be linked to the location of the traffic accident.
Since the geo positions of the speed points and the location of the traffic accident are different,
the connection is established through the street name where the accident occurred.
"""

import os
from time import sleep
import pandas as pd
from datetime import timedelta
from alive_progress import alive_bar
from dateutil.tz import tzutc
from dateutil.parser import parse
import hashlib
import math
from geopy.geocoders import Nominatim
filepath_speed = './files/csv/speed_handled.csv'
filepath_dtp = './files/csv/dtp.csv'
geolocator = Nominatim(user_agent="Mozilla/5.0 (platform; rv:geckoversion) "
                                  "Gecko/geckotrail Firefox/firefoxversion",
                       timeout=100)

def _get_street(lat, lon):
    latHash = hashlib.md5(repr(lat).encode()).hexdigest()
    lonHash = hashlib.md5(repr(lon).encode()).hexdigest()
    path = f'./files/geopoints/{latHash}{lonHash}.txt'
    if os.path.isfile(path):
        f = open(path, "r", encoding='utf-8')
        street = f.read()
        location = 'cache'
    else:
        location = geolocator.reverse((lat, lon))
        address = location.raw['address']
        street = address.get('road', '')
        location = 'internet'
        print('Download from Internet...')
        with open(path, 'w', encoding='utf-8') as f:
            f.write(street)
            f.close()
    return street

def _date_utc(s):
    """
       Parse a date string (s) into a datetime object in UTC time zone.

       Args:
           s (str): A string representing date and time.

       Returns:
           datetime: A datetime object in UTC time zone.
       """
    return parse(s, tzinfos=tzutc)
def _parse_datetime(s):
    """
      Attempt to parse a date string (s) into a datetime object with fuzzy matching (fuzzy=True).
      Return None if parsing fails.

      Args:
          s (str): A string representing date and time.

      Returns:
          datetime or None: A datetime object if parsing is successful, None if parsing fails.
      """
    try:
        return parse(s, fuzzy=True)
    except ValueError:
        return None


def main():
    with alive_bar(3, force_tty=True, title='Geopoint Mapper') as bar:
        df_dtp = pd.read_csv(filepath_dtp, sep=';')
        df_speed = pd.read_csv(filepath_speed, sep=',', date_parser=_date_utc)

        df_speed['date_time'] = df_speed['date_time'].apply(_parse_datetime) + timedelta(hours=10)
        df_dtp[['latitude2', 'longitude2']] = df_dtp['closest_point1'].str.split(',', expand=True)
        df_dtp['latitude2'] = df_dtp['latitude2'].str.replace('[', '').astype(float)
        df_dtp['longitude2'] = df_dtp['longitude2'].str.replace(']', '').astype(float)

        streets_speed = df_speed.apply(lambda row: _get_street(row['latitude'], row['longitude']) if not (
                    math.isnan(row['latitude']) or math.isnan(row['longitude'])) else '', axis=1)
        bar()
        streets_dtp = df_dtp.apply(lambda row: _get_street(row['latitude2'], row['longitude2']) if not (
                    math.isnan(row['latitude2']) or math.isnan(row['longitude2'])) else '', axis=1)
        bar()
        df_dtp['street_geo'] = streets_dtp
        df_speed['street_geo'] = streets_speed

        df_dtp.to_csv('./files/csv/dtp_with_streets.csv', index=False, sep=';', encoding="UTF-8-SIG")
        df_speed.to_csv('./files/csv/speed_with_streets.csv', index=False, sep=';', encoding="UTF-8-SIG")
        bar()

