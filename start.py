from handlers.speed_handler import main as speed
from handlers.geopoint_mapper import main as map_geocode
from handlers.concat_dtp_speed import main as dtp_speed_concat
from handlers.concat_dtp_weather import main as dtp_weather_concat

import warnings
warnings.filterwarnings("ignore")

if __name__ == '__main__':
    speed() #speed gpx to csv
    map_geocode() #geolocate dtp streets and speed streets
    dtp_speed_concat() #mapping dtp
    dtp_weather_concat() #mapping weather