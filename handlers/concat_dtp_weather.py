import pandas as pd
from alive_progress import alive_bar
from dateutil import parser
def main():
        """Стыкуем данные о ДТП с 2016 по 2021 с погодными условиями с 2016 по 2021 по ближайшей дате"""
        df_dtp = pd.read_csv('files/csv/dtp_speed_concatenated.csv', sep=';')
        df_weather = pd.read_csv('files/csv/weather.csv', sep=';')
        df_twilight = pd.read_csv('files/csv/twilight.csv', sep=';')
        # Преобразование столбцов "Дата" и "Время" в формат datetime
        df_dtp["Дата и время"] = pd.to_datetime(df_dtp["Дата"] + ' ' + df_dtp["Время"], format='%d.%m.%Y %H:%M')
        df_weather["Дата и время"] = pd.to_datetime(df_weather["Дата и время"], format='%d.%m.%Y %H:%M')
        df_dtp = df_dtp.sort_values("Дата и время")

        df_weather = df_weather.sort_values("Дата и время")
        df_weather.rename(columns={"Дата и время": "Дата и время_weather"}, inplace=True)
        merged_df = pd.merge_asof(df_dtp, df_weather, left_on="Дата и время", right_on="Дата и время_weather",
                                  direction='nearest')

        with alive_bar(len(merged_df), force_tty=True, title='Concat DTP Weather') as bar:
            def _binary_twilight(row):
                dt_date = parser.parse(row['Дата'])
                dt_time = parser.parse(row['Время'])
                df_twilight['ДАТА'] = pd.to_datetime(df_twilight['ДАТА'], format='%Y-%m-%d')
                df_period = df_twilight.loc[
                    (df_twilight['ДАТА'].dt.day == dt_date.day) & (df_twilight['ДАТА'].dt.month == dt_date.month)]
                df_period['Нач ут сум'] = df_period['Нач ут сум'].apply(lambda x: parser.parse(x))
                df_period['Кон ут сум'] = df_period['Кон ут сум'].apply(lambda x: parser.parse(x))
                df_period['Нач веч сум'] = df_period['Нач веч сум'].apply(lambda x: parser.parse(x))
                df_period['Кон веч сум'] = df_period['Кон веч сум'].apply(lambda x: parser.parse(x))
                bar()
                try:
                    if parser.parse('00:00:00') < dt_time < df_period['Нач ут сум'].iloc[0]:
                        return 1
                    if df_period['Нач ут сум'].iloc[0] < dt_time < df_period['Кон ут сум'].iloc[0]:
                        return 2
                    if df_period['Кон ут сум'].iloc[0] < dt_time < df_period['Нач веч сум'].iloc[0]:
                        return 3
                    if df_period['Нач веч сум'].iloc[0] < dt_time < df_period['Кон веч сум'].iloc[0]:
                        return 4
                    if df_period['Кон веч сум'].iloc[0] < dt_time < parser.parse('23:59:59'):
                        return 1
                except Exception as error:
                    print(error)
                    return 0
            merged_df["Часть суток"] = merged_df.apply(_binary_twilight, axis=1)
            merged_df.to_csv('files/csv/dtp_with_weather.csv', index=False, sep=';', encoding="UTF-8-SIG")

