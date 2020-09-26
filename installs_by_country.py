import requests
import pandas as pd
from datetime import datetime
import os
import json

%matplotlib inline
import seaborn as sns
import matplotlib.pyplot as plt

from datetime import date, datetime, timedelta

import gspread
from df2gspread import df2gspread as d2g
from oauth2client.service_account import ServiceAccountCredentials


# получаем данные по странам из appsflyer
def report_geo_google_sheets(app_name, app_id):
    ##app_name = 'Metronome_One'
    ##app_id = 'id1428578368'
    report_type = 'geo_by_date_report'

    with open('token.json') as f:
        token = json.load(f)

    params = {
        'api_token': token['token'],
        'from': '2020-01-01',
        'to': '2020-08-31'
    }

    request_url = 'https://hq.appsflyer.com/export/{}/{}/v5'.format(app_id, report_type)

    res = requests.request('GET', request_url, params=params)

    if res.status_code != 200:
        if res.status_code == 404:
            print('There is a problem with the request URL. Make sure that it is correct')
        else:
            print('There was a problem retrieving data: ', res.text)
    else:
        f = open('{}-{}-{}.csv'.format(app_name, app_id, report_type), 'w', newline='', encoding="utf-8")
        f.write(res.text)
        f.close()

    for file in os.listdir():
        if file.endswith("geo_by_date_report.csv") and file.startswith(app_name):
            path = os.path.join(file)

    df_geo_by_date = pd.read_csv(path)

    df_installs = df_geo_by_date[['Date', 'Country', 'Installs']]

    # для дальнейшего анализа загрузим коды стран
    url = 'https://pkgstore.datahub.io/core/country-codes/country-codes_csv/data/3b9fd39bdadd7edd7f7dcee708f47e1b/country-codes_csv.csv'
    df_country = pd.read_csv(url)
    df_country = df_country[['ISO3166-1-Alpha-2', 'official_name_en']]
    df_country = df_country.rename(columns={'ISO3166-1-Alpha-2': 'ISO3166',
                                            'official_name_en': 'full_name'})

    # объединим таблицы
    df = df_installs.merge(df_country, left_on='Country', right_on='ISO3166')

    # выбирае нужные столбцы
    df = df[['Date', 'full_name', 'Installs']]

    # название прописными и переименовываем столбец
    df.columns = df.columns.str.lower()
    df = df.rename(columns={'full_name': 'country'})

    # группируем, сортируем и делаем pivot
    df_group = df.groupby(['date', 'country'], as_index=False) \
        .agg({'installs': 'sum'}) \
        .sort_values('installs', ascending=False)

    df_group['date'] = pd.to_datetime(df_group['date'])

    df_group['month'] = df_group.date.dt.month

    df_group_month = df_group.groupby(['month', 'country'], as_index=False) \
        .agg({'installs': 'sum'})

    df_pivot = df_group_month.pivot_table(values='installs', index='country', columns='month', fill_value=0) \
        .round(2)

    df_sort = df_pivot.sort_values(by=df_pivot.columns.min(), ascending=False)

    month = {1: 'Jan',
             2: 'Feb',
             3: 'Mar',
             4: 'Apr',
             5: 'May',
             6: 'Jun',
             7: 'Jul',
             8: 'Aug',
             9: 'Sep',
             10: 'Oct',
             11: 'Now',
             12: 'Dec'}

    df_sort = df_sort.rename(columns=month)
    # df_sort = df_sort.style.highlight_max(axis='index')

    scope = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/drive']

    my_mail = 'sinitsyn.ig@gmail.com'
    path_to_credentials = 'apps-installs-418cb7cb9d1a.json'

    # Authorization
    credentials = ServiceAccountCredentials.from_json_keyfile_name(path_to_credentials, scope)
    gs = gspread.authorize(credentials)

    # create table
    table_name = 'app_installs_by_country'

    table = gs.open(table_name)
    table.share(my_mail, perm_type='user', role='writer')

    sheet_name = app_name
    d2g.upload(df_sort,
               table_name,
               sheet_name,
               credentials=credentials,
               row_names=True)

# Tuner
report_geo_google_sheets('Tuner_One', 'id1435060008')
report_geo_google_sheets('Tuner_guitar_ukulele_bass', 'id1507738194')
report_geo_google_sheets('Tuner_Pro', 'id1509460145')
report_geo_google_sheets('Tuner_ONE_PRO', 'id1479411134')
# Metronome
report_geo_google_sheets('Metronome_One', 'id1428578368')
report_geo_google_sheets('Metronome PRO', 'id1509276291')
# Dj
report_geo_google_sheets('DJ_One', 'id1514562131')
# Translator
report_geo_google_sheets('Translator_One', 'id1498930242')
