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


# write func
def report_geo_google_sheets(app_name, app_id, date_from, date_to):
    ##app_name = 'Metronome_One' # exmple app_name
    ##app_id = 'id1428578368' #example app_id

    # name report
    report_type = 'geo_by_date_report'
    # read token
    with open('token.json') as f:
        token = json.load(f)

    params = {
        'api_token': token['token'],
        'from': date_from, # date start 2020-01-01
        'to': date_to # date end 2020-08-31
    }
    # url api appsflyer
    request_url = 'https://hq.appsflyer.com/export/{}/{}/v5'.format(app_id, report_type)

    res = requests.request('GET', request_url, params=params)
    # send request
    if res.status_code != 200:
        if res.status_code == 404:
            print('There is a problem with the request URL. Make sure that it is correct')
        else:
            print('There was a problem retrieving data: ', res.text)
    else:
        f = open('{}-{}-{}.csv'.format(app_name, app_id, report_type), 'w', newline='', encoding="utf-8")
        f.write(res.text)
        f.close()

    # choosing file
    for file in os.listdir():
        if file.endswith("geo_by_date_report.csv") and file.startswith(app_name):
            path = os.path.join(file)
    # read file
    df_geo_by_date = pd.read_csv(path)
    # selected columns
    df_installs = df_geo_by_date[['Date', 'Country', 'Installs']]

    # loading full name countries
    url = 'https://pkgstore.datahub.io/core/country-codes/country-codes_csv/data/3b9fd39bdadd7edd7f7dcee708f47e1b/country-codes_csv.csv'
    df_country = pd.read_csv(url)
    df_country = df_country[['ISO3166-1-Alpha-2', 'official_name_en']]
    df_country = df_country.rename(columns={'ISO3166-1-Alpha-2': 'ISO3166',
                                            'official_name_en': 'full_name'})

    # merge tables
    df = df_installs.merge(df_country, left_on='Country', right_on='ISO3166')

    # selected columns
    df = df[['Date', 'full_name', 'Installs']]

    # lower name columns
    df.columns = df.columns.str.lower()
    # rename column
    df = df.rename(columns={'full_name': 'country'})

    # grouping, sorting, pivot
    df_group = df.groupby(['date', 'country'], as_index=False) \
        .agg({'installs': 'sum'}) \
        .sort_values('installs', ascending=False)
    # change type
    df_group['date'] = pd.to_datetime(df_group['date'])
    # create columns months
    df_group['month'] = df_group.date.dt.month
    # grouping by month, countries
    df_group_month = df_group.groupby(['month', 'country'], as_index=False) \
        .agg({'installs': 'sum'})
    # pivot
    df_pivot = df_group_month.pivot_table(values='installs', index='country', columns='month', fill_value=0) \
        .round(2)
    # sorting by min
    df_sort = df_pivot.sort_values(by=df_pivot.columns.min(), ascending=False)
    # create dictionary for month
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
    # sorting by months
    df_sort = df_sort.rename(columns=month)
    # df_sort = df_sort.style.highlight_max(axis='index')

    # loading data in googlesheets
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

# Using function
date_from = '2020-01-01'
date_to = '2020-08-31'
# Tuner
report_geo_google_sheets('Tuner_One', 'id1435060008', date_from, date_to)
report_geo_google_sheets('Tuner_guitar_ukulele_bass', 'id1507738194', date_from, date_to)
report_geo_google_sheets('Tuner_Pro', 'id1509460145', date_from, date_to)
report_geo_google_sheets('Tuner_ONE_PRO', 'id1479411134', date_from, date_to)
# Metronome
report_geo_google_sheets('Metronome_One', 'id1428578368', date_from, date_to)
report_geo_google_sheets('Metronome PRO', 'id1509276291', date_from, date_to)
# Dj
report_geo_google_sheets('DJ_One', 'id1514562131', date_from, date_to)
# Translator
report_geo_google_sheets('Translator_One', 'id1498930242', date_from, date_to)
