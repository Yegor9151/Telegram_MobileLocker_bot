# Custom libs
from process import Processor
from report import Report
from utils import create_dir, read_file

from google.cloud import bigquery
from datetime import date, timedelta

import re
import os
import shutil
import requests
import pandas as pd
import warnings
warnings.filterwarnings('ignore')


class Locker_bot:

    def __init__(self, period: tuple[str, str]|list[str], token) -> None:

        self.__PERIOD = period
        self.__period_cut = period[0], str(date(*map(int, period[1].split('-'))) + timedelta(days=1))

        self.__TOKEN = read_file(token)
        self.__BigClient = bigquery.Client.from_service_account_json('../keys/bq_token.json')

        self.__PATHS_TO_SOURCES = {
            'ru android fraud': f'./data/{self.__PERIOD[1]}/ru/ru.ligastavok.android-mob2_fraud-post-inapps_{self.__PERIOD[0]}_{self.__PERIOD[1]}_Europe_Moscow.csv',
            'ru ios fraud': f'./data/{self.__PERIOD[1]}/ru/id1065803457_fraud-post-inapps_{self.__PERIOD[0]}_{self.__PERIOD[1]}_UTC.csv',
            'all android fraud': f'./data/{self.__PERIOD[1]}/all/ru.ligastavok.android-mob2_fraud-post-inapps_{self.__PERIOD[0]}_{self.__PERIOD[1]}_Europe_Moscow.csv',
            'all ios fraud': f'./data/{self.__PERIOD[1]}/all/id1065803457_fraud-post-inapps_{self.__PERIOD[0]}_{self.__PERIOD[1]}_UTC.csv'
        }
        self.__SOURCE_DATAFRAMES = {}
        self.__RESULT_DATAFRAMES = {}

        self.__PATH_TO_REPORTS = {
            'ru report new': f'./result/RU_reports_{self.__PERIOD[0]}_{self.__PERIOD[1]}_NEW',
            'ru report old': f'./result/RU_reports_{self.__PERIOD[0]}_{self.__PERIOD[1]}_OLD',
            'ru imgs new': f'./result/RU_imgs_{self.__PERIOD[0]}_{self.__PERIOD[1]}_NEW',
            'ru imgs old': f'./result/RU_imgs_{self.__PERIOD[0]}_{self.__PERIOD[1]}_OLD',

            'all report new': f'./result/ALL_reports_{self.__PERIOD[0]}_{self.__PERIOD[1]}_NEW',
            'all report old': f'./result/ALL_reports_{self.__PERIOD[0]}_{self.__PERIOD[1]}_OLD',
            'all imgs new': f'./result/ALL_imgs_{self.__PERIOD[0]}_{self.__PERIOD[1]}_NEW',
            'all imgs old': f'./result/ALL_imgs_{self.__PERIOD[0]}_{self.__PERIOD[1]}_OLD'
        }

    def get_source_dataframes(self):
        return self.__SOURCE_DATAFRAMES

    def get_result_dataframes(self):
        return self.__RESULT_DATAFRAMES

    def __events_query(self, ru=True, android=True):
        pratform = {
            'android': 'ruligastavokandroid_mob2', 
            'ios': 'id1065803457'
        }

        query = read_file('./events.sql')
        query = re.sub('<period0>', self.__period_cut[0], query)
        query = re.sub('<period1>', self.__period_cut[1], query)
        query = re.sub('<platform>', pratform['android' if android else 'ios'], query)
        if ru:
            query += 'AND Country_Code = "RU"'

        return query

    def read_dataframes(self):

        def drop_cols(df):
            to_drop = [
                'Language', 'Original URL', 'Event Revenue RUB', 
                'Retargeting Conversion Type', 'Reengagement Window', 
                'Event Revenue Currency', 'App Name', 'User Agent', 'HTTP Referrer', 
                'Device Model', 'SDK Version', 'OS Version', 'State', 'IP', 
                'Postal Code', 'Customer User ID', 'Android ID', 'Advertising ID', 
                'IDFV', 'IDFA', 'App Version', 'Event Value', 'Event Revenue', 
                'Platform', 'Operator', 'Region', 'City', 'Is Retargeting', 
                'Is Primary Attribution', 'Device Category']

            df = df.drop(to_drop, axis=1)
            for col in ('Fraud Sub Reason', 'Store Product Page'):
                if col in df.columns:
                    df = df.drop([col], axis=1)

            return df

        # Remember sources dataframes
        self.__SOURCE_DATAFRAMES['ru android event'] = self.__BigClient.query(self.__events_query(ru=True, android=True)).result().to_dataframe().drop_duplicates()
        self.__SOURCE_DATAFRAMES['ru ios event'] = self.__BigClient.query(self.__events_query(ru=True, android=False)).result().to_dataframe().drop_duplicates()
        self.__SOURCE_DATAFRAMES['all android event'] = self.__BigClient.query(self.__events_query(ru=False, android=True)).result().to_dataframe().drop_duplicates()
        self.__SOURCE_DATAFRAMES['all ios event'] = self.__BigClient.query(self.__events_query(ru=False, android=False)).result().to_dataframe().drop_duplicates()

        self.__SOURCE_DATAFRAMES['ru android fraud'] = drop_cols(pd.read_csv(self.__PATHS_TO_SOURCES['ru android fraud']))
        self.__SOURCE_DATAFRAMES['ru ios fraud'] = drop_cols(pd.read_csv(self.__PATHS_TO_SOURCES['ru ios fraud']))
        self.__SOURCE_DATAFRAMES['all android fraud'] = drop_cols(pd.read_csv(self.__PATHS_TO_SOURCES['all android fraud']))
        self.__SOURCE_DATAFRAMES['all ios fraud'] = drop_cols(pd.read_csv(self.__PATHS_TO_SOURCES['all ios fraud']))

        # Timely
        for df in self.__SOURCE_DATAFRAMES.values():
            df.columns = df.columns.str.replace(' ', '_')

        print('dataframes readed\n')

        return self.__SOURCE_DATAFRAMES

    def process(self):

        dfs_ru = (
            self.__SOURCE_DATAFRAMES['ru android event'], self.__SOURCE_DATAFRAMES['ru ios event'],
            self.__SOURCE_DATAFRAMES['ru android fraud'], self.__SOURCE_DATAFRAMES['ru ios fraud'],
        )
        dfs_all = (
            self.__SOURCE_DATAFRAMES['all android event'], self.__SOURCE_DATAFRAMES['all ios event'],
            self.__SOURCE_DATAFRAMES['all android fraud'], self.__SOURCE_DATAFRAMES['all ios fraud'],
        )

        self.__RESULT_DATAFRAMES['new_ru'] = Processor(*dfs_ru, self.__period_cut)
        self.__RESULT_DATAFRAMES['old_ru'] = Processor(*dfs_ru, self.__period_cut)
        self.__RESULT_DATAFRAMES['new_all'] = Processor(*dfs_all, self.__period_cut)
        self.__RESULT_DATAFRAMES['old_all'] = Processor(*dfs_all, self.__period_cut)

        self.__RESULT_DATAFRAMES['new_ru'].new_attr()
        self.__RESULT_DATAFRAMES['old_ru'].old_attr()
        self.__RESULT_DATAFRAMES['new_all'].new_attr()
        self.__RESULT_DATAFRAMES['old_all'].old_attr()

        for proc in (self.__RESULT_DATAFRAMES.values()):
            proc.select_columns()
            proc.event_split()
            proc.drop_duplicates()

        for proc in (self.__RESULT_DATAFRAMES.values()):
            print(proc.get_dataframe()[0]['Android', 'ftd1'].shape)

        print('data processed\n')

        return self.__RESULT_DATAFRAMES

    def report(self):
        create_dir('./result')

        report_new_ru = Report(*self.__RESULT_DATAFRAMES['new_ru'].get_dataframe(), self.__PATH_TO_REPORTS['ru report new'], self.__PATH_TO_REPORTS['ru imgs new'])
        report_old_ru = Report(*self.__RESULT_DATAFRAMES['old_ru'].get_dataframe(),  self.__PATH_TO_REPORTS['ru report old'], self.__PATH_TO_REPORTS['ru imgs old'])
        report_new_all = Report(*self.__RESULT_DATAFRAMES['new_all'].get_dataframe(), self.__PATH_TO_REPORTS['all report new'], self.__PATH_TO_REPORTS['all imgs new'])
        report_old_all = Report(*self.__RESULT_DATAFRAMES['old_all'].get_dataframe(), self.__PATH_TO_REPORTS['all report old'], self.__PATH_TO_REPORTS['all imgs old'])

        for report in (report_new_ru, report_old_ru, report_new_all, report_old_all):
            report.save_users()
            report.save_partners()
            report.save_user_events()
            report.save_partner_result()
            report.save_plots()
            report.save_pivot()
        
        print('report collected\n')

    def archive(self):
        for direct in os.listdir('./result/'):
            path = f'./result/{direct}'
            shutil.make_archive(path, 'zip', path)
            print(path)

        print('directions archived\n')

    def telegpush(self, chat_id):
        requests.post(f'https://api.telegram.org/bot{self.__TOKEN}/sendMessage?chat_id={chat_id}&text=Загружаю файлы...')

        for file in os.listdir('./result/'):
            if '.zip' in file:
                path = './result/' + file
                print(path, os.stat(path).st_size, 'mb')

                file = {'document': open(path, 'rb')}
                requests.post(f'https://api.telegram.org/bot{self.__TOKEN}/sendDocument?chat_id={chat_id}', files=file)

        requests.post(f'https://api.telegram.org/bot{self.__TOKEN}/sendMessage?chat_id={chat_id}&text=Готово')

        print('report pushed')