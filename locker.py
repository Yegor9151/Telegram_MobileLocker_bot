# Custom libs
from process import Processor
from report import Report
from utils import create_dir, open_file

from google.cloud import bigquery
from datetime import date, timedelta

import re
import os
import json
import shutil
import requests
import pandas as pd
import warnings
warnings.filterwarnings('ignore')


class Locker_bot:

    def __init__(self, period: tuple[str, str]|list[str], tg_token: str, af_token: str, bq_token: str) -> None:
        """Main class for create a telegram bot that generate report for mobile closeing.
        Need tokens: telegram, appsflyer and bigquery

        Params:
            :period - date for report from - to
            :tg_token - path to token for telegram - name.txt
            :af_token - path to token for appsflyer - name.json
            :bq_token - path to token for bigquery - name.json"""

        self.__PERIOD = period
        self.__period_cut = period[0], str(date(*map(int, period[1].split('-'))) + timedelta(days=1))

        self.__TG_TOKEN = open_file(tg_token)
        self.__AF_TOKEN = json.loads(open_file(af_token))['appsflyer_api_key']
        self.__BigClient = bigquery.Client.from_service_account_json(bq_token)

        self.__PATHS_TO_SOURCES = {
            'ru android fraud': f'./data/{self.__PERIOD[1]}/ru/ru.ligastavok.android-mob2_fraud-post-inapps_{self.__PERIOD[0]}_{self.__PERIOD[1]}.csv',
            'ru ios fraud': f'./data/{self.__PERIOD[1]}/ru/id1065803457_fraud-post-inapps_{self.__PERIOD[0]}_{self.__PERIOD[1]}.csv',
            'all android fraud': f'./data/{self.__PERIOD[1]}/all/ru.ligastavok.android-mob2_fraud-post-inapps_{self.__PERIOD[0]}_{self.__PERIOD[1]}.csv',
            'all ios fraud': f'./data/{self.__PERIOD[1]}/all/id1065803457_fraud-post-inapps_{self.__PERIOD[0]}_{self.__PERIOD[1]}.csv'
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

    def get_source_dataframes(self) -> dict:
        """Return: dict with source dataframes"""
        return self.__SOURCE_DATAFRAMES

    def get_result_dataframes(self) -> dict:
        """Return: dict with result dataframes"""
        return self.__RESULT_DATAFRAMES

    def create_dirs(self) -> None:
        """create root directions for sources and results"""

        create_dir('./data')
        create_dir(f'./data/{self.__PERIOD[1]}')
        create_dir(f'./data/{self.__PERIOD[1]}/all')
        create_dir(f'./data/{self.__PERIOD[1]}/ru')

        create_dir('./result')

    def read_evetns(self) -> dict:
        """read bigquery to collect events data
        
        Return: dict with source dataframes"""

        def events_query(platform: str, ru: bool=True) -> str:
            """assemble query

            Params:
                :ru - regions if True then 'ru' else 'all'
                :android - platforf if True then 'android' else 'ios'
            Return: sql query"""

            query = open_file('./events.sql')
            query = re.sub('<period0>', self.__period_cut[0], query)
            query = re.sub('<period1>', self.__period_cut[1], query)
            query = re.sub('<platform>', platform, query)
            if ru:
                query += 'AND Country_Code = "RU"'

            return query

        PLATFORM = ('ruligastavokandroid_mob2', 'id1065803457')
        self.__SOURCE_DATAFRAMES['ru android event'] = self.__BigClient.query(events_query(PLATFORM[0], ru=True)).result().to_dataframe().drop_duplicates()
        self.__SOURCE_DATAFRAMES['ru ios event'] = self.__BigClient.query(events_query(PLATFORM[1], ru=True)).result().to_dataframe().drop_duplicates()
        self.__SOURCE_DATAFRAMES['all android event'] = self.__BigClient.query(events_query(PLATFORM[0], ru=False)).result().to_dataframe().drop_duplicates()
        self.__SOURCE_DATAFRAMES['all ios event'] = self.__BigClient.query(events_query(PLATFORM[1], ru=False)).result().to_dataframe().drop_duplicates()

        return self.__SOURCE_DATAFRAMES

    def read_frauds(self) -> dict:
        """read appsflyer to collect fraud data
        
        Return: dict with source dataframes"""

        def read_appsflyer(platform: str, region: str, rows : int=10_000) -> None:
            """collect data about fraud from appsflyer
            
            Params:
                :platform - platform name in appsflyer
                :region - part of path for to saver date
                :rows - how much rows to reserve"""

            URL = f"https://hq.appsflyer.com/export/{platform}/fraud-post-inapps/v5?" + \
                f"api_token={self.__AF_TOKEN}&" + \
                f"from={self.__PERIOD[0]}&" + \
                f"to={self.__PERIOD[1]}&" + \
                "timezone=Europe%2fMoscow&" + \
                "event_name=conversionStep_[registration]_success,ftd1,ftd2,dep300,std1&" + \
                f"additional_fields=match_type,rejected_reason,rejected_reason_value,detection_date,fraud_reason&" + \
                f"maximum_rows={rows}&"

            if region == 'ru':
                URL += 'country_code=ru&'

            resp = requests.get(URL)
            path = f'./data/{self.__PERIOD[1]}/{region}/{platform}_fraud-post-inapps_{self.__PERIOD[0]}_{self.__PERIOD[1]}.csv'

            open_file(path, 'w', text=resp.text)

        def drop_cols(df) -> pd.DataFrame:
            """drop extra columns for speed up and size down
            
            Params:
                :df - source dataframe
            Return: clean dataframe"""

            to_drop = [
                'Language', 'Original URL', 'Is Receipt Validated', 'Retargeting Conversion Type', 'Reengagement Window', 
                'Event Revenue Currency', 'App Name', 'User Agent', 'HTTP Referrer', 'SDK Version', 'OS Version', 
                'State', 'IP', 'Postal Code', 'Customer User ID', 'Android ID', 'Advertising ID', 'IDFV', 'IDFA', 'App Version', 
                'Event Value', 'Event Revenue', 'Platform', 'Operator', 'Region', 'City', 'Is Retargeting', 'Is Primary Attribution', 
                'Keywords', 'Sub Site ID', 'Sub Param 1', 'Sub Param 2', 'Sub Param 3', 'Sub Param 4',
                'Sub Param 5', 'WIFI', 'Device Type', 'Bundle ID'
            ]

            df = df.drop(to_drop, axis=1)
            for col in ('Fraud Sub Reason', 'Store Product Page', 'Event Revenue RUB', 'Event Revenue USD', 'Device Model', 'Device Category'):
                if col in df.columns:
                    df = df.drop([col], axis=1)

            return df

        PLATFORM = ('ru.ligastavok.android-mob2', 'id1065803457')
        read_appsflyer(PLATFORM[0], 'ru')
        read_appsflyer(PLATFORM[0], 'all')
        read_appsflyer(PLATFORM[1], 'ru')
        read_appsflyer(PLATFORM[1], 'all')

        self.__SOURCE_DATAFRAMES['ru android fraud'] = drop_cols(pd.read_csv(self.__PATHS_TO_SOURCES['ru android fraud']))
        self.__SOURCE_DATAFRAMES['ru ios fraud'] = drop_cols(pd.read_csv(self.__PATHS_TO_SOURCES['ru ios fraud']))
        self.__SOURCE_DATAFRAMES['all android fraud'] = drop_cols(pd.read_csv(self.__PATHS_TO_SOURCES['all android fraud']))
        self.__SOURCE_DATAFRAMES['all ios fraud'] = drop_cols(pd.read_csv(self.__PATHS_TO_SOURCES['all ios fraud']))

        return self.__SOURCE_DATAFRAMES

    def process(self) -> dict:
        """process data - split on new & old attribute, split on events, drop duplicates
        
        Return: dict with result dataframes"""

        for df in self.__SOURCE_DATAFRAMES.values():
            df.columns = df.columns.str.replace(' ', '_')

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
            print('events:', proc.get_dataframe()[0]['Android', 'ftd1'].shape)
            # print('fraud:', proc.get_dataframe()[0]['Android', 'ftd1'].shape)

        return self.__RESULT_DATAFRAMES

    def report(self):
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

    def split_reports(self):
        for direction in os.listdir('./result/'):
            src_path = f'./result/{direction}'

            # collect all files in mb
            files_size = {file: os.stat(f'{src_path}/{file}').st_size / (1024 ** 2) for file in os.listdir(src_path)}

            # split on parts
            dir_size = sum(files_size.values())
            parts = int(dir_size // 50) + 1

            # gen parts
            if parts > 1:
                for p in range(parts):
                    os.mkdir(f'{src_path}_part{p+1}')

                part = 1
                all_size = 0
                for file, size in files_size.items():

                    all_size += size
                    if all_size > 50:
                        part += 1
                        all_size = 0

                    os.replace(f'{src_path}/{file}', f'{src_path}_part{part}/{file}')

                os.rmdir(src_path)

    def archive(self):
        for direct in os.listdir('./result/'):
            path = f'./result/{direct}'
            shutil.make_archive(path, 'zip', path)
            print(path)

    def telegpush(self, chat_id):
        requests.post(f'https://api.telegram.org/bot{self.__TG_TOKEN}/sendMessage?chat_id={chat_id}&text=Загружаю файлы...')

        for file in os.listdir('./result/'):
            if '.zip' in file:
                path = './result/' + file
                file = {'document': open(path, 'rb')}
                requests.post(f'https://api.telegram.org/bot{self.__TG_TOKEN}/sendDocument?chat_id={chat_id}', files=file)

        requests.post(f'https://api.telegram.org/bot{self.__TG_TOKEN}/sendMessage?chat_id={chat_id}&text=Готово')