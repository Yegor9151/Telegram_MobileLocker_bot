from datetime import date, timedelta
import pandas as pd


class Processor:
    
    def __init__(self, 
                 event_android, event_ios, 
                 flaud_android, fraud_ios, 
                 period: list):
        
        self.events = {}
        self.fraud = {}
        
        self.eventsa = {'Android': event_android, 'iOS': event_ios}
        self.frauda = {'Android': flaud_android, 'iOS': fraud_ios}
        
        self.period = period
        year, month, day = map(int, period[1].split('-'))
        self.__period_cut = period[0], str(date(year, month, day) + timedelta(days=1))
        
        self.os_list = ['Android', 'iOS']
        self.event_list = ['ftd1','ftd2','std1', 'dep300', 'conversionStep_[registration]_success']

    def get_dataframe(self):
        return self.events, self.eventsa, self.fraud, self.frauda
        
    def __partners_fill(self, os):
        
        rows = pd.isna(self.eventsa[os]['Partner']) & self.eventsa[os]['Media Source'].str.endswith('_int')
        self.eventsa[os].loc[rows, 'Partner'] = self.eventsa[os].loc[rows, 'Media Source']
        
    def old_attr(self):
        for os in self.os_list:
            self.eventsa[os]['Install Time'] = pd.to_datetime(self.eventsa[os]['Install Time'])
            self.eventsa[os] = self.eventsa[os][(self.eventsa[os]['Install Time'] >= f'{self.__period_cut[0]}') &
                                                (self.eventsa[os]['Install Time'] < f'{self.__period_cut[1]}')]
            self.__partners_fill(os)
    
    def new_attr(self):
        for os in self.os_list:
            self.eventsa[os]['Install Time'] = pd.to_datetime(self.eventsa[os]['Install Time'])
            self.eventsa[os]['Event Time'] = pd.to_datetime(self.eventsa[os]['Event Time'])

            # Оставляем только те инсталлы, которые в пределах 30 дней от FTD
            installs = self.eventsa[os]['Install Time'] + pd.DateOffset(days=30) > self.eventsa[os]['Event Time']
            self.eventsa[os] = self.eventsa[os][installs]

            # Оставляем только те инсталлы, которые в пределах 30 дней от FTD
            self.eventsa[os] = self.eventsa[os][(self.eventsa[os]['Event Time'] >=  self.__period_cut[0]) & 
                                                (self.eventsa[os]['Event Time'] <  self.__period_cut[1])]
            self.__partners_fill(os)
            
    def select_columns(self):
        cols_to_keep = ['AppsFlyer ID', 'Attributed Touch Time', 'Install Time', 'Fraud Reason', 'Fraud Sub Reason', 
                        'Event Name', 'Detection Date', 'Rejected Reason Value']
        
        self.frauda['Android'] = self.frauda['Android'].loc[:, cols_to_keep]
        self.frauda['iOS'] = self.frauda['iOS'].loc[:, cols_to_keep]
            
    def event_split(self):
        for os in self.os_list:
            for event in self.event_list:
                self.events[os, event] = self.eventsa[os][self.eventsa[os]['Event Name'] == event]
                self.fraud[os, event] = self.frauda[os][self.frauda[os]['Event Name'] == event]
                
    def drop_duplicates(self):
        for os in self.os_list:
            for event in self.event_list:
                self.events[os, event]['UnDup'] = self.events[os, event]['AppsFlyer ID'] + self.events[os, event]['Event Name']
                self.events[os, event] = self.events[os, event].drop_duplicates(subset=['UnDup'], keep='first')
                self.fraud[os, event]['UnDup'] = self.fraud[os, event]['AppsFlyer ID'] + self.fraud[os, event]['Event Name']
                self.fraud[os, event] = self.fraud[os, event].drop_duplicates(subset=['UnDup'], keep='first')