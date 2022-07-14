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
        
        self.__period_cut = period
        
        self.os_list = ['Android', 'iOS']
        self.event_list = ['ftd1','ftd2','std1', 'dep300', 'conversionStep_[registration]_success']

    def get_dataframe(self):
        return self.events, self.eventsa, self.fraud, self.frauda
        
    def __partners_fill(self, os):
        
        rows = pd.isna(self.eventsa[os]['Partner']) & self.eventsa[os]['Media_Source'].str.endswith('_int')
        self.eventsa[os].loc[rows, 'Partner'] = self.eventsa[os].loc[rows, 'Media_Source']
        
    def old_attr(self):
        for os in self.os_list:
            self.eventsa[os]['Install_Time'] = pd.to_datetime(self.eventsa[os]['Install_Time'])
            self.eventsa[os] = self.eventsa[os][(self.eventsa[os]['Install_Time'] >= f'{self.__period_cut[0]}') &
                                                (self.eventsa[os]['Install_Time'] < f'{self.__period_cut[1]}')]
            self.__partners_fill(os)
    
    def new_attr(self):
        for os in self.os_list:
            self.eventsa[os]['Install_Time'] = pd.to_datetime(self.eventsa[os]['Install_Time'])
            self.eventsa[os]['Event_Time'] = pd.to_datetime(self.eventsa[os]['Event_Time'])

            # Оставляем только те инсталлы, которые в пределах 30 дней от FTD
            installs = self.eventsa[os]['Install_Time'] + pd.DateOffset(days=30) > self.eventsa[os]['Event_Time']
            self.eventsa[os] = self.eventsa[os][installs]

            # Оставляем только те инсталлы, которые в пределах 30 дней от FTD
            self.eventsa[os] = self.eventsa[os][(self.eventsa[os]['Event_Time'] >=  self.__period_cut[0]) & 
                                                (self.eventsa[os]['Event_Time'] <  self.__period_cut[1])]
            self.__partners_fill(os)
            
    def select_columns(self):
        cols_to_keep = ['AppsFlyer_ID', 'Attributed_Touch_Time', 'Install_Time', 'Fraud_Reason', 
                        'Event_Name', 'Detection_Date', 'Rejected_Reason_Value']
        
        self.frauda['Android'] = self.frauda['Android'].loc[:, cols_to_keep]
        self.frauda['iOS'] = self.frauda['iOS'].loc[:, cols_to_keep]
            
    def event_split(self):
        for os in self.os_list:
            for event in self.event_list:
                self.events[os, event] = self.eventsa[os][self.eventsa[os]['Event_Name'] == event]
                self.fraud[os, event] = self.frauda[os][self.frauda[os]['Event_Name'] == event]
                
    def drop_duplicates(self):
        for os in self.os_list:
            for event in self.event_list:
                self.events[os, event]['UnDup'] = self.events[os, event]['AppsFlyer_ID'] + self.events[os, event]['Event_Name']
                self.events[os, event] = self.events[os, event].drop_duplicates(subset=['UnDup'], keep='first')
                self.events[os, event] = self.events[os, event].drop(['UnDup'], axis=1)

                self.fraud[os, event]['UnDup'] = self.fraud[os, event]['AppsFlyer_ID'] + self.fraud[os, event]['Event_Name']
                self.fraud[os, event] = self.fraud[os, event].drop_duplicates(subset=['UnDup'], keep='first')
                self.fraud[os, event] = self.fraud[os, event].drop(['UnDup'], axis=1)