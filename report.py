import pandas as pd
import matplotlib.pyplot as plt
from utils import create_dir


class Report:
    
    def __init__(self, events, eventsa, fraud, frauda, direction, direction_img):
        self.events = events
        self.eventsa = eventsa
        self.fraud = fraud
        self.frauda = frauda
        self.direction = direction
        self.direction_img = direction_img
        
        self.join = {}
        self.partners = {}
        self.totaltable = {}
        self.totaltable_df = {}
        self.pivottable = {}
        
        self.os_list = ['Android','iOS']
        self.event_list = ['ftd1','ftd2','std1', 'dep300', 'conversionStep_[registration]_success']

        create_dir(direction)

    def save_users(self):
        for os in self.os_list:
            for event in self.event_list:
                self.join[os, event] = pd.merge(self.events[os, event], self.fraud[os, event], how="left", on=["AppsFlyer_ID"])

                # Сохраняем объединенную таблицу Пользователей
                path = self.direction + f'/{os}_{event}.xlsx'
                self.join[os, event].to_excel(path, index = False)
                print(path)
        
    def save_partners(self):
        for os in self.os_list:
            path = self.direction + f'/{os}-Partners.xlsx'
            
            self.partners[os] = self.eventsa[os]['Partner']
            self.partners[os] = self.partners[os].drop_duplicates()
            self.partners[os] = self.partners[os].dropna()
            self.partners[os].to_excel(path, index = False)
            
    def save_user_events(self):
        for os in self.os_list:
            for event in self.event_list:

                self.totaltable[os, event] = []
                for x in self.partners[os]:
                    
                    #сохраняем всех пользователей
                    a1 = self.join[os, event][self.join[os, event]['Partner'] == x]
                    a1.to_excel(self.direction + f'/{os}_{x}_{event}_All_UniqueUsers.xlsx', index = False)

                    #сохраняем фродовых пользователей
                    a2 = a1[a1['Fraud_Reason'].notna()]
                    a2.to_excel(self.direction + f'/{os}_{x}_{event}_Fraud_UniqueUsers.xlsx', index = False)

                    #сохраняем фродовых пользователей (органика)
                    a2o = a1[a1['Rejected_Reason_Value'] == 'organic']
                    a2o.to_excel(self.direction + f'/{os}_{x}_{event}_OrganicFraud_UniqueUsers.xlsx', index = False)

                    #сохраняем нефродовых пользователей 
                    a3 = a1[a1['Fraud_Reason'].isna()]
                    a3.to_excel(self.direction + f'/{os}_{x}_{event}_Clean_UniqueUsers.xlsx', index = False)

                    #Сохраняем данные в сводную таблицу
                    self.totaltable[os, event].append({'Partner': x,
                                                       'Fraud':  len(a2),
                                                       'Organic Fraud':  len(a2o),
                                                       'Non-Fraud': len(a3),
                                                       'Total': len(a1),
                                                       'Fraud %': round(len(a2) / (len(a1) + 0.0001),4)*100,
                                                       'Organic Fraud %': round(len(a2o) / (len(a1) + 0.0001),4)*100})
                
    def save_partner_result(self):
        for os in self.os_list:
            for event in self.event_list:
                self.totaltable_df[os, event] = pd.DataFrame(self.totaltable[os, event])
                self.totaltable_df[os, event] = self.totaltable_df[os,event].sort_values(by=['Total'], ascending=False)
                self.totaltable_df[os, event].to_excel(self.direction + f'/TotalTable_{os}_{event}.xlsx', index = False)
                print(f'{os} {event}')
                
    def save_plots(self):
        create_dir(self.direction_img)
        for os in self.os_list:
            for event in self.event_list:
                ax = plt.gca()

                plt.title(os + ': ' + event)

                self.totaltable_df[os, event].plot(
                    kind='bar', rot=15, x='Partner', y=['Fraud','Non-Fraud'], ax=ax, figsize=(15,5))
                self.totaltable_df[os, event].plot(
                    kind='line', rot=15, x='Partner', y='Fraud %', ax=ax, figsize=(15,5), color='gray', secondary_y=True)

                plt.savefig(self.direction_img + f'/{os}-{event}.png')
                plt.close()
                
    def save_pivot(self):
        for os in self.os_list:
            self.pivottable[os] = pd.DataFrame(columns=['Partner'])
            for event in self.event_list:
                self.pivottable[os] = pd.merge(self.pivottable[os], self.totaltable_df[os, event], on="Partner", how="outer")

            self.pivottable[os].columns = [
                "Partner", 
                "FTD1 Fraud",   "FTD1 Organic Fraud",   "FTD1 Clean",   "FTD1 Total",   "FTD1 Fraud %",   "FTD1 Organic Fraud %", 
                "FTD2 Fraud",   "FTD2 Organic Fraud",   "FTD2 Clean",   "FTD2 Total",   "FTD2 Fraud %",   "FTD2 Organic Fraud %", 
                "STD1 Fraud",   "STD1 Organic Fraud",   "STD1 Clean",   "STD1 Total",   "STD1 Fraud %",   "STD1 Organic Fraud %", 
                "Dep300 Fraud", "Dep300 Organic Fraud", "Dep300 Clean", "Dep300 Total", "Dep300 Fraud %", "Dep300 Organic Fraud %",
                "REG Fraud",    "REG Organic Fraud",    "REG Clean",    "REG Total",    "REG Fraud %",    "REG Organic Fraud %"
            ]

            self.pivottable[os].set_index('Partner',inplace=True, drop=True)
            self.pivottable[os].loc["Total"] = self.pivottable[os].sum()
            self.pivottable[os].to_excel(self.direction + f'/PivotTable_{os}.xlsx', index = True)
