# -*- coding: utf-8 -*-
"""
Created on Thu Aug 17 16:44:59 2023

@author: SVC-GASQuant2-Prod
"""

# -*- coding: utf-8 -*-
"""
Created on Mon Jan  9 17:18:16 2023

@author: SVC-GASQuant2-Prod
"""






'''
V1 
    C1 previous view use last update; 
    table: 1. plant order under maintainence, next maintain, newest. 2.OP = any number back to 201, all zeros = AA
            3. add offline date, return date. 4. offline days. 5. MW use observered 2021

V2 add S.K
V3 add two tables, sum op zeros
V4 add reduction
V5 order by offline, same order table 2
V6 add m-1 schrdule
V7 - Saeul: Shin-Kori 3 - Saeul: Shin-Kori 4 To - Saeul: Saeul 1 - Saeul: Saeul 2
V9 save data in db







'''
import pandas as pd
import numpy as np
from dateutil.relativedelta import relativedelta
import datetime
import plotly.offline as py
import plotly.graph_objs as go
from pandas.tseries.offsets import BDay
import sys
sys.path.append('C:\\Users\SVC-GASQuant2-Prod\YWwebsite\Python Scripts\class') 
from DBtoDF import DBTOPD
pd.set_option('display.max_columns',20)
import sqlalchemy
import calendar
import matplotlib.pyplot as plt


class JP_Nuk_Outage():
    
    def get_data():
        
        #get jp nuk latest
        dfjpnuk = DBTOPD.get_JP_nuk()
        dfjpnuk = dfjpnuk.loc[:,['CurveId','ValueDate','Value']]
        
        dfjpnuk = pd.pivot_table(dfjpnuk, values='Value', index='ValueDate',columns='CurveId',aggfunc='sum')
       											 	
        name_dict = {14952:'Mihama Unit 3',
                     14953:'Takahama Unit 1',
                     14954:'Takahama Unit 2',
                     14955:'Takahama Unit 3',
                     14956:'Takahama Unit 4',
                     14959:'Ohi Unit 3',
                     14960:'Ohi Unit 4',
                     14946:'Genkai Unit 3',
                     14947:'Genkai Unit 4',
                     14948:'Sendai Unit 1',
                     14949:'Sendai Unit 2',
                     14945:'Ikata Unit 3',
                     14950:'Mihama Unit 1',
                     14951:'Mihama Unit 2',
                     14957:'Ohi Unit 1',
                     14958:'Ohi Unit 2',
                     }
        
        dfjpnuk.rename(columns=name_dict, inplace=True)
        dfjpnuk.loc[:'2022-06-07'].fillna(method='ffill', inplace=True)
        
        dfjpnuk['sum'] = dfjpnuk.sum(axis=1)
        #print(dfjpnuk)
        #print('here')
        #get jp nuk last month
        jpnuk_previous = DBTOPD.get_JP_nuk_previous()  #SettingWithCopyWarning
        dfjpnuk_previous = jpnuk_previous.loc[:,['CurveId','ValueDate','Value']]
        
        dfjpnuk_previous = pd.pivot_table(dfjpnuk_previous, values='Value', index='ValueDate',columns='CurveId',aggfunc='sum')
       											 	
        dfjpnuk_previous.rename(columns=name_dict, inplace=True)
        dfjpnuk_previous['sum'] = dfjpnuk_previous.sum(axis=1)
        #print(dfjpnuk_previous)
        
        
        #outage plan date
        dfJP_outage_date = DBTOPD.get_JP_nuk_outage_date()
        dfJP_outage_date['PlantName'] = dfJP_outage_date.loc[:,'PowerPlantName']+' '+dfJP_outage_date.loc[:,'UnitName']
        
        #lastest update
        #print(dfJP_outage_date.loc[dfJP_outage_date[dfJP_outage_date['PowerPlantName'] == 'Takahama power plant'].index])
        
        #last outage plan date
        dfJP_outage_date_previous = DBTOPD.get_JP_nuk_outage_date_previous()
        dfJP_outage_date_previous['PlantName'] = dfJP_outage_date_previous.loc[:,'PowerPlantName']+' '+dfJP_outage_date_previous.loc[:,'UnitName']
        
        
        dfjpex_outage = DBTOPD.get_JPEX_outage()
        dfjpex_outage['PlantName'] = dfjpex_outage.loc[:,'PowerPlantName']+' '+dfjpex_outage.loc[:,'UnitName']
        dfjpex_outage['timestamp'] = datetime.date.today()
        
        dfjpex_outage_comments = DBTOPD.get_JPEX_outage_comments()
        dfjpex_outage_comments['PlantName FP'] = dfjpex_outage_comments.loc[:,'PlantName']+' '+dfjpex_outage_comments.loc[:,'UnitName']
        #print(dfjpex_outage_comments)
        #print(dfjpex_outage)
        #print(dfJP_outage_date_previous)
        #print(dfjpnuk)
        
        return dfjpnuk, dfjpnuk_previous, dfJP_outage_date, dfJP_outage_date_previous,dfjpex_outage, dfjpex_outage_comments
    
    def jpex_data(dfjpex_outage, dfjpex_outage_comments, dfJP_outage_date):
        
        jpex_name_dict = {'Tokyo Kashiwazaki Kariwa Kashiwazaki Kariwa Unit 3':'Kashiwazaki Kariwa Unit 3',
                            'Tokyo Kashiwazaki Kariwa Kashiwazaki Kariwa Unit 4':'Kashiwazaki Kariwa Unit 4',
                            'Onagawa Nuclear Power Station Onagawa Nuclear Power Station Unit 2':'Onagawa Unit 2',
                            'Hamaoka Nuclear Power Station Unit 3':'Hamaoka Unit 3',
                            'Takahama power plant Unit 1':'Takahama Unit 1',
                            'Genkai Nuclear Power Station Unit 2':'Genkai Unit 2',
                            'Hokuriku Electric Power Shiga Nuclear Power Station Unit 2':'Hokuriku Electric Power Shiga Nuclear Power Station Unit 2',
                            'Tomari Power Station Unit 1':'Tomari Unit 1',
                            'Tokyo Kashiwazaki Kariwa Kashiwazaki Kariwa Unit 1':'Kashiwazaki Kariwa Unit 1',
                            'Tokyo Kashiwazaki Kariwa Kashiwazaki Kariwa Unit 7':'Kashiwazaki Kariwa Unit 7',
                            'Tomari Power Station Unit 2':'Tomari Unit 2',
                            'Onagawa Nuclear Power Station Onagawa Nuclear Power Station Unit 1':'Onagawa Unit 1',
                            'Onagawa Nuclear Power Station Onagawa Nuclear Power Station Unit 3':'Onagawa Unit 3',
                            'Hokuriku Electric Power Shiga Nuclear Power Station Unit 1':'Hokuriku Electric Power Shiga Nuclear Power Station Unit 1',
                            'Takahama power plant Unit 2':'Takahama Unit 2',
                            'Shikoku Electric Power Ikata Power Station Unit 2':'Ikata Unit 2',
                            'Tokyo Kashiwazaki Kariwa Kashiwazaki Kariwa Unit 5':'Kashiwazaki Kariwa Unit 5',
                            'Hamaoka Nuclear Power Station Unit 4':'Hamaoka Unit 4',
                            'Shimane Nuclear Power Station Unit 2 Alone':'Shimane Unit 2',
                            'Hamaoka Nuclear Power Station Unit 5':'Hamaoka Unit 5',
                            'Tokyo Kashiwazaki Kariwa Kashiwazaki Kariwa Unit 6':'Kashiwazaki Kariwa Unit 6',
                            'Tomari Power Station Unit 3':'Tomari Unit 3',
                            'Mihama Power Station Unit 3':'Mihama Unit 3',
                            'Genkai Nuclear Power Station Unit 3':'Genkai Unit 3',
                            'Sendai Nuclear Power Station Unit 2':'Sendai Unit 2',
                            'Takahama power plant Unit 3':'Takahama Unit 3',
                            'Ooi Power Plant Unit 4':'Ohi Unit 4',
                            'Genkai Nuclear Power Station Unit 4':'Genkai Unit 4',
                            'Takahama power plant Unit 4':'Takahama Unit 4',
                            'Ooi Power Plant Unit 3':'Ohi Unit 3',
                            'Shikoku Electric Power Ikata Power Station Unit 3':'Ikata Unit 3',
                            'Sendai Nuclear Power Station Unit 1':'Sendai Unit 1',
                            
                                    }
        dfjpex_outage_FP = dfjpex_outage.copy()
        
        dfjpex_outage_FP['PlantName FP'] = np.nan
        for i in dfjpex_outage_FP.index:
            if dfjpex_outage_FP.loc[i,'PlantName'] in jpex_name_dict:
                dfjpex_outage_FP.loc[i,'PlantName FP'] = jpex_name_dict[dfjpex_outage_FP.loc[i,'PlantName']]
        
            for j in dfjpex_outage_comments.index:
                if dfjpex_outage_comments.loc[j,'PlantName FP'] == dfjpex_outage_FP.loc[i,'PlantName FP']:
                    dfjpex_outage_FP.loc[i,'comments'] = dfjpex_outage_comments.loc[j,'Comments']
        #print(dfjpex_outage_comments)
        #print(dfjpex_outage_FP)
        dfjpex_outage_FP_todb = dfjpex_outage_FP.copy()
        dfjpex_outage_FP_todb['create date'] = datetime.datetime.now()
        
        #save jpex to db
        db_server_lng = "PRD-DB-SQL-211"
        db_name_lng = "LNG"
        sql_engine_lng = sqlalchemy.create_engine("mssql+pyodbc://" + db_server_lng + "/" + db_name_lng + "?trusted_connection=yes&driver=ODBC+Driver+13+for+SQL+Server")
        
        dfjpex_outage_FP_todb.to_sql(name='JPEX_Outage_hist', con=sql_engine_lng, index=False, if_exists='append', schema='ana')
        
        #rename reduction plant name
        reduction = dfJP_outage_date.loc[dfJP_outage_date[dfJP_outage_date['ReductionAmount']>0].index].copy()
        for i in reduction.index:
            reduction.loc[i,'PlantName'] = jpex_name_dict[reduction.loc[i,'PlantName']]
        
        dfJPEX_Outage_hist = DBTOPD("PRD-DB-SQL-211",'LNG', 'ana','JPEX_Outage_hist').sql_to_df()
        #print(dfJPEX_Outage_hist)
        
        
        
        return dfjpex_outage_FP, reduction, dfJPEX_Outage_hist
    
    def planned_data(dfjpex_outage_FP, dfJP_outage_date, dfJP_outage_date_previous,dfjpnuk, dfjpnuk_previous):
        
        today=datetime.date.today()
        
        #copy data from H:\Yuefeng\LNG Projects\nuclear outage\jp-npps-operation20220510_en.pdf
        jpplantdict = { 'Mihama Unit 3':780,
                        'Takahama Unit 1':826,
                        'Takahama Unit 2':826,
                        'Takahama Unit 3':870,
                        'Takahama Unit 4':870,
                        'Ohi Unit 3':1180,
                        'Ohi Unit 4':1180,
                        'Genkai Unit 3':1180,
                        'Genkai Unit 4':1180,
                        'Sendai Unit 1':890,
                        'Sendai Unit 2':890,
                        'Ikata Unit 3':890,
                        'Hamaoka Unit 3':1100,
                        'Hamaoka Unit 4':1137,
                        'Hamaoka Unit 5':1380,
                        'Kashiwazaki Kariwa Unit 6':1356,
                        'Kashiwazaki Kariwa Unit 7':1356,
                        'Onagawa Unit 2':825,
                        'Shika Unit 2':1358,
                        'Shimane Unit 2':820,
                        'Tohoku/Higashidori Unit 1':1385,
                        'Tokai Unit 2':1100,
                        'Tomari Unit 1':579,
                        'Tomari Unit 2':579,
                        'Tomari Unit 3':912,
                        'Tsuruga Unit 2':1160,
                        'Ohma':1383,
                        'Shimane Unit 3':1373, 
                        'Mihama Unit 1':340,
                        'Mihama Unit 2':500,
                        'Ohi Unit 1':1175,
                        'Ohi Unit 2':1175,
                        }
        
        name_dict = {
                    'Mihama Power Station Unit 3':	'Mihama Unit 3',
                    'Takahama power plant Unit 1':	'Takahama Unit 1',
                    'Takahama power plant Unit 2':	'Takahama Unit 2',
                    'Takahama power plant Unit 3':	'Takahama Unit 3',
                    'Takahama power plant Unit 4':	'Takahama Unit 4',
                    'Ooi Power Plant Unit 3'	:'Ohi Unit 3',
                    'Ooi Power Plant Unit 4'	:'Ohi Unit 4',
                    'Genkai Nuclear Power Station Unit 3':	'Genkai Unit 3',
                    'Genkai Nuclear Power Station Unit 4':	'Genkai Unit 4',
                    'Sendai Nuclear Power Station Unit 1':	'Sendai Unit 1',
                    'Sendai Nuclear Power Station Unit 2':	'Sendai Unit 2',
                    'Shikoku Electric Power Ikata Power Station Unit 3':	'Ikata Unit 3',
                    'Hamaoka Nuclear Power Station Unit 3':'Hamaoka Unit 3',
                    'Hamaoka Nuclear Power Station Unit 4':'Hamaoka Unit 4',
                    'Hamaoka Nuclear Power Station Unit 5':	'Hamaoka Unit 5',
                    'Tokyo Kashiwazaki Kariwa Kashiwazaki Kariwa Unit 6':	'Kashiwazaki Kariwa Unit 6',
                    'Tokyo Kashiwazaki Kariwa Kashiwazaki Kariwa Unit 7'	:'Kashiwazaki Kariwa Unit 7',
                    'Onagawa Nuclear Power Station Onagawa Nuclear Power Station Unit 2':	'Onagawa Unit 2',
                    'Shimane Nuclear Power Station Unit 2 Alone':	'Shimane Unit 2',
                    'Tomari Power Station Unit 1':	'Tomari Unit 1',
                    'Tomari Power Station Unit 2':	'Tomari Unit 2',
                    'Tomari Power Station Unit 3':	'Tomari Unit 3',
                    'Hokuriku Electric Power Shiga Nuclear Power Station Unit 1':'Hokuriku Unit 1',
                    'Hokuriku Electric Power Shiga Nuclear Power Station Unit 2':'Hokuriku Unit 2',
                    	
        }
    
        
        dfob = dfjpnuk.loc['2021-01-01':'2021-12-31'].max().to_dict()
        #dfob_previous = dfjpnuk_previous.loc['2021-01-01':'2021-12-31'].max().to_dict()
        #print(dfob)
        
        mw = jpplantdict.copy()
        mw.update(dfob)
        for i in mw.keys():
            #print(i, mw[i])
            if mw[i] == 0:
                 mw[i] = jpplantdict[i]#np.nan
             
        #OP or AA
        oplist = []
        dfop = pd.DataFrame(index = [today], columns = dfjpnuk.columns[:-1])
        for i in dfop.columns:
            if dfjpnuk[i].sum() == 0:
                dfop[i] = 'AA'
            else:
                dfop[i] = 'OP'
                oplist.append(i)
        #print(mw.oplist)
        
        
        #columns = dfJP_outage_date['PlantName'].drop_duplicates().tolist()
        
        return  jpplantdict, dfop, oplist
        
    
    def chart_data(dfjpnuk, jpplantdict, dfjpex_outage_FP, dfop, oplist, reduction):
        
        #print(dfjpex_outage_FP)
        
        today=datetime.date.today()
        #print(dfop)
        dfopcapa = pd.DataFrame(index = [today], columns = oplist)
        for i in oplist:
            dfopcapa[i] = jpplantdict[i]
        dfopcapa['capa'] =    dfopcapa.sum(axis=1) 
        #print(dfopcapa.loc[today, 'capa'])
        
        
        
        #chart 1 data
        dfchart1 = pd.DataFrame(index = pd.date_range(start = str(today.year)+'-01-01', end = str(today.year)+'-12-31'), columns=[str(today.year)])
        dfchart1['MAX OP Capacity'] = dfopcapa.loc[today, 'capa']
        dfchart1[str(today.year)] = dfjpnuk.loc[str(today.year)+'-01-01':str(today.year)+'-12-31','sum']
        dfchart1[str(today.year-1)] = dfjpnuk.loc[str(today.year-1)+'-01-01':str(today.year-1)+'-12-31','sum'].values
        dfchart1[str(today.year-2)] = dfjpnuk.loc[str(today.year-2)+'-01-01':str(today.year-2)+'-12-31','sum'].values#np.delete(dfjpnuk.loc['2020-01-01':'2020-12-31','sum'].values, 59)
        #print(dfchart1)
        
    
        
        
        #chart 2 data
        dfchart2 = dfjpnuk.iloc[:,:-1]
        
        #
        index = {'Mihama Unit 3':'OP',
                    'Takahama Unit 1':'AA',
                    'Takahama Unit 2':'AA',
                    'Takahama Unit 3':'OP',
                    'Takahama Unit 4':'OP',
                    'Ohi Unit 3':'OP',
                    'Ohi Unit 4':'OP',
                    'Genkai Unit 3':'OP',
                    'Genkai Unit 4':'OP',
                    'Sendai Unit 1':'OP',
                    'Sendai Unit 2':'OP',
                    'Ikata Unit 3':'OP',}
        #print(index.keys())
        dfchart2 = dfchart2[index.keys()]
        #print(dfchart2)
        """
         now available now
            'Hamaoka Unit 3':'AA',
            'Hamaoka Unit 4':'AA',
            'Hamaoka Unit 5':'AA',
            'Kashiwazaki Kariwa Unit 6':'AA',
            'Kashiwazaki Kariwa Unit 7':'AA',
            'Onagawa Unit 2':'AA',
            'Shika Unit 2':'AA',
            'Shimane Unit 2':'AA',
            'Tohoku/Higashidori Unit 1':'AA',
            'Tokai Unit 2':'AA',
            'Tomari Unit 1':'AA',
            'Tomari Unit 2':'AA',
            'Tomari Unit 3':'AA',
            'Tsuruga Unit 2':'AA',
            'Ohma':'AA',
            'Shimane Unit 3':'AA'
            }
        """
        
        dftable = pd.DataFrame(index = list(index.keys()), columns=['D','D-1','D-7','D-30','D-365']) #dfjpnuk.columns[:-1]
        for i in dftable.index:
            dftable.loc[i,'D'] = dfjpnuk.loc[str(today),i]
            dftable.loc[i,'D-1'] = dfjpnuk.loc[str(today-relativedelta(days=1)),i]
            dftable.loc[i,'D-7'] = dfjpnuk.loc[str(today-relativedelta(days=7)),i]
            dftable.loc[i,'D-30'] = dfjpnuk.loc[str(today-relativedelta(days=30)),i]
            dftable.loc[i,'D-365'] = dfjpnuk.loc[str(today-relativedelta(days=365)),i]
        #print(dftable)
        dftable_aa = pd.DataFrame(index = ['Hamaoka Unit 3',
                                            'Hamaoka Unit 4',
                                            'Hamaoka Unit 5',
                                            'Kashiwazaki Kariwa Unit 6',
                                            'Kashiwazaki Kariwa Unit 7',
                                            'Onagawa Unit 2',
                                            'Shika Unit 2',
                                            'Shimane Unit 2',
                                            'Tohoku/Higashidori Unit 1',
                                            'Tokai Unit 2',
                                            'Tomari Unit 1',
                                            'Tomari Unit 2',
                                            'Tomari Unit 3',
                                            'Tsuruga Unit 2',
                                            'Ohma',
                                            'Shimane Unit 3'], columns=['D','D-1','D-7','D-30','D-365'])
        
        
        dftable = pd.concat([dftable, dftable_aa])
        #dftable.fillna(0, inplace=True) #avoid some missing data
        dftable = dftable.astype('float')  
        
        dftabledate = pd.DataFrame([[str(today),str(today-relativedelta(days=1)), str(today-relativedelta(days=7)), str(today-relativedelta(days=30)), str(today-relativedelta(days=365))]], index = ['Date'], columns = dftable.columns)
    
                                
        #print(jpplantdict)
        dftable['operate'] = np.nan
        dftable['MW'] = np.nan
        dftable['Offline date'] = np.nan
        dftable['Return date'] = np.nan
        dftable['Current Days Offline'] = np.nan
        #print(dfjpex_outage_FP[dfjpex_outage_FP['PlantName FP']=='Mihama Unit 3'].index.tolist())
        #print(dfop)
        #print(dftable.index)
        for i in dftable.index:
            #print(dfjpex_outage_FP['StopDateAndTime'].loc[dfjpex_outage_FP[dfjpex_outage_FP['PlantName FP']==i].index.tolist()[0]])
            try:
                dftable.loc[i, 'operate'] = dfop[i].values
                dftable.loc[i, 'MW'] = jpplantdict[i]
                dftable.loc[i,'Offline date'] = dfjpex_outage_FP['StopDateAndTime'].loc[dfjpex_outage_FP[dfjpex_outage_FP['PlantName FP']==i].index.tolist()[0]].date()
                dftable.loc[i,'Return date'] = dfjpex_outage_FP['ScheduledRecoveryDate'].loc[dfjpex_outage_FP[dfjpex_outage_FP['PlantName FP']==i].index.tolist()[0]].date()
                dftable.loc[i,'Current Days Offline'] = dfjpex_outage_FP['CurrentDaysOffline'].loc[dfjpex_outage_FP[dfjpex_outage_FP['PlantName FP']==i].index.tolist()[0]]#.astype('int')
            except (IndexError, KeyError):
                #print(i, )
                dftable.loc[i, 'operate'] = 'AA'
                dftable.loc[i, 'MW'] = jpplantdict[i]
        
        #sort by the op and Offline date
        #dftable.loc[:,['Offline date']] = dftable.loc[:,['Offline date']].astype('datetime64[ns]')
        dftable.sort_values(by=['Offline date'], ascending=True, inplace=True)
        #dftable.sort_values(by=['operate'], ascending=False, inplace=True)
        index_order = dftable[dftable['operate']=='OP'].index.to_list() + dftable[dftable['operate']=='AA'].index.to_list()
        dftable = dftable.loc[index_order]
        #print(dftable)
        #sum zeros 
        column_sum = []
        for i in dftable.columns[0:5]:
            column_sum.append((dftable[i].loc[dftable[i].loc[dftable['operate']=='OP'].index]==0).sum(axis=0))
        #print(column_sum)
        dftabledate.loc['Sum of OP and Outage'] = column_sum  
        
        dftable =   dftabledate.append(dftable, ignore_index=False)  
        dftable.fillna(' ', inplace=True)
        #print(dftable)
        
        #Chart 1 planned data
        end_date = str((today+relativedelta(months=12)).year)+'-'+str((today+relativedelta(months=12)).month)+'-'+str(calendar.monthrange((today+relativedelta(months=12)).year, (today+relativedelta(months=12)).month)[1])
        dfplanned_data = pd.DataFrame(index = dftable.index[1:] , columns = pd.date_range(start = str(today.year)+'-01-01', end = end_date).date)
        for i in dfplanned_data.index:
            for j in dfplanned_data.columns:
                try:
                    if j <= dftable.loc[i,'Offline date'] or j >= dftable.loc[i,'Return date']:
                        dfplanned_data.loc[i,j] = dftable.loc[i, 'MW']
                    else:
                        dfplanned_data.loc[i,j] = np.nan
                except TypeError:
                    pass
        dfplanned_data = dfplanned_data.T
        
        dfplanned_data['Planned '+str(today.year)] = np.nan
        dfplanned_data.loc[today:pd.to_datetime(str(today.year)+'-12-31'),'Planned '+str(today.year)] = dfplanned_data.loc[today:pd.to_datetime(str(today.year)+'-12-31')].sum(axis=1)   
        dfplanned_data['Planned '+str(today.year+1)] = np.nan
        dfplanned_data.loc[today:pd.to_datetime(end_date),'Planned '+str(today.year+1)] = dfplanned_data.loc[today:pd.to_datetime(end_date)].sum(axis=1) 
        #dfplanned_data.loc['sum'] = dfplanned_data.sum()
        
        #chart 1 reduction 
        #dfreduction =  dfJP_outage_date.loc[dfJP_outage_date[dfJP_outage_date['ReductionAmount']>0].index]
        #print(dfplanned_data.index)
        #print(dfplanned_data.loc[today:,'Takahama Unit 3'])
        for i in reduction.index:
            if reduction.loc[i, 'StopDateAndTime'] >= pd.to_datetime(dfplanned_data.index[0]) and reduction.loc[i, 'ScheduledRecoveryDate'] <= pd.to_datetime(dfplanned_data.index[-1]):
                dfplanned_data.loc[reduction.loc[i, 'StopDateAndTime'].date():reduction.loc[i, 'ScheduledRecoveryDate'].date(), reduction.loc[i, 'PlantName']] = (reduction.loc[i,'AuthorizationOutput'] - reduction.loc[i,'ReductionAmount'])/1000
            elif reduction.loc[i, 'StopDateAndTime'] < pd.to_datetime(dfplanned_data.index[0]) and reduction.loc[i, 'ScheduledRecoveryDate'] <= pd.to_datetime(dfplanned_data.index[-1]):
                dfplanned_data.loc[dfplanned_data.index[0]:reduction.loc[i, 'ScheduledRecoveryDate'].date(), reduction.loc[i, 'PlantName']] = (reduction.loc[i,'AuthorizationOutput'] - reduction.loc[i,'ReductionAmount'])/1000
            elif reduction.loc[i, 'StopDateAndTime'] >= pd.to_datetime(dfplanned_data.index[0]) and reduction.loc[i, 'ScheduledRecoveryDate'] > pd.to_datetime(dfplanned_data.index[-1]):
                dfplanned_data.loc[reduction.loc[i, 'StopDateAndTime'].date():dfplanned_data.index[-1], reduction.loc[i, 'PlantName']] = (reduction.loc[i,'AuthorizationOutput'] - reduction.loc[i,'ReductionAmount'])/1000
            else:
                dfplanned_data.loc[dfplanned_data.index[0]:dfplanned_data.index[-1], reduction.loc[i, 'PlantName']] = (reduction.loc[i,'AuthorizationOutput'] - reduction.loc[i,'ReductionAmount'])/1000
        
        #print(dfplanned_data)
        
        #offline days bar chart data
        dfoffline = pd.DataFrame(index = [str(today.year-2),str(today.year-1),str(today.year)], columns = dfjpnuk.columns)
        dfoffline.loc[str(today.year-2)] = (dfjpnuk.loc[str(today.year-2)+'-01-01':str(today.year-2)+'-12-31'] == 0).sum(axis=0)
        dfoffline.loc[str(today.year-1)] = (dfjpnuk.loc[str(today.year-1)+'-01-01':str(today.year-1)+'-12-31'] == 0).sum(axis=0)
        dfoffline.loc[str(today.year)] = (dfjpnuk.loc[str(today.year)+'-01-01':str(today.year)+'-12-31'] == 0).sum(axis=0)
        for i in dfoffline.columns:
            if i not in dftable.index:
                dfoffline.drop(columns=[i], inplace=True)
        #print(dfoffline)
        
        index = pd.date_range(start = dfjpnuk.copy().resample('MS').sum().index[0], end = dfjpnuk.copy().resample('MS').sum().index[-1], freq='MS')
        
        dfjpnukM = pd.DataFrame(index = index, columns = dfjpnuk.columns[:-1])
        #dfjpnuk.copy().resample('MS').sum()
        for i in index:
            monthstart = datetime.datetime(i.year, i.month, 1)
            monthend = datetime.datetime(i.year, i.month, calendar.monthrange(i.year, i.month)[1])
            dfjpnukM.loc[i] =  (dfjpnuk.loc[monthstart:monthend,:'sum'] == 0).sum(axis=0)
        #print(dfjpnukM)
        
        dftableM = pd.DataFrame(index = dfoffline.columns)
        for i in dftableM.index:
            dftableM.loc[i, 'Ave. 3yrs offline days'] = int((dfjpnukM.loc[str(today.year-2)+'-01-01':str(today.year)+'-12-31', i].sum())/3)
            dftableM.loc[i, 'MW Capacity'] =  jpplantdict[i]
            dftableM.loc[i, str(today.year-2)+' offline days'] = int((dfjpnukM.loc[str(today.year-2)+'-01-01':str(today.year-2)+'-12-31', i].sum()))
            dftableM.loc[i, str(today.year-1)+' offline days'] = int((dfjpnukM.loc[str(today.year-1)+'-01-01':str(today.year-1)+'-12-31', i].sum()))
            dftableM.loc[i, str(today.year-0)+' offline days'] = int((dfjpnukM.loc[str(today.year-0)+'-01-01':str(today.year-0)+'-12-31', i].sum()))
            #dftableM.loc[i, 'offline time'] = [dfjpex_outage_FP.loc[dfjpex_outage_FP[dfjpex_outage_FP['PlantName FP'] == i].index, 'StopDateAndTime'],dfjpex_outage_FP.loc[dfjpex_outage_FP[dfjpex_outage_FP['PlantName FP'] == i].index, 'ScheduledRecoveryDate']]
           
        
        #print(dfjpnukM)
        dfjpnukM_new = dfjpnukM.copy()
        for i in dfjpnukM_new.columns:
            if dfjpnukM_new.loc[str(today.year-2)+'-01-01':str(today.year-2)+'-12-31', i].sum() == 365 or dfjpnukM_new.loc[str(today.year-2)+'-01-01':str(today.year-2)+'-12-31', i].sum() == 366:
                dfjpnukM_new.loc[str(today.year-2)+'-01-01':str(today.year-2)+'-12-31', i] = 0
            if dfjpnukM_new.loc[str(today.year-1)+'-01-01':str(today.year-1)+'-12-31', i].sum() == 365 or dfjpnukM_new.loc[str(today.year-1)+'-01-01':str(today.year-1)+'-12-31', i].sum() == 366:
                dfjpnukM_new.loc[str(today.year-1)+'-01-01':str(today.year-1)+'-12-31', i] = 0
            if dfjpnukM_new.loc[str(today.year-0)+'-01-01':str(today.year-0)+'-12-31', i].sum() == 365 or dfjpnukM_new.loc[str(today.year-0)+'-01-01':str(today.year-0)+'-12-31', i].sum() == 366:
                dfjpnukM_new.loc[str(today.year-0)+'-01-01':str(today.year-0)+'-12-31', i] = 0
        #print(dfjpnukM_new)
        dftable_outageM = pd.DataFrame(index = ['Jan', 'Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'], columns=[str(today.year-2),str(today.year-1),str(today.year)])
        
        for j in dftable_outageM.columns:
            
            Jan_list = []
            Feb_list = []
            Mar_list = []
            Apr_list = []
            May_list = []
            Jun_list = []
            Jul_list = []
            Aug_list = []
            Sep_list = []
            Oct_list = []
            Nov_list = []
            Dec_list = []
            
            for i in dfjpnukM_new.columns:
                try:
                    if dfjpnukM_new.loc[j+'-01-01',i] != 0: 
                        Jan_list.append(i+'('+str(dfjpnukM_new.loc[j+'-01-01',i])+')')
                    if dfjpnukM_new.loc[j+'-02-01',i] != 0:
                        Feb_list.append(i+'('+str(dfjpnukM_new.loc[j+'-02-01',i])+')')
                    if dfjpnukM_new.loc[j+'-03-01',i] != 0:
                        Mar_list.append(i+'('+str(dfjpnukM_new.loc[j+'-03-01',i])+')')
                    if dfjpnukM_new.loc[j+'-04-01',i] != 0:
                        Apr_list.append(i+'('+str(dfjpnukM_new.loc[j+'-04-01',i])+')')
                    if dfjpnukM_new.loc[j+'-05-01',i] != 0:
                        May_list.append(i+'('+str(dfjpnukM_new.loc[j+'-05-01',i])+')')
                    if dfjpnukM_new.loc[j+'-06-01',i] != 0:
                        Jun_list.append(i+'('+str(dfjpnukM_new.loc[j+'-06-01',i])+')')
                    if dfjpnukM_new.loc[j+'-07-01',i] != 0:
                        Jul_list.append(i+'('+str(dfjpnukM_new.loc[j+'-07-01',i])+')')
                    if dfjpnukM_new.loc[j+'-08-01',i] != 0:
                        Aug_list.append(i+'('+str(dfjpnukM_new.loc[j+'-08-01',i])+')')
                    if dfjpnukM_new.loc[j+'-09-01',i] != 0:
                        Sep_list.append(i+'('+str(dfjpnukM_new.loc[j+'-09-01',i])+')')
                    if dfjpnukM_new.loc[j+'-10-01',i] != 0:
                        Oct_list.append(i+'('+str(dfjpnukM_new.loc[j+'-10-01',i])+')')
                    if dfjpnukM_new.loc[j+'-11-01',i] != 0:
                        Nov_list.append(i+'('+str(dfjpnukM_new.loc[j+'-11-01',i])+')')
                    if dfjpnukM_new.loc[j+'-12-01',i] != 0:
                        Dec_list.append(i+'('+str(dfjpnukM_new.loc[j+'-12-01',i])+')')
                except KeyError:
                    pass
   
            dftable_outageM.loc['Jan',j] =','.join(k for k in Jan_list)  
            dftable_outageM.loc['Feb',j] =','.join(k for k in Feb_list) 
            dftable_outageM.loc['Mar',j] =','.join(k for k in Mar_list) 
            dftable_outageM.loc['Apr',j] =','.join(k for k in Apr_list) 
            dftable_outageM.loc['May',j] =','.join(k for k in May_list) 
            dftable_outageM.loc['Jun',j] =','.join(k for k in Jun_list) 
            dftable_outageM.loc['Jul',j] =','.join(k for k in Jul_list) 
            dftable_outageM.loc['Aug',j] =','.join(k for k in Aug_list) 
            dftable_outageM.loc['Sep',j] =','.join(k for k in Sep_list) 
            dftable_outageM.loc['Oct',j] =','.join(k for k in Oct_list) 
            dftable_outageM.loc['Nov',j] =','.join(k for k in Nov_list) 
            dftable_outageM.loc['Dec',j] =','.join(k for k in Dec_list) 
        
        #print(dftable_outageM)
        #print(dfjpnuk)
        
        dftable = sort_table_dates('Offline date', 'Return date', dftable, 'jp_table_1')

        return dfchart1, dfchart2, dftable, dfplanned_data, dfoffline, dftableM, dftable_outageM
        
    
    def chart_date_previous(dfJPEX_Outage_hist, jpplantdict):
        
        today=datetime.date.today()
        pre_date = today - relativedelta(months = 1)
        
        #find the M-1 hist data
        dfJPEX_Outage_hist.sort_values(by = 'create date', inplace = True)
        dfJPEX_Outage_hist.set_index('create date', inplace=True)
        dfJPEX_Outage_hist = dfJPEX_Outage_hist.loc[pre_date:]
        dfJPEX_Outage_hist = dfJPEX_Outage_hist.loc[dfJPEX_Outage_hist.index[0]]
        dfJPEX_Outage_hist.reset_index(inplace=True)
        #print(dfJPEX_Outage_hist)
        
        index = {'Mihama Unit 3':'OP',
                    'Takahama Unit 1':'AA',
                    'Takahama Unit 2':'AA',
                    'Takahama Unit 3':'OP',
                    'Takahama Unit 4':'OP',
                    'Ohi Unit 3':'OP',
                    'Ohi Unit 4':'OP',
                    'Genkai Unit 3':'OP',
                    'Genkai Unit 4':'OP',
                    'Sendai Unit 1':'OP',
                    'Sendai Unit 2':'OP',
                    'Ikata Unit 3':'OP',}
        
        dftable = pd.DataFrame(index = list(index.keys()), columns=['D','D-1','D-7','D-30','D-365']) #dfjpnuk.columns[:-1]
        '''
        for i in dftable.index:
            dftable.loc[i,'D'] = dfjpnuk.loc[str(today),i]
            dftable.loc[i,'D-1'] = dfjpnuk.loc[str(today-relativedelta(days=1)),i]
            dftable.loc[i,'D-7'] = dfjpnuk.loc[str(today-relativedelta(days=7)),i]
            dftable.loc[i,'D-30'] = dfjpnuk.loc[str(today-relativedelta(days=30)),i]
            dftable.loc[i,'D-365'] = dfjpnuk.loc[str(today-relativedelta(days=365)),i]
        #print(dftable)
        dftable_aa = pd.DataFrame(index = ['Hamaoka Unit 3',
                                            'Hamaoka Unit 4',
                                            'Hamaoka Unit 5',
                                            'Kashiwazaki Kariwa Unit 6',
                                            'Kashiwazaki Kariwa Unit 7',
                                            'Onagawa Unit 2',
                                            'Shika Unit 2',
                                            'Shimane Unit 2',
                                            'Tohoku/Higashidori Unit 1',
                                            'Tokai Unit 2',
                                            'Tomari Unit 1',
                                            'Tomari Unit 2',
                                            'Tomari Unit 3',
                                            'Tsuruga Unit 2',
                                            'Ohma',
                                            'Shimane Unit 3'], columns=['D','D-1','D-7','D-30','D-365'])
        
        
        dftable = pd.concat([dftable, dftable_aa])
        #dftable.fillna(0, inplace=True) #avoid some missing data
        dftable = dftable.astype('float')  
        
        dftabledate = pd.DataFrame([[str(today),str(today-relativedelta(days=1)), str(today-relativedelta(days=7)), str(today-relativedelta(days=30)), str(today-relativedelta(days=365))]], index = ['Date'], columns = dftable.columns)
        '''
                                
        #print(jpplantdict)
        dftable['operate'] = np.nan
        dftable['MW'] = np.nan
        dftable['Offline date'] = np.nan
        dftable['Return date'] = np.nan
        dftable['Current Days Offline'] = np.nan
        #print(dfjpex_outage_FP[dfjpex_outage_FP['PlantName FP']=='Mihama Unit 3'].index.tolist())
        #print(dfop)
        #print(dfJPEX_Outage_hist)
        for i in dftable.index:
            #print(i)
            try:
                #print(dfJPEX_Outage_hist['StopDateAndTime'].loc[dfJPEX_Outage_hist[dfJPEX_Outage_hist['PlantName FP']==i].index.tolist()[0]])
                dftable.loc[i, 'MW'] = jpplantdict[i]
                dftable.loc[i,'Offline date'] = dfJPEX_Outage_hist['StopDateAndTime'].loc[dfJPEX_Outage_hist[dfJPEX_Outage_hist['PlantName FP']==i].index.tolist()[0]].date()
                dftable.loc[i,'Return date'] = dfJPEX_Outage_hist['ScheduledRecoveryDate'].loc[dfJPEX_Outage_hist[dfJPEX_Outage_hist['PlantName FP']==i].index.tolist()[0]].date()
                dftable.loc[i,'Current Days Offline'] = dfJPEX_Outage_hist['CurrentDaysOffline'].loc[dfJPEX_Outage_hist[dfJPEX_Outage_hist['PlantName FP']==i].index.tolist()[0]]#.astype('int')
            except (IndexError, KeyError):
                #print(i, )
                #dftable.loc[i, 'operate'] = 'AA'
                dftable.loc[i, 'MW'] = jpplantdict[i]
        
        #sort by the op and Offline date
        #dftable.loc[:,['Offline date']] = dftable.loc[:,['Offline date']].astype('datetime64[ns]')
        #dftable.sort_values(by=['Offline date'], ascending=True, inplace=True)
        #dftable.sort_values(by=['operate'], ascending=False, inplace=True)
        #index_order = dftable[dftable['operate']=='OP'].index.to_list() + dftable[dftable['operate']=='AA'].index.to_list()
        #dftable = dftable.loc[index_order]
        #print(dftable)
        '''
        #sum zeros 
        column_sum = []
        for i in dftable.columns[0:5]:
            column_sum.append((dftable[i].loc[dftable[i].loc[dftable['operate']=='OP'].index]==0).sum(axis=0))
        #print(column_sum)
        dftabledate.loc['Sum of OP and Outage'] = column_sum  
        
        dftable =   dftabledate.append(dftable, ignore_index=False)  
        dftable.fillna(' ', inplace=True)
        #print(dftable)
        '''
        #Chart 1 planned data
        end_date = str((today+relativedelta(months=12)).year)+'-'+str((today+relativedelta(months=12)).month)+'-'+str(calendar.monthrange((today+relativedelta(months=12)).year, (today+relativedelta(months=12)).month)[1])
        dfplanned_data = pd.DataFrame(index = dftable.index[1:] , columns = pd.date_range(start = str(today.year)+'-01-01', end = end_date).date)
        for i in dfplanned_data.index:
            for j in dfplanned_data.columns:
                try:
                    if j <= dftable.loc[i,'Offline date'] or j >= dftable.loc[i,'Return date']:
                        dfplanned_data.loc[i,j] = dftable.loc[i, 'MW']
                    else:
                        dfplanned_data.loc[i,j] = np.nan
                except TypeError:
                    pass
        dfplanned_data = dfplanned_data.T
        
        dfplanned_data['Planned '+str(today.year)] = np.nan
        dfplanned_data.loc[today:pd.to_datetime(str(today.year)+'-12-31'),'Planned '+str(today.year)] = dfplanned_data.loc[today:pd.to_datetime(str(today.year)+'-12-31')].sum(axis=1)   
        dfplanned_data['Planned '+str(today.year+1)] = np.nan
        dfplanned_data.loc[today:pd.to_datetime(end_date),'Planned '+str(today.year+1)] = dfplanned_data.loc[today:pd.to_datetime(end_date)].sum(axis=1) 
        #dfplanned_data.loc['sum'] = dfplanned_data.sum()
        #print(dfplanned_data)
        
        dfplanned_data_last = dfplanned_data.copy()
        
        return dfplanned_data_last
        
    
    def chart(dfchart1, dfchart2, dftable,dfplanned_data, dfplanned_data_last, dfjpex_outage_FP, dfoffline, dftableM, dftable_outageM):
        
        #print(dfchart1)
        
        today=datetime.date.today()
        
        chart1 = go.Figure()
        
        chart1.add_trace(go.Scatter(x=dfchart1.index, y=dfchart1[str(today.year)],
                            mode='lines',
                            name=str(today.year),
                            line=dict(color='black', dash='solid')
                            ))
        chart1.add_trace(go.Scatter(x=dfchart1.index, y=dfchart1[str(today.year-1)],
                            mode='lines',
                            name=str(today.year-1),
                            line=dict(color='red', dash='solid')
                            ))
        chart1.add_trace(go.Scatter(x=dfchart1.index, y=dfchart1[str(today.year-2)],
                            mode='lines',
                            name=str(today.year-2),
                            line=dict(color='grey', dash='solid')
                            ))
        chart1.add_trace(go.Scatter(x=dfchart1.index, y=dfplanned_data['Planned '+str(today.year)],
                            mode='lines',
                            name='Planed '+str(today.year),
                            line=dict(color='green', dash='dot')
                            ))
        chart1.add_trace(go.Scatter(x=dfchart1.index, y=dfplanned_data['Planned '+str(today.year+1)].loc[pd.to_datetime(str(today.year+1)+'-01-01'):],
                            mode='lines',
                            name='Planed '+str(today.year+1),
                            line=dict(color='orange', dash='dot')
                            ))
        
        chart1.add_trace(go.Scatter(x=dfchart1.index, y=dfplanned_data_last['Planned '+str(today.year)],
                            mode='lines',
                            name='Planed '+str(today.year)+ ' M-1',
                            line=dict(color='green', dash='dash'),
                            visible = 'legendonly'
                            ))
        chart1.add_trace(go.Scatter(x=dfchart1.index, y=dfplanned_data_last['Planned '+str(today.year+1)].loc[pd.to_datetime(str(today.year+1)+'-01-01'):],
                            mode='lines',
                            name='Planed '+str(today.year)+' M-1',
                            line=dict(color='orange', dash='dash'),
                            visible = 'legendonly'
                            ))
        
        
        chart1.add_trace(go.Scatter(x=dfchart1.index, y=dfchart1['MAX OP Capacity'],
                            mode='lines',
                            name='MAX OP Capacity',
                            line=dict(color='blue', dash='dot')
                            ))
        
        
        chart1.update_layout(
             autosize=True,
             showlegend=True,
             #colorscale='RdBu',
             #legend=dict(x=0, y=-0.2),
             legend_orientation="h",
             title_text='Japanese Daily Nuclear MW '+str(today),
             #xaxis = dict(dtick="M1"),
             #xaxis_tickformat = '%B',
             hovermode='x unified',
             plot_bgcolor = 'white',
             yaxis = dict(showgrid=True, gridcolor='lightgrey'),
    
             #template='ggplot2'  
         )
        #chart 2
        chart2_color = { 'Mihama Unit 3':'#FBDFDF',
                        'Takahama Unit 1':'#DAE3F3',
                        'Takahama Unit 2':'#DEEBF7',
                        'Takahama Unit 3':'#BDD7EE',
                        'Takahama Unit 4':'#9DC3E6',
                        'Ohi Unit 3':'#FFF2CC',
                        'Ohi Unit 4':'#FFE699',
                        'Genkai Unit 3':'#D9D9D9',
                        'Genkai Unit 4':'#BFBFBF',
                        'Sendai Unit 1':'#E2F0D9',
                        'Sendai Unit 2':'#C5E0B4',
                        'Ikata Unit 3':'#F9BDDB'       
            }
        
        
        chart2 = go.Figure()
        
        for i in dfchart2.columns:
            chart2.add_trace(go.Scatter(x=dfchart2.index, y=dfchart2[i],
                                #mode='lines',
                                mode='none',
                                name=i,
                                #fill='tozeroy',
                                stackgroup='one',
                                #hoverinfo='x+y',
                                #line=dict(color=chart2_color[i], dash='solid')
                                fillcolor=chart2_color[i],
                                line_color=chart2_color[i],
                                ))
       
        chart2.update_layout(
             autosize=True,
             showlegend=True,
             #colorscale='RdBu',
             legend=dict(x=0, y=-0.2),
             legend_orientation="h",
             title_text='Japanese Daily Nuclear MW by Plant '+str(today),
             #xaxis = dict(dtick="M1"),
             #xaxis_tickformat = '%B',
             #hovermode='x unified',
             plot_bgcolor = 'white',
             yaxis = dict(showgrid=True, gridcolor='lightgrey'),
             #barmode='relative' 
             #template='ggplot2'  
         )
        
        
        #table
        #print(dftable.iloc[[1]].info())
        #print('dtpte',dftable.iloc[1,1]==0)
        
        
        dfcolor=pd.DataFrame('white', index = dftable.index, columns = dftable.columns)
        dfcolor.iloc[0] = ['yellow','white','white','white','white','white','white','white','white','white']
        dfcolor.iloc[1] = ['red','red','red','red','red','white','white','white','white','white']
        for i in dftable.index[2:]:
            for j in dftable.columns[0:5]:
                if dftable.loc[i,j] == 0:
                    #print(dftable.loc[i,j]==0)
                    dfcolor.loc[i,j] = 'white'
                elif dftable.loc[i,j] == ' ':
                    dfcolor.loc[i,j] = 'white'
                else :
                    dfcolor.loc[i,j] = 'paleturquoise'
          
            try:
                if today <= dftable.loc[i,'Offline date'] <= today+relativedelta(days = 30):
                    dfcolor.loc[i,'Offline date'] = '#FBDFDF'
                    
                if today <= dftable.loc[i,'Return date'] <= today+relativedelta(days = 30):
                        dfcolor.loc[i,'Return date'] = 'PaleGreen'
            except TypeError:
            
                dfcolor.loc[i,'Return date'] = '#D9D9D9'
                    
        dfcolor.reset_index(inplace=True) 
        #print(dfcolor)
        dfcolor['index'] = '#D9D9D9' 
        #print(dfcolor)
        
        dfcolor = dfcolor.T            
        #dftable
        table1 = go.Figure(
                data=[go.Table(
                header=dict(values=['Delivery Date']+list(dftable.columns),
                            line_color='darkslategray',
                            fill_color='royalblue',
                            align=['center'],
                            font=dict(color='white'),
                            ),
                cells=dict(values=[dftable.index] + [dftable[pm] for pm in dftable.columns],
                           #values=[dfwind_wave.]
                           line_color='white',#'darkslategray',
                            fill=dict(color=dfcolor.values.tolist()),
                            align=[ 'center'],
                            #font_size=12,
                            #height=30,
                            
                            ))
            ])
        #print(dftable.index)
        #table 2
        index_list = dftable.index.to_list()[2:]
        index_list_FP = dfjpex_outage_FP['PlantName FP'].to_list()
        #print(index_list)
        for i in dftable.index.to_list()[2:]:
            if i not in index_list_FP:
                index_list.remove(i)
                
        for j in index_list_FP:
            if j not in index_list:
                index_list.append(j)
        #print(index_list)
        dftable2 = dfjpex_outage_FP.copy()
        dftable2.set_index('PlantName FP', inplace=True)
        dftable2 = dftable2.loc[index_list]
        dftable2.reset_index(inplace=True)
        dftable2 = dftable2[['PowerPlantName','UnitName','comments','OngoingOrUpcoming','StopDateAndTime','ScheduledRecoveryDate',	'DaysDiffOffline','DaysDiffReturn','DaysDiffTotal','CurrentDaysOffline']]
        #dftable2.sort_values(by='PowerPlantName', inplace = True)
        
        dftable2 = sort_table_dates('StopDateAndTime', 'ScheduledRecoveryDate', dftable2, 'jp_table_2')
        
        table2 = go.Figure(
                data=[go.Table(
                header=dict(values=list(dftable2.columns),
                            line_color='darkslategray',
                            fill_color='royalblue',
                            align=['center'],
                            font=dict(color='white'),
                            ),
                cells=dict(values= [dftable2[pm] for pm in dftable2.columns],
                           #values=[dfwind_wave.]
                           line_color='white',#'darkslategray',
                            #fill=dict(color=dfcolor.values.tolist()),
                            align=[ 'center'],
                            #font_size=12,
                            #height=30,
                            
                            ))
            ])
        
        
        #offline days bar chart
 
        figoffline = go.Figure()
        figoffline.add_trace(go.Bar(x=dfoffline.columns, y=dfoffline.loc[str(today.year-2)],
                #base=[-500,-600,-700],
                #marker_color='crimson',
                name=str(today.year-2)))
        figoffline.add_trace(go.Bar(x=dfoffline.columns, y=dfoffline.loc[str(today.year-1)],
                #base=[-500,-600,-700],
                #marker_color='crimson',
                name=str(today.year-1)))
        figoffline.add_trace(go.Bar(x=dfoffline.columns, y=dfoffline.loc[str(today.year)],
                #base=[-500,-600,-700],
                #marker_color='crimson',
                name=str(today.year)))
        
        figoffline.update_layout(
            autosize=True,
            showlegend=True,
            #colorscale='RdBu',
            #legend=dict(x=0, y=-0.2),
            legend_orientation="h",
            title_text='Japanese Nuclear Plants Offline Days',
            yaxis_title="days",
            #xaxis = dict(dtick="M1"),
            #xaxis_tickformat = '%B',
            #hovermode='x unified',
            plot_bgcolor = 'white',
            yaxis = dict(showgrid=True, gridcolor='lightgrey'),
            #barmode='relative' 
            #template='ggplot2'  
        )
        
        
        tableM = go.Figure(
                data=[go.Table(
                header=dict(values=['Plant']+list(dftableM.columns),
                            line_color='darkslategray',
                            fill_color='royalblue',
                            align=['center'],
                            font=dict(color='white'),
                            ),
                cells=dict(values=[dftableM.index] + [dftableM[pm] for pm in dftableM.columns],
                           #values=[dfwind_wave.]
                           line_color='white',#'darkslategray',
                            #fill=dict(color=dfcolor.values.tolist()),
                            align=[ 'center'],
                            #font_size=12,
                            #height=30,
                            
                            ))
            ])
        tableM.update_layout(title_text='Japan Nuclear Plants Past 3 Years Offline Days')
        
        table_outageM = go.Figure(
                data=[go.Table(
                header=dict(values=['Month']+list(dftable_outageM.columns),
                            line_color='darkslategray',
                            fill_color='royalblue',
                            align=['center'],
                            font=dict(color='white'),
                            ),
                cells=dict(values=[dftable_outageM.index] + [dftable_outageM[pm] for pm in dftable_outageM.columns],
                           #values=[dfwind_wave.]
                           line_color='white',#'darkslategray',
                            #fill=dict(color=dfcolor.values.tolist()),
                            align=[ 'center'],
                            #font_size=12,
                            #height=30,
                            
                            ))
            ])
        
        table_outageM.update_layout(title_text='Japan Nuclear Plants Past 3 Years Offline Days by Month')

                
        py.plot(chart1, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/nuk/Japan chart1.html', auto_open=False)
        py.plot(chart2, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/nuk/Japan chart2.html', auto_open=False)
        py.plot(table1, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/nuk/Japan table.html', auto_open=False)
        py.plot(table2, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/nuk/Japan table2.html', auto_open=False)
        py.plot(figoffline, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/nuk/Japan figoffline.html', auto_open=False)
        py.plot(tableM, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/nuk/Japan figofflineM.html', auto_open=False)
        py.plot(table_outageM, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/nuk/Japan figoutageM.html', auto_open=False)
        
        chart1.write_image("U:/Trading - Gas/LNG/LNG website/analysis/nuk/Japan chart1.png")
        chart2.write_image("U:/Trading - Gas/LNG/LNG website/analysis/nuk/Japan chart2.png")
        table1.write_image("U:/Trading - Gas/LNG/LNG website/analysis/nuk/Japan table.png")
        table2.write_image("U:/Trading - Gas/LNG/LNG website/analysis/nuk/Japan table2.png")
        figoffline.write_image("U:/Trading - Gas/LNG/LNG website/analysis/nuk/Japan figoffline.png")
        tableM.write_image("U:/Trading - Gas/LNG/LNG website/analysis/nuk/Japan figofflineM.png")
        table_outageM.write_image("U:/Trading - Gas/LNG/LNG website/analysis/nuk/Japan figoutageM.png")
   
'''
dfjpnuk, dfjpnuk_previous, dfJP_outage_date, dfJP_outage_date_previous, dfjpex_outage = get_data()
dfjpex_outage_FP = jpex_data(dfjpex_outage)
jpplantdict, dfop = planned_data(dfjpex_outage_FP, dfJP_outage_date, dfJP_outage_date_previous, dfjpnuk, dfjpnuk_previous)
dfchart1, dfchart2, dftable, dfplanned_data = chart_data(dfjpnuk, jpplantdict, dfjpex_outage_FP, dfop)
chart(dfchart1, dfchart2, dftable, dfplanned_data)
'''


class SK_Nuk_Outage():
    
    def get_data():
        
        #get sk nuk latest
        dfsknuk = DBTOPD.get_SK_nuk()
        dfsknuk = dfsknuk[['CurveId','ValueDate','Value']]
        
        dfsknuk = pd.pivot_table(dfsknuk, values='Value', index='ValueDate',columns='CurveId',aggfunc='sum')
        #print(dfsknuk) 							

        name_dict = {14919:'Kori Unit - 1',
                     14920:'Kori Unit - 2',
                     14921:'Kori Unit - 3',
                     14922:'Kori Unit - 4',
                     14923:'Kori Shin - 1',
                     14924:'Kori Shin - 2',
                     14925:'Hanbit Unit - 1',
                     14926:'Hanbit Unit - 2',
                     14927:'Hanbit Unit - 3',
                     14928:'Hanbit Unit - 4',
                     14929:'Hanbit Unit - 5',
                     14930:'Hanbit Unit - 6',
                     14931:'Wolsong Unit - 1',
                     14932:'Wolsong Unit - 2',
                     14933:'Wolsong Unit - 3',
                     14934:'Wolsong Unit - 4',
                     14935:'Wolsong Shin - 1',
                     14936:'Wolsong Shin - 2',
                     14937:'Hanul Unit - 1',
                     14938:'Hanul Unit - 2',
                     14939:'Hanul Unit - 3',
                     14940:'Hanul Unit - 4',
                     14941:'Hanul Unit - 5',
                     14942:'Hanul Unit - 6',
                     14943:'Saeul Saeul 1',
                     14944:'Saeul Saeul 2',
                     71449:'Shinhanul 1',
            
            }


        
        dfsknuk.rename(columns=name_dict, inplace=True)
        #dfsknuk.loc[:'2022-06-07'].fillna(method='ffill', inplace=True)
        dfsknuk['sum'] = dfsknuk.sum(axis=1)
        #print(dfsknuk)
        #fill lost date 
        #print(dfsknuk.index[0], dfsknuk.index[1])
        dfsknuk_full = pd.DataFrame(index = pd.date_range(start = dfsknuk.index[0], end = dfsknuk.index[-1]), columns = dfsknuk.columns)
        for i in dfsknuk.index:
            dfsknuk_full.loc[i] = dfsknuk.loc[i].copy()
        dfsknuk_full.fillna(method='ffill', inplace=True)
        
        '''
        #get sk nuk last month
        dfsknuk_previous = DBTOPD.get_sk_nuk_previous() 
        
        dfsknuk_previous = dfsknuk_previous[['CurveId','ValueDate','Value']]
        
        dfsknuk_previous = pd.pivot_table(dfsknuk_previous, values='Value', index='ValueDate',columns='CurveId',aggfunc='sum')
       											 	
        dfsknuk_previous.rename(columns=name_dict, inplace=True)
        dfsknuk_previous['sum'] = dfsknuk_previous.sum(axis=1)
        #print(dfsknuk_previous)
        
        
        #outage plan date
        dfsk_outage_date = DBTOPD.get_sk_nuk_outage_date()
        dfsk_outage_date['PlantName'] = dfsk_outage_date['PowerPlantName']+' '+dfsk_outage_date['UnitName']
        
        #lastest update
        #print(dfsk_outage_date)
        
        #last outage plan date
        dfsk_outage_date_previous = DBTOPD.get_sk_nuk_outage_date_previous()
        dfsk_outage_date_previous['PlantName'] = dfsk_outage_date_previous['PowerPlantName']+' '+dfsk_outage_date_previous['UnitName']
        '''
        
        dfKHNP_outage = DBTOPD.get_KHNP_outage()
        
        #dfskex_outage['PlantName'] = dfskex_outage['PowerPlantName']+' '+dfskex_outage['UnitName']
        #dfKHNP_outage['timestamp'] = datetime.date.today()
        
        #print(dfKHNP_outage)
        #print(dfsk_outage_date_previous)
        #print(dfsknuk)
        
        return dfsknuk_full, dfKHNP_outage
        
    
    def KHNP_data(dfKHNP_outage):
        
        
        #dfKHNP_outage_FP = dfKHNP_outage.copy()
        '''
        KHNP_name_dict = {'Hanbit 1':'Hanbit Unit - 1',
                           'Hanbit 2':'Hanbit Unit - 2',
                           'Hanbit 3':'Hanbit Unit - 3',
                           'Hanbit 4':'Hanbit Unit - 4',
                           'Hanbit 5':'Hanbit Unit - 5',
                           'Hanbit 6':'Hanbit Unit - 6',
                           'Hanul 1':'Hanul Unit - 1',
                           'Hanul 2':'Hanul Unit - 2',
                           'Hanul 3':'Hanul Unit - 3',
                           'Hanul 4':'Hanul Unit - 4',
                           'Hanul 5':'Hanul Unit - 5',
                           'Hanul 6':'Hanul Unit - 6',
                           'Kori 2':'Kori Unit - 2',
                           'Kori 3':'Kori Unit - 3',
                           'Kori 4':'Kori Unit - 4',
                           'Shingori 1':'Kori Shin - 1',
                           'Shingori 2':'Kori Shin - 2',
                           'Shingori 3':'Saeul Saeul 1', 
                           'Shingori 4':'Saeul Saeul 2', 
                           'Saeul 1':'Saeul Saeul 1', 
                           'Saeul 2':'Saeul Saeul 2', 
                           'Sinwolseong 1':'Wolsong Shin - 1',
                           'Sinwolseong 2':'Wolsong Shin - 2',
                           'Wolseong 1':'Wolsong Unit - 1', 
                           'Wolseong 2':'Wolsong Unit - 2',
                           'Wolseong 3':'Wolsong Unit - 3',
                           'Wolseong 4':'Wolsong Unit - 4',
                           'Shin-Hanul 1':'Shin-Hanul 1',
                            'Shin-Hanul 2':'Shin-Hanul 2',
                            'Shin-Kori 5':'Shin-Kori 5',
                            'Shin-Kori 6':'Shin-Kori 6',
                            
                            
                                    }
        '''
        KHNP_name_dict = {'Hanbit 1':'Hanbit Unit - 1',
                           'Hanbit 2':'Hanbit Unit - 2',
                           'Hanbit 3':'Hanbit Unit - 3',
                           'Hanbit 4':'Hanbit Unit - 4',
                           'Hanbit 5':'Hanbit Unit - 5',
                           'Hanbit 6':'Hanbit Unit - 6',
                           'Hanul 1':'Hanul Unit - 1',
                           'Hanul 2':'Hanul Unit - 2',
                           'Hanul 3':'Hanul Unit - 3',
                           'Hanul 4':'Hanul Unit - 4',
                           'Hanul 5':'Hanul Unit - 5',
                           'Hanul 6':'Hanul Unit - 6',
                           'Kori 2':'Kori Unit - 2',
                           'Kori 3':'Kori Unit - 3',
                           'Kori 4':'Kori Unit - 4',
                           'Shingori 1':'Kori Shin - 1',
                           'Shingori 2':'Kori Shin - 2',
                           'Saeul 1':'Saeul Saeul 1', 
                           'Saeul 2':'Saeul Saeul 2', 
                           'Sinwolseong 1':'Wolsong Shin - 1',
                           'Sinwolseong 2':'Wolsong Shin - 2',
                           'Wolseong 1':'Wolsong Unit - 1', 
                           'Wolseong 2':'Wolsong Unit - 2',
                           'Wolseong 3':'Wolsong Unit - 3',
                           'Wolseong 4':'Wolsong Unit - 4',
                           'Shin-Hanul 1':'Shin-Hanul 1',
                            'Shin-Hanul 2':'Shin-Hanul 2',
                            'Shin-Kori 5':'Shin-Kori 5',
                            'Shin-Kori 6':'Shin-Kori 6',
                            'Shinhanul 1':'Shinhanul 1',
                                    }
                     
                     
        dfKHNP_outage_FP = dfKHNP_outage.copy()
        dfKHNP_outage_FP.drop_duplicates(subset=['UnitName'], inplace=True)
        #print(dfKHNP_outage_FP)
        dfKHNP_outage_FP['PlantName FP'] = np.nan
        #dfKHNP_outage_FP.drop(index=[17, 18], inplace=True)
        #print(dfKHNP_outage_FP)
        #dfKHNP_outage_FP.drop(index=[17, 18], inplace=True)
        
        for i in dfKHNP_outage_FP.index:
            dfKHNP_outage_FP.loc[i,'PlantName FP'] = KHNP_name_dict[dfKHNP_outage_FP.loc[i,'UnitName']]
            
            
        dfKHNP_outage_FP_todb = dfKHNP_outage_FP.copy()
        dfKHNP_outage_FP_todb['create date'] = datetime.datetime.now()
        #print(dfKHNP_outage_FP_todb)
        #save skex to db
        db_server_lng = "PRD-DB-SQL-211"
        db_name_lng = "LNG"
        sql_engine_lng = sqlalchemy.create_engine("mssql+pyodbc://" + db_server_lng + "/" + db_name_lng + "?trusted_connection=yes&driver=ODBC+Driver+13+for+SQL+Server")
        
        dfKHNP_outage_FP_todb.to_sql(name='KHNP_Outage_hist', con=sql_engine_lng, index=False, if_exists='append', schema='ana')
        
        #get hist data
        dfKHNP_Outage_hist = DBTOPD("PRD-DB-SQL-211",'LNG', 'ana','KHNP_Outage_hist').sql_to_df()
        #print(dfKHNP_Outage_hist)
        
        return dfKHNP_outage_FP, dfKHNP_Outage_hist
    
    def planned_data(dfsknuk):
        
        today=datetime.date.today()
        
        #copy data from https://www.khnp.co.kr/eng/content/529/main.do?mnCd=EN03020101
        
        skplantdict = {'Hanbit Unit - 1':950,
                       'Hanbit Unit - 2':950,
                       'Hanbit Unit - 3':1000,
                       'Hanbit Unit - 4':1000,
                       'Hanbit Unit - 5':1000,
                       'Hanbit Unit - 6':1000,
                       'Hanul Unit - 1':950,
                       'Hanul Unit - 2':950,
                       'Hanul Unit - 3':1000,
                       'Hanul Unit - 4':1000,
                       'Hanul Unit - 5':1000,
                       'Hanul Unit - 6':1000,
                       'Kori Unit - 2':650,
                       'Kori Unit - 3':950,
                       'Kori Unit - 4':950,
                       'Kori Shin - 1':1000,
                       'Kori Shin - 2':1000,
                       'Saeul Saeul 1':1400, 
                       'Saeul Saeul 2':1400, 
                       'Saeul 1':1400,
                       'Saeul 2':1400, 
                       'Wolsong Shin - 1':1000,
                       'Wolsong Shin - 2':1000,
                       'Wolsong Unit - 1':679, 
                       'Wolsong Unit - 2':700, 
                       'Wolsong Unit - 3':700, 
                       'Wolsong Unit - 4':700,
                       'Shin-Hanul 1':1400,
                        'Shin-Hanul 2':1400,
                        'Shin-Kori 5':1400,
                        'Shin-Kori 6':1400,
                        'Shinhanul 1':1350
                       }
    
        
             
        #OP or AA
        oplist = []
        dfop = pd.DataFrame(index = [today], columns = dfsknuk.columns[:-1])
        for i in dfop.columns:
            if dfsknuk[i].sum() == 0:
                dfop[i] = 'AA'
            else:
                dfop[i] = 'OP'
                oplist.append(i)
        #print(oplist)
        
    
    
        #columns = dfsk_outage_date['PlantName'].drop_duplicates().tolist()
        
        return  skplantdict, dfop, oplist
        
    
    def chart_data(dfsknuk, skplantdict, dfKHNP_outage_FP, dfop, oplist):
        
        today=datetime.date.today()
        #print(dfsknuk.loc['2021-01-01':'2021-12-31','sum'])
        
        dfopcapa = pd.DataFrame(index = [today], columns = oplist)
        for i in oplist:
            dfopcapa[i] = skplantdict[i]
        dfopcapa['capa'] =    dfopcapa.sum(axis=1) 
        #print(dfopcapa)
        #chart 1 data
        dfchart1 = pd.DataFrame(index = pd.date_range(start = str(today.year)+'-01-01', end = str(today.year)+'-12-31'), columns=[str(today.year)])
        dfchart1['MAX OP Capacity'] = dfopcapa.loc[today, 'capa']
        dfchart1[str(today.year)] = dfsknuk.loc[str(today.year)+'-01-01':str(today.year)+'-12-31','sum']
        dfchart1[str(today.year-1)] = dfsknuk.loc[str(today.year-1)+'-01-01':str(today.year-1)+'-12-31','sum'].values
        dfchart1[str(today.year-2)] = dfsknuk.loc[str(today.year-2)+'-01-01':str(today.year-2)+'-12-31','sum'].values#np.delete(dfsknuk.loc['2020-01-01':'2020-12-31','sum'].values, 59)
        dfchart1.loc['2022-09-01':,'MAX OP Capacity'] = dfchart1.loc['2022-09-01':,'MAX OP Capacity']+1400
        #print(dfchart1)
        
        #chart 2 data
        dfchart2 = dfsknuk.iloc[:,:-1]
        
        
        #table data
        #print(dfKHNP_outage_FP)
        dftable = pd.DataFrame(index = dfKHNP_outage_FP['PlantName FP'].to_list(), columns=['D','D-1','D-7','D-30','D-365']) #dfsknuk.columns[:-1]
        for i in dftable.index:
            dftable.loc[i,'D'] = dfsknuk.loc[str(today),i]
            dftable.loc[i,'D-1'] = dfsknuk.loc[str(today-relativedelta(days=1)),i]
            dftable.loc[i,'D-7'] = dfsknuk.loc[str(today-relativedelta(days=7)),i]
            dftable.loc[i,'D-30'] = dfsknuk.loc[str(today-relativedelta(days=30)),i]
            dftable.loc[i,'D-365'] = dfsknuk.loc[str(today-relativedelta(days=365)),i]
        #print(dftable)
        dftable_aa = pd.DataFrame(index = ['Shin-Hanul 1',
                                           'Shin-Hanul 2',
                                           'Shin-Kori 5',
                                           'Shin-Kori 6'
                                           ], columns=['D','D-1','D-7','D-30','D-365'])
        
        
        dftable = pd.concat([dftable, dftable_aa])
        dftable.fillna(0, inplace=True)
        dftable = dftable.astype('float')  
        
        dftabledate = pd.DataFrame([[str(today),str(today-relativedelta(days=1)), str(today-relativedelta(days=7)), str(today-relativedelta(days=30)), str(today-relativedelta(days=365))]], index = ['Date'], columns = dftable.columns)
    
                                
        #print(skplantdict)
        dftable['operate'] = np.nan
        dftable['MW'] = np.nan
        dftable['Offline date'] = np.nan
        dftable['Return date'] = np.nan
        dftable['Current Days Offline'] = np.nan
        #print(dfskex_outage_FP[dfskex_outage_FP['PlantName FP']=='Mihama Unit 3'].index.tolist())
        #print(dfop)
        #print(dftable.index)
        #print(dfKHNP_outage_FP)
        for i in dftable.index:
            #print(i)
            try:
                #print(dftable.loc[i, 'operate'])
                #print(dfop[i])
                dftable.loc[i, 'operate'] = dfop[i].values
                dftable.loc[i, 'MW'] = skplantdict[i]
                dftable.loc[i,'Offline date'] = dfKHNP_outage_FP['OfflineDate'].loc[dfKHNP_outage_FP[dfKHNP_outage_FP['PlantName FP']==i].index.tolist()[0]].date()
                dftable.loc[i,'Return date'] = dfKHNP_outage_FP['ReturnDate'].loc[dfKHNP_outage_FP[dfKHNP_outage_FP['PlantName FP']==i].index.tolist()[0]].date()
                dftable.loc[i,'Current Days Offline'] = dfKHNP_outage_FP['CurrentDaysOffline'].loc[dfKHNP_outage_FP[dfKHNP_outage_FP['PlantName FP']==i].index.tolist()[0]]#.astype('int')
            except (IndexError, KeyError):
                #print(i, )
                dftable.loc[i, 'operate'] = 'AA'
                dftable.loc[i, 'MW'] = skplantdict[i]
        
        dftable.loc['Hanbit Unit - 4','operate'] = 'OP'
        #print(dftable)
        #sort by the op and return date
        #dftable.loc[:,['Offline date']] = dftable.loc[:,['Offline date']].astype('datetime64[ns]')
        dftable.sort_values(by=['Offline date'], ascending=True, inplace=True)
        #dftable.sort_values(by=['operate'], ascending=False, inplace=True)
        index_order = dftable[dftable['operate']=='OP'].index.to_list() + dftable[dftable['operate']=='AA'].index.to_list()
        dftable = dftable.loc[index_order]
        #sum zeros 
        column_sum = []
        for i in dftable.columns[0:5]:
            column_sum.append((dftable[i].loc[dftable[i].loc[dftable['operate']=='OP'].index]==0).sum(axis=0))
        #print(column_sum)
        dftabledate.loc['Sum of OP and Outage'] = column_sum 
        
        dftable =   dftabledate.append(dftable, ignore_index=False)  
        dftable.fillna(' ', inplace=True)
        #print(dftable['Offline date'])
        
        #Chart 1 planned data
        end_date = str((today+relativedelta(months=12)).year)+'-'+str((today+relativedelta(months=12)).month)+'-'+str(calendar.monthrange((today+relativedelta(months=12)).year, (today+relativedelta(months=12)).month)[1])

        dfplanned_data = pd.DataFrame(index = dftable.index[1:] , columns = pd.date_range(start = str(today.year)+'-01-01', end = end_date).date)
        for i in dfplanned_data.index:
            for j in dfplanned_data.columns:
                try:
                    if j <= dftable.loc[i,'Offline date'] or j >= dftable.loc[i,'Return date']:
                        dfplanned_data.loc[i,j] = dftable.loc[i, 'MW']
                        #print(i)
                    else:
                        dfplanned_data.loc[i,j] = np.nan
                except TypeError:
                    pass
        dfplanned_data = dfplanned_data.T
        #print(dfplanned_data)
        
        dfplanned_data['Planned '+str(today.year)] = np.nan
        dfplanned_data.loc[today:pd.to_datetime(str(today.year)+'-12-31'),'Planned '+str(today.year)] = dfplanned_data.loc[today:pd.to_datetime(str(today.year)+'-12-31')].sum(axis=1)   
        dfplanned_data['Planned '+str(today.year+1)] = np.nan
        dfplanned_data.loc[today:pd.to_datetime(end_date),'Planned '+str(today.year+1)] = dfplanned_data.loc[today:pd.to_datetime(end_date)].sum(axis=1) 
        

        #dfplanned_data.loc['sum'] = dfplanned_data.sum()
            
        
        #offline days bar chart data
        dfoffline = pd.DataFrame(index = [str(today.year-2),str(today.year-1),str(today.year)], columns = dfsknuk.columns)
        dfoffline.loc[str(today.year-2)] = (dfsknuk.loc[str(today.year-2)+'-01-01':str(today.year-2)+'-12-31'] == 0).sum(axis=0)
        dfoffline.loc[str(today.year-1)] = (dfsknuk.loc[str(today.year-1)+'-01-01':str(today.year-1)+'-12-31'] == 0).sum(axis=0)
        dfoffline.loc[str(today.year)] = (dfsknuk.loc[str(today.year)+'-01-01':str(today.year)+'-12-31'] == 0).sum(axis=0)
        #print(dfoffline)
        
        
        index = pd.date_range(start = dfsknuk.copy().resample('MS').sum().index[0], end = dfsknuk.copy().resample('MS').sum().index[-1], freq='MS')
        
        dfkrnukM = pd.DataFrame(index = index, columns = dfsknuk.columns[:-1])
        for i in index:
            monthstart = datetime.datetime(i.year, i.month, 1)
            monthend = datetime.datetime(i.year, i.month, calendar.monthrange(i.year, i.month)[1])
            dfkrnukM.loc[i] =  (dfsknuk.loc[monthstart:monthend,:'sum'] == 0).sum(axis=0)
        #print(dfkrnukM)
        
        dftableMkr = pd.DataFrame(index = dfoffline.columns[:-1])
        #print(dftableMkr)
        for i in dftableMkr.index:
            #print(dfkrnukM.loc[str(today.year-2)+'-01-01':str(today.year)+'-12-31', i].sum()/3)
            #print(i)
            dftableMkr.loc[i, 'Ave. 3yrs offline days'] = int((dfkrnukM.loc[str(today.year-2)+'-01-01':str(today.year)+'-12-31', i].sum()/3))
            dftableMkr.loc[i, 'MW Capacity'] =  skplantdict[i]
            dftableMkr.loc[i, str(today.year-2)+' offline days'] = int((dfkrnukM.loc[str(today.year-2)+'-01-01':str(today.year-2)+'-12-31', i].sum()))
            dftableMkr.loc[i, str(today.year-1)+' offline days'] = int((dfkrnukM.loc[str(today.year-1)+'-01-01':str(today.year-1)+'-12-31', i].sum()))
            dftableMkr.loc[i, str(today.year-0)+' offline days'] = int((dfkrnukM.loc[str(today.year-0)+'-01-01':str(today.year-0)+'-12-31', i].sum()))
            #dftableM.loc[i, 'offline time'] = [dfjpex_outage_FP.loc[dfjpex_outage_FP[dfjpex_outage_FP['PlantName FP'] == i].index, 'StopDateAndTime'],dfjpex_outage_FP.loc[dfjpex_outage_FP[dfjpex_outage_FP['PlantName FP'] == i].index, 'ScheduledRecoveryDate']]
           
        
        #print(dfjpnukM)
        dfkrnukM_new = dfkrnukM.copy()
        '''
        for i in dfkrnukM_new.columns:
            if dfkrnukM_new.loc[str(today.year-2)+'-01-01':str(today.year-2)+'-12-31', i].sum() == 365 or dfkrnukM_new.loc[str(today.year-2)+'-01-01':str(today.year-2)+'-12-31', i].sum() == 366:
                dfkrnukM_new.loc[str(today.year-2)+'-01-01':str(today.year-2)+'-12-31', i] = 0
            if dfkrnukM_new.loc[str(today.year-1)+'-01-01':str(today.year-1)+'-12-31', i].sum() == 365 or dfkrnukM_new.loc[str(today.year-1)+'-01-01':str(today.year-1)+'-12-31', i].sum() == 366:
                dfkrnukM_new.loc[str(today.year-1)+'-01-01':str(today.year-1)+'-12-31', i] = 0
            if dfkrnukM_new.loc[str(today.year-0)+'-01-01':str(today.year-0)+'-12-31', i].sum() == 365 or dfkrnukM_new.loc[str(today.year-0)+'-01-01':str(today.year-0)+'-12-31', i].sum() == 366:
                dfkrnukM_new.loc[str(today.year-0)+'-01-01':str(today.year-0)+'-12-31', i] = 0
        #print(dfjpnukM_new)
        '''
        dftable_outageM = pd.DataFrame(index = ['Jan', 'Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'], columns=[str(today.year-2),str(today.year-1),str(today.year)])
        
        for j in dftable_outageM.columns:
            
            Jan_list = []
            Feb_list = []
            Mar_list = []
            Apr_list = []
            May_list = []
            Jun_list = []
            Jul_list = []
            Aug_list = []
            Sep_list = []
            Oct_list = []
            Nov_list = []
            Dec_list = []
            
            for i in dfkrnukM_new.columns:
                try:
                    if dfkrnukM_new.loc[j+'-01-01',i] != 0: 
                        Jan_list.append(i+'('+str(dfkrnukM_new.loc[j+'-01-01',i])+')')
                    if dfkrnukM_new.loc[j+'-02-01',i] != 0:
                        Feb_list.append(i+'('+str(dfkrnukM_new.loc[j+'-02-01',i])+')')
                    if dfkrnukM_new.loc[j+'-03-01',i] != 0:
                        Mar_list.append(i+'('+str(dfkrnukM_new.loc[j+'-03-01',i])+')')
                    if dfkrnukM_new.loc[j+'-04-01',i] != 0:
                        Apr_list.append(i+'('+str(dfkrnukM_new.loc[j+'-04-01',i])+')')
                    if dfkrnukM_new.loc[j+'-05-01',i] != 0:
                        May_list.append(i+'('+str(dfkrnukM_new.loc[j+'-05-01',i])+')')
                    if dfkrnukM_new.loc[j+'-06-01',i] != 0:
                        Jun_list.append(i+'('+str(dfkrnukM_new.loc[j+'-06-01',i])+')')
                    if dfkrnukM_new.loc[j+'-07-01',i] != 0:
                        Jul_list.append(i+'('+str(dfkrnukM_new.loc[j+'-07-01',i])+')')
                    if dfkrnukM_new.loc[j+'-08-01',i] != 0:
                        Aug_list.append(i+'('+str(dfkrnukM_new.loc[j+'-08-01',i])+')')
                    if dfkrnukM_new.loc[j+'-09-01',i] != 0:
                        Sep_list.append(i+'('+str(dfkrnukM_new.loc[j+'-09-01',i])+')')
                    if dfkrnukM_new.loc[j+'-10-01',i] != 0:
                        Oct_list.append(i+'('+str(dfkrnukM_new.loc[j+'-10-01',i])+')')
                    if dfkrnukM_new.loc[j+'-11-01',i] != 0:
                        Nov_list.append(i+'('+str(dfkrnukM_new.loc[j+'-11-01',i])+')')
                    if dfkrnukM_new.loc[j+'-12-01',i] != 0:
                        Dec_list.append(i+'('+str(dfkrnukM_new.loc[j+'-12-01',i])+')')
                except KeyError:
                    pass
   
            dftable_outageM.loc['Jan',j] =','.join(k for k in Jan_list)  
            dftable_outageM.loc['Feb',j] =','.join(k for k in Feb_list) 
            dftable_outageM.loc['Mar',j] =','.join(k for k in Mar_list) 
            dftable_outageM.loc['Apr',j] =','.join(k for k in Apr_list) 
            dftable_outageM.loc['May',j] =','.join(k for k in May_list) 
            dftable_outageM.loc['Jun',j] =','.join(k for k in Jun_list) 
            dftable_outageM.loc['Jul',j] =','.join(k for k in Jul_list) 
            dftable_outageM.loc['Aug',j] =','.join(k for k in Aug_list) 
            dftable_outageM.loc['Sep',j] =','.join(k for k in Sep_list) 
            dftable_outageM.loc['Oct',j] =','.join(k for k in Oct_list) 
            dftable_outageM.loc['Nov',j] =','.join(k for k in Nov_list) 
            dftable_outageM.loc['Dec',j] =','.join(k for k in Dec_list) 
        
        dftable = sort_table_dates('Offline date', 'Return date', dftable, 'sk_table_1')
        
        
        return dfchart1, dfchart2, dftable, dfplanned_data, dfoffline, dftableMkr, dftable_outageM
        
    def chart_date_previous(dfKHNP_Outage_hist, skplantdict):
        
        today=datetime.date.today()
        pre_date = today - relativedelta(months = 1)
        
        #find the M-1 hist data
        dfKHNP_Outage_hist.sort_values(by = 'create date', inplace = True)
        dfKHNP_Outage_hist.set_index('create date', inplace=True)
        dfKHNP_Outage_hist = dfKHNP_Outage_hist.loc[pre_date:]
        dfKHNP_Outage_hist = dfKHNP_Outage_hist.loc[dfKHNP_Outage_hist.index[0]]
        dfKHNP_Outage_hist.reset_index(inplace=True)
        dfKHNP_Outage_hist.drop_duplicates(subset=['PlantName FP'], inplace=True)
        #print(dfKHNP_Outage_hist)
        
        
        dftable = pd.DataFrame(index = dfKHNP_Outage_hist['PlantName FP'].to_list(), columns=['D','D-1','D-7','D-30','D-365']) #dfjpnuk.columns[:-1]
        
        #print(jpplantdict)
        dftable['operate'] = np.nan
        dftable['MW'] = np.nan
        dftable['Offline date'] = np.nan
        dftable['Return date'] = np.nan
        dftable['Current Days Offline'] = np.nan
        #print(dfjpex_outage_FP[dfjpex_outage_FP['PlantName FP']=='Mihama Unit 3'].index.tolist())
        #print(dfop)
        #print(dfKHNP_Outage_hist)
       #print(dftable.index)
        for i in dftable.index:
            #print(i)
            try:
                #dftable.loc[i, 'operate'] = dfop[i].values
                dftable.loc[i, 'MW'] = skplantdict[i]
                dftable.loc[i,'Offline date'] = dfKHNP_Outage_hist['OfflineDate'].loc[dfKHNP_Outage_hist[dfKHNP_Outage_hist['PlantName FP']==i].index.tolist()[0]].date()
                dftable.loc[i,'Return date'] = dfKHNP_Outage_hist['ReturnDate'].loc[dfKHNP_Outage_hist[dfKHNP_Outage_hist['PlantName FP']==i].index.tolist()[0]].date()
                dftable.loc[i,'Current Days Offline'] = dfKHNP_Outage_hist['CurrentDaysOffline'].loc[dfKHNP_Outage_hist[dfKHNP_Outage_hist['PlantName FP']==i].index.tolist()[0]]#.astype('int')
            except (IndexError, KeyError):
                #print(i, )
                #dftable.loc[i, 'operate'] = 'AA'
                dftable.loc[i, 'MW'] = skplantdict[i]
        
        #print(dftable)
        #Chart 1 planned data
        end_date = str((today+relativedelta(months=12)).year)+'-'+str((today+relativedelta(months=12)).month)+'-'+str(calendar.monthrange((today+relativedelta(months=12)).year, (today+relativedelta(months=12)).month)[1])
        dfplanned_data = pd.DataFrame(index = dftable.index[1:] , columns = pd.date_range(start = str(today.year)+'-01-01', end = end_date).date)
        #print(dfplanned_data)
        for i in dfplanned_data.index:
            for j in dfplanned_data.columns:
                try:
                    #print(j <= dftable.loc[i,'Offline date'])
                    #print(j >= dftable.loc[i,'Return date'])
                    if j <= dftable.loc[i,'Offline date'] or j >= dftable.loc[i,'Return date']:
                        dfplanned_data.loc[i,j] = dftable.loc[i, 'MW']
                    else:
                        dfplanned_data.loc[i,j] = np.nan
                except TypeError:
                    pass
        dfplanned_data = dfplanned_data.T
        
        dfplanned_data['Planned '+str(today.year)] = np.nan
        dfplanned_data.loc[today:pd.to_datetime(str(today.year)+'-12-31'),'Planned '+str(today.year)] = dfplanned_data.loc[today:pd.to_datetime(str(today.year)+'-12-31')].sum(axis=1)   
        dfplanned_data['Planned '+str(today.year+1)] = np.nan
        dfplanned_data.loc[today:pd.to_datetime(end_date),'Planned '+str(today.year+1)] = dfplanned_data.loc[today:pd.to_datetime(end_date)].sum(axis=1) 
        #dfplanned_data.loc['sum'] = dfplanned_data.sum()
        #print(dfplanned_data)
        
        dfplanned_data_last = dfplanned_data.copy()
        
        return dfplanned_data_last
        
    
    
    def chart(dfchart1, dfchart2, dftable,dfplanned_data,dfplanned_data_last,dfKHNP_outage_FP, dfofflineKR, dftableM, dftable_outageM):
        
        today=datetime.date.today()
        
        #print(dfchart1.loc[:today])
        #print(dfplanned_data.loc[today:])
        
        chart1 = go.Figure()
        chart1.add_trace(go.Scatter(x=dfchart1.index, y=dfchart1[str(today.year)],
                            mode='lines',
                            name=str(today.year),
                            line=dict(color='black', dash='solid')
                            ))
        chart1.add_trace(go.Scatter(x=dfchart1.index, y=dfchart1[str(today.year-1)],
                            mode='lines',
                            name=str(today.year-1),
                            line=dict(color='red', dash='solid')
                            ))
        chart1.add_trace(go.Scatter(x=dfchart1.index, y=dfchart1[str(today.year-2)],
                            mode='lines',
                            name=str(today.year-2),
                            line=dict(color='grey', dash='solid')
                            ))
        chart1.add_trace(go.Scatter(x=dfchart1.index, y=dfplanned_data['Planned '+str(today.year)],
                            mode='lines',
                            name='Planed '+str(today.year),
                            line=dict(color='green', dash='dot')
                            ))
        chart1.add_trace(go.Scatter(x=dfchart1.index, y=dfplanned_data['Planned '+str(today.year+1)].loc[pd.to_datetime(str(today.year+1)+'-01-01'):],
                            mode='lines',
                            name='Planned '+str(today.year+1),
                            line=dict(color='orange', dash='dot')
                            ))
        chart1.add_trace(go.Scatter(x=dfchart1.index, y=dfplanned_data_last['Planned '+str(today.year)],
                            mode='lines',
                            name='Planned '+str(today.year)+' M-1',
                            line=dict(color='green', dash='dash'),
                            visible = 'legendonly'
                            ))
        chart1.add_trace(go.Scatter(x=dfchart1.index, y=dfplanned_data_last['Planned '+str(today.year+1)].loc[pd.to_datetime(str(today.year+1)+'-01-01'):],
                            mode='lines',
                            name='Planned '+str(today.year+1)+' M-1',
                            line=dict(color='orange', dash='dash'),
                            visible = 'legendonly'
                            ))
        
        chart1.add_trace(go.Scatter(x=dfchart1.index, y=dfchart1['MAX OP Capacity'],
                            mode='lines',
                            name='MAX OP Capacity',
                            line=dict(color='blue', dash='dot')
                            ))
        '''
        chart1.add_trace(go.Scatter(x=dfchart1.index, y=dfplanned_data_last['Planned 2022'],
                            mode='lines',
                            name='Previous 2022',
                            line=dict(color='blue', dash='dot')
                            ))
        '''
        chart1.update_layout(
             autosize=True,
             showlegend=True,
             #colorscale='RdBu',
             #legend=dict(x=0, y=-0.2),
             legend_orientation="h",
             title_text='S.Korea Daily Nuclear MW '+str(today),
             #xaxis = dict(dtick="M1"),
             #xaxis_tickformat = '%B',
             hovermode='x unified',
             plot_bgcolor = 'white',
             yaxis = dict(showgrid=True, gridcolor='lightgrey'),
    
             #template='ggplot2'  
         )
        #chart 2
        
        chart2_color = {'Hanbit Unit - 1':'#DAE3F3',
                       'Hanbit Unit - 2':'#C3D1EB',
                       'Hanbit Unit - 3':'#B0C3E6',
                       'Hanbit Unit - 4':'#A3B9E1',
                       'Hanbit Unit - 5':'#94AEDC',
                       'Hanbit Unit - 6':'#85A2D7',
                       'Hanul Unit - 1':'#FFF2CC',
                       'Hanul Unit - 2':'#FFECB7',
                       'Hanul Unit - 3':'#FFE8A7',
                       'Hanul Unit - 4':'#FFE089',
                       'Hanul Unit - 5':'#FFD96D',
                       'Hanul Unit - 6':'#FFD253',
                       'Kori Unit - 2':'#D9D9D9',
                       'Kori Unit - 3':'#CFCFCF',
                       'Kori Unit - 4':'#BABABA',
                       'Kori Shin - 1':'#E2F0D9',
                       'Kori Shin - 2':'#CCE5BD',
                       'Saeul Saeul 1':'#B7DAA2', 
                       'Saeul Saeul 2':'#A4D189', 
                       'Wolsong Shin - 1':'#F9BDDB',
                       'Wolsong Shin - 2':'#F7A3CD',
                       'Wolsong Unit - 1':'#F6B2A4', 
                       'Wolsong Unit - 2':'#F4A190', 
                       'Wolsong Unit - 3':'#F28E7A', 
                       'Wolsong Unit - 4':'#F0826C',
                       'Shin-Hanul 1':'#ECB2F4',
                        'Shin-Hanul 2':'#E89CF2',
                        'Shin-Kori 5':'#DF7BED',
                        'Shin-Kori 6':'#D961E9',
                        'Shinhanul 1':'#F5FFFA'
                       }
        
        chart2 = go.Figure()
        
        for i in dfchart2.columns:
            chart2.add_trace(go.Scatter(x=dfchart2.index, y=dfchart2[i],
                                #mode='lines',
                                mode='none',
                                name=i,
                                stackgroup='one',
                                fillcolor=chart2_color[i],
                                line_color=chart2_color[i],
                                ))
       
        chart2.update_layout(
             autosize=True,
             showlegend=True,
             #colorscale='RdBu',
             legend=dict(x=0, y=-0.2),
             legend_orientation="h",
             title_text='S.Korea Daily Nuclear MW by Plant '+str(today),
             #xaxis = dict(dtick="M1"),
             #xaxis_tickformat = '%B',
             #hovermode='x unified',
             plot_bgcolor = 'white',
             yaxis = dict(showgrid=True, gridcolor='lightgrey'),
             #barmode='relative' 
             #template='ggplot2'  
         )
        
        
        #table
        #print(dftable)
        
        dfcolor=pd.DataFrame('white', index = dftable.index, columns = dftable.columns)
        dfcolor.iloc[0] = ['yellow','white','white','white','white','white','white','white','white','white']
        dfcolor.iloc[1] = ['red','red','red','red','red','white','white','white','white','white']
        
        for i in dftable.index[2:]:
            for j in dftable.columns[0:5]:
                '''
                if dftable.loc[i,j] == '0.0':
                    dfcolor.loc[i,j] = 'white'
                if dftable.loc[i,j] == ' ':
                    dfcolor.loc[i,j] = 'white'
                else:
                    dfcolor.loc[i,j] = 'paleturquoise'
                '''    
                if dftable.loc[i,j] > 0:
                    dfcolor.loc[i,j] = 'paleturquoise'
                
                else:
                    dfcolor.loc[i,j] = 'white'   
            try:
                if today <= dftable.loc[i,'Offline date'] <= today+relativedelta(days = 30):
                    dfcolor.loc[i,'Offline date'] = '#FBDFDF'
                    
                if today <= dftable.loc[i,'Return date'] <= today+relativedelta(days = 30):
                        dfcolor.loc[i,'Return date'] = 'PaleGreen'
            except TypeError:
            
                dfcolor.loc[i,'Return date'] = '#D9D9D9'
                    
        dfcolor.reset_index(inplace=True) 
        #print(dfcolor)
        dfcolor['index'] = '#D9D9D9' 
        
        
        dfcolor = dfcolor.T            
        #dftable
        table1 = go.Figure(
                data=[go.Table(
                header=dict(values=['Delivery Date']+list(dftable.columns),
                            line_color='darkslategray',
                            fill_color='royalblue',
                            align=['center'],
                            font=dict(color='white'),
                            ),
                cells=dict(values=[dftable.index] + [dftable[pm] for pm in dftable.columns],
                           #values=[dfwind_wave.]
                           line_color='white',#'darkslategray',
                            fill=dict(color=dfcolor.values.tolist()),
                            align=[ 'center'],
                            #font_size=12,
                            #height=30,
                            
                            ))
            ])
        
        #table2
        index_list = dftable.index.to_list()[2:]
        index_list_FP = dfKHNP_outage_FP['PlantName FP'].to_list()

        for i in dftable.index.to_list()[2:]:
            if i not in index_list_FP:
                index_list.remove(i)
                
        for j in index_list_FP:
            if j not in index_list:
                index_list.append(j)
        #print(index_list)
        dftable2 = dfKHNP_outage_FP.copy()
        dftable2.set_index('PlantName FP', inplace=True)
        dftable2 = dftable2.loc[index_list]
        dftable2.reset_index(inplace=True)
        #dftable2['OfflineDate'] = dftable2['OfflineDate'].dt.strftime('%Y-%m-%d')
        #dftable2['ReturnDate'] = dftable2['OfflineDate'].dt.strftime('%Y-%m-%d')
        for i in dftable2.index:
            try:
                dftable2.loc[i, 'OfflineDate'] = dftable2.loc[i, 'OfflineDate'].strftime('%Y-%m-%d')
                dftable2.loc[i, 'ReturnDate'] = dftable2.loc[i, 'ReturnDate'].strftime('%Y-%m-%d')
            except ValueError:
                pass
        #print(dftable2)
        #dftable2 = dfKHNP_outage_FP.copy()
        

        dftable2 = dftable2[['PlantName','UnitName','Comment','OfflineDate','ReturnDate', 'DaysDiffOffline','DaysDiffReturn','DaysDiffTotal','CurrentDaysOffline']]
        #dftable2.sort_values(by='PowerPlantName', inplace = True)
        #dftable2.loc[:,['StopDateAndTime','ScheduledRecoveryDate']] = pd.to_datetime(dftable2.loc[:,['StopDateAndTime','ScheduledRecoveryDate']]).date
        dftable2 = sort_table_dates('OfflineDate', 'ReturnDate', dftable2, 'sk_table_2')
        
        table2 = go.Figure(
                data=[go.Table(
                header=dict(values=list(dftable2.columns),
                            line_color='darkslategray',
                            fill_color='royalblue',
                            align=['center'],
                            font=dict(color='white'),
                            ),
                cells=dict(values= [dftable2[pm] for pm in dftable2.columns],
                           #values=[dfwind_wave.]
                           line_color='white',#'darkslategray',
                            #fill=dict(color=dfcolor.values.tolist()),
                            align=[ 'center'],
                            #font_size=12,
                            #height=30,
                            
                            ))
            ])
        
        
        
        
         #offline days bar chart
 
        figoffline = go.Figure()
        figoffline.add_trace(go.Bar(x=dfofflineKR.columns, y=dfofflineKR.loc[str(today.year-2)],
                #base=[-500,-600,-700],
                #marker_color='crimson',
                name=str(today.year-2)))
        figoffline.add_trace(go.Bar(x=dfofflineKR.columns, y=dfofflineKR.loc[str(today.year-1)],
                #base=[-500,-600,-700],
                #marker_color='crimson',
                name=str(today.year-1)))
        figoffline.add_trace(go.Bar(x=dfofflineKR.columns, y=dfofflineKR.loc[str(today.year)],
                #base=[-500,-600,-700],
                #marker_color='crimson',
                name=str(today.year)))
        
        figoffline.update_layout(
            autosize=True,
            showlegend=True,
            #colorscale='RdBu',
            #legend=dict(x=0, y=-0.2),
            legend_orientation="h",
            title_text='S.Korea Nuclear Plants Offline Days',
            yaxis_title="days",
            #xaxis = dict(dtick="M1"),
            #xaxis_tickformat = '%B',
            #hovermode='x unified',
            plot_bgcolor = 'white',
            yaxis = dict(showgrid=True, gridcolor='lightgrey'),
            #barmode='relative' 
            #template='ggplot2'  
        )
        
        
        tableM = go.Figure(
                data=[go.Table(
                header=dict(values=['Plant']+list(dftableM.columns),
                            line_color='darkslategray',
                            fill_color='royalblue',
                            align=['center'],
                            font=dict(color='white'),
                            ),
                cells=dict(values=[dftableM.index] + [dftableM[pm] for pm in dftableM.columns],
                           #values=[dfwind_wave.]
                           line_color='white',#'darkslategray',
                            #fill=dict(color=dfcolor.values.tolist()),
                            align=[ 'center'],
                            #font_size=12,
                            #height=30,
                            
                            ))
            ])
        tableM.update_layout(title_text='S.Korea Nuclear Plants Past 3 Years Offline Days')
        
        table_outageM = go.Figure(
                data=[go.Table(
                header=dict(values=['Month']+list(dftable_outageM.columns),
                            line_color='darkslategray',
                            fill_color='royalblue',
                            align=['center'],
                            font=dict(color='white'),
                            ),
                cells=dict(values=[dftable_outageM.index] + [dftable_outageM[pm] for pm in dftable_outageM.columns],
                           #values=[dfwind_wave.]
                           line_color='white',#'darkslategray',
                            #fill=dict(color=dfcolor.values.tolist()),
                            align=[ 'center'],
                            #font_size=12,
                            #height=30,
                            
                            ))
            ])
        table_outageM.update_layout(title_text='S.Korea Nuclear Plants Past 3 Years Offline Days by Month')
        
        py.plot(chart1, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/nuk/SK chart1.html', auto_open=False)
        py.plot(chart2, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/nuk/SK chart2.html', auto_open=False)
        py.plot(table1, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/nuk/SK table.html', auto_open=False)
        py.plot(table2, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/nuk/SK table2.html', auto_open=False)
        py.plot(figoffline, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/nuk/SK figoffline.html', auto_open=False)
        py.plot(tableM, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/nuk/SK figofflineM.html', auto_open=False)
        py.plot(table_outageM, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/nuk/SK figoutageM.html', auto_open=False)
        
        
        chart1.write_image("U:/Trading - Gas/LNG/LNG website/analysis/nuk/SK chart1.png")
        chart2.write_image("U:/Trading - Gas/LNG/LNG website/analysis/nuk/SK chart2.png")
        table1.write_image("U:/Trading - Gas/LNG/LNG website/analysis/nuk/SK table.png")
        table2.write_image("U:/Trading - Gas/LNG/LNG website/analysis/nuk/SK table2.png")
        figoffline.write_image("U:/Trading - Gas/LNG/LNG website/analysis/nuk/SK figoffline.png")
        tableM.write_image("U:/Trading - Gas/LNG/LNG website/analysis/nuk/SK figofflineM.png")
        table_outageM.write_image("U:/Trading - Gas/LNG/LNG website/analysis/nuk/SK figoutageM.png")
        
'''
dfsknuk, dfKHNP_outage = SK_Nuk_Outage.get_data()
dfKHNP_outage_FP = SK_Nuk_Outage.KHNP_data(dfKHNP_outage)
skplantdict, dfop = SK_Nuk_Outage.planned_data(dfsknuk)
dfchart1, dfchart2, dftable, dfplanned_data = SK_Nuk_Outage.chart_data(dfsknuk, skplantdict, dfKHNP_outage_FP, dfop)
SK_Nuk_Outage.chart(dfchart1, dfchart2, dftable, dfplanned_data)
'''

def sort_table_dates(offline_date_column, return_date_column, table, nuk_type):
    today_date = datetime.datetime.now().date()
    table[offline_date_column] = pd.to_datetime(table[offline_date_column], errors='coerce').dt.date
    table[return_date_column] = pd.to_datetime(table[return_date_column], errors='coerce').dt.date
    table['MostRecentDate'] = table.apply(lambda x: get_most_recent_date(x, today_date, offline_date_column, return_date_column), axis=1)
    if nuk_type == 'sk_table_1' or nuk_type == 'jp_table_1':
        table = sort_table_1(table, offline_date_column, return_date_column, today_date)
    else:
        table = sort_table_2(table, offline_date_column, return_date_column, today_date)
    table = table.drop('MostRecentDate', axis=1)
    table[offline_date_column] = table[offline_date_column].fillna('')
    table[return_date_column] = table[return_date_column].fillna('')
    return table


def sort_table_1(table, offline_date_column, return_date_column, today_date):
    plants = table.iloc[2:, :]
    future_dates = plants[(plants[return_date_column] >= today_date) | (plants[offline_date_column] >= today_date)]. \
        sort_values(by='MostRecentDate', ascending=True)
    past_dates = plants[(plants[return_date_column] < today_date) | ((plants[return_date_column].isna())
                        & ((plants[offline_date_column] < today_date) | plants[offline_date_column].isna()))
                        ].sort_values(by='MostRecentDate', ascending=False)
    not_sorted = table.iloc[0:2, :]
    return pd.concat([not_sorted, future_dates, past_dates])


def sort_table_2(table, offline_date_column, return_date_column, today_date):
    future_dates = table[(table[return_date_column] >= today_date) | (table[offline_date_column] >= today_date)]. \
        sort_values(by='MostRecentDate', ascending=True)
    past_dates = table[(table[return_date_column] < today_date) | ((table[return_date_column].isna())
                        & ((table[offline_date_column] < today_date) | table[offline_date_column].isna()))
                        ].sort_values(by='MostRecentDate', ascending=False)
    return pd.concat([future_dates, past_dates])


def get_most_recent_date(row, today_date, offline_date_column, return_date_column):
    return row[offline_date_column] if row[offline_date_column] > today_date \
           or (~pd.isnull(row[offline_date_column]) and pd.isnull(row[return_date_column])) \
           else row[return_date_column]


class Nuk_Outage():
    
    def update():
        
        #update JP
        dfjpnuk, dfjpnuk_previous, dfJP_outage_date, dfJP_outage_date_previous, dfjpex_outage,dfjpex_outage_comments = JP_Nuk_Outage.get_data()
        dfjpex_outage_FP, reduction, dfJPEX_Outage_hist = JP_Nuk_Outage.jpex_data(dfjpex_outage,dfjpex_outage_comments, dfJP_outage_date)
        jpplantdict, dfop, oplist = JP_Nuk_Outage.planned_data(dfjpex_outage_FP, dfJP_outage_date, dfJP_outage_date_previous, dfjpnuk, dfjpnuk_previous)
        dfchart1, dfchart2, dftable, dfplanned_data, dfoffline, dftableM, dftable_outageM = JP_Nuk_Outage.chart_data(dfjpnuk, jpplantdict, dfjpex_outage_FP, dfop, oplist, reduction)
        dfplanned_data_last = JP_Nuk_Outage.chart_date_previous(dfJPEX_Outage_hist,jpplantdict)
        JP_Nuk_Outage.chart(dfchart1, dfchart2, dftable, dfplanned_data,dfplanned_data_last, dfjpex_outage_FP, dfoffline, dftableM, dftable_outageM)
        #print(dfJP_outage_date)
        #data into db
        db_server_lng = "PRD-DB-SQL-211"
        db_name_lng = "LNG"
        sql_engine_lng = sqlalchemy.create_engine("mssql+pyodbc://" + db_server_lng + "/" + db_name_lng + "?trusted_connection=yes&driver=ODBC+Driver+13+for+SQL+Server")
        dfplanned_data['timestamp'] = datetime.datetime.now()
        dfplanned_data.to_sql(name='JPNukPlanned', con=sql_engine_lng, index=True, if_exists='append', schema='ana')
        
        #update KR
        dfsknuk, dfKHNP_outage = SK_Nuk_Outage.get_data()
        dfKHNP_outage_FP, dfKHNP_Outage_hist = SK_Nuk_Outage.KHNP_data(dfKHNP_outage)
        skplantdict, dfop, oplist = SK_Nuk_Outage.planned_data(dfsknuk)
        dfchart1, dfchart2, dftable, dfplanned_data, dfofflineKR, dftableM, dftable_outageM = SK_Nuk_Outage.chart_data(dfsknuk, skplantdict, dfKHNP_outage_FP, dfop, oplist)
        dfplanned_data_last = SK_Nuk_Outage.chart_date_previous(dfKHNP_Outage_hist,skplantdict)
        SK_Nuk_Outage.chart(dfchart1, dfchart2, dftable, dfplanned_data,dfplanned_data_last,dfKHNP_outage_FP, dfofflineKR, dftableM, dftable_outageM)
        #print(dfsknuk)
        
       
        dfplanned_data['timestamp'] = datetime.datetime.now()
        dfplanned_data.to_sql(name='KRNukPlanned', con=sql_engine_lng, index=True, if_exists='append', schema='ana')
#Nuk_Outage.update()