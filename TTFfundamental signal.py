# -*- coding: utf-8 -*-
"""
Created on Thu Mar 16 13:51:49 2023

@author: SVC-GASQuant2-Prod
"""





import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from numpy import concatenate
import datetime
from pandas.tseries.offsets import BDay

from sklearn.preprocessing import StandardScaler
from sklearn.neighbors import KNeighborsRegressor
from plotly.offline import download_plotlyjs, init_notebook_mode,  plot
from plotly.graph_objs import *
#from plotly.offline import plot
import plotly.graph_objs as go
import plotly.offline as py
from plotly.subplots import make_subplots

from dateutil.relativedelta import relativedelta
import sys
sys.path.append('H:\Yuefeng\code\classes') 
from DBtoDF import DBTOPD
import pyodbc
from io import StringIO
from os import listdir
import sqlalchemy
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.combining import OrTrigger
from apscheduler.triggers.cron import CronTrigger
pd.set_option('display.max_columns',20)

init_notebook_mode()

daysbefore=0 #which day 0=today 1=yesterday ...

class TTF_DASA():
    
    def date_dict():
        
        today = datetime.date.today()
        
        if  4<=today.month<=9:
            #sa1 = 'Win '+str(today.year)[2:]
            #sa1start = str(today.year)+'-10-01'
            train_end = str(today.year)+'-03-31'
            
            train_win_end = str(today.year)+'-03-31'
            train_sum_end = str(today.year-1)+'-09-30'
            
            target_win_start =  str(today.year)+'-10-01'
            target_win_end1 = str(today.year+1)+'-03-31'
            target_win_end2 = str(today.year+1)+'-03-31'
            
            target_sum_start = str(today.year+1)+'-04-01'
            target_sum_end1 = str(today.year)+'-09-30'
            target_sum_end2 = str(today.year+1)+'-09-30' #no use
            target_end_date = str(today.year+1)+'-09-30'
            
            #sa2start = str(today.year+1)+'04-01'
            #sa2end = str(today.year+1)+'09-30'
            
        elif  10<=today.month<=12:
            #sa1 = 'Sum '+str(today.year+1)[2:]
            
            #sa1start = str(today.year+1)+'04-01'
            #sa1end = str(today.year+1)+'09-30'
            #sa2start = str(today.year+1)+'-10-01'
            #sa2end = str(today.year+2)+'03-31'
            train_end = str(today.year)+'-09-30'
            
            train_win_end = str(today.year)+'-03-31'
            train_sum_end = str(today.year)+'-09-30'
            
            target_win_start =  str(today.year+1)+'-10-01'
            target_win_end1 = str(today.year+1)+'-03-31'
            target_win_end2 = str(today.year+2)+'-03-31'
            
            target_sum_start = str(today.year+1)+'-04-01'
            target_sum_end1 = str(today.year+1)+'-09-30'
            target_sum_end2 = str(today.year+1)+'-09-30' #no use
            target_end_date = str(today.year+2)+'-03-31'
            
        else:
            #sa1 = 'Sum '+str(today.year)[2:]
            #sa1start = str(today.year)+'04-01'
            #sa1end = str(today.year)+'09-30'
            #sa2start = str(today.year)+'-10-01'
            #sa2end = str(today.year+1)+'03-31'
            train_end = str(today.year-1)+'-03-31'
            
            train_win_end = str(today.year-1)+'-03-31'
            train_sum_end = str(today.year-1)+'-09-30'
            
            target_win_start = str(today.year)+'-10-01'
            target_win_end1 = str(today.year)+'-03-31'
            target_win_end2 = str(today.year+1)+'-03-31'
            
            target_sum_start = str(today.year)+'-04-01'
            target_sum_end1 = str(today.year)+'-09-30'
            target_sum_end2 = str(today.year)+'-09-30' #no use
            target_end_date = str(today.year+1)+'-03-31'
        
            
        
        date_dict={
                   'train_start' : '2012-01-01', #initial train start
                   'train_end' : train_end,#'2022-03-31', #train end
                   'end_date' : '2021-12-31', #
                   'train_win_end':train_win_end,#'2022-03-31', #model win end
                   'train_sum_end':train_sum_end,#'2021-09-30', #model sum end
                   'target_win_start':target_win_start,#'2022-10-01',
                   'target_win_end1':target_win_end1,#'2023-03-31',
                   'target_win_end2':target_win_end2,
                   'target_sum_start':target_sum_start,
                   'target_sum_end1':target_sum_end1,#'2022-09-30',
                   'target_sum_end2':target_sum_end2,
                   'target_end_date':target_end_date,#'2023-03-31',
                   #'last_day' : datetime.date.today() - datetime.timedelta(days=1)*1,
                   'today' : datetime.date.today(),
                   #'last_month': str(datetime.date.today().year)+'-'+str(datetime.date.today().month-1)
                   }
        
        #print(date_dict)
        
        return date_dict
    
    
    def inventory_hist():
        
        ce=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'CEinventoryNWEITCEE')
        dfce=ce.sql_to_df()
        dfce.fillna(0, inplace=True)
        dfce.set_index('Date', inplace=True)
        
        dfce = dfce.loc['2012-01-01':]
            
        dfeur = pd.DataFrame()
        dfeur['Europe_Storages_Total_Inventory'] = dfce.sum(axis=1)
        #dfeur.plot()
        #print(dfeur)
        return dfeur
        
    
    def DASA():
        
        today = datetime.date.today()
        
        dfdahist = pd.read_excel('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/DASA.xlsx', sheet_name = 'DA',header=0)
        dfsahist = pd.read_excel('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/DASA.xlsx', sheet_name = 'SA',header=0)
        dfdahist.set_index('date', inplace=True)
        dfsahist.set_index('date', inplace=True)


        dfdasahist = pd.DataFrame(index = pd.date_range(start = '2013-01-01', end = today))
        dfdasahist['DA'] = dfdahist['TRNLTTFDA']
        dfdasahist['SA'] = dfsahist['TRNLTTTFSc1']
        dfdasahist.dropna(inplace=True)
        #print(dfdasahist)
        da = DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'TTFDA')
        dfda = da.sql_to_df()
        dfda.set_index('date', inplace = True)
        
        sa = DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'TTFSA')
        dfsa = sa.sql_to_df()
        dfsa.set_index('date', inplace=True)
        
        
        dfdasanew = pd.DataFrame(index = pd.date_range(start = dfdasahist.index[-1]+relativedelta(days=1), end = today))
        
        dfdasanew['DA'] = dfda['value']
        dfdasanew['SA'] = dfsa['value']
        
        
        dfdasa = pd.concat([dfdasahist, dfdasanew])
        dfdasa['DASA'] = dfdasa['DA'] - dfdasa['SA']
        #print(dfdasa)
        
        return dfdasa
    
    def inventory_fcst(date_dict):
        
        dffcst = DBTOPD.get_EU_inventory_fcst()
        
        dffcst.set_index('GasDay', inplace=True)
        dffcst.sort_index(inplace=True)
        #print(dffcst.loc[:'2022-03-30'])
        dffcst = dffcst.drop('ForecastDate', 1)
        #print(dffcst.loc[:'2023-09-30'])
        dffcst.plot()
        dffcst.rename(columns = {'Conti_IT_CEE_stock':'Europe_Storages_Total_Inventory'}, inplace=True)
        
        return dffcst
    
    
    def inventory_fcst_anna(date_dict):
        
        #UK	NL	DE_NL	DE	DE_AT	AT	BE	FR	IT	RU Gaz	DK	PL	CZ	SK
        
        dr = r'U:/Trading - Power/UK Power/Jonny/Scripts/DashApp Scripts/pickle_files/balance_saved_hist_views/'
        arr = listdir(dr)
        latest_no_upload = sorted([i for i in arr if 'eu_balance' in i], reverse=True)[0]
        balance = pd.read_pickle(dr + latest_no_upload)
        mapping = pd.read_pickle(r'U:/Trading - Power/UK Power/Jonny/Scripts/DashApp Scripts/pickle_files/mapping_hist_eu_balance2.pkl')
        balance.columns = pd.MultiIndex.from_tuples(mapping['colnames'].to_list())
        balance = balance['2013-10-01':]
        balance[[('Total Storage','Inventory'),('Total Storage','TOTAL')]]
        
        #print(balance[('Conti Storage',                            'Inventory')])
        
        df = pd.DataFrame(index = pd.date_range(start = date_dict['today'], end = date_dict['target_end_date']), columns = ['Europe_Storages_Total_Inventory'])
        df.loc[:,:] = balance.loc[date_dict['today']:date_dict['target_end_date'],[('Conti Storage', 'Inventory')]].values
        #print(df)
        return df
    
    
    def inventory_fcst_nick(date_dict):
        
        today = datetime.date.today()
        sqlConnScrape = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER=prd-db-sql-201;Trusted_Connection=yes')
        #print(yesterday)
        sql = '''
                SELECT b.*
                FROM [Sandpit].[dbo].[GEU_Balances_History] b
                where b.Label='Conti_IT_CEE_stock' and
                	b.ForecastDate = (
                		SELECT max([ForecastDate]) as [MaxFD]		 
                		FROM [Sandpit].[dbo].[GEU_Balances_History]
                		where Label='Conti_IT_CEE_stock'
                	)
                order by b.Label, b.Scenario, b.ValueDate  

              '''
        
        sql1 = '''
            SELECT b.*
                    FROM [Sandpit].[dbo].[GEU_Balances_History] b
                    where b.Label in ('UK Inventory Forecast Bal','FR H Inventory Forecast Bal','BE H Inventory Forecast Bal','ES Inventory Forecast Bal','PT Inventory Forecast Bal','NL H Inventory Forecast Bal','DE H Inventory Forecast Bal','IT Inventory Forecast Bal','PL Inventory Forecast Bal','AT CZ SK Inventory Forecast Bal')  and
                    	b.ForecastDate = (
                    		SELECT max([ForecastDate]) as [MaxFD]		 
                    		FROM [Sandpit].[dbo].[GEU_Balances_History]
                    		--where Label='Conti_IT_CEE_stock'
                    	)
                    order by b.Label, b.Scenario, b.ValueDate  
                    '''
        
        '''
        df = pd.read_sql(sql, sqlConnScrape)
        df.set_index('ValueDate', inplace=True)
        
        dfeur = pd.DataFrame(index = pd.date_range(start = date_dict['today'], end = date_dict['target_end_date']), columns = ['Europe_Storages_Total_Inventory'])
        dfeur['Europe_Storages_Total_Inventory'] = df.loc[date_dict['today']:date_dict['target_end_date'],'Value']
        #print(dfeur)
        '''
        df = pd.read_sql(sql1, sqlConnScrape)
        dfpivot = pd.pivot_table(df,index = 'ValueDate', columns='Label')
        #print(dfpivot.sum(axis=1).loc[date_dict['today']:])
        #df.set_index('ValueDate', inplace=True)
        dfpivot['Europe_Storages_Total_Inventory'] = dfpivot.sum(axis=1)
        dfeur = pd.DataFrame(index = pd.date_range(start = date_dict['today'], end = date_dict['target_end_date']), columns = ['Europe_Storages_Total_Inventory'])
        dfeur['Europe_Storages_Total_Inventory'] = dfpivot.loc[date_dict['today']:date_dict['target_end_date'],'Europe_Storages_Total_Inventory']
        
        
        
        #dfeur.plot()
        #print(dfeur)
        return dfeur
    
    def train_target_data(dfinventory_hist, dfinventory_fcst, dfdasa, date_dict):
        
        #get new withdraw
        dfinventory_hist['Europe_Storages_Total_NetWith'] = dfinventory_hist['Europe_Storages_Total_Inventory'] - dfinventory_hist['Europe_Storages_Total_Inventory'].shift(1) 
        #start and end date for train set
        dfinventory_hist = dfinventory_hist.loc['2014-01-01':'2020-09-30'].copy()
        
        #train can move to db
        dfinventory_hist['year'] = dfinventory_hist.index.year
        dfinventory_hist['month'] = dfinventory_hist.index.month
        dfinventory_hist['day'] = dfinventory_hist.index.day
        
        #create inventory normal
        inventory_normal = pd.DataFrame(index = pd.date_range(start = dfinventory_hist.index[0], end = dfinventory_fcst.index[-1]), columns=['Europe_Storages_Total_Inventory_Normal'])
        for i in inventory_normal.index:
            inventory_normal.loc[i,'Europe_Storages_Total_Inventory_Normal'] =dfinventory_hist[(dfinventory_hist['month'] == i.month) & (dfinventory_hist['day'] == i.day)].Europe_Storages_Total_Inventory.mean()
        
        dfinventory_fcst['Europe_Storages_Total_NetWith'] = dfinventory_fcst['Europe_Storages_Total_Inventory'] - dfinventory_fcst['Europe_Storages_Total_Inventory'].shift(1) 
        dfinventory_fcst = dfinventory_fcst.loc[date_dict['today']:]
        
        
        #calc uti rate
        total_size=dfinventory_hist['Europe_Storages_Total_Inventory'].max()
        injectioncapacity=dfinventory_hist.loc[:,'Europe_Storages_Total_NetWith'].values.min()
        withdrawalcapacity=dfinventory_hist.loc[:,'Europe_Storages_Total_NetWith'].values.max()
        
        df=pd.concat([dfinventory_hist,dfinventory_fcst],axis=0)
        df=df.interpolate(method='linear')
        daysin=total_size/injectioncapacity
        daysout=total_size/withdrawalcapacity
        #print('total_size',total_size)
        #print('injectioncapacity',injectioncapacity)
        #print('withdrawalcapacity',withdrawalcapacity)
        #print('daysin',daysin)
        #print('daysout',daysout)
        
        max_inventory=df['Europe_Storages_Total_Inventory'].max()
        
        df['stock_lvl']=df['Europe_Storages_Total_Inventory']/max_inventory
        df['stock level']=(-1)*df['Europe_Storages_Total_Inventory']/inventory_normal['Europe_Storages_Total_Inventory_Normal']
        
        nominal_injection_capacity = 70000/140#total_size/daysin #70000/140
        nominal_withdraw_capacity= 70000/85#total_size/daysout        #70000/85
        
        real_injection_capacity=np.interp(x=df['stock_lvl'], xp=[0,0.65,1], fp=[1,1,0.55])
        df['real_injection_capacity']=real_injection_capacity
        real_withdraw_capacity=np.interp(x=df['stock_lvl'], xp=[0,0.45,1], fp=[0.3,1,1])
        df['real_withdraw_capacity']=real_withdraw_capacity
        utilization_rate=[]
        injection_capacity=[]
        withdrawal_capacity=[]
        for i in range(0,df.shape[0]): 
            if df['Europe_Storages_Total_NetWith'].iloc[i] <= 0:
                utilization_rate.append(df['Europe_Storages_Total_NetWith'].iloc[i]/(df['real_injection_capacity'].iloc[i]*nominal_injection_capacity))
                injection_capacity.append(df['Europe_Storages_Total_NetWith'].iloc[i]/(df['real_injection_capacity'].iloc[i]*nominal_injection_capacity))
                withdrawal_capacity.append(0)
        
            else:
                utilization_rate.append(df['Europe_Storages_Total_NetWith'].iloc[i]/(df['real_withdraw_capacity'].iloc[i]*nominal_withdraw_capacity))
                withdrawal_capacity.append(df['Europe_Storages_Total_NetWith'].iloc[i]/(df['real_withdraw_capacity'].iloc[i]*nominal_withdraw_capacity))
                injection_capacity.append(0)
        df['utilization_rate']=utilization_rate
        df['injection_capacity']=injection_capacity
        df['withdrawal_capacity']=withdrawal_capacity
        
        df=df.drop(['stock_lvl',
                    'real_injection_capacity','real_withdraw_capacity','injection_capacity','withdrawal_capacity',
                    #'Europe_Storages_Total_Inventory_Normal',
                    'Europe_Storages_Total_NetWith',
                    'Europe_Storages_Total_Inventory',
                    #'year',
                    #'month',
                    #'day'
                    ],axis=1)
        #print(df)
        #df[['stock level','utilization_rate']].plot(legend=True)
        #train set
        train = df[['stock level','utilization_rate']].loc[date_dict['train_start']:date_dict['train_end']]
        #train['normal'] = inventory_normal
        train['DASA'] = dfdasa['DASA']
        train.dropna(inplace=True)
        train['quarter'] = train.index.quarter
        train['month'] = train.index.month
        '''
        #plot
        #print(train)
        #train.loc['2014-01-01':'2014-12-31'].plot.scatter(x='utilization_rate', y='DASA', label = '2014',legend=True)
        #train.loc['2015-01-01':'2015-12-31'].plot.scatter(x='utilization_rate', y='DASA', label = '2015',legend=True)
        #win plot
        ax1 = train.loc['2014-01-01':'2014-03-31'].plot(kind='scatter', x='stock level', y='DASA', color='r',legend=True, label = 'win2013')    
        ax2 = train.loc['2014-09-01':'2015-03-31'].plot(kind='scatter', x='stock level', y='DASA', color='g', ax=ax1,legend=True, label = 'win2014')    
        ax3 = train.loc['2015-09-01':'2016-03-31'].plot(kind='scatter', x='stock level', y='DASA', color='b', ax=ax1,legend=True, label = 'win2015')
        ax3 = train.loc['2016-09-01':'2017-03-31'].plot(kind='scatter', x='stock level', y='DASA', color='pink',ax=ax1,legend=True, label = 'win2016')
        ax3 = train.loc['2017-09-01':'2018-03-31'].plot(kind='scatter', x='stock level', y='DASA', color='orange',ax=ax1,legend=True, label = 'win2017')
        ax3 = train.loc['2018-09-01':'2019-03-31'].plot(kind='scatter', x='stock level', y='DASA', color='black',ax=ax1,legend=True, label = 'win2018')
        ax3 = train.loc['2019-09-01':'2020-03-31'].plot(kind='scatter', x='stock level', y='DASA', color='yellow',ax=ax1,legend=True, label = 'win2019')
        #ax3 = train.loc['2016-01-01':'2016-12-31'].plot(kind='scatter', x='utilization_rate', y='DASA', color='b', ax=ax1,legend=True)
        plt.ylim(-10, 10)
        plt.title('EU gas winter stock level level')
        plt.show()
        #sum plot
        ax1 = train.loc['2014-03-01':'2014-09-30'].plot(kind='scatter', x='stock level', y='DASA', color='r',legend=True, label = 'sum2014')    
        ax2 = train.loc['2015-03-01':'2015-09-30'].plot(kind='scatter', x='stock level', y='DASA', color='g', ax=ax1,legend=True, label = 'sum2015')    
        ax3 = train.loc['2016-03-01':'2016-09-30'].plot(kind='scatter', x='stock level', y='DASA', color='b', ax=ax1,legend=True, label = 'sum2016')
        ax3 = train.loc['2017-03-01':'2017-09-30'].plot(kind='scatter', x='stock level', y='DASA', color='pink',ax=ax1,legend=True, label = 'sum2017')
        ax3 = train.loc['2018-03-01':'2018-09-30'].plot(kind='scatter', x='stock level', y='DASA', color='orange',ax=ax1,legend=True, label = 'sum2018')
        ax3 = train.loc['2019-03-01':'2019-09-30'].plot(kind='scatter', x='stock level', y='DASA', color='black',ax=ax1,legend=True, label = 'sum2019')
        ax3 = train.loc['2020-03-01':'2020-09-30'].plot(kind='scatter', x='stock level', y='DASA', color='yellow',ax=ax1,legend=True, label = 'sum2020')
        #ax3 = train.loc['2016-01-01':'2016-12-31'].plot(kind='scatter', x='utilization_rate', y='DASA', color='b', ax=ax1,legend=True)
        plt.ylim(-10, 10)
        plt.title('EU gas summer stock level level')
        plt.show()
        '''
        #train season + shoulder month
        train_win = train[(train['quarter'] == 1) | (train['quarter'] == 4) | (train['month'] == 9)]
        train_sum = train[(train['quarter'] == 2) | (train['quarter'] == 3) | (train['month'] == 3)]
        train_win.drop(['quarter','month'], axis=1, inplace=True)
        train_sum.drop(['quarter','month'], axis=1, inplace=True)
        #print(date_dict['today'])
        #target set
        target = df[['stock level','utilization_rate']].loc[date_dict['today']:]
        #target['Europe_Storages_Total_Inventory_Normal'] = inventory_normal
        target.dropna(inplace=True)
        target['quarter'] = target.index.quarter
        target_win = target[(target['quarter'] == 1) | (target['quarter'] == 4)]
        target_sum = target[(target['quarter'] == 2) | (target['quarter'] == 3)]
        target_win.drop('quarter', axis=1, inplace=True)
        target_sum.drop('quarter', axis=1, inplace=True)
        
        #train.plot(x='utilization_rate', y='DASA', kind='scatter')
        #print(train_win)
        #print(target_win)
    
        return df, train_win, train_sum, target_win, target_sum 
        
     
        
    def knn(train_win, train_sum, target_win, target_sum, date_dict):
         
        #winter use winter...............................................................................................    
        #print(train_win)
        trainwin=train_win.loc['2014-01-01':date_dict['train_win_end']]
        #print(trainwin)
        train_Xwin,train_ywin=trainwin.loc[:,['stock level','utilization_rate']].values,trainwin.loc[:,'DASA'].values 
        train_ywin=train_ywin.reshape(-1,1)  
         
    

        #print('----------------KNN------------------')
        knnwin=KNeighborsRegressor(n_neighbors=3, #weights='distance'
                                weights='uniform')
        knnwin.fit(train_Xwin,train_ywin)
        
        #sum use summer................................................................................................  
        trainsum=train_sum.loc['2014-01-01':date_dict['train_sum_end']]
        #print(trainsum)
        train_Xsum,train_ysum=trainsum.loc[:,['stock level','utilization_rate']].values,trainsum.loc[:,'DASA'].values
        train_ysum=train_ysum.reshape(-1,1)           
        
        
        #print('----------------KNN------------------')
        #KNN
        knnsum=KNeighborsRegressor(n_neighbors=3,# weights='distance'
                                weights='uniform'
                                )
        knnsum.fit(train_Xsum,train_ysum)
        
        if  4<=date_dict['today'].month<=9: 
            
            #target summer...............................................................................................    
            targetsum1=target_sum.loc[date_dict['today']:date_dict['target_sum_end1']]
            
            target_Xsum1 = targetsum1.loc[:,['stock level','utilization_rate']].values
            #target_ysum22 = targetsum21[:,'DASA'].values  
            #target_ysum22=target_ysum22.reshape(-1,1)
            
            #forecast sum
            knn_y_predictsum1 = knnsum.predict(target_Xsum1)
            knn_y_predictsum1=knn_y_predictsum1.reshape(-1,1)
            
            #target summmer2
            targetsum2=target_sum.loc[date_dict['target_sum_start']:date_dict['target_sum_end2']]
            
            target_Xsum2 = targetsum2.loc[:,['stock level','utilization_rate']].values
            #target_ysum22 = targetsum21[:,'DASA'].values  
            #target_ysum22=target_ysum22.reshape(-1,1)
            
            #forecast sum
            knn_y_predictsum2 = knnsum.predict(target_Xsum2)
            knn_y_predictsum2=knn_y_predictsum2.reshape(-1,1)
            
            #target winter...............................................................................................    
            targetwin=target_win.loc[date_dict['target_win_start']:date_dict['target_win_end1']]
        
            target_Xwin=targetwin.loc[:,['stock level','utilization_rate']].values 
            #forecast win
            knn_y_predictwin = knnwin.predict(target_Xwin)
            knn_y_predictwin=knn_y_predictwin.reshape(-1,1)
            
            #compart resut
            
            compare=concatenate((knn_y_predictsum1, knn_y_predictwin, knn_y_predictsum2), axis=0)
            
        else:
            #target winter...............................................................................................    
            targetwin1=target_win.loc[date_dict['today']:date_dict['target_win_end1']]
        
            target_Xwin1=targetwin1.loc[:,['stock level','utilization_rate']].values 
            #forecast win
            knn_y_predictwin1 = knnwin.predict(target_Xwin1)
            knn_y_predictwin1=knn_y_predictwin1.reshape(-1,1)
            
            #target winter2...............................................................................................    
            targetwin2=target_win.loc[date_dict['target_win_start']:date_dict['target_win_end2']]
        
            target_Xwin2=targetwin2.loc[:,['stock level','utilization_rate']].values 
            #forecast win
            knn_y_predictwin2 = knnwin.predict(target_Xwin2)
            knn_y_predictwin2=knn_y_predictwin2.reshape(-1,1)
            
            
            #target summer...............................................................................................    
            targetsum=target_sum.loc[date_dict['target_sum_start']:date_dict['target_sum_end1']]
            
            target_Xsum = targetsum.loc[:,['stock level','utilization_rate']].values
            #target_ysum22 = targetsum21[:,'DASA'].values  
            #target_ysum22=target_ysum22.reshape(-1,1)
            
            #forecast sum
            knn_y_predictsum = knnsum.predict(target_Xsum)
            knn_y_predictsum = knn_y_predictsum.reshape(-1,1)
           
            #print(targetwin1)
            #print(targetwin2)
            #print(targetsum)
            #compart resut
            
            compare=concatenate((knn_y_predictwin1, knn_y_predictsum, knn_y_predictwin2), axis=0)
        
        #compare=np.transpose(compare)
        n=pd.date_range(date_dict['today'], date_dict['target_end_date'], freq='D').shape[0]
        compare_df=pd.DataFrame(compare,index=pd.date_range(start=date_dict['today'],freq='D',periods=n),
                        columns=['knn'])    
        #print(compare_df)
        #plot
        #compare_df['knn'].plot(figsize=(16,6),grid=True,fontsize=15)
        
        #plt.title('KNN DASA FCST',fontsize=12)
        #plt.ylabel('DASA',fontsize=15)
        #plt.legend(loc='best',fontsize=12)
        #plt.show()
        
        #compare_df.to_csv('H:/MARKET/MTTF/Gas/Python - General/Yuefeng/data/utilization_knn'+str(today)+'.csv')
        #compare_df.to_csv('H:/MARKET/MTTF/Gas/Python - General/Yuefeng/data/utilization_knn.csv')
        #print(compare_df.loc['2022-10-01':'2022-10-31'].mean())
        #print(compare_df)

         
        return  compare_df
         
              
    
    
    def get_trade_price():
    #%% Read in data and merge
        today =  datetime.datetime.today()
        #print((today-BDay(1)).date())
        dftrayport_hist = DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'TTF_trayport_daily').sql_to_df()
        dftrayport_hist.set_index('DateTime', inplace=True)
        #print(dftrayport_hist)
        
        '''
        ttfma1 = DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'BBGTTFMA1')
        dfttfma1 = ttfma1.sql_to_df()
        dfttfma1.set_index('date', inplace=True)
        ttfsa1 = DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'BBGTTFSA1')
        dfttfsa1 = ttfsa1.sql_to_df()
        dfttfsa1.set_index('date', inplace=True)
        ttfca1 = DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'BBGTTFCA1')
        dfttfca1 = ttfca1.sql_to_df()
        dfttfca1.set_index('date', inplace=True)
        ttfca2 = DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'BBGTTFCA2')
        dfttfca2 = ttfca2.sql_to_df()
        dfttfca2.set_index('date', inplace=True)
        #print(dfttfma1)
        '''
        sqlConnScrape = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER=PRD-DB-SQL-214;Trusted_Connection=yes')
            #print(yesterday)
        sql = """
                DECLARE @today DATETIME
                SET @today = DATEADD(DD, DATEDIFF(DD, 0, GETDATE()), 0)
                
                DECLARE @monthAheadContract CHAR(6)
                SET @monthAheadContract = FORMAT(DATEADD(MONTH, 1, @today), 'MMM') + '-' + CAST(YEAR(DATEADD(MONTH, 1, @today)) % 2000 AS CHAR)
                DECLARE @monthPlus2Contract CHAR(6)
                SET @monthPlus2Contract = FORMAT(DATEADD(MONTH, 2, @today), 'MMM') + '-' + CAST(YEAR(DATEADD(MONTH, 2, @today)) % 2000 AS CHAR)
                DECLARE @monthPlus3Contract CHAR(6)
                SET @monthPlus3Contract = FORMAT(DATEADD(MONTH, 3, @today), 'MMM') + '-' + CAST(YEAR(DATEADD(MONTH, 3, @today)) % 2000 AS CHAR)
                DECLARE @monthPlus4Contract CHAR(6)
                SET @monthPlus4Contract = FORMAT(DATEADD(MONTH, 4, @today), 'MMM') + '-' + CAST(YEAR(DATEADD(MONTH, 4, @today)) % 2000 AS CHAR)
                DECLARE @monthPlus5Contract CHAR(6)
                SET @monthPlus5Contract = FORMAT(DATEADD(MONTH, 5, @today), 'MMM') + '-' + CAST(YEAR(DATEADD(MONTH, 5, @today)) % 2000 AS CHAR)
                DECLARE @monthPlus6Contract CHAR(6)
                SET @monthPlus6Contract = FORMAT(DATEADD(MONTH, 6, @today), 'MMM') + '-' + CAST(YEAR(DATEADD(MONTH, 6, @today)) % 2000 AS CHAR)
                
                DECLARE @currentMonth INT
                SET @currentMonth = MONTH(@today)
                
                DECLARE @currentYear INT
                SET @currentYear = YEAR(@today)
                
                DECLARE @curentYearForSeason CHAR(2)
                DECLARE @nextYearForSeason CHAR(2)
                DECLARE @nextNextYearForSeason CHAR(2)
                SET @curentYearForSeason = CAST(@currentYear % 2000 AS CHAR(2))
                SET @nextYearForSeason = CAST((@currentYear + 1) % 2000 AS CHAR(2))
                SET @nextNextYearForSeason = CAST((@currentYear + 2) % 2000 AS CHAR(2))
                
                DECLARE @seasonAheadContract CHAR(6)
                DECLARE @seasonPlus2Contract CHAR(6)
                DECLARE @seasonPlus3Contract CHAR(6)
                IF @currentMonth >= 4 AND @currentMonth <= 9
                BEGIN
                    SET @seasonAheadContract = 'Win ' + @curentYearForSeason
                	SET @seasonPlus2Contract = 'Sum ' + @nextYearForSeason
                	SET @seasonPlus3Contract = 'Win ' + @nextYearForSeason
                END
                ELSE
                BEGIN
                    SET @seasonAheadContract = 'Sum ' + @nextYearForSeason
                	SET @seasonPlus2Contract = 'Win ' + @nextYearForSeason
                	SET @seasonPlus3Contract = 'Sum ' + @nextNextYearForSeason
                END
                
                SET @seasonAheadContract =
                    CASE
                        WHEN @currentMonth >= 4 AND @currentMonth <= 9 THEN 'Win ' + @curentYearForSeason
                        WHEN @currentMonth >= 10 AND @currentMonth <= 12 THEN 'Sum ' + @nextYearForSeason
                        ELSE 'Sum '+ @curentYearForSeason
                    END
                
                DECLARE @nextCal CHAR(4)
                DECLARE @nextNextCal CHAR(4)
                SET @nextCal = CAST(@currentYear + 1 AS CHAR(4))
                SET @nextNextCal = CAST(@currentYear + 2 AS CHAR(4))
                
                DECLARE @sql_command NVARCHAR(MAX)
                SET @sql_command = '
                SELECT [Id]
                    ,[DateTime]
                    ,[LastUpdate]
                    ,[TradeID]
                    ,[OrderID]
                    ,[Action]
                    ,[InstrumentID]
                    ,[InstrumentName]
                    ,[FirstSequenceItemName]
                    ,[ExecutionVenueID]
                    ,[Price]
                    ,[Volume]
                    ,[Timestamp]
                FROM [Trayport].[ts].[Trade] WITH (NOLOCK)
                WHERE [DateTime] >= ''' + CONVERT(NVARCHAR, DATEADD(DD, -1, @today), 120) + ''' and [SeqSpan] = ''Single''
                AND ([InstrumentName] = ''TTF Hi Cal 51.6'' or [InstrumentName] = ''TTF Hi Cal 51.6 1MW''
                or [InstrumentName] = ''TTF Hi Cal 51.6 EEX'' or [InstrumentName] = ''TTF Hi Cal 51.6 EEX OTF''
                or [InstrumentName] = ''TTF Hi Cal 51.6 1MW EEX Non-MTF'' or [InstrumentName] = ''TTF Hi Cal 51.6 PEGAS''
                or [InstrumentName] = ''TTF Hi Cal 51.6 PEGAS OTF'' or [InstrumentName] = ''TTF Hi Cal 51.6 1MW PEGAS Non-MTF''
                or [InstrumentName] = ''TTF Hi Cal 51.6 ICE ENDEX'' or [InstrumentName] = ''TTF Hi Cal 51.6 ICE'')
                AND [Volume] >= 5 
                AND FirstSequenceItemName IN ('+
                ''''+@monthAheadContract+''','+
                ''''+@monthPlus2Contract+''','+
                ''''+@monthPlus3Contract+''','+
                ''''+@monthPlus4Contract+''','+
                ''''+@monthPlus5Contract+''','+
                ''''+@monthPlus6Contract+''','+
                ''''+@seasonAheadContract+''','+
                ''''+@seasonPlus2Contract+''','+
                ''''+@seasonPlus3Contract+''','+
                ''''+@nextCal+''','+
                ''''+@nextNextCal+''')'
                
                EXEC sp_executesql @sql_command;

          """
        sqlnew = """
        DECLARE @today DATETIME
        SET @today = DATEADD(DD, DATEDIFF(DD, 0, GETDATE()), 0)
        ​
        DECLARE @monthAheadContract CHAR(6)
        SET @monthAheadContract = FORMAT(DATEADD(MONTH, 1, @today), 'MMM') + '-' + CAST(YEAR(DATEADD(MONTH, 1, @today)) % 2000 AS CHAR)
        ​
        DECLARE @monthPlus2Contract CHAR(6)
        SET @monthPlus2Contract = FORMAT(DATEADD(MONTH, 2, @today), 'MMM') + '-' + CAST(YEAR(DATEADD(MONTH, 2, @today)) % 2000 AS CHAR)
        ​
        DECLARE @monthPlus3Contract CHAR(6)
        SET @monthPlus3Contract = FORMAT(DATEADD(MONTH, 3, @today), 'MMM') + '-' + CAST(YEAR(DATEADD(MONTH, 3, @today)) % 2000 AS CHAR)
        ​
        DECLARE @monthPlus4Contract CHAR(6)
        SET @monthPlus4Contract = FORMAT(DATEADD(MONTH, 4, @today), 'MMM') + '-' + CAST(YEAR(DATEADD(MONTH, 4, @today)) % 2000 AS CHAR)
        ​
        DECLARE @monthPlus5Contract CHAR(6)
        SET @monthPlus5Contract = FORMAT(DATEADD(MONTH, 5, @today), 'MMM') + '-' + CAST(YEAR(DATEADD(MONTH, 5, @today)) % 2000 AS CHAR)
        ​
        DECLARE @monthPlus6Contract CHAR(6)
        SET @monthPlus6Contract = FORMAT(DATEADD(MONTH, 6, @today), 'MMM') + '-' + CAST(YEAR(DATEADD(MONTH, 6, @today)) % 2000 AS CHAR)
        ​
        DECLARE @currentMonth INT
        SET @currentMonth = MONTH(@today)
        ​
        DECLARE @currentYear INT
        SET @currentYear = YEAR(@today)
        ​
        DECLARE @curentYearForSeason CHAR(2)
        DECLARE @nextYearForSeason CHAR(2)
        DECLARE @nextNextYearForSeason CHAR(2)
        SET @curentYearForSeason = CAST(@currentYear % 2000 AS CHAR(2))
        SET @nextYearForSeason = CAST((@currentYear + 1) % 2000 AS CHAR(2))
        SET @nextNextYearForSeason = CAST((@currentYear + 2) % 2000 AS CHAR(2))
        ​
        DECLARE @seasonAheadContract CHAR(6)
        DECLARE @seasonPlus2Contract CHAR(6)
        DECLARE @seasonPlus3Contract CHAR(6)
        IF @currentMonth >= 4 AND @currentMonth <= 9
        BEGIN
            SET @seasonAheadContract = 'Win ' + @curentYearForSeason
            SET @seasonPlus2Contract = 'Sum ' + @nextYearForSeason
            SET @seasonPlus3Contract = 'Win ' + @nextYearForSeason
        END
        ELSE IF @currentMonth >= 10 AND @currentMonth <= 12
        BEGIN
            SET @seasonAheadContract = 'Sum ' + @nextYearForSeason
            SET @seasonPlus2Contract = 'Win ' + @nextYearForSeason
            SET @seasonPlus3Contract = 'Sum ' + @nextNextYearForSeason
        END
        ELSE
        BEGIN
            SET @seasonAheadContract = 'Sum ' + @curentYearForSeason
        	SET @seasonPlus2Contract = 'Win ' + @curentYearForSeason
        	SET @seasonPlus3Contract = 'Sum ' + @nextYearForSeason
        END
        SET @seasonAheadContract =
            CASE
                WHEN @currentMonth >= 4 AND @currentMonth <= 9 THEN 'Win ' + @curentYearForSeason
                WHEN @currentMonth >= 10 AND @currentMonth <= 12 THEN 'Sum ' + @nextYearForSeason
                ELSE 'Sum '+ @curentYearForSeason
            END
        DECLARE @nextCal CHAR(4)
        DECLARE @nextNextCal CHAR(4)
        SET @nextCal = CAST(@currentYear + 1 AS CHAR(4))
        SET @nextNextCal = CAST(@currentYear + 2 AS CHAR(4))
        ​
        DECLARE @sql_command NVARCHAR(MAX)
        SET @sql_command = '
        SELECT [Id]
            ,[DateTime]
            ,[LastUpdate]
            ,[TradeID]
            ,[OrderID]
            ,[Action]
            ,[InstrumentID]
            ,[InstrumentName]
            ,[FirstSequenceItemName]
            ,[ExecutionVenueID]
            ,[Price]
            ,[Volume]
            ,[Timestamp]
        FROM [Trayport].[ts].[Trade] WITH (NOLOCK)
        WHERE [DateTime] >= ''' + CONVERT(NVARCHAR, DATEADD(DD, -1, @today), 120) + ''' and [SeqSpan] = ''Single''
        AND ([InstrumentName] = ''TTF Hi Cal 51.6'' or [InstrumentName] = ''TTF Hi Cal 51.6 1MW''
        or [InstrumentName] = ''TTF Hi Cal 51.6 EEX'' or [InstrumentName] = ''TTF Hi Cal 51.6 EEX OTF''
        or [InstrumentName] = ''TTF Hi Cal 51.6 1MW EEX Non-MTF'' or [InstrumentName] = ''TTF Hi Cal 51.6 PEGAS''
        or [InstrumentName] = ''TTF Hi Cal 51.6 PEGAS OTF'' or [InstrumentName] = ''TTF Hi Cal 51.6 1MW PEGAS Non-MTF''
        or [InstrumentName] = ''TTF Hi Cal 51.6 ICE ENDEX'' or [InstrumentName] = ''TTF Hi Cal 51.6 ICE'')
        AND [Volume] >= 5
        AND FirstSequenceItemName IN ('+
        ''''+@monthAheadContract+''','+
        ''''+@monthPlus2Contract+''','+
        ''''+@monthPlus3Contract+''','+
        ''''+@monthPlus4Contract+''','+
        ''''+@monthPlus5Contract+''','+
        ''''+@monthPlus6Contract+''','+
        ''''+@seasonAheadContract+''','+
        ''''+@seasonPlus2Contract+''','+
        ''''+@seasonPlus3Contract+''','+
        ''''+@nextCal+''','+
        ''''+@nextNextCal+''')'
        EXEC sp_executesql @sql_command;
"""
          
        df = pd.read_sql(sqlnew, sqlConnScrape)
        
        dftrayport = df[['DateTime','FirstSequenceItemName','Price']]
        dftrayport_group = dftrayport.groupby(['DateTime','FirstSequenceItemName'], as_index = False).mean()
        dftrayport_pivot = pd.pivot(dftrayport_group, index='DateTime', columns='FirstSequenceItemName')
        dftrayport_pivot.columns = dftrayport_pivot.columns.droplevel(0)
        dftrayport_pivot.fillna(method = 'ffill', inplace=True)
        #print(dftrayport_hist.index[-1].date())
        #print((today-BDay(1)) == dftrayport_hist.index[-1].date())
        #print(dftrayport_pivot)
        if  (today-BDay(1)) == pd.to_datetime(dftrayport_hist.index[-1]):#.date():
            dftrayport_pivot_all = pd.concat([dftrayport_hist, dftrayport_pivot.loc[today.date().strftime('%Y-%m-%d')].iloc[[-1]]], axis=0)
        else:
            dftrayport_pivot_all = pd.concat([dftrayport_hist, dftrayport_pivot.loc[(today-BDay(1)).date().strftime('%Y-%m-%d')].iloc[[-1]], dftrayport_pivot.loc[today.date().strftime('%Y-%m-%d')].iloc[[-1]]], axis=0)
        #print(dftrayport_pivot_all.round(2))
        
        
        return dftrayport_pivot_all#, dfttfma1, dfttfsa1, dfttfca1, dfttfca2
    
    def regroup_trade(dftrayport_pivot_all):
        
        today = datetime.date.today()
        m1 = pd.to_datetime(str(today.year)+'-'+str(today.month)+'-01') + relativedelta(months=1)
        #print(m1)
        #ma1 = (datetime.date.today()+relativedelta(months=1)).strftime('%b-%y')
    
        if  3<=today.month <=8:
            #print('now')
            sa1 = 'Win '+str(today.year)[2:]
            sa2 = 'Sum '+str(today.year+1)[2:]
            sa3 = 'Win '+str(today.year+1)[2:]
        elif  9<=today.month<=11:
            sa1 = 'Sum '+str(today.year+1)[2:]
            sa2 = 'Win '+str(today.year+1)[2:]
            sa3 = 'Sum '+str(today.year+2)[2:]
        elif today.month==12:
            sa1 = 'Sum '+str(today.year+1)[2:]
            sa2 = 'Win '+str(today.year+1)[2:]
            sa3 = 'Sum '+str(today.year+2)[2:]    
        
        else:
            #print('nnnnnnnnnnnnnnnnn')
            sa1 = 'Sum '+str(today.year)[2:]
            sa2 = 'Win '+str(today.year)[2:]
            sa3 = 'Sum '+str(today.year+1)[2:]
        
        date_dict = {
            'MA1' : m1.strftime('%b')+'-'+str(m1.year)[2:],
            'MA2' : (m1 + relativedelta(months=1)).strftime('%b')+'-'+str((m1 + relativedelta(months=1)).year)[2:],
            'MA3' : (m1 + relativedelta(months=2)).strftime('%b')+'-'+str((m1 + relativedelta(months=2)).year)[2:],
            'MA4' : (m1 + relativedelta(months=3)).strftime('%b')+'-'+str((m1 + relativedelta(months=3)).year)[2:],
            'MA5' : (m1 + relativedelta(months=4)).strftime('%b')+'-'+str((m1 + relativedelta(months=4)).year)[2:],
            'MA6' : (m1 + relativedelta(months=5)).strftime('%b')+'-'+str((m1 + relativedelta(months=5)).year)[2:],
            'SA1' : sa1,
            'SA2' : sa2,
            'SA3' : sa3
            
            }
        
        
        
        
        #print(dftrayport_pivot_all)
        dftrayport_ts = pd.DataFrame(columns = ['Last trade'])
        dftrayport_ts.loc[date_dict['MA1']+'/'+date_dict['SA1'],'Last trade'] = dftrayport_pivot_all[date_dict['MA1']].iloc[-1] - dftrayport_pivot_all[date_dict['SA1']].iloc[-1]
        #dftrayport_ts.loc[date_dict['MA2']+'/'+date_dict['SA1'],'Last trade'] = dftrayport_pivot_all[date_dict['MA1']].iloc[-1] - dftrayport_pivot_all[date_dict['SA1']].iloc[-1]
        #dftrayport_ts.loc[date_dict['MA3']+'/'+date_dict['SA1'],'Last trade'] = dftrayport_pivot_all[date_dict['MA1']].iloc[-1] - dftrayport_pivot_all[date_dict['SA1']].iloc[-1]
        #dftrayport_ts.loc[date_dict['MA4']+'/'+date_dict['SA1'],'Last trade'] = dftrayport_pivot_all[date_dict['MA1']].iloc[-1] - dftrayport_pivot_all[date_dict['SA1']].iloc[-1]
        #dftrayport_ts.loc[date_dict['MA5']+'/'+date_dict['SA1'],'Last trade'] = dftrayport_pivot_all[date_dict['MA1']].iloc[-1] - dftrayport_pivot_all[date_dict['SA1']].iloc[-1]
        #dftrayport_ts.loc[date_dict['MA6']+'/'+date_dict['SA1'],'Last trade'] = dftrayport_pivot_all[date_dict['MA1']].iloc[-1] - dftrayport_pivot_all[date_dict['SA1']].iloc[-1]
        #dftrayport_ts.loc[date_dict['MA1']+'/'+date_dict['SA2'],'Last trade'] = dftrayport_pivot_all[date_dict['MA1']].iloc[-1] - dftrayport_pivot_all[date_dict['SA1']].iloc[-1]
        #dftrayport_ts.loc[date_dict['MA1']+'/'+date_dict['SA1'],'Last trade'] = dftrayport_pivot_all[date_dict['MA1']].iloc[-1] - dftrayport_pivot_all[date_dict['SA1']].iloc[-1]
        #dftrayport_ts.loc[date_dict['MA1']+'/'+date_dict['SA1'],'Last trade'] = dftrayport_pivot_all[date_dict['MA1']].iloc[-1] - dftrayport_pivot_all[date_dict['SA1']].iloc[-1]
        
        #print(dftrayport_ts)
        return dftrayport_ts


    
    def statis(dffcst, dftrayport_pivot):
        print(dffcst)
        today = datetime.datetime.today()
        
        #month
        dffcst_m = dffcst.copy()
        dffcst_m = dffcst_m.resample('M').mean()
        
        dffcst_m['index'] = np.nan
        
        for i in dffcst_m.index:
        
            if  4<=i.month<=9:
                dffcst_m.loc[i,'index'] = i.strftime('%b') + '-' + str(i.year)[2:] + '/' + 'Win '+str(i.year)[2:]
            elif  10<=i.month<=12:
                dffcst_m.loc[i,'index'] = i.strftime('%b') + '-' + str(i.year)[2:] + '/' + 'Sum '+str(i.year+1)[2:]
            else:
                dffcst_m.loc[i,'index'] = i.strftime('%b') + '-' + str(i.year)[2:] + '/' + 'Sum '+str(i.year)[2:]
         
        #print(dffcst_m)
        
        #quarter
        dffcst_q = dffcst.copy()
        dffcst_q = dffcst_q.resample('Q').mean()
        dffcst_q['quarter'] = dffcst_q.index.to_period('Q') 
       
        for i in dffcst_q.index:
            
            if  i.month == 3:
                dffcst_q.loc[i,'index'] = str(dffcst_q.loc[i,'quarter'])[-2:] + str(i.year)[2:] + '/' + 'Sum '+str(i.year)[2:]
                
            if  i.month == 6:
                dffcst_q.loc[i,'index'] = str(dffcst_q.loc[i,'quarter'])[-2:] + str(i.year)[2:] + '/' + 'Win '+str(i.year)[2:]
                
            if  i.month == 9:
                dffcst_q.loc[i,'index'] = str(dffcst_q.loc[i,'quarter'])[-2:] + str(i.year)[2:] + '/' + 'Win '+str(i.year)[2:]
                
            if  i.month == 12:
                dffcst_q.loc[i,'index'] = str(dffcst_q.loc[i,'quarter'])[-2:] + str(i.year)[2:] + '/' + 'Sum '+str(i.year + 1)[2:]
               
        for i in dffcst_q.index:
            dffcst_q.loc[i,'season'] = dffcst_q.loc[i,'index'][5:]
        #print(dffcst_q)
        
        
        #season
        dffcst_s = dffcst_q.groupby(by=['season'], sort=False, as_index = False).mean()
        dffcst_s['index'] = np.nan
        for i in dffcst_s.index:
            #print(dffcst_s.loc[i,'season'][0:3])
            if dffcst_s.loc[i,'season'][0:3] == 'Win':
                dffcst_s.loc[i, 'index'] = 'Sum'+dffcst_s.loc[i,'season'][3:]+'/'+dffcst_s.loc[i,'season']
            if dffcst_s.loc[i,'season'][0:3] == 'Sum':
                dffcst_s.loc[i, 'index'] = 'Win '+str(int(dffcst_s.loc[i,'season'][3:])-1)+'/'+dffcst_s.loc[i,'season']
        #dffcst_s
        
        #print(dffcst_s)
        
        #signal df
        dfstatis1 = pd.DataFrame(index = dffcst_m['index'][0:6], columns = ['Forecast Price'])
        dfstatis1.loc[:,'Forecast Price'] = dffcst_m.loc[dffcst_m.index[0:6],'knn'].values
        
        dfstatis2 = pd.DataFrame(index = dffcst_q['index'], columns = ['Forecast Price'])
        dfstatis2.loc[:,'Forecast Price'] = dffcst_q.loc[:,'knn'].values
        
        dfstatis3 = pd.DataFrame(index = dffcst_s['index'], columns = ['Forecast Price'])
        dfstatis3.loc[:,'Forecast Price'] = dffcst_s.loc[:,'knn'].values
        
        dfstatis = pd.concat([dfstatis1, dfstatis2, dfstatis3])
        dfstatis = dfstatis.round(2)
        #print(dfstatis)
        #last trade price MASA
        dfstatis['Last trade'] = np.nan
        for i in dfstatis.index[1:6]:
            dfstatis.loc[i, 'Last trade'] = dftrayport_pivot[i[0:6]].iloc[-1] - dftrayport_pivot[i[7:13]].iloc[-1]
        #print(dfstatis)
        #last trade price SASA
        for i in dfstatis.index[13:15]:
            dfstatis.loc[i, 'Last trade'] = dftrayport_pivot[i[0:6]].iloc[-1] - dftrayport_pivot[i[7:13]].iloc[-1]
            
        
        #print(dfstatis)
        
        #signal
        dfstatis['signal'] = np.nan
        for i in dfstatis.index:
            if dfstatis.loc[i,'Forecast Price'] - dfstatis.loc[i,'Last trade'] < -1:
                dfstatis.loc[i,'signal'] = 'Sell'
            if dfstatis.loc[i,'Forecast Price'] - dfstatis.loc[i,'Last trade'] > 1:
                dfstatis.loc[i,'signal'] = 'Buy'
        
        dfstatis = dfstatis.round(2)
        dfstatis.fillna(' ', inplace=True)
        #print(dfstatis.round(2))
        dfstatis.reset_index(inplace=True)
        
        return dfstatis
    
    def chart(dfstatis):
        
        #dfstatis.set_index('index', inplace=True)
        #print(dfstatis)
        #full curve signal
        fig = go.Figure() 
                
        
        fig.add_trace(go.Scatter(x=dfstatis.loc[0:6, 'index'], y=dfstatis['Forecast Price'].iloc[0:6],
                    mode='lines+markers',
                    name='Forecast Price',
                    line=dict(color='blue', dash='solid')
                    ))
        fig.add_trace(go.Scatter(x=dfstatis.loc[0:6, 'index'], y=dfstatis['Last trade'].iloc[0:6],
                    mode='lines+markers',
                    name='Last trade',
                    line=dict(color='black', dash='solid')
                    ))
                
        for i in dfstatis.index[0:6]:
            if dfstatis.loc[i,'signal'] == 'Sell':
                fig.add_annotation(x=i, y=dfstatis['Last trade'].loc[i],
                        text = 'Sell',
                        showarrow=True,
                        arrowhead=5)
            if dfstatis.loc[i,'signal'] == 'Buy':
                fig.add_annotation(x=i, y=dfstatis['Last trade'].loc[i],
                        text = 'Buy',
                        showarrow=True,
                        arrowhead=5)
           
            
            
        #print(Basin)    
        fig.update_layout(
            autosize=True,
            showlegend=True,
            legend=dict(x=0, y=-0.2),
            legend_orientation="h",
            title_text='TTF Timespread Fundamental Trading Signal '+ str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')),#+'<br>'+'Live Cumulative PnL '+str(livepnl_ca2),
            yaxis_title="Eur",
            #xaxis = dict(dtick="M1"),
            hovermode='x unified',
            height=666,
            plot_bgcolor = 'white',
            template='ggplot2'  ,
            bargap  = 0
        )
        fig.update_xaxes(
            showline=True, 
            linewidth=1, 
            linecolor='LightGrey', 
            showgrid=False, 
            gridwidth=1, 
            gridcolor='LightGrey'
        )
        fig.update_yaxes(
            showline=True, 
            linewidth=1, 
            linecolor='LightGrey', 
            showgrid=True, 
            gridwidth=1, 
            gridcolor='LightGrey'
        )
        fig.update_layout(xaxis_rangeslider_visible=False)
        
        
        py.plot(fig, filename='H:/Yuefeng/LNG Projects/TTFDASA/DASA.html', auto_open=False) 
        
        return fig
    

    def ma1sa1_data_chart(dftrayport_ts, dffcst, date_dict):
        
        today = datetime.date.today()
        #print(today)
        #print(dffcst.loc[str((today+relativedelta(months=1)).year)+'-'+str((today+relativedelta(months=1)).month)+'-01':])
        
        dffcst = dffcst.loc[str((today+relativedelta(months=1)).year)+'-'+str((today+relativedelta(months=1)).month)+'-01':]
        #print(dffcst)
        #MA1/SA1  mar/sum when in Mar
        df = pd.DataFrame(index=[str(date_dict['today'])], columns = ['contract','fcst','Last trade', 'signal','threshold'])
        df.loc[str(date_dict['today']), 'contract'] = dftrayport_ts.index[0]
        df.loc[str(date_dict['today']), 'Last trade'] = dftrayport_ts.iloc[0,0]
        df.loc[str(date_dict['today']), 'fcst'] = dffcst.resample('MS').mean().iloc[0,0]
        df.loc[str(date_dict['today']), 'threshold'] = 1
        if df.loc[str(date_dict['today']), 'fcst'] - df.loc[str(date_dict['today']), 'Last trade'] > df.loc[str(date_dict['today']), 'threshold']:
            df.loc[str(date_dict['today']), 'signal'] = 'Buy'
        
        elif df.loc[str(date_dict['today']), 'fcst'] - df.loc[str(date_dict['today']), 'Last trade'] < - df.loc[str(date_dict['today']), 'threshold']:
            df.loc[str(date_dict['today']), 'signal'] = 'Sell'
        else:
            df.loc[str(date_dict['today']), 'signal'] = np.nan
        
        df['timestamp'] = datetime.datetime.now()
        
        #MA2/SA1
        df1 = pd.DataFrame(index=[str(date_dict['today'])], columns = ['contract','fcst','Last trade', 'signal','threshold'])
        df1.loc[str(date_dict['today']), 'contract'] = dftrayport_ts.index[0]
        df1.loc[str(date_dict['today']), 'Last trade'] = dftrayport_ts.iloc[0,0]
        df1.loc[str(date_dict['today']), 'fcst'] = dffcst.resample('MS').mean().iloc[1,0]
        df1.loc[str(date_dict['today']), 'threshold'] = 1
        if df1.loc[str(date_dict['today']), 'fcst'] - df1.loc[str(date_dict['today']), 'Last trade'] > df1.loc[str(date_dict['today']), 'threshold']:
            df1.loc[str(date_dict['today']), 'signal'] = 'Buy'
        
        elif df1.loc[str(date_dict['today']), 'fcst'] - df1.loc[str(date_dict['today']), 'Last trade'] < - df1.loc[str(date_dict['today']), 'threshold']:
            df1.loc[str(date_dict['today']), 'signal'] = 'Sell'
        else:
            df.loc[str(date_dict['today']), 'signal'] = np.nan
        
        df1['timestamp'] = datetime.datetime.now()
        #print(df)
        
        
        db_server_lng = "PRD-DB-SQL-211"
        db_name_lng = "LNG"
        sql_engine_lng = sqlalchemy.create_engine("mssql+pyodbc://" + db_server_lng + "/" + db_name_lng + "?trusted_connection=yes&driver=ODBC+Driver+13+for+SQL+Server")
        
        df.to_sql(name='TTFMA1SA1signal', con=sql_engine_lng, index=False, if_exists='append', schema='ana')
        
     
    def utichart(dfinventory_hist, dfinventory_fcst, date_dict):
        
        #get new withdraw
        dfinventory_hist['Europe_Storages_Total_NetWith'] = dfinventory_hist['Europe_Storages_Total_Inventory'] - dfinventory_hist['Europe_Storages_Total_Inventory'].shift(1) 
        #start and end date for train set
        dfinventory_hist = dfinventory_hist.loc['2014-01-01':].copy()
        
        #train can move to db
        dfinventory_hist['year'] = dfinventory_hist.index.year
        dfinventory_hist['month'] = dfinventory_hist.index.month
        dfinventory_hist['day'] = dfinventory_hist.index.day
        
        #create inventory normal
        inventory_normal = pd.DataFrame(index = pd.date_range(start = dfinventory_hist.index[0], end = dfinventory_fcst.index[-1]), columns=['Europe_Storages_Total_Inventory_Normal'])
        for i in inventory_normal.index:
            inventory_normal.loc[i,'Europe_Storages_Total_Inventory_Normal'] =dfinventory_hist[(dfinventory_hist['month'] == i.month) & (dfinventory_hist['day'] == i.day)].Europe_Storages_Total_Inventory.mean()
        
        dfinventory_fcst['Europe_Storages_Total_NetWith'] = dfinventory_fcst['Europe_Storages_Total_Inventory'] - dfinventory_fcst['Europe_Storages_Total_Inventory'].shift(1) 
        dfinventory_fcst = dfinventory_fcst.loc[date_dict['today']:]
        
        
        #calc uti rate
        total_size=dfinventory_hist['Europe_Storages_Total_Inventory'].max()
        injectioncapacity=dfinventory_hist.loc[:,'Europe_Storages_Total_NetWith'].values.min()
        withdrawalcapacity=dfinventory_hist.loc[:,'Europe_Storages_Total_NetWith'].values.max()
        
        #print(dfinventory_hist)
        #print(dfinventory_fcst)
        
        #df=pd.concat([dfinventory_hist,dfinventory_fcst],axis=0)
        df = dfinventory_hist.copy() # no fcst view
        df=df.interpolate(method='linear')
        daysin=total_size/injectioncapacity
        daysout=total_size/withdrawalcapacity
        #print('total_size',total_size)
        #print('injectioncapacity',injectioncapacity)
        #print('withdrawalcapacity',withdrawalcapacity)
        #print('daysin',daysin)
        #print('daysout',daysout)
        
        max_inventory=df['Europe_Storages_Total_Inventory'].max()
        
        df['stock_lvl']=df['Europe_Storages_Total_Inventory']/max_inventory
        df['stock level']=(-1)*df['Europe_Storages_Total_Inventory']/inventory_normal['Europe_Storages_Total_Inventory_Normal']
        
        nominal_injection_capacity = 70000/140#total_size/daysin #70000/140
        nominal_withdraw_capacity= 70000/85#total_size/daysout        #70000/85
        
        real_injection_capacity=np.interp(x=df['stock_lvl'], xp=[0,0.65,1], fp=[1,1,0.55])
        df['real_injection_capacity']=real_injection_capacity
        real_withdraw_capacity=np.interp(x=df['stock_lvl'], xp=[0,0.45,1], fp=[0.3,1,1])
        df['real_withdraw_capacity']=real_withdraw_capacity
        utilization_rate=[]
        injection_capacity=[]
        withdrawal_capacity=[]
        for i in range(0,df.shape[0]): 
            if df['Europe_Storages_Total_NetWith'].iloc[i] <= 0:
                utilization_rate.append(df['Europe_Storages_Total_NetWith'].iloc[i]/(df['real_injection_capacity'].iloc[i]*nominal_injection_capacity))
                injection_capacity.append(df['Europe_Storages_Total_NetWith'].iloc[i]/(df['real_injection_capacity'].iloc[i]*nominal_injection_capacity))
                withdrawal_capacity.append(0)
        
            else:
                utilization_rate.append(df['Europe_Storages_Total_NetWith'].iloc[i]/(df['real_withdraw_capacity'].iloc[i]*nominal_withdraw_capacity))
                withdrawal_capacity.append(df['Europe_Storages_Total_NetWith'].iloc[i]/(df['real_withdraw_capacity'].iloc[i]*nominal_withdraw_capacity))
                injection_capacity.append(0)
        df['utilization_rate']=utilization_rate
        df['injection_capacity']=injection_capacity
        df['withdrawal_capacity']=withdrawal_capacity
        
        df=df.drop(['stock_lvl',
                    'real_injection_capacity','real_withdraw_capacity','injection_capacity','withdrawal_capacity',
                    #'Europe_Storages_Total_Inventory_Normal',
                    'Europe_Storages_Total_NetWith',
                    'Europe_Storages_Total_Inventory',
                    #'year',
                    #'month',
                    #'day'
                    ],axis=1)
        
        dffund = df.copy()
        dffund['stock level'] = dffund['stock level']*-1
        #dffund.dropna(inplace=True)
        #dffund['quarter'] = dffund.index.quarter
        #print(dffund.index)
        today = datetime.date.today()
        
        dfuti = pd.DataFrame(index = pd.date_range(start = str(today.year)+'-01-01', end = str(today.year)+'-12-31'),columns=[str(today.year)])
        dfsl = pd.DataFrame(index = pd.date_range(start = str(today.year)+'-01-01', end = str(today.year)+'-12-31'), columns=[str(today.year)])
        
        for i in dfuti.index:
            dfuti.loc[i, '2014'] = dffund.loc['2014-'+str(i.month)+'-'+str(i.day),'utilization_rate']
            dfuti.loc[i, '2015'] = dffund.loc['2015-'+str(i.month)+'-'+str(i.day),'utilization_rate']
            dfuti.loc[i, '2016'] = dffund.loc['2016-'+str(i.month)+'-'+str(i.day),'utilization_rate']
            dfuti.loc[i, '2017'] = dffund.loc['2017-'+str(i.month)+'-'+str(i.day),'utilization_rate']
            dfuti.loc[i, '2018'] = dffund.loc['2018-'+str(i.month)+'-'+str(i.day),'utilization_rate']
            dfuti.loc[i, '2019'] = dffund.loc['2019-'+str(i.month)+'-'+str(i.day),'utilization_rate']
            dfuti.loc[i, '2020'] = dffund.loc['2020-'+str(i.month)+'-'+str(i.day),'utilization_rate']
            dfuti.loc[i, '2021'] = dffund.loc['2021-'+str(i.month)+'-'+str(i.day),'utilization_rate']
            dfuti.loc[i, '2022'] = dffund.loc['2022-'+str(i.month)+'-'+str(i.day),'utilization_rate']
            try:
                dfuti.loc[i, '2023'] = dffund.loc['2023-'+str(i.month)+'-'+str(i.day),'utilization_rate']
            except KeyError:
                dfuti.loc[i, '2023'] = np.nan
        
            dfsl.loc[i, '2014'] = dffund.loc['2014-'+str(i.month)+'-'+str(i.day),'stock level']
            dfsl.loc[i, '2015'] = dffund.loc['2015-'+str(i.month)+'-'+str(i.day),'stock level']
            dfsl.loc[i, '2016'] = dffund.loc['2016-'+str(i.month)+'-'+str(i.day),'stock level']
            dfsl.loc[i, '2017'] = dffund.loc['2017-'+str(i.month)+'-'+str(i.day),'stock level']
            dfsl.loc[i, '2018'] = dffund.loc['2018-'+str(i.month)+'-'+str(i.day),'stock level']
            dfsl.loc[i, '2019'] = dffund.loc['2019-'+str(i.month)+'-'+str(i.day),'stock level']
            dfsl.loc[i, '2020'] = dffund.loc['2020-'+str(i.month)+'-'+str(i.day),'stock level']
            dfsl.loc[i, '2021'] = dffund.loc['2021-'+str(i.month)+'-'+str(i.day),'stock level']
            dfsl.loc[i, '2022'] = dffund.loc['2022-'+str(i.month)+'-'+str(i.day),'stock level']
            try:
                dfsl.loc[i, '2023'] = dffund.loc['2023-'+str(i.month)+'-'+str(i.day),'stock level']
            except KeyError:
                dfsl.loc[i, '2023'] = np.nan
        
        #dfuti = dfuti.resample('MS').mean()
        #dfsl = dfsl.resample('MS').mean()
        
        #print(dfuti)
        #print(dfsl)
        fig1 = go.Figure() 
        
        for i in dfuti.columns[1:]:
            fig1.add_trace(go.Scatter(x=dfuti.index, y=dfuti[i],
                        #mode='lines',
                        name=i,
                        visible='legendonly',
                        #line=dict(color='blue', dash='solid')
                        ))
        fig1.add_trace(go.Scatter(x=dfuti.index, y=dfuti[str(today.year)],
                        #mode='lines',
                        name=str(today.year),
                        #visible='legendonly',
                        #line=dict(color='blue', dash='solid')
                        ))
        fig1.add_trace(go.Scatter( x=dfuti.index,y=dfuti[['2014','2015','2016','2017','2018','2019','2020','2021']].min(axis=1),
                   fill='tonexty',
                   fillcolor='rgba(65,105,225,0)',
                   line_color='rgba(65,105,225,0)',
                   showlegend=False,
                   name='2014-2021 range Min'
                   ))
           
        fig1.add_trace(go.Scatter(x=dfuti.index,y=dfuti[['2014','2015','2016','2017','2018','2019','2020','2021']].max(axis=1),
                fill='tonexty',
                fillcolor='rgba(191,191,191,0.7)',
                line_color='rgba(65,105,225,0)',
                showlegend=True,
                name='2014-2021 range'
                ))
        fig1.update_layout(
            autosize=True,
            showlegend=True,
            legend=dict(x=0, y=-0.2),
            legend_orientation="h",
            title_text='EU Storage Utilisation Index '+ str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')),#+'<br>'+'Live Cumulative PnL '+str(livepnl_ca2),
            #yaxis_title="Eur",
            xaxis = dict(dtick="M1"),
            hovermode='x unified',
            height=666,
            plot_bgcolor = 'white',
            template='ggplot2'  ,
            bargap  = 0
        )
        fig1.update_xaxes(
            showline=True, 
            linewidth=1, 
            linecolor='LightGrey', 
            showgrid=False, 
            gridwidth=1, 
            gridcolor='LightGrey'
        )
        fig1.update_yaxes(
            showline=True, 
            linewidth=1, 
            linecolor='LightGrey', 
            showgrid=True, 
            gridwidth=1, 
            gridcolor='LightGrey'
        )
        fig1.update_layout(xaxis_rangeslider_visible=False)
        #stock level
        fig2 = go.Figure() 
        for i in dfsl.columns[1:]:
            fig2.add_trace(go.Scatter(x=dfsl.index, y=dfsl[i],
                        #mode='lines',
                        name=i,
                        visible='legendonly',
                        #line=dict(color='blue', dash='solid')
                        ))
        fig2.add_trace(go.Scatter(x=dfsl.index, y=dfsl[str(today.year)],
                        #mode='lines',
                        name=str(today.year),
                        #visible='legendonly',
                        #line=dict(color='blue', dash='solid')
                        ))
        fig2.add_trace(go.Scatter( x=dfsl.index,y=dfsl[['2014','2015','2016','2017','2018','2019','2020','2021']].min(axis=1),
                   fill='tonexty',
                   fillcolor='rgba(65,105,225,0)',
                   line_color='rgba(65,105,225,0)',
                   showlegend=False,
                   name='2014-2021 range Min'
                   ))
           
        fig2.add_trace(go.Scatter(x=dfsl.index,y=dfsl[['2014','2015','2016','2017','2018','2019','2020','2021']].max(axis=1),
                fill='tonexty',
                fillcolor='rgba(191,191,191,0.7)',
                line_color='rgba(65,105,225,0)',
                showlegend=True,
                name='2014-2021 range'
                ))
        
        
        fig2.update_layout(
            autosize=True,
            showlegend=True,
            legend=dict(x=0, y=-0.2),
            legend_orientation="h",
            title_text='EU Storage level '+ str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')),#+'<br>'+'Live Cumulative PnL '+str(livepnl_ca2),
            #yaxis_title="Eur",
            xaxis = dict(dtick="M1"),
            hovermode='x unified',
            height=666,
            plot_bgcolor = 'white',
            template='ggplot2'  ,
            bargap  = 0
        )
        fig2.update_xaxes(
            showline=True, 
            linewidth=1, 
            linecolor='LightGrey', 
            showgrid=False, 
            gridwidth=1, 
            gridcolor='LightGrey'
        )
        fig2.update_yaxes(
            showline=True, 
            linewidth=1, 
            linecolor='LightGrey', 
            showgrid=True, 
            gridwidth=1, 
            gridcolor='LightGrey'
        )
        fig2.update_layout(xaxis_rangeslider_visible=False)
        
        
        py.plot(fig1, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/signal/utilasation.html', auto_open=False) 
        py.plot(fig2, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/signal/stocklevel.html', auto_open=False)
        
    def update():
        a = TTF_DASA
        date_dict = a.date_dict()
        dfinventory_hist = a.inventory_hist()
        dfdasa = a.DASA()
        #dfinventory_fcst = a.inventory_fcst(date_dict)
        #dfinventory_fcst_anna = a.inventory_fcst_anna(date_dict)
        dfinventory_fcst_nick = a.inventory_fcst_nick(date_dict)
        dffull, train_win, train_sum, target_win, target_sum = a.train_target_data(dfinventory_hist, dfinventory_fcst_nick, dfdasa, date_dict)
        dffcst = a.knn(train_win, train_sum, target_win, target_sum, date_dict)
        dftrayport_pivot = a.get_trade_price()
        dftrayport_ts = a.regroup_trade(dftrayport_pivot)
        a.ma1sa1_data_chart(dftrayport_ts, dffcst, date_dict) #save fcst to db
        #dfstatis = a.statis(dffcst, dftrayport_pivot)
        #fig = a.chart(dfstatis)
        a.utichart(dfinventory_hist, dfinventory_fcst_nick, date_dict)

        #return dfstatis, fig
    
#TTF_DASA.update()

scheduler = BackgroundScheduler(job_defaults={'misfire_grace_time': 15*60})
trigger = OrTrigger([CronTrigger(day_of_week='0-4', hour='17', minute='11')])
scheduler.add_job(func=TTF_DASA.update,trigger=trigger,id='platts')
scheduler.start()
runtime = datetime.datetime.now()
print (scheduler.get_jobs(), runtime)
