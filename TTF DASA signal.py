# -*- coding: utf-8 -*-
"""
Created on Thu Feb 17 14:40:03 2022

@author: SVC-GASQuant2-Prod
"""



import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from numpy import concatenate
import datetime
from sklearn.preprocessing import StandardScaler
from sklearn.neighbors import KNeighborsRegressor
from plotly.offline import download_plotlyjs, init_notebook_mode,  plot
from plotly.graph_objs import *
#from plotly.offline import plot
import plotly.graph_objs as go
from dateutil.relativedelta import relativedelta
import sys
sys.path.append('C:\\Users\SVC-GASQuant2-Prod\YWwebsite\Python Scripts\class') 
from DBtoDF import DBTOPD
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
    
    def inventory_fcst():
        
        dffcst = DBTOPD.get_EU_inventory_fcst()
        
        dffcst.set_index('GasDay', inplace=True)
        dffcst.sort_index(inplace=True)
        #print(dffcst.loc[:'2022-03-30'])
        dffcst = dffcst.drop('ForecastDate', 1)
        #print(dffcst)
        
        dffcst.rename(columns = {'Conti_IT_CEE_stock':'Europe_Storages_Total_Inventory'}, inplace=True)
        
        return dffcst
    
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
    
        return train_win, train_sum, target_win, target_sum 
        
     
        
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
           
            
            #compart resut
            
            compare=concatenate((knn_y_predictwin1, knn_y_predictsum, knn_y_predictwin2), axis=0)
        
        #compare=np.transpose(compare)
        n=pd.date_range(date_dict['today'], date_dict['target_end_date'], freq='D').shape[0]
        compare_df=pd.DataFrame(compare,index=pd.date_range(start=date_dict['today'],freq='D',periods=n),
                        columns=['knn'])    
        #print(compare_df)
        #plot
        compare_df['knn'].plot(figsize=(16,6),grid=True,fontsize=15)
        
        plt.title('KNN DASA FCST',fontsize=12)
        plt.ylabel('DASA',fontsize=15)
        plt.legend(loc='best',fontsize=12)
        plt.show()
        
        #compare_df.to_csv('H:/MARKET/MTTF/Gas/Python - General/Yuefeng/data/utilization_knn'+str(today)+'.csv')
        #compare_df.to_csv('H:/MARKET/MTTF/Gas/Python - General/Yuefeng/data/utilization_knn.csv')
        #print(compare_df.loc['2022-10-01':'2022-10-31'].mean())
        #print(compare_df)

         
        return  compare_df
         
              
    def statis(dffcst):
        
        today = datetime.datetime.today()
        
        #month
        dffcst_m = dffcst.copy()
        dffcst_m = dffcst_m.resample('M').mean()
        
        dffcst_m['index'] = np.nan
        
        for i in dffcst_m.index:
        
            if  4<=i.month<=9:
                dffcst_m.loc[i,'index'] = i.strftime('%b') + '/' + 'Win'+str(i.year)[2:]
            elif  10<=i.month<=12:
                dffcst_m.loc[i,'index'] = i.strftime('%b') + '/' + 'Sum'+str(i.year+1)[2:]
            else:
                dffcst_m.loc[i,'index'] = i.strftime('%b') + '/' + 'Sum'+str(i.year)[2:]
         
        #print(dffcst_m)
        
        #quarter
        dffcst_q = dffcst.copy()
        dffcst_q = dffcst_q.resample('Q').mean()
        dffcst_q['quarter'] = dffcst_q.index.to_period('Q') 
       
        for i in dffcst_q.index:
            
            if  i.month == 3:
                dffcst_q.loc[i,'index'] = str(dffcst_q.loc[i,'quarter'])[-2:] + str(i.year)[2:] + '/' + 'Sum'+str(i.year)[2:]
                
            if  i.month == 6:
                dffcst_q.loc[i,'index'] = str(dffcst_q.loc[i,'quarter'])[-2:] + str(i.year)[2:] + '/' + 'Win'+str(i.year)[2:]
                
            if  i.month == 9:
                dffcst_q.loc[i,'index'] = str(dffcst_q.loc[i,'quarter'])[-2:] + str(i.year)[2:] + '/' + 'Win'+str(i.year)[2:]
                
            if  i.month == 12:
                dffcst_q.loc[i,'index'] = str(dffcst_q.loc[i,'quarter'])[-2:] + str(i.year)[2:] + '/' + 'Sum'+str(i.year + 1)[2:]
               
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
                dffcst_s.loc[i, 'index'] = 'Win'+str(int(dffcst_s.loc[i,'season'][3:])-1)+'/'+dffcst_s.loc[i,'season']
        dffcst_s
        
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
        print(dfstatis)
        
        return dfstatis
    
a = TTF_DASA
date_dict = a.date_dict()
dfinventory_hist = a.inventory_hist()
dfdasa = a.DASA()
dfinventory_fcst = a.inventory_fcst()
train_win, train_sum, target_win, target_sum = a.train_target_data(dfinventory_hist, dfinventory_fcst, dfdasa, date_dict)
dffcst = a.knn(train_win, train_sum, target_win, target_sum, date_dict)
a.statis(dffcst)

