# -*- coding: utf-8 -*-
"""
Created on Wed Aug  3 10:30:53 2022

@author: SVC-GASQuant2-Prod
"""

#V1 add regas data

#from CErename import CEid
import CErename
import pandas as pd
from CEtools import CEtools
import sys
sys.path.append('C:\\Users\SVC-GASQuant2-Prod\YWwebsite\Python Scripts\class') 
import numpy as np
import datetime
from apscheduler.schedulers.background import BackgroundScheduler
import sqlalchemy

#import CEtools
from CEtools import *
ce=CEtools.CElink('swasti@freepoint.com','kfGa5uwL1+')
pd.set_option('display.max_columns',20)
from DBtoDF import DBTOPD


class country_Inventory(object):
    
    #country=[] 
    DateFrom=''
    DateTo=''
    country_code={}
    
    def __init__(self,datefrom,dateto):
        #self.country = short_name.split(',')
        self.DateFrom = datefrom
        self.DateTo = dateto
    
    def inventory(self):    
        
        #CE login
        ce=CEtools.CElink('swasti@freepoint.com','kfGa5uwL1+')   
        
        
        seriesid=[]
        #read series id 
        inventory=pd.read_excel('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/EU Inventory.xlsx',
                                     header=(0),sheet_name=0)
        inventory_code= inventory['seriesId'].iloc[:].tolist()
        inventory_code=','.join(str(i) for i in inventory_code)
        seriesid.append(inventory_code)
        #print(seriesid)
        #try to get a new series id and dfcode
        seriesid=','.join(str(i) for i in seriesid)
        #print(seriesid)
        #read CE data
        dataFrame = ce.eugasseries(id=seriesid,dateFrom=self.DateFrom,dateTo=self.DateTo, unit='mcm')
        #dataFrame = ce.eugasseries(id=25491,dateFrom='01_01_2022',dateTo='11_02_2022', unit='mcm')
        
        dataFrame['Date']=pd.to_datetime(dataFrame['Date'])
        dataFrame.set_index('Date', inplace=True)
        dataFrame.drop(['DateExcel'],axis=1, inplace=True)
        
        #rename 
        rename=CErename.CEid(dataFrame)
        df=rename.replace()
        df=df.fillna(method='ffill')

        #add series id on new row
        new_index=np.array(dataFrame.index.strftime('%Y-%m-%d'))
        new_index=np.insert(new_index,0,'SeriesID')
        seriesid=np.array(seriesid.split(',')).reshape(1,-1)
     
        df_concat=np.concatenate((np.array(seriesid).reshape(1,-1),df),axis=0)
        dfnew=pd.DataFrame(df_concat, index=new_index, columns=df.columns)
        #print(dfnew)
        
        
        db_server_lng = "PRD-DB-SQL-211"
        db_name_lng = "LNG"
        sql_engine_lng = sqlalchemy.create_engine("mssql+pyodbc://" + db_server_lng + "/" + db_name_lng + "?trusted_connection=yes&driver=ODBC+Driver+13+for+SQL+Server")
        
        df.to_sql(name='CEinventory', con=sql_engine_lng, index=True, if_exists='replace', schema='ana')
        
        #print(df)
        
        return df
        #print(rename.replace())
    
        
    def inventory_country(self):    
        
        #CE login
        ce=CEtools.CElink('swasti@freepoint.com','kfGa5uwL1+')   
        
        
        seriesid=[]
        #read series id 
        inventory=pd.read_excel('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/EU Inventory.xlsx',
                                     header=(0),sheet_name=1)
        inventory_code= inventory['seriesId'].iloc[:].tolist()
        inventory_code=','.join(str(i) for i in inventory_code)
        seriesid.append(inventory_code)
        #print(seriesid)
        #try to get a new series id and dfcode
        seriesid=','.join(str(i) for i in seriesid)
        #print(seriesid)
        #read CE data
        dataFrame = ce.eugasseries(id=seriesid,dateFrom=self.DateFrom,dateTo=self.DateTo, unit='mcm')
        #dataFrame = ce.eugasseries(id=25491,dateFrom='01_01_2022',dateTo='11_02_2022', unit='mcm')
        
        dataFrame['Date']=pd.to_datetime(dataFrame['Date'])
        dataFrame.set_index('Date', inplace=True)
        dataFrame.drop(['DateExcel'],axis=1, inplace=True)
        
        #rename 
        rename=CErename.CEid(dataFrame)
        df=rename.replace()
        df=df.fillna(method='ffill')

        #add series id on new row
        new_index=np.array(dataFrame.index.strftime('%Y-%m-%d'))
        new_index=np.insert(new_index,0,'SeriesID')
        seriesid=np.array(seriesid.split(',')).reshape(1,-1)
     
        df_concat=np.concatenate((np.array(seriesid).reshape(1,-1),df),axis=0)
        dfnew=pd.DataFrame(df_concat, index=new_index, columns=df.columns)
        #print(dfnew)
        
        
        db_server_lng = "PRD-DB-SQL-211"
        db_name_lng = "LNG"
        sql_engine_lng = sqlalchemy.create_engine("mssql+pyodbc://" + db_server_lng + "/" + db_name_lng + "?trusted_connection=yes&driver=ODBC+Driver+13+for+SQL+Server")
        
        df.to_sql(name='CEinventorybycountry', con=sql_engine_lng, index=True, if_exists='replace', schema='ana')
        
        #print(df)
        
        return df
    
    def inventory_NWEUKIT(self):    
        
        #CE login
        ce=CEtools.CElink('swasti@freepoint.com','kfGa5uwL1+')   
        
        
        seriesid=[]
        #read series id 
        inventory=pd.read_excel('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/NWEITCEE_Inventory_id.xlsx',
                                     header=(0),sheet_name=0)
        inventory_code= inventory['id'].iloc[:].tolist()
        inventory_code=','.join(str(i) for i in inventory_code)
        seriesid.append(inventory_code)
        #print(seriesid)
        #try to get a new series id and dfcode
        seriesid=','.join(str(i) for i in seriesid)
        #print(seriesid)
        #read CE data
        dataFrame = ce.eugasseries(id=seriesid,dateFrom=self.DateFrom,dateTo=self.DateTo, unit='mcm')
        #dataFrame = ce.eugasseries(id=25491,dateFrom='01_01_2022',dateTo='11_02_2022', unit='mcm')
        
        dataFrame['Date']=pd.to_datetime(dataFrame['Date'])
        dataFrame.set_index('Date', inplace=True)
        dataFrame.drop(['DateExcel'],axis=1, inplace=True)
        
        #rename 
        rename=CErename.CEid(dataFrame)
        df=rename.replace()
        df=df.fillna(method='ffill')

        #add series id on new row
        new_index=np.array(dataFrame.index.strftime('%Y-%m-%d'))
        new_index=np.insert(new_index,0,'SeriesID')
        seriesid=np.array(seriesid.split(',')).reshape(1,-1)
     
        df_concat=np.concatenate((np.array(seriesid).reshape(1,-1),df),axis=0)
        dfnew=pd.DataFrame(df_concat, index=new_index, columns=df.columns)
        #print(dfnew)
        
        
        db_server_lng = "PRD-DB-SQL-211"
        db_name_lng = "LNG"
        sql_engine_lng = sqlalchemy.create_engine("mssql+pyodbc://" + db_server_lng + "/" + db_name_lng + "?trusted_connection=yes&driver=ODBC+Driver+13+for+SQL+Server")
        
        df.to_sql(name='CEinventoryNWEITCEE', con=sql_engine_lng, index=True, if_exists='replace', schema='ana')
        
        #print(df)
        
        return df
    
    

class EU_regas():
    
    def get_regas_stock():
        
        
        dfce = DBTOPD.get_ce_regas_stock()
        
        dfce_povit = pd.pivot_table(dfce, index='Date',columns='SeriesId')
        dfce_povit.columns = dfce_povit.columns.droplevel(0)
        dfce_povit.rename_axis(None, axis=1, inplace=True)
        #print(dfce_povit)
        
        dfcename = DBTOPD('PRD-DB-SQL-211','LNG', 'ana', '[CERegasMap]').sql_to_df()
        #ce_dict = dfcename[['seriesId','Name']].to_dict('index')#‘dict’, ‘list’, ‘series’, ‘split’, ‘records’, ‘index’
        ce_dict={}
        for i in dfcename.index:
            ce_dict[dfcename.loc[i,'seriesId']] = dfcename.loc[i,'Name']
            
        dfce_povit.rename(columns=ce_dict, inplace=True)   
        #print(dfce_povit)
        
        return dfce_povit
    
#imput      country short name , date from or NA  , date to or NA
#         (         'GB'       , 'DD_MMM_YYYY'    ,'DD_MMM_YYYY’ )  ('GB','01_Jan_2020','NA')
def inventory_update():
    
    '''
    delta = datetime.timedelta(days=1)
    #dateFrom=datetime.date.today()-29*delta
    dateFrom = pd.to_datetime( '2012-01-01')
    dateFrom=dateFrom.strftime('%d_%b_%Y')
    dateTo=datetime.date.today()-0*delta
    dateTo=dateTo.strftime('%d_%b_%Y')
    
    
    t=country_Inventory(dateFrom,dateTo)
    t.inventory()
    t.inventory_country()
    t.inventory_NWEUKIT()
    '''
    
    EU_regas.get_regas_stock()

#inventory_update()
'''
scheduler = BackgroundScheduler(job_defaults={'misfire_grace_time': 15*60})

#scheduler.add_job(func=domination_update,trigger='cron',day_of_week='0-6', hour='13',minute='48',id='domination')
scheduler.add_job(func=inventory_update,trigger='cron',day_of_week='0-4', hour='07',minute='03',id='inventory')

scheduler.start()
print (scheduler.get_jobs(),datetime.datetime.now())
#scheduler.remove_job('inventory')
'''