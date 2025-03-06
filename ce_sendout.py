# -*- coding: utf-8 -*-
"""
Created on Wed Sep  7 16:53:29 2022

@author: SVC-GASQuant2-Prod
"""

import pandas as pd
from CEtools import CEtools
import sys
sys.path.append('C:\\Users\SVC-GASQuant2-Prod\YWwebsite\Python Scripts\class') 
import numpy as np
import datetime
from apscheduler.schedulers.background import BackgroundScheduler
import sqlalchemy
from dateutil.relativedelta import relativedelta
import plotly.offline as py
import plotly.graph_objs as go
from plotly.subplots import make_subplots

#import CEtools

pd.set_option('display.max_columns',20)
from DBtoDF import DBTOPD

class EU_CE_regas():
    
    def get_regas_stock():
        
        
        dfce = DBTOPD.get_ce_regas_stock()
        
        dfce_povit = pd.pivot_table(dfce,values='Value', index='Date',columns='SeriesId')
        #dfce_povit.columns = dfce_povit.columns.droplevel(0)
        dfce_povit.rename_axis(None, axis=1, inplace=True)
        #print(dfce_povit)
        
        dfcename = DBTOPD('PRD-DB-SQL-211','LNG', 'ana', '[CERegasMap]').sql_to_df()
        #ce_dict = dfcename[['seriesId','Name']].to_dict('index')#‘dict’, ‘list’, ‘series’, ‘split’, ‘records’, ‘index’
        ce_dict={}
        for i in dfcename.index:
            ce_dict[dfcename.loc[i,'seriesId']] = dfcename.loc[i,'Name']
            
        dfce_povit.rename(columns=ce_dict, inplace=True)   
        
        #dfce_povit = dfce_povit.loc[(datetime.date.today()-relativedelta(days=15)):]
        
        #print(dfce_povit)
        
        return dfce_povit
    
    def get_regas_sendout():
        
        dfce = DBTOPD.get_ce_regas_sendout()
        #print(dfce)
        dfce_povit = pd.pivot_table(dfce,values='Value', index='Date',columns='SeriesId')
        #dfce_povit.columns = dfce_povit.columns.droplevel(0)
        dfce_povit.rename_axis(None, axis=1, inplace=True)
        #print(dfce_povit)
        
        dfcename = DBTOPD('PRD-DB-SQL-211','LNG', 'ana', '[CERegasMap]').sql_to_df()
        #ce_dict = dfcename[['seriesId','Name']].to_dict('index')#‘dict’, ‘list’, ‘series’, ‘split’, ‘records’, ‘index’
        ce_dict={}
        for i in dfcename.index:
            ce_dict[dfcename.loc[i,'seriesId']] = dfcename.loc[i,'Name']
            
        dfce_povit.rename(columns=ce_dict, inplace=True)   
        
        #dfce_povit = dfce_povit.loc[(datetime.date.today()-relativedelta(days=15)):]
        #print(dfce_povit.columns)
        
        return dfce_povit
    
    
    def get_regas_capa():
        
        dfce = DBTOPD.get_ce_regas_capa()
        dfce_povit = pd.pivot_table(dfce,values='Value', index='Date',columns='SeriesId')
        #dfce_povit.columns = dfce_povit.columns.droplevel(0)
        dfce_povit.rename_axis(None, axis=1, inplace=True)
        #print(dfce_povit)
        
        dfcename = DBTOPD('PRD-DB-SQL-211','LNG', 'ana', '[CERegasMap]').sql_to_df()
        #ce_dict = dfcename[['seriesId','Name']].to_dict('index')#‘dict’, ‘list’, ‘series’, ‘split’, ‘records’, ‘index’
        ce_dict={}
        for i in dfcename.index:
            ce_dict[dfcename.loc[i,'seriesId']] = dfcename.loc[i,'Name']
            
        dfce_povit.rename(columns=ce_dict, inplace=True)   
        
        dfce_povit = dfce_povit.loc[(datetime.date.today()-relativedelta(days=15)):]
        #print(dfce_povit)
        
        return dfce_povit
    
    def update():
        
        
        dfstorage = EU_CE_regas.get_regas_stock()
        #print(dfstorage)
        dfsendout = EU_CE_regas.get_regas_sendout()
        #print(dfsendout)
        dfcapa = EU_CE_regas.get_regas_capa()
        #print(dfcapa)
        
        
        db_server_lng = "PRD-DB-SQL-211"
        db_name_lng = "LNG"
        sql_engine_lng = sqlalchemy.create_engine("mssql+pyodbc://" + db_server_lng + "/" + db_name_lng + "?trusted_connection=yes&driver=ODBC+Driver+13+for+SQL+Server")
        
        dfstorage.to_sql(name='CEstorage', con=sql_engine_lng, index=True, if_exists='replace', schema='ana')
        dfsendout.to_sql(name='CEsendout', con=sql_engine_lng, index=True, if_exists='replace', schema='ana')
        dfcapa.to_sql(name='CEcapa', con=sql_engine_lng, index=True, if_exists='replace', schema='ana')
        
        