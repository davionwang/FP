# -*- coding: utf-8 -*-
"""
Created on Tue May 16 10:36:35 2023

@author: SVC-GASQuant2-Prod
"""

# -*- coding: utf-8 -*-
"""
Created on Mon Jul 18 12:45:34 2022

@author: SVC-GASQuant2-Prod
"""


#V1 beta version, flash is correct now
#V2 add position table
#V3 add filter on TTF trade price <0.5, split table to TTF+TFU+JKM+NBP and others
#V4 round int, color green for long, red for short, inplace 0 to empty, add cum column
#V5 add multi page and make live pnl
#V6 split live new deal and live total, add loading states
#V7 use fx curve, format
#V8 add calc details
#v9 go live with editable table for curve
#v10 will's book


import pyodbc
import pandas as pd
import re
import numpy as np
import requests
import json
from urllib.parse import quote
import datetime
from dateutil.relativedelta import relativedelta
from pandas.tseries.offsets import BDay
import dash
import dash_html_components as html
import dash_core_components as dcc
#from dash import dcc

from dash.dependencies import Input, Output
import dash_table
from dash.exceptions import PreventUpdate
from sklearn.linear_model import LinearRegression
from DBtoDF import DBTOPD
import time
import sqlalchemy
import io

pd.set_option('display.max_columns',10)
pd.set_option('display.width', 1000)
pd.reset_option('display.float_format')


def get_position():
    #============================Your Input COB Date=====================================================================
    cob = (datetime.date.today()-BDay(1)).strftime('%Y-%m-%d')
    sql = "SELECT * from fpeod.EOD_POSITION_DETAIL_GTMP_HIST with (nolock) where COB_DATE='{}' and BOOK_CD ='C EUR - BK' ".format(cob)
    #====================================================================================================================
    username = 'readonly'
    password = 'readonly'
    server = 'YKT-VS-SQL-03'
    database = 'fpolapdb_arch'
    conn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    
    dfdata = pd.read_sql_query(sql, conn)
    #print(dfdata.columns.tolist())
    #dfdata.to_csv('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/position.csv')
    
    #print(dfdata)
    pivot = dfdata[(dfdata.COB_DATE == cob) & (dfdata.BOOK_CD == 'C EUR - BK') & (dfdata.POSITION_TYPE_IND == 'Financial')].pivot_table(values= 'RISK_DELTA',index = 'POS_DMO', columns='LOCATION_NUM_POINT', aggfunc='sum')
    
    pivot = pivot.round(2)
    pivot.reset_index(inplace=True)
    
    #pivot.to_csv('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/position.csv')
    #print(pivot)
    return pivot
     
def get_ccy():
    
    
    cob = "'"+(datetime.date.today()-BDay(1)).strftime('%Y-%m-%d')+"'"
    sql = "SELECT COB_DATE,BOOK_CD,SUM(CCY_POSITION) CCY_POSITION FROM fpolapdb.FPEOD.fact_FX_GAP with (nolock) where BOOK_CD='- BK' and COB_DATE={} GROUP BY COB_DATE,BOOK_CD".format(cob)
    #====================================================================================================================
    username = 'readonly'
    password = 'readonly'GEU JPT 
    server = 'YKT-VS-SQL-03'
    database = 'fpolapdb_arch'
    conn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    
    dfdata = pd.read_sql_query(sql, conn)
    
     
    return dfdata


def get_fx():
    
    fx = DBTOPD('sql-01','fpsqlprod', 'REALTIME', 'FX_RATE') 
    dffx = fx.get_live_fx()
    
    #set up datetime index
    today = datetime.date.today()
    start = today + relativedelta(days = 1)
    d1 = str(start + relativedelta(days = 1))
    w1 = str(start + relativedelta(weeks = 1)+ relativedelta(days = 1))
    w2 = str(start + relativedelta(weeks = 2)+ relativedelta(days = 1))
    w3 = str(start + relativedelta(weeks = 3)+ relativedelta(days = 1))
    m1 = str(start + relativedelta(months=1)+ relativedelta(days = 1))
    m2 = str(start + relativedelta(months=2)+ relativedelta(days = 1))
    m3 = str(start + relativedelta(months=3)+ relativedelta(days = 1))
    m4 = str(start + relativedelta(months=4)+ relativedelta(days = 1))
    m5 = str(start + relativedelta(months=5)+ relativedelta(days = 1))
    m6 = str(start + relativedelta(months=6)+ relativedelta(days = 1))
    m9 = str(start + relativedelta(months=9)+ relativedelta(days = 1))
    m12 = str(start + relativedelta(months=12)+ relativedelta(days = 1))
    m15 = str(start + relativedelta(months=15)+ relativedelta(days = 1))
    m18 = str(start + relativedelta(months=18)+ relativedelta(days = 1))
    m24 = str(start + relativedelta(months=24)+ relativedelta(days = 1))
    m36 = str(start + relativedelta(months=36)+ relativedelta(days = 1))
    m48 = str(start + relativedelta(months=48)+ relativedelta(days = 1))
    
    index_dict = {'FX Spot':str(today), 
                  'FX Fwd 1D':d1, 
                  'FX Fwd 1W':w1,
                  'FX Fwd 2W':w2,
                  'FX Fwd 3W':w3,
                  'FX Fwd 1M':m1,
                  'FX Fwd 2M':m2,
                  'FX Fwd 3M':m3,
                  'FX Fwd 4M':m4,
                  'FX Fwd 5M':m5,
                  'FX Fwd 6M':m6,
                  'FX Fwd 9M':m9,
                  'FX Fwd 12M':m12,
                  'FX Fwd 15M':m15,
                  'FX Fwd 18M':m18,
                  'FX Fwd 2Y':m24,
                  'FX Fwd 3Y':m36,
                  'FX Fwd 4Y':m48,
                  }
    
    index_order = ['FX Spot',
                'FX Fwd 1D',
                'FX Fwd 1W',
                'FX Fwd 2W',
                'FX Fwd 3W',
                'FX Fwd 1M',
                'FX Fwd 2M',
                'FX Fwd 3M',
                'FX Fwd 4M',
                'FX Fwd 5M',
                'FX Fwd 6M',
                'FX Fwd 9M',
                'FX Fwd 12M',
                'FX Fwd 15M',
                'FX Fwd 18M',
                'FX Fwd 2Y',
                'FX Fwd 3Y',
                'FX Fwd 4Y',
                ]
    
    
    index_eur = []# [str(today),d1,w1,w2,w3,m1,m2,m3,m4,m5,m6,m9,m12,m15,m18,m24]
    #print(today + relativedelta(weeks = 1)+datetime.timedelta(days=2))
    #get eurusd
    dfusdeur = dffx.loc[dffx[dffx['CURR_CD'] == 'EUR'].index,['FX_PERIOD_CD','FWD_OFFSET','FX_VALUE']]
    
    dfusdeur.set_index('FX_PERIOD_CD', inplace=True)
    dfusdeur = dfusdeur.loc[index_order]
    dfusdeur.reset_index(inplace=True)
    for i in dfusdeur.index:
        dfusdeur.loc[i,'FX_PERIOD_CD'] = index_dict[dfusdeur.loc[i,'FX_PERIOD_CD']]  #index_gbp.append(index_dict[i])
    dfusdeur.sort_values(by='FX_PERIOD_CD',inplace=True)
    dfusdeur.set_index('FX_PERIOD_CD', inplace=True)
    #print(dfusdeur)
    #get GBPUSD    
    dfusdgbp = dffx.loc[dffx[dffx['CURR_CD'] == 'GBP'].index,['FX_PERIOD_CD','FWD_OFFSET','FX_VALUE']]
    
    dfusdgbp.set_index('FX_PERIOD_CD', inplace=True)
    dfusdgbp = dfusdgbp.loc[index_order]
    dfusdgbp.reset_index(inplace=True)
    index_gbp=[]
    for i in dfusdgbp.index:
        dfusdgbp.loc[i,'FX_PERIOD_CD'] = index_dict[dfusdgbp.loc[i,'FX_PERIOD_CD']]  #index_gbp.append(index_dict[i])
    dfusdgbp.sort_values(by='FX_PERIOD_CD',inplace=True)
    dfusdgbp.set_index('FX_PERIOD_CD', inplace=True)

    #calc. curve
    start = str((today+relativedelta(months=1)).year)+'-'+str((today+relativedelta(months=1)).month)+'-01'
    end = str((today+relativedelta(months=48)).year)+'-'+str((today+relativedelta(months=48)).month)+'-01'
    dfcurve = pd.DataFrame(np.nan, index=pd.date_range(start=start, end=end, freq='MS').date, columns=['eurusd','gbpusd'])
    #print(dfcurve)
    for i in dfcurve.index:
        
        newlist = dfusdeur.copy().index.tolist()
        
        x1 = min(newlist, key=lambda x: abs(datetime.datetime.strptime(x, "%Y-%m-%d") - datetime.datetime.strptime(str(i), "%Y-%m-%d")))
        newlist.remove(x1)
        #print(newlist)
        x2 = min(newlist, key=lambda x: abs(datetime.datetime.strptime(x, "%Y-%m-%d") - datetime.datetime.strptime(str(i), "%Y-%m-%d")))
        
        ndays = (datetime.datetime.strptime(x1, "%Y-%m-%d")-datetime.datetime.strptime(str(i), "%Y-%m-%d"))/pd.Timedelta(1,'D')
        totaldays = (datetime.datetime.strptime(x1, "%Y-%m-%d")-datetime.datetime.strptime(x2, "%Y-%m-%d"))/pd.Timedelta(1,'D')
        X1 = ndays/totaldays
        #eurusd
        y1 = dfusdeur.loc[x1,'FX_VALUE']
        y2 = dfusdeur.loc[x2,'FX_VALUE']
        b=y1
        a=y2-y1
        
        dfcurve.loc[i,'eurusd'] =a*X1+b
        
        #gbpusd
        y1 = dfusdgbp.loc[x1,'FX_VALUE']
        y2 = dfusdgbp.loc[x2,'FX_VALUE']
        b=y1
        a=y2-y1
        
        #print(a,b)
        #print(dfcurve.loc[i])
        dfcurve.loc[i,'gbpusd'] =a*X1+b
    
    
    dfcurve.index=pd.to_datetime(dfcurve.index)
    dfeurusdspot = dfusdeur.loc[str(today),'FX_VALUE']
    dfgbpusdspot = dfusdgbp.loc[str(today),'FX_VALUE']
    #print(dfcurve)
    #y_interp = scipy.interpolate.interp1d(x, y)
    return dfcurve,dfeurusdspot,dfgbpusdspot

def get_fx_ytd():

        today = datetime.date.today()
        
        fx = DBTOPD('FAST-DB','Scrape', 'dbo', 'CurveFPExchangeRate')
        dffx = fx.get_ytd_fx()
        dffx = dffx[['CurveDate','FromCurrency','Multiplier']]
        
        dffxeur = dffx.loc[dffx[dffx['FromCurrency']=='EUR'].index]
        dffxeur.set_index('CurveDate', inplace=True)
        #print(dffxeur)
        dffxgbp = dffx.loc[dffx[dffx['FromCurrency']=='GBP'].index]
        dffxgbp.set_index('CurveDate', inplace=True)
        #print(dffxgbp)
        
        start = str((today+relativedelta(months=1)).year)+'-'+str((today+relativedelta(months=1)).month)+'-01'
        end = str((today+relativedelta(months=24)).year)+'-'+str((today+relativedelta(months=24)).month)+'-01'
        dfcurve = pd.DataFrame(np.nan, index=pd.date_range(start=start, end=end, freq='MS').date, columns=['eurusd','gbpusd'])
        dfcurve['eurusd'] = dffxeur.loc[dfcurve.index, 'Multiplier']
        dfcurve['gbpusd'] = dffxgbp.loc[dfcurve.index, 'Multiplier']
        #print(dfcurve)
        return dfcurve

def get_spot_ytd():
    
    from_ccy = 'EUR'
    to_ccy = 'USD'
    start_date = datetime.date.today() -BDay(1) #datetime(2021,8,1)
    end_date = start_date + relativedelta(months=1)
    
    url = 'http://prd-alp-app-13:18080/frmService/rest/getFxRateHistoricStrip'
        
    load = {    'startDate': start_date.strftime('%d-%b-%Y'),
                'endDate': end_date.strftime('%d-%b-%Y'), 
                'fromCcy': from_ccy,
                'toCcy': to_ccy,
                'justSpot':'true'
            }

    
    r = requests.get(url, params=load)
    
    if r.status_code == 200:
        df = pd.read_json(r.text)
    else:
        raise Exception('Error: request failed with status code ' + str(r.status_code) + '. Reason: ' + r.reason)
        
    #print(df)
    eurusdspotytd = df.loc[0,'fxValue']
    return eurusdspotytd
    
def get_curve():
    
    #curve_id = quote('TTF Fwd')
    #TTF FWD Mwh-G EUR, TTF USD MMBtu USD, JKM MMBtu USD, BRENT USD BBL, EUA MT EUR
    curve_id = ['TTF FWD', 'TTF USD FWD', 'JKM FWD','ICE BRENT FWD','EUA FWD','NBP FWD','Henry Hub'] 
    today=datetime.date.today()
    cob = today#'2021-12-15'
    time_period_id = 2
    start = '2022-01-01' #str((today+relativedelta(months=1)).year)+'-'+str((today+relativedelta(months=1)).month)+'-01'# 
    end = '2025-12-31'
    
    dfcurvefull=pd.DataFrame(index=pd.date_range(start=start, end=end, freq='MS'))
    for i in curve_id:
        url =\
            f'http://prd-rsk-app-06:28080/price-manager/service/curveJson?name={i}'\
                + f'&cob={cob}&time_period_id={time_period_id}&start={start}&end={end}'
        
        response = requests.request('GET', url)
        data_json = json.loads(response.text)
        dfcurve=pd.DataFrame.from_dict(data_json)
        dfcurve.set_index('CURVE_START_DT', inplace=True)
        dfcurve.index=pd.to_datetime(dfcurve.index)
        #print(dfcurve)
        dfcurvefull[i] = dfcurve['PRICE'] 
    #dfcurvefull.to_csv('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/curvetd.csv') 
    #print(dfcurvefull)
    return dfcurvefull

def get_curveytd():
    
    curve_id = ['TTF FWD', 'TTF USD FWD', 'JKM FWD','ICE BRENT FWD','EUA FWD','NBP FWD','Henry Hub'] 
    today=datetime.date.today()
    time_period_id = 2
    start = '2022-01-01' #str((today+relativedelta(months=1)).year)+'-'+str((today+relativedelta(months=1)).month)+'-01'# 
    end = '2025-12-31'
    cobytd = today - BDay(1)#relativedelta(days=1)
    dfcurvefullytd=pd.DataFrame(index=pd.date_range(start=start, end=end, freq='MS'))
    for i in curve_id:
        url =\
            f'http://prd-rsk-app-06:28080/price-manager/service/curveJson?name={i}'\
                + f'&cob={cobytd}&time_period_id={time_period_id}&start={start}&end={end}'
        
        response = requests.request('GET', url)
        data_json = json.loads(response.text)
        dfcurve=pd.DataFrame.from_dict(data_json)
        dfcurve.set_index('CURVE_START_DT', inplace=True)
        dfcurve.index=pd.to_datetime(dfcurve.index)
        #print(dfcurve)
        dfcurvefullytd[i] = dfcurve['PRICE'] 
        
    #print(dfcurvefullytd)
    return dfcurvefullytd
    
def get_new_deal(curve, fx,eurusdspot,gbpusdspot):
    
    
    try:
        cob = datetime.date.today().strftime('%m/%d/%Y')
        url =  'http://prd-mbus-app-01:9080/cxlGateway/rest/tradeManagerService/tradeManager?strategyName=GEU%20JPT%20-%20BK&tradeDate={}'.format(cob)
    
        dfposition=pd.read_csv(url)
        #print(dfposition.loc[dfposition.loc[dfposition['TRADE_PRICE']<=0.5].index])
        dfposition.drop(dfposition.loc[dfposition['TRADE_PRICE']<=0.5].index, inplace=True) #drop TAS trade
        #print(dfposition)
        dfposition['END_DT'] = pd.to_datetime(dfposition['END_DT'], format='%Y-%m-%d %H:%M:%S.%f')
        for i in dfposition.index:
            dfposition.loc[i,'END_DT'] = str(dfposition.loc[i,'END_DT'].year)+'-'+str(dfposition.loc[i,'END_DT'].month)+'-01'
            
        dfposition['TRADE_DATE'] = pd.to_datetime(dfposition.loc[:,'TRADE_DATE'], format='%Y-%m-%d %H:%M:%S.%f')
        dfposition.set_index('TRADE_DATE', inplace=True)
        dfposition = dfposition.loc[str(datetime.date.today())]
        #dfposition.to_csv('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/new deal.csv')
        #print(curve.loc[:,'TTF FWD'])
        #print(dfposition)
        dfposition = dfposition[['START_DT','END_DT','LOCATION','BUY_SELL_IND','EXTENDED_QTY','TRADE_PRICE' ]]
        #print(dfposition)
        new_deal_position = dfposition.copy()
        dfposition.set_index('LOCATION', inplace=True)
        
        #print(curve)
        #print(fx)
        
        location_list = dfposition.index
        
        if 'TTF' in location_list:
            dfttf = dfposition.loc[['TTF']]
            dfttf['START_DT'] = pd.to_datetime(dfttf.loc[:,'START_DT'], format='%Y-%m-%d %H:%M:%S.%f')
            dfttf.reset_index(inplace=True)
            for i in dfttf.index:     
                if dfttf.loc[i,'BUY_SELL_IND']=='Sell':
                    dfttf.loc[i,'EXTENDED_QTY'] = -dfttf.loc[i,'EXTENDED_QTY']
            dfttf['curve'] = np.nan#curve.loc[:,'TTF FWD']
            dfttf.reset_index(inplace=True)
            dfttf['pnl'] = np.nan
            
            for j in dfttf.index:
                dfttf.loc[j,'pnl'] = dfttf.loc[j,'EXTENDED_QTY']*(curve.loc[dfttf.loc[j,'START_DT']:dfttf.loc[j,'END_DT'],'TTF FWD'].mean() - dfttf.loc[j,'TRADE_PRICE'])*fx.loc[dfttf.loc[j,'START_DT']:dfttf.loc[j,'END_DT'],'eurusd'].mean()#eurusdspot
                #dfttf.loc[j,'curve'] = curve.loc[dfttf.loc[j,'START_DT']:dfttf.loc[j,'END_DT'],'TTF FWD'].mean()
            #dfttf['pnl'] = dfttf['EXTENDED_QTY']*(dfttf['curve'] - dfttf['TRADE_PRICE'])*eurusdspot
                #print(dfttf)
            #dfttf.to_csv('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/new deal TTF.csv')
            #print(dfttf)
        if 'TTF USD' in location_list:
            dfttfusd = dfposition.loc[['TTF USD']]
            dfttfusd['START_DT'] = pd.to_datetime(dfttfusd.loc[:,'START_DT'], format='%Y-%m-%d %H:%M:%S.%f')
            dfttfusd.reset_index(inplace=True)
            for i in dfttfusd.index:     
                if dfttfusd.loc[i,'BUY_SELL_IND']=='Sell':
                    dfttfusd.loc[i,'EXTENDED_QTY'] = -dfttfusd.loc[i,'EXTENDED_QTY']
            dfttfusd['curve'] = curve.loc[:,'TTF USD FWD']
            dfttfusd.reset_index(inplace=True)
            dfttfusd['pnl'] = np.nan
            for j in dfttfusd.index:
                dfttfusd.loc[j,'pnl'] = dfttfusd.loc[j,'EXTENDED_QTY']*(curve.loc[dfttfusd.loc[j,'START_DT']:dfttfusd.loc[j,'END_DT'],'TTF USD FWD'].mean() - dfttfusd.loc[j,'TRADE_PRICE'])
            #print(dfttfusd)
        if 'European Union Allowance' in location_list:
            dfeua = dfposition.loc[['European Union Allowance']]
            dfeua['START_DT'] = pd.to_datetime(dfeua.loc[:,'START_DT'], format='%Y-%m-%d %H:%M:%S.%f')
            dfeua.reset_index(inplace=True)
            for i in dfeua.index:     
                if dfeua.loc[i,'BUY_SELL_IND']=='Sell':
                    dfeua.loc[i,'EXTENDED_QTY'] = -dfeua.loc[i,'EXTENDED_QTY']
            dfeua['curve'] = curve.loc[:,'EUA FWD']
            dfeua.reset_index(inplace=True)
            dfeua['pnl'] = np.nan
            for j in dfeua.index:
                dfeua.loc[j,'pnl'] = dfeua.loc[j,'EXTENDED_QTY']*(curve.loc[dfeua.loc[j,'START_DT']:dfeua.loc[j,'END_DT'],'EUA FWD'].mean() - dfeua.loc[j,'TRADE_PRICE'])*fx.loc[dfttf.loc[j,'START_DT']:dfttf.loc[j,'END_DT'],'eurusd'].mean()#*eurusdspot
            
        if 'LNG-JKM' in location_list:
            dfjkm = dfposition.loc[['LNG-JKM']]
            dfjkm['START_DT'] = pd.to_datetime(dfjkm.loc[:,'START_DT'], format='%Y-%m-%d %H:%M:%S.%f')
            dfjkm.reset_index(inplace=True)
            for i in dfjkm.index:     
                if dfjkm.loc[i,'BUY_SELL_IND']=='Sell':
                    dfjkm.loc[i,'EXTENDED_QTY'] = -dfjkm.loc[i,'EXTENDED_QTY']
            dfjkm['curve'] = curve.loc[:,'JKM FWD']
            dfjkm.reset_index(inplace=True)
            dfjkm['pnl'] = np.nan
            for j in dfjkm.index:
                dfjkm.loc[j,'pnl'] = dfjkm.loc[j,'EXTENDED_QTY']*(curve.loc[dfjkm.loc[j,'START_DT']:dfjkm.loc[j,'END_DT'],'JKM FWD'].mean() - dfjkm.loc[j,'TRADE_PRICE'])
                    
        if 'North Sea' in location_list:
            dfbrent = dfposition.loc[['North Sea']]
            dfbrent['START_DT'] = pd.to_datetime(dfbrent.loc[:,'START_DT'], format='%Y-%m-%d %H:%M:%S.%f')
            dfbrent.reset_index(inplace=True)
            for i in dfbrent.index:     
                if dfbrent.loc[i,'BUY_SELL_IND']=='Sell':
                    dfbrent.loc[i,'EXTENDED_QTY'] = -dfbrent.loc[i,'EXTENDED_QTY']
            dfbrent['curve'] = curve.loc[:,'ICE BRENT FWD']
            dfbrent.reset_index(inplace=True)
            dfbrent['pnl'] = np.nan
            for j in dfbrent.index:
                dfbrent.loc[j,'pnl'] = dfbrent.loc[j,'EXTENDED_QTY']*(curve.loc[dfbrent.loc[j,'START_DT']:dfbrent.loc[j,'END_DT'],'ICE BRENT FWD'].mean() - dfbrent.loc[j,'TRADE_PRICE'])
            #print(dfbrent)        
        if 'FX' in location_list:
            dffx = dfposition.loc[['FX']]
            dffx['START_DT'] = pd.to_datetime(dffx.loc[:,'START_DT'], format='%Y-%m-%d %H:%M:%S.%f')
            dffx.reset_index(inplace=True)
            for i in dffx.index:     
                if dffx.loc[i,'BUY_SELL_IND']=='Sell':
                    dffx.loc[i,'EXTENDED_QTY'] = -dffx.loc[i,'EXTENDED_QTY']
            dffx['curve'] = fx.loc[:,'eurusd']
            dffx.reset_index(inplace=True)
            dffx['pnl'] = np.nan
            for j in dffx.index:
                dffx.loc[j,'pnl'] = dffx.loc[j,'EXTENDED_QTY']*(fx.loc[dffx.loc[j,'END_DT'],'eurusd'].mean() - dffx.loc[j,'TRADE_PRICE'])
            #print(dffx)        
        if 'NBP' in location_list:
            dfnbp = dfposition.loc[['NBP']]
            dfnbp['START_DT'] = pd.to_datetime(dfnbp.loc[:,'START_DT'], format='%Y-%m-%d %H:%M:%S.%f')
            dfnbp.reset_index(inplace=True)
            for i in dfnbp.index:     
                if dfnbp.loc[i,'BUY_SELL_IND']=='Sell':
                    dfnbp.loc[i,'EXTENDED_QTY'] = -dfnbp.loc[i,'EXTENDED_QTY']
            dfnbp['curve'] = curve.loc[:,'NBP FWD']
            dfnbp.reset_index(inplace=True)
            dfnbp['pnl'] = np.nan
            for j in dfnbp.index:
                dfnbp.loc[j,'pnl'] = dfnbp.loc[j,'EXTENDED_QTY']*(curve.loc[dfnbp.loc[j,'START_DT']:dfnbp.loc[j,'END_DT'],'NBP FWD'].mean() - dfnbp.loc[j,'TRADE_PRICE'])*fx.loc[dfttf.loc[j,'START_DT']:dfttf.loc[j,'END_DT'],'gbpusd'].mean()/10#*gbpusdspot
            
        if 'Henry Hub' in location_list:
            dfhh = dfposition.loc[['Henry Hub']]
            dfhh['START_DT'] = pd.to_datetime(dfhh.loc[:,'START_DT'], format='%Y-%m-%d %H:%M:%S.%f')
            dfhh.reset_index(inplace=True)
            for i in dfhh.index:     
                if dfhh.loc[i,'BUY_SELL_IND']=='Sell':
                    dfhh.loc[i,'EXTENDED_QTY'] = -dfhh.loc[i,'EXTENDED_QTY']
            dfhh['curve'] = curve.loc[:,'Henry Hub']
            dfhh.reset_index(inplace=True)
            dfhh['pnl'] = np.nan
            for j in dfhh.index:
                dfhh.loc[j,'pnl'] = dfhh.loc[j,'EXTENDED_QTY']*(curve.loc[dfhh.loc[j,'START_DT']:dfhh.loc[j,'END_DT'],'Henry Hub'].mean() - dfhh.loc[j,'TRADE_PRICE'])
            
        dfpnl=pd.DataFrame(index=pd.date_range(start=cob,periods=1), columns=['JKM','TTF','TTF USD','EUA','BRENT','NBP','FX','HH'])
        
        if 'LNG-JKM' in location_list:
            dfpnl['JKM'] = dfjkm['pnl'].sum()
        if 'TTF' in location_list:
            dfpnl['TTF'] = dfttf['pnl'].sum()
        if 'TTF USD' in location_list:     
            dfpnl['TTF USD'] = dfttfusd['pnl'].sum()
        if 'European Union Allowance' in location_list:
            dfpnl['EUA'] = dfeua['pnl'].sum()
        if 'North Sea' in location_list:
            dfpnl['BRENT'] = dfbrent['pnl'].sum()
        if 'FX' in location_list:
            dfpnl['FX'] = dffx['pnl'].sum()
        if 'NBP' in location_list:
            dfpnl['NBP'] = dfnbp['pnl'].sum()
        if 'Henry Hub' in location_list:
            dfpnl['HH'] = dfhh['Henry Hub'].sum()
        dfpnl['today'] = dfpnl.sum(axis=1)
        dfpnl=dfpnl.round(2)
        
    except KeyError as e:
        dfpnl = pd.DataFrame()
        new_deal_position = pd.DataFrame()
        print(e)
    
    dfpnl.fillna(0, inplace=True)
    
    print('NEW DEAL PNL: ',dfpnl)
    return dfpnl, new_deal_position

def position_pnl(position, curve, curveytd, fx, fxytd,eurusdspot,gbpusdspot):
    
    current_position = position.copy()
    
    current_position.set_index('POS_DMO',inplace=True)
    current_position.index = pd.to_datetime(current_position.index)
    current_position.fillna(0, inplace=True)
    
    dfcurvedod = curve - curveytd
    #print(dfcurvedod)
    position_pnl = pd.DataFrame(index=current_position.index)
    #print(position_pnl)
    position_pnl['European Union Allowance'] = current_position['EUA']*dfcurvedod['EUA FWD']*fx['eurusd']  #eur to usd eurusdspot#
    position_pnl['LNG-JKM'] = current_position['LNG-JKM']*dfcurvedod['JKM FWD'] #usd
    position_pnl['NBP'] = current_position['NBP']*dfcurvedod['NBP FWD']*fx['gbpusd']/10 #gbp to usd gbpusdspot/10#
    position_pnl['TTF'] = current_position['TTF']*dfcurvedod['TTF FWD']*fx['eurusd']/3.41214 #eur to usd eurusdspot/3.41214#
    position_pnl['TTF USD'] = current_position['TTF USD']*dfcurvedod['TTF USD FWD'] #usd
    position_pnl['North Sea'] = current_position['North Sea']*dfcurvedod['ICE BRENT FWD'] #usd
    position_pnl['HH'] = current_position['Henry Hub']*dfcurvedod['Henry Hub']
    #position_pnl['FX'] = current_position['FX']
    position_pnl.fillna(0, inplace=True)
    position_pnl['grand total'] =  position_pnl.sum(axis=1)
    position_pnl.loc['grand total'] = position_pnl.sum(axis=0)
    position_pnl.to_csv('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/positionpnl.csv')
    #print(position_pnl)
    
    return position_pnl

def flash(dfposition_pnl, ccy, newdeal, fx, fxytd, eurusdspot,eurusdspotytd):
    
    #print(fx)
    #print(eurusdspot)
    #print(newdeal)
    
    today = datetime.date.today()
    
    dfflash = pd.DataFrame(index=[str(today)], columns = ['CCY','Today New Deal'])
    #dfflash['current position DoD PnL'] = dfposition_pnl.loc['grand total','grand total']#.values
    dfflash[dfposition_pnl.columns[:-1]] = dfposition_pnl.loc['grand total',dfposition_pnl.columns[:-1]]
    dfflash['CCY'] = ccy['CCY_POSITION'].values *(eurusdspot - eurusdspotytd)
    try:
        dfflash['Today New Deal'] = newdeal.loc[str(today),'today']
    except KeyError:
        dfflash['Today New Deal'] = 0
    dfflash['Flash'] = dfflash.sum(axis=1) 
    dfflash = dfflash.round(0).astype('int')
    #print(dfflash)
    
    
    
    return dfflash

def position_sum(position):
    
    start = str((datetime.date.today()+relativedelta(months=1)).year)+'-'+str((datetime.date.today()+relativedelta(months=1)).month)+'-01'
    
    position_table = position.copy()
    position_table['POS_DMO'] = pd.to_datetime(position_table['POS_DMO'])#.dt.date
    position_table.set_index('POS_DMO', inplace=True)
    
    #start
    position_table = position_table.loc[pd.to_datetime(start):]
    position_table.fillna(0, inplace=True)
    position_table = position_table.round(0).astype('int')
    #left table
    position_table_left = position_table[['LNG-JKM','TTF','TTF USD']].copy()
    position_table_left['grand total'] =  position_table_left.sum(axis=1).round(1)
    position_table_left.loc['grand total'] = position_table_left.sum(axis=0).round(1)
    position_table_left['Cum. grand total'] = position_table_left['grand total'].cumsum()
    position_table_left.loc['grand total','Cum. grand total'] = position_table_left['Cum. grand total'].loc[position_table_left.index[-2]]
        
    position_table_left.reset_index(inplace=True)
    position_table_left.loc[:position_table_left.index[-2],'POS_DMO'] = pd.to_datetime(position_table_left.loc[:position_table_left.index[-2],'POS_DMO']).dt.date
    #1000 format 
    position_table_left_format = position_table_left.copy()
    for i in position_table_left_format.columns[1:]:
        position_table_left_format[i] = position_table_left_format[i].iloc[0:].apply(lambda x : "{:,}".format(x))
    #right table
    #print(position_table)
    position_table_right = position_table[['ARA','Cash','EUA','FX','Henry Hub','NBP','North Sea']].copy()
    position_table_right['grand total'] =  position_table_right.sum(axis=1).round(1)
    position_table_right.loc['grand total'] = position_table_right.sum(axis=0).round(1)
    
    position_table_right.reset_index(inplace=True)
    position_table_right.loc[:position_table_right.index[-2],'POS_DMO'] = pd.to_datetime(position_table_right.loc[:position_table_right.index[-2],'POS_DMO']).dt.date
    #1000 format 
    position_table_right_format = position_table_right.copy()
    for i in position_table_right_format.columns[1:]:
        position_table_right_format[i] = position_table_right_format[i].iloc[0:].apply(lambda x : "{:,}".format(x))
        
    #print(position_table_right)
    return position_table_left_format, position_table_right_format, position_table_left, position_table_right
    
def live_total_position(position_table_left, position_table_right, new_deal_position):
    
    #print(position_table_left)
    #print(new_deal_position)
    position_table_left['POS_DMO'].iloc[:-1] = pd.to_datetime(position_table_left['POS_DMO'].iloc[:-1]).dt.date
    position_table_left.set_index('POS_DMO', inplace=True)  #care change all left index, may need copy
    
    position_table_right['POS_DMO'].iloc[:-1] = pd.to_datetime(position_table_right['POS_DMO'].iloc[:-1]).dt.date
    position_table_right.set_index('POS_DMO', inplace=True)
    
    new_deal_position.reset_index(inplace=True)
    for i in new_deal_position.index:     
            if new_deal_position.loc[i,'BUY_SELL_IND']=='Sell':
                new_deal_position.loc[i,'EXTENDED_QTY'] = -new_deal_position.loc[i,'EXTENDED_QTY']
    #print(position_table_left)
    
    try:
        live_pivot = new_deal_position.pivot_table(values= 'EXTENDED_QTY',index = 'END_DT', columns='LOCATION', aggfunc='sum')
        live_pivot.index = pd.to_datetime(live_pivot.index).date
        
        live_table = pd.DataFrame(index = position_table_left.index[:-1])
        
        #print(live_table.index, live_pivot.index)
        live_table_total = pd.concat([live_pivot, live_table], axis = 1)
    
    except KeyError:
        live_table_total = pd.DataFrame(index = position_table_left.index[:-1])
    
    
    live_table_total.sort_index(inplace=True)
    live_table_total.fillna(0, inplace=True)
    
    live_position_table_left_total = pd.DataFrame(index = position_table_left.index[:-1], columns=['LNG-JKM','TTF','TTF USD'])
    if 'LNG-JKM' in live_table_total.columns:
        live_position_table_left_total['LNG-JKM'] = position_table_left['LNG-JKM'] + live_table_total['LNG-JKM']
    if 'TTF' in live_table_total.columns:
        live_position_table_left_total['TTF'] = position_table_left['TTF'] + live_table_total['TTF']
    if 'TTF USD' in live_table_total.columns:
        live_position_table_left_total['TTF USD'] = position_table_left['TTF USD'] + live_table_total['TTF USD']
    
    
    live_position_table_right_total = position_table_right.iloc[:-1,:-1].copy()
    #print(live_position_table_right_total)
    for i in live_table_total.columns:
        if i in live_position_table_right_total.columns:
            live_position_table_right_total[i] = live_position_table_right_total[i] + live_table_total[i]
    
    #print(live_table_total)
    #format new deal
    #live_table_total.fillna(0, inplace=True)
    live_table_total = live_table_total.astype('int')
    live_table_total_format = live_table_total.copy()
    for i in live_table_total_format.columns:
        live_table_total_format[i] = live_table_total_format[i].iloc[0:].apply(lambda x : "{:,}".format(x))
    live_table_total_format[live_table_total_format.eq('0')] = ''
    
    #format left 
    live_position_table_left_total.fillna(0, inplace=True)
    live_position_table_left_total['grand total'] =  live_position_table_left_total.sum(axis=1).round(1)
    
    live_position_table_left_total.loc['grand total'] = live_position_table_left_total.sum(axis=0).round(1)
    live_position_table_left_total['Cum. grand total'] = live_position_table_left_total['grand total'].cumsum()
    live_position_table_left_total.loc['grand total','Cum. grand total'] = live_position_table_left_total['Cum. grand total'].loc[live_position_table_left_total.index[-2]]
    
    live_position_table_left_total = live_position_table_left_total.astype('int')
    live_position_table_left_total_format = live_position_table_left_total.copy()
    for i in live_position_table_left_total_format.columns:
        live_position_table_left_total_format[i] = live_position_table_left_total_format[i].iloc[0:].apply(lambda x : "{:,}".format(x))
    live_position_table_left_total_format[live_position_table_left_total_format.eq('0')] = ''
    live_position_table_left_total.to_csv('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/left.csv')
    #format right
    live_position_table_right_total.fillna(0, inplace=True)
    live_position_table_right_total['grand total'] =  live_position_table_right_total.sum(axis=1).round(1)
    live_position_table_right_total.loc['grand total'] = live_position_table_right_total.sum(axis=0).round(1)
    #live_position_table_right_total['Cum. grand total'] = live_position_table_right_total['grand total'].cumsum()
    live_position_table_right_total = live_position_table_right_total.astype('int')
    live_position_table_right_total_format = live_position_table_right_total.copy()
    for i in live_position_table_right_total_format.columns:
        live_position_table_right_total_format[i] = live_position_table_right_total_format[i].iloc[0:].apply(lambda x : "{:,}".format(x))
    live_position_table_right_total_format[live_position_table_right_total_format.eq('0')] = ''

    
    live_table_total.reset_index(inplace=True)
    live_position_table_left_total.reset_index(inplace=True) 
    live_position_table_right_total.reset_index(inplace=True)
    
    live_table_total.rename(columns = {'index':'POS_DMO'}, inplace=True)
    
    live_table_total_format.reset_index(inplace=True)
    live_position_table_left_total_format.reset_index(inplace=True) 
    live_position_table_right_total_format.reset_index(inplace=True)
    live_table_total_format.rename(columns = {'index':'POS_DMO'}, inplace=True)
    #print(live_position_table_right_total)

    return live_table_total, live_position_table_left_total, live_position_table_right_total, live_table_total_format, live_position_table_left_total_format, live_position_table_right_total_format
    
    
def live_new_deal_position(new_deal_position):
    
    today = datetime.date.today()
    start = str((today + relativedelta(month=1)).year) + '-'+str((today + relativedelta(month=1)).month)+'-01'
    end = '2025-12-01'
    
    new_deal_position.reset_index(inplace=True)
    for i in new_deal_position.index:     
            if new_deal_position.loc[i,'BUY_SELL_IND']=='Sell':
                new_deal_position.loc[i,'EXTENDED_QTY'] = -new_deal_position.loc[i,'EXTENDED_QTY']
    #print(position_table_left)
    try:
        live_pivot = new_deal_position.pivot_table(values= 'EXTENDED_QTY',index = 'END_DT', columns='LOCATION', aggfunc='sum')
        live_pivot.index = pd.to_datetime(live_pivot.index)#.dt.date
    
        live_table = pd.DataFrame(index = pd.date_range(start=start, end =end, freq='MS'))
    
    #print(live_table.index, live_pivot.index)
        live_table_total = pd.concat([live_pivot, live_table], axis = 1)
    except KeyError:
        live_table_total = pd.DataFrame(index = pd.date_range(start=start, end =end, freq='MS'))
        
    live_table_total.sort_index(inplace=True)
    
    #format new deal
    live_table_total.fillna(0, inplace=True)
    live_table_total = live_table_total.astype('int')
    live_table_total_format = live_table_total.copy()
    for i in live_table_total_format.columns:
        live_table_total_format[i] = live_table_total_format[i].iloc[0:].apply(lambda x : "{:,}".format(x))
    live_table_total_format[live_table_total_format.eq('0')] = ''
    

    
    live_table_total.reset_index(inplace=True)
    
    live_table_total.rename(columns = {'index':'POS_DMO'}, inplace=True)
    live_table_total['POS_DMO'] = pd.to_datetime(live_table_total['POS_DMO']).dt.date
    
    live_table_total_format.reset_index(inplace=True)
    live_table_total_format.rename(columns = {'index':'POS_DMO'}, inplace=True)
    live_table_total_format['POS_DMO'] = pd.to_datetime(live_table_total_format['POS_DMO']).dt.date
    #print(live_table_total_format)

    return live_table_total, live_table_total_format
        
    

def plot_layout(dfflash):
    
    column = []
    
    for i in dfflash.columns:
        hd={"name":i,"id": i}
        column.append(hd)  
    
    layout = html.Div([
        
        html.Div([html.I("Daily PnL "+str(datetime.date.today()))]), #page header
        html.Hr(),
        #html.A(html.Button('Refresh Data'),href='/'),
        #,title='Today: '+str(datetime.date.today())),
        #html.Div([html.I(str(datetime.date.today()))]),
        html.Button('Download All Position and Curves (need about 30s)', id='refrash'),
        html.Div(id='body-div'),
        
    ])
    

    return layout
    
def update_eod():

    position = get_position() 
    
    ccy = get_ccy()
    fx,eurusdspot,gbpusdspot=get_fx()
    eurusdspotytd = get_spot_ytd()
    fxytd = get_fx_ytd()
    curve = get_curve()
    curveytd = get_curveytd()
    #print(position)
    newdeal, new_deal_position = get_new_deal(curve, fx,eurusdspot,gbpusdspot)
    dfposition_pnl = position_pnl(position, curve, curveytd, fx, fxytd,eurusdspot,gbpusdspot)
    #print(dfposition_pnl)
    dfflash = flash(dfposition_pnl, ccy, newdeal, fx, fxytd,eurusdspot,eurusdspotytd)
    #print(dfflash)
    dfflash_format = dfflash.copy()
    for i in dfflash_format:
        dfflash_format[i] = dfflash_format[i].apply(lambda x : "{:,}".format(x))
        
    dfnewdeal_format = newdeal.copy()
    for i in dfnewdeal_format:
        dfnewdeal_format[i] = dfnewdeal_format[i].apply(lambda x : "{:,}".format(x))
    print(dfnewdeal_format)
    position_table_left_format, position_table_right_format, position_table_left, position_table_right = position_sum(position)
    
    
    return dfflash, dfflash_format, newdeal, dfnewdeal_format, position_table_left_format, position_table_right_format, position_table_left, position_table_right

def update_live_total():
    
    position = get_position()
    position_table_left_format, position_table_right_format, position_table_left, position_table_right = position_sum(position)
    curve = get_curve()
    fx,eurusdspot,gbpusdspot=get_fx()
    newdeal, new_deal_position = get_new_deal(curve, fx,eurusdspot,gbpusdspot)
    live_table_total, live_position_table_left_total, live_position_table_right_total, live_table_total_format, live_position_table_left_total_format, live_position_table_right_total_format = live_total_position(position_table_left, position_table_right, new_deal_position)

    return live_table_total, live_position_table_left_total, live_position_table_right_total, live_table_total_format, live_position_table_left_total_format, live_position_table_right_total_format

def update_live_new_deal():
    
    #position = get_position()
    #position_table_left_format, position_table_right_format, position_table_left, position_table_right = position_sum(position)
    curve = get_curve()
    fx,eurusdspot,gbpusdspot=get_fx()
    newdeal, new_deal_position = get_new_deal(curve, fx,eurusdspot,gbpusdspot)
    #print(new_deal_position)
    live_table_total, live_table_total_format = live_new_deal_position( new_deal_position)
    #print(live_table_total)
    #print(live_table_total_format.head(10))
    return live_table_total, live_table_total_format

def update_calc_detail():
    
    position = get_position() 
    
    ccy = get_ccy()
    fx,eurusdspot,gbpusdspot=get_fx()
    eurusdspotytd = get_spot_ytd()
    fxytd = get_fx_ytd()
    curve = get_curve()
    curveytd = get_curveytd()
    #print(curve)
    newdeal, new_deal_position = get_new_deal(curve, fx,eurusdspot,gbpusdspot)
    #dfposition_pnl = position_pnl(position, curve, curveytd, fx, fxytd,eurusdspot,gbpusdspot)
    
    #dfflash = flash(dfposition_pnl, ccy, newdeal, fx, fxytd,eurusdspot,eurusdspotytd)
    
    #position_table_left_format, position_table_right_format, position_table_left, position_table_right = position_sum(position)
    
    dffx = fx.copy()
    dffx['eurusdspot'] = eurusdspot
    dffx['eurusdspotytd'] = eurusdspotytd
    dffx['gbpusdspot'] = gbpusdspot
    
    dffx.index = pd.to_datetime(dffx.index).date
    curve.index = pd.to_datetime(curve.index).date
    curveytd.index = pd.to_datetime(curveytd.index).date
    new_deal_position.index = pd.to_datetime(new_deal_position.index).date
    position.index = pd.to_datetime(position.index).date
    
    new_deal_position.reset_index(inplace=True)
    for i in new_deal_position.index:     
                if new_deal_position.loc[i,'BUY_SELL_IND']=='Sell':
                    new_deal_position.loc[i,'EXTENDED_QTY'] = -new_deal_position.loc[i,'EXTENDED_QTY']
    #print(new_deal_position)
    #print(position)
    
    #print(dffx)
    return dffx, curve, curveytd, new_deal_position, position

#update_eod()
#update_live_total() 
#update_calc_detail()
#update_live_new_deal()

app = dash.Dash(__name__)

app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-content')
])


index_page = html.Div([
    dcc.Link('EoD PnL', href='/EoD_PnL'),
    html.Br(),
    dcc.Link('Live Position Total', href='/Live_Position_total'),
    html.Br(),
    dcc.Link('Live Position New', href='/Live_Position_New'),
    html.Br(),
    dcc.Link('Calc', href='/Calc'),
    
    
])

# Update the index
@app.callback(Output('page-content', 'children'),
              [Input('url', 'pathname')])
def display_page(pathname):
    if pathname == '/EoD_PnL':
        
        eod_layout = html.Div([
        
        html.Div([html.I("End of Day PnL")]), #page header
        html.Hr(),
        
        html.Button('Download All Position and Curves (need about 30s)', id='refrash'),
        dcc.Loading(id="loading pnl", 
                        children=[
        html.Div(id='body-div'), ])       ])
        
        return eod_layout
    
    if pathname == '/Live_Position_total':
        
        Live_position_total_layout = html.Div([
        
        html.Div([html.I("Live Position Total")]), #page header
        html.Hr(),
        
        html.Button('Download All Position (need about 1 min)', id='refrash_live_position_total'),
        dcc.Loading(id="loading live total position", 
                        children=[
        html.Div(id='body-div-position-total'),])        ])
        
        return Live_position_total_layout
    
    if pathname == '/Live_Position_New':
        
        
        Live_position_new_layout = html.Div([
        
        html.Div([html.I("Live Position New Deals")]), #page header
        html.Hr(),
        
        html.Button('Download New Deals Position (need about 10s)', id='refrash_live_position_new'),
        
        dcc.Loading(id="loading live new deal", 
                        children=[
        html.Div(id='body-div-position-new'),  ])
                            ])
        
        return Live_position_new_layout
    
    if pathname == '/Calc':
        
        
        calc_layout = html.Div([
        
        html.Div([html.I("Calculation Details")]), #page header
        html.Hr(),
        
        html.Button('Download Calculation Details (need about 60s)', id='refrash_calc'),
        html.Button('Download All in Excel', id='download'),
        dcc.Download(id="download-excel"),
        dcc.Loading(id="calc", 
                        children=[
        html.Div(id='body-div-calc'), ]),
        
        
        
                            ])
        
        return calc_layout
    
    else:
        return index_page
    
#EoD PnL
@app.callback(
    Output(component_id='body-div', component_property='children'),
    Input(component_id='refrash', component_property='n_clicks')
)
def update_output(n_clicks):
    if n_clicks is None:
        raise PreventUpdate
    else:
        
        dfflash, dfflash_format, newdeal, dfnewdeal_format, position_table_left_format, position_table_right_format, position_table_left, position_table_right = update_eod()
        
        dfflash_format[dfflash_format.eq('0')] = ''
        dfnewdeal_format[dfnewdeal_format.eq('0')] = ''
        #print(dfflash_format)
        
        position_table_left_format[position_table_left_format.eq('0')] = ''
        position_table_right_format[position_table_right_format.eq('0')] = ''

        #print(position_table_left_format)
        column_new_deal = []
        column_color_new_deal = []
        for i in dfnewdeal_format.columns:
            hd={"name":i,"id": i}
            column_new_deal.append(hd) 
        #color cell
        for i in newdeal.columns:    
            for j in newdeal.index:
                if newdeal.loc[j,i] > 0:
                    cc = {'if': {'column_id': i,'row_index':j},
                              'backgroundColor': 'Green',
                              'color': 'white'}
                    column_color_new_deal.append(cc)
                if newdeal.loc[j,i] < 0:
                    cc = {'if': {'column_id': i,'row_index':j},
                              'backgroundColor': 'Crimson',
                              'color': 'white'}    
                    column_color_new_deal.append(cc)

        column = []
        column_color = []
        for i in dfflash_format.columns:
            hd={"name":i,"id": i}
            column.append(hd) 
        #color cell
        for i in dfflash.columns:    
            for j in dfflash.index:
                if dfflash.loc[j,i] > 0:
                    cc = {'if': {'column_id': i,'row_index':j},
                              'backgroundColor': 'Green',
                              'color': 'white'}
                    column_color.append(cc)
                if dfflash.loc[j,i] < 0:
                    cc = {'if': {'column_id': i,'row_index':j},
                              'backgroundColor': 'Crimson',
                              'color': 'white'}    
                    column_color.append(cc)
        #print(column_color)
        
        column_position_left = []
        column_position_right = []
        color_left = []
        color_right = []
        #set id and columns left
        for i in position_table_left_format.columns:
            hd={"name":i+' ',"id": i+' '}
            column_position_left.append(hd) 
            position_table_left_format.rename(columns = {i: i+' '} , inplace=True)  #position has same name with flash, add a space on to the name
        #color cel
        for i in position_table_left.columns[1:]:    
            for j in position_table_left.index:
                if position_table_left.loc[j,i] > 0:
                    cc = {'if': {'column_id': i+' ','row_index':j},
                              'backgroundColor': 'Green',
                              'color': 'white'}
                    color_left.append(cc)
                if position_table_left.loc[j,i] < 0:
                    cc = {'if': {'column_id': i+' ','row_index':j},
                              'backgroundColor': 'Crimson',
                              'color': 'white'}    
                    color_left.append(cc)
        #print(color_left)  
        #set id and columns left 
        for i in position_table_right_format.columns:
            hd={"name":i+' ',"id": i+' '}
            column_position_right.append(hd) 
            position_table_right_format.rename(columns = {i: i+' '} , inplace=True)  #position has same name with flash, add a space on to the name
        #color cel
        for i in position_table_right.columns[1:]:    
            for j in position_table_right.index:
                if position_table_right.loc[j,i] > 0:
                    cc = {'if': {'column_id': i+' ','row_index':j},
                              'backgroundColor': 'Green',
                              'color': 'white'}
                    color_right.append(cc)
                if position_table_right.loc[j,i] < 0:
                    cc = {'if': {'column_id': i+' ','row_index':j},
                              'backgroundColor': 'Crimson',
                              'color': 'white'}    
                    color_right.append(cc)
        
        #print(column, column_position)  
        layout = html.Div([
            html.H4("PnL Date: "+str(datetime.date.today())), #page header
            html.Hr(),
            html.H5("Today's New Deal"),
            dash_table.DataTable(
            
            id='new deal',
            columns=column_new_deal,
            data=dfnewdeal_format.to_dict('records'),   
    
            style_cell={
                'whiteSpace': 'normal',
                'height': 'auto',
                'textAlign': 'center',
                #'border': '3px solid grey'
                #'backgroundColor': '#D9D9D9',
                'backgroundColor': '#DDEBF7',
                #'minWidth': '200px',
            },
                
            style_header={
                'backgroundColor': 'royalblue',
                'fontWeight': 'bold',
                'textAlign': 'center',
                #'border': '3px solid black'
                'color': 'white'
            },
            fixed_columns = {'headers': True, 'data': 0},
            style_data_conditional = (column_color_new_deal),
            style_as_list_view=False,
            style_table={'minWidth': '100%'},
            
            
            
        ),
            html.Hr(),
            html.H5("EoD PnL"),
            dash_table.DataTable(
            
            id='flash',
            columns=column,
            data=dfflash_format.to_dict('records'),   
    
            style_cell={
                'whiteSpace': 'normal',
                'height': 'auto',
                'textAlign': 'center',
                #'border': '3px solid grey'
                #'backgroundColor': '#D9D9D9',
                'backgroundColor': '#DDEBF7',
                #'minWidth': '200px',
            },
                
            style_header={
                'backgroundColor': 'royalblue',
                'fontWeight': 'bold',
                'textAlign': 'center',
                #'border': '3px solid black'
                'color': 'white'
            },
            fixed_columns = {'headers': True, 'data': 0},
            style_data_conditional = (column_color),
            style_as_list_view=False,
            style_table={'minWidth': '100%'},
            
            
            
        ),
        
        #
        html.H4("Position Date: "+str((datetime.date.today()-BDay(1)).date())),
        html.Hr(),
        html.Div([dash_table.DataTable(
            
                            id='position left',
                            columns=column_position_left,
                            data=position_table_left_format.to_dict('records'),
    
            style_cell={
                'whiteSpace': 'normal',
                'height': 'auto',# no data will be small
                'textAlign': 'center',
                'backgroundColor': '#DDEBF7',
            },
            style_data_conditional= ( 
                                        #color the bottom row
                                         
                                         [
                                        {
                                            'if': {
                                                #'filter_query': '{{{col}}} < {value}'.format(col=i.rstrip('\"'), value=0),
                                                'column_id': i,
                                            },
                                            'backgroundColor': 'paleturquoise',
                                            #'fontWeight': 'bold',
                                        } for i in ['POS_DMO ','grand total ','Cum. grand total ']
                                        ]
                                        +
                                        [
                                       {
                                            'if': {'row_index': position_table_left.shape[0]-1},
                                            'backgroundColor': 'royalblue',
                                            'color' : 'white',
                                            'fontWeight': 'bold',
                                        }
                                    ]
                                       +color_left
                                    ),
            
            style_header={
                'backgroundColor': 'royalblue',
                'fontWeight': 'bold',
                'textAlign': 'center',
                #'border': '3px solid black'
                'color': 'white'
            },
             
            fixed_columns = {'headers': True, 'data': 0},
            #style_data_conditional=stylessupplyup,
            style_as_list_view=False,
            style_table={'minWidth': '100%'},
            
        ),
           
            

        ], style={'display': 'inline-block', 'width': '30%','margin': '10px'}),
        
        #html.Tr(),
        
        html.Div([dash_table.DataTable(
            
                            id='position right',
                            columns=column_position_right,
                            data=position_table_right_format.to_dict('records'),
    
            style_cell={
                'whiteSpace': 'normal',
                'height': 'auto',# no data will be small
                'textAlign': 'center',
                'backgroundColor': '#DDEBF7',
            },
            
            style_data_conditional= ( 
                                        #color the bottom row
                                         [
                                        {
                                            'if': {
                                                #'filter_query': '{{{col}}} < {value}'.format(col=i.rstrip('\"'), value=0),
                                                'column_id': i,
                                            },
                                            'backgroundColor': 'paleturquoise'
                                        } for i in ['POS_DMO ','grand total ']
                                        ]
                                        +
                                         [
                                       {
                                            'if': {'row_index': position_table_left.shape[0]-1},
                                            'backgroundColor': 'royalblue',
                                            'color' : 'white',
                                            'fontWeight': 'bold',
                                        }
                        
                                    ]+color_right
                                        ),
            
            style_header={
                'backgroundColor': 'royalblue',
                'fontWeight': 'bold',
                'textAlign': 'center',
                #'border': '3px solid black'
                'color': 'white'
            },
            fixed_columns = {'headers': True, 'data': 0},
            #style_data_conditional=stylessupplyup,
            style_as_list_view=False,
            style_table={'minWidth': '100%'},
               
           
            
        ),

        ], style={'display': 'inline-block', 'width': '35%'})
        
        ])
        
        return layout


#Live position total
@app.callback(
    Output(component_id='body-div-position-total', component_property='children'),
    Input(component_id='refrash_live_position_total', component_property='n_clicks')
)
def update_output_position(n_clicks):
    if n_clicks is None:
        raise PreventUpdate
    else:
        
        live_table_total, live_position_table_left_total, live_position_table_right_total, live_table_total_format, live_position_table_left_total_format, live_position_table_right_total_format = update_live_total()
        
        column_position_left = []
        column_position_right = []
        color_left = []
        color_right = []
        #set id and columns left
        for i in live_position_table_left_total_format.columns:
            hd={"name":i+' ',"id": i+' '}
            column_position_left.append(hd) 
            live_position_table_left_total_format.rename(columns = {i: i+' '} , inplace=True)  #position has same name with flash, add a space on to the name
        #color cel
        for i in live_position_table_left_total.columns[1:]:    
            for j in live_position_table_left_total.index:
                if live_position_table_left_total.loc[j,i] > 0:
                    cc = {'if': {'column_id': i+' ','row_index':j},
                              'backgroundColor': 'Green',
                              'color': 'white'}
                    color_left.append(cc)
                if live_position_table_left_total.loc[j,i] < 0:
                    cc = {'if': {'column_id': i+' ','row_index':j},
                              'backgroundColor': 'Crimson',
                              'color': 'white'}    
                    color_left.append(cc)
        #print(color_left)  
        #set id and columns left 
        for i in live_position_table_right_total_format.columns:
            hd={"name":i+' ',"id": i+' '}
            column_position_right.append(hd) 
            live_position_table_right_total_format.rename(columns = {i: i+' '} , inplace=True)  #position has same name with flash, add a space on to the name
        #color cel
        for i in live_position_table_right_total.columns[1:]:    
            for j in live_position_table_right_total.index:
                if live_position_table_right_total.loc[j,i] > 0:
                    cc = {'if': {'column_id': i+' ','row_index':j},
                              'backgroundColor': 'Green',
                              'color': 'white'}
                    color_right.append(cc)
                if live_position_table_right_total.loc[j,i] < 0:
                    cc = {'if': {'column_id': i+' ','row_index':j},
                              'backgroundColor': 'Crimson',
                              'color': 'white'}    
                    color_right.append(cc)
        
        #print(column, column_position)  
        layout = html.Div([
            
            
            html.H4("Total Position, Update Time: "+str(datetime.datetime.now().strftime("%Y-%m-%d, %H:%M:%S")), style={'display': 'inline-block'}),
            html.Hr(),
        
        html.Div([dash_table.DataTable(
            
                            id='position left',
                            columns=column_position_left,
                            data=live_position_table_left_total_format.to_dict('records'),
    
            style_cell={
                'whiteSpace': 'normal',
                'height': 'auto',# no data will be small
                'textAlign': 'center',
                'backgroundColor': '#DDEBF7',
            },
            style_data_conditional= ( 
                                        #color the bottom row
                                         
                                         [
                                        {
                                            'if': {
                                                #'filter_query': '{{{col}}} < {value}'.format(col=i.rstrip('\"'), value=0),
                                                'column_id': i,
                                            },
                                            'backgroundColor': 'paleturquoise',
                                            #'fontWeight': 'bold',
                                        } for i in ['POS_DMO ','grand total ','Cum. grand total ']
                                        ]
                                        +
                                        [
                                       {
                                            'if': {'row_index': live_position_table_left_total.shape[0]-1},
                                            'backgroundColor': 'royalblue',
                                            'color' : 'white',
                                            'fontWeight': 'bold',
                                        }
                                    ]
                                       +color_left
                                    ),
            
            style_header={
                'backgroundColor': 'royalblue',
                'fontWeight': 'bold',
                'textAlign': 'center',
                #'border': '3px solid black'
                'color': 'white'
            },
             
            fixed_columns = {'headers': True, 'data': 0},
            #style_data_conditional=stylessupplyup,
            style_as_list_view=False,
            style_table={'minWidth': '100%'},
            
        ),
           
            

        ], style={'display': 'inline-block', 'width': '40%','margin': '10px'}),
        
        #html.Tr(),
        
        html.Div([dash_table.DataTable(
            
                            id='position right',
                            columns=column_position_right,
                            data=live_position_table_right_total_format.to_dict('records'),
    
            style_cell={
                'whiteSpace': 'normal',
                'height': 'auto',# no data will be small
                'textAlign': 'center',
                'backgroundColor': '#DDEBF7',
            },
            
            style_data_conditional= ( 
                                        #color the bottom row
                                         [
                                        {
                                            'if': {
                                                #'filter_query': '{{{col}}} < {value}'.format(col=i.rstrip('\"'), value=0),
                                                'column_id': i,
                                            },
                                            'backgroundColor': 'paleturquoise'
                                        } for i in ['POS_DMO ','grand total ']
                                        ]
                                        +
                                         [
                                       {
                                            'if': {'row_index': live_position_table_right_total.shape[0]-1},
                                            'backgroundColor': 'royalblue',
                                            'color' : 'white',
                                            'fontWeight': 'bold',
                                        }
                        
                                    ]+color_right
                                        ),
            
            style_header={
                'backgroundColor': 'royalblue',
                'fontWeight': 'bold',
                'textAlign': 'center',
                #'border': '3px solid black'
                'color': 'white'
            },
            fixed_columns = {'headers': True, 'data': 0},
            #style_data_conditional=stylessupplyup,
            style_as_list_view=False,
            style_table={'minWidth': '100%'},
               
           
            
        ),

        ], style={'display': 'inline-block', 'width': '55%'})
        
        ])
        
        return layout
    
#Live position new deal
@app.callback(
    Output('body-div-position-new',    'children'),
    Input(component_id='refrash_live_position_new', component_property='n_clicks')
    )
def update_output_position_new(n_clicks):
    if n_clicks is None:
        raise PreventUpdate
    else:
        
        live_table_total, live_table_total_format = update_live_new_deal()
        
        column = []
        column_color = []
        
        for i in live_table_total_format.columns:
            hd={"name":i,"id": i}
            column.append(hd) 
        #color cell
        for i in live_table_total.columns[1:]:    
            for j in live_table_total.index:
                if live_table_total.loc[j,i] > 0:
                    cc = {'if': {'column_id': i,'row_index':j},
                              'backgroundColor': 'Green',
                              'color': 'white'}
                    column_color.append(cc)
                if live_table_total.loc[j,i] < 0:
                    cc = {'if': {'column_id': i,'row_index':j},
                              'backgroundColor': 'Crimson',
                              'color': 'white'}    
                    column_color.append(cc)
        #print(column_color)
        
        #print(column, column_position)  
        layout = html.Div([
            
            html.H4("New Deal Position, Update Time: "+str(datetime.datetime.now().strftime("%Y-%m-%d, %H:%M:%S")), style={'display': 'inline-block','width': '100%','margin': '10px'}), #page header    
            html.Hr(),
            html.Div([
                                dash_table.DataTable(
            
                                    id='flash',
                                    columns=column,
                                    data=live_table_total_format.to_dict('records'),   
                            
                                    style_cell={
                                        'whiteSpace': 'normal',
                                        'height': 'auto',
                                        'textAlign': 'center',
                                        #'border': '3px solid grey'
                                        #'backgroundColor': '#D9D9D9',
                                        'backgroundColor': '#DDEBF7',
                                        #'minWidth': '200px',
                                    },
                                        
                                    style_header={
                                        'backgroundColor': 'royalblue',
                                        'fontWeight': 'bold',
                                        'textAlign': 'center',
                                        #'border': '3px solid black'
                                        'color': 'white'
                                    },
                                    fixed_columns = {'headers': True, 'data': 0},
                                    style_data_conditional = ( #color the bottom row
                                                                 [
                                                                {
                                                                    'if': {
                                                                        #'filter_query': '{{{col}}} < {value}'.format(col=i.rstrip('\"'), value=0),
                                                                        'column_id': i,
                                                                    },
                                                                    'backgroundColor': 'paleturquoise',
                                                                    #'fontWeight': 'bold',
                                                                } for i in ['POS_DMO']
                                                                ]
                                                                
                                                               +column_color),
                                    style_as_list_view=False,
                                    style_table={'minWidth': '100%'},
                            
                                    
                                )], style={'display': 'inline-block', 'width': '100%'}),
        
        
        ])
        
        return layout
    
#Cala
@app.callback(
    Output('body-div-calc',    'children'),
    Input(component_id='refrash_calc', component_property='n_clicks')
    )
def update_calc(n_clicks):
    
    if n_clicks is None:
        raise PreventUpdate
    else:
        
        dffx, curve, curveytd, new_deal_position, position = update_calc_detail()
        dffx = dffx.round(4)
        curve = curve.round(2)
        curveytd = curveytd.round(2)
        dfcurvedod = (curve - curveytd).round(2)
        dffx.reset_index(inplace=True) 
        curve.reset_index(inplace=True) 
        curveytd.reset_index(inplace=True) 
        new_deal_position.reset_index(inplace=True) 
        position.reset_index(inplace=True) 
        
        column_fx = []
        for i in dffx.columns:
            hd={"name":i+' ',"id": i}
            column_fx.append(hd) 
        
        column_curve = []
        for i in curve.columns:
            hd={"name":i+' ',"id": i}
            column_curve.append(hd) 
            #curve.rename(columns = {i: i+' '} , inplace=True)  #position has same name with flash, add a space on to the name
           
        column_curveytd = []
        for i in curveytd.columns:
            hd={"name":i+' ',"id": i+' ytd'}
            column_curveytd.append(hd) 
            curveytd.rename(columns = {i: i+' ytd'} , inplace=True)  #position has same name with flash, add a space on to the name
         
        column_curvedod = []
        for i in dfcurvedod.columns:
            hd={"name":i+' ',"id": i+' dod'}
            column_curvedod.append(hd) 
            dfcurvedod.rename(columns = {i: i+' dod'} , inplace=True)  #position has same name with flash, add a space on to the name
        
        
        
        #print(column, column_position)  
        layout = html.Div([
            
            html.H4("FX Curve and Spot: "+str(datetime.datetime.now().strftime("%Y-%m-%d, %H:%M:%S")), style={'display': 'inline-block','width': '100%','margin': '10px'}), #page header    
            html.Hr(),
            html.Div([
                dash_table.DataTable(
            
                                    id='fx',
                                    columns=column_fx,
                                    data=dffx.to_dict('records'),   
                            
                                    style_cell={
                                        'whiteSpace': 'normal',
                                        'height': 'auto',
                                        'textAlign': 'center',
                                        #'border': '3px solid grey'
                                        #'backgroundColor': '#D9D9D9',
                                        'backgroundColor': '#DDEBF7',
                                        #'minWidth': '200px',
                                    },
                                        
                                    style_header={
                                        'backgroundColor': 'royalblue',
                                        'fontWeight': 'bold',
                                        'textAlign': 'center',
                                        #'border': '3px solid black'
                                        'color': 'white'
                                    },
                                    fixed_columns = {'headers': True, 'data': 0},
                                    
                                    style_as_list_view=False,
                                    style_table={'minWidth': '100%'},
                            
                                    
                                )], style={'display': 'inline-block', 'width': '20%', 'margin': '10px'}),
            
            
            html.Div([dash_table.DataTable(
            
                                id='curve',
                                columns=column_curve,
                                data=curve.to_dict('records'),
        
                                style_cell={
                                    'whiteSpace': 'normal',
                                    'height': 'auto',# no data will be small
                                    'textAlign': 'center',
                                    'backgroundColor': '#DDEBF7',
                                },
                                
                    
                                
                                style_header={
                                    'backgroundColor': 'royalblue',
                                    'fontWeight': 'bold',
                                    'textAlign': 'center',
                                    #'border': '3px solid black'
                                    'color': 'white'
                                },
                                fixed_columns = {'headers': True, 'data': 0},
                                #style_data_conditional=stylessupplyup,
                                style_as_list_view=False,
                                style_table={'minWidth': '100%'},
                   
               
                
            ),
    
            ], style={'display': 'inline-block', 'width': '25%', 'margin': '10px'}),
            
            html.Div([dash_table.DataTable(
            
                                id='curveytd',
                                columns=column_curveytd,
                                data=curveytd.to_dict('records'),
        
                                style_cell={
                                    'whiteSpace': 'normal',
                                    'height': 'auto',# no data will be small
                                    'textAlign': 'center',
                                    'backgroundColor': '#DDEBF7',
                                },
                                
                    
                                
                                style_header={
                                    'backgroundColor': 'royalblue',
                                    'fontWeight': 'bold',
                                    'textAlign': 'center',
                                    #'border': '3px solid black'
                                    'color': 'white'
                                },
                                fixed_columns = {'headers': True, 'data': 0},
                                #style_data_conditional=stylessupplyup,
                                style_as_list_view=False,
                                style_table={'minWidth': '100%'},
                   
               
                
            ),
    
            ], style={'display': 'inline-block', 'width': '25%', 'margin': '10px'}),
            
            html.Div([dash_table.DataTable(
            
                                id='curvedod',
                                columns=column_curvedod,
                                data=dfcurvedod.to_dict('records'),
        
                                style_cell={
                                    'whiteSpace': 'normal',
                                    'height': 'auto',# no data will be small
                                    'textAlign': 'center',
                                    'backgroundColor': '#DDEBF7',
                                },
                                
                    
                                
                                style_header={
                                    'backgroundColor': 'royalblue',
                                    'fontWeight': 'bold',
                                    'textAlign': 'center',
                                    #'border': '3px solid black'
                                    'color': 'white'
                                },
                                fixed_columns = {'headers': True, 'data': 0},
                                #style_data_conditional=stylessupplyup,
                                style_as_list_view=False,
                                style_table={'minWidth': '100%'},
                   
               
                
            ),
    
            ], style={'display': 'inline-block', 'width': '25%'})
        
        
        ])
        
        return layout


@app.callback(
    Output("download-excel", "data"),
    Input("download", "n_clicks"),
    prevent_initial_call=True,
)
def func(n_clicks):
    
    dffx, curve, curveytd, new_deal_position, position = update_calc_detail()
    
    
    #dffx.to_excel('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/pnl detail.xlsx',header=(0),sheet_name='fx')
    #curve.to_excel('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/pnl detail.xlsx',header=(0),sheet_name='curve')
    #curveytd.to_excel('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/pnl detail.xlsx',header=(0),sheet_name='curveytd')
    #position.to_excel('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/pnl detail.xlsx',header=(0),sheet_name='eod position')
    #new_deal_position.to_excel('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/pnl detail.xlsx',header=(0),sheet_name='new position')


    output = io.BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter')
    dffx.to_excel(writer, sheet_name='fx', index=True)  # writes to BytesIO buffer
    curve.to_excel(writer, sheet_name='curve', index=True)
    curveytd.to_excel(writer, sheet_name='curveytd', index=True)
    position.to_excel(writer, sheet_name='eod position', index=True)
    new_deal_position.to_excel(writer, sheet_name='new position', index=True)
    writer.save()
    data = output.getvalue()
    
    return dcc.send_bytes(data, 'pnl detail.xlsx')


if __name__ == '__main__':
    app.run_server(debug=False, host='0.0.0.0', port=8110)


