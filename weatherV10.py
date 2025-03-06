# -*- coding: utf-8 -*-
"""
Created on Mon Aug  7 08:44:37 2023

@author: SVC-GASQuant2-Prod
"""

# -*- coding: utf-8 -*-
"""
Created on Thu Aug  3 10:08:35 2023

@author: SVC-GASQuant2-Prod
"""

 # -*- coding: utf-8 -*-
"""
Created on Fri Jun  2 14:25:23 2023

@author: SVC-GASQuant2-Prod
"""




#weather
#weather v1, wind wave table no green, header before&after color, + 1 row of num of cargos, + 1 row demand send out from CE, change summary table null to N/A, better show summary?
#V2 add delta bar for C1, end by EC46; and colour code table for M and M+1, 
#V3 add DoD table, 10days Ens, + weekly EC46, group by region and order by abs ens change
#V4 add normal temp on the top sum table 1st column
#V5 change db and add Turkmenistan','Uzbekistan','Kazakhstan','Myanmar','Russia''Iran'
#v6 weight 65%JWA and 35%mete
#v7 send email for south hook dragon wind wave
#v8 temp minmax change to mix-obs 
#v9 minmax mean adjust with narmal
#V10 add dod ens and ec on temp top table

import time
import sys
import numpy as np
import pandas as pd
import plotly.offline as py
import plotly.graph_objs as go
from plotly.subplots import make_subplots
import datetime
import calendar
import plotly.express as px
#from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import requests
sys.path.append('C:\\Users\SVC-GASQuant2-Prod\YWwebsite\Python Scripts\class') 
pd.set_option('display.max_columns',10)
from DBtoDF import DBTOPD
from temp3 import temp_hist
import sqlalchemy
import pyodbc



class Weather():
    
    def get_curve_id():
        curve_id=DBTOPD('PRD-DB-SQL-211','Meteorology', 'dbo', 'RefInternalMeteomaticCoordinatesLNGTerminalsCurves')
        dfcurve_id=curve_id.sql_to_df()
        dfcurve_id=dfcurve_id[[ 'synthetic_curve_id' ,       'country' ,  'installation']]
        dfcurve_id.set_index('synthetic_curve_id', inplace=True)
        #print(dfcurve_id)
 
        curve_id_market=DBTOPD('PRD-DB-SQL-211','Meteorology', 'dbo', 'RefInternalMeteomaticCoordinatesLNGMarketsCurves')
        dfcurve_id_market=curve_id_market.sql_to_df()
        dfcurve_id_market=dfcurve_id_market[[ 'synthetic_curve_id' , 'country','wmo_weighting']]
        dfcurve_id_market.set_index('synthetic_curve_id', inplace=True)
        #print(dfcurve_id_market)
        
        return dfcurve_id,dfcurve_id_market
    
    def get_country():
    
        sqlConnScrape = pyodbc.connect('DRIVER={SQL SERVER};SERVER=PRD-DB-SQL-211;Trusted_Connection=yes')
    
        sql = '''
              select distinct CountryName from [Meteorology].[dbo].[WeatherStationsCurveDefinition] where ParameterName = 't_2m:C'
    
                
                  '''
    
        country = pd.read_sql(sql, sqlConnScrape)
        country.dropna(axis=0, inplace=True)
        country_list = country['CountryName'].values
        #print(country)

        return country_list.tolist()
    
    def get_temp_latest(country_list):
        
        #country='Japan'
        today = datetime.date.today()
        sqlConnScrape = pyodbc.connect('DRIVER={SQL SERVER};SERVER=PRD-DB-SQL-211;Trusted_Connection=yes')
        
        #dfens = pd.DataFrame(columns=country_list)
        dfec46 = pd.DataFrame(columns=country_list)
        dfsff = pd.DataFrame(columns=country_list)
        
        if datetime.datetime.now().hour >9:
    
            sqlens = '''
                        SELECT * FROM Meteorology.dbo.WeatherStationTimeSeriesHistory
                        WHERE ParameterName = 't_2m:C' AND ModelSourceName = 'ecmwf-ens' and WeightingName = 'temperature'
                        AND ForecastDate = {time}
            
                          '''.format(time='\''+str(datetime.date.today())+' 00:00:00'+'\'')
            
            dfens = pd.read_sql(sqlens, sqlConnScrape)
            #print(df)
            dfens=dfens[['ValueDate','CountryName','Weighting','Value']]
            dfens['weighted'] = dfens['Weighting']*dfens['Value']
            dfens['ValueDate'] = pd.to_datetime(dfens['ValueDate'])
            
            dfpivot = dfens.pivot_table(values='weighted',index='ValueDate',columns='CountryName',aggfunc=np.sum)
            #print(dfpivot)
            dfpivot = dfpivot.resample('D').mean()
            dfpivot = dfpivot.loc[:today+relativedelta(days=14)]
            #dfpivot['sum'] = dfpivot.sum(axis=1)
            #print(dfpivot)
        #if 8>datetime.datetime.now().hour > 20:
        else:
            sqlens = '''
                        SELECT * FROM Meteorology.dbo.WeatherStationTimeSeriesHistory
                        WHERE ParameterName = 't_2m:C' AND ModelSourceName = 'ecmwf-ens' and WeightingName = 'temperature'
                        AND ForecastDate = {time}
            
                          '''.format(time='\''+str(datetime.date.today()-relativedelta(days=1))+' 12:00:00'+'\'')
            
            dfens = pd.read_sql(sqlens, sqlConnScrape)
            #print(df)
            dfens=dfens[['ValueDate','CountryName','Weighting','Value']]
            dfens['weighted'] = dfens['Weighting']*dfens['Value']
            dfens['ValueDate'] = pd.to_datetime(dfens['ValueDate'])
            #print(i, df)
            dfpivot = dfens.pivot_table(values='weighted',index='ValueDate',columns='CountryName',aggfunc=np.sum)
            #print(dfpivot)
            dfpivot = dfpivot.resample('D').mean()
            dfpivot = dfpivot.loc[:today+relativedelta(days=14)]
            
        for i in country_list:
            try:
            #print(i)
            
            #ec
                sqlec = '''
                        SET NOCOUNT ON; 
                          EXEC Meteorology.dbo.GetModelRunWeatherStations 1, 'ecmwf-vareps', 't_2m:C', NULL, {country}, 'temperature'
            
                          '''.format(country='\''+str(i)+'\'')
                          
                dfec = pd.read_sql(sqlec, sqlConnScrape)
                #print(df)
                dfec=dfec[['ValueDate','WMOId','Weighting','Value']]
                dfec['weighted'] = dfec['Weighting']*dfec['Value']
                dfec['ValueDate'] = pd.to_datetime(dfec['ValueDate'])
                #print(i, df)
                dfpivotec = dfec.pivot_table(values='weighted',index='ValueDate',columns='WMOId')
                dfpivotec['sum'] = dfpivotec.sum(axis=1)
                #print(dfpivot)
                #df.set_index('ValueDate', inplace=True)
                dfpivotec.sort_index(inplace=True)
                #print(df)
                dfpivotec = dfpivotec.resample('D').mean()
                #print(dfpivot)
                dfec46[i] = dfpivotec['sum']
            
            #sf    
                sqlsf = '''
                        SET NOCOUNT ON; 
                          EXEC Meteorology.dbo.GetModelRunWeatherStations 1, 'ecmwf-mmsf', 't_2m:C', NULL, {country}, 'temperature'
            
                          '''.format(country='\''+str(i)+'\'')
                          
                dfsf = pd.read_sql(sqlsf, sqlConnScrape)
                #print(df)
                dfsf=dfsf[['ValueDate','WMOId','Weighting','Value']]
                dfsf['weighted'] = dfsf['Weighting']*dfsf['Value']
                dfsf['ValueDate'] = pd.to_datetime(dfsf['ValueDate'])
                #print(i, df)
                dfpivotsf = dfsf.pivot_table(values='weighted',index='ValueDate',columns='WMOId')
                dfpivotsf['sum'] = dfpivotsf.sum(axis=1)
                #print(dfpivot)
                #df.set_index('ValueDate', inplace=True)
                #df.sort_index(inplace=True)
                #print(df)
                dfpivotsf = dfpivotsf.resample('D').mean()
                #print(dfpivot)
                dfsff[i] = dfpivotsf['sum']
            except Exception:
                print(i, 'not in temp latest 186')
                pass
        
       
        #print(dfpivot)
        dfec46 = dfec46.loc[:today+relativedelta(days=45)]
        #print(dfec46)
        
        return dfpivot, dfec46, dfsff

    def get_temp_last(country_list):
    
        #country='Japan'
        
        sqlConnScrape = pyodbc.connect('DRIVER={SQL SERVER};SERVER=PRD-DB-SQL-211;Trusted_Connection=yes')
        
        dfens = pd.DataFrame(columns=country_list)
        dfec46 = pd.DataFrame(columns=country_list)
        dfsff = pd.DataFrame(columns=country_list)
        
        if datetime.datetime.now().hour >8:
    
            sqlens = '''
                        SELECT * FROM Meteorology.dbo.WeatherStationTimeSeriesHistory
                        WHERE ParameterName = 't_2m:C' AND ModelSourceName = 'ecmwf-ens' and WeightingName = 'temperature'
                        AND ForecastDate = {time}
            
                          '''.format(time='\''+str(datetime.date.today()-relativedelta(days=1))+' 00:00:00'+'\'')
            
            dfens = pd.read_sql(sqlens, sqlConnScrape)
            #print(df)
            dfens=dfens[['ValueDate','CountryName','Weighting','Value']]
            dfens['weighted'] = dfens['Weighting']*dfens['Value']
            dfens['ValueDate'] = pd.to_datetime(dfens['ValueDate'])
            #print(i, df)
            dfpivot = dfens.pivot_table(values='weighted',index='ValueDate',columns='CountryName',aggfunc=np.sum)
            dfpivot = dfpivot.resample('D').mean()
            #dfpivot['sum'] = dfpivot.sum(axis=1)
            #print(dfpivot)
        #if 8>datetime.datetime.now().hour > 20:
        else:
            sqlens = '''
                        SELECT * FROM Meteorology.dbo.WeatherStationTimeSeriesHistory
                        WHERE ParameterName = 't_2m:C' AND ModelSourceName = 'ecmwf-ens' and WeightingName = 'temperature'
                        AND ForecastDate = {time}
            
                          '''.format(time='\''+str(datetime.date.today()-relativedelta(days=1))+' 12:00:00'+'\'')
            
            dfens = pd.read_sql(sqlens, sqlConnScrape)
            #print(df)
            dfens=dfens[['ValueDate','CountryName','Weighting','Value']]
            dfens['weighted'] = dfens['Weighting']*dfens['Value']
            dfens['ValueDate'] = pd.to_datetime(dfens['ValueDate'])
            #print(i, df)
            dfpivot = dfens.pivot_table(values='weighted',index='ValueDate',columns='CountryName',aggfunc=np.sum)
            dfpivot = dfpivot.resample('D').mean()
        
        for i in country_list:
            try:
                
            #ec
                sqlec = '''
                        SET NOCOUNT ON; 
                          EXEC Meteorology.dbo.GetModelRunWeatherStations 2, 'ecmwf-vareps', 't_2m:C', NULL, {country}, 'temperature'
            
                          '''.format(country='\''+str(i)+'\'')
                          
                dfec = pd.read_sql(sqlec, sqlConnScrape)
                #print(df)
                dfec=dfec[['ValueDate','WMOId','Weighting','Value']]
                dfec['weighted'] = dfec['Weighting']*dfec['Value']
                dfec['ValueDate'] = pd.to_datetime(dfec['ValueDate'])
                #print(i, df)
                dfpivotec = dfec.pivot_table(values='weighted',index='ValueDate',columns='WMOId')
                dfpivotec['sum'] = dfpivotec.sum(axis=1)
                #print(dfpivot)
                #df.set_index('ValueDate', inplace=True)
                #df.sort_index(inplace=True)
                #print(df)
                dfpivotec = dfpivotec.resample('D').mean()
                #print(dfpivot)
                dfec46[i] = dfpivotec['sum']
            
            #sf    
                sqlsf = '''
                        SET NOCOUNT ON; 
                          EXEC Meteorology.dbo.GetModelRunWeatherStations 2, 'ecmwf-mmsf', 't_2m:C', NULL, {country}, 'temperature'
            
                          '''.format(country='\''+str(i)+'\'')
                          
                dfsf = pd.read_sql(sqlsf, sqlConnScrape)
                #print(df)
                dfsf=dfsf[['ValueDate','WMOId','Weighting','Value']]
                dfsf['weighted'] = dfsf['Weighting']*dfsf['Value']
                dfsf['ValueDate'] = pd.to_datetime(dfsf['ValueDate'])
                #print(i, df)
                dfpivotsf = dfsf.pivot_table(values='weighted',index='ValueDate',columns='WMOId')
                dfpivotsf['sum'] = dfpivotsf.sum(axis=1)
                #print(dfpivot)
                #df.set_index('ValueDate', inplace=True)
                #df.sort_index(inplace=True)
                #print(df)
                dfpivotsf = dfpivotsf.resample('D').mean()
                #print(dfpivot)
                dfsff[i] = dfpivotsf['sum']
            except Exception:
                print(i, 'not in temp last 292')
                pass
            
        today = datetime.date.today()
        dfpivot = dfpivot.loc[:today+relativedelta(days=13)]
        dfec46 =    dfec46.loc[:today+relativedelta(days=44)]
            
        return dfpivot , dfec46, dfsff
    
    def get_JWA_temp_latest(country_list):
        
        #country='Japan'
        today = datetime.date.today()
        sqlConnScrape = pyodbc.connect('DRIVER={SQL SERVER};SERVER=PRD-DB-SQL-211;Trusted_Connection=yes')
        
        dfnormal = pd.DataFrame(index =pd.date_range(start=today+relativedelta(days=(1)), end=today+relativedelta(days=(41))), columns=country_list)
        dfec46 = pd.DataFrame(index = pd.date_range(start=today+relativedelta(days=(1)), end=today+relativedelta(days=(41))), columns=country_list)
        
        if datetime.datetime.now().hour >=11:
    
            sqlens = '''
                        SELECT * FROM Meteorology.dbo.WeatherStationTimeSeriesHistory
                        WHERE ParameterName = 't_2m:C' AND ModelSourceName = 'jwa-14-d'and WeightingName = 'temperature'
                        AND ForecastDate = {time}
            
                          '''.format(time='\''+str(datetime.date.today())+' 00:00:00'+'\'')
            
            dfens = pd.read_sql(sqlens, sqlConnScrape)
            #print(df)
            dfens=dfens[['ValueDate','CountryName','Weighting','Value']]
            dfens['weighted'] = dfens['Weighting']*dfens['Value']
            dfens['ValueDate'] = pd.to_datetime(dfens['ValueDate'])
            #print(i, df)
            dfpivot = dfens.pivot_table(values='weighted',index='ValueDate',columns='CountryName',aggfunc=np.sum)
            dfpivot = dfpivot.resample('D').mean()
            dfpivot = dfpivot.loc[:today+relativedelta(days=14)]
            #dfpivot['sum'] = dfpivot.sum(axis=1)
            #print(dfpivot)
        else:
        #if 11 > datetime.datetime.now().hour:
            sqlens = '''
                        SELECT * FROM Meteorology.dbo.WeatherStationTimeSeriesHistory
                        WHERE ParameterName = 't_2m:C' AND ModelSourceName = 'jwa-14-d'and WeightingName = 'temperature'
                        AND ForecastDate = {time}
            
                          '''.format(time='\''+str(datetime.date.today())+' 00:00:00'+'\'')
            
            dfens = pd.read_sql(sqlens, sqlConnScrape)
            #print(df)
            dfens=dfens[['ValueDate','CountryName','Weighting','Value']]
            dfens['weighted'] = dfens['Weighting']*dfens['Value']
            dfens['ValueDate'] = pd.to_datetime(dfens['ValueDate'])
            #print(i, df)
            dfpivot = dfens.pivot_table(values='weighted',index='ValueDate',columns='CountryName',aggfunc=np.sum)
            #print(dfpivot)
            dfpivot = dfpivot.resample('D').mean()
            dfpivot = dfpivot.loc[:today+relativedelta(days=14)]
       
        sqlec = '''
                    SET NOCOUNT ON;                
                    EXEC Meteorology.dbo.GetModelRunWeatherStations 1, 'jwa-15-w', 't_2m:C', NULL, Null , 'temperature'
                '''
            
        dfec = pd.read_sql(sqlec, sqlConnScrape)
        #print(df)
        dfec=dfec[['ValueDate','CountryName','Weighting','Value']]
        dfec['weighted'] = dfec['Weighting']*dfec['Value']
        dfec['ValueDate'] = pd.to_datetime(dfec['ValueDate'])
        #print(i, df)
        dfpivotec = dfec.pivot_table(values='weighted',index='ValueDate',columns='CountryName',aggfunc=np.sum)
        #dfec15w = dfpivotec.loc[:today+relativedelta(days=45)]
        dfpivotec = dfpivotec.resample('D').mean()
        #print(dfpivotec)
        dfec46 = dfpivotec
        #dfec15w.fillna(method='ffill', inplace=True)
        dfec46.interpolate(inplace=True)
        #print(dfec46['Turkey'])
        
        sqlnor = '''
                    select * from Meteorology.dbo.JwaDailyTemperatureHistory hist
                    inner join Meteorology.dbo.WeatherStationsCurveDefinition ws on ws.CurveId = hist.CurveId
                    where WeightingName = 'temperature'
                '''
            
        dfnor = pd.read_sql(sqlnor, sqlConnScrape)
        #print(df)
        dfnor=dfnor[['ValueDate','CountryName','Weighting','Value']]
        dfnor['weighted'] = dfnor['Weighting']*dfnor['Value']
        #dfnor['ValueDate'] = pd.to_datetime(dfnor['ValueDate'])
        #print(i, df)
        dfnorpivot = dfnor.pivot_table(values='weighted',index='ValueDate',columns='CountryName',aggfunc=np.sum)
        #print(dfpivot)
        
        #dfnorpivot = dfnorpivot.resample('D').mean()
        #dfnorpivot = dfnorpivot.loc[:today+relativedelta(days=14)]
        index =list(map(lambda x:str(today.year)+'-'+x,dfnorpivot.index.to_list()))
        dfnorpivot.index =  index
        dfnormal = pd.DataFrame(index= pd.date_range(start = str(today.year-4)+'-01-01', end = str(today.year)+'-12-31'), columns = dfnorpivot.columns)
        for i in dfnormal.index:
            dfnormal.loc[i] = dfnorpivot.loc[str(today.year)+'-'+i.strftime('%m-%d')]
        #print(dfnormal)
        #dfnormal.interpolate(inplace=True)
        #print(dfnormal.loc[today:,'China'])
        #print(dfec46.loc[today:,'China'])
        #print(dfpivot.loc[today:,'China'])
        
        return dfpivot, dfec46, dfnormal
    
    def get_jwa_last(country_list):
    
        #country='Japan'
        
        sqlConnScrape = pyodbc.connect('DRIVER={SQL SERVER};SERVER=PRD-DB-SQL-211;Trusted_Connection=yes')
        
        dfens = pd.DataFrame(columns=country_list)
        dfec46 = pd.DataFrame(columns=country_list)
        dfsff = pd.DataFrame(columns=country_list)
        
        if datetime.datetime.now().hour >8:
    
            sqlens = '''
                        SELECT * FROM Meteorology.dbo.WeatherStationTimeSeriesHistory
                        WHERE ParameterName = 't_2m:C' AND ModelSourceName = 'jwa-14-d' and WeightingName = 'temperature'
                        AND ForecastDate = {time}
            
                          '''.format(time='\''+str(datetime.date.today()-relativedelta(days=0))+' 00:00:00'+'\'')
            
            dfens = pd.read_sql(sqlens, sqlConnScrape)
            #print(df)
            dfens=dfens[['ValueDate','CountryName','Weighting','Value']]
            dfens['weighted'] = dfens['Weighting']*dfens['Value']
            dfens['ValueDate'] = pd.to_datetime(dfens['ValueDate'])
            #print(i, df)
            dfpivot = dfens.pivot_table(values='weighted',index='ValueDate',columns='CountryName',aggfunc=np.sum)
            dfpivot = dfpivot.resample('D').mean()
            #dfpivot['sum'] = dfpivot.sum(axis=1)
            #print(dfpivot)
        #if 8>datetime.datetime.now().hour > 20:
        else:
            sqlens = '''
                        SELECT * FROM Meteorology.dbo.WeatherStationTimeSeriesHistory
                        WHERE ParameterName = 't_2m:C' AND ModelSourceName = 'jwa-14-d' and WeightingName = 'temperature'
                        AND ForecastDate = {time}
            
                          '''.format(time='\''+str(datetime.date.today()-relativedelta(days=1))+' 12:00:00'+'\'')
            
            dfens = pd.read_sql(sqlens, sqlConnScrape)
            #print(df)
            dfens=dfens[['ValueDate','CountryName','Weighting','Value']]
            dfens['weighted'] = dfens['Weighting']*dfens['Value']
            dfens['ValueDate'] = pd.to_datetime(dfens['ValueDate'])
            #print(i, df)
            dfpivot = dfens.pivot_table(values='weighted',index='ValueDate',columns='CountryName',aggfunc=np.sum)
            dfpivot = dfpivot.resample('D').mean()
        
        for i in country_list:
            try:
                
            #ec
                sqlec = '''
                        SET NOCOUNT ON; 
                          EXEC Meteorology.dbo.GetModelRunWeatherStations 2, 'jwa-15-w', 't_2m:C', NULL, {country}, 'temperature'
            
                          '''.format(country='\''+str(i)+'\'')
                          
                dfec = pd.read_sql(sqlec, sqlConnScrape)
                #print(df)
                dfec=dfec[['ValueDate','WMOId','Weighting','Value']]
                dfec['weighted'] = dfec['Weighting']*dfec['Value']
                dfec['ValueDate'] = pd.to_datetime(dfec['ValueDate'])
                #print(i, df)
                dfpivotec = dfec.pivot_table(values='weighted',index='ValueDate',columns='WMOId')
                dfpivotec['sum'] = dfpivotec.sum(axis=1)
                #print(dfpivot)
                #df.set_index('ValueDate', inplace=True)
                #df.sort_index(inplace=True)
                #print(df)
                dfpivotec = dfpivotec.resample('D').mean()
                #print(dfpivot)
                dfec46[i] = dfpivotec['sum']
                dfec46.interpolate(inplace=True)
            
            #sf    
                sqlsf = '''
                        SET NOCOUNT ON; 
                          EXEC Meteorology.dbo.GetModelRunWeatherStations 2, 'jwa-15-w', 'norm_t_2m:C', NULL, {country}, 'temperature'
            
                          '''.format(country='\''+str(i)+'\'')
                          
                dfsf = pd.read_sql(sqlsf, sqlConnScrape)
                #print(df)
                dfsf=dfsf[['ValueDate','WMOId','Weighting','Value']]
                dfsf['weighted'] = dfsf['Weighting']*dfsf['Value']
                dfsf['ValueDate'] = pd.to_datetime(dfsf['ValueDate'])
                #print(i, df)
                dfpivotsf = dfsf.pivot_table(values='weighted',index='ValueDate',columns='WMOId')
                dfpivotsf['sum'] = dfpivotsf.sum(axis=1)
                #print(dfpivot)
                #df.set_index('ValueDate', inplace=True)
                #df.sort_index(inplace=True)
                #print(df)
                dfpivotsf = dfpivotsf.resample('D').mean()
                #print(dfpivot)
                dfsff[i] = dfpivotsf['sum']
            except Exception:
                print(i, 'not in jwa temp last 497/')
                pass
            
        today = datetime.date.today()
        dfpivot = dfpivot.loc[:today+relativedelta(days=13)]
        dfec46 =    dfec46.loc[:today+relativedelta(days=44)]
            
        return dfpivot , dfec46, dfsff
    
    def weight_temp(dfenslatest,dfec46latest,dfenslast, dfec46last,dfnormal, dfjwaens, dfjwaec46,dfjwaenslast, dfjwaec46last,dfjwanorpivot ):
        
        dfensweight = dfenslatest*0.35 + dfjwaens*0.65
        #print(dfenslatest)
        #print(dfjwaens)
        dfec46weight = dfec46latest*0.35 + dfjwaec46*0.65
        dfec46weight = dfec46weight.loc[dfec46latest.index[0]:dfec46latest.index[-1]]
        #print(dfec46latest)
        #print(dfjwaec46)
        #print(dfec46least)
        dfenslast_weight = dfenslast*0.35 + dfjwaenslast*0.65
        #print(dfenslast)
        #print(dfjwaenslast)
        #print(dfenslast)
        dfec46last_weight = dfec46last*0.35 + dfjwaec46last*0.65
        dfec46last_weight = dfec46last_weight.loc[dfec46last.index[0]:dfec46last.index[-1]]
        #print(dfjwaec46last)
        
        #for i in dfnormal.index:
        dfnormal_weight = dfnormal*0.35 + dfjwanorpivot*0.65
            #dfnormal_weight = dfnormal_weight.loc[dfec46last.index[0]:dfec46last.index[-1]]
        #print(dfnormal.loc['2023-03-06':,'Turkey'])
        #print(dfjwanorpivot.loc['2023-03-06':,'Turkey'])
        #print(dfjwanorpivot)
        #print(dfnormal_weight)
        return dfensweight, dfec46weight, dfenslast_weight, dfec46last_weight, dfnormal_weight
    
    def get_temp_hist_gap():
        
         
        sqlConnScrape = pyodbc.connect('DRIVER={SQL SERVER};SERVER=PRD-DB-SQL-211;Trusted_Connection=yes')
        sqlens = '''
                        SELECT * FROM Meteorology.dbo.WeatherStationTimeSeriesHistory
                        WHERE ParameterName = 't_2m:C' AND ModelSourceName = 'ecmwf-ens' and WeightingName = 'temperature'
                        AND ForecastDate = {time}
            
                          '''.format(time='\''+str(datetime.date.today()-relativedelta(days=10))+' 00:00:00'+'\'')
            
        dfens = pd.read_sql(sqlens, sqlConnScrape)
        #print(df)
        dfens=dfens[['ValueDate','CountryName','Weighting','Value']]
        dfens['weighted'] = dfens['Weighting']*dfens['Value']
        dfens['ValueDate'] = pd.to_datetime(dfens['ValueDate'])
        #print(i, df)
        dfpivot = dfens.pivot_table(values='weighted',index='ValueDate',columns='CountryName',aggfunc=np.sum)
        dfpivot = dfpivot.resample('D').mean()
        
        sqlensjwa = '''
                        SELECT * FROM Meteorology.dbo.WeatherStationTimeSeriesHistory
                        WHERE ParameterName = 't_2m:C' AND ModelSourceName = 'jwa-14-d' and WeightingName = 'temperature'
                        AND ForecastDate = {time}
            
                          '''.format(time='\''+str(datetime.date.today()-relativedelta(days=10))+' 00:00:00'+'\'')
            
        dfensjwa = pd.read_sql(sqlensjwa, sqlConnScrape)
        #print(df)
        dfensjwa=dfensjwa[['ValueDate','CountryName','Weighting','Value']]
        dfensjwa['weighted'] = dfensjwa['Weighting']*dfensjwa['Value']
        dfensjwa['ValueDate'] = pd.to_datetime(dfensjwa['ValueDate'])
        #print(i, df)
        dfpivotjwa = dfensjwa.pivot_table(values='weighted',index='ValueDate',columns='CountryName',aggfunc=np.sum)
        dfpivotjwa = dfpivotjwa.resample('D').mean()
        
        dfpivot_weight = dfpivot*0.35 + dfpivotjwa*0.65
        #print(dfpivot)
        return dfpivot_weight
    
    def get_wind_wave():
        sqlConnScrape = pyodbc.connect('DRIVER={SQL SERVER};SERVER=PRD-DB-SQL-211;Trusted_Connection=yes')
        #wind direction
        sql = '''
              SELECT * FROM Meteorology.dbo.LNGTerminalTimeSeriesLatest
                WHERE ValueDate > CAST(DATEADD(DAY, -5, GETDATE()) AS date) and ValueDate < CAST(DATEADD(DAY, 11, GETDATE()) AS date)
                AND ModelSourceName = 'Mix'
                ORDER BY ValueDate
                
                  '''
    
        dfweather = pd.read_sql(sql, sqlConnScrape)
        
        dfweather.set_index('ParameterName', inplace=True)
        #print(dfweather)
        
        #print(3.4, datetime.datetime.now())
        #get wind data
        dfwind = dfweather.loc['wind_dir_10m:d'].copy()
        dfwind.reset_index(inplace=True)
        dfwind['ValueDate'] = pd.to_datetime(dfwind['ValueDate'])
        dfwindpivot = dfwind.pivot_table(values='Value',index='ValueDate',columns='Name')
        dfwindpivot = dfwindpivot.resample('D').mean()
        dfwindpivot.sort_index(inplace=True)
        
        #print(3.5, datetime.datetime.now())
        dfwindspeed = dfweather.loc['wind_speed_10m:kn'].copy()
        dfwindspeed.reset_index(inplace=True)        #print(dfwindspeed)
        dfwindspeed['ValueDate'] = pd.to_datetime(dfwindspeed['ValueDate'])
        dfwindspeedpivot = dfwindspeed.pivot_table(values='Value',index='ValueDate',columns='Name')
        dfwindspeedpivot = dfwindspeedpivot.resample('D').max()
        dfwindspeedpivot.sort_index(inplace=True)
        #get wave data
        #print(3.6, datetime.datetime.now())
        dfwave = dfweather.loc['mean_wave_direction:d'].copy()
        dfwave.reset_index(inplace=True)  
        dfwave['ValueDate'] = pd.to_datetime(dfwave['ValueDate'])
        dfwavepivot = dfwave.pivot_table(values='Value',index='ValueDate',columns='Name')
        dfwavepivot = dfwavepivot.resample('D').mean()
        dfwavepivot.sort_index(inplace=True)
        #print(3.7, datetime.datetime.now())
        dfwaveheight = dfweather.loc['significant_wave_height:m'].copy()
        dfwaveheight.reset_index(inplace=True)        #print(dfwaveheight)
        dfwaveheight['ValueDate'] = pd.to_datetime(dfwaveheight['ValueDate'])
        dfwvhpivot = dfwaveheight.pivot_table(values='Value',index='ValueDate',columns='Name')
        dfwvhpivot = dfwvhpivot.resample('D').max()
        dfwvhpivot.sort_index(inplace=True)
        #print(dfplantwaveheight)
        return dfwindpivot, dfwindspeedpivot, dfwavepivot, dfwvhpivot
        '''
        dfwind_wave=pd.DataFrame(index=pd.date_range(start=today-datetime.timedelta(days=1)*5, end=today+datetime.timedelta(days=1)*10).date)
        #print(dfwind_wave)
        #print(dfplantwind)
        dfwind_wave=pd.concat([dfwind_wave, dfwindpivot, dfwindspeedpivot, dfwavepivot, dfwvhpivot], axis=1)
        dfwind_wave=dfwind_wave.round(1)
        dfwind_wave.sort_index(inplace=True)
        #dfwind_wave.index = pd.to_datetime(dfwind_wave.index, format = '%b%d')
        dfwind_wave.fillna(0, inplace=True)
        dfwind_wave = dfwind_wave.loc[today-datetime.timedelta(days=1)*5:today+datetime.timedelta(days=1)*10]
        #dfwind_wave.reset_index(inplace=True)
        #dfwind_wave['index'] =  dfwind_wave['index'].dt.date
        dfwind_wave=dfwind_wave.T
        print(dfwind_wave)
        column=[]
        for i in dfwind_wave.columns:
            column.append(str(i.day)+'/'+i.strftime("%b"))
        
        dfwind_wave.columns=column
        #print(column)
        
        return dfwind_wave#dfplantwind,dfplantwindspeed,dfplantwave,dfplantwaveheight
        '''
        
    def num_cargos(dfcurve_id, SupplyCategories, DemandCategories):
        
        today=datetime.date.today()
        #SupplyCategories = pd.read_excel('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/Categories.xlsx',header=(0),sheet_name='Index_Supply')
        #SupplyCategories = SupplyCategories.iloc[:44,0:5]
        #DemandCategories = pd.read_excel('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/Categories.xlsx',header=(0),sheet_name='Index_Demand')
        #DemandCategories = DemandCategories.iloc[:64,0:6]
        #print(SupplyCategories)
        
        Kplertrade=DBTOPD('PRD-DB-SQL-211','LNG', 'dbo', 'KplerLNGTrades')
        dfkplerall = Kplertrade.sql_to_df()
        
        dfkplerall = dfkplerall[['CountryOrigin','StartOrigin','InstallationOrigin','EtaOrigin','CountryDestination','InstallationDestination','StartDestination','EtaDestination']]
        
        dfplant = dfkplerall[['StartOrigin','InstallationOrigin','EtaOrigin']]
        dfterminal = dfkplerall[['InstallationDestination','StartDestination','EtaDestination']]
        
        #plant counts
        dfplantnum = pd.DataFrame(np.nan, index=pd.date_range(start=today-datetime.timedelta(days=1)*5, end=today+datetime.timedelta(days=1)*10).date, columns=SupplyCategories['Plant'].values)
        #count hist cargo numbers
        dfplanthist = dfplant[['StartOrigin','InstallationOrigin']].copy()
        dfplanthist['StartOrigin'] = pd.to_datetime(dfplanthist['StartOrigin']).dt.date
        dfplanthist.set_index('StartOrigin', inplace=True)
        dfplanthist.sort_index(inplace=True)
        #print(dfplanthist.loc[today-datetime.timedelta(days=1)*1:])
        dfplanthist = dfplanthist.loc[today-datetime.timedelta(days=1)*5:today]
        dfplanthist.reset_index(inplace=True)

        #count eta numbers        
        dfplantfcst = dfplant[['EtaOrigin','InstallationOrigin']].copy()
        dfplantfcst['EtaOrigin'] = pd.to_datetime(dfplantfcst['EtaOrigin']).dt.date
        dfplantfcst.set_index('EtaOrigin', inplace=True)
        dfplantfcst.sort_index(inplace=True)
        dfplantfcst = dfplantfcst.loc[today+datetime.timedelta(days=1):today+datetime.timedelta(days=1)*10]
        dfplantfcst.reset_index(inplace=True)
        
        for i in dfplantnum.columns:
            #df1 for past
            df1 = dfplanthist.loc[dfplanthist[dfplanthist['InstallationOrigin']==i].index]
            df1.set_index('StartOrigin', inplace=True)
            #df2 for fcst
            df2 = dfplantfcst.loc[dfplantfcst[dfplantfcst['InstallationOrigin']==i].index]
            df2.set_index('EtaOrigin', inplace=True)
            
            for j in df1.index:
                dfplantnum.loc[j,i] = df1.loc[j].shape[0]
        
            for k in df2.index:
                dfplantnum.loc[k,i] = df2.loc[k].shape[0]
        
        
        dfplantnum.fillna(0, inplace=True)
        dfplantnum=dfplantnum.astype(int)        
        
        #print(dfcurve_id)
        
        
        #demand counts
        #get demand terminal list
        demandterminal = dfcurve_id['installation'].values.tolist()
        supplycountry = SupplyCategories['Plant'].values.tolist()
        
        for i in supplycountry:
            #print(i)
            try:
                demandterminal.remove(i)
            except (ValueError) as e:
                print(i, e)
                
        dfterminalnum = pd.DataFrame(np.nan, index=pd.date_range(start=today-datetime.timedelta(days=1)*5, end=today+datetime.timedelta(days=1)*10).date, columns=demandterminal)
        #count hist cargo numbers
        dfterminalhist = dfterminal[['StartDestination','InstallationDestination']].copy()
        dfterminalhist['StartDestination'] = pd.to_datetime(dfterminalhist['StartDestination']).dt.date
        dfterminalhist.set_index('StartDestination', inplace=True)
        dfterminalhist.sort_index(inplace=True)
        dfterminalhist = dfterminalhist.loc[today-datetime.timedelta(days=1)*5:today]
        dfterminalhist.reset_index(inplace=True)

        #count eta numbers        
        dfterminalfcst = dfterminal[['EtaDestination','InstallationDestination']].copy()
        dfterminalfcst['EtaDestination'] = pd.to_datetime(dfterminalfcst['EtaDestination']).dt.date
        dfterminalfcst.set_index('EtaDestination', inplace=True)
        dfterminalfcst.sort_index(inplace=True)
        dfterminalfcst = dfterminalfcst.loc[today+datetime.timedelta(days=1):today+datetime.timedelta(days=1)*10]
        dfterminalfcst.reset_index(inplace=True)
        
        for i in dfterminalnum.columns:
            #df3 for past
            df3 = dfterminalhist.loc[dfterminalhist[dfterminalhist['InstallationDestination']==i].index]
            df3.set_index('StartDestination', inplace=True)
            #df4 for fcst
            df4 = dfterminalfcst.loc[dfterminalfcst[dfterminalfcst['InstallationDestination']==i].index]
            df4.set_index('EtaDestination', inplace=True)
            
            for j in df3.index:
                dfterminalnum.loc[j,i] = df3.loc[j].shape[0]
        
            for k in df4.index:
                dfterminalnum.loc[k,i] = df4.loc[k].shape[0]
        
        
        dfterminalnum.fillna(0, inplace=True)
        dfterminalnum=dfterminalnum.astype(int)   
        dfcargonum = pd.concat([dfplantnum, dfterminalnum], axis=1)
        #dfcargonum = dfcargonum.T
        
        #print(dfcargonum)
        #print(dfterminalnum)
        
        return dfcargonum


    def wind_wave_table(plant, dfwind, dfwindspeed, dfwave, dfwaveheight, dfcargonum):
        today=datetime.date.today()
        #print(cargo)
        dfwind_wave=pd.DataFrame(index=pd.date_range(start=today-datetime.timedelta(days=1)*5, end=today+datetime.timedelta(days=1)*10).date)
        #print(dfwind_wave)
        #print(dfplantwind)
        dfwind_wave=pd.concat([dfwind_wave, dfwind[[plant]], dfwindspeed[[plant]], dfwave[[plant]], dfwaveheight[[plant]]], axis=1)
        dfwind_wave=dfwind_wave.round(1)
        dfwind_wave.sort_index(inplace=True)
        #dfwind_wave.index = pd.to_datetime(dfwind_wave.index, format = '%b%d')
        dfwind_wave.fillna(0, inplace=True)
        dfwind_wave = dfwind_wave.loc[today-datetime.timedelta(days=1)*5:today+datetime.timedelta(days=1)*10]
        #dfwind_wave.reset_index(inplace=True)
        #dfwind_wave['index'] =  dfwind_wave['index'].dt.date
        dfwind_wave.columns=['Wind Dir','Wind Speed (Knots)','Wave Dir','Wave Height (m)']
        dfwind_wave=dfwind_wave.T
        
        column=[]
        for i in dfwind_wave.columns:
            column.append(str(i.day)+'/'+i.strftime("%b"))
        
        dfwind_wave.columns=column
        
        
        cargo = dfcargonum[[plant]].T
        #print(dfwind_wave)
        #print(cargo)
        dfwind_wave.loc['num of cargos',:] = cargo.values
        
        dfcolor = dfwind_wave.copy()
        dfcolor.loc[['Wind Dir','Wave Dir','num of cargos'],:] = 'white'
        #print(dfcolor.loc['Wind Speed',i])
        for i in dfcolor.columns:
            if dfwind_wave.loc['Wind Speed (Knots)',i] >= 18:
                dfcolor.loc['Wind Speed (Knots)',i] = 'pink'
                dfcolor.loc['num of cargos',i] = 'pink'
            #print(dfcolor.loc['Wind Speed',i])
            if dfwind_wave.loc['Wind Speed (Knots)',i] < 18:
                dfcolor.loc['Wind Speed (Knots)',i] = 'white'
            if dfwind_wave.loc['Wave Height (m)',i] >= 2:
                dfcolor.loc['Wave Height (m)',i] = 'pink'
                dfcolor.loc['num of cargos',i] = 'pink'
            if dfwind_wave.loc['Wave Height (m)',i] < 2:
                dfcolor.loc['Wave Height (m)',i] = 'white'
                
        #dfcolor.loc[:,'index']='paleturquoise'    
        dfcolor.insert(0,'index',['paleturquoise']*dfcolor.shape[0])
        dfcolor=dfcolor.T    
        #create header 
        header1 = ['','','','Historical','','','Today','','','','','','Forecast','','','','','']
        header2 = ['Data type']+list(dfwind_wave.columns)
        header=[]
        for i in range(len(header1)-1):
            header.append([header1[i],header2[i]])
        #print(header)
        
        fig_wind_wave = go.Figure(
            data=[go.Table(
            header=dict(values=header,#['Index']+list(dfwind_wave.columns),
                        line_color='darkslategray',
                        fill_color=['paleturquoise','royalblue','royalblue','royalblue','royalblue','royalblue','SkyBlue','Navy'],
                        align=['center'],
                        font=dict(color=['black','white']),
                        ),
            
            cells=dict(values=[dfwind_wave.index] + [dfwind_wave[pm] for pm in dfwind_wave.columns],
                       #values=[dfwind_wave.]
                       line_color='darkslategray',
                        fill=dict(color=dfcolor.values.tolist()),
                        align=[ 'center'],
                        #font_size=12,
                        #height=30,
                        
                        ))
        ])
        fig_wind_wave.update_layout(title_text=plant)
        
        
        py.plot(fig_wind_wave, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/weather/'+plant+' wind wave.html', auto_open=False) 
        fig_wind_wave.write_image('U:/Trading - Gas/LNG/LNG website/analysis/weather/'+plant+' wind wave.png')
    
    def wind_wave_table_sum(dfwind,dfwindspeed,dfwave,dfwaveheight,SupplyCategories, DemandCategories, dfcurve_id, dfcargonum):
        
        dfcargonum.index = pd.to_datetime(dfcargonum.index)
        
        today=datetime.date.today()
        #get s&d terminal list
        demandterminal = dfcurve_id['installation'].values.tolist()
        supplyterminal = SupplyCategories['Plant'].values.tolist()
        for i in supplyterminal:
            #print(i)
            try:
                demandterminal.remove(i)
            except ValueError:
                #print(i, e)
                pass
        #
        supplyterminal.remove('Coral South FLNG')
        supplyterminal.remove('Elba Island Liq.')
        supplyterminal.remove('Cameron (Liqu.)')
        supplyterminal.remove('Balhaf')
        supplyterminal.remove('Marsa El Brega')
        supplyterminal.append('Coral South LNG')
        supplyterminal.append('Elba Island')
        supplyterminal.append('Cameron')
        
        demandterminal.append('Golden Pass')
        demandterminal.remove('Golden Pass Liq.')
        demandterminal.remove('Son My')
        #Coral South FLNG:Coral South LNG
        #Elba Island Liq.:Elba Island
        #Cameron (Liqu.):Cameron
        #so wind speed no data for Sakhalin 2, Balhaf, Marsa El Brega', 'Portovaya', 'Yamal
        #print(supplyterminal)
        '''WInd'''
        
        #print(dfwindspeed)
        dfwindsignal_supply = dfwindspeed[supplyterminal].copy()
        for i in dfwindsignal_supply.index:
            for j in dfwindsignal_supply.columns:
                if dfwindsignal_supply.loc[i,j] < 18:
                    dfwindsignal_supply.loc[i,j] = np.nan
                else:
                    pass
        #print(dfwindsignal_supply)
        #get sum of supply info and num of cargos
        dfwindsignal_supply_sum = pd.DataFrame(index=dfwindsignal_supply.index, columns=['Terminals (Num of Cargos to load)'])
        dfcargonum.rename(columns={
            'Cameron (Liqu.)':'Cameron',
            'Elba Island Liq.':'Elba Island',
            #'Coral South FLNG':'Coral South LNG',
            'Golden Pass Liq.':'Golden Pass'
            
            },inplace=True)
        
        #print(dfcargonum.columns)
        for i in dfwindsignal_supply.index:
            dfcolumn = dfwindsignal_supply.loc[i].copy()
            
            dfcolumn.dropna(axis=0, inplace=True)
            
            columnlist = dfcolumn.index#dfcolumn.columns.tolist()
            #print(columnlist)
            fulllist = []
            for j in columnlist:
                if dfcargonum.loc[i.strftime('%Y-%m-%d'),j] == 0:
                    pass
                else:
                    fulllist.append(j+'('+str(dfcargonum.loc[i,j])+')')
                
            dfwindsignal_supply_sum.loc[i,'Terminals (Num of Cargos to load)'] = ','.join(k for k in fulllist) 
        #print(dfwindsignal_supply_sum)
        #add date info
        dfwindsignal_supply_sum[' '] = ['','','Historical','','','Today','','','','','Forecast','','','','']
            
        dfwindsignal_supply_sum.reset_index(inplace=True)
        dfwindsignal_supply_sum['ValueDate'] = dfwindsignal_supply_sum['ValueDate'].dt.strftime('%Y-%m-%d')
        #print(dfwindsignal_supply_sum)
        #re index columns
        dfwindsignal_supply_sum = dfwindsignal_supply_sum[[' ','ValueDate','Terminals (Num of Cargos to load)']]
        dfwindsignal_supply_sum = dfwindsignal_supply_sum.rename(columns={'ValueDate':'Date'})
        dfwindsignal_supply_sum.set_index(' ', inplace=True)
        #color list
        colorlist = [['royalblue','royalblue','royalblue','royalblue','royalblue','SkyBlue','Navy']]
        #plot table
        fig_wind_sum = go.Figure(
            data=[go.Table(
            header=dict(values=['']+list(dfwindsignal_supply_sum.columns),
                        line_color='darkslategray',
                        fill_color='paleturquoise',
                        align=['center'],
                        font=dict(color='white', family="Arial Black"),
                        ),
            cells=dict(values=[dfwindsignal_supply_sum.index] + [dfwindsignal_supply_sum[pm] for pm in dfwindsignal_supply_sum.columns],
                       #values=[dfwind_wave.]
                       line_color='darkslategray',
                        fill=dict(color=colorlist),
                        align=[ 'center'],
                        #font_size=12,
                        #height=30,
                        font=dict(color='white', family="Arial Black"),
                        ))
        ])
        fig_wind_sum.update_layout(title_text='Plant and Numbers of Cargos for Max Wind Speed ≥ 18 Knots')
        
        
        py.plot(fig_wind_sum, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/weather/windsum.html', auto_open=False) 
        fig_wind_sum.write_image('U:/Trading - Gas/LNG/LNG website/analysis/weather/windsum.png')
        '''demand ternimals'''
        
        #filter plant wind
        dfwindsignal_demand = dfwindspeed[demandterminal].copy()
        for i in dfwindsignal_demand.index:
            for j in dfwindsignal_demand.columns:
                if dfwindsignal_demand.loc[i,j] < 18:
                    dfwindsignal_demand.loc[i,j] = np.nan
                else:
                    pass
        
        #get sum of demand info and num of cargos
        dfwindsignal_demand_sum = pd.DataFrame(index=dfwindsignal_demand.index, columns=['Terminals (Num of Cargos to load)'])
        #print(dfcargonum.index)
        for i in dfwindsignal_demand.index:
            dfcolumn = dfwindsignal_demand.loc[i].copy()
            
            dfcolumn.dropna(axis=0, inplace=True)
            
            columnlist = dfcolumn.index#dfcolumn.columns.tolist()
            #print(columnlist)
            fulllist = []
            for j in columnlist:
                if dfcargonum.loc[i.strftime('%Y-%m-%d'),j] == 0:
                    pass
                else:
                    fulllist.append(j+'('+str(dfcargonum.loc[i,j])+')')
                
            dfwindsignal_demand_sum.loc[i,'Terminals (Num of Cargos to load)'] = ','.join(k for k in fulllist) 
        
        #print(dfcargonum)
        #add date info
        dfwindsignal_demand_sum[' '] = ['','','Historical','','','Today','','','','','Forecast','','','','']
            
        dfwindsignal_demand_sum.reset_index(inplace=True)
        dfwindsignal_demand_sum['ValueDate'] = dfwindsignal_demand_sum['ValueDate'].dt.strftime('%Y-%m-%d')
        #print(dfwindsignal_demand_sum)
        #re index columns
        dfwindsignal_demand_sum = dfwindsignal_demand_sum[[' ','ValueDate','Terminals (Num of Cargos to load)']]
        dfwindsignal_demand_sum = dfwindsignal_demand_sum.rename(columns={'ValueDate':'Date'})
        dfwindsignal_demand_sum.set_index(' ', inplace=True)
        #color list
        colorlist = [['royalblue','royalblue','royalblue','royalblue','royalblue','SkyBlue','Navy']]
        #plot table
        fig_wind_sum_demand = go.Figure(
            data=[go.Table(
            header=dict(values=['']+list(dfwindsignal_demand_sum.columns),
                        line_color='darkslategray',
                        fill_color='paleturquoise',
                        align=['center'],
                        font=dict(color='white', family="Arial Black"),
                        ),
            cells=dict(values=[dfwindsignal_demand_sum.index] + [dfwindsignal_demand_sum[pm] for pm in dfwindsignal_demand_sum.columns],
                       #values=[dfwind_wave.]
                       line_color='darkslategray',
                        fill=dict(color=colorlist),
                        align=[ 'center'],
                        #font_size=12,
                        #height=30,
                        font=dict(color='white', family="Arial Black"),
                        ))
        ])
        fig_wind_sum_demand.update_layout(title_text='Terminal and Numbers of Cargos for Max Wind Speed ≥ 18 Knots')
        
        
        py.plot(fig_wind_sum_demand, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/weather/windsum_demand.html', auto_open=False) 
        fig_wind_sum_demand.write_image('U:/Trading - Gas/LNG/LNG website/analysis/weather/windsum_demand.png')
        
        '''WAVE'''
        #print(dfwaveheight.columns)
        dfwavesignal_supply = pd.DataFrame(index = dfwaveheight.index)
        for i in supplyterminal:
            try:
                dfwavesignal_supply[i] = dfwaveheight[i].copy()
            except KeyError as e:
                print(e, 'not in wave supply')
                pass
        for i in dfwavesignal_supply.index:
            for j in dfwavesignal_supply.columns:
                if dfwavesignal_supply.loc[i,j] < 2:
                    dfwavesignal_supply.loc[i,j] = np.nan
                else:
                    pass
        #print(dfwindsignal_supply)
        #get sum of supply info and num of cargos
        dfwavesignal_supply_sum = pd.DataFrame(index=dfwavesignal_supply.index, columns=['Terminals (Num of Cargos to load)'])
        #print(dfcargonum.index)
        for i in dfwavesignal_supply.index:
            dfcolumn = dfwavesignal_supply.loc[i].copy()
            
            dfcolumn.dropna(axis=0, inplace=True)
            
            columnlist = dfcolumn.index#dfcolumn.columns.tolist()
            #print(columnlist)
            fulllist = []
            for j in columnlist:
                if dfcargonum.loc[i.strftime('%Y-%m-%d'),j] == 0:
                    pass
                else:
                    fulllist.append(j+'('+str(dfcargonum.loc[i,j])+')')
                
            dfwavesignal_supply_sum.loc[i,'Terminals (Num of Cargos to load)'] = ','.join(k for k in fulllist) 
       
        #add date info
        dfwavesignal_supply_sum[' '] = ['','','Historical','','','Today','','','','','Forecast','','','','']
            
        dfwavesignal_supply_sum.reset_index(inplace=True)
        dfwavesignal_supply_sum['ValueDate'] = dfwavesignal_supply_sum['ValueDate'].dt.strftime('%Y-%m-%d')
        #print(dfwavesignal_supply_sum)
        #re index columns
        dfwavesignal_supply_sum = dfwavesignal_supply_sum[[' ','ValueDate','Terminals (Num of Cargos to load)']]
        dfwavesignal_supply_sum = dfwavesignal_supply_sum.rename(columns={'ValueDate':'Date'})
        dfwavesignal_supply_sum.set_index(' ', inplace=True)
        #color list
        colorlist = [['royalblue','royalblue','royalblue','royalblue','royalblue','SkyBlue','Navy']]
        #plot table
        fig_wave_sum = go.Figure(
            data=[go.Table(
            header=dict(values=['']+list(dfwavesignal_supply_sum.columns),
                        line_color='darkslategray',
                        fill_color='paleturquoise',
                        align=['center'],
                        font=dict(color='white', family="Arial Black"),
                        ),
            cells=dict(values=[dfwavesignal_supply_sum.index] + [dfwavesignal_supply_sum[pm] for pm in dfwavesignal_supply_sum.columns],
                       #values=[dfwave_wave.]
                       line_color='darkslategray',
                        fill=dict(color=colorlist),
                        align=[ 'center'],
                        #font_size=12,
                        #height=30,
                        font=dict(color='white', family="Arial Black"),
                        ))
        ])
        fig_wave_sum.update_layout(title_text='Plant and Numbers of Cargos for Max Wave Height ≥ 2 m')
        
        
        py.plot(fig_wave_sum, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/weather/wavesum.html', auto_open=False) 
        fig_wave_sum.write_image('U:/Trading - Gas/LNG/LNG website/analysis/weather/wavesum.png')
        
        '''demand ternimals'''
        #filter plant wind
        dfwavesignal_demand = pd.DataFrame(index = dfwaveheight.index)
        for i in demandterminal:
            try:
                dfwavesignal_demand[i] = dfwaveheight[i].copy()
            except KeyError as e:
                print(e, 'not in wave demand')
                pass
            
        for i in dfwavesignal_demand.index:
            for j in dfwavesignal_demand.columns:
                if dfwavesignal_demand.loc[i,j] < 2:
                    dfwavesignal_demand.loc[i,j] = np.nan
                else:
                    pass
        
        #get sum of demand info and num of cargos
        dfwavesignal_demand_sum = pd.DataFrame(index=dfwavesignal_demand.index, columns=['Terminals (Num of Cargos to load)'])
        #print(dfcargonum.index)
        for i in dfwavesignal_demand.index:
            dfcolumn = dfwavesignal_demand.loc[i].copy()
            
            dfcolumn.dropna(axis=0, inplace=True)
            
            columnlist = dfcolumn.index#dfcolumn.columns.tolist()
            #print(columnlist)
            fulllist = []
            for j in columnlist:
                if dfcargonum.loc[i.strftime('%Y-%m-%d'),j] == 0:
                    pass
                else:
                    fulllist.append(j+'('+str(dfcargonum.loc[i,j])+')')
                
            dfwavesignal_demand_sum.loc[i,'Terminals (Num of Cargos to load)'] = ','.join(k for k in fulllist) 
        
        #print(dfcargonum)
        #add date info
        dfwavesignal_demand_sum[' '] = ['','','Historical','','','Today','','','','','Forecast','','','','']
            
        dfwavesignal_demand_sum.reset_index(inplace=True)
        dfwavesignal_demand_sum['ValueDate'] = dfwavesignal_demand_sum['ValueDate'].dt.strftime('%Y-%m-%d')
        #print(dfwavesignal_demand_sum)
        #re index columns
        dfwavesignal_demand_sum = dfwavesignal_demand_sum[[' ','ValueDate','Terminals (Num of Cargos to load)']]
        dfwavesignal_demand_sum = dfwavesignal_demand_sum.rename(columns={'ValueDate':'Date'})
        dfwavesignal_demand_sum.set_index(' ', inplace=True)
        #color list
        colorlist = [['royalblue','royalblue','royalblue','royalblue','royalblue','SkyBlue','Navy']]
        #plot table
        fig_wave_sum_demand = go.Figure(
            data=[go.Table(
            header=dict(values=['']+list(dfwavesignal_demand_sum.columns),
                        line_color='darkslategray',
                        fill_color='paleturquoise',
                        align=['center'],
                        font=dict(color='white', family="Arial Black"),
                        ),
            cells=dict(values=[dfwavesignal_demand_sum.index] + [dfwavesignal_demand_sum[pm] for pm in dfwavesignal_demand_sum.columns],
                       #values=[dfwave_wave.]
                       line_color='darkslategray',
                        fill=dict(color=colorlist),
                        align=[ 'center'],
                        #font_size=12,
                        #height=30,
                        font=dict(color='white', family="Arial Black"),
                        ))
        ])
        fig_wave_sum_demand.update_layout(title_text='Terminal and Numbers of Cargos for Max Wave Height ≥ 2 m')
        
        
        py.plot(fig_wave_sum_demand, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/weather/wavesum_demand.html', auto_open=False)
        fig_wave_sum_demand.write_image('U:/Trading - Gas/LNG/LNG website/analysis/weather/wavesum_demand.png')
        
        #print(dfcargonum[['Coral South LNG']])
        
    def temp_hist_chart(country_list, dftemphist, dfsflatest, dfnormal):
        
        today=datetime.date.today()

        index=pd.date_range(start=str(today.year)+'-01-01', end=str(today.year)+'-12-31')
        #print(dfsflatest)
        
        for i in country_list:
            
            #tempcountry = Weather.temp(i, dftemp)
            #season = pd.DataFrame(index=index)
            #for j in dfsflatest.index:
            #    season.loc[str(today.year)+'-'+str(j.month)+'-'+str(j.day),i] = dfsflatest['ecmwf-mmsf'].loc[j]
            #print(tempcountry['ecmwf-mmsf'])
            #print(season)
            
        
            #chart
            figchart = go.Figure()
            figchart.add_trace(go.Scatter(x=index, y=dftemphist.loc[str(today.year-3)+'-01-01':str(today.year-3)+'-12-31',i].values,
                                mode='lines',
                                name=str(today.year-3),
                                visible='legendonly',
                                line=dict(color='lightskyblue', dash='dot')))
            figchart.add_trace(go.Scatter(x=index, y=np.delete(dftemphist.loc[str(today.year-2)+'-01-01':str(today.year-2)+'-12-31',i].values,59),
                                mode='lines',
                                name=str(today.year-2),
                                visible='legendonly',
                                line=dict(color='lightblue', dash='dash')))
            figchart.add_trace(go.Scatter(x=index, y=dftemphist.loc[str(today.year-1)+'-01-01':str(today.year-1)+'-12-31',i].values,
                                mode='lines',
                                name=str(today.year-1),
                                #visible='legendonly',
                                line=dict(color='SkyBlue', dash='solid',width=1)))
            figchart.add_trace(go.Scatter(x=index, y=dftemphist.loc[str(today.year-0)+'-01-01':today,i].values,
                                mode='lines',
                                name=str(today.year-0),
                                line=dict(color='red', dash='solid')))
            figchart.add_trace(go.Scatter(x=index, y=dfnormal.loc[str(today.year-0)+'-01-01':str(today.year-0)+'-12-31',i],
                                mode='lines',
                                name='normal',
                                line=dict(color='black', dash='solid')))
            figchart.add_trace(go.Scatter(x=dfsflatest.index, y=dfsflatest[i],
                            mode='lines',
                            name='Seasonal Fcst',
                            line=dict(color='grey', dash='dot')))
            figchart.add_trace(go.Scatter(x=[datetime.datetime.today().date(),datetime.datetime.today().date()],y=[dftemphist[i].min(),dftemphist[i].max()],
                   mode="lines",
                   line=dict(color='red', width=2, dash='dot'),
                   showlegend=False,
                   name='Today'
                   )
               )
            
            figchart.update_layout(
                autosize=True,
                showlegend=True,
                legend=dict(x=0, y=-0.2),
                legend_orientation="h",
                title_text=i+' Temperature hist by '+str(today),
                xaxis = dict(dtick="M1"),
                yaxis = dict(showgrid=True, gridcolor='lightgrey'),
                hovermode='x unified',
                plot_bgcolor = 'white',
                #xaxis_tickformat = '%B',
                template='ggplot2' ,
                annotations=[dict(x=datetime.datetime.today().date(), y=1, yref='paper',font=dict(color="red",size=14),showarrow=False, xanchor='left', text='Today')],
            )
            
            py.plot(figchart, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/weather/'+i+' hist chart.html', auto_open=False)
            figchart.write_image('U:/Trading - Gas/LNG/LNG website/analysis/weather/'+i+' hist chart.png')
            
            '''
            #table
            dftemphistM=dftemphist.copy().resample('MS').mean().round(1)
            #print(dftemphist.loc['2022-06-28':,i])
            #print(dftemphistM.loc['2020',i+' normal'])
            #print(dftemphistM.loc['2018',i] - dftemphistM.loc['2020',i+' normal'])
            dftemphistM.loc['2018',i] = dftemphistM.loc['2018',i].values - dftemphistM.loc['2020',i+' normal'].values
            dftemphistM.loc['2019',i] = dftemphistM.loc['2019',i].values - dftemphistM.loc['2020',i+' normal'].values
            dftemphistM.loc['2020',i] = dftemphistM.loc['2020',i].values - dftemphistM.loc['2020',i+' normal'].values
            dftemphistM.loc['2021',i] = dftemphistM.loc['2021',i].values - dftemphistM.loc['2020',i+' normal'].values
            dftemphistM.loc['2022',i] = dftemphistM.loc['2022',i].values - dftemphistM.loc['2020',i+' normal'].values
            
            dftemphistM=dftemphistM.round(1)
            
            #print(dftemphistM)
            dfcolor=pd.DataFrame()
            dfcolor.loc[:,'2018'] = dftemphistM.loc['2018',i].values
            dfcolor.loc[:,'2019'] = dftemphistM.loc['2019',i].values
            dfcolor.loc[:,'2020'] = dftemphistM.loc['2020',i].values
            dfcolor.loc[:,'2021'] = dftemphistM.loc['2021',i].values
            dfcolor.loc[:,'2022'] = dftemphistM.loc['2022',i].values
            
            #print(dfcolor)
            for ism in dfcolor.index:
                for jsm in dfcolor.columns:
                    if dfcolor.loc[ism,jsm] > 0:
                        dfcolor.loc[ism,jsm] = 'green'
                    elif dfcolor.loc[ism,jsm] <0:
                        dfcolor.loc[ism,jsm] = 'red'
                    else:
                        dfcolor.loc[ism,jsm] = 'white'
            dfcolor.insert(0,'normal',['paleturquoise']*dfcolor.shape[0])
            dfcolor.insert(0,'index',['paleturquoise']*dfcolor.shape[0])
            dfcolor=dfcolor.T
            
            dftemphistM.fillna(' ', inplace=True)
            
            figtable = go.Figure(data=[go.Table(
                        header=dict(values=['Month  Deg.C','Normal','2018 Delta','2019 Delta','2020 Delta','2021 Delta', '2022 Delta'],
                                    fill_color='paleturquoise',
                                    align='left'),
                        cells=dict(values=[[calendar.month_name[month_idx] for month_idx in range(1, 13)], 
                                           dftemphistM.loc['2020',i+' normal'].values,
                                           dftemphistM.loc['2018',i].values,
                                           dftemphistM.loc['2019',i].values,
                                           dftemphistM.loc['2020',i].values,
                                           dftemphistM.loc['2021',i].values,
                                           dftemphistM.loc['2022',i].values],
                                   #fill_color='lavender',
                                   fill=dict(color=dfcolor.values.tolist()),
                                   #font=dict(color="white"),
                                   align='center'))
                    ])
            figtable.update_layout(title_text=i+' Temperature hist table')
            py.plot(figtable, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/weather/'+i+' hist table.html', auto_open=False)
            '''
    
    def temp_chart(country_list, dfenslatest, dfec46latest, dfsflatest, dfenslast, dfec46last, dfsflast,dfnormal,dftemphistminmax,dftemphistminmaxob, dftemphist,dftemphistgap): 
    #def temp_chart(country, dftempcountry, dftemphist):    
        
        today = datetime.date.today()
        
        index = pd.date_range(start=today, end=dfec46latest.index[-1])
        
        
        
        
        for country in country_list:
            
        
            dfdelta = pd.DataFrame(index = index)#dftempcountry.copy()# - dftempcountrylast
            dfdelta['delta'] = dfenslatest.loc[today:today+relativedelta(days = 14),country].round(2) - dfnormal.loc[today:today+relativedelta(days = 14),country]
            dfdelta.loc[today+relativedelta(days = 14):,'delta'] = dfec46latest.loc[today+relativedelta(days = 14):,country].round(2) - dfnormal.loc[today+relativedelta(days = 14):,country]
      
        #C1 df bar colour, 
            dfdelta["Color"] = np.where(dfdelta["delta"]<0, 'blue', 'red')
        
        
            #chart
            #for i in countrylist:
            figchart = make_subplots(specs=[[{"secondary_y": True}]])
            figchart.add_trace(go.Bar(name='Delta (Fcst - Normal)', x=index,y=dfdelta.loc[index,'delta'],
                                  marker_color=dfdelta.loc[index, 'Color'])
                               ,secondary_y=True)
            figchart.add_trace(go.Scatter(x=index, y=dfenslatest.loc[:,country],
                                mode='lines',
                                name='Ensemble '+str(today),
                                line=dict(color='blue', dash='solid')),secondary_y=False)
            figchart.add_trace(go.Scatter(x=index, y=dfec46latest.loc[index,country],
                                mode='lines',
                                name='EC46',
                                line=dict(color='lightskyblue', dash='solid')),secondary_y=False)
            figchart.add_trace(go.Scatter(x=index, y=dfsflatest.loc[index,country],
                                mode='lines',
                                name='Seasonal Fcst',
                                visible='legendonly',
                                line=dict(color='red', dash='solid')),secondary_y=False)
            
            figchart.add_trace(go.Scatter(x=index, y=dfenslast.loc[index[0]:,country],
                                mode='lines',
                                name='Ensemble last '+str(today-relativedelta(days=(1))),
                                line=dict(color='blue', dash='dot')),secondary_y=False)
            figchart.add_trace(go.Scatter(x=index, y=dfec46last.loc[index[0]:,country],
                                mode='lines',
                                name='EC46 last',
                                line=dict(color='lightskyblue', dash='dot')),secondary_y=False)
            '''
            figchart.add_trace(go.Scatter(x=index, y=dftempcountrylast.loc[index,'ecmwf-mmsf'],
                                mode='lines',
                                name='Seasonal Fcst last',
                                visible='legendonly',
                                line=dict(color='red', dash='dot')))
            '''
            figchart.add_trace(go.Scatter(x=index, y=dfnormal.loc[index,country],
                                mode='lines',
                                name='Normal',
                                line=dict(color='black', dash='solid')),secondary_y=False)
            try:
                figchart.add_trace(go.Scatter(x=index,y=dftemphistminmaxob.loc['2020-'+str(index[0].month)+'-'+str(index[0].day): '2020-'+str(index[-1].month)+'-'+str(index[-1].day), country+' max'],
                                    fill='tonexty',
                                    fillcolor='rgba(65,105,225,0)',
                                    line_color='rgba(65,105,225,0)',
                                    showlegend=False,
                                    name='Min/Max 1990-2022'
                                    ),secondary_y=False)
                        
                figchart.add_trace(go.Scatter(x=index,y=dftemphistminmaxob.loc['2020-'+str(index[0].month)+'-'+str(index[0].day): '2020-'+str(index[-1].month)+'-'+str(index[-1].day), country+' min'],
                                    fill='tonexty',
                                    fillcolor='rgba(65,105,225,0.1)',
                                    line_color='rgba(65,105,225,0)',
                                    showlegend=True,
                                    name='Min/Max 1990-2022'
                                    ),secondary_y=False)
            except KeyError:
                print(country + ' not in mis-obs')
                figchart.add_trace(go.Scatter(x=index,y=dftemphistminmax.loc['2020-'+str(index[0].month)+'-'+str(index[0].day): '2020-'+str(index[-1].month)+'-'+str(index[-1].day), country+' man'],
                                    fill='tonexty',
                                    fillcolor='rgba(65,105,225,0)',
                                    line_color='rgba(65,105,225,0)',
                                    showlegend=False,
                                    name='Min/Max 1990-2022'
                                    ),secondary_y=False)
                        
                figchart.add_trace(go.Scatter(x=index,y=dftemphistminmax.loc['2020-'+str(index[0].month)+'-'+str(index[0].day): '2020-'+str(index[-1].month)+'-'+str(index[-1].day), country+' min'],
                                    fill='tonexty',
                                    fillcolor='rgba(65,105,225,0.1)',
                                    line_color='rgba(65,105,225,0)',
                                    showlegend=True,
                                    name='Min/Max 1990-2022'
                                    ),secondary_y=False)
                
                
                
            figchart.update_layout(barmode='stack')
            figchart.update_yaxes(range=[dfdelta['delta'].min()*2, dfdelta['delta'].max()*10], title_text="Delta ℃", secondary_y=True)
            figchart.update_yaxes(title_text="Temp", secondary_y=False)
            figchart.update_layout(
                autosize=True,
                showlegend=True,
                legend=dict(x=0, y=-0.2),
                legend_orientation="h",
                title_text=country+' Temperature '+str(today),
                #xaxis = dict(dtick="W1"),
                yaxis = dict(showgrid=True, gridcolor='lightgrey'),
                hovermode='x unified',
                plot_bgcolor = 'white',
                #xaxis_tickformat = '%B',
                template='ggplot2'  
            )
            '''
            figchart.update_xaxes(
                showline=True, 
                linewidth=1, 
                linecolor='LightGrey', 
                showgrid=False, 
                gridwidth=1, 
                gridcolor='LightGrey',
                rangeselector=dict(
                                    buttons=list([
                                        dict(count=1, label="1m", step="month", stepmode="backward"),
                                        
                                    ]))
            )
            '''
            figchart.update_layout(xaxis_range=[today, today+relativedelta(days=(41))])
            
            py.plot(figchart, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/weather/'+country+' chart.html', auto_open=False)
            figchart.write_image('U:/Trading - Gas/LNG/LNG website/analysis/weather/'+country+' chart.png')
        
        #print(dftemphist)
        #print(dftempcountry)
        #hist_end = dftemphist[country].loc[~dftemphist[country].isnull()].index[-1]
        #ens_end = dfenslatest[country].loc[~dfenslatest[country].isnull()].index[-1]
        #ec_end = dftempcountry['ecmwf-vareps'].loc[~dftempcountry['ecmwf-vareps'].isnull()].index[-1]
            hist_end = dftemphist.index[-1]
            ens_end = dfenslatest.index[-1]
            ec_end = dfec46latest.index[-1]
       
            #1 months forward view by ec46 end date
            if ec_end <=  pd.to_datetime(str((today+relativedelta(months=1)).year)+'-'+str((today+relativedelta(months=1)).month)+'-15'):
                table_end = str((today+relativedelta(months=0)).year)+'-'+str((today+relativedelta(months=0 )).month)+'-'+str(calendar.monthrange((today+relativedelta(months=0)).year, (today+relativedelta(months=0)).month)[1])
            else:
                table_end = str((today+relativedelta(months=1)).year)+'-'+str((today+relativedelta(months=1)).month)+'-'+str(calendar.monthrange((today+relativedelta(months=1)).year, (today+relativedelta(months=1)).month)[1])
        
            #print(ens_end, ec_end, table_end)
            
            dftemphistM= pd.DataFrame(index = dfnormal.index)
            dftemphistM[country] = dftemphist[[country]].copy()
            dftemphistM[country+' normal'] = dfnormal[country]
            
            #print(dftemphistM.loc[ec_end:pd.to_datetime(table_end),country] )
            #print( dfnormal.loc[ec_end:pd.to_datetime(table_end),country])
            #print(dfnormal)
            dftemphistM.loc[hist_end:today,country] = dftemphistgap.loc[hist_end:today, country].values
            dftemphistM.loc[today:ens_end,country] = dfenslatest.loc[today:ens_end, country].values
            #print( dftemphistM.loc[ens_end:table_end,country])
            #print(dfec46latest.loc[ens_end:table_end,country].values)
            if ec_end < pd.to_datetime(table_end):
                dftemphistM.loc[ens_end:ec_end,country] = dfec46latest.loc[ens_end:ec_end,country].values
                #dftemphistM.loc[ec_end:pd.to_datetime(table_end),country] = dfsflatest.loc[ec_end:pd.to_datetime(table_end),country].values
                
                dftemphistM.loc[ec_end:pd.to_datetime(table_end),country] = dfnormal.loc[ec_end:pd.to_datetime(table_end),country].values
            else:
                dftemphistM.loc[ens_end:table_end,country] = dfec46latest.loc[ens_end:table_end,country].values
                

            #print(dftemphistM.loc['2022-06-18':'2022-08-30',country])
            dftemphistM = dftemphistM.astype('float')
            dftemphistM=dftemphistM.resample('MS').mean().round(1)
            #print(dftemphistM)
            #print(dftemphistM.loc['2020',i+' normal'])
            #print(dftemphistM.loc['2018',i] - dftemphistM.loc['2020',i+' normal'])
            
            dftemphistM.loc[str(today.year-4),country] = dftemphistM.loc[str(today.year-4),country].values - dftemphistM.loc[str(today.year-4),country+' normal'].values
            dftemphistM.loc[str(today.year-3),country] = dftemphistM.loc[str(today.year-3),country].values - dftemphistM.loc[str(today.year-3),country+' normal'].values
            dftemphistM.loc[str(today.year-2),country] = dftemphistM.loc[str(today.year-2),country].values - dftemphistM.loc[str(today.year-2),country+' normal'].values
            dftemphistM.loc[str(today.year-1),country] = dftemphistM.loc[str(today.year-1),country].values - dftemphistM.loc[str(today.year-1),country+' normal'].values
            dftemphistM.loc[str(today.year-0),country] = dftemphistM.loc[str(today.year-0),country].values - dftemphistM.loc[str(today.year-0),country+' normal'].values
            
            dftemphistM=dftemphistM.round(1)
            dftemphistM = dftemphistM.loc[:table_end]
            #dfcolor = dftemphistM.copy().T
            
            #print(dftemphistM)
            dfcolor=pd.DataFrame()
            dfcolor.loc[:,str(today.year-4)] = dftemphistM.loc[str(today.year-4),country].values
            dfcolor.loc[:,str(today.year-3)] = dftemphistM.loc[str(today.year-3),country].values
            dfcolor.loc[:,str(today.year-2)] = dftemphistM.loc[str(today.year-2),country].values
            dfcolor.loc[:,str(today.year-1)] = dftemphistM.loc[str(today.year-1),country].values
            #print(dfcolor)
            #print( dftemphistM.loc[str(today.year-0),country])
            dfcolor.loc[:pd.to_datetime(table_end).month-1,str(today.year-0)] = dftemphistM.loc[str(today.year-0),country].values
            
            '''
            #print(dfcolor)
            for ism in dfcolor.index:
                for jsm in dfcolor.columns:
                    if dfcolor.loc[ism,jsm] > 0:
                        dfcolor.loc[ism,jsm] = 'red'
                    elif dfcolor.loc[ism,jsm] <0:
                        dfcolor.loc[ism,jsm] = 'blue'
                    else:
                        dfcolor.loc[ism,jsm] = 'white'
            dfcolor.insert(0,'normal',['paleturquoise']*dfcolor.shape[0])
            dfcolor.insert(0,'index',['paleturquoise']*dfcolor.shape[0])
            dfcolor.loc[today.month-1:, '2022'] = 'white'
            #print(dfcolor)
            dfcolor=dfcolor.T
            '''
            if ens_end.year > today.year or ec_end.year > today.year:
                dfcolor.loc[:,str(today.year+1)] = dftemphistM.loc[str(today.year+1),country]
            #print(dfcolor)
            for ism in dfcolor.index:
                for jsm in dfcolor.columns:
                    if dfcolor.loc[ism,jsm] > 0:
                        dfcolor.loc[ism,jsm] = 'red'
                    elif dfcolor.loc[ism,jsm] <0:
                        dfcolor.loc[ism,jsm] = 'blue'
                    else:
                        dfcolor.loc[ism,jsm] = 'white'
            dfcolor.insert(0,'normal',['paleturquoise']*dfcolor.shape[0])
            dfcolor.insert(0,'index',['paleturquoise']*dfcolor.shape[0])
            dfcolor.loc[today.month-1:, str(today.year-0)] = 'white'
            #print(dfcolor)
            dfcolor=dfcolor.T
            
            dftext_color = pd.DataFrame()
            dftext_color.loc[:,str(today.year-4)] = dftemphistM.loc[str(today.year-4),country].values
            dftext_color.loc[:,str(today.year-3)] = dftemphistM.loc[str(today.year-3),country].values
            dftext_color.loc[:,str(today.year-2)] = dftemphistM.loc[str(today.year-2),country].values
            dftext_color.loc[:,str(today.year-1)] = dftemphistM.loc[str(today.year-1),country].values
            #print(dftext_color.loc[:pd.to_datetime(table_end).month-1,str(today.year-0)])
            dftext_color.loc[:pd.to_datetime(table_end).month-1,str(today.year-0)] = dftemphistM.loc[str(today.year-0),country].values
            #print(dftext_color)
            '''
            for ism in dftext_color.index:
                for jsm in dftext_color.columns:
                    if dftext_color.loc[ism,jsm] > 0:
                        dftext_color.loc[ism,jsm] = 'white'
                    elif dftext_color.loc[ism,jsm] < 0:
                        dftext_color.loc[ism,jsm] = 'white'
                    else:#if dftext_color.loc[ism,jsm] == 0:
                        dftext_color.loc[ism,jsm] = 'black'
            dftext_color.insert(0,'normal',['black']*dftext_color.shape[0])
            dftext_color.insert(0,'index',['black']*dftext_color.shape[0])
            #print(dftemphistM.loc[str(today.year-0),i])
            for j in pd.date_range(start = str(today.year)+'-'+str(today.month)+'-01', end=str(today.year)+'-12-01', freq='MS'):
                if dftemphistM.loc[j, i] < 0:
                    dftext_color.loc[(j.month-1), str(today.year)] = 'blue'
                elif dftemphistM.loc[j, i] > 0:
                    dftext_color.loc[(j.month-1), str(today.year)] = 'red'
                elif dftemphistM.loc[j, i] == 0:
                    dftext_color.loc[(j.month-1), str(today.year)] = 'black'
                else:
                    dftext_color.loc[(j.month-1), str(today.year)] = 'white'
            '''
            if ens_end.year > today.year or ec_end.year > today.year:
                dftext_color.loc[:,str(today.year+1)] = dftemphistM.loc[str(today.year+1),country]
            for ism in dftext_color.index:
                for jsm in dftext_color.columns[:-1]:
                    if dftext_color.loc[ism,jsm] > 0:
                        dftext_color.loc[ism,jsm] = 'white'
                    elif dftext_color.loc[ism,jsm] < 0:
                        dftext_color.loc[ism,jsm] = 'white'
                    else:#if dftext_color.loc[ism,jsm] == 0:
                        dftext_color.loc[ism,jsm] = 'black'
            
            #color text for cunnrent year and month
            #print(dftext_color.columns)
            for ism in dftext_color.index[:today.month-1]:
                #for jsm in dftext_color.columns[-1]:
                    #print(ism, jsm)
                if dftext_color.loc[ism,dftext_color.columns[-1]] > 0:
                    dftext_color.loc[ism,dftext_color.columns[-1]] = 'white'
                elif dftext_color.loc[ism,dftext_color.columns[-1]] < 0:
                    dftext_color.loc[ism,dftext_color.columns[-1]] = 'white'
                else:#if dftext_color.loc[ism,jsm] == 0:
                    dftext_color.loc[ism,dftext_color.columns[-1]] = 'black'
            #print(dftext_color)
            #print(dftext_color.columns[-1])
            for ism in dftext_color.index[today.month-1:]:
                #for jsm in dftext_color.columns[-1]:
                #print(jsm)
                if dftext_color.loc[ism,dftext_color.columns[-1]] > 0:
                    dftext_color.loc[ism,dftext_color.columns[-1]] = 'red'
                elif dftext_color.loc[ism,dftext_color.columns[-1]] < 0:
                    dftext_color.loc[ism,dftext_color.columns[-1]] = 'blue'
                else:#if dftext_color.loc[ism,jsm] == 0:
                    dftext_color.loc[ism,dftext_color.columns[-1]] = 'black'
            
            #print(dftext_color)
            dftext_color.insert(0,'normal',['black']*dftext_color.shape[0])
            dftext_color.insert(0,'index',['black']*dftext_color.shape[0])
            #print(dftemphistM)
            #print(dftemphistM)
            dftext_color=dftext_color.T
            
            dftemphistM.fillna(' ', inplace=True)
            
            figtable = go.Figure(data=[go.Table(
                        header=dict(values=['Month  Deg.C','Normal',str(today.year-4)+' Delta',str(today.year-3)+' Delta',str(today.year-2)+' Delta',str(today.year-1)+' Delta', str(today.year)+' Delta'],
                                    fill_color='paleturquoise',
                                    align='left'),
                        cells=dict(values=[[calendar.month_name[month_idx] for month_idx in range(1, 13)], 
                                           dftemphistM.loc['2020',country+' normal'].values,
                                           dftemphistM.loc[str(today.year-4),country].values,
                                           dftemphistM.loc[str(today.year-3),country].values,
                                           dftemphistM.loc[str(today.year-2),country].values,
                                           dftemphistM.loc[str(today.year-1),country].values,
                                           dftemphistM.loc[str(today.year-0),country].values],
                                   #fill_color='lavender',
                                   fill=dict(color=dfcolor.values.tolist()),
                                   font=dict(color=dftext_color.values.tolist()),
                                   align='center'))
                    ])
            figtable.update_layout(title_text=country+' Temperature hist and fcst table '+ str(today))
        
            py.plot(figtable, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/weather/'+country+' table.html', auto_open=False)
            figtable.write_image('U:/Trading - Gas/LNG/LNG website/analysis/weather/'+country+' table.png')

    def temp_dod_table(countrylist, dfenslatest, dfec46latest,dfnormal,dfenslast, dfec46last):  
        
        today = datetime.date.today()
        
        week_index = pd.date_range(start=today+relativedelta(days=(11)), end=today+relativedelta(days=(40)), freq='W')
        
        #print(week_index)
        #print(dfenslatest)
        ens_end = dfenslatest.index[-1]
        ec_end = dfec46latest.index[-1]
   
        
        #dftableensdod = pd.DataFrame(index = pd.date_range(start=today, end = dfenslatest.index[-1]), columns = countrylist)
        dftableensdod = (dfenslatest.loc[today:today+relativedelta(days=(10))] - dfenslast.loc[today:today+relativedelta(days=(10))]).mean().round(1)
        
        #dftableecdod = pd.DataFrame(index = week_index, columns = ['delta'])
        dftableecdod = (dfec46latest.loc[week_index] - dfec46last.loc[week_index]).mean().round(1)
        #print(dftableensdod.loc['South Korea'])
        #print(dftableecdod)
        
        
        
        #dfdelta = dftemphistM - dfnormal
        #print(dfdelta)
        dfdelta_table1 = pd.DataFrame(index = pd.date_range(start=today, end=today+relativedelta(days=(10))), columns = countrylist)
        dfdelta_table2 = pd.DataFrame(index = week_index, columns = countrylist)
        dfdelta_table = pd.concat([dfdelta_table1, dfdelta_table2])
        #dfdelta_table.loc[week_index] = np.nan
        
        dfnormal = dfnormal.astype('float')
        
        for i in countrylist:
            #print(dfenslatest.loc[today:today+relativedelta(days=(10)), i])
            #print(dfnormal.loc[today:today+relativedelta(days=(10)), i])
            dfdelta_table.loc[today:today+relativedelta(days=(10)), i] = (dfenslatest.loc[today:today+relativedelta(days=(10)), i] - dfnormal.loc[today:today+relativedelta(days=(10)), i]).round(1)# dfdelta.loc[today:today+relativedelta(days=(10)), i].round(1)
            dfdelta_table.loc[week_index, i] =  (dfec46latest.loc[week_index, i] - dfnormal.loc[week_index, i]).resample('W').mean().round(1)  #dfdelta.loc[week_index, 'ecmwf-vareps '+i].resample('W').mean().round(1)                                                                                                 
        
        #dfdelta_table = dfdelta_table
        dfdelta_table_ave = pd.DataFrame(index = ['Ens Ave.','EC46 Ave.','Ens DoD','EC46 DoD'], columns = countrylist)
        dfdelta_table_ave.loc['Ens Ave.'] = dfdelta_table.loc[today:today+relativedelta(days=(10))].mean().round(1)
        dfdelta_table_ave.loc['EC46 Ave.'] = dfdelta_table.loc[week_index].mean().round(1)
        dfdelta_table_ave.loc['Ens DoD'] = dftableensdod
        dfdelta_table_ave.loc['EC46 DoD'] = dftableecdod
        
        #print(dfdelta_table)
        #print(dfdelta_table_ave)
        dfdelta_table = pd.concat([dfdelta_table_ave, dfdelta_table], axis=0)
        #print(dfdelta_table)
        #print(dfdelta_table_ave)
        
        DemandCategories=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'CategoriesDemand').sql_to_df()
        DemandCategories.drop(DemandCategories[DemandCategories['Country'] == 'Russian Federation'].index, inplace=True)
        DemandCategories.set_index('Region', inplace=True)
        
        #dfdelta_table_JKTC['JKTC'] = np.nan
        JKTC = DemandCategories['Country'].loc['JKTC'].values.tolist()
        dfdelta_table_JKTC = dfdelta_table.loc[:,JKTC]
        #print(dfdelta_table_JKTC)
        dfdelta_table_JKTC['Lat Am'] = np.nan
        
        LatAm = DemandCategories['Country'].loc['Lat Am'].values.tolist()
        for i in ['El Salvador', 'Dominican Republic', 'Panama', 'Chile', 'Jamaica', 'Puerto Rico']:
            LatAm.remove(i)
        dfdelta_table_LatAm = dfdelta_table.loc[:,LatAm]
        dfdelta_table_LatAm['MEIP'] = np.nan
        #print(dfdelta_table_LatAm)
            
        MEIP = DemandCategories['Country'].loc['MEIP'].values.tolist()
        for i in ['Bahrain', 'Oman', 'Jordan', 'United Arab Emirates']:
        #for i in ['Bahrain', 'Bangladesh', 'Oman', 'Kuwait', 'Jordan', 'United Arab Emirates']:
            MEIP.remove(i)
        dfdelta_table_MEIP  = dfdelta_table.loc[:,MEIP]
        dfdelta_table_MEIP['NW Eur'] = np.nan
        
        NWEur = DemandCategories['Country'].loc['NW Eur'].values.tolist()
        dfdelta_table_NWEur = dfdelta_table.loc[:,NWEur]
        dfdelta_table_NWEur['Other Eur'] = np.nan
        
        OtherEur = DemandCategories['Country'].loc['Other Eur'].values.tolist()
        for i in ['Malta', 'Cyprus']:
            OtherEur.remove(i)
        dfdelta_table_OtherEur = dfdelta_table.loc[:,OtherEur]
        dfdelta_table_OtherEur['Med Eur'] = np.nan
        
        MedEur = DemandCategories['Country'].loc['Med Eur'].values.tolist()
        dfdelta_table_MedEur = dfdelta_table.loc[:,MedEur]
        dfdelta_table_MedEur['Other RoW'] = np.nan
        
        OtherRoW = DemandCategories['Country'].loc['Other RoW'].values.tolist()
        for i in ['Israel', 'Singapore Republic', 'United States', 'Canada', 'Myanmar', 'Thailand', 'Ghana']:
            OtherRoW.remove(i)
        dfdelta_table_OtherRoW = dfdelta_table.loc[:,OtherRoW]
        
        #dfdelta_table_ave = dfdelta_table.loc[['Ens Ave.','EC46 Ave.']]
        
        delta_table_all = pd.concat([dfdelta_table_JKTC, dfdelta_table_LatAm, dfdelta_table_MEIP, dfdelta_table_NWEur, dfdelta_table_OtherEur, dfdelta_table_MedEur,dfdelta_table_OtherRoW], axis=1)
        #print(delta_table_all)
        
        #histnormal=pd.DataFrame(index=pd.to_datetime(delta_table_all.index[2:]), columns = dftemphist.columns)

        #for i in histnormal.index:
            #histnormal.loc[i] = dftemphist.loc['2020-'+str(i.month)+'-'+str(i.day)]
        #print(histnormal)    
        #print(histnormal.index)
        dfnormaltable = pd.DataFrame(index = ['normal'], columns = delta_table_all.columns)
        #delta_table_all.loc['normal'] = np.nan
        for i in dfnormaltable.columns:
            try:
                #print(histnormal.loc[:,i+' normal'].mean())
                dfnormaltable.loc['normal',i] = round(dfnormal.loc[today:ec_end,i].mean(),1)#.round(1)
            except KeyError:
                dfnormaltable.loc['normal',i] = np.nan
        
        #print(dfnormaltable)
        delta_table_all = pd.concat([dfnormaltable, delta_table_all])
        #print(delta_table_all)
        
        #C1 df bar colour, 
        dfdelta_color = delta_table_all.copy()
        dfdelta_color_cell = delta_table_all.copy()
        #print(dfdelta_color)
        
        for ism in dfdelta_color.index[5:]:
            for jsm in dfdelta_color.columns:
                #print(dfdelta_color.loc[ism,jsm])
                if dfdelta_color.loc[ism,jsm] > 0:
                    dfdelta_color.loc[ism,jsm] = 'red'
                    dfdelta_color_cell.loc[ism,jsm] = 'white'
                elif dfdelta_color.loc[ism,jsm] < 0:
                    dfdelta_color.loc[ism,jsm] = 'blue'
                    dfdelta_color_cell.loc[ism,jsm] = 'white'
                elif dfdelta_color.loc[ism,jsm] == 0:
                    dfdelta_color.loc[ism,jsm] = 'black'
                    dfdelta_color_cell.loc[ism,jsm] = 'white'
                else:
                    dfdelta_color.loc[ism,jsm] = 'LightGrey'
                    dfdelta_color_cell.loc[ism,jsm] = 'LightGrey'
        #color ave two columns
        for ism in dfdelta_color.index[1:5]:
            for jsm in dfdelta_color.columns:
                #print(dfdelta_color.loc[ism,jsm])
                if dfdelta_color.loc[ism,jsm] > 0:
                    dfdelta_color.loc[ism,jsm] = 'white'
                    dfdelta_color_cell.loc[ism,jsm] = 'red'
                elif dfdelta_color.loc[ism,jsm] < 0:
                    dfdelta_color.loc[ism,jsm] = 'white'
                    dfdelta_color_cell.loc[ism,jsm] = 'blue'
                elif dfdelta_color.loc[ism,jsm] == 0:
                    dfdelta_color.loc[ism,jsm] = 'black'
                    dfdelta_color_cell.loc[ism,jsm] = 'white'
                else:
                    dfdelta_color.loc[ism,jsm] = 'LightGrey'
                    dfdelta_color_cell.loc[ism,jsm] = 'LightGrey'    
        dfdelta_color.loc['normal'] = 'black'
        dfdelta_color_cell.loc['normal'] = 'white'    
        #print(dfdelta_color)
        #print(dfdelta_color_cell)
        
        dfdelta_color=dfdelta_color
        
        columns = list(delta_table_all.index[0:5]) + list(pd.to_datetime(delta_table_all.index[5:]).strftime('%Y-%m-%d')) 
        index = list(delta_table_all.columns)
        #print(columns)
        #print(index)
        
        delta_table_all = delta_table_all.T 
        delta_table_all.fillna(' ', inplace=True)
        delta_table_all.columns=columns
        #delta_table_all.reset_index(inplace=True)
        #print(list(delta_table_all.columns))
        #print(delta_table_all.loc[:,'2022-07-11'].to_list())
        #print([index]+[delta_table_all.loc[:,'2022-07-11'].to_list()])
        '''
        delta_table_header = [[' ', 'Market'],
                              [' ', columns[0]],
                              [' ', columns[1]],
                              ['Ensemble vs. Normal', columns[2]],
                              ['Ensemble vs. Normal', columns[3]],
                              ['Ensemble vs. Normal', columns[4]],
                              ['Ensemble vs. Normal', columns[5]],
                              ['Ensemble vs. Normal', columns[6]],
                              ['Ensemble vs. Normal', columns[7]],
                              ['Ensemble vs. Normal', columns[8]],
                              ['Ensemble vs. Normal', columns[9]],
                              ['Ensemble vs. Normal', columns[10]],
                              ['Ensemble vs. Normal', columns[11]],
                              ['Ensemble vs. Normal', columns[12]],
                              ['Ensemble vs. Normal', columns[13]],
                              ['EC46 Weekly Average vs. Normal', columns[14]],
                              ['EC46 Weekly Average vs. Normal', columns[15]],
                              ['EC46 Weekly Average vs. Normal', columns[16]],
                              ['EC46 Weekly Average vs. Normal', columns[17]] 
            ]
        '''
        delta_table_header = [[' ', 'Market'],
                              [str(today)+' - '+ec_end.strftime('%Y-%m-%d'), columns[0]],
                              [str(today)+' - '+(today+relativedelta(days=(10))).strftime('%Y-%m-%d'), columns[1]],
                              [str(today+relativedelta(days=(11)))+' - '+str(today+relativedelta(days=(40))), columns[2]],
                              [str(today)+' vs. '+ str(today-relativedelta(days=1)), columns[3]],
                              [dfec46latest.index[0].strftime('%Y-%m-%d')+' vs. '+dfec46last.index[0].strftime('%Y-%m-%d'), columns[4]]
                              ]
        for i in columns[5:16]:
            delta_table_header.append(['Ensemble vs. Normal', i])
        for i in columns[16:]:
            delta_table_header.append(['EC46 Weekly End Average vs. Normal', i])
        #print(delta_table_header)
        #print(delta_table_all)
        
        
        figtable = go.Figure(data=[go.Table(
                    header=dict(values = delta_table_header,#['Market']+columns,
                                fill_color=['SkyBlue']*6+['paleturquoise']*10+['SkyBlue']*len(week_index),
                                align='center'),
                    cells=dict(values = [index] + [delta_table_all.loc[:,pm].to_list() for pm in columns],#delta_table_all.columns],
                               #fill_color='lavender',
                               
                               fill=dict(color=[['white','white','white','white','LightGrey','white','white','white','LightGrey','white','white','white','white','white','LightGrey','white','white','white','white','white','LightGrey','white','white','white','white','white','white','LightGrey','white','white','white','white','white','LightGrey','white','white','white','white',]]+dfdelta_color_cell.values.tolist()),
                               font=dict(color=['black']+dfdelta_color.values.tolist()),
                               align='center'))
                ])
        figtable.update_layout(title_text='DoD Temperature fcst delta table(Ens daily & EC46 weekly ave.) '+str(today))
    
        py.plot(figtable, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/weather/Temp DoD table.html', auto_open=False)
        figtable.write_image('U:/Trading - Gas/LNG/LNG website/analysis/weather/Temp DoD table.png')
        
    
    def send_email(sender, receivers, cc, subject, body):
        # Import smtplib for the actual sending function
        # Import smtplib for the actual sending function
        import smtplib
        # Import the email modules we'll need
        from email.message import EmailMessage
        from email.mime.image import MIMEImage
        from email.mime.multipart import MIMEMultipart
        # Create a text/plain message
       # msg = EmailMessage()
        msg = MIMEMultipart("related")
        
        #msg.set_content(body, subtype='html')
        # me == the sender's email address
        # you == the recipient's email address
        with open(body, "rb") as fp:
            img = MIMEImage(fp.read())
        msg.attach(img)
       
        msg['Subject'] = subject #'SMTP e-mail test 2'
        msg['From'] = sender
        msg['To'] = receivers  #'svaiyani@freepoint.com'
        msg['Cc'] = cc
        # Send the message via our own SMTP server.
        s = smtplib.SMTP('smtp.freepoint.local')
        s.send_message(msg)
        s.quit()
    
    def email_alart(plant, dfwind, dfwindspeed, dfwave, dfwaveheight, dfcargonum):
        
        today=datetime.date.today()
        #print(cargo)
        dfwind_wave=pd.DataFrame(index=pd.date_range(start=today-datetime.timedelta(days=1)*5, end=today+datetime.timedelta(days=1)*10).date)
        dfwind_wave=pd.concat([dfwind_wave, dfwind[[plant]], dfwindspeed[[plant]], dfwave[[plant]], dfwaveheight[[plant]]], axis=1)
        dfwind_wave=dfwind_wave.round(1)
        dfwind_wave.sort_index(inplace=True)
        dfwind_wave.fillna(0, inplace=True)
        dfwind_wave = dfwind_wave.loc[today-datetime.timedelta(days=1)*5:today+datetime.timedelta(days=1)*10]
        dfwind_wave.columns=['Wind Dir','Wind Speed (Knots)','Wave Dir','Wave Height (m)']
        dfwind_wave=dfwind_wave.T
        
        column=[]
        for i in dfwind_wave.columns:
            column.append(str(i.day)+'/'+i.strftime("%b"))
        
        dfwind_wave.columns=column
        
        
        dfcolor = dfwind_wave.copy()
        #print(dfcolor.loc['Wind Speed',i])
        
        send_list = []
        for i in dfcolor.columns[5:]:
            if dfwind_wave.loc['Wind Speed (Knots)',i] >= 30:
                send_list.append('True')
             
            if dfwind_wave.loc['Wave Height (m)',i] >= 3:
                dfcolor.loc['Wave Height (m)',i] = 'pink'
                send_list.append('True')
        
        if len(send_list) >0:
            print(plant+' Wind and Wave alart')
            #print()
        
            Weather.send_email('ywang1@freepoint.com', 'ywang1@freepoint.com, jcooper@freepoint.com', '', plant+' Wind and Wave alart',  'U:/Trading - Gas/LNG/LNG website/analysis/weather/'+plant+' wind wave.png')
    
   
    def update_minmax(dfnormal, dfminmaxobs, dfminmax):
        
        #print(dfnormal)
        #print(dfminmaxobs.loc['2020-07-01':,['India min', 'India max']])
        
        
        for i in dfnormal.columns:
            
            try:
                dfminmaxobs[i+' mean'] = (dfminmaxobs[i+' min'] + dfminmaxobs[i+' max'])/2
                for j in dfminmaxobs.index:
                    dfminmaxobs.loc[j, i+' min'] = dfminmaxobs.loc[j, i+' min'] - (dfminmaxobs.loc[j, i+' mean'] - dfnormal.loc[j,i])
                    dfminmaxobs.loc[j, i+' max'] = dfminmaxobs.loc[j, i+' max'] - (dfminmaxobs.loc[j, i+' mean'] - dfnormal.loc[j,i])
            except KeyError:
                dfminmax[i+' mean'] = (dfminmax[i+' min'] + dfminmax[i+' man'])/2
                for j in dfminmax.index:
                    dfminmax.loc[j, i+' min'] = dfminmax.loc[j, i+' min'] - (dfminmax.loc[j, i+' mean'] - dfnormal.loc[j,i])
                    dfminmax.loc[j, i+' man'] = dfminmax.loc[j, i+' man'] - (dfminmax.loc[j, i+' mean'] - dfnormal.loc[j,i])
        
        #print(dfminmaxobs.loc['2020-07-01':,['India min', 'India max']])
        
        return dfminmaxobs, dfminmax
        
    def update():
        
        a=Weather
        
        dfcurve_id,dfcurve_id_market=a.get_curve_id()
        #print(1, datetime.datetime.now())
        
        
        #dfwind_wave = a.wind_wave()
        
        SupplyCategories=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'CategoriesSupply').sql_to_df()
        drop_list_supply = ['Senkang', 'Greater Tortue FLNG']
        for i in drop_list_supply:
            SupplyCategories.drop(SupplyCategories[SupplyCategories['Plant'] == i].index, inplace=True)
        
        DemandCategories=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'CategoriesDemand').sql_to_df()

        dfcargonum = a.num_cargos(dfcurve_id, SupplyCategories, DemandCategories)
        #print(dfcargonum[['Coral South LNG']])
        
        dfwind,dfwindspeed,dfwave,dfwaveheight = a.get_wind_wave()
        #dfwind,dfwindspeed,dfwave,dfwaveheight,dftemp,dftemplast=a.get_weather_data(dfcurve_id,dfcurve_id_market)
        
        a.wind_wave_table_sum(dfwind, dfwindspeed, dfwave, dfwaveheight,SupplyCategories, DemandCategories,dfcurve_id,dfcargonum)
        
        #print(dfcurve_id['installation'])
        for i in dfcurve_id['installation']:
            #print(i)
            try:
                #dfwind_wave=a.wind_wave(i, dfwind, dfwindspeed, dfwave, dfwaveheight)
                #print(i,dfwind_wave)
                a.wind_wave_table(i, dfwind, dfwindspeed, dfwave, dfwaveheight, dfcargonum)
            except KeyError as e:
                print(e, ' not available windwave table')
                #raise Exception from None
        
        
        for plant in ['South Hook','Dragon']:
            a.email_alart(plant, dfwind, dfwindspeed, dfwave, dfwaveheight, dfcargonum)
        
        country_list = a.get_country()
        
        country_list.remove('Russian Federation')
        country_list.remove('Vietnam')
        country_list.remove('Philippines')
        country_list.remove('Iceland')
        #print(country_list)
        #country_list=['China']#test
        
        dfenslatest, dfec46latest, dfsflatest = a.get_temp_latest(country_list)
        #print(dfenslatest)
        dfenslast, dfec46last, dfsflast = a.get_temp_last(country_list)
        
        dfjwaens, dfjwaec46, dfjwanorpivot = a.get_JWA_temp_latest(country_list)
        dfjwaenslast, dfjwaec46last, dfjwanorpivotlast = a.get_jwa_last(country_list)
        
        dftemphist = DBTOPD.get_temp_hist_data()
        dftemphistgap = a.get_temp_hist_gap()
        today=datetime.date.today()
        
        dfnormal=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'TempNormal').sql_to_df()
        dfnormal.set_index('index', inplace=True)
        
        #['Egypt', 'Malaysia', 'Jamaica', 'Indonesia','Uzbekistan', 'Lithuania', 'Iceland', 'Kuwait', 'Estonia', 'Mexico',  'Myanmar', 'Sweden', 'Argentina', 'Peru','Canada',  'Latvia', 'Republic of Macedonia', 'Thailand', 'Turkmenistan', 'Bangladesh', 'Pakistan', 'Colombia', 'Iran', 'Kazakhstan' ,'Paraguay']
        dftemphistminmaxob = DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'tempmixob').sql_to_df()
        dftemphistminmaxob.set_index('index', inplace=True)

        dftemphistminmax = DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'TempHistMinMax').sql_to_df()
        dftemphistminmax.set_index('index', inplace=True)
        
        dftemphistminmaxob,dftemphistminmax = a.update_minmax(dfnormal, dftemphistminmaxob, dftemphistminmax)
        
        #jwa and mete weight
        dfensweight, dfec46weight, dfenslast_weight, dfec46last_weight, dfnormal_weight = a.weight_temp(dfenslatest,dfec46latest,dfenslast, dfec46last,dfnormal, dfjwaens, dfjwaec46,dfjwaenslast, dfjwaec46last,dfjwanorpivot )
        
        
        a.temp_hist_chart(country_list, dftemphist, dfsflatest, dfnormal)
        
        
        a.temp_chart(country_list, dfensweight, dfec46weight, dfsflatest, dfenslast_weight, dfec46last_weight, dfsflast,dfnormal_weight,dftemphistminmax,dftemphistminmaxob, dftemphist, dftemphistgap)
        
        a.temp_dod_table(country_list, dfensweight, dfec46weight,dfnormal_weight,dfenslast_weight, dfec46last_weight)
        
        dffcst = pd.DataFrame(index = dfsflatest.index)
        dffcst_last = pd.DataFrame(index = dfsflast.index)
        
        for i in country_list:
            
            dffcst['ecmwf-ens '+i] = dfensweight[i]
            dffcst['ecmwf-vareps '+i] = dfec46weight[i]
            dffcst['ecmwf-mmsf '+i] = dfsflatest[i]
            
            dffcst_last['ecmwf-ens '+i] = dfenslast_weight[i]
            dffcst_last['ecmwf-vareps '+i] = dfec46last_weight[i]
            dffcst_last['ecmwf-mmsf '+i] = dfsflast[i]
            
            dftemphist[i + ' normal'] = dfnormal_weight[i]
        #print(dffcst)
        #print(dffcst_last)
        
        db_server_lng = "PRD-DB-SQL-211"
        db_name_lng = "LNG"
        sql_engine_lng = sqlalchemy.create_engine("mssql+pyodbc://" + db_server_lng + "/" + db_name_lng + "?trusted_connection=yes&driver=ODBC+Driver+13+for+SQL+Server")
        
        dftemphist.to_sql(name='TempHistNormal', con=sql_engine_lng, index=True, if_exists='replace', schema='ana')
        dffcst.to_sql(name='TempFcst', con=sql_engine_lng, index=True, if_exists='replace', schema='ana')
        dffcst_last.to_sql(name='TempFcstLast', con=sql_engine_lng, index=True, if_exists='replace', schema='ana')
        
#Weather.update()