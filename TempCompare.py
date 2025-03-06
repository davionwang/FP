# -*- coding: utf-8 -*-
"""
Created on Mon Feb 13 15:27:18 2023

@author: SVC-GASQuant2-Prod
"""
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


class model_compare():
    
    
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
        
        
     #get Meteomatics   
    def get_temp_latest(country_list):
        
        #print(country_list)
        #country='Japan'
        today = datetime.date.today()
        sqlConnScrape = pyodbc.connect('DRIVER={SQL SERVER};SERVER=PRD-DB-SQL-211;Trusted_Connection=yes')
        
        #dfens = pd.DataFrame(columns=country_list)
        dfec46 = pd.DataFrame(columns=country_list)
        dfsff = pd.DataFrame(columns=country_list)
        
        if datetime.datetime.now().hour >8 or 20 > datetime.datetime.now().hour:
            
            sqlens = '''
                        SELECT * FROM Meteorology.dbo.WeatherStationTimeSeriesHistory
                        WHERE ParameterName = 't_2m:C' AND ModelSourceName = 'ecmwf-ens' and WeightingName = 'temperature'
                        AND ForecastDate = {time}
            
                          '''.format(time='\''+str(datetime.date.today())+' 00:00:00'+'\'')
            
            dfens = pd.read_sql(sqlens, sqlConnScrape)
            #print(dfens)
            dfens=dfens[['ValueDate','CountryName','Weighting','Value']]
            dfens['weighted'] = dfens['Weighting']*dfens['Value']
            dfens['ValueDate'] = pd.to_datetime(dfens['ValueDate'])
            #print(dfens)
            dfpivot = dfens.pivot_table(values='weighted',index='ValueDate',columns='CountryName',aggfunc=np.sum)
            dfpivot = dfpivot.resample('D').mean()
            dfpivot = dfpivot.loc[:today+relativedelta(days=14)]
            #dfpivot['sum'] = dfpivot.sum(axis=1)
            #print(dfpivot)
        else:
        #if 8>datetime.datetime.now().hour or datetime.datetime.now().hour>20:
            sqlens = '''
                        SELECT * FROM Meteorology.dbo.WeatherStationTimeSeriesHistory
                        WHERE ParameterName = 't_2m:C' AND ModelSourceName = 'ecmwf-ens' and WeightingName = 'temperature'
                        AND ForecastDate = {time}
            
                          '''.format(time='\''+str(datetime.date.today())+' 12:00:00'+'\'')
            
            dfens = pd.read_sql(sqlens, sqlConnScrape)
            #print(df)
            dfens=dfens[['ValueDate','CountryName','Weighting','Value']]
            dfens['weighted'] = dfens['Weighting']*dfens['Value']
            dfens['ValueDate'] = pd.to_datetime(dfens['ValueDate'])
            #print(dfens)
            dfpivot = dfens.pivot_table(values='weighted',index='ValueDate',columns='CountryName',aggfunc=np.sum)
            dfpivot = dfpivot.resample('D').mean()
            dfpivot = dfpivot.loc[:today+relativedelta(days=14)]
            
            
        for i in country_list:
            try:
            
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
                #df.sort_index(inplace=True)
                #print(df)
                dfpivotec = dfpivotec.resample('D').mean()
                #print(dfpivot)
                dfec46[i] = dfpivotec['sum']
            
            
            except Exception:
                print(i, 'not in temp latest')
                pass
        
       
        #print(dfpivot)
        dfec46 = dfec46.loc[:today+relativedelta(days=45)]
        
        return dfpivot, dfec46
    
    
    global headers
    #Authorization is key provided by Jeremy from Metdesk
    headers = {'Content-Type': 'application/json',
               'Authorization': 'jwt eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ0b2tlbklkIjoiRnJlZXBvaW50QVZGbHA4Q1hvRmZMUlNTS3hGd3g1N2oiLCJleHAiOjE2ODg3NzQzOTl9.KWv1X_9mYUCQCX5YAv36gc_IE1ZHjcCJAvmL7hHWB2E'}
    
    def MetDesk_API_get_issues(model):
        
        url = 'https://api.metdesk.com/get/metdesk/cwg/v1/issues'
        params = {'model': model,
                  }
        r = requests.get(url, headers=headers, params=params)
        
        return r.json()['data']
    
    def MetDesk_API_get_latest(model, issue, element, location, mean=1, median=0, percentiles=0, members=0):
        
        url = 'https://api.metdesk.com/get/metdesk/cwg/v1/latest'
        
        params = {'model': model,
                  'issue': issue,
                  'element': element,
                  'location': location,
                  'mean': mean,
                  'median': median,
                  'percentiles': percentiles,
                  'members': members
                  }
        
        r = requests.get(url, headers=headers, params=params)
        
        df = pd.DataFrame(r.json()["data"]).set_index("dtg")
        #print(df)
        df.index = pd.to_datetime(df.index)
        
        return df
    
    def MetDesk_API_get_dtgs(model, issue):
        
        url = 'https://api.metdesk.com/get/metdesk/cwg/v1/dtgs'
        
        params = {'model': model,
                  'issue': issue,
                  }
        
        r = requests.get(url, headers=headers, params=params)
        
        return r.json()["data"]
    
    def MetDesk_API_get_climate(start_dtg, end_dtg, element, location):
        
        url = 'https://api.metdesk.com/get/metdesk/cwg/v1/climate'
        
        params = {'start_dtg': start_dtg,
                  'end_dtg': end_dtg,
                  'element': element,
                  'location': location,
                  }
        
        r = requests.get(url, headers=headers, params=params)
        
        df = pd.DataFrame(r.json()["data"]).set_index("dtg")
        df.index = pd.to_datetime(df.index)
        
        return df
    
    def MetDesk_API_get_forecasts(model, issue, start_dtg, end_dtg, element, location, mean=1, median=0, percentiles=0, members=0):
        
        url = 'https://api.metdesk.com/get/metdesk/cwg/v1/forecasts'
        
        params = {'model': model,
                  'issue': issue,
                  'start_dtg': start_dtg,
                  'end_dtg': end_dtg,
                  'element': element,
                  'location': location,
                  'mean': mean,
                  'median': median,
                  'percentiles': percentiles,
                  'members': members
                  }
        
        r = requests.get(url, headers=headers, params=params)
        
        df = pd.DataFrame(r.json()["data"]).set_index("dtg")
        df.index = pd.to_datetime(df.index)
        
        return df
    
    def MetDesk_API_get_locations():
        
        url = 'https://api.metdesk.com/get/metdesk/cwg/v1/locations'
        
        r = requests.get(url, headers=headers)
        print('11',r.json())
        df = pd.DataFrame(r.json()["data"])
        
        return df
    
    #print(MetDesk_API_get_locations())
    
    def metdesk_location_name(country_list,MetDesk_locations):
        #print(MetDesk_locations['name'].tolist())
        MetDesk_locations.set_index('name', inplace=True)
        #print(MetDesk_locations)
        #print(country_list)
        df = pd.DataFrame(index = country_list, columns = ['location'])
        for i in df.index:
            try:
                df.loc[i] = MetDesk_locations.loc[i]
            except KeyError as e:
                print(e, ' is not in metdesk_location_name')
        
        df.loc['Russian Federation'] = MetDesk_locations.loc['Russia']
        df.loc['Belgium'] = MetDesk_locations.loc['Belgium and Luxembourg']
        df.loc['Serbia and Montenegro'] = MetDesk_locations.loc['Serbia']
        df.dropna(axis=0, inplace=True)
        
        #print(df)
        return df
        
        
    #dflocation = MetDesk_API_get_locations()
    #print(dflocation.loc[dflocation[dflocation['name'] == 'Taiwan'].index, 'location'])
    
    def get_metdesk_data(metdesk_name):
        
        #location_list = ['CN']
        today = datetime.date.today()
        location_list = metdesk_name['location'].tolist()
        
        dftemp_latest = pd.DataFrame()
        dftemp_normal = pd.DataFrame()
        #print(location_list)
        #print(dftemp_normal)
        
        for i in location_list:
            try:
                latest_ec46_dt = model_compare.MetDesk_API_get_issues('ec46')[-1]
                ec46_fcstdt = model_compare.MetDesk_API_get_dtgs('ec46', latest_ec46_dt)
                dfec46 = model_compare.MetDesk_API_get_forecasts('ec46', latest_ec46_dt, ec46_fcstdt[0], ec46_fcstdt[-1], 'tt', i, mean=1, median=0, percentiles=0, members=0)
                dfec46_pivot = dfec46.pivot_table('value', index=dfec46.index, columns='member')
                dfec46_pivot= dfec46_pivot.resample('D').mean()
                #print(dfec46_pivot)
                
                dftemp_latest[i+' ec46'] = dfec46_pivot['mean']
                #print()
                #normal
                dfnormal = model_compare.MetDesk_API_get_climate(ec46_fcstdt[0], ec46_fcstdt[-1], 'tt', i)
                dftemp_normal[i+' normal'] = dfnormal['value']
            except KeyError as e:
                print(i, ' is not in metdesk_data ec46')
        
        for i in location_list:
            #print(i)
            try:
                #lastet
                latest_ens_dt = model_compare.MetDesk_API_get_issues('eceps')[-1]
                #latest_ec46_dt = model_compare.MetDesk_API_get_issues('ec46')[-1]
                #print(latest_ens_dt)
                ens_fcstdt = model_compare.MetDesk_API_get_dtgs('eceps', latest_ens_dt)
                #ec46_fcstdt = model_compare.MetDesk_API_get_dtgs('ec46', latest_ec46_dt)
                dfens = model_compare.MetDesk_API_get_forecasts('eceps', latest_ens_dt, ens_fcstdt[0], ens_fcstdt[-1], 'tt', i, mean=1, median=0, percentiles=0, members=0)
                dfens_pivot = dfens.pivot_table('value', index=dfens.index, columns='member')
                #print(dfens_pivot)
                dftemp_latest[i+' ens'] = dfens_pivot['mean']
                
                #dfec46 = model_compare.MetDesk_API_get_forecasts('ec46', latest_ec46_dt, ec46_fcstdt[0], ec46_fcstdt[-1], 'tt', i, mean=1, median=0, percentiles=0, members=0)
                #dfec46_pivot = dfec46.pivot_table('value', index=dfec46.index, columns='member')
                #dfec46_pivot= dfec46_pivot.resample('D').mean()
                #print(dfec46_pivot)
                
                #dftemp_latest[i+' ec46'] = dfec46_pivot['mean']
                #print()
                #normal
                #dfnormal = model_compare.MetDesk_API_get_climate(ec46_fcstdt[0], ec46_fcstdt[-1], 'tt', i)
                #dftemp_normal[i+' normal'] = dfnormal['value']
            except KeyError as e:
                print(i, ' is not in metdesk_data ens')
            
            
        
        
        
        
        issue_date = {
            'ens': latest_ens_dt,
            'ec46': latest_ec46_dt
            }
        
        #print(dftemp_latest)    
        dftemp_latest = dftemp_latest.resample('D').mean()  
        dftemp_latest.index = dftemp_latest.index.date
        #print(dftemp_latest) 
        #print(dftemp_normal)
        dftemp_normal = dftemp_normal.resample('D').sum().astype('float') 
        dftemp_normal.index = dftemp_normal.index.date
        
       
        
     
        return dftemp_latest, dftemp_normal
    
    #get JWA
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
        """    
        for i in country_list:
            try:
            
            #ec
                sqlec = '''
                        SET NOCOUNT ON; 
                          EXEC Meteorology.dbo.GetModelRunWeatherStations 1, 'jwa-15-w', 't_2m:C', NULL, {country}
            
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
            
            
            except Exception:
                print(i, 'not in JWA temp latest')
                pass
        """    
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
                    SET NOCOUNT ON;                
                    EXEC Meteorology.dbo.GetModelRunWeatherStations 1, 'jwa-15-w', 'norm_t_2m:C', NULL, Null, 'temperature'
                '''
            
        dfnor = pd.read_sql(sqlnor, sqlConnScrape)
        #print(df)
        dfnor=dfnor[['ValueDate','CountryName','Weighting','Value']]
        dfnor['weighted'] = dfnor['Weighting']*dfnor['Value']
        dfnor['ValueDate'] = pd.to_datetime(dfnor['ValueDate'])
        #print(i, df)
        dfnorpivot = dfnor.pivot_table(values='weighted',index='ValueDate',columns='CountryName',aggfunc=np.sum)
        #print(dfpivot)
        
        dfnorpivot = dfnorpivot.resample('D').mean()
        #dfnorpivot = dfnorpivot.loc[:today+relativedelta(days=14)]
        dfnormal = dfnorpivot
        
        dfnormal.interpolate(inplace=True)
        #print(dfnormal.loc[today:,'China'])
        #print(dfec46.loc[today:,'China'])
        #print(dfpivot.loc[today:,'China'])
        
        return dfpivot, dfec46, dfnormal
    
    def chart(metdesk_name, dfmeteens, dfmeteec46, dfmetdesk,demetdesknormal, dfjwaens,dfjwaeec46,dfjwanorpivot):
        
        today = datetime.date.today()
        metdesk_name.drop('Iceland', inplace=True)
        metdesk_name.drop('Russian Federation', inplace=True)
        #print(metdesk_name)
        
        dfmetenormal =  DBTOPD('PRD-DB-SQL-211','LNG','ana','TempNormal').sql_to_df()
        dfmetenormal.set_index('index', inplace=True)
        #print(dfmetdesk[])
        #index = pd.date_range(start=today, end=dfmeteec46.index[-1])
        index = pd.date_range(start=today+relativedelta(days=(1)), end=today+relativedelta(days=(41)))
        
        for country in metdesk_name.index:
            
            #print(dfmetenormal.loc[today:,country])
            #print(demetdesknormal)
            try:
                
                #chart
                figchart = make_subplots(specs=[[{"secondary_y": False}]])
                
                figchart.add_trace(go.Scatter(x=index, y=dfmeteens.loc[today:,country],
                                    mode='lines',
                                    name='Mete Ens',
                                    line=dict(color='red', dash='solid')),secondary_y=False)
                figchart.add_trace(go.Scatter(x=index, y=dfmeteec46.loc[index,country],
                                    mode='lines',
                                    name='Mete EC46',
                                    line=dict(color='red', dash='dot')),secondary_y=False)
                figchart.add_trace(go.Scatter(x=index, y=dfmetenormal.loc[today:,country],
                                    mode='lines',
                                    name='Mete normal',
                                    line=dict(color='red', dash='dash')),secondary_y=False)
                figchart.add_trace(go.Scatter(x=index, y=dfmetdesk.loc[index,metdesk_name.loc[country,'location']+' ens'],
                                    mode='lines',
                                    name='Metdesk ens',
                                    line=dict(color='orange', dash='solid')),secondary_y=False)
 
                figchart.add_trace(go.Scatter(x=index, y=dfmetdesk.loc[index,metdesk_name.loc[country,'location']+' ec46'],
                                    mode='lines',
                                    name='Metdesk ec46',
                                    line=dict(color='orange', dash='dot')),secondary_y=False)
                figchart.add_trace(go.Scatter(x=index, y=demetdesknormal.loc[today:,metdesk_name.loc[country,'location']+' normal'],
                                    mode='lines',
                                    name='Metdesk normal',
                                    line=dict(color='orange', dash='dash')),secondary_y=False)
                figchart.add_trace(go.Scatter(x=index, y=dfjwaens.loc[today:,country],
                                    mode='lines',
                                    name='JWA 14d',
                                    line=dict(color='green', dash='solid')),secondary_y=False)
                
                figchart.add_trace(go.Scatter(x=index, y=dfjwaeec46.loc[today:,country],
                                    mode='lines',
                                    name='JWA 15w',
                                    line=dict(color='green', dash='dot')),secondary_y=False)
                figchart.add_trace(go.Scatter(x=dfjwanorpivot.loc[today+relativedelta(days=(1)):,country].index, y=dfjwanorpivot.loc[today+relativedelta(days=(1)):,country],
                                    mode='lines',
                                    name='JWA normal',
                                    line=dict(color='green', dash='dash')),secondary_y=False)
                
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
                
                figchart.update_layout(xaxis_range=[today+relativedelta(days=(1)), today+relativedelta(days=(41))])
                
                py.plot(figchart, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/tempmodels/'+country+' chart.html', auto_open=False)
                #figchart.write_image('U:/Trading - Gas/LNG/LNG website/analysis/tempmodels/'+country+' chart.png')
            except KeyError as e:
                print(e)
                #chart
                figchart = make_subplots(specs=[[{"secondary_y": False}]])
                
                figchart.add_trace(go.Scatter(x=index, y=dfmeteens.loc[today:,country],
                                    mode='lines',
                                    name='Mete Ens',
                                    line=dict(color='red', dash='solid')),secondary_y=False)
                figchart.add_trace(go.Scatter(x=index, y=dfmeteec46.loc[index,country],
                                    mode='lines',
                                    name='Mete EC46',
                                    line=dict(color='red', dash='dot')),secondary_y=False)
                figchart.add_trace(go.Scatter(x=index, y=dfmetenormal.loc[today:,country],
                                    mode='lines',
                                    name='Mete normal',
                                    line=dict(color='red', dash='dash')),secondary_y=False)
                figchart.add_trace(go.Scatter(x=index, y=dfmetdesk.loc[index,metdesk_name.loc[country,'location']+' ens'],
                                    mode='lines',
                                    name='Metdesk ens',
                                    line=dict(color='orange', dash='solid')),secondary_y=False)
                '''
                figchart.add_trace(go.Scatter(x=index, y=dfmetdesk.loc[index,metdesk_name.loc[country,'location']+' ec46'],
                                    mode='lines',
                                    name='Metdesk ec46',
                                    line=dict(color='orange', dash='dot')),secondary_y=False)
                
                figchart.add_trace(go.Scatter(x=index, y=demetdesknormal.loc[today:,metdesk_name.loc[country,'location']+' normal'],
                                    mode='lines',
                                    name='Metdesk normal',
                                    line=dict(color='orange', dash='dash')),secondary_y=False)
                '''
                figchart.add_trace(go.Scatter(x=index, y=dfjwaens.loc[today:,country],
                                    mode='lines',
                                    name='JWA 14d',
                                    line=dict(color='green', dash='solid')),secondary_y=False)
                
                figchart.add_trace(go.Scatter(x=index, y=dfjwaeec46.loc[today:,country],
                                    mode='lines',
                                    name='JWA 15w',
                                    line=dict(color='green', dash='dot')),secondary_y=False)
                
                figchart.add_trace(go.Scatter(x=dfjwanorpivot.loc[today+relativedelta(days=(1)):,country].index, y=dfjwanorpivot.loc[today+relativedelta(days=(1)):,country],
                                    mode='lines',
                                    name='JWA normal',
                                    line=dict(color='green', dash='dash')),secondary_y=False)
                
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
                
                figchart.update_layout(xaxis_range=[today+relativedelta(days=(1)), today+relativedelta(days=(41))])
                
                py.plot(figchart, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/tempmodels/'+country+' chart.html', auto_open=False)
        

    def update():
        country_list =    model_compare.get_country()
        dfmeteens, dfmeteec46 = model_compare.get_temp_latest(country_list)
        
        MetDesk_locations = model_compare.MetDesk_API_get_locations() 
        metdesk_name = model_compare.metdesk_location_name(country_list, MetDesk_locations)
        
        dfmetdesk, demetdesknormal = model_compare.get_metdesk_data(metdesk_name)
        #print(dfmetdesk['BR ens'])
        #print(demetdesknormal['BR normal'])
        
        dfjwaens, dfjwaeec46, dfjwanorpivot = model_compare.get_JWA_temp_latest(country_list)
        model_compare.chart(metdesk_name, dfmeteens, dfmeteec46, dfmetdesk,demetdesknormal, dfjwaens, dfjwaeec46,dfjwanorpivot)    
    
#model_compare.update()
    
    
    
    
    
    
    