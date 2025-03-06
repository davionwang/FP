# -*- coding: utf-8 -*-
"""
Created on Mon Aug 22 11:39:23 2022

@author: SVC-GASQuant2-Prod
"""

# -*- coding: utf-8 -*-
"""

Functions that get CWG data via Metdesk API

model - ecop, eceps, ec46, gfsop, gfsens, gem, cmcens, cfs, cfsd, cfsw, magma, ukgl, ukgr
issue - A valid model issue, represented as an ISO 8601 format date/time eg. 2022-08-03T00:00:00Z
element - Available values : tt, rad, rrr6, ff100
location - see MetDesk_API_get_locations() eg. "JP" for Japan
start_dtg - in the format eg. 2022-08-03T00:00:00Z
end_dtg - in the format eg. 2022-08-18T00:00:00Z

"""

#v1 change top table to fcst vs normal

import requests
import pandas as pd
import numpy as np
from plotly.subplots import make_subplots
import plotly.graph_objs as go
import plotly.offline as py
import calendar

import datetime
from dateutil.relativedelta import relativedelta
import sys
sys.path.append('C:\\Users\SVC-GASQuant2-Prod\YWwebsite\Python Scripts\class') 
from DBtoDF import DBTOPD
import pyodbc




class JKTC_Temp():
    
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
        
        df = pd.DataFrame(r.json()["data"])
        
        return df
    
    #print(MetDesk_API_get_locations())
    
    #dflocation = MetDesk_API_get_locations()
    #print(dflocation.loc[dflocation[dflocation['name'] == 'Taiwan'].index, 'location'])
    
    def get_data():
        
        location_list = ['CN', 'JP', 'KR','TW']
        
        dftemp_latest = pd.DataFrame()
        dftemp_last = pd.DataFrame()
        #print( MetDesk_API_get_issues('ec46'))
        dftemp_normal = pd.DataFrame()
        
        #print(dftemp_normal)
        for i in location_list:
            
            #lastet
            #print(JKTC_Temp.MetDesk_API_get_issues('eceps'))
            latest_ens_dt = JKTC_Temp.MetDesk_API_get_issues('eceps')[-1]
            latest_ec46_dt = JKTC_Temp.MetDesk_API_get_issues('ec46')[-1]
            
            ens_fcstdt = JKTC_Temp.MetDesk_API_get_dtgs('eceps', latest_ens_dt)
            ec46_fcstdt = JKTC_Temp.MetDesk_API_get_dtgs('ec46', latest_ec46_dt)
            dfens = JKTC_Temp.MetDesk_API_get_forecasts('eceps', latest_ens_dt, ens_fcstdt[0], ens_fcstdt[-1], 'tt', i, mean=1, median=0, percentiles=0, members=0)
            dfens_pivot = dfens.pivot_table('value', index=dfens.index, columns='member')
            
            dfec46 = JKTC_Temp.MetDesk_API_get_forecasts('ec46', latest_ec46_dt, ec46_fcstdt[0], ec46_fcstdt[-1], 'tt', i, mean=1, median=0, percentiles=0, members=0)
            dfec46_pivot = dfec46.pivot_table('value', index=dfec46.index, columns='member')
            
            
            dftemp_latest[i+' ec46'] = dfec46_pivot['mean']
            dftemp_latest[i+' ens'] = dfens_pivot['mean']
            
            #normal
            #print(ec46_fcstdt[-1][0:9])
            dfnormal = JKTC_Temp.MetDesk_API_get_climate(ec46_fcstdt[0], ec46_fcstdt[-1], 'tt', i)
            dftemp_normal[i+' normal'] = dfnormal['value']
            
            #print(dftemp_latest)
            #last
            last_ens_dt = JKTC_Temp.MetDesk_API_get_issues('eceps')[-2]
            last_ec46_dt = JKTC_Temp.MetDesk_API_get_issues('ec46')[-2]
            
            ens_fcstdt = JKTC_Temp.MetDesk_API_get_dtgs('eceps', last_ens_dt)
            ec46_fcstdt = JKTC_Temp.MetDesk_API_get_dtgs('ec46', last_ec46_dt)
            dfens = JKTC_Temp.MetDesk_API_get_forecasts('eceps', last_ens_dt, ens_fcstdt[0], ens_fcstdt[-1], 'tt', i, mean=1, median=0, percentiles=0, members=0)
            dfens_pivot = dfens.pivot_table('value', index=dfens.index, columns='member')
            
            dfec46 = JKTC_Temp.MetDesk_API_get_forecasts('ec46', last_ec46_dt, ec46_fcstdt[0], ec46_fcstdt[-1], 'tt', i, mean=1, median=0, percentiles=0, members=0)
            dfec46_pivot = dfec46.pivot_table('value', index=dfec46.index, columns='member')
            
            
            #print(ec46_fcstdt[0], ec46_fcstdt[-1])
            #dfnormal = MetDesk_API_get_climate(ec46_fcstdt[0], ec46_fcstdt[-1], 'tt', i)
            dftemp_last[i+' ec46'] = dfec46_pivot['mean']
            dftemp_last[i+' ens'] = dfens_pivot['mean']
            
            #dftemp_last[i+' normal'] = dfnormal['value']
        
        issue_date = {
            'ens': latest_ens_dt,
            'ec46': latest_ec46_dt
            }
        
        #print(dftemp_latest)    
        dftemp_latest = dftemp_latest.resample('D').mean()  
        dftemp_latest.index = dftemp_latest.index.date
        dftemp_last = dftemp_last.resample('D').mean() 
        dftemp_last.index = dftemp_last.index.date
        
        dftemp_normal = dftemp_normal.resample('D').sum().astype('float') 
        dftemp_normal.index = dftemp_normal.index.date
        
        dftemp_normal_hist = DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'JKTCMetdeskNormal').sql_to_df()
        dftemp_normal_hist.set_index('index', inplace=True)
        #print(dftemp_normal)
        #print(dftemp_last)
        
        return dftemp_latest, dftemp_last, dftemp_normal, dftemp_normal_hist,issue_date
    
    
    def get_temp_hist_gap():
        
         
        sqlConnScrape = pyodbc.connect('DRIVER={SQL SERVER};SERVER=PRD-DB-SQL-211;Trusted_Connection=yes')
        sqlens = '''
                        SELECT * FROM Meteorology.dbo.WeatherStationTimeSeriesHistory
                        WHERE ParameterName = 't_2m:C' AND ModelSourceName = 'ecmwf-ens' and WeightingName = 'temperature'
                        AND ForecastDate = {time}
            
                          '''.format(time='\''+str(datetime.date.today()-relativedelta(days=9))+' 00:00:00'+'\'')
            
        dfens = pd.read_sql(sqlens, sqlConnScrape)
        #print(df)
        dfens=dfens[['ValueDate','CountryName','Weighting','Value']]
        dfens['weighted'] = dfens['Weighting']*dfens['Value']
        dfens['ValueDate'] = pd.to_datetime(dfens['ValueDate'])
        #print(i, df)
        dfpivot = dfens.pivot_table(values='weighted',index='ValueDate',columns='CountryName',aggfunc=np.sum)
        dfpivot = dfpivot.resample('D').mean()
        #print(dfpivot)
        return dfpivot
    
    def temp_chart(country, dftemp_latest, dftemp_last, dftemp_normal, dftemp_normal_hist, dftemphist, dftemprange, dftemphistgap): #add sf line from 5th Mar
        
        today = datetime.date.today()
        index = pd.date_range(start=today+relativedelta(days=(1)), end=today+relativedelta(days=(41)))
        
        
        name_dict = {'CN':'China',
                     'JP':'Japan',
                     'KR':'South Korea',
                     'TW':'Taiwan'
            }
        
       
        dfrangefull = dftemprange[[name_dict[country]+' min', name_dict[country]+' man']].copy()
        
        dfrange = pd.DataFrame(index=index, columns=['min','max'])
        for i in dfrange.index: 
            dfrange['min'].loc[i] = dfrangefull[name_dict[country]+' min'].loc['2020-'+str(i.month)+'-'+str(i.day)]
            dfrange['max'].loc[i] = dfrangefull[name_dict[country]+' man'].loc['2020-'+str(i.month)+'-'+str(i.day)] 
        
        #C1 delta bar
        dfdelta = dftemp_latest.copy()# - dftempcountrylast
        dfdelta['delta'] = dfdelta.loc[today+relativedelta(days=(1)):today+relativedelta(days = 14),country+' ens'].round(2) - dftemp_normal.loc[today+relativedelta(days=(1)):today+relativedelta(days = 14),country+' normal']
        dfdelta.loc[today+relativedelta(days = 14):,'delta'] = dfdelta.loc[today+relativedelta(days = 14):,country+' ec46'].round(2) - dftemp_normal.loc[today+relativedelta(days = 14):,country+' normal']
        #C1 df bar colour, 
        dfdelta["Color"] = np.where(dfdelta["delta"]<0, 'blue', 'red')
        
        
        #chart
        #for i in countrylist:
        figchart = make_subplots(specs=[[{"secondary_y": True}]])
        figchart.add_trace(go.Bar(name='Delta (Fcst - Normal)', x=index,y=dfdelta.loc[index,'delta'],
                              marker_color=dfdelta.loc[index, 'Color'])
                           ,secondary_y=True)
        figchart.add_trace(go.Scatter(x=index, y=dftemp_latest.loc[index,country+' ens'],
                            mode='lines',
                            name='Ensemble '+str(today),
                            line=dict(color='blue', dash='solid')),secondary_y=False)
        figchart.add_trace(go.Scatter(x=index, y=dftemp_latest.loc[index,country+' ec46'],
                            mode='lines',
                            name='EC46',
                            line=dict(color='lightskyblue', dash='solid')),secondary_y=False)
        '''
        figchart.add_trace(go.Scatter(x=index, y=dftempcountry.loc[index,'ecmwf-mmsf'],
                            mode='lines',
                            name='Seasonal Fcst',
                            visible='legendonly',
                            line=dict(color='red', dash='solid')),secondary_y=False)
        '''
        figchart.add_trace(go.Scatter(x=index, y=dftemp_last.loc[index[0]:,country+' ens'],
                            mode='lines',
                            name='Ensemble last '+str(today-relativedelta(days=(1))),
                            line=dict(color='blue', dash='dot')),secondary_y=False)
        figchart.add_trace(go.Scatter(x=index, y=dftemp_last.loc[index[0]:,country+' ec46'],
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
        #print('2021'+str(index[0])[4:10])
        figchart.add_trace(go.Scatter(x=index, y=dftemp_normal.loc[index,country+' normal'],
                            mode='lines',
                            name='Normal',
                            line=dict(color='black', dash='solid')),secondary_y=False)
        figchart.add_trace(go.Scatter(x=index,y=dfrange['max'],
                            fill='tonexty',
                            fillcolor='rgba(65,105,225,0)',
                            line_color='rgba(65,105,225,0)',
                            showlegend=False,
                            name='Min/Max 1980-2021'
                            ),secondary_y=False)
                
        figchart.add_trace(go.Scatter(x=index,y=dfrange['min'],
                            fill='tonexty',
                            fillcolor='rgba(65,105,225,0.1)',
                            line_color='rgba(65,105,225,0)',
                            showlegend=True,
                            name='Min/Max 1980-2021'
                            ),secondary_y=False)
        
        figchart.update_layout(barmode='stack')
        figchart.update_yaxes(range=[dfdelta['delta'].min()*2, dfdelta['delta'].max()*5], title_text="Delta ℃", secondary_y=True)
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
        
        py.plot(figchart, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/JKTC temp/'+country+' chart.html', auto_open=False)
        
        #table
        hist_end = dftemphist[name_dict[country]].loc[~dftemphist[name_dict[country]].isnull()].index[-1]
        ens_end = dftemp_latest[country+' ens'].loc[~dftemp_latest[country+' ens'].isnull()].index[-1]
        ec_end = dftemp_latest[country+' ec46'].loc[~dftemp_latest[country+' ec46'].isnull()].index[-1]
        #2 months forward view by ec46 end date
        #if ec_end.day<=15:
        #    table_end = str((today+relativedelta(months=0)).year)+'-'+str((today+relativedelta(months=0)).month)+'-'+str(calendar.monthrange((today+relativedelta(months=0)).year, (today+relativedelta(months=0)).month)[1])
        #else:
        #    table_end = str((today+relativedelta(months=1)).year)+'-'+str((today+relativedelta(months=1)).month)+'-'+str(calendar.monthrange((today+relativedelta(months=1)).year, (today+relativedelta(months=1)).month)[1])
        if ec_end <=  pd.to_datetime(str((today+relativedelta(months=1)).year)+'-'+str((today+relativedelta(months=1)).month)+'-15'):
            table_end = str((today+relativedelta(months=0)).year)+'-'+str((today+relativedelta(months=0 )).month)+'-'+str(calendar.monthrange((today+relativedelta(months=0)).year, (today+relativedelta(months=0)).month)[1])
        else:
            table_end = str((today+relativedelta(months=1)).year)+'-'+str((today+relativedelta(months=1)).month)+'-'+str(calendar.monthrange((today+relativedelta(months=1)).year, (today+relativedelta(months=1)).month)[1])
        
        #print(dftemp_latest.index[0], ens_end, table_end)
        dftempmor_full = pd.DataFrame(index = pd.date_range(start = str(today.year-4)+'-01-01', end = table_end), columns = dftemp_normal_hist.columns)
        for i in dftempmor_full.index:
            try:
                dftempmor_full.loc[i] = dftemp_normal_hist.loc['2021-'+str(i.month)+'-'+str(i.day)]
            except KeyError:
                dftempmor_full.loc[i] = dftemp_normal_hist.loc['2021-'+str(i.month)+'-'+str(i.day-1)]
        
        #print(dftempmor_full)
        
        #print(ec_end)
        #print(dftemphist)
        #table
        for i in [name_dict[country]]:
            #print(dftemphist)
            '''
            dftemphistM = dftemphist[[i, i+' normal']].copy()
            if ens_end.year > today.year or ec_end.year > today.year:
                dfyear1 = pd.DataFrame(index = pd.date_range(start = str(today.year+1)+'-01-01', end = ec_end), columns = dftemphistM.columns)
                dftemphistM = pd.concat([dftemphistM, dfyear1])
                
            print(dftemphistM)
            print(dftemp_latest.loc[dftemp_latest.index[0]:ens_end, country+' ens'])
            dftemphistM.loc[hist_end:today,country] = dftemphistgap.loc[hist_end:today, i].values
            dftemphistM.loc[today:ens_end,i] = dftemp_latest.loc[today:ens_end, country+' ens']
            dftemphistM.loc[ens_end:pd.to_datetime(ec_end),i] = dftemp_latest.loc[ens_end:pd.to_datetime(ec_end),country+' ec46']
            #print(dftemphistM)
            
            dftemphistM.loc[dftemp_latest.index[0]:ens_end,i+' normal'] = dftempmor_full.loc[dftemp_latest.index[0]:ens_end,country+' normal']

            dftemphistM.loc[ens_end:ec_end,i+' normal'] = dftempmor_full.loc[ens_end:ec_end,country+' normal']
            dftemphistM = dftemphistM.astype('float')
            dftemphistM = dftemphistM.resample('MS').mean().round(1)
            
            '''
            #hist_end = dftemphist.index[-1]
            #ens_end = dfenslatest.index[-1]
            #ec_end = dfec46latest.index[-1]
       
            #1 months forward view by ec46 end date
            #if ec_end.day<=15:
            #    table_end = str((today+relativedelta(months=0)).year)+'-'+str((today+relativedelta(months=0)).month)+'-'+str(calendar.monthrange((today+relativedelta(months=0)).year, (today+relativedelta(months=0)).month)[1])
            #else:
            #    table_end = str((today+relativedelta(months=1)).year)+'-'+str((today+relativedelta(months=1)).month)+'-'+str(calendar.monthrange((today+relativedelta(months=1)).year, (today+relativedelta(months=1)).month)[1])
            
            
            if ec_end <=  pd.to_datetime(str((today+relativedelta(months=1)).year)+'-'+str((today+relativedelta(months=1)).month)+'-15'):
                table_end = str((today+relativedelta(months=0)).year)+'-'+str((today+relativedelta(months=0 )).month)+'-'+str(calendar.monthrange((today+relativedelta(months=0)).year, (today+relativedelta(months=0)).month)[1])
            else:
                table_end = str((today+relativedelta(months=1)).year)+'-'+str((today+relativedelta(months=1)).month)+'-'+str(calendar.monthrange((today+relativedelta(months=1)).year, (today+relativedelta(months=1)).month)[1])
        
            #after hist_end use metdesk normal
            dftemphistM= pd.DataFrame(index = pd.date_range(start = str(today.year-4)+'-01-01', end = table_end))
            dftemphistM[country] = dftemphist[[i]].copy()
            dftemphistM[country+' normal'] = dftemphist[i+' normal']
            dftemphistM.loc[hist_end:pd.to_datetime(table_end), country+' normal'] = dftempmor_full.loc[hist_end:pd.to_datetime(table_end),country+' normal']
            
            #print(dftemphistM.loc[ec_end:pd.to_datetime(table_end),country])
            #print(dftempmor_full.loc[ec_end:pd.to_datetime(table_end),country+' normal'])
            #print(dftemphistgap)
            dftemphistM.loc[hist_end:today,country] = dftemphistgap.loc[hist_end:today, i].values
            dftemphistM.loc[today:ens_end,country] = dftemp_latest.loc[today:ens_end, country+' ens'].values
            dftemphistM.loc[ens_end:pd.to_datetime(ec_end),country] = dftemp_latest.loc[ens_end:pd.to_datetime(ec_end),country+' ec46']
            dftemphistM.loc[ec_end:pd.to_datetime(table_end),country] = dftempmor_full.loc[ec_end:pd.to_datetime(table_end),country+' normal']
            dftemphistM = dftemphistM.astype('float')
            #print(dftemphistM)
            dftemphistM=dftemphistM.resample('MS').mean().round(1)
            dftemphistM.loc[str(today.year-4),country] = dftemphistM.loc[str(today.year-4),country].values - dftemphistM.loc[str(today.year-4),country+' normal'].values
            dftemphistM.loc[str(today.year-3),country] = dftemphistM.loc[str(today.year-3),country].values - dftemphistM.loc[str(today.year-3),country+' normal'].values
            dftemphistM.loc[str(today.year-2),country] = dftemphistM.loc[str(today.year-2),country].values - dftemphistM.loc[str(today.year-2),country+' normal'].values
            dftemphistM.loc[str(today.year-1),country] = dftemphistM.loc[str(today.year-1),country].values - dftemphistM.loc[str(today.year-1),country+' normal'].values
            dftemphistM.loc[str(today.year-0),country] = dftemphistM.loc[str(today.year-0),country].values - dftemphistM.loc[str(today.year-0),country+' normal'].values
            dftemphistM=dftemphistM.round(1)
            dftemphistM = dftemphistM.loc[:table_end]
            #print(dftemphistM)
            dfcolor=pd.DataFrame()
            dfcolor.loc[:,str(today.year-4)] = dftemphistM.loc[str(today.year-4),country].values
            dfcolor.loc[:,str(today.year-3)] = dftemphistM.loc[str(today.year-3),country].values
            dfcolor.loc[:,str(today.year-2)] = dftemphistM.loc[str(today.year-2),country].values
            dfcolor.loc[:,str(today.year-1)] = dftemphistM.loc[str(today.year-1),country].values
            dfcolor.loc[:table_end,str(today.year-0)] = dftemphistM.loc[str(today.year-0),country].values
            
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
            dftext_color.loc[:table_end,str(today.year-0)] = dftemphistM.loc[str(today.year-0),country].values
            #print(dftext_color)
            
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
            #print(dftext_color)
            dftext_color=dftext_color.T
            #print(dftemphistM)
            
            dftemphistM.fillna(' ', inplace=True)
            if ens_end.year > today.year or ec_end.year > today.year:
                figtable = go.Figure(data=[go.Table(
                        header=dict(values=['Month  Deg.C','Normal',str(today.year-4)+' Delta',str(today.year-3)+' Delta',str(today.year-2)+' Delta',str(today.year-1)+' Delta', str(today.year)+' Delta', str(today.year+1)+' Delta'],
                                    fill_color='paleturquoise',
                                    align='left'),
                        cells=dict(values=[[calendar.month_name[month_idx] for month_idx in range(1, 13)], 
                                           dftemphistM.loc['2021',country+' normal'].values,
                                           dftemphistM.loc[str(today.year-4),country].values,
                                           dftemphistM.loc[str(today.year-3),country].values,
                                           dftemphistM.loc[str(today.year-2),country].values,
                                           dftemphistM.loc[str(today.year-1),country].values,
                                           dftemphistM.loc[str(today.year-0),country].values,
                                           dftemphistM.loc[str(today.year+1),country].values],
                                   #fill_color='lavender',
                                   fill=dict(color=dfcolor.values.tolist()),
                                   font=dict(color=dftext_color.values.tolist()),
                                   align='center'))
                    ])
            else:    
                figtable = go.Figure(data=[go.Table(
                            header=dict(values=['Month  Deg.C','Normal',str(today.year-4)+' Delta',str(today.year-3)+' Delta',str(today.year-2)+' Delta',str(today.year-1)+' Delta', str(today.year)+' Delta'],
                                        fill_color='paleturquoise',
                                        align='left'),
                            cells=dict(values=[[calendar.month_name[month_idx] for month_idx in range(1, 13)], 
                                               dftemphistM.loc['2021',country+' normal'].values,
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
            figtable.update_layout(title_text=i+' Temperature hist and fcst table')
        
            py.plot(figtable, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/JKTC temp/'+country+' table.html', auto_open=False)
            
    
    
    def temp_dod_table(countrylist, dffcst, dffcst_last, dftemp_normal,issue_date):  
           
           today = datetime.date.today()
           
           week_index = pd.date_range(start=today+relativedelta(days=(11)), end=today+relativedelta(days=(40)), freq='W')
           
           #print(issue_date)
           ensdate = pd.to_datetime(issue_date['ens']).strftime('%Y-%m-%d')
           ec46date = pd.to_datetime(issue_date['ec46']).strftime('%Y-%m-%d')
           #print(ensdate)
           
           #print(dffcst)
           #print(dftemp_normal)
           ##dfdelta = dffcst - dffcst_last
           #dfdelta = dffcst - dftemp_normal
           #print(dfdelta)
           dfdelta_table1 = pd.DataFrame(index = pd.date_range(start=today+relativedelta(days=(1)), end=today+relativedelta(days=(10))), columns = countrylist)
           dfdelta_table2 = pd.DataFrame(index = week_index, columns = countrylist)
           dfdelta_table = pd.concat([dfdelta_table1, dfdelta_table2])
           #dfdelta_table.loc[week_index] = np.nan
           for i in countrylist:
               dfdelta_table.loc[today:today+relativedelta(days=(10)), i] = (dffcst.loc[today:today+relativedelta(days=(10)), i+' ens'] - dftemp_normal.loc[today:today+relativedelta(days=(10)), i+' normal']).round(1)# dfdelta.loc[today:today+relativedelta(days=(10)), i].round(1)
               dfdelta_table.loc[week_index, i] =  (dffcst.loc[week_index, i+' ec46'] - dftemp_normal.loc[week_index, i+' normal']).resample('W').mean().round(1)  
        
           #print((dffcst.loc[today+relativedelta(days=(1)):today+relativedelta(days=(10)),['CN ens','JP ens','KR ens','TW ens']].values - dftemp_normal.loc[today+relativedelta(days=(1)):today+relativedelta(days=(10)),['CN normal',  'JP normal',  'KR normal',  'TW normal']].values).mean(axis=0).round(1))
           #dfdelta_table = dfdelta_table
           dfdelta_table_ave = pd.DataFrame(index = ['Ens Ave. vs SN', 'EC46 Ave. vs SN', 'Ens Ave.','EC46 Ave.'], columns = countrylist)
           dfdelta_table_ave.loc['Ens Ave. vs SN'] = (dffcst.loc[today+relativedelta(days=(1)):today+relativedelta(days=(10)),['CN ens','JP ens','KR ens','TW ens']].values - dftemp_normal.loc[today+relativedelta(days=(1)):today+relativedelta(days=(10)),['CN normal',  'JP normal',  'KR normal',  'TW normal']].values).mean(axis=0).round(1)
           dfdelta_table_ave.loc['EC46 Ave. vs SN'] = (dffcst.loc[week_index,['CN ec46','JP ec46','KR ec46','TW ec46']].values - dftemp_normal.loc[week_index,['CN normal',  'JP normal',  'KR normal',  'TW normal']].values).mean(axis=0).round(1)
           dfdelta_table_ave.loc['Ens Ave.'] = dfdelta_table.loc[today:today+relativedelta(days=(10))].mean().round(1)
           dfdelta_table_ave.loc['EC46 Ave.'] = dfdelta_table.loc[week_index].mean().round(1)
           
           
           dfdelta_table = pd.concat([dfdelta_table_ave, dfdelta_table], axis=0)
           #print(dfdelta_table)
           #print(dfdelta_table_ave)
           
           dfdelta_table_JKTC = dfdelta_table.copy()
           #dfdelta_table_ave = dfdelta_table.loc[['Ens Ave.','EC46 Ave.']]
           
           delta_table_all = dfdelta_table_JKTC
       
           
           #C1 df bar colour, 
           dfdelta_color = delta_table_all.copy()
           dfdelta_color_cell = delta_table_all.copy()
           #print(dfdelta_color)
           
           for ism in dfdelta_color.index[4:]:
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
           for ism in dfdelta_color.index[0:4]:
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
                       
           #print(dfdelta_color)
           #print(dfdelta_color_cell)
           
           dfdelta_color=dfdelta_color
           
           columns = list(delta_table_all.index[0:4]) + list(pd.to_datetime(delta_table_all.index[4:]).strftime('%Y-%m-%d')) 
           index = list(delta_table_all.columns)
           #print(delta_table_all.index)
           #print(columns)
           
           delta_table_all = delta_table_all.T 
           delta_table_all.fillna(' ', inplace=True)
           delta_table_all.columns=columns
           #delta_table_all.reset_index(inplace=True)
           #print(list(delta_table_all.columns))
           #print(delta_table_all.loc[:,'2022-07-11'].to_list())
           #print([index]+[delta_table_all.loc[:,'2022-07-11'].to_list()])
           
           delta_table_header = [[' ', ' ', 'Market'],
                                 [ensdate, str(today+relativedelta(days=(1)))+'<br>-<br>'+str(today+relativedelta(days=(10))), columns[0]],
                                 [ec46date, str(today+relativedelta(days=(11)))+'<br>-<br>'+str(today+relativedelta(days=(40))), columns[1]],
                                 ['DoD', ' ', columns[2]],
                                 ['DoD', ' ', columns[3]],
                                 ]
           for i in columns[4:14]:
               delta_table_header.append(['Ensemble vs. Normal', '10 days', i])
           for i in columns[14:]:
               delta_table_header.append(['EC46 Weekly Average vs. Normal', '30 days',i])
           #print(delta_table_header)
           #print(delta_table_all)
           figtable = go.Figure(data=[go.Table(
                       header=dict(values = delta_table_header,#['Market']+columns,
                                   fill_color=['LightSkyBlue']*5+['paleturquoise']*10+['SkyBlue']*len(week_index),
                                   align='center'),
                       cells=dict(values = [index] + [delta_table_all.loc[:,pm].to_list() for pm in columns],#delta_table_all.columns],
                                  #fill_color='lavender',
                                  fill=dict(color=[['white', 'white', 'white', 'white', 'LightGrey', 'white', 'white', 'white', 'LightGrey', 'white', 'white', 'LightGrey', 'white', 'white', 'white', 'white', 'white', 'LightGrey', 'white', 'white', 'white', 'white', 'white', 'white', 'LightGrey', 'white', 'white', 'white', 'white', 'white', 'LightGrey', 'white']]+dfdelta_color_cell.values.tolist()),
                                  font=dict(color=['black']+dfdelta_color.values.tolist()),
                                  align='center'))
                   ])
           figtable.update_layout(title_text='DoD Temperature fcst delta table(Ens daily & EC46 weekly ave.) '+str(today))
       
           py.plot(figtable, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/JKTC temp/Temp DoD table.html', auto_open=False)
           figtable.write_image("U:/Trading - Gas/LNG/LNG website/analysis/JKTC temp/Temp DoD table.png")
        
       
    def update():
            
        dftemp_latest, dftemp_last, dftemp_normal, dftemp_normal_hist,issue_date = JKTC_Temp.get_data()
        dftemphist = DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'TempHistNormal').sql_to_df()
        dftemphist.set_index('ValueDate', inplace=True)
        
        dftemprange =  DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'TempHistMinMax').sql_to_df()
        dftemprange.set_index('index', inplace=True)
        
        dftemphistgap = JKTC_Temp.get_temp_hist_gap()

        
        for country in ['CN','JP','KR','TW']:
            JKTC_Temp.temp_chart(country, dftemp_latest, dftemp_last, dftemp_normal, dftemp_normal_hist, dftemphist, dftemprange, dftemphistgap)
        countrylist = ['CN','JP','KR','TW']
        JKTC_Temp.temp_dod_table(countrylist, dftemp_latest, dftemp_last, dftemp_normal,issue_date)
        
#JKTC_Temp.update()