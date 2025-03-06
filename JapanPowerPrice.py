# -*- coding: utf-8 -*-
"""
Created on Mon Oct 25 17:06:56 2021

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
import json


#from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import requests
sys.path.append('C:\\Users\SVC-GASQuant2-Prod\YWwebsite\Python Scripts\class') 
pd.set_option('display.max_columns',10)
from DBtoDF import DBTOPD
import sqlalchemy


class Japan_Power_price():
    
    
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
        dfusdjpy = dffx.loc[dffx[dffx['CURR_CD'] == 'JPY'].index,['FX_PERIOD_CD','FWD_OFFSET','FX_VALUE']]
        
        dfusdjpy.set_index('FX_PERIOD_CD', inplace=True)
        dfusdjpy = dfusdjpy.loc[index_order]
        dfusdjpy.reset_index(inplace=True)
        index_jpy=[]
        for i in dfusdjpy.index:
            dfusdjpy.loc[i,'FX_PERIOD_CD'] = index_dict[dfusdjpy.loc[i,'FX_PERIOD_CD']]  #index_gbp.append(index_dict[i])
        dfusdjpy.sort_values(by='FX_PERIOD_CD',inplace=True)
        dfusdjpy.set_index('FX_PERIOD_CD', inplace=True)
    
        #calc. curve
        start = str((today+relativedelta(months=1)).year)+'-'+str((today+relativedelta(months=0)).month)+'-01'
        end = str((today+relativedelta(months=48)).year)+'-'+str((today+relativedelta(months=48)).month)+'-01'
        dfcurve = pd.DataFrame(np.nan, index=pd.date_range(start=start, end=end, freq='MS').date, columns=['eurusd','jpyusd'])
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
            y1 = dfusdjpy.loc[x1,'FX_VALUE']
            y2 = dfusdjpy.loc[x2,'FX_VALUE']
            b=y1
            a=y2-y1
            
            #print(a,b)
            #print(dfcurve.loc[i])
            dfcurve.loc[i,'jpyusd'] =a*X1+b
        
        
        dfcurve.index=pd.to_datetime(dfcurve.index)
        db_server_lng = "PRD-DB-SQL-211"
        db_name_lng = "LNG"
        sql_engine_lng = sqlalchemy.create_engine("mssql+pyodbc://" + db_server_lng + "/" + db_name_lng + "?trusted_connection=yes&driver=ODBC+Driver+13+for+SQL+Server")
        
        dfcurve.to_sql(name='FXJPEURcurve', con=sql_engine_lng, index=True, if_exists='replace', schema='ana')
        #print(dfcurve)
        
        return dfcurve
    
    
    def get_curve(dffx):
        
        curve =DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'BBGJKMCurve')
        dfcurve = curve.sql_to_df()
        dfcurve.set_index('index', inplace=True)
        
        platts=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'PlattsPrint')
        dbplatts=platts.sql_to_df()
        
        curve_id = ['JKM FWD','ICE BRENT FWD'] 
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
        
        
        #print(dfcurvefull)
        #dfcurve = dfcurve[['index','jkm fwd']]
        df=pd.DataFrame(index=pd.date_range(start=str(datetime.date.today().year)+'-'+str(datetime.date.today().month+0)+'-01', end=str((datetime.date.today()+relativedelta(months=12)).year)+'-'+str((datetime.date.today()+relativedelta(months=12)).month)+'-01', freq='MS'))
        df['ICE Brent Fwd'] = dfcurvefull.loc[df.index[0]:df.index[-1], 'ICE BRENT FWD']
        df['jkm fwd'] = dfcurvefull.loc[df.index[0]:df.index[-1], 'JKM FWD']
        df['3-1-1 @ 13%'] = dfcurvefull.loc[:df.index[-1], 'ICE BRENT FWD'].rolling(3).mean()*0.13
        df['JPY/US'] = dffx['jpyusd']
        df['EUR/US'] = dffx['eurusd']
        
        #print(df)
        return df
    
    
    def get_power_curve():
        
        '''
        df = DBTOPD.get_japan_power_price()
        
        df_pivot = df.pivot_table('VALUE', columns = 'PRODUCT', index = 'CONTRACT_DATE')
        #df_pivot.columns = df_pivot.columns.droplevel(0)
        df_pivot.index = pd.to_datetime(df_pivot.index)
        #print(df_pivot.columns)
        name_dict = {'JEBD':'JapEastBas',
                     'JEPD':'JapEastPeak',
                     'JWBD':'JapWestBas',
                     'JWPD':'JapWestPeak'
            
            }
        
        df_pivot.rename(columns = name_dict, inplace=True)
        df_pivot = df_pivot.loc[str(datetime.date.today().year)+'-'+str(datetime.date.today().month+0)+'-01':str((datetime.date.today()+relativedelta(months=12)).year)+'-'+str((datetime.date.today()+relativedelta(months=12)).month)+'-01']
        #print(df_pivot)
        '''
        
        df = DBTOPD.get_japan_power_price()
        least_ts = df['timestamp'].iloc[-1]
        dfprice = df.loc[df[df['timestamp']==least_ts].index]
        dfprice.set_index('index', inplace=True)
        dfprice = dfprice[['EastBase' , 'EastPeak' , 'WestBase',  'WestPeak']]
        dfprice.drop(index=datetime.date.today(), inplace=True)
        #print(dfprice)
        
        
        
        return dfprice
    
    def full_data(dfcurve, dfpower):
        
        #print(dfcurve)
        #print(dfpower  )
        #df=pd.concat([dfpower,dfcurve], axis=1)
        df = dfpower.copy()
        df[dfcurve.columns] = dfcurve
        df.loc[df.index[0], dfcurve.columns] = dfcurve.loc[dfcurve.index[0], dfcurve.columns].values
        
        df['JapWestBas $/mmbtu'] = df['WestBase']/df['JPY/US']/3.412*1000*0.5
        df['JapWestPeak $/mmbtu'] = df['WestPeak']/df['JPY/US']/3.412*1000*0.5
        df['JapEastBas $/mmbtu'] = df['EastBase']/df['JPY/US']/3.412*1000*0.5
        df['JapEastPeak $/mmbtu'] = df['EastPeak']/df['JPY/US']/3.412*1000*0.5
        
        #print(df)
        df=df.astype(float).round(2)
        df.sort_index(inplace=True)
        return df
    
    def plot_table(df):
        
        #print(df)
        #df table1
        #df1=pd.DataFrame(index = pd.date_range(start=str(datetime.date.today().year)+'-'+str(datetime.date.today().month+0)+'-01', periods=df.shape[0], freq='MS'))
        df1 = pd.DataFrame(index = df.index)
        df1['JapWestBas'] = df['JapWestBas $/mmbtu'] - df['jkm fwd']
        df1['JapWestPeak']= df['JapWestPeak $/mmbtu'] - df['jkm fwd']
        df1['JapEastBas']= df['JapEastBas $/mmbtu'] - df['jkm fwd']
        df1['JapEastPeak']= df['JapEastPeak $/mmbtu'] - df['jkm fwd']
        df1.index=df1.index.strftime("%Y-%m-%d")
        df1=df1.round(2)
        
        #print( df1)
        #table 1 color
        df1color=df1.copy()
        for i in df1color.index:
            for j in df1color.columns:
                if df1color.loc[i,j] >0:
                    df1color.loc[i,j] = 'LightGreen'
                elif df1color.loc[i,j] <0:
                    df1color.loc[i,j] = 'pink'
                else:
                    df1color.loc[i,j] = 'white'
    
        df1color.insert(0,'date',['paleturquoise']*df1color.shape[0])
        df1color=df1color.T
        #print(df1color)
        #df table2
        df2=pd.DataFrame(index=df.index)
        df2['JapWestBas'] = df['JapWestBas $/mmbtu'] - df['3-1-1 @ 13%']
        df2['JapWestPeak']= df['JapWestPeak $/mmbtu'] - df['3-1-1 @ 13%']
        df2['JapEastBas']= df['JapEastBas $/mmbtu'] - df['3-1-1 @ 13%']
        df2['JapEastPeak']= df['JapEastPeak $/mmbtu'] - df['3-1-1 @ 13%']
        df2.index=df2.index.strftime("%Y-%m-%d")
        df2=df2.round(2)
        #print(df[['JapWestBas','3-1-1 @ 13%']] )
        #table 2 color
        df2color=df2.copy()
        for i in df2color.index:
            for j in df2color.columns:
                if df2color.loc[i,j] >0:
                    df2color.loc[i,j] = 'LightGreen'
                elif df2color.loc[i,j] <0:
                    df2color.loc[i,j] = 'pink'
                else:
                    df2color.loc[i,j] = 'white'
    
        df2color.insert(0,'date',['paleturquoise']*df2color.shape[0])
        df2color=df2color.T
        
        
        fig1 = go.Figure(data=[go.Table(
                        header=dict(values=['date']+list(df1.columns),
                                    fill_color='paleturquoise',
                                    align='center'),
                        cells=dict(values=[df1.index, df1.JapWestBas, df1.JapWestPeak, df1.JapEastBas, df1.JapEastPeak],
                                   #fill_color='lavender',
                                   fill=dict(color=df1color.values.tolist()),
                                   #font=dict(color="white"),
                                   align='center'))
                    ])
        
        fig1.update_layout(
             title_text='Japan Power Price - JKM Forward',
         )
        
        py.plot(fig1, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/Japan power price/JapPowerTable1.html', auto_open=False)
        fig1.write_image('U:/Trading - Gas/LNG/LNG website/analysis/Japan power price/JapPowerTable1.png')
        
        fig2 = go.Figure(data=[go.Table(
                       header=dict(values=['date']+list(df2.columns),
                                   fill_color='paleturquoise',
                                   align='center'),
                       cells=dict(values=[df2.index, df2.JapWestBas, df2.JapWestPeak, df2.JapEastBas, df2.JapEastPeak],
                                  #fill_color='lavender',
                                  fill=dict(color=df2color.values.tolist()),
                                  align='center'))
                   ])
        fig2.update_layout(
             title_text='Japan Power Price - 13% Brent 3-1-1',
         )
        py.plot(fig2, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/Japan power price/JapPowerTable2.html', auto_open=False)
        fig2.write_image('U:/Trading - Gas/LNG/LNG website/analysis/Japan power price/JapPowerTable2.png')
    
    def plot_chart(df):
        
        #dfchart = df.iloc[1:].copy()
        dfchart = df.copy()
        print(dfchart)
        today=datetime.date.today()
        
        fig1 = make_subplots(specs=[[{"secondary_y": True}]])
        #fig1 = go.Figure()
        fig1.add_trace(go.Scatter(x=dfchart.index, y=dfchart['jkm fwd'],
                            mode='lines',
                            name='jkm fwd',
                            line=dict(color='red', dash='solid')),
                       secondary_y=False,)
        fig1.add_trace(go.Scatter(x=dfchart.index, y=dfchart['WestBase'],
                            mode='lines',
                            name='JapWestBas Yen/kWh',
                            line=dict(color='blue', dash='dash')),
                       secondary_y=False)
        fig1.add_trace(go.Scatter(x=dfchart.index, y=dfchart['JapWestBas $/mmbtu'],
                            mode='lines',
                            name='JapWestBas $/mmbtu',
                            line=dict(color='black', dash='solid')),
                       secondary_y=False,)
        fig1.add_trace(go.Scatter(x=dfchart.index, y=dfchart['3-1-1 @ 13%'],
                            mode='lines',
                            name='3-1-1 @ 13%',
                            line=dict(color='ForestGreen', dash='solid')),
                       secondary_y=False,)
        fig1.update_layout(
            autosize=True,
            showlegend=True,
            legend=dict(x=0, y=-0.2),
            legend_orientation="h",
            title_text='JapWestBas '+str(today),
            xaxis = dict(dtick="M1"),
            hovermode='x unified',
            plot_bgcolor = 'white',
            template='ggplot2'  
        )
        #fig1.update_yaxes(title_text="Yen/KWh", secondary_y=False)
        fig1.update_yaxes(#title_text="$/mmbtu", 
                          secondary_y=False,rangemode="tozero")


        
        fig2 = make_subplots(specs=[[{"secondary_y": True}]])
        #fig2 = go.Figure()
        fig2.add_trace(go.Scatter(x=dfchart.index, y=dfchart['jkm fwd'],
                            mode='lines',
                            name='jkm fwd',
                            line=dict(color='red', dash='solid')),
                       secondary_y=False,)
        fig2.add_trace(go.Scatter(x=dfchart.index, y=dfchart['WestPeak'],
                            mode='lines',
                            name='JapWestPeak Yen/kWh',
                            line=dict(color='blue', dash='dash')),
                       secondary_y=False,)
        fig2.add_trace(go.Scatter(x=dfchart.index, y=dfchart['JapWestPeak $/mmbtu'],
                            mode='lines',
                            name='JapWestPeak $/mmbtu',
                            line=dict(color='black', dash='solid')),
                       secondary_y=False,)
        fig2.add_trace(go.Scatter(x=dfchart.index, y=dfchart['3-1-1 @ 13%'],
                            mode='lines',
                            name='3-1-1 @ 13%',
                            line=dict(color='ForestGreen', dash='solid')),
                        secondary_y=False,)

        fig2.update_layout(
            autosize=True,
            showlegend=True,
            legend=dict(x=0, y=-0.2),
            legend_orientation="h",
            title_text='JapWestPeak '+str(today),
            xaxis = dict(dtick="M1"),
            hovermode='x unified',
            plot_bgcolor = 'white',
            template='ggplot2'  
        )
        #fig2.update_yaxes(title_text="Yen/KWh", secondary_y=False)
        fig2.update_yaxes(#title_text="$/mmbtu", 
                          secondary_y=False,rangemode="tozero")
        
        fig3 = make_subplots(specs=[[{"secondary_y": True}]])
        #fig3 = go.Figure()
        fig3.add_trace(go.Scatter(x=dfchart.index, y=dfchart['jkm fwd'],
                            mode='lines',
                            name='jkm fwd',
                            line=dict(color='red', dash='solid')),
                       secondary_y=False,)
        fig3.add_trace(go.Scatter(x=dfchart.index, y=dfchart['EastBase'],
                            mode='lines',
                            name='JapEastBas Yen/kWh',
                            line=dict(color='blue', dash='dash')),
                       secondary_y=False,)
        fig3.add_trace(go.Scatter(x=dfchart.index, y=dfchart['JapEastBas $/mmbtu'],
                            mode='lines',
                            name='JapEastBas $/mmbtu',
                            line=dict(color='black', dash='solid')),
                       secondary_y=False,)
        fig3.add_trace(go.Scatter(x=dfchart.index, y=dfchart['3-1-1 @ 13%'],
                            mode='lines',
                            name='3-1-1 @ 13%',
                            line=dict(color='ForestGreen', dash='solid')),
                       secondary_y=False,)
        fig3.update_layout(
            autosize=True,
            showlegend=True,
            legend=dict(x=0, y=-0.2),
            legend_orientation="h",
            title_text='JapEastBas '+str(today),
            xaxis = dict(dtick="M1"),
            hovermode='x unified',
            plot_bgcolor = 'white',
            template='ggplot2'  
        )
        #fig3.update_yaxes(title_text="Yen/KWh", secondary_y=False)
        fig3.update_yaxes(#title_text="$/mmbtu", 
                          secondary_y=False,rangemode="tozero")
        
        fig4 = make_subplots(specs=[[{"secondary_y": True}]])
        #fig4 = go.Figure()
        fig4.add_trace(go.Scatter(x=dfchart.index, y=dfchart['jkm fwd'],
                            mode='lines',
                            name='jkm fwd',
                            line=dict(color='red', dash='solid')),
                       secondary_y=False,)
        fig4.add_trace(go.Scatter(x=dfchart.index, y=dfchart['EastPeak'],
                            mode='lines',
                            name='JapEastPeak Yen/kWh',
                            line=dict(color='blue', dash='dash')),
                       secondary_y=False,)
        fig4.add_trace(go.Scatter(x=dfchart.index, y=dfchart['JapEastPeak $/mmbtu'],
                            mode='lines',
                            name='JapEastPeak $/mmbtu',
                            line=dict(color='black', dash='solid')),
                       secondary_y=False,)
        fig4.add_trace(go.Scatter(x=dfchart.index, y=dfchart['3-1-1 @ 13%'],
                            mode='lines',
                            name='3-1-1 @ 13%',
                            line=dict(color='ForestGreen', dash='solid')),
                       secondary_y=False,)
        fig4.update_layout(
            autosize=True,
            showlegend=True,
            legend=dict(x=0, y=-0.2),
            legend_orientation="h",
            title_text='JapEastPeak '+str(today),
            xaxis = dict(dtick="M1"),
            hovermode='x unified',
            plot_bgcolor = 'white',
            template='ggplot2'  
        )
        #fig4.update_yaxes(title_text="Yen/KWh", secondary_y=False)
        fig4.update_yaxes(#title_text="$/mmbtu", 
                          secondary_y=False,rangemode="tozero"            
                          )
        
        py.plot(fig1, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/Japan power price/JapWestBas.html', auto_open=False)
        py.plot(fig2, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/Japan power price/JapWestPeak.html', auto_open=False)
        py.plot(fig3, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/Japan power price/JapEastBas.html', auto_open=False)
        py.plot(fig4, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/Japan power price/JapEastPeak.html', auto_open=False)
        
        fig1.write_image('U:/Trading - Gas/LNG/LNG website/analysis/Japan power price/JapWestBas.png')
        fig2.write_image('U:/Trading - Gas/LNG/LNG website/analysis/Japan power price/JapWestPeak.png')
        fig3.write_image('U:/Trading - Gas/LNG/LNG website/analysis/Japan power price/JapEastBas.png')
        fig4.write_image('U:/Trading - Gas/LNG/LNG website/analysis/Japan power price/JapEastPeak.png')
    
    def plot_table_all(df):
        
        
        df1=df.copy()
        df1['Contract'] =  df1.index.strftime('%b-%Y')
        df1.loc[df.index[0],'Contract'] = '1D Ahead'
        df1 = df1[['Contract','WestBase','JapWestBas $/mmbtu','WestPeak','JapWestPeak $/mmbtu','EastBase','JapEastBas $/mmbtu','EastPeak','JapEastPeak $/mmbtu','JPY/US', 'EUR/US', '3-1-1 @ 13%','jkm fwd','ICE Brent Fwd']]
        df1.index = df1.index.strftime('%d-%m-%Y')
        df1.reset_index(inplace=True)
        
        name_dict = {
                    'WestBase' : 'JapWestBas Yen/kWh',
                    'WestPeak':'JapWestPeak Yen/kWh',
                    'EastBase':'JapEastBas Yen/kWh',
                    'EastPeak':'JapEastPeak Yen/kWh'
            }
        
        df1.rename(columns = name_dict, inplace=True)
        '''
        df1color=df1.copy()
        df1color['Contract'] = 0
        
        #print(df1color)
        for i in df1color.index:
            for j in df1color.columns:
                if df1color.loc[i,j] > 0:
                    df1color.loc[i,j] = 'LightGreen'
                elif df1color.loc[i,j] < 0:
                    df1color.loc[i,j] = 'pink'
                else:
                    df1color.loc[i,j] = 'white'
    
        df1color.insert(0,'date',['paleturquoise']*df1color.shape[0])
        df1color=df1color.T
        '''
        fig1 = go.Figure(data=[go.Table(
                        header=dict(values=list(df1.columns),
                                    fill_color='paleturquoise',
                                    align='center'),
                        cells=dict(values=[df1[i] for i in df1.columns],#[df1.index, df1.JapWestBas, df1.JapWestPeak, df1.JapEastBas, df1.JapEastPeak],
                                   #fill_color='lavender',
                                   #fill=dict(color=df1color.values.tolist()),
                                   #font=dict(color="white"),
                                   align='center'))
                    ])
        
        fig1.update_layout(
             title_text='Japan Power Price',
         )
        
        py.plot(fig1, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/Japan power price/JapPowerTableFull.html', auto_open=False)
        fig1.write_image('U:/Trading - Gas/LNG/LNG website/analysis/Japan power price/JapPowerTableFull.png')
        
        
    def update():

        a=Japan_Power_price
        dffx = a.get_fx()
        dfpower=a.get_power_curve()
        dfcurve=a.get_curve(dffx)
        df=a.full_data(dfcurve, dfpower)
        a.plot_chart(df)
        a.plot_table(df)
        a.plot_table_all(df)

#Japan_Power_price.update()