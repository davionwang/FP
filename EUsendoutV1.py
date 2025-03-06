# -*- coding: utf-8 -*-
"""
Created on Wed Feb 15 09:07:17 2023

@author: SVC-GASQuant2-Prod
"""


#V1 add DE

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
import calendar

pd.set_option('display.max_columns',20)
from DBtoDF import DBTOPD


class EU_sendout():
    
    def get_regas_stock():
        
        today = datetime.date.today()
        
        dfce = DBTOPD.get_ce_regas_stock()
        
        dfce_povit = pd.pivot_table(dfce,values='Value', index='Date',columns='SeriesId')
        #dfce_povit.columns = dfce_povit.columns.droplevel(0)
        dfce_povit.rename_axis(None, axis=1, inplace=True)
        #print(dfce_povit.columns)
        
        dfcename = DBTOPD('PRD-DB-SQL-211','LNG', 'ana', '[CERegasMap]').sql_to_df()
        #ce_dict = dfcename[['seriesId','Name']].to_dict('index')#‘dict’, ‘list’, ‘series’, ‘split’, ‘records’, ‘index’
        #print(dfcename)
        ce_dict={}
        for i in dfcename.index:
            ce_dict[dfcename.loc[i,'seriesId']] = dfcename.loc[i,'Name']
            
        dfce_povit.rename(columns=ce_dict, inplace=True)   
        dfce_povit['All EU'] = dfce_povit.sum(axis=1).round(2)
        dfce_povit['NWE'] = dfce_povit[['Zeebrugge Lng','Dunkerque LNG','Fos','Montoir','Dragon','Isle Of Grain','South Hook','Eemshaven','Gate LNG','Brunsbuettel','Lubmin, Deutsche ReGas','Wilhelmshaven']].sum(axis=1).round(2)
        #dfce_povit = dfce_povit.loc[(datetime.date.today()-relativedelta(days=15)):]
        dfce_povit = dfce_povit.loc[:today]
        dfce_povit.fillna(method='ffill', inplace=True)
        #print(dfce_povit)
        
        return dfce_povit
    
    def get_regas_sendout():
        
        today = datetime.date.today()
        
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
        dfce_povit['All EU'] = dfce_povit.sum(axis=1).round(2)
        #dfce_povit = dfce_povit.loc[(datetime.date.today()-relativedelta(days=15)):]
        dfce_povit.drop(columns=[60384], inplace=True)
        dfce_povit['EU Desk'] = dfce_povit[['Zeebrugge Lng','Dunkerque LNG','Fos','Montoir','Dragon','Isle Of Grain','South Hook','Eemshaven','Gate LNG','Brunsbuettel','Lubmin, Deutsche ReGas','Wilhelmshaven','Barcelona LNG','Bilbao LNG','Cartagena LNG','Huelva LNG','Mugardos LNG','Sagunto LNG','Adriatic','Panigaglia','Toscana','Swinoujscie']].sum(axis=1).round(2)
        dfce_povit = dfce_povit.loc[:today]
        dfce_povit.fillna(method='ffill', inplace=True)
        #print(dfce_povit)
        
        return dfce_povit
    
    
    def get_regas_capa():
        
        today = datetime.date.today()
        
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
        dfce_povit.fillna(method = 'ffill', inplace=True)
        dfce_povit['All EU'] = dfce_povit.sum(axis=1).round(2)
        dfce_povit['EU Desk'] = dfce_povit[['Zeebrugge Lng','Dunkerque LNG','Fos','Montoir','Dragon','Isle Of Grain','South Hook','Eemshaven','Gate LNG','Brunsbuettel','Lubmin, Deutsche ReGas','Wilhelmshaven','Barcelona LNG','Bilbao LNG','Cartagena LNG','Huelva LNG','Mugardos LNG','Sagunto LNG','Adriatic','Panigaglia','Toscana','Swinoujscie']].sum(axis=1).round(2)
        dfce_povit = dfce_povit.loc[:today]
        #dfce_povit = dfce_povit.loc[(datetime.date.today()-relativedelta(days=15)):]
        #print(dfce_povit)
        
        
        return dfce_povit
        
    
    def get_regas_arrivel(terminal):
        
        dfkpler = DBTOPD.get_regas_arrivel()
        #m3 to mcm
        dfkpler['VolumeDestinationMcm'] = dfkpler['VolumeDestinationM3']*0.000612
        
        dfkpler_destination = dfkpler[['InstallationDestination','StartDestination','EtaDestination','VolumeDestinationMcm']]
        dfarrivel = dfkpler_destination.loc[dfkpler_destination[dfkpler_destination['InstallationDestination'] == terminal].index]
        dfarrivel['StartDestination'] = pd.to_datetime(dfarrivel['StartDestination']).dt.date
        dfarrivel['EtaDestination'] = pd.to_datetime(dfarrivel['EtaDestination']).dt.date
        
        today = datetime.date.today()
        dfarrivel_full = pd.DataFrame(index = pd.date_range(start=today-relativedelta(days=15), end = today+relativedelta(days=30)), columns=['Cargo arrival'])
        for i in dfarrivel.index:
            dfarrivel_full.loc[pd.to_datetime(dfarrivel.loc[i,'StartDestination']),'Cargo arrival'] = dfarrivel.loc[i,'VolumeDestinationMcm'].round(1)
            if dfarrivel.loc[i,'EtaDestination'] > today:
                dfarrivel_full.loc[pd.to_datetime(dfarrivel.loc[i,'EtaDestination']),'Cargo arrival'] = dfarrivel.loc[i,'VolumeDestinationMcm'].round(1)
        #print(dfarrivel_full)
        return dfarrivel_full
    
    
    def chart_sendout_data(dfstorage, dfcapa, dfsendout):
        
        today = datetime.date.today()
        
        terminal_list = list(set(dfstorage.columns)&set(dfsendout.columns)&set(dfcapa.columns))
        #print(terminal_list)
        #namemap = DBTOPD('PRD-DB-SQL-211','LNG', 'ana', '[CEKplerNameMap]').sql_to_df()
        #namemap.set_index('CE name', inplace=True)
        #namemap = namemap[namemap[namemap['Variable'] == 'withdrawal'].index]
        #namemap_dict = namemap[['CE name','Kpler name']].to_dict('records')
        #print(namemap_dict)
        for terminal in terminal_list:
            
            #kpler_terminal = namemap.loc[namemap[namemap['CE name'] == terminal].index, 'Kpler name'].values
            #dfarrivel = EU_regas.get_regas_arrivel(kpler_terminal[0])
            #print(terminal)
            df = pd.concat([dfcapa[terminal], dfstorage[terminal], dfsendout[terminal]], axis=1)
            df.columns = ['capacity','storage','sendout']
            df = df.loc[str(today.year-6)+'-01-01':today, ['capacity','storage','sendout']]
            
            #print(df)
            #df.set_index(terminal, inplace=True)
            df['capacity'].fillna(method='bfill', inplace=True)
            #df = df.loc[:'2022-08-01']
            #df['Cargo arrival'].fillna(0, inplace=True)
            #print(df)
            df['year'] = df.index.year
            df['date'] = df.index.strftime('%m-%d')
            dfyoy = df.set_index(['year', 'date']).sendout.unstack(0)
            dfyoy['Average'] = dfyoy.loc[:,[today.year-6, today.year-5,today.year-4,today.year-3,today.year-2,today.year-1]].mean(axis=1)
            dfyoy['min'] = dfyoy.loc[:,[today.year-6, today.year-5,today.year-4,today.year-3,today.year-2,today.year-1]].min(axis=1)
            dfyoy['max'] = dfyoy.loc[:,[today.year-6, today.year-5,today.year-4,today.year-3,today.year-2,today.year-1]].max(axis=1)
            dfyoy['max sendout'] = df['sendout'].max()
            #dfcapayoy = df.set_index(['year', 'date']).capacity.unstack(0)
            #dfyoy['capa'] = dfcapayoy[today.year]
            #dfyoy['capa'].fillna(method = 'ffill', inplace=True)
            #print(dfyoy)
            
            EU_sendout.chart_sendout(terminal, dfyoy)
    
    def chart_sendout(terminal, dfyoy):
        
        today = datetime.date.today()
        
       
        index = pd.date_range(start='2020-01-01', end ='2020-12-31')
        
        chart1 = go.Figure()
        chart1.add_trace(go.Scatter(x=index, y=dfyoy['min'],
                            mode='lines',
                            name='6 yrs range',
                            fill='tonexty',
                            fillcolor='rgba(211,211,211,0)',
                            line_color='rgba(211,211,211,0)',
                            showlegend=False,
                            #fillpattern=dict(fillmode='overlay'),
                            #line=dict(color='black', dash='solid')
                            ))
        chart1.add_trace(go.Scatter(x=index, y=dfyoy['max'],
                            mode='lines',
                            name='6 yrs range',
                            fill='tonexty',
                            fillcolor='rgba(211,211,211,0.35)',
                            line_color='rgba(211,211,211,0)',
                            showlegend=True,
                            #fillpattern=dict(fillmode='overlay'),
                            #line=dict(color='black', dash='solid')
                            ))
        
        chart1.add_trace(go.Scatter(x=index, y=dfyoy.loc[:,today.year],
                            mode='lines',
                            name=str(today.year),
                            line=dict(color='red', dash='solid'),
                            ))
        chart1.add_trace(go.Scatter(x=index, y=dfyoy.loc[:,today.year-1],
                            mode='lines',
                            name=str(today.year-1),
                            line=dict(color='black', dash='solid'),
                            ))
        chart1.add_trace(go.Scatter(x=index, y=dfyoy.loc[:,today.year-2],
                            mode='lines',
                            name=str(today.year-2),
                            line=dict(color='grey', dash='solid'),
                            ))
        chart1.add_trace(go.Scatter(x=index, y=dfyoy.loc[:,'Average'],
                            mode='lines',
                            name='6 yrs Average',
                            line=dict(color='blue', dash='dot'),
                            ))
        
        chart1.add_trace(go.Scatter(x=index, y=dfyoy.loc[:,'max sendout'],
                            mode='lines',
                            name='max sendout',
                            line=dict(color='black', dash='dot')
                            ))
        #print(np.nanmin(dfyoy.values), np.nanmax(dfyoy.values))
        chart1.add_trace(go.Scatter(x=['2020-'+str(today.month)+'-'+str(today.day),'2020-'+str(today.month)+'-'+str(today.day)],y=[np.nanmin(dfyoy.values), np.nanmax(dfyoy.values)],
                   mode="lines",
                   line=dict(color='red', width=2, dash='dot'),
                   showlegend=False,
                   name='Today'
                   ))
        chart1.update_layout(barmode='relative', annotations=[dict(x='2020-'+str(today.month)+'-'+str(today.day), y=1, yref='paper',font=dict(color="red",size=14),showarrow=False, xanchor='left', text='Today'),
                                           ],)
               
        chart1.update_yaxes(
            title_text="mcm/d",
                    showline=True, 
                    linewidth=1, 
                    linecolor='LightGrey', 
                    showgrid=True, 
                    gridwidth=1, 
                    gridcolor='LightGrey'
                )
        
        chart1.update_layout(
             autosize=True,
             showlegend=True,
             #colorscale='RdBu',
             #legend=dict(x=0, y=-0.2),
             legend_orientation="h",
             title_text=terminal+' Sendout (Mcm/d) '+str(today),
             #xaxis = dict(dtick="D"),
             xaxis_tickformat = '%m-%d',
             hovermode='x unified',
             plot_bgcolor = 'white',
             #yaxis = dict(showgrid=True, gridcolor='lightgrey', dtick="D2"),
             #xaxis_dtick=86400000*7
             #template='ggplot2'  
         )
        
        py.plot(chart1, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/LNG tracking/'+terminal+'.html', auto_open=False) 
        chart1.write_image('U:/Trading - Gas/LNG/LNG website/analysis/LNG tracking/'+terminal+'.png')
        

    def chart_storage_data(dfstorage, dfcapa, dfsendout):
        
        today = datetime.date.today()
        
        terminal_list = list(set(dfstorage.columns)&set(dfsendout.columns)&set(dfcapa.columns))
        #print(terminal_list)
        #namemap = DBTOPD('PRD-DB-SQL-211','LNG', 'ana', '[CEKplerNameMap]').sql_to_df()
        #namemap.set_index('CE name', inplace=True)
        
        #namemap_dict = namemap[['CE name','Kpler name']].to_dict('records')
        #print(namemap_dict)
        for terminal in terminal_list:
            
            #kpler_terminal = namemap.loc[namemap[namemap['CE name'] == terminal].index, 'Kpler name'].values
            #dfarrivel = EU_regas.get_regas_arrivel(kpler_terminal[0])
            #print(terminal)
            df = pd.concat([dfcapa[terminal], dfstorage[terminal], dfsendout[terminal]], axis=1)
            df.columns = ['capacity','storage','sendout']
            df = df.loc[str(today.year-9)+'-01-01':today, ['capacity','storage','sendout']]
            
            #print(df)
            #df.set_index(terminal, inplace=True)
            df['capacity'].fillna(method='bfill', inplace=True)
            #df = df.loc[:'2022-08-01']
            #df['Cargo arrival'].fillna(0, inplace=True)
            #print(df)
            df['year'] = df.index.year
            df['date'] = df.index.strftime('%m-%d')
            dfyoy = df.set_index(['year', 'date']).storage.unstack(0)
            #dfyoy['Average'] = dfyoy.loc[:,[today.year-6, today.year-5,today.year-4,today.year-3,today.year-2,today.year-1]].mean(axis=1)
            dfyoy['min'] = dfyoy.loc[:,[today.year-9,today.year-8,today.year-7,today.year-6, today.year-5,today.year-4,today.year-3,today.year-2,today.year-1]].min(axis=1)
            dfyoy['max'] = dfyoy.loc[:,[today.year-9,today.year-8,today.year-7,today.year-6, today.year-5,today.year-4,today.year-3,today.year-2,today.year-1]].max(axis=1)
           
           
            dfcapayoy = df.set_index(['year', 'date']).capacity.unstack(0)
                #print(dfcapayoy)
            
            dfyoy['max storage'] = dfcapayoy[today.year]
            dfyoy['max storage'] .fillna(method='ffill', inplace=True)
                
            #dfyoy['max storage'] = df['capacity']#df['storage'].max()
            dfyoy['% fill'] = dfyoy[today.year]/dfyoy['max storage']
            #dfcapayoy = df.set_index(['year', 'date']).capacity.unstack(0)
            #dfyoy['capa'] = dfcapayoy[today.year]
            #dfyoy['capa'].fillna(method = 'ffill', inplace=True)
            #print(dfyoy)
            
            EU_sendout.chart_storage(terminal, dfyoy)
    
    def chart_storage(terminal, dfyoy):
        
        today = datetime.date.today()
        
       
        index = pd.date_range(start='2020-01-01', end ='2020-12-31')
        #print(index)
        chart1 = make_subplots(specs=[[{"secondary_y": True}]])
        chart1.add_trace(go.Scatter(x=index, y=dfyoy['min'],
                            mode='lines',
                            name=str(today.year-9)+'-'+str(today.year-1)+' range',
                            fill='tonexty',
                            fillcolor='rgba(211,211,211,0)',
                            line_color='rgba(211,211,211,0)',
                            showlegend=False,
                            #fillpattern=dict(fillmode='overlay'),
                            #line=dict(color='black', dash='solid')
                            ),secondary_y=False)
        chart1.add_trace(go.Scatter(x=index, y=dfyoy['max'],
                            mode='lines',
                            name=str(today.year-9)+'-'+str(today.year-1)+' range',
                            fill='tonexty',
                            fillcolor='rgba(211,211,211,0.35)',
                            line_color='rgba(211,211,211,0)',
                            showlegend=True,
                            #fillpattern=dict(fillmode='overlay'),
                            #line=dict(color='black', dash='solid')
                            ),secondary_y=False)
        
        chart1.add_trace(go.Scatter(x=index, y=dfyoy.loc[:,today.year],
                            mode='lines',
                            name=str(today.year),
                            line=dict(color='red', dash='solid'),
                            ),secondary_y=False)
        chart1.add_trace(go.Scatter(x=index, y=dfyoy.loc[:,today.year-1],
                            mode='lines',
                            name=str(today.year-1),
                            line=dict(color='black', dash='solid'),
                            ),secondary_y=False)
        chart1.add_trace(go.Scatter(x=index, y=dfyoy.loc[:,today.year-2],
                            mode='lines',
                            name=str(today.year-2),
                            line=dict(color='grey', dash='solid'),
                            ),secondary_y=False)
        chart1.add_trace(go.Scatter(x=index, y=dfyoy.loc[:,'% fill'],
                            mode='lines',
                            name='% fill',
                            line=dict(color='blue', dash='dot'),
                            ),secondary_y=True)
        
        chart1.add_trace(go.Scatter(x=index, y=dfyoy.loc[:,'max storage'],
                            mode='lines',
                            name='max storage',
                            line=dict(color='black', dash='dot')
                            ),secondary_y=False)
        
        chart1.add_trace(go.Scatter(x=['2020-'+str(today.month)+'-'+str(today.day),'2020-'+str(today.month)+'-'+str(today.day)],y=[np.nanmin(dfyoy.values), np.nanmax(dfyoy.values)],
                   mode="lines",
                   line=dict(color='red', width=2, dash='dot'),
                   showlegend=False,
                   name='Today'
                   ))
        chart1.update_layout(barmode='relative', annotations=[dict(x='2020-'+str(today.month)+'-'+str(today.day), y=1, yref='paper',font=dict(color="red",size=14),showarrow=False, xanchor='left', text='Today'),
                                           ],)
        
        chart1.update_yaxes(
            title_text="Mcm",
                    showline=True, 
                    linewidth=1, 
                    linecolor='LightGrey', 
                    showgrid=True, 
                    gridwidth=1, 
                    gridcolor='LightGrey', secondary_y=False
                )
        chart1.update_yaxes(title_text="%", tickformat= ',.0%', secondary_y=True)
        chart1.update_layout(
             autosize=True,
             showlegend=True,
             #colorscale='RdBu',
             #legend=dict(x=0, y=-0.2),
             legend_orientation="h",
             title_text=terminal+' Storage '+str(today),
             #xaxis = dict(dtick="D"),
             xaxis_tickformat = '%m-%d',
             hovermode='x unified',
             plot_bgcolor = 'white',
             #yaxis = dict(showgrid=True, gridcolor='lightgrey', dtick="D2"),
             #xaxis_dtick=86400000*7
             #template='ggplot2'  
         )
        
        py.plot(chart1, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/LNG tracking/'+terminal+' storage.html', auto_open=False) 
        chart1.write_image('U:/Trading - Gas/LNG/LNG website/analysis/LNG tracking/'+terminal+' storage.png')

    def chart_sendout_market_data(dfstorage, dfcapa, dfsendout):
        
        today = datetime.date.today()
        
        #terminal_list = list(set(dfstorage.columns)&set(dfsendout.columns)&set(dfcapa.columns))
        #print(terminal_list)
        namemap = DBTOPD('PRD-DB-SQL-211','LNG', 'ana', '[CERegasMap]').sql_to_df()
        #namemap.set_index('Name', inplace=True)
        #print(namemap)
        #namemap_dict = namemap[['Name','Direction']].to_dict('records')
        namemap = namemap.loc[namemap[namemap['Variable'] == 'withdrawal'].index]
        #  print(namemap)
        #print(dfsendout)
        for i in namemap['Direction']:
            try:
                market_list = namemap.loc[namemap[namemap['Direction']==i].index,'Name'].to_list()
                dfsendoutmarket = dfsendout[market_list].sum(axis=1).to_frame()
                dfsendoutmarket.columns = ['sendout']
                #dfcapamarket = dfcapa[market_list].sum(axis=1).to_frame()
                #dfcapamarket.columns = ['capacity']
                #print(dfcapamarket.info())
                #dfcapamarket['capacity'].fillna(method='bfill', inplace=True)
                #df = pd.concat([dfcapamarket, dfsendoutmarket], axis=1)
                df = dfsendoutmarket.copy()
                df = df.loc[str(today.year-6)+'-01-01':today, ['sendout']]
                #df['capacity'].fillna(method='bfill', inplace=True)
                
                df['year'] = df.index.year
                df['date'] = df.index.strftime('%m-%d')
                dfyoy = df.set_index(['year', 'date']).sendout.unstack(0)
                dfyoy['Average'] = dfyoy.loc[:,[today.year-6, today.year-5,today.year-4,today.year-3,today.year-2,today.year-1]].mean(axis=1)
                dfyoy['min'] = dfyoy.loc[:,[today.year-6, today.year-5,today.year-4,today.year-3,today.year-2,today.year-1]].min(axis=1)
                dfyoy['max'] = dfyoy.loc[:,[today.year-6, today.year-5,today.year-4,today.year-3,today.year-2,today.year-1]].max(axis=1)
                dfyoy['max sendout'] = df['sendout'].max()
            except KeyError as e:
                print(e)
                pass
                
            #print(i)
            #print(dfyoy)
            
        
            
            
            EU_sendout.chart_sendout_market(i, dfyoy)
    
    def chart_sendout_market(market, dfyoy):
        
        today = datetime.date.today()
        
       
        index = pd.date_range(start='2020-01-01', end ='2020-12-31')
        
        chart1 = go.Figure()
        chart1.add_trace(go.Scatter(x=index, y=dfyoy['min'],
                            mode='lines',
                            name='6 yrs range',
                            fill='tonexty',
                            fillcolor='rgba(211,211,211,0)',
                            line_color='rgba(211,211,211,0)',
                            showlegend=False,
                            #fillpattern=dict(fillmode='overlay'),
                            #line=dict(color='black', dash='solid')
                            ))
        chart1.add_trace(go.Scatter(x=index, y=dfyoy['max'],
                            mode='lines',
                            name='6 yrs range',
                            fill='tonexty',
                            fillcolor='rgba(211,211,211,0.35)',
                            line_color='rgba(211,211,211,0)',
                            showlegend=True,
                            #fillpattern=dict(fillmode='overlay'),
                            #line=dict(color='black', dash='solid')
                            ))
        
        chart1.add_trace(go.Scatter(x=index, y=dfyoy.loc[:,today.year],
                            mode='lines',
                            name=str(today.year),
                            line=dict(color='red', dash='solid'),
                            ))
        chart1.add_trace(go.Scatter(x=index, y=dfyoy.loc[:,today.year-1],
                            mode='lines',
                            name=str(today.year-1),
                            line=dict(color='black', dash='solid'),
                            ))
        chart1.add_trace(go.Scatter(x=index, y=dfyoy.loc[:,today.year-2],
                            mode='lines',
                            name=str(today.year-2),
                            line=dict(color='grey', dash='solid'),
                            ))
        chart1.add_trace(go.Scatter(x=index, y=dfyoy.loc[:,'Average'],
                            mode='lines',
                            name='6 yrs Average',
                            line=dict(color='blue', dash='dot'),
                            ))
        
        chart1.add_trace(go.Scatter(x=index, y=dfyoy.loc[:,'max sendout'],
                            mode='lines',
                            name='max sendout',
                            line=dict(color='black', dash='dot')
                            ))
        chart1.add_trace(go.Scatter(x=['2020-'+str(today.month)+'-'+str(today.day),'2020-'+str(today.month)+'-'+str(today.day)],y=[np.nanmin(dfyoy.values), np.nanmax(dfyoy.values)],
                   mode="lines",
                   line=dict(color='red', width=2, dash='dot'),
                   showlegend=False,
                   name='Today'
                   ))
        chart1.update_layout(barmode='relative', annotations=[dict(x='2020-'+str(today.month)+'-'+str(today.day), y=1, yref='paper',font=dict(color="red",size=14),showarrow=False, xanchor='left', text='Today'),
                                           ],)
               
        chart1.update_yaxes(
            title_text="mcm/d",
                    showline=True, 
                    linewidth=1, 
                    linecolor='LightGrey', 
                    showgrid=True, 
                    gridwidth=1, 
                    gridcolor='LightGrey'
                )
        
        chart1.update_layout(
             autosize=True,
             showlegend=True,
             #colorscale='RdBu',
             #legend=dict(x=0, y=-0.2),
             legend_orientation="h",
             title_text=market+' Sendout (Mcm/d) '+str(today),
             #xaxis = dict(dtick="D"),
             xaxis_tickformat = '%m-%d',
             hovermode='x unified',
             plot_bgcolor = 'white',
             #yaxis = dict(showgrid=True, gridcolor='lightgrey', dtick="D2"),
             #xaxis_dtick=86400000*7
             #template='ggplot2'  
         )
        
        py.plot(chart1, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/LNG tracking/'+market+'.html', auto_open=False) 
        chart1.write_image('U:/Trading - Gas/LNG/LNG website/analysis/LNG tracking/'+market+' .png')

    def chart_storage_market_data(dfstorage, dfcapa, dfsendout):
        
        today = datetime.date.today()
        
        #terminal_list = list(set(dfstorage.columns)&set(dfsendout.columns)&set(dfcapa.columns))
        #print(terminal_list)
        namemap = DBTOPD('PRD-DB-SQL-211','LNG', 'ana', '[CERegasMap]').sql_to_df()
        #namemap.set_index('Name', inplace=True)
        #print(dfcapa.loc['2022-11-14':,['Dragon','Isle Of Grain','South Hook']])
        #namemap_dict = namemap[['Name','Direction']].to_dict('records')
        namemap = namemap.loc[namemap[namemap['Variable'] == 'stock'].index]
        #print(namemap)
        #for i in ['GB']:
        for i in namemap['Direction']:
            
            try:
                market_list = namemap.loc[namemap[namemap['Direction']==i].index,'Name'].to_list()
                dfstoragemarket = dfstorage[market_list].sum(axis=1).to_frame()
                dfstoragemarket.columns = ['storage']
                dfcapamarket = dfcapa[market_list].sum(axis=1).to_frame()
                dfcapamarket.columns = ['capacity']
                dfcapamarket['capacity'].fillna(method='ffill', inplace=True)
                #print(dfcapamarket)
                df = pd.concat([dfcapamarket, dfstoragemarket], axis=1)
                #df = dfstoragemarket.copy()
                df = df.loc[str(today.year-9)+'-01-01':today, ['storage','capacity']]
                #df['capacity'].fillna(method='bfill', inplace=True)
                #print(df)
                df['year'] = df.index.year
                df['date'] = df.index.strftime('%m-%d')
                dfyoy = df.set_index(['year', 'date']).storage.unstack(0)
                #print(dfyoy)
                dfyoy['min'] = dfyoy.loc[:,[today.year-9,today.year-8,today.year-7,today.year-6, today.year-5,today.year-4,today.year-3,today.year-2,today.year-1]].min(axis=1)
                dfyoy['max'] = dfyoy.loc[:,[today.year-9,today.year-8,today.year-7,today.year-6, today.year-5,today.year-4,today.year-3,today.year-2,today.year-1]].max(axis=1)
                #print(dfyoy)
                
                dfcapayoy = df.set_index(['year', 'date']).capacity.unstack(0)
                #print(dfcapayoy)
                
                dfyoy['max storage'] = dfcapayoy[today.year]
                dfyoy['max storage'] .fillna(method='ffill', inplace=True)
                dfyoy['% fill'] = dfyoy[today.year]/dfyoy['max storage']
            
                #print(dfyoy)
            
                EU_sendout.chart_storage_market(i, dfyoy)
            except KeyError as e:
                print (e)
                pass
            
    
    def chart_storage_market(market, dfyoy):
        
        today = datetime.date.today()
        
       
        index = pd.date_range(start='2020-01-01', end ='2020-12-31')
        
        chart1 = make_subplots(specs=[[{"secondary_y": True}]])
        chart1.add_trace(go.Scatter(x=index, y=dfyoy['min'],
                            mode='lines',
                            name=str(today.year-9)+'-'+str(today.year-1)+' range',
                            fill='tonexty',
                            fillcolor='rgba(211,211,211,0)',
                            line_color='rgba(211,211,211,0)',
                            showlegend=False,
                            #fillpattern=dict(fillmode='overlay'),
                            #line=dict(color='black', dash='solid')
                            ),secondary_y=False)
        chart1.add_trace(go.Scatter(x=index, y=dfyoy['max'],
                            mode='lines',
                            name=str(today.year-9)+'-'+str(today.year-1)+' range',
                            fill='tonexty',
                            fillcolor='rgba(211,211,211,0.35)',
                            line_color='rgba(211,211,211,0)',
                            showlegend=True,
                            #fillpattern=dict(fillmode='overlay'),
                            #line=dict(color='black', dash='solid')
                            ),secondary_y=False)
        
        chart1.add_trace(go.Scatter(x=index, y=dfyoy.loc[:,today.year],
                            mode='lines',
                            name=str(today.year),
                            line=dict(color='red', dash='solid'),
                            ),secondary_y=False)
        chart1.add_trace(go.Scatter(x=index, y=dfyoy.loc[:,today.year-1],
                            mode='lines',
                            name=str(today.year-1),
                            line=dict(color='black', dash='solid'),
                            ),secondary_y=False)
        chart1.add_trace(go.Scatter(x=index, y=dfyoy.loc[:,today.year-2],
                            mode='lines',
                            name=str(today.year-2),
                            line=dict(color='grey', dash='solid'),
                            ),secondary_y=False)
        chart1.add_trace(go.Scatter(x=index, y=dfyoy.loc[:,'% fill'],
                            mode='lines',
                            name='% fill',
                            line=dict(color='blue', dash='dot'),
                            ),secondary_y=True)
        
        chart1.add_trace(go.Scatter(x=index, y=dfyoy.loc[:,'max storage'],
                            mode='lines',
                            name='max storage',
                            line=dict(color='black', dash='dot')
                            ),secondary_y=False)
        
        chart1.add_trace(go.Scatter(x=['2020-'+str(today.month)+'-'+str(today.day),'2020-'+str(today.month)+'-'+str(today.day)],y=[np.nanmin(dfyoy.values), np.nanmax(dfyoy.values)],
                   mode="lines",
                   line=dict(color='red', width=2, dash='dot'),
                   showlegend=False,
                   name='Today'
                   ))
        chart1.update_layout(barmode='relative', annotations=[dict(x='2020-'+str(today.month)+'-'+str(today.day), y=1, yref='paper',font=dict(color="red",size=14),showarrow=False, xanchor='left', text='Today'),
                                           ],) 
        chart1.update_yaxes(
            title_text="Mcm",
                    showline=True, 
                    linewidth=1, 
                    linecolor='LightGrey', 
                    showgrid=True, 
                    gridwidth=1, 
                    gridcolor='LightGrey', secondary_y=False
                )
        chart1.update_yaxes(title_text="%", tickformat= ',.0%', secondary_y=True)
        chart1.update_layout(
             autosize=True,
             showlegend=True,
             #colorscale='RdBu',
             #legend=dict(x=0, y=-0.2),
             legend_orientation="h",
             title_text=market+' Storage '+str(today),
             #xaxis = dict(dtick="D"),
             xaxis_tickformat = '%m-%d',
             hovermode='x unified',
             plot_bgcolor = 'white',
             #yaxis = dict(showgrid=True, gridcolor='lightgrey', dtick="D2"),
             #xaxis_dtick=86400000*7
             #template='ggplot2'  
         )
        
        py.plot(chart1, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/LNG tracking/'+market+' storage.html', auto_open=False)
        chart1.write_image('U:/Trading - Gas/LNG/LNG website/analysis/LNG tracking/'+market+' storage.png')


    def table_data(dfstorage, dfcapa, dfsendout):
        #print(dfsendout)
        today = datetime.date.today()
        
    
        index = [str(today), str(today-relativedelta(days=1)), str(today-relativedelta(days=7)),str(today-relativedelta(months=1)), str(today-relativedelta(years=1))]
        #terminal
        dfsendouttable = pd.DataFrame(index = index, columns=dfsendout.columns)
        dfsendouttable.loc[index[0]] = dfsendout.loc[index[0]]
        dfsendouttable.loc[index[1]] = dfsendout.loc[index[1]]
        dfsendouttable.loc[index[2]] = dfsendout.loc[index[2]]
        dfsendouttable.loc[index[3]] = dfsendout.loc[index[3]]
        dfsendouttable.loc[index[4]] = dfsendout.loc[index[4]]
        dfsendouttable.loc['M Ave.'] = dfsendout.loc[str(today.year)+'-'+str(today.month)+'-01':today].sum()/today.day
        dfsendouttable.loc['M-1 Ave.'] = dfsendout.loc[str((today-relativedelta(months=1)).year)+'-'+str((today-relativedelta(months=1)).month)+'-01':str((today-relativedelta(months=1)).year)+'-'+str((today-relativedelta(months=1)).month)+'-'+str(calendar.monthrange((today-relativedelta(months=1)).year,(today-relativedelta(months=1)).month)[1])].sum()/calendar.monthrange((today-relativedelta(months=1)).year,(today-relativedelta(months=1)).month)[1]
        dfsendouttable.loc['Y-1 Ave.'] = dfsendout.loc[str((today-relativedelta(years=1)).year)+'-'+str((today-relativedelta(years=1)).month)+'-01':str((today-relativedelta(years=1)).year)+'-'+str((today-relativedelta(years=1)).month)+'-'+str(calendar.monthrange((today-relativedelta(years=1)).year,(today-relativedelta(years=1)).month)[1])].sum()/calendar.monthrange((today-relativedelta(years=1)).year,(today-relativedelta(years=1)).month)[1]
    
        #print(dfsendouttable.info())
        dfsendouttable=dfsendouttable.astype('float')
        dfsendouttable = dfsendouttable.round(2)
        #print(dfsendouttable)
        
        dfstoragetable = pd.DataFrame(index = index, columns=dfstorage.columns)
        dfstoragetable.loc[index[0]] = dfstorage.loc[index[0]]
        dfstoragetable.loc[index[1]] = dfstorage.loc[index[1]]
        dfstoragetable.loc[index[2]] = dfstorage.loc[index[2]]
        dfstoragetable.loc[index[3]] = dfstorage.loc[index[3]]
        dfstoragetable.loc[index[4]] = dfstorage.loc[index[4]]
        dfstoragetable = dfstoragetable.round(2)
        #print(dfstoragetable)
        EU_sendout.table(dfsendouttable, dfstoragetable)
        
        #market 
        dfsendoutmarkettable = pd.DataFrame(index = index)
        dfstoragemarkettable = pd.DataFrame(index = index)
        
        namemap = DBTOPD('PRD-DB-SQL-211','LNG', 'ana', '[CERegasMap]').sql_to_df()
        
        namemap = namemap.loc[namemap[namemap['Variable'] == 'withdrawal'].index]

        for i in namemap['Direction']:
            try:
                market_list = namemap.loc[namemap[namemap['Direction']==i].index,'Name'].to_list()
                dfsendoutmarket = dfsendout[market_list].sum(axis=1).to_frame()
                dfsendoutmarket.columns = ['sendout']
                
                df = dfsendoutmarket.copy()
                dfsendoutmarkettable.loc[index[0],i] = df.loc[index[0],'sendout']
                dfsendoutmarkettable.loc[index[1],i] = df.loc[index[1],'sendout']
                dfsendoutmarkettable.loc[index[2],i] = df.loc[index[2],'sendout']
                dfsendoutmarkettable.loc[index[3],i] = df.loc[index[3],'sendout']
                dfsendoutmarkettable.loc[index[4],i] = df.loc[index[4],'sendout']
                dfsendoutmarkettable.loc['M Ave.',i] = df.loc[str(today.year)+'-'+str(today.month)+'-01':today,'sendout'].sum()/today.day
                dfsendoutmarkettable.loc['M-1 Ave.',i] = df.loc[str((today-relativedelta(months=1)).year)+'-'+str((today-relativedelta(months=1)).month)+'-01':str((today-relativedelta(months=1)).year)+'-'+str((today-relativedelta(months=1)).month)+'-'+str(calendar.monthrange((today-relativedelta(months=1)).year,(today-relativedelta(months=1)).month)[1]),'sendout'].sum()/calendar.monthrange((today-relativedelta(months=1)).year,(today-relativedelta(months=1)).month)[1]
                dfsendoutmarkettable.loc['Y-1 Ave.',i] = df.loc[str((today-relativedelta(years=1)).year)+'-'+str((today-relativedelta(years=1)).month)+'-01':str((today-relativedelta(years=1)).year)+'-'+str((today-relativedelta(years=1)).month)+'-'+str(calendar.monthrange((today-relativedelta(years=1)).year,(today-relativedelta(years=1)).month)[1]),'sendout'].sum()/calendar.monthrange((today-relativedelta(years=1)).year,(today-relativedelta(years=1)).month)[1]
                
                
                dfstoragemarket = dfstorage[market_list].sum(axis=1).to_frame()
                dfstoragemarket.columns = ['storage']
                
                df1 = dfstoragemarket.copy()
                dfstoragemarkettable.loc[index[0],i] = df1.loc[index[0],'storage']
                dfstoragemarkettable.loc[index[1],i] = df1.loc[index[1],'storage']
                dfstoragemarkettable.loc[index[2],i] = df1.loc[index[2],'storage']
                dfstoragemarkettable.loc[index[3],i] = df1.loc[index[3],'storage']
                dfstoragemarkettable.loc[index[4],i] = df1.loc[index[4],'storage']
                
                
            except KeyError as e:
                print('752 ',e)
                pass
        
        dfsendoutmarkettable['All EU'] = dfsendoutmarkettable.sum(axis=1)
        dfstoragemarkettable['All EU'] = dfstoragemarkettable.sum(axis=1)
        dfsendoutmarkettable['EU Desk'] = dfsendoutmarkettable[['BE','FR','GB','NL','DE','ES','IT','PL']].sum(axis=1)
        dfstoragemarkettable['EU DEsk'] = dfstoragemarkettable[['BE','FR','GB','NL','DE','ES','IT','PL']].sum(axis=1)
        #print(dfsendoutmarkettable)
        dfsendoutmarkettable = dfsendoutmarkettable.round(2)
        dfstoragemarkettable = dfstoragemarkettable.round(2)
        EU_sendout.markettable(dfsendoutmarkettable, dfstoragemarkettable)
        
    
    def table(dfsendouttable, dfstoragetable):
        
        fig1 = go.Figure(data=[go.Table(
                                        header=dict(values=[' ', 'Mcm/d'] + list(dfsendouttable.columns),
                                                    fill_color='paleturquoise',
                                                    align='center'),
                                        cells=dict(values=[['Today','Today-1','Today-7','Month-1','Year-1','Month Ave.','Month-1 Ave.','Year-1 Ave.']]+[dfsendouttable.index] + [dfsendouttable[pm] for pm in dfsendouttable.columns],
                                                   fill_color='lavender',
                                                   #font=dict(color = ['black','black']+dfcolor.values.tolist()),
                                                   align='center'))
                                    ])
    
        py.plot(fig1, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/LNG tracking/sendouttable.html', auto_open=False)
        fig1.write_image('U:/Trading - Gas/LNG/LNG website/analysis/LNG tracking/sendouttable.png')
        
        fig2 = go.Figure(data=[go.Table(
                                        header=dict(values=[' ', 'Mcm/d'] + list(dfstoragetable.columns),
                                                    fill_color='paleturquoise',
                                                    align='center'),
                                        cells=dict(values=[['Today','Today-1','Today-7','Month-1','Year-1']]+[dfstoragetable.index] + [dfstoragetable[pm] for pm in dfstoragetable.columns],
                                                   fill_color='lavender',
                                                   #font=dict(color = ['black','black']+dfcolor.values.tolist()),
                                                   align='center'))
                                    ])
    
        py.plot(fig2, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/LNG tracking/storagetable.html', auto_open=False)
        fig2.write_image('U:/Trading - Gas/LNG/LNG website/analysis/LNG tracking/storagetable.png')
        
    def markettable(dfsendoutmarkettable, dfstoragemarkettable):
        
        fig1 = go.Figure(data=[go.Table(
                                        header=dict(values=['Mcm/d', 'Date'] + list(dfsendoutmarkettable.columns),
                                                    fill_color='paleturquoise',
                                                    align='center'),
                                        cells=dict(values=[['Today','Today-1','Today-7','Month-1','Year-1','Month Ave.','Month-1 Ave.','Year-1 Ave.']]+[dfsendoutmarkettable.index] + [dfsendoutmarkettable[pm] for pm in dfsendoutmarkettable.columns],
                                                   fill_color='lavender',
                                                   #font=dict(color = ['black','black']+dfcolor.values.tolist()),
                                                   align='center'))
                                    ])
    
        py.plot(fig1, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/LNG tracking/sendoutmarkettable.html', auto_open=False)
        fig1.write_image('U:/Trading - Gas/LNG/LNG website/analysis/LNG tracking/sendoutmarkettable.png')
        
        fig2 = go.Figure(data=[go.Table(
                                        header=dict(values=['Mcm/d', 'Date'] + list(dfstoragemarkettable.columns),
                                                    fill_color='paleturquoise',
                                                    align='center'),
                                        cells=dict(values=[['Today','Today-1','Today-7','Month-1','Year-1']]+[dfstoragemarkettable.index] + [dfstoragemarkettable[pm] for pm in dfstoragemarkettable.columns],
                                                   fill_color='lavender',
                                                   #font=dict(color = ['black','black']+dfcolor.values.tolist()),
                                                   align='center'))
                                    ])
    
        py.plot(fig2, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/LNG tracking/storagemarkettable.html', auto_open=False)
        fig2.write_image('U:/Trading - Gas/LNG/LNG website/analysis/LNG tracking/storagemarkettable.png')
        
        
    def markettable_p(dfcapa, dfstorage):
        
        #print(dfstorage)
        #print(dfcapa)
        today = datetime.date.today()
        index = [str(today), str(today-relativedelta(days=1)), str(today-relativedelta(days=7)),str(today-relativedelta(months=1)), str(today-relativedelta(years=1))]
        
        namemap = DBTOPD('PRD-DB-SQL-211','LNG', 'ana', '[CERegasMap]').sql_to_df()
        
        namemap = namemap.loc[namemap[namemap['Variable'] == 'withdrawal'].index]
        dfstoragemarket = pd.DataFrame()
        dfcapamarket = pd.DataFrame()
        
        #print(namemap)
        for i in namemap['Direction']:
            try:
                market_list = namemap.loc[namemap[namemap['Direction']==i].index,'Name'].to_list()
                #print(market_list)
                dfstoragemarket[i] = dfstorage[market_list].sum(axis=1)#.to_frame()
                dfcapamarket[i] = dfcapa[market_list].sum(axis=1)
                
            except KeyError as e:
                print('840 ',e)
                pass
           
        
        
        

        #dfstorageterminalp['Adriatic'] = dfstorageterminalp['Adriatic'].apply(lambda x: format(x,'.2%'))
        
        #print(dfstoragemarket)
        dfstoragemarket['All EU'] = dfstoragemarket.sum(axis=1)
        dfcapamarket['All EU'] = dfcapamarket.sum(axis=1)
        dfstoragemarket['EU Desk'] = dfstoragemarket[['BE','FR','GB','NL','DE','ES','IT','PL']].sum(axis=1)
        dfcapamarket['EU Desk'] = dfcapamarket[['BE','FR','GB','NL','DE','ES','IT','PL']].sum(axis=1)
        #print(dfstoragemarket)
        #print(dfcapamarket)
        dfstoragemarketp = (dfstoragemarket/dfcapamarket)#.apply(lambda x: format(x,'.2%'))
        dfstoragemarketp = dfstoragemarketp.loc[index]
        dfstoragemarketp.index = dfstoragemarketp.index.date
        #print(dfstoragemarketp)
       
        dfstorageterminalp = (dfstorage/dfcapa)
        #print(dfstorageterminalp)
        dfstorageterminalp = dfstorageterminalp.loc[index]
        #dfstorageterminalp = pd.concat([dfstorageterminalp, dfstoragemarketp[['NWE','All EU']]], axis=1)
        #dfstorageterminalp.loc[:,'NWE'] = dfstoragemarketp.loc[:,'NWE'].copy()
        #dfstorageterminalp.loc[:,'All EU'] = dfstoragemarketp.loc[:,'All EU'].copy()
        #dfstorageterminalp.fillna(0, inplace=True)
        dfstorageterminalp.index = dfstorageterminalp.index.date    
        dfstorageterminalp = dfstorageterminalp[dfstorage.columns]
        #print(dfstorageterminalp)
        
        #dfp = dfstorage/dfcapa
        
        fig1 = go.Figure(data=[go.Table(
                                        header=dict(values=[' ', 'Mcm/d'] + list(dfstorageterminalp.columns),
                                                    fill_color='paleturquoise',
                                                    align='center'),
                                        cells=dict(values=[['Today','Today-1','Today-7','Month-1','Year-1']]+[dfstorageterminalp.index] + [dfstorageterminalp[pm] for pm in dfstorageterminalp.columns],
                                                   fill_color='lavender',
                                                   #font=dict(color = ['black','black']+dfcolor.values.tolist()),
                                                   align='center', format=["","",".2%"]))
                                    ])
    
        py.plot(fig1, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/LNG tracking/storageterminaltable%.html', auto_open=False)
        fig1.write_image('U:/Trading - Gas/LNG/LNG website/analysis/LNG tracking/storageterminaltable%.png')
        
        fig2 = go.Figure(data=[go.Table(
                                        header=dict(values=[' ', 'Mcm/d'] + list(dfstoragemarketp.columns),
                                                    fill_color='paleturquoise',
                                                    align='center'),
                                        cells=dict(values=[['Today','Today-1','Today-7','Month-1','Year-1']]+[dfstoragemarketp.index] + [dfstoragemarketp[pm] for pm in dfstoragemarketp.columns],
                                                   fill_color='lavender',
                                                   #font=dict(color = ['black','black']+dfcolor.values.tolist()),
                                                   align='center',format=["","",".2%"]))
                                    ])
    
        py.plot(fig2, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/LNG tracking/storagemarkettable%.html', auto_open=False)
        fig2.write_image('U:/Trading - Gas/LNG/LNG website/analysis/LNG tracking/storagemarkettable%.png')
        

    def update():
        dfstorage = EU_sendout.get_regas_stock()   
        dfcapa = EU_sendout.get_regas_capa()
        dfsendout = EU_sendout.get_regas_sendout()
        EU_sendout.chart_sendout_data(dfstorage, dfcapa, dfsendout)
        EU_sendout.chart_storage_data(dfstorage, dfcapa, dfsendout)
        EU_sendout.chart_sendout_market_data(dfstorage, dfcapa, dfsendout)
        EU_sendout.chart_storage_market_data(dfstorage, dfcapa, dfsendout)
        EU_sendout.table_data(dfstorage, dfcapa, dfsendout)
        EU_sendout.markettable_p(dfcapa, dfstorage)
        
#EU_sendout.update()