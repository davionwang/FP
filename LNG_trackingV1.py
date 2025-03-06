# -*- coding: utf-8 -*-
"""
Created on Mon May 22 10:50:11 2023

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
import pyodbc

#import CEtools

pd.set_option('display.max_columns',20)
from DBtoDF import DBTOPD

class EU_LNG_Tracker():
    
    def get_kpler_data():
        
        kplerhist = DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'DemandBasinHist').sql_to_df()
        dfkplerhist = kplerhist[['Date','EurDesk','OtherEur']].copy()
        dfkplerhist.set_index('Date', inplace=True)
        dfkplerhist['EU'] = dfkplerhist.sum(axis=1)
        
        kplerhistUK = DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'DemandCountryHist').sql_to_df()
        dfkplerhistUK = kplerhistUK[['Date','United Kingdom']].copy()
        dfkplerhistUK.set_index('Date', inplace=True)

        ma10 = DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'BasinDemand').sql_to_df()  
        dfma10 = ma10[['Date', 'EurDesk', 'OtherEur', 'United Kingdom']].copy()
        dfma10.set_index('Date', inplace=True)
        dfma10['EU desk'] = dfma10[['EurDesk', 'OtherEur']].sum(axis=1)
        
        ma10UK = DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'DemandCountry').sql_to_df()
        dfma10UK = ma10UK[['Date','United Kingdom']].copy()
        dfma10UK.set_index('Date', inplace=True)
        
        return dfkplerhist, dfkplerhistUK, dfma10, dfma10UK
        
        
    def create_df_etademand_MA ():
    
        
        today = datetime.datetime.today()
        Kpler=DBTOPD('PRD-DB-SQL-211','LNG', 'dbo', 'KplerLNGTrades') #new DB
        dfkpler=Kpler.sql_to_df()
        #get supply and demand df
        demand_columns=['StartDestination','CountryDestination','VolumeDestinationM3']
        #df_supply = df_supply.sort_values(by = 'EndOrigin')
        df_demand = dfkpler[demand_columns]
        etademandstart = df_demand.copy()
        etademandstart = etademandstart.sort_values(by = 'StartDestination')
        etademandstart.set_index('StartDestination', inplace=True)
        etademandstart = etademandstart.loc[:today.strftime('%Y-%m-%d')]
        etademandstart.reset_index(inplace=True)
        etademandstart.columns=['Date','ForecastedDestination','VolumeDestinationM3']
        #print(etademandstart)
        demand_columns_eta=['EtaDestination','CountryDestination','VolumeDestinationM3']
        #df_supply = df_supply.sort_values(by = 'EndOrigin')
        df_demand_eta = dfkpler[demand_columns_eta]
        df_demand_eta = df_demand_eta.sort_values(by = 'EtaDestination')
        df_demand_eta.set_index('EtaDestination', inplace=True)
        df_demand_eta = df_demand_eta.loc[(today+relativedelta(days=0)).strftime('%Y-%m-%d'):]
        df_demand_eta.reset_index(inplace=True)
        df_demand_eta.columns=['Date','ForecastedDestination','VolumeDestinationM3']
        #print(df_demand_eta)
        df_demand_eta = pd.concat([etademandstart,df_demand_eta])
        print(df_demand_eta)
        
        df_demand_eta_pivot = pd.pivot_table(df_demand_eta, values='VolumeDestinationM3', index='Date', columns='ForecastedDestination')
        dfeta = df_demand_eta_pivot.resample('D').sum()*0.000612
        dfeta = dfeta.loc[today:]
        
        #no cyp[rus]
        dfeta["EurDesk"] = dfeta[['France','Germany','United Kingdom','Belgium','Netherlands','Spain','Italy','Poland']].sum(axis=1)
        dfeta['EU'] = dfeta["EurDesk"] + dfeta[['Turkey','Lithuania','Croatia','Portugal','Greece','Poland','Finland','Malta','Norway','Sweden']].sum(axis=1)
        print(dfeta.loc['2023-05-30':])
        
        
        return dfeta   
    
    def get_data_eta():
        
        sqlConnScrape = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER=prd-db-sql-211;Trusted_Connection=yes')
        
        sqleta = '''
                    select k.EtaSourceDestination, k.InstallationOrigin, k.Vessel, k.charterer, k.IdTrade, k.TradeStatus, k.VolumeDestinationM3, k.EndOrigin, k.EtaDestination, k.Destination, k.ContinentDestination, k.SubcontinentDestination, k.ZoneDestination, k.CountryDestination, k.InstallationDestination
                    from	LNG.dbo.KplerLNGTrades k
                    where	(k.ContinentDestination = 'Europe' or k.Destination = 'Europe' or k.ForecastedDestination = 'Destination')
                    		and k.TradeStatus <> 'Delivered'
                    		and k.EtaSourceDestination <> 'Model'
                    order by k.EtaDestination
                '''
                
        dfeta = pd.read_sql(sqleta, sqlConnScrape)
        
        dfeta_pivot = pd.pivot_table(dfeta,values = 'VolumeDestinationM3', index = 'EtaDestination', columns='CountryDestination')
        dfeta_pivot = dfeta_pivot*0.000612
        dfeta_pivot = dfeta_pivot.resample('D').sum()
        #print(dfeta_pivot)
        for i in ['United Kingdom','France','Germany','Belgium','Netherlands','Spain','Italy','Poland']:
            if i not in dfeta_pivot.columns:
                dfeta_pivot[i]=0
            
            
        dfeta = pd.DataFrame(index = dfeta_pivot.index, columns=['EU','EurDesk','United Kingdom'])
        dfeta['EU'] = dfeta_pivot.sum(axis=1)
        #print(dfeta)
        dfeta['EurDesk'] = dfeta_pivot[['United Kingdom','France','Germany','Belgium','Netherlands','Spain','Italy','Poland']].sum(axis=1)
        dfeta['United Kingdom'] = dfeta_pivot[['United Kingdom']]
        #print(dfeta)
        
        return dfeta
        
    def get_data_hist():
        
        sqlConnScrape = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER=prd-db-sql-211;Trusted_Connection=yes')
        
        sqlhist = '''
                        DECLARE @today DATETIME SET @today = DATEADD(DD, DATEDIFF(DD, 0, GETDATE()), 0)
                        declare @startdate datetime
                        SET @startdate =DATEADD(day, -31, @today)
                        declare @enddate datetime 
                        SET @enddate = DATEADD(day, 30, @today)
                        SELECT [Vessel],[CountryOrigin],[InstallationOrigin],[EndOrigin],[CountryDestination],[ZoneDestination],[ContinentDestination],
                        [InstallationDestination],[StartDestination],[VolumeDestinationM3],[ForecastedDestination],[ForecastedETA],
                        [Charterer],[Link1Type],[EtaSourceDestination]
                        FROM LNG.[dbo].[KplerLNGTrades]
                        where [StartDestination] >=@startdate and [StartDestination]<=@enddate and [ContinentDestination] ='Europe' 
                        order by [StartDestination]
                '''
                
        dfhist = pd.read_sql(sqlhist, sqlConnScrape)
        
        dfhist_pivot = pd.pivot_table(dfhist,values = 'VolumeDestinationM3', index = 'StartDestination', columns='CountryDestination')
        dfhist_pivot = dfhist_pivot*0.000612
        dfhist_pivot = dfhist_pivot.resample('D').sum()
        #print(dfhist_pivot)    
        
        dfhist = pd.DataFrame(index = dfhist_pivot.index, columns=['EU','NWE','UK'])
        dfhist['EU'] = dfhist_pivot.sum(axis=1)
        dfhist['EurDesk'] = dfhist_pivot[['United Kingdom','France','Germany','Belgium','Netherlands','Spain','Italy','Poland']].sum(axis=1)
        dfhist['United Kingdom'] = dfhist_pivot[['United Kingdom']]
        
        #print(dfhist)
        
        return dfhist
    
    def get_data_unknow():
        
        
        sqlConnScrape = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER=prd-db-sql-211;Trusted_Connection=yes')
        
        sqlunknow = '''
                        select k.EtaSourceDestination, k.InstallationOrigin, k.Vessel, k.charterer, k.IdTrade, k.TradeStatus, k.VolumeDestinationM3,
                        k.EndOrigin, k.EtaDestination, k.Destination, k.ContinentDestination, k.SubcontinentDestination, k.ZoneDestination, 
                        k.CountryDestination, k.InstallationDestination
                        from LNG.dbo.KplerLNGTrades k
                        where (k.ContinentDestination = 'Europe' or k.Destination = 'Europe' or k.ForecastedDestination = 'Destination')
                        and k.TradeStatus <> 'Delivered'
                        and k.EtaSourceDestination <> 'Model'
                        and k.Destination in ('Europe','France','Germany','United Kingdom','Belgium','Netherlands','Spain','Italy','Turkey','Lithuania','Croatia','Poland','Portugal','Greece','Poland','Finland','Malta','Norway','Sweden')
                                              order by k.EtaDestination

                    '''
                
        dfunknow = pd.read_sql(sqlunknow, sqlConnScrape)
        
        dfunknow_pivot = pd.pivot_table(dfunknow,values = 'VolumeDestinationM3', index = 'EtaDestination', columns='Destination')
        dfunknow_pivot = dfunknow_pivot*0.000612
        dfunknow_pivot = dfunknow_pivot.resample('D').sum()
        #print(dfunknow_pivot)    
        return dfunknow_pivot
    
    def get_desk():
        
        desk = DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'BasinDemand').sql_to_df()
        
        dfdesk = desk[['Date', 'EurDesk Desk', 'OtherEur Desk', 'United Kingdom Desk']].copy()
        
        dfdesk.set_index('Date', inplace=True)
        
        dfdesk['EU desk'] = dfdesk[['EurDesk Desk', 'OtherEur Desk']].sum(axis=1)
        
        #print(dfdesk)
        return dfdesk
     
    def full_data(dfkplerhist, dfunknow, dfeta,dfdesk):
       
        #print(dfkplerhist.round(2))
        today = datetime.date.today()
        start = today - relativedelta(days=30)
        end = today + relativedelta(days=30)
        
        df = pd.DataFrame(index = pd.date_range(start = start, end = end), columns = ['EU', 'EurDesk','United Kingdom'])
        #print(df.index)
        df.loc[start:today - relativedelta(days=1),['EU','EurDesk','United Kingdom']] = dfkplerhist.loc[start:today,['EU','EurDesk','United Kingdom']].round(2)
        #print(dfeta.loc[today.strftime('%Y-%m-%d'),['EU', 'EurDesk','United Kingdom']])
        #print(dfkplerhist.loc[today.strftime('%Y-%m-%d'),['EU', 'EurDesk','United Kingdom']])
        #print(df.loc[today.strftime('%Y-%m-%d'),['EU', 'EurDesk','United Kingdom']])
        df.loc[today.strftime('%Y-%m-%d'),['EU', 'EurDesk','United Kingdom']] = dfkplerhist.loc[today.strftime('%Y-%m-%d'),['EU', 'EurDesk','United Kingdom']] + dfeta.loc[today.strftime('%Y-%m-%d'),['EU', 'EurDesk','United Kingdom']]
        df.loc[today+relativedelta(days=1):,['EU', 'EurDesk','United Kingdom']] =  dfeta.loc[today:,['EU', 'EurDesk','United Kingdom']]
        df[['EU desk', 'EurDesk desk','United Kingdom desk']] = dfdesk.loc[start:end,['EU desk','EurDesk Desk','United Kingdom Desk']]
        #print(df)
        
        return df
    
    def chart_data(df):
        
        today = datetime.date.today()

        df[['EU MA10','EurDesk MA10','United Kingdom MA10']] = df[['EU','EurDesk','United Kingdom']].rolling(10).mean()
        df[['EU MTD','EurDesk MTD','United Kingdom MTD']] = df.loc[str(today.year)+'-'+str(today.month)+'-01':today,['EU','EurDesk','United Kingdom']].expanding().mean()
        df.loc[today:today+relativedelta(days=6),['EU ETA7','EurDesk ETA7','United Kingdom ETA7']] = df.loc[today+relativedelta(days=1):today+relativedelta(days=7),['EU','EurDesk','United Kingdom']].mean().values
        df.loc[today-relativedelta(days=7):today+relativedelta(days=6),['EU HETA7','EurDesk HETA7','United Kingdom HETA7']] = df.loc[today-relativedelta(days=7):today+relativedelta(days=6),['EU','EurDesk','United Kingdom']].mean().values
        df.loc[str(today.year)+'-'+str(today.month)+'-01':today+relativedelta(days=6),['EU MTDETA','EurDesk MTDETA','United Kingdom MTDETA']] = df.loc[str(today.year)+'-'+str(today.month)+'-01':today+relativedelta(days=6),['EU','EurDesk','United Kingdom']].mean().values
        
        #print((df[['EU MA10','EurDesk MA10']] - df[['EU desk','EurDesk desk']].values).head(20))
        df[['EU MA10 delta','EurDesk MA10 delta','United Kingdom MA10 delta']] = df[['EU MA10','EurDesk MA10','United Kingdom MA10']] - df[['EU desk', 'EurDesk desk','United Kingdom desk']].values
        df[['EU MTD delta','EurDesk MTD delta','United Kingdom MTD delta']] = df[['EU MTD','EurDesk MTD','United Kingdom MTD']] - df[['EU desk', 'EurDesk desk','United Kingdom desk']].values
        df[['EU ETA7 delta','EurDesk ETA7 delta','United Kingdom ETA7 delta']] = df[['EU ETA7','EurDesk ETA7','United Kingdom ETA7']] - df[['EU desk', 'EurDesk desk','United Kingdom desk']].values
        df[['EU HETA7 delta','EurDesk HETA7 delta','United Kingdom HETA7 delta']] = df[['EU HETA7','EurDesk HETA7','United Kingdom HETA7']] - df[['EU desk', 'EurDesk desk','United Kingdom desk']].values
        df[['EU MTDETA delta','EurDesk MTDETA delta','United Kingdom MTDETA delta']] = df[['EU MTDETA','EurDesk MTDETA','United Kingdom MTDETA']] - df[['EU desk', 'EurDesk desk','United Kingdom desk']].values
        #print(df.loc[today-relativedelta(days=7):today+relativedelta(days=6)])
        df = df.round(2)

        return df
    
    
    def chart(dfchart):
        
        today = datetime.date.today()
        
        for i in ['EU','EurDesk','United Kingdom']:
            fig1=go.Figure()
            fig1.add_trace(go.Bar(x=dfchart.index, y=dfchart[i],
                            name='cargo arrival',
                            marker_color='yellow',
                            ))
            fig1.add_trace(go.Scatter(x=dfchart.index, y=dfchart[i+' desk'],
                                mode='lines',
                                name='Desk',
                                line=dict(color='red', dash='solid')
                                ))
            fig1.add_trace(go.Scatter(x=dfchart.index, y=dfchart[i+' MA10'],
                                mode='lines',
                                name='MA10',
                                line=dict(color='black', dash='solid')
                                ))
            fig1.add_trace(go.Scatter(x=dfchart.index, y=dfchart[i+' MTD'],
                                mode='lines',
                                name='MTD',
                                line=dict(color='green', dash='solid')
                                ))
            fig1.add_trace(go.Scatter(x=dfchart.index, y=dfchart[i+' ETA7'],
                                mode='lines',
                                name='7 Days ETA',
                                line=dict(color='green', dash='dot')
                                ))
            fig1.add_trace(go.Scatter(x=dfchart.index, y=dfchart[i+' HETA7'],
                                mode='lines',
                                name='7 Days Hist&ETA',
                                line=dict(color='blue', dash='dot')
                                ))
            fig1.add_trace(go.Scatter(x=dfchart.index, y=dfchart[i+' MTDETA'],
                                mode='lines',
                                name='Hist MTD & ETA',
                                line=dict(color='purple', dash='dash')
                                ))
            fig1.add_trace(go.Scatter(x=[datetime.datetime.today().date(),datetime.datetime.today().date()],y=[0,dfchart[i].max()],
                   mode="lines",
                   line=dict(color='red', width=2, dash='dot'),
                   showlegend=False,
                   name='Today'
                   )
               )
            fig1.update_layout(
                 autosize=True,
                 showlegend=True,
                 #colorscale='RdBu',
                 legend=dict(x=0, y=-0.2),
                 legend_orientation="h",
                 title_text=i+' '+str(today)+' ,Mcm/d',
                 #xaxis = dict(dtick="M1"),
                 hovermode='x unified',
                 plot_bgcolor = 'white',
                 template='ggplot2'  ,
                 annotations=[dict(x=datetime.datetime.today().date(), y=1, yref='paper',font=dict(color="red",size=14),showarrow=False, xanchor='left', text='Today')],

             )
            fig1.update_yaxes(title_text="Mcm/d")
            py.plot(fig1, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/LNG tracking/'+i+'.html', auto_open=False)
    
    
    def table(dfchart):
           
        today = datetime.date.today()
        
        dftable = pd.DataFrame(index=['Desk','Today','MA10','MTD','7 Day ETA','7 hist & 7 day ETA','Hist Mtd 7 day ETA'], columns=['EU','EurDesk','United Kingdom'])
        dftabledelta = pd.DataFrame(index=['MA10','MTD','7 Day ETA','7 hist & 7 day ETA','Hist Mtd 7 day ETA'], columns=['EU','EurDesk','United Kingdom'])

        #print(dfchart.index)
        
        for i in dftable.columns:
            dftable.loc['Desk',i] = dfchart.loc[str(today),i+' desk']
            dftable.loc['Today',i] = dfchart.loc[str(today),i].round(2)
            dftable.loc['MA10',i] = dfchart.loc[str(today),i+' MA10']
            dftable.loc['MTD',i] = dfchart.loc[str(today),i+' MTD']
            dftable.loc['7 Day ETA',i] = dfchart.loc[str(today),i+' ETA7']
            dftable.loc['7 hist & 7 day ETA',i] = dfchart.loc[str(today),i+' HETA7']
            dftable.loc['Hist Mtd 7 day ETA',i] = dfchart.loc[str(today),i+' MTDETA']
            
            dftabledelta.loc['MA10',i] = dfchart.loc[str(today),i+' MA10 delta']
            dftabledelta.loc['MTD',i] = dfchart.loc[str(today),i+' MTD delta']
            dftabledelta.loc['7 Day ETA',i] = dfchart.loc[str(today),i+' ETA7 delta']
            dftabledelta.loc['7 hist & 7 day ETA',i] = dfchart.loc[str(today),i+' HETA7 delta']
            dftabledelta.loc['Hist Mtd 7 day ETA',i] = dfchart.loc[str(today),i+' MTDETA delta']
        
        #print(dftable)
        #print(dftabledelta)
        #print( df1)
        #table 1 color
        
       
        #table 2 color
        df2color=dftabledelta.copy()
        for i in df2color.index:
            for j in df2color.columns:
                if df2color.loc[i,j] >10:
                    df2color.loc[i,j] = 'LightGreen'
                elif df2color.loc[i,j] <-10:
                    df2color.loc[i,j] = 'pink'
                else:
                    df2color.loc[i,j] = 'white'
    
        df2color.insert(0,'date',['paleturquoise']*df2color.shape[0])
        df2color=df2color.T
        #print(df2color)
        
        fig1 = go.Figure(data=[go.Table(
                        header=dict(values=['date']+list(dftable.columns),
                                    fill_color='paleturquoise',
                                    align='center'),
                        cells=dict(values=[dftable.index, dftable.EU, dftable.EurDesk, dftable['United Kingdom']],
                                   #fill_color='lavender',
                                   #fill=dict(color=df1color.values.tolist()),
                                   #font=dict(color="white"),
                                   align='center'))
                    ])
        
        fig1.update_layout(
             title_text='EU, EurDesk and UK LNG Tracker Mcm/d',
         )
        
        py.plot(fig1, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/LNG tracking/EUtrackertable.html', auto_open=False)
        #fig1.write_image('U:/Trading - Gas/LNG/LNG website/analysis/Japan power price/JapPowerTable1.png')
        
        fig2 = go.Figure(data=[go.Table(
                       header=dict(values=['date']+list(dftabledelta.columns),
                                   fill_color='paleturquoise',
                                   align='center'),
                       cells=dict(values=[dftabledelta.index, dftabledelta.EU, dftabledelta.EurDesk, dftabledelta['United Kingdom']],
                                  #fill_color='lavender',
                                  fill=dict(color=df2color.values.tolist()),
                                  align='center'))
                   ])
        fig2.update_layout(
             title_text='EU, EurDesk and UK LNG Tracker Actual - Desk Delta Mcm/d',
         )
        py.plot(fig2, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/LNG tracking/EUtrackertabledelta.html', auto_open=False)
        #fig2.write_image('U:/Trading - Gas/LNG/LNG website/analysis/Japan power price/JapPowerTable2.png')

    def update():        
        dfeta = EU_LNG_Tracker.get_data_eta()  
        dfkplerhist = EU_LNG_Tracker.get_data_hist() 
        dfunknow = EU_LNG_Tracker.get_data_unknow()
        #dfkplerhist, dfkplerhistUK, dfma10, dfma10UK = EU_LNG_Tracker.get_kpler_data()
        #dfeta = EU_LNG_Tracker.create_df_etademand_MA()        
        dfdesk = EU_LNG_Tracker.get_desk()
        
        df = EU_LNG_Tracker.full_data(dfkplerhist, dfunknow, dfeta,dfdesk)
        dfchart = EU_LNG_Tracker.chart_data(df)
        EU_LNG_Tracker.chart(dfchart)
        EU_LNG_Tracker.table(dfchart)
        
#EU_LNG_Tracker.update()