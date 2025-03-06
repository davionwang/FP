# -*- coding: utf-8 -*-
"""
Created on Tue Nov  9 16:12:11 2021

@author: SVC-GASQuant2-Prod
"""

#V0 MA30 charts

import sys
import plotly.offline as py
import plotly.graph_objs as go
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import time
import datetime
import calendar
import numpy as np
import pandas as pd
from plotly.subplots import make_subplots
import matplotlib.pyplot as plt
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.combining import OrTrigger
from apscheduler.triggers.cron import CronTrigger
from dateutil.relativedelta import relativedelta



sys.path.append('C:\\Users\SVC-GASQuant2-Prod\YWwebsite\Python Scripts\class') 
from DBtoDF import DBTOPD

pd.set_option('display.max_columns',10)


#to_csv('H:/Yuefeng/LNG Flows/Deskdatatestcom.csv')
#to_csv('H:/Yuefeng/LNG Flows/Deskdatatest.csv')
class FlowMA30Chart():
    
    def full_data():
        
        #get data from DB
        demand=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'DemandCountry30')
        dfdemand=demand.sql_to_df()
        dfdemand.set_index('Date', inplace=True)
        
        supplyplant=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'SupplyPlant30')
        dfsupplyplant=supplyplant.sql_to_df()
        dfsupplyplant.set_index('Date', inplace=True)
        
        supplycountry=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'SupplyCountry30')
        dfsupplycountry=supplycountry.sql_to_df()
        dfsupplycountry.set_index('Date', inplace=True)
        
        supplyMA_eta=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'SupplyCountryETA30')
        dfsupplyMA_eta=supplyMA_eta.sql_to_df()
        dfsupplyMA_eta.set_index('Date', inplace=True)
        
        supplyMA_eta_plant=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'SupplyPlantETA')
        dfsupplyMA_eta_plant=supplyMA_eta_plant.sql_to_df()
        dfsupplyMA_eta_plant.set_index('Date', inplace=True)
        
        demandMA_eta=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'DemandETA30')
        dfdemandMA_eta=demandMA_eta.sql_to_df()
        dfdemandMA_eta.set_index('Date', inplace=True)
        
        desk_supply_plant_view=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'DeskSupplyPlant')
        desk_supply_plant_view=desk_supply_plant_view.sql_to_df()
        desk_supply_plant_view.set_index('Date', inplace=True)
        
        desk_supply_country_view=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'DeskSupplyCountry')
        desk_supply_country_view=desk_supply_country_view.sql_to_df()
        desk_supply_country_view.set_index('Date', inplace=True)
        
        desk_demand_view=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'DeskDemand')
        desk_demand_view=desk_demand_view.sql_to_df()
        desk_demand_view.set_index('Date', inplace=True)
        
        ihscontractdemand=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'IHSContractdemand')
        dfihscontractdemand=ihscontractdemand.sql_to_df()
        dfihscontractdemand.set_index('Date', inplace=True)
        
        ihscontractsupply=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'IHSContractsupply')
        dfihscontractsupply=ihscontractsupply.sql_to_df()
        dfihscontractsupply.set_index('Date', inplace=True)
        
        '''
        #get country list
        SupplyCurveId = pd.read_excel('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/DeskViewID.xlsx',header=(0),sheet_name='supply')
        #remove repeat country and get country list
        SupplyCurveId.drop_duplicates(inplace=True)
        supply_country_list=SupplyCurveId['Country']
        supply_country_list.drop_duplicates(inplace=True)
        supply_country_list=supply_country_list.values.tolist()
        #print(supply_country_list)
        DemandCurveId = pd.read_excel('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/DeskViewID.xlsx',header=(0),sheet_name='demand')
        DemandCurveId.drop_duplicates(inplace=True)
        demand_country_list = DemandCurveId['Country']
        demand_country_list.drop_duplicates(inplace=True)
        demand_country_list=demand_country_list.values.tolist()
        '''
        curveid = DBTOPD.get_curve_id()
        dfcurveid = curveid.loc[:,['CurveId','Type','Location','Country']]
        #print(dfcurveid)
        SupplyCurveId = dfcurveid.loc[dfcurveid[dfcurveid['Type']=='Supply'].index]
        SupplyCurveId = SupplyCurveId[['CurveId','Country','Location']]
        SupplyCurveId.rename(columns={'CurveId':'CurveID','Country':'Country','Location':'plant'}, inplace=True)
        SupplyCurveId.drop_duplicates(inplace=True)
        supply_country_list=SupplyCurveId['Country']
        supply_country_list.drop_duplicates(inplace=True)
        supply_country_list=supply_country_list.values.tolist()
        supply_country_list.remove('Senegal')
        #supply_country_list.remove('United States') #new market 2024
        #supply_country_list.remove('Congo')
        #supply_country_list.remove('Mexico')
        
        DemandCurveId = dfcurveid.loc[dfcurveid[dfcurveid['Type']=='Demand'].index]
        DemandCurveId = DemandCurveId[['CurveId','Country','Location']]
        DemandCurveId.rename(columns={'CurveId':'CurveID','Country':'Country','Location':'plant'}, inplace=True)
        DemandCurveId.drop(DemandCurveId[DemandCurveId['Country']=='Russian Federation'].index, inplace=True)
        DemandCurveId.drop_duplicates(inplace=True)
        demand_country_list = DemandCurveId['Country']
        demand_country_list.drop_duplicates(inplace=True)
        demand_country_list=demand_country_list.values.tolist()
        demand_country_list.remove('South Africa') #new market
        demand_country_list.remove('Sri Lanka')
        #print(df_supply)
        basinsupply=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'BasinSupply30')
        dfbasinsupply=basinsupply.sql_to_df()
        dfbasinsupply.set_index('Date', inplace=True)
        
        basindemand=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'BasinDemand30')
        dfbasindemand=basindemand.sql_to_df()
        dfbasindemand.set_index('Date', inplace=True)
        
        cumsupply=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'CumulativeSupply30')
        dfcumsupply=cumsupply.sql_to_df()
        dfcumsupply.rename(columns=({'index':'Date'}), inplace=True)
        dfcumsupply.set_index('Date', inplace=True)
        
        cumdemand=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'CumulativeDemand30')
        dfcumdemand=cumdemand.sql_to_df()
        dfcumdemand.rename(columns=({'index':'Date'}), inplace=True)
        dfcumdemand.set_index('Date', inplace=True)
        
        
        
        
        return dfdemand, dfsupplyplant, dfsupplycountry, dfsupplyMA_eta, dfsupplyMA_eta_plant, dfdemandMA_eta, desk_supply_plant_view, desk_supply_country_view, desk_demand_view, dfihscontractdemand,dfihscontractsupply, supply_country_list, demand_country_list, dfbasinsupply,dfbasindemand,dfcumsupply,dfcumdemand
    
    
        
    def plot_chart(df, sd, country, today):
        
        this_year_start = str(datetime.date.today().year)+'-01-01'
        last_month = str(datetime.date.today().year)+'-'+str(datetime.date.today().month-1)
            
        if datetime.date.today().month-1==0:
            if sd == 'supply':
  
                fig1 = go.Figure() 
                
                fig1.add_trace(
                    go.Scatter(
                        x=df.index,
                        y=df['IHS Contract'],
                        mode="lines",
                        name='IHS Contract 80%',
                        showlegend=True,
                        line=dict(color='#00B0F0', width=2, dash='dash')
                        )
                    )
                
                fig1.add_trace(
                    go.Scatter(
                        x=df.index,
                        y=df['IHS Contract']/0.8,
                        mode="lines",
                        name='IHS Contract 100%',
                        showlegend=True,
                        line=dict(color='#00B0F0', width=2, dash='dot')
                        )
                    )
                
                
                fig1.add_trace(
                    go.Scatter(
                        x=df.index,
                        y=df['min'],
                        fill='tonexty',
                        fillcolor='rgba(65,105,225,0)',
                        line_color='rgba(65,105,225,0)',
                        showlegend=False,
                        name='5 Years'
                        )
                    )
                
                fig1.add_trace(
                    go.Scatter(
                        x=df.index,
                        y=df['max'],
                        fill='tonexty',
                        fillcolor='rgba(65,105,225,0.1)',
                        line_color='rgba(65,105,225,0)',
                        showlegend=True,
                        name='5 Years'
                        )
                    )
                
                fig1.add_trace(
                    go.Scatter(
                        x=df.index,
                        y=df['Kpler '+str(today.year-1)],
                        mode="lines",
                        name='Kpler '+str(today.year-1),
                        showlegend=True,
                        line=dict(color='#44546A', width=2, dash='dash')
                        
                        )
                    )
                
                fig1.add_trace(
                    go.Scatter(
                        x=df.index,
                        y=df['Kpler '+str(today.year-0)],
                        mode="lines",
                        name='Kpler '+str(today.year-0),
                        showlegend=True,
                        line=dict(color='black', width=2)
                        
                        )
                    )
                
                fig1.add_trace(
                    go.Scatter(
                        x=df.index,
                        y=df['Kpler ETA'].loc[:today+datetime.timedelta(days=7)],
                        mode="lines",
                        name='Kpler ETA',
                        showlegend=True,
                        line=dict(color='blue', width=2, dash='solid')
                        
                        )
                    )
                
                fig1.add_trace(
                    go.Scatter(
                        x=df.index,
                        y=df['Desk View'],
                        mode="lines",
                        name='Desk View',
                        showlegend=True,
                        line=dict(color='red', width=2)
                        
                        )
                    )
                fig1.add_trace(
                    go.Scatter(
                        x=df.index,
                        y=df['Desk View '+str(today.year+1)],
                        mode="lines",
                        name='Desk View '+str(today.year+1),
                        showlegend=True,
                        line=dict(color='red', width=2,dash='dot')
                        
                        )
                    )
                fig1.update_layout(
                    autosize=True,
                    showlegend=True,
                    legend=dict(x=0, y=-0.2),
                    legend_orientation="h",
                    title_text=country + ' 30 days MA, Mcm/d '+ str(today),
                    yaxis_title="Exports (Mcm/d)",
                    xaxis = dict(dtick="M1"),
                    hovermode='x unified',
                    plot_bgcolor = 'white',
                    template='ggplot2'  
                )
                fig1.update_xaxes(
                    showline=True, 
                    linewidth=1, 
                    linecolor='LightGrey', 
                    showgrid=True, 
                    gridwidth=1, 
                    gridcolor='LightGrey'
                )
                fig1.update_yaxes(
                    showline=True, 
                    linewidth=1, 
                    linecolor='LightGrey', 
                    showgrid=True, 
                    gridwidth=1, 
                    gridcolor='LightGrey'
                )
                   
                py.plot(fig1, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/Flow30/daily/supply/all market/'+country+'.html', auto_open=False)
                fig1.write_image("U:/Trading - Gas/LNG/LNG website/Flow30/daily/supply/all market/'+country+'.png")
            if sd == 'demand':       
                fig1 = go.Figure() 
                
                fig1.add_trace(
                    go.Scatter(
                        x=df.index,
                        y=df['IHS Contract'],
                        mode="lines",
                        name='IHS Contract 80%',
                        showlegend=True,
                        line=dict(color='#00B0F0', width=2, dash='dash')
                        )
                    )
                
                fig1.add_trace(
                    go.Scatter(
                        x=df.index,
                        y=df['IHS Contract']/0.8,
                        mode="lines",
                        name='IHS Contract 100%',
                        showlegend=True,
                        line=dict(color='#00B0F0', width=2, dash='dot')
                        )
                    )
                
                
                fig1.add_trace(
                    go.Scatter(
                        x=df.index,
                        y=df['min'],
                        fill='tonexty',
                        fillcolor='rgba(65,105,225,0)',
                        line_color='rgba(65,105,225,0)',
                        showlegend=False,
                        name='5 Years'
                        )
                    )
                
                fig1.add_trace(
                    go.Scatter(
                        x=df.index,
                        y=df['max'],
                        fill='tonexty',
                        fillcolor='rgba(65,105,225,0.1)',
                        line_color='rgba(65,105,225,0)',
                        showlegend=True,
                        name='5 Years'
                        )
                    )
                
                fig1.add_trace(
                    go.Scatter(
                        x=df.index,
                        y=df['Kpler '+str(today.year-1)],
                        mode="lines",
                        name='Kpler '+str(today.year-1),
                        showlegend=True,
                        line=dict(color='#44546A', width=2, dash='dot')
                        
                        )
                    )
                
                fig1.add_trace(
                    go.Scatter(
                        x=df.index,
                        y=df['Kpler '+str(today.year-0)],
                        mode="lines",
                        name='Kpler '+str(today.year-0),
                        showlegend=True,
                        line=dict(color='black', width=2)
                        
                        )
                    )
                fig1.add_trace(
                   go.Scatter(
                       x=df.index,
                       y=df['Kpler ETA'].loc[:today+datetime.timedelta(days=7)],
                       mode="lines",
                       name='Kpler ETA',
                       showlegend=True,
                       line=dict(color='blue', width=2, dash='solid')
                       
                       )
                   )
                 
                fig1.add_trace(
                    go.Scatter(
                        x=df.index,
                        y=df['Desk View'],
                        mode="lines",
                        name='Desk View',
                        showlegend=True,
                        line=dict(color='red', width=2)
                        
                        )
                    )
                
                fig1.add_trace(
                    go.Scatter(
                        x=df.index,
                        y=df['Desk View '+str(today.year+1)],
                        mode="lines",
                        name='Desk View '+str(today.year+1),
                        showlegend=True,
                        line=dict(color='red', width=2,  dash='dot')
                        
                        )
                    )
                fig1.update_layout(
                    autosize=True,
                    showlegend=True,
                    legend=dict(x=0, y=-0.2),
                    legend_orientation="h",
                    title_text=country + ' 30 days MA, Mcm/d '+ str(today),
                    yaxis_title="Net imports (Mcm/d)",
                    xaxis = dict(dtick="M1"),
                    hovermode='x unified',
                    plot_bgcolor = 'white',
                    template='ggplot2'
                      
                )
                fig1.update_xaxes(
                    showline=True, 
                    linewidth=1, 
                    linecolor='LightGrey', 
                    showgrid=True, 
                    gridwidth=1, 
                    gridcolor='LightGrey'
                )
                fig1.update_yaxes(
                    showline=True, 
                    linewidth=1, 
                    linecolor='LightGrey', 
                    showgrid=True, 
                    gridwidth=1, 
                    gridcolor='LightGrey'
                )
                
                py.plot(fig1, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/Flow30/daily/demand/all market/'+country+'.html', auto_open=False) 
                fig1.write_image('U:/Trading - Gas/LNG/LNG website/Flow30/daily/demand/all market/'+country+'.png')
        else:
            
            if sd == 'supply':
  
                fig1 = go.Figure() 
                
                fig1.add_trace(
                    go.Scatter(
                        x=df.index,
                        y=df['IHS Contract'],
                        mode="lines",
                        name='IHS Contract 80%',
                        showlegend=True,
                        line=dict(color='#00B0F0', width=2, dash='dash')
                        )
                    )
                
                fig1.add_trace(
                    go.Scatter(
                        x=df.index,
                        y=df['IHS Contract']/0.8,
                        mode="lines",
                        name='IHS Contract 100%',
                        showlegend=True,
                        line=dict(color='#00B0F0', width=2, dash='dot')
                        )
                    )
                
                fig1.add_trace(
                    go.Bar(
                        x=df.index,
                        y=df.loc[this_year_start:last_month,'Difference'],
                        name='Kpler '+str(today.year)+' vs. Desk '+str(today.year)+' delta',
                        #width=0.5,
                        showlegend=True,
                        marker_color='Silver',
                        )
                    )
                
                fig1.add_trace(
                    go.Scatter(
                        x=df.index,
                        y=df['min'],
                        fill='tonexty',
                        fillcolor='rgba(65,105,225,0)',
                        line_color='rgba(65,105,225,0)',
                        showlegend=False,
                        name='5 Years'
                        )
                    )
                
                fig1.add_trace(
                    go.Scatter(
                        x=df.index,
                        y=df['max'],
                        fill='tonexty',
                        fillcolor='rgba(65,105,225,0.1)',
                        line_color='rgba(65,105,225,0)',
                        showlegend=True,
                        name='5 Years'
                        )
                    )
                
                fig1.add_trace(
                    go.Scatter(
                        x=df.index,
                        y=df['Kpler '+str(today.year-1)],
                        mode="lines",
                        name='Kpler '+str(today.year-1),
                        showlegend=True,
                        line=dict(color='#44546A', width=2, dash='dash')
                        
                        )
                    )
                
                fig1.add_trace(
                    go.Scatter(
                        x=df.index,
                        y=df['Kpler '+str(today.year-0)],
                        mode="lines",
                        name='Kpler '+str(today.year-0),
                        showlegend=True,
                        line=dict(color='black', width=2)
                        
                        )
                    )
                
                fig1.add_trace(
                    go.Scatter(
                        x=df.index,
                        y=df['Kpler ETA'].loc[:today+datetime.timedelta(days=7)],
                        mode="lines",
                        name='Kpler ETA',
                        showlegend=True,
                        line=dict(color='blue', width=2, dash='solid')
                        
                        )
                    )
                
                fig1.add_trace(
                    go.Scatter(
                        x=df.index,
                        y=df['Desk View'],
                        mode="lines",
                        name='Desk View',
                        showlegend=True,
                        line=dict(color='red', width=2)
                        
                        )
                    )
                fig1.add_trace(
                    go.Scatter(
                        x=df.index,
                        y=df['Desk View '+str(today.year+1)],
                        mode="lines",
                        name='Desk View '+str(today.year+1),
                        showlegend=True,
                        line=dict(color='red', width=2,dash='dot')
                        
                        )
                    )
                fig1.update_layout(
                    autosize=True,
                    showlegend=True,
                    legend=dict(x=0, y=-0.2),
                    legend_orientation="h",
                    title_text=country + ' 30 days MA, Mcm/d '+ str(today),
                    yaxis_title="Exports (Mcm/d)",
                    xaxis = dict(dtick="M1"),
                    hovermode='x unified',
                    plot_bgcolor = 'white',
                    template='ggplot2'  
                )
                fig1.update_xaxes(
                    showline=True, 
                    linewidth=1, 
                    linecolor='LightGrey', 
                    showgrid=True, 
                    gridwidth=1, 
                    gridcolor='LightGrey'
                )
                fig1.update_yaxes(
                    showline=True, 
                    linewidth=1, 
                    linecolor='LightGrey', 
                    showgrid=True, 
                    gridwidth=1, 
                    gridcolor='LightGrey'
                )
                   
                py.plot(fig1, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/Flow30/daily/supply/all market/'+country+'.html', auto_open=False)
                fig1.write_image('U:/Trading - Gas/LNG/LNG website/Flow30/daily/supply/all market/'+country+'.png')
                
            if sd == 'demand':       
                fig1 = go.Figure() 
                
                fig1.add_trace(
                    go.Scatter(
                        x=df.index,
                        y=df['IHS Contract'],
                        mode="lines",
                        name='IHS Contract 80%',
                        showlegend=True,
                        line=dict(color='#00B0F0', width=2, dash='dash')
                        )
                    )
                
                fig1.add_trace(
                    go.Scatter(
                        x=df.index,
                        y=df['IHS Contract']/0.8,
                        mode="lines",
                        name='IHS Contract 100%',
                        showlegend=True,
                        line=dict(color='#00B0F0', width=2, dash='dot')
                        )
                    )
                
                fig1.add_trace(
                    go.Bar(
                        x=df.index,
                        y=df.loc[this_year_start:last_month,'Difference'],
                        name='Kpler '+str(today.year)+' vs. Desk '+str(today.year)+' delta',
                        #width=0.5,
                        showlegend=True,
                        marker_color='Silver',
                        )
                    )
                
                fig1.add_trace(
                    go.Scatter(
                        x=df.index,
                        y=df['min'],
                        fill='tonexty',
                        fillcolor='rgba(65,105,225,0)',
                        line_color='rgba(65,105,225,0)',
                        showlegend=False,
                        name='5 Years'
                        )
                    )
                
                fig1.add_trace(
                    go.Scatter(
                        x=df.index,
                        y=df['max'],
                        fill='tonexty',
                        fillcolor='rgba(65,105,225,0.1)',
                        line_color='rgba(65,105,225,0)',
                        showlegend=True,
                        name='5 Years'
                        )
                    )
                
                fig1.add_trace(
                    go.Scatter(
                        x=df.index,
                        y=df['Kpler '+str(today.year-1)],
                        mode="lines",
                        name='Kpler '+str(today.year-1),
                        showlegend=True,
                        line=dict(color='#44546A', width=2, dash='dot')
                        
                        )
                    )
                
                fig1.add_trace(
                    go.Scatter(
                        x=df.index,
                        y=df['Kpler '+str(today.year-0)],
                        mode="lines",
                        name='Kpler '+str(today.year-0),
                        showlegend=True,
                        line=dict(color='black', width=2)
                        
                        )
                    )
                fig1.add_trace(
                   go.Scatter(
                       x=df.index,
                       y=df['Kpler ETA'].loc[:today+datetime.timedelta(days=7)],
                       mode="lines",
                       name='Kpler ETA',
                       showlegend=True,
                       line=dict(color='blue', width=2, dash='solid')
                       
                       )
                   )
                 
                fig1.add_trace(
                    go.Scatter(
                        x=df.index,
                        y=df['Desk View'],
                        mode="lines",
                        name='Desk View',
                        showlegend=True,
                        line=dict(color='red', width=2)
                        
                        )
                    )
                
                fig1.add_trace(
                    go.Scatter(
                        x=df.index,
                        y=df['Desk View '+str(today.year+1)],
                        mode="lines",
                        name='Desk View '+str(today.year+1),
                        showlegend=True,
                        line=dict(color='red', width=2,  dash='dot')
                        
                        )
                    )
                fig1.update_layout(
                    autosize=True,
                    showlegend=True,
                    legend=dict(x=0, y=-0.2),
                    legend_orientation="h",
                    title_text=country + ' 30 days MA, Mcm/d '+ str(today),
                    yaxis_title="Net imports (Mcm/d)",
                    xaxis = dict(dtick="M1"),
                    hovermode='x unified',
                    plot_bgcolor = 'white',
                    template='ggplot2'
                      
                )
                fig1.update_xaxes(
                    showline=True, 
                    linewidth=1, 
                    linecolor='LightGrey', 
                    showgrid=True, 
                    gridwidth=1, 
                    gridcolor='LightGrey'
                )
                fig1.update_yaxes(
                    showline=True, 
                    linewidth=1, 
                    linecolor='LightGrey', 
                    showgrid=True, 
                    gridwidth=1, 
                    gridcolor='LightGrey'
                )
                
                py.plot(fig1, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/Flow30/daily/demand/all market/'+country+'.html', auto_open=False) 
                fig1.write_image('U:/Trading - Gas/LNG/LNG website/Flow30/daily/demand/all market/'+country+'.png')
            
              
    '''all country chart'''
    def country_chart(date_dict, supply_country_list, demand_country_list, dfsupplycountry, dfdemand, dfsupplyMA_eta, dfdemandMA_eta, desk_supply_country_view, desk_demand_view, dfihscontractdemand,dfihscontractsupply):    
        #MA period
        #rolling_period=10
        #date
        today=datetime.date.today()
        #delta = datetime.timedelta(days=1)
        start_date='2016-01-01' 
        current_year_start = date_dict['current_year_start']
        current_year_end = date_dict['current_year_end']
        next_year_start=date_dict['next_year_start']
        year_end = str(datetime.date.today().year+1)+'-12-31'#'2023-12-31'
        
        #supply_country_list.remove('Portovaya LNG')

        #ihscontract = dfihscontract(year_start, year_end) 
        #print(supply_country_list)
        
        for i in supply_country_list:
            
            supply_fulldate=pd.DataFrame(index=pd.date_range(start=start_date, end = year_end))
            supply_fulldate.index.name='Date'
            
            #print(dfihscontractsupply[i])
            supply_country=pd.concat([supply_fulldate, dfsupplycountry[i], dfsupplyMA_eta[i], desk_supply_country_view[i],dfihscontractsupply[i]], axis=1)
            #print(supply_country)
            supply_country.columns=['Kpler', 'eta', 'Desk View','IHS Contract']
            #print(supply_country.loc[next_year_start:year_end, 'Desk View'])
            supply_country['Difference']=(supply_country['Kpler'] - supply_country['Desk View']).resample('M').sum()
            supply_country['Difference'].fillna(method='bfill', inplace=True)
            
                                         
            for day in pd.date_range(start=current_year_start, end=today):
                days=calendar.monthrange(day.year,day.month)
                supply_country.loc[day,'Difference']=supply_country.loc[day,'Difference']/days[1]
            
            supply_country = supply_country[~((supply_country.index.month==2)&(supply_country.index.day == 29))]
            #print(supply_country)
            #print(supply_country.loc[next_year_start:year_end, 'Desk View'])
            supply_yoy=pd.DataFrame(index=pd.date_range(start=current_year_start, end=current_year_end), columns=['Kpler '+str(today.year-5),'Kpler '+str(today.year-4),'Kpler '+str(today.year-3),'Kpler '+str(today.year-2),'Kpler '+str(today.year-1),'max','min','Kpler '+str(today.year-0),'Desk View','Desk View '+str(today.year+1),'Difference','Kpler ETA'])
            #supply_yoy['Kpler 2016'].loc[current_year_start:current_year_end]=np.delete(supply_country.loc['2016-01-01':'2016-12-31', 'Kpler'].values, 59)
            supply_yoy['Kpler '+str(today.year-5)].loc[current_year_start:current_year_end] = supply_country.loc[str(today.year-5)+'-01-01':str(today.year-5)+'-12-31', 'Kpler'].values
            supply_yoy['Kpler '+str(today.year-4)].loc[current_year_start:current_year_end] = supply_country.loc[str(today.year-4)+'-01-01':str(today.year-4)+'-12-31', 'Kpler'].values
            supply_yoy['Kpler '+str(today.year-3)].loc[current_year_start:current_year_end] = supply_country.loc[str(today.year-3)+'-01-01':str(today.year-3)+'-12-31', 'Kpler'].values
            supply_yoy['Kpler '+str(today.year-2)].loc[current_year_start:current_year_end] = supply_country.loc[str(today.year-2)+'-01-01':str(today.year-2)+'-12-31', 'Kpler'].values
            supply_yoy['Kpler '+str(today.year-1)].loc[current_year_start:current_year_end] = supply_country.loc[str(today.year-1)+'-01-01':str(today.year-1)+'-12-31', 'Kpler'].values
            supply_yoy['max']=supply_yoy[['Kpler '+str(today.year-5),'Kpler '+str(today.year-4),'Kpler '+str(today.year-3),'Kpler '+str(today.year-2),'Kpler '+str(today.year-1)]].max(axis=1)
            supply_yoy['min']=supply_yoy[['Kpler '+str(today.year-5),'Kpler '+str(today.year-4),'Kpler '+str(today.year-3),'Kpler '+str(today.year-2),'Kpler '+str(today.year-1)]].min(axis=1)
            supply_yoy['Kpler '+str(today.year-0)].loc[current_year_start:current_year_end] = supply_country.loc[current_year_start:current_year_end, 'Kpler'].values
            supply_yoy['Desk View'] = supply_country.loc[current_year_start:current_year_end, 'Desk View'].values
            supply_yoy['Desk View '+str(today.year+1)] = supply_country.loc[next_year_start:year_end, 'Desk View'].values
            supply_yoy['Difference'] = supply_country.loc[current_year_start:current_year_end, 'Difference'].values
            supply_yoy['IHS Contract'] = supply_country.loc[current_year_start:current_year_end, 'IHS Contract'].values

            supply_yoy['Kpler ETA'] = supply_country.loc[current_year_start:current_year_end,'eta'].values
            supply_yoy=supply_yoy.round()
            FlowMA30Chart.plot_chart(supply_yoy, 'supply', i, today)
        
        for j in demand_country_list:
            #
            demand_fulldate=pd.DataFrame(index=pd.date_range(start=start_date, end = year_end))
            demand_fulldate.index.name='Date'
            demand_country=pd.concat([demand_fulldate, dfdemand[j], dfdemandMA_eta[j], desk_demand_view[j], dfihscontractdemand[j]], axis=1)
            demand_country.columns=['Kpler','eta', 'Desk View','IHS Contract']
            demand_country['Difference']=(demand_country['Kpler'] - demand_country['Desk View']).resample('M').sum()
            demand_country['Difference'].fillna(method='bfill', inplace=True)
            #demand_country.to_csv('H:/Yuefeng/LNG Flows/Deskdatatest.csv')
            for day in pd.date_range(start=current_year_start, end=today):
                days=calendar.monthrange(day.year,day.month)
                demand_country.loc[day,'Difference']=demand_country.loc[day,'Difference']/days[1]
                
            demand_country = demand_country[~((demand_country.index.month==2)&(demand_country.index.day == 29))]
            demand_yoy=pd.DataFrame(index=pd.date_range(start=current_year_start, end=current_year_end), columns=['Kpler '+str(today.year-5),'Kpler '+str(today.year-4),'Kpler '+str(today.year-3),'Kpler '+str(today.year-2),'Kpler '+str(today.year-1),'max','min','Kpler '+str(today.year-0),'Desk View','Desk View '+str(today.year+1),'Difference','Kpler ETA'])
            #demand_yoy['Kpler 2016'].loc[current_year_start:current_year_end] = np.delete(demand_country.loc['2016-01-01':'2016-12-31', 'Kpler'].values, 59)
            demand_yoy['Kpler '+str(today.year-5)].loc[current_year_start:current_year_end] = demand_country.loc[str(today.year-5)+'-01-01':str(today.year-5)+'-12-31', 'Kpler'].values
            demand_yoy['Kpler '+str(today.year-4)].loc[current_year_start:current_year_end] = demand_country.loc[str(today.year-4)+'-01-01':str(today.year-4)+'-12-31', 'Kpler'].values
            demand_yoy['Kpler '+str(today.year-3)].loc[current_year_start:current_year_end] = demand_country.loc[str(today.year-3)+'-01-01':str(today.year-3)+'-12-31', 'Kpler'].values
            demand_yoy['Kpler '+str(today.year-2)].loc[current_year_start:current_year_end] = demand_country.loc[str(today.year-2)+'-01-01':str(today.year-2)+'-12-31', 'Kpler'].values
            demand_yoy['Kpler '+str(today.year-1)].loc[current_year_start:current_year_end] = demand_country.loc[str(today.year-1)+'-01-01':str(today.year-1)+'-12-31', 'Kpler'].values
            demand_yoy['max']=demand_yoy[['Kpler '+str(today.year-5),'Kpler '+str(today.year-4),'Kpler '+str(today.year-3),'Kpler '+str(today.year-2),'Kpler '+str(today.year-1)]].max(axis=1)
            demand_yoy['min']=demand_yoy[['Kpler '+str(today.year-5),'Kpler '+str(today.year-4),'Kpler '+str(today.year-3),'Kpler '+str(today.year-2),'Kpler '+str(today.year-1)]].min(axis=1)
            demand_yoy['Kpler '+str(today.year-0)].loc[current_year_start:current_year_end] = demand_country.loc[current_year_start:current_year_end, 'Kpler'].values
            demand_yoy['Desk View'] = demand_country.loc[current_year_start:current_year_end, 'Desk View'].values
            demand_yoy['Desk View '+str(today.year+1)] = demand_country.loc[next_year_start:year_end, 'Desk View'].values
            
            demand_yoy['Difference'] = demand_country.loc[current_year_start:current_year_end, 'Difference'].values
            demand_yoy['IHS Contract'] = demand_country.loc[current_year_start:current_year_end, 'IHS Contract'].values
            demand_yoy['Kpler ETA'] = demand_country.loc[current_year_start:current_year_end, 'eta'].values
            demand_yoy=demand_yoy.round()
            
            FlowMA30Chart.plot_chart(demand_yoy, 'demand', j, today)
        
    '''global chart'''
    def global_chart(date_dict, supply_country_list, demand_country_list, dfsupplycountry, dfdemand, dfsupplyMA_eta, dfdemandMA_eta, desk_supply_country_view, desk_demand_view, dfihscontractdemand,dfihscontractsupply):
        
        today=datetime.date.today()
        start_date='2016-01-01' 
        current_year_start = date_dict['current_year_start']
        current_year_end = date_dict['current_year_end']
        next_year_start=date_dict['next_year_start']
        year_end = str(datetime.date.today().year+1)+'-12-31'
        
        #Supply global
        supply_global = pd.DataFrame(index=pd.date_range(start=start_date, end = year_end))
        supply_global['Kpler'] = dfsupplycountry.sum(axis=1)
        supply_global['eta'] = dfsupplyMA_eta.sum(axis=1)
        supply_global['Desk View'] = desk_supply_country_view.sum(axis=1)
        supply_global['IHS Contract'] = dfihscontractsupply.sum(axis=1)/2
        supply_global['Difference']=(supply_global['Kpler'] - supply_global['Desk View']).resample('M').sum()
        supply_global['Difference'].fillna(method='bfill', inplace=True)
        for day in pd.date_range(start=current_year_start, end=today):
                days=calendar.monthrange(day.year,day.month)
                supply_global.loc[day,'Difference']=supply_global.loc[day,'Difference']/days[1]
        
        supply_global = supply_global[~((supply_global.index.month==2)&(supply_global.index.day == 29))]
        supply_global_yoy=pd.DataFrame(index=pd.date_range(start=current_year_start, end=current_year_end), columns=['Kpler '+str(today.year-5),'Kpler '+str(today.year-4),'Kpler '+str(today.year-3),'Kpler '+str(today.year-2),'Kpler '+str(today.year-1),'max','min','Kpler '+str(today.year-0),'Desk View','Desk View '+str(today.year+1),'Difference','Kpler ETA'])
        #supply_global_yoy['Kpler 2016'].loc[current_year_start:current_year_end] = np.delete(supply_global.loc['2016-01-01':'2016-12-31', 'Kpler'].values, 59)
        supply_global_yoy['Kpler '+str(today.year-5)].loc[current_year_start:current_year_end] = supply_global.loc[str(today.year-5)+'-01-01':str(today.year-5)+'-12-31', 'Kpler'].values
        supply_global_yoy['Kpler '+str(today.year-4)].loc[current_year_start:current_year_end] = supply_global.loc[str(today.year-4)+'-01-01':str(today.year-4)+'-12-31', 'Kpler'].values
        supply_global_yoy['Kpler '+str(today.year-3)].loc[current_year_start:current_year_end] = supply_global.loc[str(today.year-3)+'-01-01':str(today.year-3)+'-12-31', 'Kpler'].values
        supply_global_yoy['Kpler '+str(today.year-2)].loc[current_year_start:current_year_end] = supply_global.loc[str(today.year-2)+'-01-01':str(today.year-2)+'-12-31', 'Kpler'].values
        supply_global_yoy['Kpler '+str(today.year-1)].loc[current_year_start:current_year_end] = supply_global.loc[str(today.year-1)+'-01-01':str(today.year-1)+'-12-31', 'Kpler'].values
        supply_global_yoy['max']=supply_global_yoy[['Kpler '+str(today.year-5),'Kpler '+str(today.year-4),'Kpler '+str(today.year-3),'Kpler '+str(today.year-2),'Kpler '+str(today.year-1)]].max(axis=1)
        supply_global_yoy['min']=supply_global_yoy[['Kpler '+str(today.year-5),'Kpler '+str(today.year-4),'Kpler '+str(today.year-3),'Kpler '+str(today.year-2),'Kpler '+str(today.year-1)]].min(axis=1)
        supply_global_yoy['Kpler '+str(today.year-0)].loc[current_year_start:current_year_end] = supply_global.loc[current_year_start:current_year_end, 'Kpler'].values
        supply_global_yoy['Desk View'] = supply_global.loc[current_year_start:current_year_end, 'Desk View'].values
        supply_global_yoy['Desk View '+str(today.year+1)] = supply_global.loc[next_year_start:year_end, 'Desk View'].values
            
        supply_global_yoy['Difference'] = supply_global.loc[current_year_start:current_year_end, 'Difference'].values
        supply_global_yoy['IHS Contract'] = supply_global.loc[current_year_start:current_year_end, 'IHS Contract'].values
        supply_global_yoy=supply_global_yoy.round()
        FlowMA30Chart.plot_chart(supply_global_yoy, 'supply', 'Global', today)  
    
        
        #Demand global
        demand_global = pd.DataFrame(index=pd.date_range(start=start_date, end = year_end))
        demand_global['Kpler'] = dfdemand.sum(axis=1)
        demand_global['eta'] = dfdemandMA_eta.sum(axis=1)
        demand_global['Desk View'] = desk_demand_view.sum(axis=1)
        demand_global['IHS Contract'] = dfihscontractdemand.sum(axis=1)
        demand_global['Difference']=(demand_global['Kpler'] - demand_global['Desk View']).resample('M').sum()
        demand_global['Difference'].fillna(method='bfill', inplace=True)
        for day in pd.date_range(start=current_year_start, end=today):
                days=calendar.monthrange(day.year,day.month)
                demand_global.loc[day,'Difference']=demand_global.loc[day,'Difference']/days[1]
        
        demand_global = demand_global[~((demand_global.index.month==2)&(demand_global.index.day == 29))]
        
        demand_global_yoy=pd.DataFrame(index=pd.date_range(start=current_year_start, end=current_year_end),columns=['Kpler '+str(today.year-5),'Kpler '+str(today.year-4),'Kpler '+str(today.year-3),'Kpler '+str(today.year-2),'Kpler '+str(today.year-1),'max','min','Kpler '+str(today.year-0),'Desk View','Desk View '+str(today.year+1),'IHS Contract','Kpler ETA'])
        #demand_global_yoy['Kpler 2016'].loc[current_year_start:current_year_end] = np.delete(demand_global.loc['2016-01-01':'2016-12-31', 'Kpler'].values, 59)
        demand_global_yoy['Kpler '+str(today.year-5)].loc[current_year_start:current_year_end] = demand_global.loc[str(today.year-5)+'-01-01':str(today.year-5)+'-12-31', 'Kpler'].values
        demand_global_yoy['Kpler '+str(today.year-4)].loc[current_year_start:current_year_end] = demand_global.loc[str(today.year-4)+'-01-01':str(today.year-4)+'-12-31', 'Kpler'].values
        demand_global_yoy['Kpler '+str(today.year-3)].loc[current_year_start:current_year_end] = demand_global.loc[str(today.year-3)+'-01-01':str(today.year-3)+'-12-31', 'Kpler'].values
        demand_global_yoy['Kpler '+str(today.year-2)].loc[current_year_start:current_year_end] = demand_global.loc[str(today.year-2)+'-01-01':str(today.year-2)+'-12-31', 'Kpler'].values
        demand_global_yoy['Kpler '+str(today.year-1)].loc[current_year_start:current_year_end] = demand_global.loc[str(today.year-1)+'-01-01':str(today.year-1)+'-12-31', 'Kpler'].values
        demand_global_yoy['max']=demand_global_yoy[['Kpler '+str(today.year-5),'Kpler '+str(today.year-4),'Kpler '+str(today.year-3),'Kpler '+str(today.year-2),'Kpler '+str(today.year-1)]].max(axis=1)
        demand_global_yoy['min']=demand_global_yoy[['Kpler '+str(today.year-5),'Kpler '+str(today.year-4),'Kpler '+str(today.year-3),'Kpler '+str(today.year-2),'Kpler '+str(today.year-1)]].min(axis=1)
        demand_global_yoy['Kpler '+str(today.year-0)].loc[current_year_start:current_year_end] = demand_global.loc[current_year_start:current_year_end, 'Kpler'].values
        demand_global_yoy['Desk View'] = demand_global.loc[current_year_start:current_year_end, 'Desk View'].values
        demand_global_yoy['Desk View '+str(today.year+1)] = demand_global.loc[next_year_start:year_end, 'Desk View'].values
        
        demand_global_yoy['Difference'] = demand_global.loc[current_year_start:current_year_end, 'Difference'].values
        demand_global_yoy['IHS Contract'] = demand_global.loc[current_year_start:current_year_end, 'IHS Contract'].values
        demand_global_yoy=demand_global_yoy.round()
        FlowMA30Chart.plot_chart(demand_global_yoy, 'demand', 'Global', today)

    '''Basin supply, all plant and cum.'''
    def Basin_Chart_supply(df_basin, date_dict):
    
        today=datetime.date.today()
        x_index=pd.date_range(date_dict['current_year_start'],date_dict['current_year_end'])
        fig_Basin = go.Figure() 
        
        fig_Basin.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin['MENA'].loc[date_dict['current_year_start']:date_dict['current_year_end']],
                    mode="lines",
                    name='MENA '+str(today.year),
                    showlegend=True,
                    line=dict(color='LightGrey', width=2, dash='solid')
                    
                    )
                )
        
        fig_Basin.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin['MENA'].loc[date_dict['last_year_start']:date_dict['last_year_end']],
                    mode="lines",
                    name='MENA '+str(today.year-1),
                    showlegend=True,
                    line=dict(color='LightGrey', width=2, dash='dot')
                    
                    )
                )
        
        fig_Basin.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin['MENA Desk'].loc[date_dict['current_year_start']:date_dict['current_year_end']],
                    mode="lines",
                    name='MENA Desk '+str(today.year),
                    showlegend=True,
                    line=dict(color='LightGrey', width=2, dash='dash')
                    
                    )
                )
        
        fig_Basin.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin['PB'].loc[date_dict['current_year_start']:date_dict['current_year_end']],
                    mode="lines",
                    name='PB '+str(today.year),
                    showlegend=True,
                    line=dict(color='black', width=2, dash='solid')
                    
                    )
                )
        
        fig_Basin.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin['PB'].loc[date_dict['last_year_start']:date_dict['last_year_end']],
                    mode="lines",
                    name='PB '+str(today.year-1),
                    showlegend=True,
                    line=dict(color='black', width=2, dash='dot')
                    
                    )
                )
        
        fig_Basin.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin['PB Desk'].loc[date_dict['current_year_start']:date_dict['current_year_end']],
                    mode="lines",
                    name='PB Desk '+str(today.year),
                    showlegend=True,
                    line=dict(color='black', width=2, dash='dash')
                    
                    )
                )
        
        fig_Basin.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin['AB'].loc[date_dict['current_year_start']:date_dict['current_year_end']],
                    mode="lines",
                    name='AB '+str(today.year),
                    showlegend=True,
                    line=dict(color='red', width=2, dash='solid')
                    
                    )
                )
        
        fig_Basin.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin['AB'].loc[date_dict['last_year_start']:date_dict['last_year_end']],
                    mode="lines",
                    name='AB '+str(today.year-1),
                    showlegend=True,
                    line=dict(color='red', width=2, dash='dot')
                    
                    )
                )
        
        fig_Basin.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin['AB Desk'].loc[date_dict['current_year_start']:date_dict['current_year_end']],
                    mode="lines",
                    name='AB Desk '+str(today.year),
                    showlegend=True,
                    line=dict(color='red', width=2, dash='dash')
                    
                    )
                )
        
        fig_Basin.update_layout(
            autosize=True,
            showlegend=True,
            legend=dict(x=0, y=-0.2),
            legend_orientation="h",
            title_text='Basin Supply, 30 days MA, Mcm/d '+ str(today),
            yaxis_title="Mcm/d",
            xaxis = dict(dtick="M1"),
            hovermode='x unified',
            plot_bgcolor = 'white',
            template='ggplot2'
              
        )
        fig_Basin.update_xaxes(
            showline=True, 
            linewidth=1, 
            linecolor='LightGrey', 
            showgrid=False, 
            gridwidth=1, 
            gridcolor='LightGrey'
        )
        fig_Basin.update_yaxes(
            showline=True, 
            linewidth=1, 
            linecolor='LightGrey', 
            showgrid=True, 
            gridwidth=1, 
            gridcolor='LightGrey'
        )
    
        
        fig_MENA = go.Figure() 
        
        fig_MENA.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin['MENA'].loc[date_dict['current_year_start']:date_dict['current_year_end']],
                    mode="lines",
                    name='MENA '+str(today.year),
                    showlegend=True,
                    line=dict(color='Gray', width=2, dash='solid')
                    
                    )
                )
        
        fig_MENA.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin['MENA'].loc[date_dict['last_year_start']:date_dict['last_year_end']],
                    mode="lines",
                    name='MENA '+str(today.year-1),
                    showlegend=True,
                    line=dict(color='Gray', width=2, dash='dot')
                    
                    )
                )
        
        fig_MENA.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin['MENA Desk'].loc[date_dict['current_year_start']:date_dict['current_year_end']],
                    mode="lines",
                    name='MENA Desk '+str(today.year),
                    showlegend=True,
                    line=dict(color='Gray', width=2, dash='dash')
                    
                    )
                )
        
        fig_MENA.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin['Ras Laffan'].loc[date_dict['current_year_start']:date_dict['current_year_end']],
                    mode="lines",
                    name='Ras lfa '+str(today.year),
                    showlegend=True,
                    line=dict(color='black', width=2, dash='solid')
                    
                    )
                )
        
        fig_MENA.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin['Ras Laffan'].loc[date_dict['last_year_start']:date_dict['last_year_end']],
                    mode="lines",
                    name='Ras lfa '+str(today.year-1),
                    showlegend=True,
                    line=dict(color='black', width=2, dash='dot')
                    
                    )
                )
        
        fig_MENA.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin['Ras Laffan Desk'].loc[date_dict['current_year_start']:date_dict['current_year_end']],
                    mode="lines",
                    name='Desk Ras lfa '+str(today.year),
                    showlegend=True,
                    line=dict(color='black', width=2, dash='dash')
                    
                    )
                )
        
        fig_MENA.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin[['Skikda','Bethioua']].sum(axis=1).loc[date_dict['current_year_start']:date_dict['today']],
                    mode="lines",
                    name='Alg '+str(today.year),
                    showlegend=True,
                    line=dict(color='#FFC000', width=2, dash='solid')
                    
                    )
                )
        
        fig_MENA.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin[['Skikda','Bethioua']].loc[date_dict['last_year_start']:date_dict['last_year_end']].sum(axis=1),
                    mode="lines",
                    name='Alg '+str(today.year-1),
                    showlegend=True,
                    line=dict(color='#FFC000', width=2, dash='dot')
                    
                    )
                )
        
        fig_MENA.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin[['Skikda Desk','Bethioua Desk']].loc[date_dict['current_year_start']:date_dict['current_year_end']].sum(axis=1),
                    mode="lines",
                    name='Desk Alg '+str(today.year),
                    showlegend=True,
                    line=dict(color='#FFC000', width=2, dash='dash')
                    
                    )
                )
        
        fig_MENA.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin[['Damietta','Idku']].sum(axis=1).loc[date_dict['current_year_start']:date_dict['today']],
                    mode="lines",
                    name='Egypt '+str(today.year),
                    showlegend=True,
                    line=dict(color='#ED7D31', width=2, dash='solid')
                    
                    )
                )
        
        fig_MENA.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin[['Damietta','Idku']].sum(axis=1).loc[date_dict['last_year_start']:date_dict['last_year_end']],
                    mode="lines",
                    name='Egypt '+str(today.year-1),
                    showlegend=True,
                    line=dict(color='#ED7D31', width=2, dash='dot')
                    
                    )
                )
        
        fig_MENA.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin[['Damietta Desk','Idku Desk']].loc[date_dict['current_year_start']:date_dict['current_year_end']].sum(axis=1),
                    mode="lines",
                    name='Desk Egypt '+str(today.year),
                    showlegend=True,
                    line=dict(color='#ED7D31', width=2, dash='dash')
                    
                    )
                )
        
        fig_MENA.update_layout(
            autosize=True,
            showlegend=True,
            legend=dict(x=0, y=-0.2),
            legend_orientation="h",
            title_text='MENA Supply, 30 days MA, Mcm/d '+ str(today),
            yaxis_title="Mcm/d",
            xaxis = dict(dtick="M1"),
            hovermode='x unified',
            plot_bgcolor = 'white',
            template='ggplot2'
              
        )
        fig_MENA.update_xaxes(
            showline=True, 
            linewidth=1, 
            linecolor='LightGrey', 
            showgrid=False, 
            gridwidth=1, 
            gridcolor='LightGrey'
        )
        fig_MENA.update_yaxes(
            showline=True, 
            linewidth=1, 
            linecolor='LightGrey', 
            showgrid=True, 
            gridwidth=1, 
            gridcolor='LightGrey'
        )
        
        
        fig_PB = go.Figure() 
        
        fig_PB.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin['PB'].loc[date_dict['current_year_start']:date_dict['current_year_end']],
                    mode="lines",
                    name='PB '+str(today.year),
                    showlegend=True,
                    line=dict(color='black', width=2, dash='solid')
                    
                    )
                )
        
        fig_PB.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin['PB'].loc[date_dict['last_year_start']:date_dict['last_year_end']],
                    mode="lines",
                    name='PB '+str(today.year-1),
                    showlegend=True,
                    line=dict(color='black', width=2, dash='dot')
                    
                    )
                )
        
        fig_PB.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin['PB Desk'].loc[date_dict['current_year_start']:date_dict['current_year_end']],
                    mode="lines",
                    name='PB Desk '+str(today.year),
                    showlegend=True,
                    line=dict(color='black', width=2, dash='dash')
                    
                    )
                )
        
        fig_PB.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin[['APLNG','Darwin','GLNG','Gorgon','NWS','Pluto','QCLNG','Wheatstone','Ichthys','Prelude']].sum(axis=1).loc[date_dict['current_year_start']:date_dict['today']],
                    mode="lines",
                    name='Aust '+str(today.year),
                    showlegend=True,
                    line=dict(color='red', width=2, dash='solid')
                    
                    )
                )
        
        fig_PB.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin[['APLNG','Darwin','GLNG','Gorgon','NWS','Pluto','QCLNG','Wheatstone','Ichthys','Prelude']].loc[date_dict['last_year_start']:date_dict['last_year_end']].sum(axis=1),
                    mode="lines",
                    name='Aust '+str(today.year-1),
                    showlegend=True,
                    line=dict(color='red', width=2, dash='dot')
                    
                    )
                )
        
        fig_PB.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin[['APLNG Desk','Darwin Desk','GLNG Desk','Gorgon Desk','NWS Desk','Pluto Desk','QCLNG Desk','Wheatstone Desk','Ichthys Desk','Prelude Desk']].loc[date_dict['current_year_start']:date_dict['current_year_end']].sum(axis=1),
                    mode="lines",
                    name='Desk Aust '+str(today.year),
                    showlegend=True,
                    line=dict(color='red', width=2, dash='dash')
                    
                    )
                )
        
        fig_PB.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin[['Bintulu','PFLNG 1 Sabah']].sum(axis=1).loc[date_dict['current_year_start']:date_dict['today']],
                    mode="lines",
                    name='Malay '+str(today.year),
                    showlegend=True,
                    line=dict(color='#7030A0', width=2, dash='solid')
                    
                    )
                )
        
        fig_PB.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin[['Bintulu','PFLNG 1 Sabah']].loc[date_dict['last_year_start']:date_dict['last_year_end']].sum(axis=1),
                    mode="lines",
                    name='Malay '+str(today.year-1),
                    showlegend=True,
                    line=dict(color='#7030A0', width=2, dash='dot')
                    
                    )
                )
        
        fig_PB.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin[['Bintulu Desk','PFLNG 1 Sabah Desk']].loc[date_dict['current_year_start']:date_dict['current_year_end']].sum(axis=1),
                    mode="lines",
                    name='Desk Malay '+str(today.year),
                    showlegend=True,
                    line=dict(color='#7030A0', width=2, dash='dash')
                    
                    )
                )
        
        fig_PB.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin[['Bontang','Tangguh','DSLNG']].sum(axis=1).loc[date_dict['current_year_start']:date_dict['today']],
                    mode="lines",
                    name='Indo '+str(today.year),
                    showlegend=True,
                    line=dict(color='#00B0F0', width=2, dash='solid')
                    
                    )
                )
        
        fig_PB.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin[['Bontang','Tangguh','DSLNG']].loc[date_dict['last_year_start']:date_dict['last_year_end']].sum(axis=1),
                    mode="lines",
                    name='Indo '+str(today.year-1),
                    showlegend=True,
                    line=dict(color='#00B0F0', width=2, dash='dot')
                    
                    )
                )
        
        fig_PB.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin[['Bontang Desk','Tangguh Desk','DSLNG Desk']].loc[date_dict['current_year_start']:date_dict['current_year_end']].sum(axis=1),
                    mode="lines",
                    name='Desk Indo '+str(today.year),
                    showlegend=True,
                    line=dict(color='#00B0F0', width=2, dash='dash')
                    
                    )
                )
        
        fig_PB.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin['PNG LNG'].loc[date_dict['current_year_start']:date_dict['today']],
                    mode="lines",
                    name='PNG '+str(today.year),
                    showlegend=True,
                    line=dict(color='#7F7F7F', width=2, dash='solid')
                    
                    )
                )
        
        fig_PB.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin['PNG LNG'].loc[date_dict['last_year_start']:date_dict['last_year_end']],
                    mode="lines",
                    name='PNG '+str(today.year-1),
                    showlegend=True,
                    line=dict(color='#7F7F7F', width=2, dash='dot')
                    
                    )
                )
        
        fig_PB.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin['PNG LNG Desk'].loc[date_dict['current_year_start']:date_dict['current_year_end']],
                    mode="lines",
                    name='PNG Desk '+str(today.year),
                    showlegend=True,
                    line=dict(color='#7F7F7F', width=2, dash='dash')
                    
                    )
                )
        
        
        fig_PB.update_layout(
            autosize=True,
            showlegend=True,
            legend=dict(x=0, y=-0.2),
            legend_orientation="h",
            title_text='PB Supply, 30 days MA, Mcm/d '+ str(today),
            yaxis_title="Mcm/d",
            xaxis = dict(dtick="M1"),
            hovermode='x unified',
            plot_bgcolor = 'white',
            template='ggplot2'
              
        )
        fig_PB.update_xaxes(
            showline=True, 
            linewidth=1, 
            linecolor='LightGrey', 
            showgrid=False, 
            gridwidth=1, 
            gridcolor='LightGrey'
        )
        fig_PB.update_yaxes(
            showline=True, 
            linewidth=1, 
            linecolor='LightGrey', 
            showgrid=True, 
            gridwidth=1, 
            gridcolor='LightGrey'
        )
        
            
        fig_AB = go.Figure() 
        
        fig_AB.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin['AB'].loc[date_dict['current_year_start']:date_dict['current_year_end']],
                    mode="lines",
                    name='AB '+str(today.year),
                    showlegend=True,
                    line=dict(color='black', width=2, dash='solid')
                    
                    )
                )
        
        fig_AB.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin['AB'].loc[date_dict['last_year_start']:date_dict['last_year_end']],
                    mode="lines",
                    name='AB'+str(today.year-1),
                    showlegend=True,
                    line=dict(color='black', width=2, dash='dot')
                    
                    )
                )
        
        fig_AB.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin['AB Desk'].loc[date_dict['current_year_start']:date_dict['current_year_end']],
                    mode="lines",
                    name='AB Desk '+str(today.year),
                    showlegend=True,
                    line=dict(color='black', width=2, dash='dash')
                    
                    )
                )
        #NO Calcasieu Pass for US
        fig_AB.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin[['Cameron (Liqu.)','Cove Point','Sabine Pass','Corpus Christi','Elba Island Liq.','Freeport']].sum(axis=1).loc[date_dict['current_year_start']:date_dict['today']],
                    mode="lines",
                    name='US '+str(today.year),
                    showlegend=True,
                    line=dict(color='red', width=2, dash='solid')
                    
                    )
                )
        
        fig_AB.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin[['Cameron (Liqu.)','Cove Point','Sabine Pass','Corpus Christi','Elba Island Liq.','Freeport']].loc[date_dict['last_year_start']:date_dict['last_year_end']].sum(axis=1),
                    mode="lines",
                    name='US '+str(today.year-1),
                    showlegend=True,
                    line=dict(color='red', width=2, dash='dot')
                    
                    )
                )
        
        fig_AB.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin[['Cameron (Liqu.) Desk','Cove Point Desk','Sabine Pass Desk','Corpus Christi Desk','Elba Island Liq. Desk','Freeport Desk']].loc[date_dict['current_year_start']:date_dict['current_year_end']].sum(axis=1),
                    mode="lines",
                    name='Desk US '+str(today.year),
                    showlegend=True,
                    line=dict(color='red', width=2, dash='dash')
                    
                    )
                )
        
        fig_AB.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin['Bonny LNG'].loc[date_dict['current_year_start']:date_dict['today']],
                    mode="lines",
                    name='Bonny '+str(today.year),
                    showlegend=True,
                    line=dict(color='#00B050', width=2, dash='solid')
                    
                    )
                )
        
        fig_AB.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin['Bonny LNG'].loc[date_dict['last_year_start']:date_dict['last_year_end']],
                    mode="lines",
                    name='Bonny '+str(today.year-1),
                    showlegend=True,
                    line=dict(color='#00B050', width=2, dash='dot')
                    
                    )
                )
        
        fig_AB.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin['Bonny LNG Desk'].loc[date_dict['current_year_start']:date_dict['current_year_end']],
                    mode="lines",
                    name='Desk Bonny '+str(today.year),
                    showlegend=True,
                    line=dict(color='#00B050', width=2, dash='dash')
                    
                    )
                )
        
        fig_AB.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin['Atlantic LNG'].loc[date_dict['current_year_start']:date_dict['today']],
                    mode="lines",
                    name='T&T '+str(today.year),
                    showlegend=True,
                    line=dict(color='#FFC000', width=2, dash='solid')
                    
                    )
                )
        
        fig_AB.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin['Atlantic LNG'].loc[date_dict['last_year_start']:date_dict['last_year_end']],
                    mode="lines",
                    name='T&T '+str(today.year-1),
                    showlegend=True,
                    line=dict(color='#FFC000', width=2, dash='dot')
                    
                    )
                )
        
        fig_AB.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin['Atlantic LNG Desk'].loc[date_dict['current_year_start']:date_dict['current_year_end']],
                    mode="lines",
                    name='Desk T&T '+str(today.year),
                    showlegend=True,
                    line=dict(color='#FFC000', width=2, dash='dash')
                    
                    )
                )
        
        fig_AB.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin['Yamal'].loc[date_dict['current_year_start']:date_dict['today']],
                    mode="lines",
                    name='Yamal '+str(today.year),
                    showlegend=True,
                    line=dict(color='#A6A6A6', width=2, dash='solid')
                    
                    )
                )
        
        fig_AB.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin['Yamal'].loc[date_dict['last_year_start']:date_dict['last_year_end']],
                    mode="lines",
                    name='Yamal '+str(today.year-1),
                    showlegend=True,
                    line=dict(color='#A6A6A6', width=2, dash='dot')
                    
                    )
                )
        
        fig_AB.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin['Yamal Desk'].loc[date_dict['current_year_start']:date_dict['current_year_end']],
                    mode="lines",
                    name='Yamal Desk '+str(today.year),
                    showlegend=True,
                    line=dict(color='#A6A6A6', width=2, dash='dash')
                    
                    )
                )
        
        
        fig_AB.update_layout(
            autosize=True,
            showlegend=True,
            legend=dict(x=0, y=-0.2),
            legend_orientation="h",
            title_text='AB Supply, 30 days MA, Mcm/d '+ str(today),
            yaxis_title="Mcm/d",
            xaxis = dict(dtick="M1"),
            hovermode='x unified',
            plot_bgcolor = 'white',
            template='ggplot2'
              
        )
        fig_AB.update_xaxes(
            showline=True, 
            linewidth=1, 
            linecolor='LightGrey', 
            showgrid=False, 
            gridwidth=1, 
            gridcolor='LightGrey'
        )
        fig_AB.update_yaxes(
            showline=True, 
            linewidth=1, 
            linecolor='LightGrey', 
            showgrid=True, 
            gridwidth=1, 
            gridcolor='LightGrey'
        )
        
        
        fig_EoS = go.Figure() 
        
        fig_EoS.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin['EoS'].loc[date_dict['current_year_start']:date_dict['current_year_end']],
                    mode="lines",
                    name='Kpler '+str(today.year),
                    showlegend=True,
                    line=dict(color='black', width=2, dash='solid')
                    
                    )
                )
        
        fig_EoS.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin['EoS'].loc[date_dict['last_year_start']:date_dict['last_year_end']],
                    mode="lines",
                    name='Kpler '+str(today.year-1),
                    showlegend=True,
                    line=dict(color='LightGrey', width=2, dash='dot')
                    
                    )
                )
        
        fig_EoS.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin['EoS Desk'].loc[date_dict['current_year_start']:date_dict['current_year_end']],
                    mode="lines",
                    name='Desk view',
                    showlegend=True,
                    line=dict(color='red', width=2, dash='solid')
                    
                    )
                )
        
        fig_EoS.add_trace(
                    go.Bar(
                        x=x_index,
                        y=df_basin['EoS Difference'].loc[date_dict['current_year_start']:date_dict['last_month']].values,
                        name='Kpler '+str(today.year)+' vs. Desk '+str(today.year)+' delta',
                        #width=1,
                    
                        showlegend=True,
                        marker_color='Silver',
                        #text=df_cumulative[p+' Cum Difference'].loc[:last_month].values,
                        #textposition='auto',
                        )
                    )       
        
        fig_EoS.update_layout(
            autosize=True,
            showlegend=True,
            legend=dict(x=0, y=-0.2),
            legend_orientation="h",
            title_text='EoS, 30 days MA, Mcm/d '+ str(today),
            yaxis_title="Exports (Mcm/d)",
            xaxis = dict(dtick="M1"),
            hovermode='x unified',
            plot_bgcolor = 'white',
            template='ggplot2'
              
        )
        fig_EoS.update_xaxes(
            showline=True, 
            linewidth=1, 
            linecolor='LightGrey', 
            showgrid=False, 
            gridwidth=1, 
            gridcolor='LightGrey'
        )
        fig_EoS.update_yaxes(
            showline=True, 
            linewidth=1, 
            linecolor='LightGrey', 
            showgrid=True, 
            gridwidth=1, 
            gridcolor='LightGrey'
        )
    
        
        fig_WoS = go.Figure() 
        
        fig_WoS.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin['WoS'].loc[date_dict['current_year_start']:date_dict['current_year_end']],
                    mode="lines",
                    name='Kpler '+str(today.year),
                    showlegend=True,
                    line=dict(color='black', width=2, dash='solid')
                    
                    )
                )
        
        fig_WoS.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin['WoS'].loc[date_dict['last_year_start']:date_dict['last_year_end']],
                    mode="lines",
                    name='Kpler '+str(today.year-1),
                    showlegend=True,
                    line=dict(color='LightGrey', width=2, dash='dot')
                    
                    )
                )
        
        fig_WoS.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin['WoS Desk'].loc[date_dict['current_year_start']:date_dict['current_year_end']],
                    mode="lines",
                    name='Desk view',
                    showlegend=True,
                    line=dict(color='red', width=2, dash='solid')
                    
                    )
                )
        
        fig_WoS.add_trace(
                    go.Bar(
                        x=x_index,
                        y=df_basin['WoS Difference'].loc[date_dict['current_year_start']:date_dict['last_month']].values,
                        name='Kpler '+str(today.year)+' vs. Desk '+str(today.year)+' delta',
                        #width=1,
                    
                        showlegend=True,
                        marker_color='Silver',
                        #text=df_cumulative[p+' Cum Difference'].loc[:last_month].values,
                        #textposition='auto',
                        )
                    )       
        
        fig_WoS.update_layout(
            autosize=True,
            showlegend=True,
            legend=dict(x=0, y=-0.2),
            legend_orientation="h",
            title_text='WoS, 30 days MA, Mcm/d '+ str(today),
            yaxis_title="Exports (Mcm/d)",
            xaxis = dict(dtick="M1"),
            hovermode='x unified',
            plot_bgcolor = 'white',
            template='ggplot2'
              
        )
        fig_WoS.update_xaxes(
            showline=True, 
            linewidth=1, 
            linecolor='LightGrey', 
            showgrid=False, 
            gridwidth=1, 
            gridcolor='LightGrey'
        )
        fig_WoS.update_yaxes(
            showline=True, 
            linewidth=1, 
            linecolor='LightGrey', 
            showgrid=True, 
            gridwidth=1, 
            gridcolor='LightGrey'
        )
        
        py.plot(fig_MENA, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/Flow30/daily/supply/regional/MENA Supply.html', auto_open=False)
        py.plot(fig_Basin, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/Flow30/daily/supply/regional/Basin Supply.html', auto_open=False)
        py.plot(fig_PB, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/Flow30/daily/supply/regional/PB Supply.html', auto_open=False)
        py.plot(fig_AB, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/Flow30/daily/supply/regional/AB Supply.html', auto_open=False)
        py.plot(fig_EoS, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/Flow30/daily/supply/regional/EoS Supply.html', auto_open=False)
        py.plot(fig_WoS, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/Flow30/daily/supply/regional/WoS Supply.html', auto_open=False)
        
        fig_MENA.write_image('U:/Trading - Gas/LNG/LNG website/Flow30/daily/supply/regional/MENA Supply.png')
        fig_Basin.write_image('U:/Trading - Gas/LNG/LNG website/Flow30/daily/supply/regional/Basin Supply.png')
        fig_PB.write_image('U:/Trading - Gas/LNG/LNG website/Flow30/daily/supply/regional/PB Supply.png')
        fig_AB.write_image('U:/Trading - Gas/LNG/LNG website/Flow30/daily/supply/regional/AB Supply.png')
        fig_EoS.write_image('U:/Trading - Gas/LNG/LNG website/Flow30/daily/supply/regional/EoS Supply.png')
        fig_WoS.write_image('U:/Trading - Gas/LNG/LNG website/Flow30/daily/supply/regional/WoS Supply.png')
    
    
    
    def Supply_Cumulative_Chart(df_cumulative, date_dict, SupplyCategories):
        
        today=datetime.date.today()
        last_month= date_dict['last_month']+'-01'
        x_index=pd.date_range(date_dict['current_year_start'],date_dict['current_year_end'], freq='MS')

        BasinSuez = ['AB', 'PB', 'MENA','Global', 'EoS', 'WoS']
    
        allplant = SupplyCategories['Plant'].values.tolist()
        #allplant.remove('Mozambique Area 1')
        #allplant.remove('Calcasieu Pass')
        
        #SupplyCategories.drop(SupplyCategories[SupplyCategories['Plant']=='Calcasieu Pass'].index, inplace=True)
        #SupplyCategories.drop(SupplyCategories[SupplyCategories['Plant']=='Mozambique Area 1'].index, inplace=True)
        market_list = SupplyCategories['Market']
        market_list.drop_duplicates(inplace=True)
        market_list = market_list.tolist()   
       
        
        
        #PLot Basin and Suez cumsum
        for p in BasinSuez:
            fig_Cumsum_Basin = go.Figure() 
            
            fig_Cumsum_Basin.add_trace(
                    go.Scatter(
                        x=x_index,
                        y=df_cumulative[p+' Cum'].loc[date_dict['current_year_start']:last_month],
                        mode="lines",
                        name='Kpler '+str(today.year),
                        showlegend=True,
                        line=dict(color='black', width=2, dash='solid')
                        
                        )
                    )
            fig_Cumsum_Basin.add_trace(
                    go.Scatter(
                        x=x_index,
                        y=df_cumulative[p+' Desk Cum'].loc[date_dict['current_year_start']:date_dict['current_year_end']],
                        mode="lines",
                        name='Desk view',
                        showlegend=True,
                        line=dict(color='red', width=2, dash='solid')
                        
                        )
                    )
            
            fig_Cumsum_Basin.add_trace(
                    go.Scatter(
                        x=x_index,
                        y=df_cumulative[p+' Desk Cum'].loc[date_dict['next_year_start']:date_dict['next_year_end']],
                        mode="lines",
                        name='Desk view '+str(today.year+1),
                        showlegend=True,
                        line=dict(color='red', width=2, dash='dot')
                        
                        )
                    )
            
            fig_Cumsum_Basin.add_trace(
                    go.Scatter(
                        x=x_index,
                        y=df_cumulative[p+' Cum'].loc[date_dict['last_year_start']:date_dict['last_year_end']],
                        mode="lines",
                        name='Kpler '+str(today.year-1),
                        showlegend=True,
                        line=dict(color='#7F7F7F', width=2, dash='dash')
                        
                        )
                    )
            fig_Cumsum_Basin.add_trace(
                    go.Bar(
                        x=x_index,
                        y=df_cumulative[p+' Cum Difference'].loc[date_dict['current_year_start']:date_dict['last_month']],
                        name='Kpler '+str(today.year)+' vs. Desk '+str(today.year)+' delta',
                        #width=1,
                    
                        showlegend=True,
                        marker_color='Silver',
                        #text=df_cumulative[p+' Cum Difference'].loc[:last_month].values,
                        #textposition='auto',
                        )
                    )       
            fig_Cumsum_Basin.add_annotation(x=last_month, y=df_cumulative[p+' Cum'].loc[last_month],
                    text = str(df_cumulative[p+' Cum'].loc[last_month])+' (Kpler '+str(today.year)+')',
                    showarrow=True,
                    arrowhead=5)
            fig_Cumsum_Basin.add_annotation(x=x_index[-1], y=df_cumulative[p+' Cum'].loc[date_dict['last_year_month']],
                    text = str(df_cumulative[p+' Cum'].loc[date_dict['last_year_month']])+' (Kpler '+str(today.year-1)+')',
                    showarrow=True,
                    ax=10,
                    ay=30,
                    arrowhead=5)  
            fig_Cumsum_Basin.add_annotation(x=x_index[-1], y=df_cumulative[p+' Desk Cum'].loc[date_dict['currentt_year_end_month']],
                    text=str(df_cumulative[p+' Desk Cum'].loc[date_dict['currentt_year_end_month']]) + ' (Desk view '+str(today.year)+')',
                    showarrow=True,
                    arrowhead=5)
            
            bardate=[]
            
            for i in range (1,today.month):
                this_month_start = datetime.datetime(today.year, i , 1)
                bardate.append(this_month_start.strftime("%Y-%m-%d"))
            for j in range(0, today.month -1 ):
                fig_Cumsum_Basin.add_annotation(x=bardate[j], y=5,
                        text=str(df_cumulative[p+' Cum Difference'].loc[bardate[j]]),
                        showarrow=False)
            #print(Basin)    
            fig_Cumsum_Basin.update_layout(
                autosize=True,
                showlegend=True,
                legend=dict(x=0, y=-0.2),
                legend_orientation="h",
                title_text=p+' Cum. '+ str(today),
                yaxis_title="Exports (Mt)",
                xaxis = dict(dtick="M1"),
                hovermode='x unified',
                plot_bgcolor = 'white',
                template='ggplot2'  ,
                bargap  = 0
            )
            fig_Cumsum_Basin.update_xaxes(
                showline=True, 
                linewidth=1, 
                linecolor='LightGrey', 
                showgrid=False, 
                gridwidth=1, 
                gridcolor='LightGrey'
            )
            fig_Cumsum_Basin.update_yaxes(
                showline=True, 
                linewidth=1, 
                linecolor='LightGrey', 
                showgrid=True, 
                gridwidth=1, 
                gridcolor='LightGrey'
            )
            py.plot(fig_Cumsum_Basin, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/Flow30/cumulate/supply/BasinSuez/'+p+' Cum Supply.html', auto_open=False)    
            fig_Cumsum_Basin.write_image('U:/Trading - Gas/LNG/LNG website/Flow30/cumulate/supply/BasinSuez/'+p+' Cum Supply.png')
        
        #creat all plant charts
        for q in allplant:
            fig_Cumsum_Plant = go.Figure() 
            
            fig_Cumsum_Plant.add_trace(
                    go.Scatter(
                        x=x_index,
                        y=df_cumulative[q+' Cum'].loc[date_dict['current_year_start']:last_month],
                        mode="lines",
                        name='Kpler '+str(today.year),
                        showlegend=True,
                        line=dict(color='black', width=2, dash='solid')
                        
                        )
                    )
            fig_Cumsum_Plant.add_trace(
                    go.Scatter(
                        x=x_index,
                        y=df_cumulative[q+' Desk Cum'].loc[date_dict['current_year_start']:date_dict['current_year_end']],
                        mode="lines",
                        name='Desk view',
                        showlegend=True,
                        line=dict(color='red', width=2, dash='solid')
                        
                        )
                    )
            fig_Cumsum_Plant.add_trace(
                    go.Scatter(
                        x=x_index,
                        y=df_cumulative[q+' Desk Cum'].loc[date_dict['next_year_start']:date_dict['next_year_end']],
                        mode="lines",
                        name='Desk view '+str(today.year+1),
                        showlegend=True,
                        line=dict(color='red', width=2, dash='dot')
                        
                        )
                    )
            fig_Cumsum_Plant.add_trace(
                    go.Scatter(
                        x=x_index,
                        y=df_cumulative[q+' Cum'].loc[date_dict['last_year_start']:date_dict['last_year_end']],
                        mode="lines",
                        name='Kpler '+str(today.year-1),
                        showlegend=True,
                        line=dict(color='#7F7F7F', width=2, dash='dash')
                        
                        )
                    )
            fig_Cumsum_Plant.add_trace(
                    go.Bar(
                        x=x_index,
                        y=df_cumulative[q+' Cum Difference'].loc[date_dict['current_year_start']:last_month],
                        name='Kpler '+str(today.year)+' vs. Desk '+str(today.year)+' delta',
                        #width=1,
                        showlegend=True,
                        marker_color='Silver',
                        #text=df_cumulative[q+' Cum Difference'].loc[:last_month],
                        #textposition='auto'
                        )
                    )       
            fig_Cumsum_Plant.add_annotation(x=last_month, y=df_cumulative[q+' Cum'].loc[last_month],
                    text = str(df_cumulative[q+' Cum'].loc[last_month])+' (Kpler '+str(today.year)+')',
                    showarrow=True,
                    arrowhead=5)
            fig_Cumsum_Plant.add_annotation(x=x_index[-1], y=df_cumulative[q+' Cum'].loc[date_dict['last_year_month']],
                    text = str(df_cumulative[q+' Cum'].loc[date_dict['last_year_month']])+' (Kpler '+str(today.year-1)+')',
                    showarrow=True,
                    ax=10,
                    ay=30,
                    arrowhead=5)   
            fig_Cumsum_Plant.add_annotation(x=x_index[-1], y=df_cumulative[q+' Desk Cum'].loc[date_dict['currentt_year_end_month']],
                    text=str(df_cumulative[q+' Desk Cum'].loc[date_dict['currentt_year_end_month']]) + ' (Desk view '+str(today.year)+')',
                    showarrow=True,
                    arrowhead=5)
            
            bardate=[]
            
            for i in range (1,today.month):
                this_month_start = datetime.datetime(today.year, i , 1)
                bardate.append(this_month_start.strftime("%Y-%m-%d"))
            for j in range(0, today.month -1 ):
                fig_Cumsum_Plant.add_annotation(x=bardate[j], y=df_cumulative[q+' Cum Difference'].loc[bardate].min()+0.5,
                        text=df_cumulative[q+' Cum Difference'].loc[bardate[j]],
                        showarrow=False)
                
            fig_Cumsum_Plant.update_layout(
                autosize=True,
                showlegend=True,
                legend=dict(x=0, y=-0.2),
                legend_orientation="h",
                title_text=q+' Cum. '+ str(today),
                yaxis_title="Exports (Mt)",
                xaxis = dict(dtick="M1"),
                hovermode='x unified',
                plot_bgcolor = 'white',
                template='ggplot2'  ,
                bargap  = 0
            )
            fig_Cumsum_Plant.update_xaxes(
                showline=True, 
                linewidth=1, 
                linecolor='LightGrey', 
                showgrid=False, 
                gridwidth=1, 
                gridcolor='LightGrey'
            )
            fig_Cumsum_Plant.update_yaxes(
                showline=True, 
                linewidth=1, 
                linecolor='LightGrey', 
                showgrid=True, 
                gridwidth=1, 
                gridcolor='LightGrey'
            )
            py.plot(fig_Cumsum_Plant, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/Flow30/cumulate/supply/all plant/'+q+' Cum Supply.html', auto_open=False)    
            fig_Cumsum_Plant.write_image('U:/Trading - Gas/LNG/LNG website/Flow30/cumulate/supply/all plant/'+q+' Cum Supply.png')
        #plot all market cumsum
        for m in market_list:
            fig_Cumsum_Market = go.Figure() 
            
            fig_Cumsum_Market.add_trace(
                    go.Scatter(
                        x=x_index,
                        y=df_cumulative[m+' Cum'].loc[date_dict['current_year_start']:last_month],
                        mode="lines",
                        name='Kpler '+str(today.year),
                        showlegend=True,
                        line=dict(color='black', width=2, dash='solid')
                        
                        )
                    )
            fig_Cumsum_Market.add_trace(
                    go.Scatter(
                        x=x_index,
                        y=df_cumulative[m+' Desk Cum'].loc[date_dict['current_year_start']:date_dict['current_year_end']],
                        mode="lines",
                        name='Desk view',
                        showlegend=True,
                        line=dict(color='red', width=2, dash='solid')
                        
                        )
                    )
            fig_Cumsum_Market.add_trace(
                    go.Scatter(
                        x=x_index,
                        y=df_cumulative[m+' Desk Cum'].loc[date_dict['next_year_start']:date_dict['next_year_end']],
                        mode="lines",
                        name='Desk view '+str(today.year+1),
                        showlegend=True,
                        line=dict(color='red', width=2, dash='dot')
                        
                        )
                    )
            fig_Cumsum_Market.add_trace(
                    go.Scatter(
                        x=x_index,
                        y=df_cumulative[m+' Cum'].loc[date_dict['last_year_start']:date_dict['last_year_end']],
                        mode="lines",
                        name='Kpler '+str(today.year-1),
                        showlegend=True,
                        line=dict(color='#7F7F7F', width=2, dash='dash')
                        
                        )
                    )
            fig_Cumsum_Market.add_trace(
                    go.Bar(
                        x=x_index,
                        y=df_cumulative[m+' Cum Difference'].loc[date_dict['current_year_start']:last_month],
                        name='Kpler '+str(today.year)+' vs. Desk '+str(today.year)+' delta',
                        #width=1,
                        showlegend=True,
                        marker_color='Silver',
                        #text=df_cumulative[q+' Cum Difference'].loc[:last_month],
                        #textposition='auto'
                        )
                    )       
            fig_Cumsum_Market.add_annotation(x=last_month, y=df_cumulative[m+' Cum'].loc[last_month],
                    text = str( df_cumulative[m+' Cum'].loc[last_month])+' (Kpler '+str(today.year)+')',
                    showarrow=True,
                    arrowhead=5)
            fig_Cumsum_Market.add_annotation(x=x_index[-1], y=df_cumulative[m+' Cum'].loc[date_dict['last_year_month']],
                    text = str(df_cumulative[m+' Cum'].loc[date_dict['last_year_month']])+' (Kpler '+str(today.year-1)+')',
                    showarrow=True,
                    ax=10,
                    ay=30,
                    arrowhead=5)  
            fig_Cumsum_Market.add_annotation(x=x_index[-1], y=df_cumulative[m+' Desk Cum'].loc[date_dict['currentt_year_end_month']],
                    text=str(df_cumulative[m+' Desk Cum'].loc[date_dict['currentt_year_end_month']]) + ' (Desk view '+str(today.year)+')',
                    showarrow=True,
                    arrowhead=5)
            
            bardate=[]
            
            for i in range (1,today.month):
                this_month_start = datetime.datetime(today.year, i , 1)
                bardate.append(this_month_start.strftime("%Y-%m-%d"))
            for j in range(0, today.month -1 ):
                fig_Cumsum_Market.add_annotation(x=bardate[j], y=df_cumulative[m+' Cum Difference'].loc[bardate].min()+0.5,
                        text=df_cumulative[m+' Cum Difference'].loc[bardate[j]],
                        showarrow=False)
                
            fig_Cumsum_Market.update_layout(
                autosize=True,
                showlegend=True,
                legend=dict(x=0, y=-0.2),
                legend_orientation="h",
                title_text=m+' Cum. '+ str(today),
                yaxis_title="Exports (Mt)",
                xaxis = dict(dtick="M1"),
                hovermode='x unified',
                plot_bgcolor = 'white',
                template='ggplot2'  ,
                bargap  = 0
            )
            fig_Cumsum_Market.update_xaxes(
                showline=True, 
                linewidth=1, 
                linecolor='LightGrey', 
                showgrid=False, 
                gridwidth=1, 
                gridcolor='LightGrey'
            )
            fig_Cumsum_Market.update_yaxes(
                showline=True, 
                linewidth=1, 
                linecolor='LightGrey', 
                showgrid=True, 
                gridwidth=1, 
                gridcolor='LightGrey'
            )
            py.plot(fig_Cumsum_Market, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/Flow30/cumulate/supply/all market/'+m+' Cum Supply.html', auto_open=False)    
            fig_Cumsum_Market.write_image('U:/Trading - Gas/LNG/LNG website/Flow30/cumulate/supply/all market/'+m+' Cum Supply.png')
    
    def all_plant_daily(df_basin, dfsupplyMA, dfsupplyMA_eta_plant, dfihscontractsupply, date_dict, SupplyCategories):
        
        #print(dfsupplyMA.loc['2016-01-01':'2016-12-31'])
        today=datetime.date.today()
        last_month= date_dict['last_month']+'-01'
        allplant = SupplyCategories['Plant'].values.tolist()
        #allplant.remove('Mozambique Area 1')
        #allplant.remove('Calcasieu Pass')
        #print(allplant)
        x_index=pd.date_range(date_dict['current_year_start'],date_dict['current_year_end'])
        dfeta = pd.DataFrame(index = x_index, columns = dfsupplyMA_eta_plant.columns)
        for i in pd.date_range(today+datetime.timedelta(days=1),today+datetime.timedelta(days=7)):
            dfeta.loc[i] = dfsupplyMA_eta_plant.loc[i]
        #dfihscontractsupply.rename(columns={'PFLNG 1':'PFLNG 1 Sabah', 'Elba Island':'Elba Island Liq.'}, inplace=True)
        #dfihscontractsupply.rename(columns={'Mozambique':'Coral South FLNG'}, inplace=True)
        #print(dfihscontractsupply['PFLNG 1 Sabah'])
        for p in allplant:
            #print(dfsupplyMA.loc['2016-01-01':'2016-12-31', p])
            supply_yoy=pd.DataFrame(index=pd.date_range(start=date_dict['current_year_start'], end=date_dict['current_year_end']))
            supply_yoy['Kpler 2016'] = np.delete(dfsupplyMA.loc['2016-01-01':'2016-12-31', p].values, 59)
            supply_yoy['Kpler 2017'] = dfsupplyMA.loc['2017-01-01':'2017-12-31', p].values
            supply_yoy['Kpler 2018'] = dfsupplyMA.loc['2018-01-01':'2018-12-31', p].values
            supply_yoy['Kpler 2019'] = dfsupplyMA.loc['2019-01-01':'2019-12-31', p].values
            supply_yoy['Kpler 2020'] = np.delete(dfsupplyMA.loc['2020-01-01':'2020-12-31', p].values, 59)
            supply_yoy['max']=supply_yoy[['Kpler 2016','Kpler 2017','Kpler 2018','Kpler 2019','Kpler 2020']].max(axis=1)
            supply_yoy['min']=supply_yoy[['Kpler 2016','Kpler 2017','Kpler 2018','Kpler 2019','Kpler 2020']].min(axis=1)
            
            fig_allplant_daily = go.Figure() 
            
            fig_allplant_daily.add_trace(
                go.Scatter(
                    x=x_index,
                    y=dfihscontractsupply[p].loc[date_dict['current_year_start']:date_dict['current_year_end']],
                    mode="lines",
                    name='IHS Contract 80%',
                    showlegend=True,
                    line=dict(color='#00B0F0', width=2, dash='dash')
                    )
                )
            
            fig_allplant_daily.add_trace(
                go.Scatter(
                    x=x_index,
                    y=dfihscontractsupply[p].loc[date_dict['current_year_start']:date_dict['current_year_end']]/0.8,
                    mode="lines",
                    name='IHS Contract 100%',
                    showlegend=True,
                    line=dict(color='#00B0F0', width=2, dash='dot')
                    )
                )
            
            fig_allplant_daily.add_trace(
                    go.Scatter(
                        x=x_index,
                        y=df_basin[p].loc[date_dict['current_year_start']:date_dict['today']],
                        mode="lines",
                        name='Kpler '+str(today.year),
                        showlegend=True,
                        line=dict(color='black', width=2, dash='solid')
                        
                        )
                    )
            fig_allplant_daily.add_trace(
                    go.Scatter(
                        x=x_index,
                        y=df_basin[p+' Desk'].loc[date_dict['current_year_start']:date_dict['current_year_end']],
                        mode="lines",
                        name='Desk view '+ str(today.year),
                        showlegend=True,
                        line=dict(color='red', width=2, dash='solid')
                        
                        )
                    )
            
            fig_allplant_daily.add_trace(
                    go.Scatter(
                        x=x_index,
                        y=df_basin[p+' Desk'].loc[date_dict['next_year_start']:date_dict['next_year_end']],
                        mode="lines",
                        name='Desk view '+ str(today.year+1),
                        showlegend=True,
                        line=dict(color='red', width=2, dash='dot')
                        
                        )
                    )
            fig_allplant_daily.add_trace(
                    go.Scatter(
                        x=x_index,
                        y=df_basin[p].loc[date_dict['last_year_start']:date_dict['last_year_end']],
                        mode="lines",
                        name='Kpler '+str(today.year-1),
                        showlegend=True,
                        line=dict(color='#7F7F7F', width=2, dash='dash')
                        
                        )
                    )
            
            fig_allplant_daily.add_trace(
                    go.Scatter(
                        x=x_index,
                        y=dfeta[p].loc[:today+datetime.timedelta(days=7)],
                        mode="lines",
                        name='Kpler ETA',
                        showlegend=True,
                        line=dict(color='blue', width=2, dash='solid')
                        
                        )
                    )
            
            fig_allplant_daily.add_trace(
                    go.Bar(
                        x=x_index,
                        y=df_basin[p+' Difference'].loc[date_dict['current_year_start']:date_dict['last_month']],
                        name='Kpler '+str(today.year)+' vs. Desk '+str(today.year)+' delta',
                        #width=1,
                    
                        showlegend=True,
                        marker_color='Silver',
                        #text=df_cumulative[p+' Cum Difference'].loc[:last_month].values,
                        #textposition='auto',
                        )
                    )    
            fig_allplant_daily.add_trace(
                go.Scatter(
                    x=x_index,
                    y=supply_yoy['min'],
                    fill='tonexty',
                    fillcolor='rgba(65,105,225,0)',
                    line_color='rgba(65,105,225,0)',
                    showlegend=False,
                    name='5 Years'
                    )
                )
            
            fig_allplant_daily.add_trace(
                go.Scatter(
                    x=x_index,
                    y=supply_yoy['max'],
                    fill='tonexty',
                    fillcolor='rgba(65,105,225,0.1)',
                    line_color='rgba(65,105,225,0)',
                    showlegend=True,
                    name='5 Years'
                    )
                )
            fig_allplant_daily.update_layout(
                        autosize=True,
                        showlegend=True,
                        legend=dict(x=0, y=-0.2),
                        legend_orientation="h",
                        title_text=p + ' 30 days MA, Mcm/d '+ str(today),
                        yaxis_title="Export (Mcm/d)",
                        xaxis = dict(dtick="M1"),
                        hovermode='x unified',
                        plot_bgcolor = 'white',
                        template='ggplot2'  
                   )
            fig_allplant_daily.update_xaxes(
                        showline=True, 
                        linewidth=1, 
                        linecolor='LightGrey', 
                        showgrid=True, 
                        gridwidth=1, 
                        gridcolor='LightGrey'
                    )
            fig_allplant_daily.update_yaxes(
                        showline=True, 
                        linewidth=1, 
                        linecolor='LightGrey', 
                        showgrid=True, 
                        gridwidth=1, 
                        gridcolor='LightGrey'
                    )
            py.plot(fig_allplant_daily, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/Flow30/daily/supply/all plant/'+p+' supply.html', auto_open=False)    
            fig_allplant_daily.write_image('U:/Trading - Gas/LNG/LNG website/Flow30/daily/supply/all plant/'+p+' supply.png')
    
    '''Basin demand cum. and flex'''
    def Basin_Chart_demand(df_basin_demand, date_dict):
        
        today=datetime.date.today()
        x_index=pd.date_range(date_dict['current_year_start'],date_dict['current_year_end'])

        #basin chart
        fig_Basin = go.Figure() 
        
        fig_Basin.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin_demand['MENA'].loc[date_dict['current_year_start']:date_dict['current_year_end']],
                    mode="lines",
                    name='MENA '+str(today.year),
                    showlegend=True,
                    line=dict(color='#7F7F7F', width=2, dash='solid')
                    
                    )
                )
        
        fig_Basin.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin_demand['MENA'].loc[date_dict['last_year_start']:date_dict['last_year_end']],
                    mode="lines",
                    name='MENA '+str(today.year-1),
                    showlegend=True,
                    line=dict(color='#7F7F7F', width=2, dash='dot')
                    
                    )
                )
        
        fig_Basin.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin_demand['MENA Desk'].loc[date_dict['current_year_start']:date_dict['current_year_end']],
                    mode="lines",
                    name='MENA Desk '+str(today.year),
                    showlegend=True,
                    line=dict(color='#7F7F7F', width=2, dash='dash')
                    
                    )
                )
        
        fig_Basin.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin_demand['PB'].loc[date_dict['current_year_start']:date_dict['current_year_end']],
                    mode="lines",
                    name='PB '+str(today.year),
                    showlegend=True,
                    line=dict(color='black', width=2, dash='solid')
                    
                    )
                )
        
        fig_Basin.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin_demand['PB'].loc[date_dict['last_year_start']:date_dict['last_year_end']],
                    mode="lines",
                    name='PB'+str(today.year-1),
                    showlegend=True,
                    line=dict(color='black', width=2, dash='dot')
                    
                    )
                )
        
        fig_Basin.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin_demand['PB Desk'].loc[date_dict['current_year_start']:date_dict['current_year_end']],
                    mode="lines",
                    name='PB Desk '+str(today.year),
                    showlegend=True,
                    line=dict(color='black', width=2, dash='dash')
                    
                    )
                )
        
        fig_Basin.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin_demand['AB'].loc[date_dict['current_year_start']:date_dict['current_year_end']],
                    mode="lines",
                    name='AB '+str(today.year),
                    showlegend=True,
                    line=dict(color='red', width=2, dash='solid')
                    
                    )
                )
        
        fig_Basin.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin_demand['AB'].loc[date_dict['last_year_start']:date_dict['last_year_end']],
                    mode="lines",
                    name='AB '+str(today.year-1),
                    showlegend=True,
                    line=dict(color='red', width=2, dash='dot')
                    
                    )
                )
        
        fig_Basin.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin_demand['AB Desk'].loc[date_dict['current_year_start']:date_dict['current_year_end']],
                    mode="lines",
                    name='AB Desk '+str(today.year),
                    showlegend=True,
                    line=dict(color='red', width=2, dash='dash')
                    
                    )
                )
        
        fig_Basin.update_layout(
            autosize=True,
            showlegend=True,
            legend=dict(x=0, y=-0.2),
            legend_orientation="h",
            title_text='Basin Demand, 30 days MA, Mcm/d '+ str(today),
            yaxis_title="Mcm/d",
            xaxis = dict(dtick="M1"),
            hovermode='x unified',
            plot_bgcolor = 'white',
            template='ggplot2'
              
        )
        fig_Basin.update_xaxes(
            showline=True, 
            linewidth=1, 
            linecolor='LightGrey', 
            showgrid=False, 
            gridwidth=1, 
            gridcolor='LightGrey'
        )
        fig_Basin.update_yaxes(
            showline=True, 
            linewidth=1, 
            linecolor='LightGrey', 
            showgrid=True, 
            gridwidth=1, 
            gridcolor='LightGrey'
        )
    
        #JKTC chart
        fig_JKTC = go.Figure() 
        
        fig_JKTC.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin_demand['Japan'].loc[date_dict['current_year_start']:date_dict['current_year_end']],
                    mode="lines",
                    name='Japan '+str(today.year),
                    showlegend=True,
                    line=dict(color='red', width=2, dash='solid')
                    
                    )
                )
        
        fig_JKTC.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin_demand['Japan'].loc[date_dict['last_year_start']:date_dict['last_year_end']],
                    mode="lines",
                    name='Japan '+str(today.year-1),
                    showlegend=True,
                    line=dict(color='red', width=2, dash='dot')
                    
                    )
                )
        
        fig_JKTC.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin_demand['Japan Desk'].loc[date_dict['current_year_start']:date_dict['current_year_end']],
                    mode="lines",
                    name='Desk Japan '+str(today.year),
                    showlegend=True,
                    line=dict(color='red', width=2, dash='dash')
                    
                    )
                )
        
        fig_JKTC.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin_demand['China'].loc[date_dict['current_year_start']:date_dict['current_year_end']],
                    mode="lines",
                    name='China '+str(today.year),
                    showlegend=True,
                    line=dict(color='black', width=2, dash='solid')
                    
                    )
                )
        
        fig_JKTC.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin_demand['China'].loc[date_dict['last_year_start']:date_dict['last_year_end']],
                    mode="lines",
                    name='China '+str(today.year-1),
                    showlegend=True,
                    line=dict(color='black', width=2, dash='dot')
                    
                    )
                )
        
        fig_JKTC.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin_demand['China Desk'].loc[date_dict['current_year_start']:date_dict['current_year_end']],
                    mode="lines",
                    name='Desk China '+str(today.year),
                    showlegend=True,
                    line=dict(color='black', width=2, dash='dash')
                    
                    )
                )
        
        fig_JKTC.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin_demand['South Korea'].loc[date_dict['current_year_start']:date_dict['current_year_end']],
                    mode="lines",
                    name='Korea '+str(today.year),
                    showlegend=True,
                    line=dict(color='#00B0F0', width=2, dash='solid')
                    
                    )
                )
        
        fig_JKTC.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin_demand['South Korea'].loc[date_dict['last_year_start']:date_dict['last_year_end']],
                    mode="lines",
                    name='Korea '+str(today.year-1),
                    showlegend=True,
                    line=dict(color='#00B0F0', width=2, dash='dot')
                    
                    )
                )
        
        fig_JKTC.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin_demand['South Korea Desk'].loc[date_dict['current_year_start']:date_dict['current_year_end']],
                    mode="lines",
                    name='Desk Korea '+str(today.year),
                    showlegend=True,
                    line=dict(color='#00B0F0', width=2, dash='dash')
                    
                    )
                )
        
        
        fig_JKTC.update_layout(
            autosize=True,
            showlegend=True,
            legend=dict(x=0, y=-0.2),
            legend_orientation="h",
            title_text='JKC Demand, 30 days MA, Mcm/d '+ str(today),
            yaxis_title="Mcm/d",
            xaxis = dict(dtick="M1"),
            hovermode='x unified',
            plot_bgcolor = 'white',
            template='ggplot2'
              
        )
        fig_JKTC.update_xaxes(
            showline=True, 
            linewidth=1, 
            linecolor='LightGrey', 
            showgrid=False, 
            gridwidth=1, 
            gridcolor='LightGrey'
        )
        fig_JKTC.update_yaxes(
            showline=True, 
            linewidth=1, 
            linecolor='LightGrey', 
            showgrid=True, 
            gridwidth=1, 
            gridcolor='LightGrey'
        )
        
        #region chart
        fig_region = go.Figure() 
        
        fig_region.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin_demand['MEIP'].loc[date_dict['current_year_start']:date_dict['current_year_end']],
                    mode="lines",
                    name='MEIP '+str(today.year),
                    showlegend=True,
                    line=dict(color='black', width=2, dash='solid')
                    
                    )
                )
        
        fig_region.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin_demand['MEIP'].loc[date_dict['last_year_start']:date_dict['last_year_end']],
                    mode="lines",
                    name='MEIP '+str(today.year-1),
                    showlegend=True,
                    line=dict(color='black', width=2, dash='dot')
                    
                    )
                )
        
        fig_region.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin_demand['MEIP Desk'].loc[date_dict['current_year_start']:date_dict['current_year_end']],
                    mode="lines",
                    name='MEIP Desk '+str(today.year),
                    showlegend=True,
                    line=dict(color='black', width=2, dash='dash')
                    
                    )
                )
        
        fig_region.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin_demand['EurDesk'].loc[date_dict['current_year_start']:date_dict['current_year_end']],
                    mode="lines",
                    name='Eur Desk '+str(today.year),
                    showlegend=True,
                    line=dict(color='red', width=2, dash='solid')
                    
                    )
                )
        
        fig_region.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin_demand['EurDesk'].loc[date_dict['last_year_start']:date_dict['last_year_end']],
                    mode="lines",
                    name='Eur Desk '+str(today.year-1),
                    showlegend=True,
                    line=dict(color='red', width=2, dash='dot')
                    
                    )
                )
        
        fig_region.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin_demand['EurDesk Desk'].loc[date_dict['current_year_start']:date_dict['current_year_end']],
                    mode="lines",
                    name='Eur Desk Desk '+str(today.year),
                    showlegend=True,
                    line=dict(color='red', width=2, dash='dash')
                    
                    )
                )
        '''
        fig_region.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin_demand['MedEur'].loc[date_dict['current_year_start']:date_dict['current_year_end']],
                    mode="lines",
                    name='Med Eur '+str(today.year),
                    showlegend=True,
                    line=dict(color='#A6A6A6', width=2, dash='solid')
                    
                    )
                )
        
        fig_region.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin_demand['MedEur'].loc[date_dict['last_year_start']:date_dict['last_year_end']],
                    mode="lines",
                    name='Med Eur '+str(today.year-1),
                    showlegend=True,
                    line=dict(color='#A6A6A6', width=2, dash='dot')
                    
                    )
                )
        
        fig_region.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin_demand['MedEur Desk'].loc[date_dict['current_year_start']:date_dict['current_year_end']],
                    mode="lines",
                    name='Med Eur Desk '+str(today.year),
                    showlegend=True,
                    line=dict(color='#A6A6A6', width=2, dash='dash')
                    
                    )
                )
        '''
        fig_region.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin_demand['LatAm'].loc[date_dict['current_year_start']:date_dict['current_year_end']],
                    mode="lines",
                    name='Lat Am '+str(today.year),
                    showlegend=True,
                    line=dict(color='#00B0F0', width=2, dash='solid')
                    
                    )
                )
        
        fig_region.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin_demand['LatAm'].loc[date_dict['last_year_start']:date_dict['last_year_end']],
                    mode="lines",
                    name='Lat Am '+str(today.year-1),
                    showlegend=True,
                    line=dict(color='#00B0F0', width=2, dash='dot')
                    
                    )
                )
        
        fig_region.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin_demand['LatAm Desk'].loc[date_dict['current_year_start']:date_dict['current_year_end']],
                    mode="lines",
                    name='Lat Am Desk '+str(today.year),
                    showlegend=True,
                    line=dict(color='#00B0F0', width=2, dash='dash')
                    
                    )
                )
        fig_region.update_layout(
            autosize=True,
            showlegend=True,
            legend=dict(x=0, y=-0.2),
            legend_orientation="h",
            title_text='Regional aggregated demand, 30 days MA, Mcm/d '+ str(today),
            yaxis_title="Mcm/d",
            xaxis = dict(dtick="M1"),
            hovermode='x unified',
            plot_bgcolor = 'white',
            template='ggplot2'
              
        )
        fig_region.update_xaxes(
            showline=True, 
            linewidth=1, 
            linecolor='LightGrey', 
            showgrid=False, 
            gridwidth=1, 
            gridcolor='LightGrey'
        )
        fig_region.update_yaxes(
            showline=True, 
            linewidth=1, 
            linecolor='LightGrey', 
            showgrid=True, 
            gridwidth=1, 
            gridcolor='LightGrey'
        )
        
            
        fig_Market = go.Figure() 
        
        fig_Market.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin_demand['India'].loc[date_dict['current_year_start']:date_dict['current_year_end']],
                    mode="lines",
                    name='India '+str(today.year),
                    showlegend=True,
                    line=dict(color='black', width=2, dash='solid')
                    
                    )
                )
        
        fig_Market.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin_demand['India'].loc[date_dict['last_year_start']:date_dict['last_year_end']],
                    mode="lines",
                    name='India '+str(today.year-1),
                    showlegend=True,
                    line=dict(color='black', width=2, dash='dot')
                    
                    )
                )
        
        fig_Market.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin_demand['India Desk'].loc[date_dict['current_year_start']:date_dict['current_year_end']],
                    mode="lines",
                    name='India Desk '+str(today.year),
                    showlegend=True,
                    line=dict(color='black', width=2, dash='dash')
                    
                    )
                )
        fig_Market.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin_demand['Spain'].loc[date_dict['current_year_start']:date_dict['current_year_end']],
                    mode="lines",
                    name='Spain '+str(today.year),
                    showlegend=True,
                    line=dict(color='#FFC000', width=2, dash='solid')
                    
                    )
                )
        
        fig_Market.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin_demand['Spain'].loc[date_dict['last_year_start']:date_dict['last_year_end']],
                    mode="lines",
                    name='Spain '+str(today.year-1),
                    showlegend=True,
                    line=dict(color='#FFC000', width=2, dash='dot')
                    
                    )
                )
        
        fig_Market.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin_demand['Spain Desk'].loc[date_dict['current_year_start']:date_dict['current_year_end']],
                    mode="lines",
                    name='Spain Desk '+str(today.year),
                    showlegend=True,
                    line=dict(color='#FFC000', width=2, dash='dash')
                    
                    )
                )
        fig_Market.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin_demand['Italy'].loc[date_dict['current_year_start']:date_dict['current_year_end']],
                    mode="lines",
                    name='Italy '+str(today.year),
                    showlegend=True,
                    line=dict(color='#00B0F0', width=2, dash='solid')
                    
                    )
                )
        
        fig_Market.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin_demand['Italy'].loc[date_dict['last_year_start']:date_dict['last_year_end']],
                    mode="lines",
                    name='Italy '+str(today.year-1),
                    showlegend=True,
                    line=dict(color='#00B0F0', width=2, dash='dot')
                    
                    )
                )
        
        fig_Market.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin_demand['Italy Desk'].loc[date_dict['current_year_start']:date_dict['current_year_end']],
                    mode="lines",
                    name='Italy Desk '+str(today.year),
                    showlegend=True,
                    line=dict(color='#00B0F0', width=2, dash='dash')
                    
                    )
                )
        fig_Market.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin_demand['Pakistan'].loc[date_dict['current_year_start']:date_dict['current_year_end']],
                    mode="lines",
                    name='Pak '+str(today.year),
                    showlegend=True,
                    line=dict(color='#7030A0', width=2, dash='solid')
                    
                    )
                )
        
        fig_Market.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin_demand['Pakistan'].loc[date_dict['last_year_start']:date_dict['last_year_end']],
                    mode="lines",
                    name='Pak '+str(today.year-1),
                    showlegend=True,
                    line=dict(color='#7030A0', width=2, dash='dot')
                    
                    )
                )
        
        fig_Market.add_trace(
                go.Scatter(
                    x=x_index,
                    y=df_basin_demand['Pakistan Desk'].loc[date_dict['current_year_start']:date_dict['current_year_end']],
                    mode="lines",
                    name='Pak Desk '+str(today.year),
                    showlegend=True,
                    line=dict(color='#7030A0', width=2, dash='dash')
                    
                    )
                )
        
        fig_Market.update_layout(
            autosize=True,
            showlegend=True,
            legend=dict(x=0, y=-0.2),
            legend_orientation="h",
            title_text='Growth Markets demand, 30 days MA, Mcm/d '+ str(today),
            yaxis_title="Mcm/d",
            xaxis = dict(dtick="M1"),
            hovermode='x unified',
            plot_bgcolor = 'white',
            template='ggplot2'
              
        )
        fig_Market.update_xaxes(
            showline=True, 
            linewidth=1, 
            linecolor='LightGrey', 
            showgrid=False, 
            gridwidth=1, 
            gridcolor='LightGrey'
        )
        fig_Market.update_yaxes(
            showline=True, 
            linewidth=1, 
            linecolor='LightGrey', 
            showgrid=True, 
            gridwidth=1, 
            gridcolor='LightGrey'
        )
        
        
        chart_list=['PB','AB','MENA','EoS','WoS','JKTC','LatAm','EurDesk','MEIP','OtherEur']
        for i in chart_list:
            fig = go.Figure() 
            
            fig.add_trace(
                    go.Scatter(
                        x=x_index,
                        y=df_basin_demand[i].loc[date_dict['current_year_start']:date_dict['current_year_end']],
                        mode="lines",
                        name='Kpler '+str(today.year),
                        showlegend=True,
                        line=dict(color='black', width=2, dash='solid')
                        
                        )
                    )
            
            fig.add_trace(
                    go.Scatter(
                        x=x_index,
                        y=df_basin_demand[i].loc[date_dict['last_year_start']:date_dict['last_year_end']],
                        mode="lines",
                        name='Kpler '+str(today.year-1),
                        showlegend=True,
                        line=dict(color='LightGrey', width=2, dash='dot')
                        
                        )
                    )
            
            fig.add_trace(
                    go.Scatter(
                        x=x_index,
                        y=df_basin_demand[i+' Desk'].loc[date_dict['current_year_start']:date_dict['current_year_end']],
                        mode="lines",
                        name='Desk view',
                        showlegend=True,
                        line=dict(color='red', width=2, dash='solid')
                        
                        )
                    )
            
            fig.add_trace(
                        go.Bar(
                            x=x_index,
                            y=df_basin_demand[i+' Difference'].loc[date_dict['current_year_start']:date_dict['last_month']].values,
                            name='Kpler '+str(today.year)+' vs. Desk '+str(today.year)+' delta',
                            #width=1,
                        
                            showlegend=True,
                            marker_color='Silver',
                            #text=df_cumulative[p+' Cum Difference'].loc[:last_month].values,
                            #textposition='auto',
                            )
                        )       
            
            fig.update_layout(
                autosize=True,
                showlegend=True,
                legend=dict(x=0, y=-0.2),
                legend_orientation="h",
                title_text=i+' , 30 days MA, Mcm/d '+ str(today),
                yaxis_title="Net Imports (Mcm/d)",
                xaxis = dict(dtick="M1"),
                hovermode='x unified',
                plot_bgcolor = 'white',
                template='ggplot2'
                  
            )
            fig.update_xaxes(
                showline=True, 
                linewidth=1, 
                linecolor='LightGrey', 
                showgrid=False, 
                gridwidth=1, 
                gridcolor='LightGrey'
            )
            fig.update_yaxes(
                showline=True, 
                linewidth=1, 
                linecolor='LightGrey', 
                showgrid=True, 
                gridwidth=1, 
                gridcolor='LightGrey'
            )
    
            py.plot(fig, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/Flow30/daily/demand/region/'+i+'.html', auto_open=False)
            fig.write_image('U:/Trading - Gas/LNG/LNG website/Flow30/daily/demand/region/'+i+'.png')
        
        
        py.plot(fig_Basin, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/Flow30/daily/demand/region/Basin demand.html', auto_open=False)
        py.plot(fig_JKTC, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/Flow30/daily/demand/region/JKTC demand.html', auto_open=False)
        py.plot(fig_Market, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/Flow30/daily/demand/region/Market demand.html', auto_open=False)
        py.plot(fig_region, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/Flow30/daily/demand/region/region demand.html', auto_open=False)
        
        fig_Basin.write_image('U:/Trading - Gas/LNG/LNG website/Flow30/daily/demand/region/Basin demand.png')
        fig_JKTC.write_image('U:/Trading - Gas/LNG/LNG website/Flow30/daily/demand/region/JKTC demand.png')
        fig_Market.write_image('U:/Trading - Gas/LNG/LNG website/Flow30/daily/demand/region/Market demand.png')
        fig_region.write_image('U:/Trading - Gas/LNG/LNG website/Flow30/daily/demand/region/region demand.png')
    
    def Demand_Cumulative_Chart(df_cumulative_demand, date_dict, DemandCategories):
        
        today=datetime.date.today()
        last_month= last_month= date_dict['last_month']+'-01'
        x_index=pd.date_range(date_dict['current_year_start'],date_dict['current_year_end'], freq='MS')

        BasinSuez = ['AB', 'PB', 'MENA','Global', 'EoS', 'WoS','JKTC','LatAm','EurDesk','MEIP','OtherEur']
        
        DemandCurveId = pd.read_excel('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/DeskViewID.xlsx',header=(0),sheet_name='demand')
        DemandCurveId=DemandCurveId[['CurveID','Country']]
        allcountry = DemandCurveId['Country'].values.tolist()
        
        #PLot Basin and Suez cumsum
        for p in BasinSuez:
            fig_Cumsum_Basin = go.Figure() 
            
            fig_Cumsum_Basin.add_trace(
                    go.Scatter(
                        x=x_index,
                        y=df_cumulative_demand[p+' Cum'].loc[date_dict['current_year_start']:last_month],
                        mode="lines",
                        name='Kpler '+str(today.year),
                        showlegend=True,
                        line=dict(color='black', width=2, dash='solid')
                        
                        )
                    )
            fig_Cumsum_Basin.add_trace(
                    go.Scatter(
                        x=x_index,
                        y=df_cumulative_demand[p+' Desk Cum'].loc[date_dict['current_year_start']:date_dict['current_year_end']],
                        mode="lines",
                        name='Desk view',
                        showlegend=True,
                        line=dict(color='red', width=2, dash='solid')
                        
                        )
                    )
            fig_Cumsum_Basin.add_trace(
                    go.Scatter(
                        x=x_index,
                        y=df_cumulative_demand[p+' Cum'].loc[date_dict['last_year_start']:date_dict['last_year_end']],
                        mode="lines",
                        name='Kpler '+str(today.year-1),
                        showlegend=True,
                        line=dict(color='#7F7F7F', width=2, dash='dash')
                        
                        )
                    )
            fig_Cumsum_Basin.add_trace(
                    go.Scatter(
                        x=x_index,
                        y=df_cumulative_demand[p+' Desk Cum'].loc[date_dict['next_year_start']:date_dict['next_year_end']],
                        mode="lines",
                        name='Desk view '+str(today.year+1),
                        showlegend=True,
                        line=dict(color='red', width=2, dash='dot')
                        
                        )
                    )
            fig_Cumsum_Basin.add_trace(
                    go.Bar(
                        x=x_index,
                        y=df_cumulative_demand[p+' Cum Difference'].loc[date_dict['current_year_start']:date_dict['last_month']],
                        name='Kpler '+str(today.year)+' vs. Desk '+str(today.year)+' delta',
                        #width=1,
                    
                        showlegend=True,
                        marker_color='Silver',
                        #text=df_cumulative[p+' Cum Difference'].loc[:last_month].values,
                        #textposition='auto',
                        )
                    )       
            fig_Cumsum_Basin.add_annotation(x=last_month, y=df_cumulative_demand[p+' Cum'].loc[last_month],
                    text = str( df_cumulative_demand[p+' Cum'].loc[last_month])+' (Kpler '+str(today.year)+')',
                    showarrow=True,
                    arrowhead=5)
            fig_Cumsum_Basin.add_annotation(x=x_index[-1], y=df_cumulative_demand[p+' Cum'].loc[date_dict['last_year_month']],
                    text = str(df_cumulative_demand[p+' Cum'].loc[date_dict['last_year_month']])+' (Kpler '+str(today.year-1)+')',
                    showarrow=True,
                    ax=10,
                    ay=30,
                    arrowhead=5)   
            fig_Cumsum_Basin.add_annotation(x=x_index[-1], y=df_cumulative_demand[p+' Desk Cum'].loc[date_dict['currentt_year_end_month']],
                    text=str(df_cumulative_demand[p+' Desk Cum'].loc[date_dict['currentt_year_end_month']]) + ' (Desk view '+str(today.year)+')',
                    showarrow=True,
                    arrowhead=5)
            
            bardate=[]
            
            for i in range (1,today.month):
                this_month_start = datetime.datetime(today.year, i , 1)
                bardate.append(this_month_start.strftime("%Y-%m-%d"))
            for j in range(0, today.month -1 ):
                fig_Cumsum_Basin.add_annotation(x=bardate[j], y=5,
                        text=df_cumulative_demand[p+' Cum Difference'].loc[bardate[j]],
                        showarrow=False)
            #print(Basin)    
            fig_Cumsum_Basin.update_layout(
                autosize=True,
                showlegend=True,
                legend=dict(x=0, y=-0.2),
                legend_orientation="h",
                title_text=p+' Cum. '+ str(today),
                yaxis_title="Net imports (Mt)",
                xaxis = dict(dtick="M1"),
                hovermode='x unified',
                plot_bgcolor = 'white',
                template='ggplot2'  ,
                bargap  = 0
            )
            fig_Cumsum_Basin.update_xaxes(
                showline=True, 
                linewidth=1, 
                linecolor='LightGrey', 
                showgrid=False, 
                gridwidth=1, 
                gridcolor='LightGrey'
            )
            fig_Cumsum_Basin.update_yaxes(
                showline=True, 
                linewidth=1, 
                linecolor='LightGrey', 
                showgrid=True, 
                gridwidth=1, 
                gridcolor='LightGrey'
            )
            py.plot(fig_Cumsum_Basin, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/Flow30/cumulate/demand/BasinSuez/'+p+' Cum Demand.html', auto_open=False)    
            fig_Cumsum_Basin.write_image('U:/Trading - Gas/LNG/LNG website/Flow30/cumulate/demand/BasinSuez/'+p+' Cum Demand.png')
    
        #plot all market cumsum
        for m in allcountry:
            fig_Cumsum_Market = go.Figure() 
            
            fig_Cumsum_Market.add_trace(
                    go.Scatter(
                        x=x_index,
                        y=df_cumulative_demand[m+' Cum'].loc[date_dict['current_year_start']:last_month],
                        mode="lines",
                        name='Kpler '+str(today.year),
                        showlegend=True,
                        line=dict(color='black', width=2, dash='solid')
                        
                        )
                    )
            fig_Cumsum_Market.add_trace(
                    go.Scatter(
                        x=x_index,
                        y=df_cumulative_demand[m+' Desk Cum'].loc[date_dict['current_year_start']:date_dict['current_year_end']],
                        mode="lines",
                        name='Desk view',
                        showlegend=True,
                        line=dict(color='red', width=2, dash='solid')
                        
                        )
                    )
            fig_Cumsum_Market.add_trace(
                    go.Scatter(
                        x=x_index,
                        y=df_cumulative_demand[m+' Cum'].loc[date_dict['last_year_start']:date_dict['last_year_end']],
                        mode="lines",
                        name='Kpler '+str(today.year-1),
                        showlegend=True,
                        line=dict(color='#7F7F7F', width=2, dash='dash')
                        
                        )
                    )
            fig_Cumsum_Market.add_trace(
                    go.Scatter(
                        x=x_index,
                        y=df_cumulative_demand[m+' Desk Cum'].loc[date_dict['next_year_start']:date_dict['next_year_end']],
                        mode="lines",
                        name='Desk view '+str(today.year+1),
                        showlegend=True,
                        line=dict(color='red', width=2, dash='dot')
                        
                        )
                    )
            fig_Cumsum_Market.add_trace(
                    go.Bar(
                        x=x_index,
                        y=df_cumulative_demand[m+' Cum Difference'].loc[date_dict['current_year_start']:date_dict['last_month']],
                        name='Kpler '+str(today.year)+' vs. Desk '+str(today.year)+' delta',
                        #width=1,
                    
                        showlegend=True,
                        marker_color='Silver',
                        #text=df_cumulative[p+' Cum Difference'].loc[:last_month].values,
                        #textposition='auto',
                        )
                    )       
            fig_Cumsum_Market.add_annotation(x=last_month, y=df_cumulative_demand[m+' Cum'].loc[last_month],
                    text = str( df_cumulative_demand[m+' Cum'].loc[last_month])+' (Kpler '+str(today.year)+')',
                    showarrow=True,
                    arrowhead=5)
            fig_Cumsum_Market.add_annotation(x=x_index[-1], y=df_cumulative_demand[m+' Cum'].loc[date_dict['last_year_month']],
                    text = str(df_cumulative_demand[m+' Cum'].loc[date_dict['last_year_month']])+' (Kpler '+str(today.year-1)+')',
                    showarrow=True,
                    ax=10,
                    ay=30,
                    arrowhead=5)   
            fig_Cumsum_Market.add_annotation(x=x_index[-1], y=df_cumulative_demand[m+' Desk Cum'].loc[date_dict['currentt_year_end_month']],
                    text=str(df_cumulative_demand[m+' Desk Cum'].loc[date_dict['currentt_year_end_month']]) + ' (Desk view '+str(today.year)+')',
                    showarrow=True,
                    arrowhead=5)
            
            bardate=[]
            
            for i in range (1,today.month):
                this_month_start = datetime.datetime(today.year, i , 1)
                bardate.append(this_month_start.strftime("%Y-%m-%d"))
            for j in range(0, today.month -1 ):
                fig_Cumsum_Market.add_annotation(x=bardate[j], y=df_cumulative_demand[m+' Cum Difference'].loc[bardate].min()+0.5,
                        text=df_cumulative_demand[m+' Cum Difference'].loc[bardate[j]],
                        showarrow=False)
                
            fig_Cumsum_Market.update_layout(
                autosize=True,
                showlegend=True,
                legend=dict(x=0, y=-0.2),
                legend_orientation="h",
                title_text=m+' Cum. '+ str(today),
                yaxis_title="Net imports (Mt)",
                xaxis = dict(dtick="M1"),
                hovermode='x unified',
                plot_bgcolor = 'white',
                template='ggplot2'  ,
                bargap  = 0
            )
            fig_Cumsum_Market.update_xaxes(
                showline=True, 
                linewidth=1, 
                linecolor='LightGrey', 
                showgrid=False, 
                gridwidth=1, 
                gridcolor='LightGrey'
            )
            fig_Cumsum_Market.update_yaxes(
                showline=True, 
                linewidth=1, 
                linecolor='LightGrey', 
                showgrid=True, 
                gridwidth=1, 
                gridcolor='LightGrey'
            )
            py.plot(fig_Cumsum_Market, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/Flow30/cumulate/demand/all market/'+m+' Cum Demand.html', auto_open=False)    
            fig_Cumsum_Market.write_image('U:/Trading - Gas/LNG/LNG website/Flow30/cumulate/demand/all market/'+m+' Cum Demand.png')
    
    def Region_Flex(df_basin_demand, date_dict, DemandCategories, mttomcmd):
        
        today = datetime.date.today()
        Kpler=DBTOPD('PRD-DB-SQL-209','AnalyticsModel', 'ts', 'AnalyticsHistory')
        dflastdeskview=Kpler.lastdeskdemand_to_df()
        '''
        DemandCurveId = pd.read_excel('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/DeskViewID.xlsx',header=(0),sheet_name='demandwithRU')
        DemandCurveId=DemandCurveId[['CurveID','Country']]
        DemandCurveId=DemandCurveId.set_index('CurveID').T.to_dict('list')
        
        #dfdeskdemand=deskdemand.deskdemand_to_df()
        dflastdeskview['CurveId'].replace(DemandCurveId, inplace=True)
        dflastdeskview.set_index('ForecastDate',inplace=True)
        dflastdeskview.drop(dflastdeskview.index[-1], inplace=True)
        dflastdeskview=dflastdeskview.loc[dflastdeskview.index[-1]]
        newdflastdeskview = pd.DataFrame(dflastdeskview[['ValueDate','CurveId', 'Value']].values, columns=['ValueDate','CurveId','Value'])
        '''
        curveid = DBTOPD.get_curve_id()
        dfcurveid = curveid.loc[:,['CurveId','Type','Location','Country']]
        DemandCurveId = dfcurveid.loc[dfcurveid[dfcurveid['Type']=='Demand'].index]
        DemandCurveId = DemandCurveId[['CurveId','Country','Location']]
        DemandCurveId.rename(columns={'CurveId':'CurveID','Country':'Country','Location':'plant'}, inplace=True)
        DemandCurveId=DemandCurveId[['CurveID','Country']]
        DemandCurveId=DemandCurveId.set_index('CurveID').T.to_dict('list')
        #dfdeskdemand=deskdemand.deskdemand_to_df()
        dflastdeskview['CurveId'].replace(DemandCurveId, inplace=True)
        dflastdeskview.set_index('ForecastDate',inplace=True)
        #avoid new market in no previous view
        try:
            dflastdeskview_copy = dflastdeskview.copy()
            dflastdeskview_copy.drop(dflastdeskview_copy.index[-1], inplace=True)
            dflastdeskview_copy=dflastdeskview_copy.loc[dflastdeskview_copy.index[-1]]
            print(dflastdeskview_copy['Germany'])

        except KeyError as e:
            print(e)
            dflastdeskview_copy = dflastdeskview.copy()
            dflastdeskview_copy=dflastdeskview_copy.loc[dflastdeskview_copy.index[-1]]
            
        #dflastdeskview.drop(dflastdeskview.index[-1], inplace=True)
        #dflastdeskview=dflastdeskview.loc[dflastdeskview.index[-1]]
        newdflastdeskview = pd.DataFrame(dflastdeskview_copy[['ValueDate','CurveId', 'Value']].values, columns=['ValueDate','CurveId','Value'])
        #print(newdflastdeskview)
        
        #print(newdflastdeskview)
        dfdemandnew=newdflastdeskview.groupby(['ValueDate','CurveId'], as_index=False).sum()
        dfdemandnew['ValueDate'] = pd.to_datetime(dfdemandnew['ValueDate'])
        dfdemandnew=pd.pivot(dfdemandnew, index='ValueDate', columns='CurveId')
        dfdemandnew.index.name='Date'
        dfdemandnew.columns=dfdemandnew.columns.droplevel(0)
        dfdemandnew.rename_axis(None, axis=1, inplace=True)
        #print(dfdemandnew)
        mttomcmd=1397
        for i in dfdemandnew.index:
            days=calendar.monthrange(i.year,i.month)
            dfdemandnew.loc[i]=dfdemandnew.loc[i]*mttomcmd/days[1]
            
        #change to full date data
        dfalldeskdemand=pd.DataFrame(index=pd.date_range(start=date_dict['current_year_start'],end=date_dict['current_year_end']))
        dfalldeskdemand.index.name='Date'
        
        merged = pd.merge(dfalldeskdemand, dfdemandnew, on='Date', how='outer')
        merged.fillna(method='ffill', inplace=True) 
        merged=merged.loc[date_dict['current_year_start']:date_dict['current_year_end']]
        merged = merged.round()
        #print(merged)
        DemandCategories.set_index('Region', inplace=True)
        EurDesk = DemandCategories['Country'].loc['Eur Desk'].values.tolist()
        
        
        df_flex = pd.DataFrame(index = pd.date_range(start=date_dict['current_year_start'], end=date_dict['current_year_end']))
        df_flex['Previous View'] = merged[EurDesk].loc[date_dict['current_year_start']:date_dict['current_year_end']].sum(axis=1)
        #df_flex['Previous View'].loc[:today] = None 
        df_flex['ALL Flex'] = df_basin_demand['EurDesk Desk'].loc[date_dict['current_year_start']:date_dict['current_year_end']]
        df_flex['Eur Desk '+str(today.year)] = df_basin_demand['EurDesk'].loc[date_dict['current_year_start']:date_dict['current_year_end']]
        df_flex['Eur Desk '+str(today.year-1)] = df_basin_demand['EurDesk'].loc[date_dict['last_year_start']:date_dict['last_year_end']].values
        #print(df_flex)
        df_flex = df_flex.fillna(0).round(0).astype(int)#apply(lambda x: int(round(x)) if str(x) != '<NA>' else x)#.
        
        date_list = []
        for i in range(1,13):
            date_list.append(str(today.year)+'-' + str(i) + '-15')
        #print(date_list)
        df_text=pd.DataFrame(df_flex['ALL Flex'].loc[date_list].astype(int).values, index=pd.to_datetime(date_list,format= '%Y-%m-%d' ), columns=['text'])
        df_text['previous text'] = df_flex['Previous View'].loc[date_list].values.round()
        
        
        
        df_flex['text'] = df_text['text'].astype(str)
        df_flex['previous text'] = df_text['previous text'].astype(str)
        #print(df_flex.loc[date_list,['text','previous text']])
        df_flex.loc[date_list,['text','previous text']] = df_flex.loc[date_list,['text','previous text']].astype(int)
        #df_flex = round(df_flex)
        #df_flex = df_flex.astype(int)
        df_flex.fillna('',inplace=True)
        #print(df_flex.loc['2022-01'])
        
        fig_Flex = go.Figure() 
        
        
        fig_Flex.add_trace(
                go.Scatter(
                    x=df_flex.index,
                    y=df_flex['Eur Desk '+str(today.year)].loc[:today],#.loc[date_dict['current_year_start']:date_dict['current_year_end']],
                    mode="lines",
                    name='Eur Desk '+str(today.year),
                    showlegend=True,
                    line=dict(color='red', width=2, dash='solid')
                    
                    )
                )
        
        
        fig_Flex.add_trace(
                go.Scatter(
                    x=df_flex.index,
                    y=df_flex['Eur Desk '+str(today.year-1)],#.loc[date_dict['last_year_start']:date_dict['last_year_end']],
                    mode="lines",
                    name='Eur Desk '+str(today.year-1),
                    showlegend=True,
                    line=dict(color='red', width=2, dash='dot')
                    
                    )
                )
    
        fig_Flex.add_trace(
                go.Scatter(
                    x=df_flex.index,
                    y=df_flex['ALL Flex'],
                    mode="lines+text",
                    name='ALL Flex',
                    showlegend=True,
                    line=dict(color='red', width=2, dash='dash'),
                    text= df_flex['text'],
                    textposition="top center",
                    textfont=dict(
                    family="sans serif",
                    size=8,
                    color="red")
                    
                    )
                )
        
        fig_Flex.add_trace(
                go.Scatter(
                    x=df_flex.index,
                    y= df_flex['Previous View'],
                    mode="lines+text",
                    name='Previous View',
                    showlegend=True,
                    line=dict(color='#7F7F7F', width=2, dash='dash'),
                    text = df_flex['previous text'],
                    textposition="bottom center",
                    textfont=dict(
                    family="sans serif",
                    size=8,
                    color="#7F7F7F")
                    
                    )
                )
        
        
        fig_Flex.update_layout(
            autosize=True,
            showlegend=True,
            legend=dict(x=0, y=-0.2),
            legend_orientation="h",
            title_text='Eur Deskope, 30 days MA, Mcm/d '+ str(today),
            yaxis_title="Mcm/d",
            xaxis = dict(dtick="M1"),
            hovermode='x unified',
            plot_bgcolor = 'white',
            template='ggplot2'
              
        )
        fig_Flex.update_xaxes(
            showline=True, 
            linewidth=1, 
            linecolor='LightGrey', 
            showgrid=False, 
            gridwidth=1, 
            gridcolor='LightGrey'
        )
        fig_Flex.update_yaxes(
            showline=True, 
            linewidth=1, 
            linecolor='LightGrey', 
            showgrid=True, 
            gridwidth=1, 
            gridcolor='LightGrey'
        )
    
        
        py.plot(fig_Flex, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/Flow30/daily/demand/region/region flex.html', auto_open=False)
        fig_Flex.write_image('U:/Trading - Gas/LNG/LNG website/Flow30/daily/demand/region/region flex.png')
    
    def NWE_cargo(df_basin_demand, df_cumulative_demand, date_dict, DemandCategories, mttomcmd):
        today = datetime.date.today()
        x_index=pd.date_range(date_dict['current_year_start'],date_dict['current_year_end'], freq='MS')
        Kpler=DBTOPD('PRD-DB-SQL-209','AnalyticsModel', 'ts', 'AnalyticsHistory')
        dflastdeskview=Kpler.lastdeskdemand_to_df()
        '''
        DemandCurveId = pd.read_excel('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/DeskViewID.xlsx',header=(0),sheet_name='demandwithRU')
        DemandCurveId=DemandCurveId[['CurveID','Country']]
        DemandCurveId=DemandCurveId.set_index('CurveID').T.to_dict('list')
        
        #dfdeskdemand=deskdemand.deskdemand_to_df()
        dflastdeskview['CurveId'].replace(DemandCurveId, inplace=True)
        dflastdeskview.set_index('ForecastDate',inplace=True)
        dflastdeskview.drop(dflastdeskview.index[-1], inplace=True)
        dflastdeskview=dflastdeskview.loc[dflastdeskview.index[-1]]
        newdflastdeskview = pd.DataFrame(dflastdeskview[['ValueDate','CurveId', 'Value']].values, columns=['ValueDate','CurveId','Value'])
        #print(newdflastdeskview)
        '''
        curveid = DBTOPD.get_curve_id()
        dfcurveid = curveid.loc[:,['CurveId','Type','Location','Country']]
        DemandCurveId = dfcurveid.loc[dfcurveid[dfcurveid['Type']=='Demand'].index]
        DemandCurveId = DemandCurveId[['CurveId','Country','Location']]
        DemandCurveId.rename(columns={'CurveId':'CurveID','Country':'Country','Location':'plant'}, inplace=True)
        DemandCurveId=DemandCurveId[['CurveID','Country']]
        DemandCurveId=DemandCurveId.set_index('CurveID').T.to_dict('list')
        #dfdeskdemand=deskdemand.deskdemand_to_df()
        dflastdeskview['CurveId'].replace(DemandCurveId, inplace=True)
        dflastdeskview.set_index('ForecastDate',inplace=True)
        #avoid new market in no previous view
        try:
            dflastdeskview_copy = dflastdeskview.copy()
            dflastdeskview_copy.drop(dflastdeskview_copy.index[-1], inplace=True)
            dflastdeskview_copy=dflastdeskview_copy.loc[dflastdeskview_copy.index[-1]]
            print(dflastdeskview_copy['Germany'])

        except KeyError as e:
            print(e)
            dflastdeskview_copy = dflastdeskview.copy()
            dflastdeskview_copy=dflastdeskview_copy.loc[dflastdeskview_copy.index[-1]]
            
        #dflastdeskview.drop(dflastdeskview.index[-1], inplace=True)
        #dflastdeskview=dflastdeskview.loc[dflastdeskview.index[-1]]
        newdflastdeskview = pd.DataFrame(dflastdeskview_copy[['ValueDate','CurveId', 'Value']].values, columns=['ValueDate','CurveId','Value'])
        #print(newdflastdeskview)
        dfdemandnew=newdflastdeskview.groupby(['ValueDate','CurveId'], as_index=False).sum()
        dfdemandnew['ValueDate'] = pd.to_datetime(dfdemandnew['ValueDate'])
        dfdemandnew=pd.pivot(dfdemandnew, index='ValueDate', columns='CurveId')
        dfdemandnew.index.name='Date'
        dfdemandnew.columns=dfdemandnew.columns.droplevel(0)
        dfdemandnew.rename_axis(None, axis=1, inplace=True)
        #print(dfdemandnew)
        mttomcmd=1397
        for i in dfdemandnew.index:
            days=calendar.monthrange(i.year,i.month)
            dfdemandnew.loc[i]=dfdemandnew.loc[i]*mttomcmd/days[1]
            
        #change to full date data
        dfalldeskdemand=pd.DataFrame(index=pd.date_range(start=date_dict['current_year_start'],end=date_dict['current_year_end']))
        dfalldeskdemand.index.name='Date'
        
        merged = pd.merge(dfalldeskdemand, dfdemandnew, on='Date', how='outer')
        merged.fillna(method='ffill', inplace=True) 
        merged=merged.loc[date_dict['current_year_start']:date_dict['current_year_end']]
        merged = merged.round()
        
        EurDesk = DemandCategories['Country'].loc['Eur Desk'].values.tolist()
        
        
        df_flex = pd.DataFrame(index = pd.date_range(start=date_dict['current_year_start'], end=date_dict['current_year_end']))
        df_flex['Previous View'] = merged[EurDesk].loc[date_dict['current_year_start']:date_dict['current_year_end']].sum(axis=1)
        #df_flex['Previous View'].loc[:today] = None 
        df_flex['ALL Flex'] = df_basin_demand['EurDesk Desk'].loc[date_dict['current_year_start']:date_dict['current_year_end']]
        df_flex['Eur Desk '+str(today.year)] = df_basin_demand['EurDesk'].loc[date_dict['current_year_start']:date_dict['current_year_end']]
        df_flex['Eur Desk '+str(today.year-1)] = df_basin_demand['EurDesk'].loc[date_dict['last_year_start']:date_dict['last_year_end']].values
        #mcm/d to mt/y =0.2613,
        df_flex_mt = df_flex*0.2613
        #mcm/d to cm/y = 596379, standard cargo = 150000cm
        df_flex_cargo = df_flex*596379/150000
        for j in df_flex_mt.index:
            days=calendar.monthrange(j.year,j.month)
            df_flex_mt.loc[j]=df_flex_mt.loc[j]/365*days[1]
            df_flex_cargo.loc[j]=df_flex_cargo.loc[j]/365*days[1]
        df_flex_mt = df_flex_mt.fillna(0).round(0).astype(int)
        df_flex_cargo = df_flex_cargo.fillna(0).round(0).astype(int)
        #print(df_flex_mt)
        #print(df_flex_cargo)
        
        date_list = []
        for i in range(1,13):
            date_list.append(str(today.year)+'-' + str(i) + '-15')
        #print(date_list)
        df_text_mt=pd.DataFrame(df_flex_mt['ALL Flex'].loc[date_list].values, index=pd.to_datetime(date_list,format= '%Y-%m-%d' ), columns=['text'])
        df_text_mt['previous text'] = df_flex_mt['Previous View'].loc[date_list].values.round()

        df_flex_mt['text'] = df_text_mt['text'].astype(str)
        df_flex_mt['previous text'] = df_text_mt['previous text'].astype(str)
        df_flex_mt.loc[date_list,['text','previous text']] = df_flex_mt.loc[date_list,['text','previous text']].astype(int)
        
        df_flex_mt.fillna('',inplace=True)
        #print(df_flex)
        
        fig_Flex = go.Figure() 
    
        fig_Flex.add_trace(
                go.Scatter(
                    x=df_flex_mt.index,
                    y=df_flex_mt['Eur Desk '+str(today.year)].loc[:today],#.loc[date_dict['current_year_start']:date_dict['current_year_end']],
                    mode="lines",
                    name='Eur Desk '+str(today.year),
                    showlegend=True,
                    line=dict(color='red', width=2, dash='solid')
                    
                    )
                )
        
        
        fig_Flex.add_trace(
                go.Scatter(
                    x=df_flex_mt.index,
                    y=df_flex_mt['Eur Desk '+str(today.year-1)],
                    mode="lines",
                    name='Eur Desk '+str(today.year-1),
                    showlegend=True,
                    line=dict(color='red', width=2, dash='dot')
                    
                    )
                )
    
        fig_Flex.add_trace(
                go.Scatter(
                    x=df_flex_mt.index,
                    y=df_flex_mt['ALL Flex'],
                    mode="lines+text",
                    name='ALL Flex',
                    showlegend=True,
                    line=dict(color='red', width=2, dash='dash'),
                    text= df_flex_mt['text'],
                    textposition="top center",
                    textfont=dict(
                    family="sans serif",
                    size=8,
                    color="black")
                    
                    )
                )
        
        fig_Flex.add_trace(
                go.Scatter(
                    x=df_flex_mt.index,
                    y= df_flex_mt['Previous View'],
                    mode="lines+text",
                    name='Previous View',
                    showlegend=True,
                    line=dict(color='#7F7F7F', width=2, dash='dash'),
                    text = df_flex_mt['previous text'],
                    textposition="bottom center",
                    textfont=dict(
                    family="sans serif",
                    size=8,
                    color="#7F7F7F")
                    
                    )
                )
        
        
        fig_Flex.update_layout(
            autosize=True,
            showlegend=True,
            legend=dict(x=0, y=-0.2),
            legend_orientation="h",
            title_text='Eur Deskope, 30 days MA, Mt '+ str(today),
            yaxis_title="Mt",
            xaxis = dict(dtick="M1"),
            hovermode='x unified',
            plot_bgcolor = 'white',
            template='ggplot2'
              
        )
        fig_Flex.update_xaxes(
            showline=True, 
            linewidth=1, 
            linecolor='LightGrey', 
            showgrid=False, 
            gridwidth=1, 
            gridcolor='LightGrey'
        )
        fig_Flex.update_yaxes(
            showline=True, 
            linewidth=1, 
            linecolor='LightGrey', 
            showgrid=True, 
            gridwidth=1, 
            gridcolor='LightGrey'
        )
        py.plot(fig_Flex, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/Flow30/daily/demand/region/region flex mt.html', auto_open=False)
        fig_Flex.write_image('U:/Trading - Gas/LNG/LNG website/Flow30/daily/demand/region/region flex mt.png')
        
        
        df_text_cargo=pd.DataFrame(df_flex_cargo['ALL Flex'].loc[date_list].values, index=pd.to_datetime(date_list,format= '%Y-%m-%d' ), columns=['text'])
        df_text_cargo['previous text'] = df_flex_cargo['Previous View'].loc[date_list].values.round()

        df_flex_cargo['text'] = df_text_cargo['text'].astype(str)
        df_flex_cargo['previous text'] = df_text_cargo['previous text'].astype(str)
        df_flex_cargo.loc[date_list,['text','previous text']] = df_flex_cargo.loc[date_list,['text','previous text']].astype(int)
        
        #print(df_text.index)
        
        #df_flex_cargo['text'] = df_text_cargo['text']
        df_flex_cargo.fillna('',inplace=True)
        #print(df_flex)
        
        fig_Flex_cargo = go.Figure() 
        
        
        fig_Flex_cargo.add_trace(
                go.Scatter(
                    x=df_flex_cargo.index,
                    y=df_flex_cargo['Eur Desk '+str(today.year)].loc[:today],
                    mode="lines",
                    name='Eur Desk '+str(today.year),
                    showlegend=True,
                    line=dict(color='red', width=2, dash='solid')
                    
                    )
                )
        
        
        fig_Flex_cargo.add_trace(
                go.Scatter(
                    x=df_flex_cargo.index,
                    y=df_flex_cargo['Eur Desk '+str(today.year-1)],
                    mode="lines",
                    name='Eur Desk '+str(today.year),
                    showlegend=True,
                    line=dict(color='red', width=2, dash='dot')
                    
                    )
                )
    
        fig_Flex_cargo.add_trace(
                go.Scatter(
                    x=df_flex_cargo.index,
                    y=df_flex_cargo['ALL Flex'],
                    mode="lines+text",
                    name='ALL Flex',
                    showlegend=True,
                    line=dict(color='red', width=2, dash='dash'),
                    text= df_flex_cargo['text'],
                    textposition="top center",
                    textfont=dict(
                    family="sans serif",
                    size=8,
                    color="black")
                    
                    )
                )
        
        fig_Flex_cargo.add_trace(
                go.Scatter(
                    x=df_flex_cargo.index,
                    y= df_flex_cargo['Previous View'],
                    mode="lines+text",
                    name='Previous View',
                    showlegend=True,
                    line=dict(color='#7F7F7F', width=2, dash='dash'),
                    text = df_flex_cargo['previous text'],
                    textposition="bottom center",
                    textfont=dict(
                    family="sans serif",
                    size=8,
                    color="#7F7F7F")
                    
                    )
                )
        
        
        fig_Flex_cargo.update_layout(
            autosize=True,
            showlegend=True,
            legend=dict(x=0, y=-0.2),
            legend_orientation="h",
            title_text='Eur Deskope, 30 days MA, cargos '+ str(today),
            yaxis_title="cargos",
            xaxis = dict(dtick="M1"),
            hovermode='x unified',
            plot_bgcolor = 'white',
            template='ggplot2'
              
        )
        fig_Flex_cargo.update_xaxes(
            showline=True, 
            linewidth=1, 
            linecolor='LightGrey', 
            showgrid=False, 
            gridwidth=1, 
            gridcolor='LightGrey'
        )
        fig_Flex_cargo.update_yaxes(
            showline=True, 
            linewidth=1, 
            linecolor='LightGrey', 
            showgrid=True, 
            gridwidth=1, 
            gridcolor='LightGrey'
        )
        py.plot(fig_Flex_cargo, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/Flow30/daily/demand/region/region flex cargo.html', auto_open=False)
        fig_Flex_cargo.write_image('U:/Trading - Gas/LNG/LNG website/Flow30/daily/demand/region/region flex cargo.png')
    
        
        
        EurCum=df_cumulative_demand[['EurDesk Cum','EurDesk Desk Cum', 'EurDesk Cum Difference']]
        EurCumCargo=EurCum/0.062
        #for k in EurCumCargo.index:
            #days=calendar.monthrange(k.year,k.month)
            #EurCumCargo.loc[k]=EurCumCargo.loc[k]/365*days[1]
        EurCumBcm=EurCum*1.397
        EurCumCargo = EurCumCargo.round(2)
        #print(EurCumCargo)
        EurCumBcm = EurCumBcm.round(2)
        
        last_month= last_month= date_dict['last_month']+'-01'
        #print(EurCumBcm)
        #print(df_cumulative_demand)
        fig_Cumsum_cargo = go.Figure() 
            
        fig_Cumsum_cargo.add_trace(
                go.Scatter(
                    x=x_index,
                    y=EurCumCargo['EurDesk Cum'].loc[date_dict['current_year_start']:date_dict['last_month']],
                    mode="lines",
                    name='Kpler '+str(today.year),
                    showlegend=True,
                    line=dict(color='black', width=2, dash='solid')
                    
                    )
                )
        fig_Cumsum_cargo.add_trace(
                go.Scatter(
                    x=x_index,
                    y=EurCumCargo['EurDesk Desk Cum'].loc[date_dict['current_year_start']:date_dict['current_year_end']],
                    mode="lines",
                    name='Desk view',
                    showlegend=True,
                    line=dict(color='red', width=2, dash='solid')
                    
                    )
                )
        fig_Cumsum_cargo.add_trace(
                go.Scatter(
                    x=x_index,
                    y=EurCumCargo['EurDesk Cum'].loc[date_dict['last_year_start']:date_dict['last_year_end']],
                    mode="lines",
                    name='Kpler '+str(today.year-1),
                    showlegend=True,
                    line=dict(color='#7F7F7F', width=2, dash='dash')
                    
                    )
                )
        fig_Cumsum_cargo.add_trace(
                go.Bar(
                    x=x_index,
                    y=EurCumCargo['EurDesk Cum Difference'].loc[date_dict['current_year_start']:date_dict['last_month']],
                    name='Kpler '+str(today.year)+' vs. Desk '+str(today.year)+' delta',
                    #width=1,
                
                    showlegend=True,
                    marker_color='Silver',
                    #text=df_cumulative[p+' Cum Difference'].loc[:last_month].values,
                    #textposition='auto',
                    )
                )       
        fig_Cumsum_cargo.add_annotation(x=last_month, y=EurCumCargo['EurDesk Cum'].loc[last_month],
                text = str(EurCumCargo['EurDesk Cum'].loc[last_month])+' (Kpler '+str(date_dict['today'].year)+')',
                showarrow=True,
                arrowhead=5)
        fig_Cumsum_cargo.add_annotation(x=x_index[-1], y=EurCumCargo['EurDesk Cum'].loc[date_dict['last_year_month']],
                text = str(EurCumCargo['EurDesk Cum'].loc[date_dict['last_year_month']])+' (Kpler '+str(date_dict['today'].year-1)+')',
                showarrow=True,
                ax=10,
                ay=30,
                arrowhead=5)  
        fig_Cumsum_cargo.add_annotation(x=x_index[-1], y=EurCumCargo['EurDesk Desk Cum'].loc[date_dict['currentt_year_end_month']],
                text=str(EurCumCargo['EurDesk Desk Cum'].loc[date_dict['currentt_year_end_month']]) + ' (Desk view '+str(date_dict['today'].year)+')',
                showarrow=True,
                arrowhead=5)
        
        bardate=[]
        
        for i in range (1,date_dict['today'].month):
            this_month_start = datetime.datetime(date_dict['today'].year, i , 1)
            bardate.append(this_month_start.strftime("%Y-%m-%d"))
        for j in range(0, date_dict['today'].month -1 ):
            fig_Cumsum_cargo.add_annotation(x=bardate[j], y=5,
                    text=EurCumCargo['EurDesk Cum Difference'].loc[bardate[j]],
                    showarrow=False)
        #print(Basin)    
        fig_Cumsum_cargo.update_layout(
            autosize=True,
            showlegend=True,
            legend=dict(x=0, y=-0.2),
            legend_orientation="h",
            title_text='EurDesk Cum. Cargos'+ str(date_dict['today']),
            yaxis_title="Net imports (Cargos)",
            xaxis = dict(dtick="M1"),
            hovermode='x unified',
            plot_bgcolor = 'white',
            template='ggplot2'  ,
            bargap  = 0
        )
        fig_Cumsum_cargo.update_xaxes(
            showline=True, 
            linewidth=1, 
            linecolor='LightGrey', 
            showgrid=False, 
            gridwidth=1, 
            gridcolor='LightGrey'
        )
        fig_Cumsum_cargo.update_yaxes(
            showline=True, 
            linewidth=1, 
            linecolor='LightGrey', 
            showgrid=True, 
            gridwidth=1, 
            gridcolor='LightGrey'
        )
        py.plot(fig_Cumsum_cargo, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/Flow30/cumulate/demand/BasinSuez/EurDesk Cum cargo.html', auto_open=False) 
        fig_Cumsum_cargo.write_image('U:/Trading - Gas/LNG/LNG website/Flow30/cumulate/demand/BasinSuez/EurDesk Cum cargo.png')
        
        
        fig_Cumsum_bcm = go.Figure() 
            
        fig_Cumsum_bcm.add_trace(
                go.Scatter(
                    x=x_index,
                    y=EurCumBcm['EurDesk Cum'].loc[date_dict['current_year_start']:date_dict['last_month']],
                    mode="lines",
                    name='Kpler '+str(today.year),
                    showlegend=True,
                    line=dict(color='black', width=2, dash='solid')
                    
                    )
                )
        fig_Cumsum_bcm.add_trace(
                go.Scatter(
                    x=x_index,
                    y=EurCumBcm['EurDesk Desk Cum'].loc[date_dict['current_year_start']:date_dict['current_year_end']],
                    mode="lines",
                    name='Desk view',
                    showlegend=True,
                    line=dict(color='red', width=2, dash='solid')
                    
                    )
                )
        fig_Cumsum_bcm.add_trace(
                go.Scatter(
                    x=x_index,
                    y=EurCumBcm['EurDesk Cum'].loc[date_dict['last_year_start']:date_dict['last_year_end']],
                    mode="lines",
                    name='Kpler '+str(today.year-1),
                    showlegend=True,
                    line=dict(color='#7F7F7F', width=2, dash='dash')
                    
                    )
                )
        fig_Cumsum_bcm.add_trace(
                go.Bar(
                    x=x_index,
                    y=EurCumBcm['EurDesk Cum Difference'].loc[date_dict['current_year_start']:date_dict['last_month']],
                    name='Kpler '+str(today.year)+' vs. Desk '+str(today.year)+' delta',
                    #width=1,
                
                    showlegend=True,
                    marker_color='Silver',
                    #text=df_cumulative[p+' Cum Difference'].loc[:last_month].values,
                    #textposition='auto',
                    )
                )       
        
        fig_Cumsum_bcm.add_annotation(x=last_month, y=EurCumBcm['EurDesk Cum'].loc[last_month],
                text = str(EurCumBcm['EurDesk Cum'].loc[date_dict['last_month']].values)+' (Kpler '+str(today.year)+')',
                showarrow=True,
                arrowhead=5)
        fig_Cumsum_bcm.add_annotation(x=x_index[-1], y=EurCumBcm['EurDesk Cum'].loc[date_dict['last_year_month']],
                text = str(EurCumBcm['EurDesk Cum'].iloc[-1])+' (Kpler '+str(today.year-1)+')',
                showarrow=True,
                ax=10,
                ay=30,
                arrowhead=5)  
        fig_Cumsum_bcm.add_annotation(x=x_index[-1], y=EurCumBcm['EurDesk Desk Cum'].loc[date_dict['currentt_year_end_month']],
                text=str(EurCumBcm['EurDesk Desk Cum'].loc[date_dict['currentt_year_end_month']]) + ' (Desk view '+str(today.year)+')',
                showarrow=True,
                arrowhead=5)
        
        bardate=[]
        
        for i in range (1,today.month):
            this_month_start = datetime.datetime(today.year, i , 1)
            bardate.append(this_month_start.strftime("%Y-%m-%d"))
        for j in range(0, today.month -1 ):
            fig_Cumsum_bcm.add_annotation(x=bardate[j], y=5,
                    text=EurCumBcm['EurDesk Cum Difference'].loc[bardate[j]],
                    showarrow=False)
        #print(Basin)    
        fig_Cumsum_bcm.update_layout(
            autosize=True,
            showlegend=True,
            legend=dict(x=0, y=-0.2),
            legend_orientation="h",
            title_text='EurDesk Cum. Bcm'+ str(today),
            yaxis_title="Net imports (Bcm)",
            xaxis = dict(dtick="M1"),
            hovermode='x unified',
            plot_bgcolor = 'white',
            template='ggplot2'  ,
            bargap  = 0
        )
        fig_Cumsum_bcm.update_xaxes(
            showline=True, 
            linewidth=1, 
            linecolor='LightGrey', 
            showgrid=False, 
            gridwidth=1, 
            gridcolor='LightGrey'
        )
        fig_Cumsum_bcm.update_yaxes(
            showline=True, 
            linewidth=1, 
            linecolor='LightGrey', 
            showgrid=True, 
            gridwidth=1, 
            gridcolor='LightGrey'
        )
        py.plot(fig_Cumsum_bcm, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/Flow30/cumulate/demand/BasinSuez/EurDesk Cum bcm.html', auto_open=False) 
        fig_Cumsum_bcm.write_image('U:/Trading - Gas/LNG/LNG website/Flow30/cumulate/demand/BasinSuez/EurDesk Cum bcm.png')

#a=FlowMA10Chart
#a.full_data()

    def flow_daily():
    
        #create dict of date
        date_dict={
               'last_year_start' : str(datetime.date.today().year-1)+'-01-01',
               'last_year_end' : str(datetime.date.today().year-1)+'-12-31',
               'start_date' : '2020-01-01',
               'end_date' : '2021-12-31',
               'current_year_start' : str(datetime.date.today().year)+'-01-01',
               'current_year_end' : str(datetime.date.today().year)+'-12-31',
               'last_day' : datetime.date.today() - datetime.timedelta(days=1)*1,
               'today' : datetime.date.today(),
               'last_month': str((datetime.date.today() - relativedelta(months=1)).year)+'-'+str((datetime.date.today() - relativedelta(months=1)).month),
               'last_year_month':str(datetime.date.today().year-1)+'-12-01',
               'currentt_year_end_month':str(datetime.date.today().year)+'-12-01',
               'next_year_start':str(datetime.date.today().year+1)+'-01-01',
               'next_year_end':str(datetime.date.today().year+1)+'-12-01',
               }    
        #print(date_dict['last_year_month'])
        mttomcmd = 1397
        
        a=FlowMA30Chart
        
        dfdemand, dfsupplyplant, dfsupplycountry, dfsupplyMA_eta, dfsupplyMA_eta_plant, dfdemandMA_eta, desk_supply_plant_view, desk_supply_country_view, desk_demand_view, dfihscontractdemand,dfihscontractsupply, supply_country_list, demand_country_list, dfbasinsupply,dfbasindemand,dfcumsupply,dfcumdemand = a.full_data()
        
        #regional data plot
        a.country_chart(date_dict, supply_country_list, demand_country_list, dfsupplycountry, dfdemand, dfsupplyMA_eta, dfdemandMA_eta, desk_supply_country_view, desk_demand_view, dfihscontractdemand,dfihscontractsupply)
        
        a.global_chart(date_dict, supply_country_list, demand_country_list, dfsupplycountry, dfdemand, dfsupplyMA_eta, dfdemandMA_eta, desk_supply_country_view, desk_demand_view, dfihscontractdemand,dfihscontractsupply)
        #print(datetime.datetime.now()) 
        print('\033[0;31;46m 30MA Country and Golbal has updated at: ～\033[0m' ,datetime.datetime.now() )
        
        #get categories
        SupplyCategories=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'CategoriesSupply').sql_to_df()
        DemandCategories=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'CategoriesDemand').sql_to_df()
        drop_list_supply = ['Vysotsk','Kollsnes', 'Stavanger','Greater Tortue FLNG']
        for i in drop_list_supply:
            SupplyCategories.drop(SupplyCategories[SupplyCategories['Plant'] == i].index, inplace=True)
        #plot basin, plant and region flex chart 
        a.Basin_Chart_supply(dfbasinsupply, date_dict)
        a.Supply_Cumulative_Chart(dfcumsupply, date_dict, SupplyCategories)
        a.all_plant_daily(dfbasinsupply, dfsupplyplant, dfsupplyMA_eta_plant, dfihscontractsupply, date_dict, SupplyCategories)
        a.Basin_Chart_demand(dfbasindemand, date_dict)
        a.Demand_Cumulative_Chart(dfcumdemand, date_dict, DemandCategories)
        a.Region_Flex(dfbasindemand, date_dict, DemandCategories, mttomcmd)
        a.NWE_cargo(dfbasindemand, dfcumdemand, date_dict, DemandCategories, mttomcmd)
        print('\033[0;31;46m 30 MA basin, plant and region flex has updated at: ～\033[0m' ,datetime.datetime.now() )

#FlowMA30Chart.flow_daily()
'''
#print(datetime.date.today())
#flow_daily()    
#C:\AnalystCode\YWang\LNGflow.py

scheduler = BackgroundScheduler(job_defaults={'misfire_grace_time': 15*60})
trigger = OrTrigger([ CronTrigger(day_of_week='0-6', hour='06, 12, 16', minute='22')])
#scheduler.add_job(func=flow_daily,trigger='cron',day_of_week='0-6', hour='06, 12, 16', minute='15',id='flow daily')
scheduler.add_job(func=flow_daily,trigger=trigger,id='flow daily')
scheduler.start()
runtime = datetime.datetime.now()
print (scheduler.get_jobs(), runtime)
#scheduler.remove_job('flow daily') 
'''