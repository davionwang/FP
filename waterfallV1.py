# -*- coding: utf-8 -*-
"""
Created on Thu Feb  2 09:19:00 2023

@author: SVC-GASQuant2-Prod
"""

#V1 not remove nwe for demand

import time

import dash
import dash_html_components as html
#from dash import html
#from dash import dcc
import dash_core_components as dcc
from dash.dependencies import Input, Output
from dash.exceptions import PreventUpdate
import plotly.graph_objects as go
import plotly.offline as py

import sys

import datetime
import numpy as np
import pandas as pd

from dateutil.relativedelta import relativedelta
sys.path.append('C:\\Users\SVC-GASQuant2-Prod\YWwebsite\Python Scripts\class') 
#from SDdata import SD_Data_Country
#from SDdata import SD_Data_Basin
import dash_table 
from DBtoDF import DBTOPD

class WaterFall():
    
    def get_nwe_data():
        
        today = datetime.date.today()
        #get nwe desk total
        basindemand=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'BasinDemand')
        dfbasindemand=basindemand.sql_to_df()
        dfbasindemand.set_index('Date', inplace=True)
        dfbasindemand.sort_index(inplace=True)
        
        #nwe_desk = dfbasindemand.loc[str(today-relativedelta(days=1)), 'NWEur Desk']
        globaldesk =  dfbasindemand.loc[str(today-relativedelta(days=1)), 'Global Desk'] 
        #get nwe country list
        DemandCategories = pd.read_excel('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/Categories.xlsx',header=(0),sheet_name='Index_Demand')
        DemandCategories = DemandCategories.iloc[:,0:6]
        DemandCategories.drop(DemandCategories[DemandCategories['Country'] == 'Russian Federation'].index, inplace=True)
        DemandCategories.set_index('Region', inplace=True)
        #print(DemandCategories)
        NWEur = DemandCategories['Country'].loc['NW Eur'].values.tolist()
        
        
        #get maket desk view
        desk=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'DeskDemand')
        dfdesk=desk.sql_to_df()
        dfdesk.set_index('Date', inplace=True)
        dfdesk.sort_index(inplace=True)
        
        demand=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'DemandCountryHist')
        dfdemand=demand.sql_to_df()
        dfdemand.set_index('Date', inplace=True)
        
        index = dfdemand.columns.to_list()
        #for i in NWEur:
        #    index.remove(i)
        #print(dfdesk)
        #get calc
        df=pd.DataFrame(index=index)
        #print(df)
        df['Current month desk view (Mcm/d)'] = dfdesk.loc[str((today-relativedelta(days=1)))]
        df['Current month average delivered (Mcm/d)'] = dfdemand.loc[str((today-relativedelta(days=1)).year)+'-'+str((today-relativedelta(days=1)).month)+'-01':(today-relativedelta(days=1))].sum()/(today-relativedelta(days=1)).day
        df['Current month average delivered (Mcm/d) - Desk'] = df['Current month average delivered (Mcm/d)'] -  df['Current month desk view (Mcm/d)']
        df.sort_values(by = 'Current month average delivered (Mcm/d) - Desk',ascending=(False) ,inplace=True)
        #print(df)
        
        #df for chart
        dfwaterfall_demand = pd.DataFrame(index = [str(today)], columns = ['Global Desk View']+list(df.index))
        dfwaterfall_demand.loc[str(today), 'Global Desk View'] = globaldesk
        dfwaterfall_demand.loc[str(today), list(df.index)] = df['Current month average delivered (Mcm/d) - Desk'].values
        dfwaterfall_demand = dfwaterfall_demand.astype('float').round(2)
        dfwaterfall_demand.dropna(inplace=True, axis=1)
        dfwaterfall_demand = dfwaterfall_demand.loc[:, (dfwaterfall_demand != 0).any(axis=0)]
        #print(dfwaterfall_demand)        
        
        return dfwaterfall_demand
    
    def get_supply_data():
        
        today = datetime.date.today()
        #get global desk total
        basinsupply=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'BasinSupply')
        dfbasinsupply=basinsupply.sql_to_df()
        dfbasinsupply.set_index('Date', inplace=True)
        dfbasinsupply.sort_index(inplace=True)
        
        global_desk = dfbasinsupply.loc[str(today-relativedelta(days=1)), 'Global Desk']
        
        #get plant desk view
        desk=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'DeskSupplyPlant')
        dfsupplydesk = desk.sql_to_df()
        dfsupplydesk.set_index('Date', inplace=True)
        
        supply = DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'SupplyPlantHist')
        dfsupply = supply.sql_to_df()
        dfsupply.set_index('Date', inplace=True)
        
        df=pd.DataFrame(index=dfsupply.columns)
        
        df['Current month desk view (Mcm/d)'] = dfsupplydesk.loc[str((today-relativedelta(days=1)))]
        df['Current month average EndOrigin (Mcm/d)'] = dfsupply.loc[str((today-relativedelta(days=1)).year)+'-'+str((today-relativedelta(days=1)).month)+'-01':(today-relativedelta(days=1))].sum()/(today-relativedelta(days=1)).day
        df['Current Month Delta (Mcm/d) - Desk'] = df['Current month average EndOrigin (Mcm/d)'] - df['Current month desk view (Mcm/d)']
        df.sort_values(by = 'Current Month Delta (Mcm/d) - Desk',ascending=(False) ,inplace=True)
        #print(df)
        #df for chart
        dfwaterfall_supply = pd.DataFrame(index = [str(today)], columns = ['Global Desk View']+list(df.index))
        dfwaterfall_supply.loc[str(today), 'Global Desk View'] = global_desk
        dfwaterfall_supply.loc[str(today), list(df.index)] = df['Current Month Delta (Mcm/d) - Desk'].values
        dfwaterfall_supply = dfwaterfall_supply.astype('float').round(2)
        dfwaterfall_supply.dropna(inplace=True, axis=1)
        dfwaterfall_supply = dfwaterfall_supply.loc[:, (dfwaterfall_supply != 0).any(axis=0)]
        #print(dfwaterfall_supply)
        
        return dfwaterfall_supply
    
    
    def plot_chart(dfwaterfall_demand, dfwaterfall_supply):
        
        #print(dfwaterfall.iloc[0].to_list())
        #print(dfwaterfall_demand.iloc[0,0]*0.8, dfwaterfall_demand.sum(axis = 1).values[0].round(2))

        fig_nwe = go.Figure(go.Waterfall(
            name = "Global", orientation = "v",
            measure = ["absolute"] +["relative"]*(dfwaterfall_demand.shape[1]-1)+['total'],
            x = dfwaterfall_demand.columns.to_list()+['Total'],
            textposition = "outside",
            text = dfwaterfall_demand.iloc[0].to_list()+[str(dfwaterfall_demand.sum(axis = 1).values[0].round(2))],
            y = dfwaterfall_demand.iloc[0].to_list()+[0],
            connector = {"line":{"color":"rgb(63, 63, 63)"}},
        ))
        fig_nwe.update_yaxes(range=[dfwaterfall_demand.iloc[0,0]*0.8, 2400])#(range = [80, 260]),#
        fig_nwe.update_layout(
                title = "Global Current month average delivered (Mcm/d) - Desk View and Market Desk" + str(datetime.date.today()),
                showlegend = True
        )


        fig_supply = go.Figure(go.Waterfall(
            name = "Global Supply", orientation = "v",
            measure = ["absolute"]+["relative"]*(dfwaterfall_supply.shape[1]-1)+['total'],
            x = dfwaterfall_supply.columns.to_list()+['Total'],
            textposition = "outside",
            text = dfwaterfall_supply.iloc[0].to_list()+[str(dfwaterfall_supply.sum(axis = 1).values[0].round(2))],
            y = [dfwaterfall_supply.iloc[0,0]*0.8]+dfwaterfall_supply.iloc[0,1:].to_list()+[0],
            connector = {"line":{"color":"rgb(63, 63, 63)"}},
        ))
        fig_supply.update_yaxes(range=[dfwaterfall_supply.iloc[0,0]*0.7, 2000]) #(range=[950,1300]),#
        fig_supply.update_layout(
                title = "Global Supply Desk View and Current Month Delta (Mcm/d) - Desk " + str(datetime.date.today()),
                showlegend = True
        )

        py.plot(fig_nwe, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/Flow/daily/demand/region/NWE WaterFall.html', auto_open=False)
        py.plot(fig_supply, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/Flow/daily/demand/region/Global supply WaterFall.html', auto_open=False)
        
        fig_nwe.write_image("U:/Trading - Gas/LNG/LNG website/Flow/daily/demand/region/NWE WaterFall.png")
        fig_supply.write_image("U:/Trading - Gas/LNG/LNG website/Flow/daily/demand/region/Global supply WaterFall.png")
        
    def update():
        
        dfwaterfall_demand = WaterFall.get_nwe_data()
        dfwaterfall_supply = WaterFall.get_supply_data()
        WaterFall.plot_chart(dfwaterfall_demand, dfwaterfall_supply)
        
#WaterFall.update()