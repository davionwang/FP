# -*- coding: utf-8 -*-
"""
Created on Tue Aug 10 16:34:09 2021

@author: SVC-PowerUK-Test
"""

import sys
import plotly.offline as py
import plotly.graph_objs as go
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import time
import datetime
import numpy as np
import pandas as pd
from plotly.subplots import make_subplots
import matplotlib.pyplot as plt
import calendar

sys.path.append('C:\\AnalystCode\\YWang\\class') 
from DBtoDF import DBTOPD

pd.set_option('display.max_columns',10)



class ABtoPB():
    
    def Kpler_data():
        
        #read data from Kpler
        Kpler=DBTOPD('TST-DB-SQL-208','Scrape', 'dbo', 'KplerLNGTradesView')
        dfkpler=Kpler.sql_to_df()
        
        #get supply and demand df
        columns=['EndOrigin','CountryOrigin','CountryDestination', 'VolumeOriginM3']
        #demand_columns=['StartDestination','CountryDestination','VolumeDestinationM3']
        
        df = dfkpler[columns]
        
        #Supply and demand categories
        SupplyCategories = pd.read_excel('C:/Users/SVC-PowerUK-Test/YWwebsite/data/Categories.xlsx',header=(0),sheet_name='Index_Supply')
        SupplyCategories = SupplyCategories.iloc[:44,0:5]
        DemandCategories = pd.read_excel('C:/Users/SVC-PowerUK-Test/YWwebsite/data/Categories.xlsx',header=(0),sheet_name='Index_Demand')
        DemandCategories = DemandCategories.iloc[:,0:6]
        
        return df, SupplyCategories, DemandCategories
    
    
    def get_ab_to_pb():
        
        df, SupplyCategories, DemandCategories=ABtoPB.Kpler_data()
        
        df['EndOrigin']=pd.to_datetime(df['EndOrigin'])
        df.set_index('EndOrigin',inplace=True)
        df.sort_values(by='EndOrigin',inplace=True)
        df =  df.loc['2016-01-01':]
        #df = df.groupby(['EndOrigin']).sum()
        #print(df)
        
        SupplyCategories = SupplyCategories[['Market', 'Basin']]
        SupplyCategories.set_index('Market', inplace=True)
        #supply_dict= SupplyCategories.to_dict()
        supply_dict = {'Australia': 'PB', 'Yemen': 'MENA_Bas', 'Algeria': 'MENA_Bas', 'Malaysia': 'PB', 'Equatorial Guinea': 'AB', 'Nigeria': 'AB', 'Indonesia': 'PB', 'United States': 'AB', 'Cameroon': 'AB', 'Egypt': 'MENA_Bas', 'United Arab Emirates': 'MENA_Bas', 'Brunei': 'PB', 'Libya': 'MENA_Bas', 'Peru': 'PB', 'Papua New Guinea': 'PB', 'Oman': 'MENA_Bas', 'Qatar': 'MENA_Bas', 'Russian Federation': 'AB', 'Norway': 'AB', 'Angola': 'AB', 'Mozambique': 'PB', 'Trinidad and Tobago': 'AB', 'Argentina': 'AB'}
        
        #AB-PB
        df.replace(supply_dict, inplace=True)
        
        DemandCategories = DemandCategories[['Country', 'Basin']]
        DemandCategories.set_index('Country', inplace=True)
        Demand_dict=  {'Argentina': 'AB', 'Australia': 'PB', 'Bahrain': 'MENA', 'Bangladesh': 'MENA', 'Belgium': 'AB', 'Brazil': 'AB', 'Canada': 'AB', 'Chile': 'PB', 'China': 'PB', 'Colombia': 'AB', 'Croatia': 'AB', 'Cyprus': 'MENA', 'Dominican Republic': 'AB', 'Egypt': 'MENA', 'Finland': 'AB', 'France': 'AB', 'Ghana': 'AB', 'Greece': 'AB', 'India': 'MENA', 'Indonesia': 'PB', 'Israel': 'MENA', 'Italy': 'AB', 'Jamaica': 'AB', 'Japan': 'PB', 'Jordan': 'MENA', 'Kuwait': 'MENA', 'Lithuania': 'AB', 'Malaysia': 'PB', 'Malta': 'AB', 'Myanmar': 'PB', 'Mexico': 'PB', 'Netherlands': 'AB', 'Norway': 'AB', 'Oman': 'MENA', 'Pakistan': 'MENA', 'Panama': 'AB', 'Poland': 'AB', 'Portugal': 'AB', 'Puerto Rico': 'AB', 'Singapore Republic': 'PB', 'South Korea': 'PB', 'Spain': 'AB', 'Sweden': 'AB', 'Taiwan': 'PB', 'Thailand': 'PB', 'Turkey': 'AB', 'United Arab Emirates': 'MENA', 'United Kingdom': 'AB', 'United States': 'AB'}
        df.replace(Demand_dict, inplace=True)
        df.reset_index(inplace=True)
            
        
        ABPB=pd.DataFrame(index=df.index, columns=['EndOrigin','CountryOrigin','CountryDestination', 'VolumeOriginM3'])
        for i in df.index:
            if df.loc[i,'CountryOrigin'] == 'AB' and df.loc[i, 'CountryDestination']=='PB':
                ABPB.loc[i] = df.loc[i]
        
        ABPB.dropna(axis=0,inplace=True)
        ABPB['EndOrigin']=pd.to_datetime(ABPB['EndOrigin']).dt.strftime('%Y-%m-%d') 
        ABPB=ABPB.groupby(['EndOrigin']).sum()
        ABPB['VolumeOriginM3'] = ABPB['VolumeOriginM3']*0.000000438
        ABPB.index.name='Date'
        ABPB.index=pd.DatetimeIndex(ABPB.index)
        #print(ABPB)
        dffulldate=pd.DataFrame(index=pd.date_range(start='2016-01-01',end='2021-12-31')) 
        dffulldate.index.name='Date'
        merged = pd.merge(dffulldate, ABPB, on='Date', how='outer')
        merged.fillna(0, inplace=True)
        merged['VolumeOriginM3'] = merged['VolumeOriginM3'].rolling(10).mean()
        #print(merged)
        return merged

#a= ABtoPB
#abpb=a.get_ab_to_pb()
#print(abpb)
