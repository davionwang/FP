# -*- coding: utf-8 -*-
"""
Created on Thu Feb  3 09:24:54 2022

@author: SVC-GASQuant2-Prod
"""


# -*- coding: utf-8 -*-
"""
diff table v1
V2 fix with new dataframe with 2022
V3 use actual data instead of MA10
V4 add % tables
V5 rolling back 12 months to current month
"""

import sys
import plotly.offline as py
import plotly.graph_objs as go
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import numpy as np
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import time
import datetime
import calendar
from dateutil.relativedelta import relativedelta
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.combining import OrTrigger
from apscheduler.triggers.cron import CronTrigger

sys.path.append('C:\\Users\SVC-GASQuant2-Prod\YWwebsite\Python Scripts\class') 
from DBtoDF import DBTOPD

class DiffTable():
    
    def diff(today):
        #supply_country['Difference']=(supply_country['Kpler'] - supply_country['Desk View']).resample('M').sum()
        #supply_country['Difference'].fillna(method='bfill', inplace=True)
        
        now_year=str(today.year)#today.year
        lastmonthday=datetime.date(datetime.date.today().year,datetime.date.today().month,1)-datetime.timedelta(1)
        start = str((today-relativedelta(months=13)).year)+'-'+str((today-relativedelta(months=13)).month)+'-01'
        #end = str(today-relativedelta(months=1).year)+'-'+str(today-relativedelta(months=1).month)+'-01'
        print('Diff table end to: ', lastmonthday)
        signal_trigger = 0.05
        
        '''get supply plant and desk supply'''
        supply=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'SupplyPlantHist')
        dfsupply=supply.sql_to_df()
        dfsupply.set_index('Date', inplace=True)
        dfsupply = dfsupply.resample('M').sum() #small diff with Salman's because m3 to mcm to mt. He use M3 to mt directly.
        #print(dfsupply.loc['2021-01-31':])
        supplydesk = DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'DeskSupplyPlant')
        dfsupplydesk=supplydesk.sql_to_df()
        dfsupplydesk.set_index('Date', inplace=True)
        dfsupplydesk = dfsupplydesk.resample('M').sum()
        
        supplybasindesk = DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'BasinSupply')
        dfbasinsupplydesk=supplybasindesk.sql_to_df()
        dfbasinsupplydesk.set_index('Date', inplace=True)
        dfbasinsupplydesk = dfbasinsupplydesk.resample('M').sum()
        #print(dfbasinsupplydesk)
        '''get supply global and basin and top 5 suppliers'''
        #supplyglobal=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'SupplyGlobal')
        #dfsupplyglobal=supplyglobal.sql_to_df()
        #dfsupplyglobal.set_index('index', inplace=True)
        #dfsupplyglobal = dfsupplyglobal.resample('M').sum()
        dfsupplyglobal=pd.DataFrame(index=pd.date_range(start=start, end=str(now_year)+'-12-31', freq='M').date)
        dfsupplyglobal['Hist'] = dfsupply.sum(axis=1)
        dfsupplyglobal['desk'] = dfsupplydesk.sum(axis=1)
        dfsupplyglobal['Difference'] = dfsupplyglobal['desk'] - dfsupplyglobal['Hist']
        #print(dfsupplyglobal)
        
        #pb and ab
        supplybasin=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'SupplyBasinHist')
        dfsupplybasin=supplybasin.sql_to_df()
        dfsupplybasin.set_index('Date', inplace=True)
        dfsupplybasin = dfsupplybasin.resample('M').sum()
        PBAB=dfsupplybasin[['PB','AB']].loc[start:str(now_year)+'-12-31'] 
        #print(dfsupplybasin)
        #top 5 suppliers
        supplycountry=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'SupplyCountryHist')
        dfsupplycountry=supplycountry.sql_to_df()
        dfsupplycountry.set_index('Date', inplace=True)
        dfsupplycountry = dfsupplycountry.resample('M').sum()
        
        desksupplycountry=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'DeskSupplyCOuntry')
        dfdesksupplycountry=desksupplycountry.sql_to_df()
        dfdesksupplycountry.set_index('Date', inplace=True)
        dfdesksupplycountry = dfdesksupplycountry.resample('M').sum()
        
        '''creat supply market df'''
        diff_supply_market=pd.DataFrame(index=pd.date_range(start=start, end=str(now_year)+'-12-31', freq='M').date)
        diff_supply_market['Global'] = dfsupplyglobal['Difference']
        diff_supply_market['PB'] =  dfbasinsupplydesk['PB Desk'].loc[start:str(now_year)+'-12-31'] - PBAB['PB'].loc[start:str(now_year)+'-12-31']    
        diff_supply_market['AB'] =  dfbasinsupplydesk['AB Desk'].loc[start:str(now_year)+'-12-31'] - PBAB['AB'].loc[start:str(now_year)+'-12-31']
        diff_supply_market['United States'] = dfdesksupplycountry['United States'] - dfsupplycountry['United States']
        diff_supply_market['Qatar'] =  dfdesksupplycountry['Qatar'] - dfsupplycountry['Qatar']
        diff_supply_market['Australia'] = dfdesksupplycountry['Australia'] - dfsupplycountry['Australia']
        diff_supply_market['Russian Federation'] = dfdesksupplycountry['Russian Federation'] - dfsupplycountry['Russian Federation']
        diff_supply_market['Nigeria'] = dfdesksupplycountry['Nigeria'] - dfsupplycountry['Nigeria']
        
        supply_market_signal = pd.DataFrame(index = diff_supply_market.index)
        supply_market_signal['Global'] = dfsupplyglobal['Hist'] * signal_trigger
        supply_market_signal['PB'] = PBAB['PB'].loc[start:str(now_year)+'-12-31'] * signal_trigger
        supply_market_signal['AB'] = PBAB['AB'].loc[start:str(now_year)+'-12-31'] * signal_trigger
        supply_market_signal['United States'] = dfsupplycountry['United States']* signal_trigger
        supply_market_signal['Qatar'] = dfsupplycountry['Qatar'] * signal_trigger
        supply_market_signal['Australia'] = dfsupplycountry['Australia'] * signal_trigger
        supply_market_signal['Russian Federation'] = dfsupplycountry['Russian Federation'] * signal_trigger
        supply_market_signal['Nigeria'] = dfsupplycountry['Nigeria'] * signal_trigger
        
        '''creat supply market df %%%%%%%'''
        diff_supply_marketP=pd.DataFrame(index=pd.date_range(start=start, end=str(now_year)+'-12-31', freq='M').date)
        diff_supply_marketP['Global'] = (dfsupplyglobal['desk'] - dfsupplyglobal['Hist'])/dfsupplyglobal['Hist']
        diff_supply_marketP['PB'] =  (dfbasinsupplydesk['PB Desk'].loc[start:str(now_year)+'-12-31'] - PBAB['PB'].loc[start:str(now_year)+'-12-31'])/PBAB['PB'].loc[start:str(now_year)+'-12-31']    
        diff_supply_marketP['AB'] =  (dfbasinsupplydesk['AB Desk'].loc[start:str(now_year)+'-12-31'] - PBAB['AB'].loc[start:str(now_year)+'-12-31'])/PBAB['AB'].loc[start:str(now_year)+'-12-31']
        diff_supply_marketP['United States'] = (dfdesksupplycountry['United States'] - dfsupplycountry['United States'])/dfsupplycountry['United States']
        diff_supply_marketP['Qatar'] =  (dfdesksupplycountry['Qatar'] - dfsupplycountry['Qatar'])/dfsupplycountry['Qatar']
        diff_supply_marketP['Australia'] = (dfdesksupplycountry['Australia'] - dfsupplycountry['Australia'])/dfsupplycountry['Australia']
        diff_supply_marketP['Russian Federation'] = (dfdesksupplycountry['Russian Federation'] - dfsupplycountry['Russian Federation'])/dfsupplycountry['Russian Federation']
        diff_supply_marketP['Nigeria'] = (dfdesksupplycountry['Nigeria'] - dfsupplycountry['Nigeria'])/dfsupplycountry['Nigeria']
        diff_supply_marketP = diff_supply_marketP.T.round(2)
        diff_supply_marketP.reset_index(inplace=True)
        diff_supply_marketP.rename(columns={'index':'Market'}, inplace=True)
        diff_supply_marketP = diff_supply_marketP.loc[:,:lastmonthday]
        
        #devide month days
        for jm in diff_supply_market.index:
                    days=calendar.monthrange(jm.year,jm.month)
                    diff_supply_market.loc[jm]=diff_supply_market.loc[jm]/days[1]
                    supply_market_signal.loc[jm]=supply_market_signal.loc[jm]/days[1]
        #market df            
        diff_supply_market = diff_supply_market.T.round(2)
        diff_supply_market.reset_index(inplace=True)
        diff_supply_market.rename(columns={'index':'Market'}, inplace=True)
        diff_supply_market = diff_supply_market.loc[:,:lastmonthday]
        #market signal df
        supply_market_signal = supply_market_signal.T.round(2)
        supply_market_signal.reset_index(inplace=True)
        supply_market_signal.rename(columns={'index':'Market'}, inplace=True)
        supply_market_signal = supply_market_signal.loc[:,:lastmonthday]
        
        #print(supply_market_signal)
        '''creat supply plant df'''
        diff_supply=pd.DataFrame(index=pd.date_range(start=start, end=str(now_year)+'-12-31', freq='M').date)
        diff_supplyP=pd.DataFrame(index=pd.date_range(start=start, end=str(now_year)+'-12-31', freq='M').date)
        #print(dfsupply.columns)
        #print(dfsupplydesk.columns)
        columns = list(set(dfsupply.columns)&set(dfsupplydesk.columns))
        for i in columns:
            diff_supply[i]=(dfsupplydesk.loc[start:,i] - dfsupply.loc[start:,i])
            diff_supplyP[i]=(dfsupplydesk.loc[start:,i] - dfsupply.loc[start:,i])/ dfsupply.loc[start:,i]
        for j in diff_supply.index:
                    days=calendar.monthrange(j.year,j.month)
                    diff_supply.loc[j]=diff_supply.loc[j]/days[1]
        
        
        #print(diff_supply)
        #print(start, lastmonthday)
        diff_supply = diff_supply.loc[:lastmonthday,:]
        diff_supply = diff_supply.T.round(2)
        diff_supply.reset_index(inplace=True)
        diff_supply.rename(columns={'index':'Plant'}, inplace=True)
        #print(diff_supply)
        #diff_supply = diff_supply.loc[:,start:lastmonthday]
        diff_supplyP = diff_supplyP.loc[:lastmonthday,:]
        diff_supplyP = diff_supplyP.T.round(2)
        diff_supplyP.reset_index(inplace=True)
        diff_supplyP.rename(columns={'index':'Plant'}, inplace=True)
        #diff_supplyP = diff_supplyP.loc[:,start:lastmonthday]
        #print(diff_supply)
        
        '''get demand and desk demand'''
        demand=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'DemandCountryHist')
        dfdemand=demand.sql_to_df()
        dfdemand.set_index('Date', inplace=True)
        #print(dfkpler)
        dfdemand = dfdemand.resample('M').sum() #small diff with Salman's because m3 to mcm to mt. He use M3 to mt directly.
        #print(dfdemand)
        demanddesk = DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'DeskDemand')
        dfdemanddesk=demanddesk.sql_to_df()
        dfdemanddesk.set_index('Date', inplace=True)
        dfdemanddesk = dfdemanddesk.resample('M').sum()
        #print(dfdemanddesk)
        demandbasindesk = DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'BasinDemand')
        dfbasindemanddesk=demandbasindesk.sql_to_df()
        dfbasindemanddesk.set_index('Date', inplace=True)
        dfbasindemanddesk = dfbasindemanddesk.resample('M').sum()
        '''get demand region df'''
        #demandglobal=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'DemandGlobal')
        #dfdemandglobal=demandglobal.sql_to_df()
        #dfdemandglobal.set_index('index', inplace=True)
        #dfdemandglobal = dfdemandglobal.resample('M').sum()
        dfdemandglobal=pd.DataFrame(index=pd.date_range(start=start, end=str(now_year)+'-12-31', freq='M').date)
        dfdemandglobal['Hist'] = dfdemand.sum(axis=1)
        dfdemandglobal['desk'] = dfdemanddesk.sum(axis=1)
        dfdemandglobal['Difference'] = dfdemandglobal['desk'] - dfdemandglobal['Hist']
        #print(dfdemandglobal['Hist'])
        
        #region demand
        demandbasin=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'DemandBasinHist')
        dfdemandbasin=demandbasin.sql_to_df()
        dfdemandbasin.set_index('Date', inplace=True)
        dfdemandbasin = dfdemandbasin.resample('M').sum()
        region=dfdemandbasin[['PB','AB','MENA','JKTC','LatAm','MEIP',
                              'EurDesk','OtherEur','OtherRoW']]
        
        
        
        '''create demand region'''
        diff_demand_region=pd.DataFrame(index=pd.date_range(start=start, end=str(now_year)+'-12-31', freq='M').date)
        diff_demand_region['Global'] = dfdemandglobal['Difference']
        diff_demand_region['PB'] =  dfbasindemanddesk['PB Desk'].loc[start:str(now_year)+'-12-31'] - region['PB'].loc[start:str(now_year)+'-12-31']    
        diff_demand_region['AB'] =  dfbasindemanddesk['AB Desk'].loc[start:str(now_year)+'-12-31'] - region['AB'].loc[start:str(now_year)+'-12-31'] 
        diff_demand_region['MENA'] = dfbasindemanddesk['MENA Desk'].loc[start:str(now_year)+'-12-31'] - region['MENA'].loc[start:str(now_year)+'-12-31']
        diff_demand_region['JKTC'] = dfbasindemanddesk['JKTC Desk'].loc[start:str(now_year)+'-12-31'] - region['JKTC'].loc[start:str(now_year)+'-12-31']
        diff_demand_region['LatAm'] = dfbasindemanddesk['LatAm Desk'].loc[start:str(now_year)+'-12-31'] - region['LatAm'].loc[start:str(now_year)+'-12-31']
        diff_demand_region['MEIP'] = dfbasindemanddesk['MEIP Desk'].loc[start:str(now_year)+'-12-31'] - region['MEIP'].loc[start:str(now_year)+'-12-31'] 
        diff_demand_region['EurDesk'] = dfbasindemanddesk['EurDesk Desk'].loc[start:str(now_year)+'-12-31'] - region['EurDesk'].loc[start:str(now_year)+'-12-31']
        diff_demand_region['OtherEur'] = dfbasindemanddesk['OtherEur Desk'].loc[start:str(now_year)+'-12-31'] - region['OtherEur'].loc[start:str(now_year)+'-12-31'] 
        #diff_demand_region['MedEur'] = dfbasindemanddesk['MedEur Desk'].loc[start:str(now_year)+'-12-31'] - region['MedEur'].loc[start:str(now_year)+'-12-31']
        diff_demand_region['OtherRoW'] = dfbasindemanddesk['OtherRoW Desk'].loc[start:str(now_year)+'-12-31'] - region['OtherRoW'].loc[start:str(now_year)+'-12-31']
        
        demand_region_signal = pd.DataFrame(index = diff_demand_region.index)
        demand_region_signal['Global'] = dfdemandglobal['Hist'] * signal_trigger
        demand_region_signal['PB'] = region['PB'].loc[start:str(now_year)+'-12-31']  * signal_trigger
        demand_region_signal['AB'] = region['AB'].loc[start:str(now_year)+'-12-31']  * signal_trigger
        demand_region_signal['MENA'] = region['MENA'].loc[start:str(now_year)+'-12-31']  * signal_trigger
        demand_region_signal['JKTC'] = region['JKTC'].loc[start:str(now_year)+'-12-31']  * signal_trigger
        demand_region_signal['LatAm'] = region['LatAm'].loc[start:str(now_year)+'-12-31']  * signal_trigger
        demand_region_signal['MEIP'] = region['MEIP'].loc[start:str(now_year)+'-12-31']  * signal_trigger
        demand_region_signal['EurDesk'] = region['EurDesk'].loc[start:str(now_year)+'-12-31']  * signal_trigger
        demand_region_signal['OtherEur'] = region['OtherEur'].loc[start:str(now_year)+'-12-31']  * signal_trigger
        #demand_region_signal['MedEur'] = region['MedEur'].loc[start:str(now_year)+'-12-31']  * signal_trigger
        demand_region_signal['OtherRoW'] = region['OtherRoW'].loc[start:str(now_year)+'-12-31']  * signal_trigger
        
        #demand region %
        diff_demand_regionP=pd.DataFrame(index=pd.date_range(start=start, end=str(now_year)+'-12-31', freq='M').date)
        diff_demand_regionP['Global'] = (dfdemandglobal['desk'] - dfdemandglobal['Hist'])/dfdemandglobal['Hist']
        diff_demand_regionP['PB'] =  (dfbasindemanddesk['PB Desk'].loc[start:str(now_year)+'-12-31'] - region['PB'].loc[start:str(now_year)+'-12-31'] ) /region['PB'].loc[start:str(now_year)+'-12-31']  
        diff_demand_regionP['AB'] =  (dfbasindemanddesk['AB Desk'].loc[start:str(now_year)+'-12-31'] - region['AB'].loc[start:str(now_year)+'-12-31'] )/region['AB'].loc[start:str(now_year)+'-12-31']
        diff_demand_regionP['MENA'] = (dfbasindemanddesk['MENA Desk'].loc[start:str(now_year)+'-12-31'] - region['MENA'].loc[start:str(now_year)+'-12-31'])/region['MENA'].loc[start:str(now_year)+'-12-31']
        diff_demand_regionP['JKTC'] = (dfbasindemanddesk['JKTC Desk'].loc[start:str(now_year)+'-12-31'] - region['JKTC'].loc[start:str(now_year)+'-12-31'])/region['JKTC'].loc[start:str(now_year)+'-12-31']
        diff_demand_regionP['LatAm'] = (dfbasindemanddesk['LatAm Desk'].loc[start:str(now_year)+'-12-31'] - region['LatAm'].loc[start:str(now_year)+'-12-31'])/region['LatAm'].loc[start:str(now_year)+'-12-31']
        diff_demand_regionP['MEIP'] = (dfbasindemanddesk['MEIP Desk'].loc[start:str(now_year)+'-12-31'] - region['MEIP'].loc[start:str(now_year)+'-12-31'] )/region['MEIP'].loc[start:str(now_year)+'-12-31']
        diff_demand_regionP['EurDesk'] = (dfbasindemanddesk['EurDesk Desk'].loc[start:str(now_year)+'-12-31'] - region['EurDesk'].loc[start:str(now_year)+'-12-31'])/region['EurDesk'].loc[start:str(now_year)+'-12-31']
        diff_demand_regionP['OtherEur'] = (dfbasindemanddesk['OtherEur Desk'].loc[start:str(now_year)+'-12-31'] - region['OtherEur'].loc[start:str(now_year)+'-12-31'] )/region['OtherEur'].loc[start:str(now_year)+'-12-31']
        #diff_demand_regionP['MedEur'] = (dfbasindemanddesk['MedEur Desk'].loc[start:str(now_year)+'-12-31'] - region['MedEur'].loc[start:str(now_year)+'-12-31'])/region['MedEur'].loc[start:str(now_year)+'-12-31']
        diff_demand_regionP['OtherRoW'] = (dfbasindemanddesk['OtherRoW Desk'].loc[start:str(now_year)+'-12-31'] - region['OtherRoW'].loc[start:str(now_year)+'-12-31'])/region['OtherRoW'].loc[start:str(now_year)+'-12-31']
        diff_demand_regionP = diff_demand_regionP.T.round(2)
        diff_demand_regionP.reset_index(inplace=True)
        diff_demand_regionP.rename(columns={'index':'Market'}, inplace=True)
        diff_demand_regionP = diff_demand_regionP.loc[:,:lastmonthday]
        #devide month days
        for nr in demand_region_signal.index:
                    days=calendar.monthrange(nr.year,nr.month)
                    demand_region_signal.loc[nr]=demand_region_signal.loc[nr]/days[1]
                    diff_demand_region.loc[nr]=diff_demand_region.loc[nr]/days[1]
        #region df            
        diff_demand_region = diff_demand_region.T.round(2)
        diff_demand_region.reset_index(inplace=True)
        diff_demand_region.rename(columns={'index':'Market'}, inplace=True)
        diff_demand_region = diff_demand_region.loc[:,:lastmonthday]
        #print(diff_demand_region)
        #region signal df
        demand_region_signal = demand_region_signal.T.round(2)
        demand_region_signal.reset_index(inplace=True)
        demand_region_signal.rename(columns={'index':'Market'}, inplace=True)
        demand_region_signal = demand_region_signal.loc[:,:lastmonthday]
        #print(demand_region_signal)
        
        '''creat demand market df'''
        diff_demand=pd.DataFrame(index=pd.date_range(start=start, end=str(now_year)+'-12-31', freq='M').date)
        diff_demandP=pd.DataFrame(index=pd.date_range(start=start, end=str(now_year)+'-12-31', freq='M').date)
        
        #print(dfdemanddesk.loc[start:,'China'])
        #print(dfdemand.loc[start:,'China'])
        
        for m in dfdemanddesk.columns:
            diff_demand[m]=(dfdemanddesk.loc[start:,m]-dfdemand.loc[start:,m])
            diff_demandP[m]=(dfdemanddesk.loc[start:,m]-dfdemand.loc[start:,m])/dfdemand.loc[start:,m]
        #demand country %
        diff_demandP = diff_demandP.loc[:lastmonthday,:]
        diff_demandP = diff_demandP.T.round(2)
        diff_demandP.reset_index(inplace=True)
        diff_demandP.rename(columns={'index':'Country'}, inplace=True)
        diff_demandP=diff_demandP.loc[:,:lastmonthday]
        
        # per day
        for n in diff_demand.index:
                    days=calendar.monthrange(n.year,n.month)
                    diff_demand.loc[n]=diff_demand.loc[n]/days[1]
        diff_demand = diff_demand.loc[:lastmonthday,:]
        diff_demand = diff_demand.T.round(2)
        diff_demand.reset_index(inplace=True)
        diff_demand.rename(columns={'index':'Country'}, inplace=True)
        diff_demand=diff_demand.loc[:,:lastmonthday]
        #print(diff_demand)
        
        '''create color list'''
        #supply market
        dfsupplycolor_market=diff_supply_market.iloc[:,1:]
        for ism in dfsupplycolor_market.index:
            for jsm in dfsupplycolor_market.columns:
                if dfsupplycolor_market.loc[ism,jsm] >= supply_market_signal.loc[ism,jsm]:
                    dfsupplycolor_market.loc[ism,jsm] = 'green'
                elif dfsupplycolor_market.loc[ism,jsm] <=-supply_market_signal.loc[ism,jsm]:
                    dfsupplycolor_market.loc[ism,jsm] = 'red'
                else:
                    dfsupplycolor_market.loc[ism,jsm] = 'white'
    
        dfsupplycolor_market.insert(0,'index',['paleturquoise']*dfsupplycolor_market.shape[0])
        dfsupplycolor_market=dfsupplycolor_market.T
        
        #supply plant
        dfsupplycolor=diff_supply.iloc[:,1:]
        for isp in dfsupplycolor.index:
            for jsp in dfsupplycolor.columns:
                if dfsupplycolor.loc[isp,jsp] >=10:
                    dfsupplycolor.loc[isp,jsp] = 'green'
                elif dfsupplycolor.loc[isp,jsp] <=-10:
                    dfsupplycolor.loc[isp,jsp] = 'red'
                else:
                    dfsupplycolor.loc[isp,jsp] = 'white'
    
        dfsupplycolor.insert(0,'index',['paleturquoise']*dfsupplycolor.shape[0])
        dfsupplycolor=dfsupplycolor.T
        
        #demand region
        dfdemandcolor_region=diff_demand_region.iloc[:,1:]
        for idr in dfdemandcolor_region.index:
            for jdr in dfdemandcolor_region.columns:
                if dfdemandcolor_region.loc[idr,jdr] >=demand_region_signal.loc[idr,jdr]:
                    dfdemandcolor_region.loc[idr,jdr] = 'green'
                elif dfdemandcolor_region.loc[idr,jdr] <=-demand_region_signal.loc[idr,jdr]:
                    dfdemandcolor_region.loc[idr,jdr] = 'red'
                else:
                    dfdemandcolor_region.loc[idr,jdr] = 'white'
        
        dfdemandcolor_region.insert(0,'index',['paleturquoise']*dfdemandcolor_region.shape[0])
        dfdemandcolor_region=dfdemandcolor_region.T
        #print(dfdemandcolor_region)
        #demand market
        dfdemandcolor=diff_demand.iloc[:,1:]
        for idm in dfdemandcolor.index:
            for jdm in dfdemandcolor.columns:
                if dfdemandcolor.loc[idm,jdm] >=10:
                    dfdemandcolor.loc[idm,jdm] = 'green'
                elif dfdemandcolor.loc[idm,jdm] <=-10:
                    dfdemandcolor.loc[idm,jdm] = 'red'
                else:
                    dfdemandcolor.loc[idm,jdm] = 'white'
    
        dfdemandcolor.insert(0,'index',['paleturquoise']*dfdemandcolor.shape[0])
        dfdemandcolor=dfdemandcolor.T
       
        #print(diff_demand) 
        diff_supply.to_csv('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/diffsupply.csv')
       
        return diff_supply_market, dfsupplycolor_market, diff_supply, dfsupplycolor, diff_supplyP, diff_supply_marketP, diff_demand_region, dfdemandcolor_region, diff_demand, dfdemandcolor, diff_demand_regionP, diff_demandP
    
    def plot_diff(diff_supply_market, dfsupplycolor_market, diff_supply, dfsupplycolor, diff_demand_region, dfdemandcolor_region, diff_demand, dfdemandcolor):
        today=datetime.date.today()
        '''supply chart'''
        #supply market
        fig_supply_market = go.Figure(data=[go.Table(
            header=dict(values=list(diff_supply_market.columns),
                        line_color='darkslategray',
                        fill_color='royalblue',
                        align=['left','center'],
                        font=dict(color='white', size=12),
                        height=40),
            cells=dict(values=[diff_supply_market[p] for p in diff_supply_market.columns],
                       line_color='darkslategray',
                        fill=dict(color=dfsupplycolor_market.values.tolist()),
                        align=['left', 'center'],
                        font_size=12,
                        height=30))
        ])
        
        fig_supply_market.update_layout(
                 
                 title_text='Kpler vs Desk view delta, Mcm/d '+str(today),
                   
             )
        
        py.plot(fig_supply_market, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/diff table/supply diff market.html', auto_open=False) 
        fig_supply_market.write_image('U:/Trading - Gas/LNG/LNG website/analysis/diff table/supply diff market.png')
        
        #supply plant
        fig_supply = go.Figure(data=[go.Table(
            header=dict(values=list(diff_supply.columns),
                        line_color='darkslategray',
                        fill_color='royalblue',
                        align=['left','center'],
                        font=dict(color='white', size=12),
                        height=40),
            cells=dict(values=[diff_supply[pm] for pm in diff_supply.columns],
                       line_color='darkslategray',
                        fill=dict(color=dfsupplycolor.values.tolist()),
                        align=['left', 'center'],
                        font_size=12,
                        height=30))
        ])
        
        fig_supply.update_layout(
                 
                 title_text='Kpler vs Desk view delta, Mcm/d '+str(today),
                   
             )
        
        py.plot(fig_supply, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/diff table/supply diff.html', auto_open=False) 
        fig_supply.write_image('U:/Trading - Gas/LNG/LNG website/analysis/diff table/supply diff.png')
        
        #demand 
        fig_demand_region = go.Figure(data=[go.Table(
            header=dict(values=list(diff_demand_region.columns),
                        line_color='darkslategray',
                        fill_color='royalblue',
                        align=['left','center'],
                        font=dict(color='white', size=12),
                        height=40),
            cells=dict(values=[diff_demand_region[qr] for qr in diff_demand_region.columns],
                       line_color='darkslategray',
                        fill=dict(color=dfdemandcolor_region.values.tolist()),
                        align=['left', 'center'],
                        font_size=12,
                        height=30))
        ])
        
        fig_demand_region.update_layout(
                 
                 title_text='Kpler vs Desk view delta, Mcm/d '+str(today),
                   
             )
        
        py.plot(fig_demand_region, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/diff table/demand diff region.html', auto_open=False) 
        fig_demand_region.write_image('U:/Trading - Gas/LNG/LNG website/analysis/diff table/demand diff region.png')
        
        
        fig_demand = go.Figure(data=[go.Table(
            header=dict(values=list(diff_demand.columns),
                        line_color='darkslategray',
                        fill_color='royalblue',
                        align=['left','center'],
                        font=dict(color='white', size=12),
                        height=40),
            cells=dict(values=[diff_demand[q] for q in diff_demand.columns],
                       line_color='darkslategray',
                        fill=dict(color=dfdemandcolor.values.tolist()),
                        align=['left', 'center'],
                        font_size=12,
                        height=30))
        ])
        
        fig_demand.update_layout(
                 
                 title_text='Kpler vs Desk view delta, Mcm/d '+str(today),
                   
             )
        
        py.plot(fig_demand, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/diff table/demand diff.html', auto_open=False) 
        fig_demand.write_image('U:/Trading - Gas/LNG/LNG website/analysis/diff table/demand diff.png')
    
    def plot_diffP(dfsupplycolor_market, dfsupplycolor, diff_supplyP, diff_supply_marketP,dfdemandcolor_region, dfdemandcolor, diff_demand_regionP, diff_demandP):
        
        
        
        '''supply chart'''
        #supply market
        fig_supply_market = go.Figure(data=[go.Table(
            header=dict(values=list(diff_supply_marketP.columns),
                        line_color='darkslategray',
                        fill_color='royalblue',
                        align=['left','center'],
                        font=dict(color='white', size=12),
                        height=40),
            cells=dict(values=[diff_supply_marketP[p] for p in diff_supply_marketP.columns],
                       line_color='darkslategray',
                        fill=dict(color=dfsupplycolor_market.values.tolist()),
                        align=['left', 'center'],format=[""]+[".0%"]*diff_supply_marketP.shape[1],
                        font_size=12,
                        height=30))
        ])
        
        py.plot(fig_supply_market, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/diff table/supply diff marketP.html', auto_open=False) 
        fig_supply_market.write_image('U:/Trading - Gas/LNG/LNG website/analysis/diff table/supply diff marketP.png')
        
        #supply plant
        fig_supply = go.Figure(data=[go.Table(
            header=dict(values=list(diff_supplyP.columns),
                        line_color='darkslategray',
                        fill_color='royalblue',
                        align=['left','center'],
                        font=dict(color='white', size=12),
                        height=40),
            cells=dict(values=[diff_supplyP[pm] for pm in diff_supplyP.columns],
                       line_color='darkslategray',
                        fill=dict(color=dfsupplycolor.values.tolist()),
                        align=['left', 'center'],format=[""]+[".0%"]*diff_supplyP.shape[1],
                        font_size=12,
                        height=30))
        ])
        
        py.plot(fig_supply, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/diff table/supply diffP.html', auto_open=False) 
        fig_supply.write_image('U:/Trading - Gas/LNG/LNG website/analysis/diff table/supply diffP.png')
        
        #demand 
        fig_demand_region = go.Figure(data=[go.Table(
            header=dict(values=list(diff_demand_regionP.columns),
                        line_color='darkslategray',
                        fill_color='royalblue',
                        align=['left','center'],
                        font=dict(color='white', size=12),
                        height=40),
            cells=dict(values=[diff_demand_regionP[qr] for qr in diff_demand_regionP.columns],
                       line_color='darkslategray',
                        fill=dict(color=dfdemandcolor_region.values.tolist()),
                        align=['left', 'center'],format=[""]+[".0%"]*diff_demand_regionP.shape[1],
                        font_size=12,
                        height=30))
        ])
        
        py.plot(fig_demand_region, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/diff table/demand diff regionP.html', auto_open=False) 
        fig_demand_region.write_image('U:/Trading - Gas/LNG/LNG website/analysis/diff table/demand diff regionP.png')
        
        
        fig_demand = go.Figure(data=[go.Table(
            header=dict(values=list(diff_demandP.columns),
                        line_color='darkslategray',
                        fill_color='royalblue',
                        align=['left','center'],
                        font=dict(color='white', size=12),
                        height=40),
            cells=dict(values=[diff_demandP[q] for q in diff_demandP.columns],
                       line_color='darkslategray',
                        fill=dict(color=dfdemandcolor.values.tolist()),
                        align=['left', 'center'],format=[""]+[".0%"]*diff_demandP.shape[1],
                        font_size=12,
                        height=30))
        ])
        
        py.plot(fig_demand, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/diff table/demand diffP.html', auto_open=False)
        fig_demand.write_image('U:/Trading - Gas/LNG/LNG website/analysis/diff table/demand diffP.png')
        
    def update():
        
        today=datetime.date.today()
        
        diff_supply_market, dfsupplycolor_market, diff_supply, dfsupplycolor, diff_supplyP, diff_supply_marketP, diff_demand_region, dfdemandcolor_region, diff_demand, dfdemandcolor, diff_demand_regionP, diff_demandP = DiffTable.diff(today)
        
        DiffTable.plot_diff(diff_supply_market, dfsupplycolor_market, diff_supply, dfsupplycolor, diff_demand_region, dfdemandcolor_region, diff_demand, dfdemandcolor)
        DiffTable.plot_diffP(dfsupplycolor_market, dfsupplycolor, diff_supplyP, diff_supply_marketP,dfdemandcolor_region, dfdemandcolor, diff_demand_regionP, diff_demandP)
        
#DiffTable.update()
'''
scheduler = BackgroundScheduler(job_defaults={'misfire_grace_time': 15*60})
trigger = OrTrigger([CronTrigger(day_of_week='0-6', hour='06, 12, 16', minute='23')])
scheduler.add_job(func= diff,trigger=trigger,id='diff')
scheduler.start()
runtime = datetime.datetime.now()
print (scheduler.get_jobs(), runtime)
#scheduler.remove_job('diff') 
'''