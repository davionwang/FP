# -*- coding: utf-8 -*-
"""
Created on Fri Sep 30 10:57:28 2022

@author: SVC-GASQuant2-Prod
"""

'''
2 table2
    1. supply, same group with ma10 table, 3 drop downs, [plant],[market],[basin], delta of latest vs selected
    2. demand, same group with ma10 table, 3 drop downs, [market],[region],[basin], delta of latest vs selected
    
2 chart
    1. plant dropdown, past 7 runs, 1y, 6m, 3m
    2. market dropdown, past 10 runs, 1y, 6m, 3m
'''

import pandas as pd
import sys
sys.path.append('C:\\Users\SVC-GASQuant2-Prod\YWwebsite\Python Scripts\class') 
import numpy as np
import datetime
import sqlalchemy
from dateutil.relativedelta import relativedelta
import plotly.offline as py
import plotly.graph_objs as go
from plotly.subplots import make_subplots
pd.set_option('display.max_columns',20)
from DBtoDF import DBTOPD
import calendar

import dash
import dash_html_components as html
import dash_core_components as dcc
from dash.dependencies import Input, Output
import dash_table 


class model_delta_chart():
    
    def get_data():
        
        
        #latest data
        desk_plant_latest = DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'DeskSupplyPlant').sql_to_df()
        desk_country_latest = DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'DeskSupplyCountry').sql_to_df()
        desk_supplybasin_latest = DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'BasinSupply').sql_to_df()
        
        
        desk_market_latest = DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'DeskDemand').sql_to_df()
        desk_demandbasin_latest = DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'BasinDemand').sql_to_df()
        #print(desk_market_latest)
        
        #hist data
        curveid = DBTOPD.get_curve_id()
        dfcurveid = curveid.loc[:,['CurveId','Type','Location','Country']]
        #print(dfcurveid)
        SupplyCurveId = dfcurveid.loc[dfcurveid[dfcurveid['Type']=='Supply'].index]
        SupplyCurveId = SupplyCurveId[['CurveId','Country','Location']]
        SupplyCurveId.rename(columns={'CurveId':'CurveID','Country':'Country','Location':'plant'}, inplace=True)
        SupplyCurveId.drop(SupplyCurveId[SupplyCurveId['plant']=='Arctic LNG 2'].index, inplace=True)
        
        DemandCurveId = dfcurveid.loc[dfcurveid[dfcurveid['Type']=='Demand'].index]
        DemandCurveId = DemandCurveId[['CurveId','Country','Location']]
        DemandCurveId.rename(columns={'CurveId':'CurveID','Country':'Country','Location':'plant'}, inplace=True)
        DemandCurveId.drop(DemandCurveId[DemandCurveId['Country']=='Russian Federation'].index, inplace=True)
        
        
        desk_supply_hist = DBTOPD.get_desk_supply_hist()
        SupplyCurveId_plant=SupplyCurveId.loc[:,['CurveID','plant']]
        SupplyCurveId_plant=SupplyCurveId_plant.set_index('CurveID').T.to_dict('list')
        #replace curve id to country name
        desk_supply_hist_plant = desk_supply_hist.copy()
        desk_supply_hist_plant.loc[:,'CurveId'].replace(SupplyCurveId_plant, inplace=True)
        #print(desk_supply_hist.columns)
        #print(desk_supply_hist.loc[desk_supply_hist[desk_supply_hist['ForecastDate'] == str(update_date.loc[0, 'ForecastDate'])].index])
        
        supplyCurveId_country=SupplyCurveId.iloc[:,0:2]
        supplyCurveId_country=supplyCurveId_country.set_index('CurveID').T.to_dict('list')
        desk_supply_hist_country = desk_supply_hist.copy()
        desk_supply_hist_country.loc[:,'CurveId'].replace(supplyCurveId_country, inplace=True)
        #print(desk_supply_hist_country)
        
        desk_demand_hist = DBTOPD.get_desk_demand_hist()
        DemandCurveId=DemandCurveId.loc[:,['CurveID','Country']]
        DemandCurveId=DemandCurveId.set_index('CurveID').T.to_dict('list')
        #replace curve id to country name
        desk_demand_hist.loc[:,'CurveId'].replace(DemandCurveId, inplace=True)
        
        demand=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'DemandCountry')
        dfdemand=demand.sql_to_df()
        dfdemand.set_index('Date', inplace=True)
        
        supplyplant=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'SupplyPlant')
        dfsupplyplant=supplyplant.sql_to_df()
        dfsupplyplant.set_index('Date', inplace=True)
        
        
        
        #print(dfdeskmarkethistfull)
        
        return desk_plant_latest, desk_country_latest, desk_supplybasin_latest, desk_supply_hist_plant, desk_supply_hist_country, desk_market_latest, desk_demandbasin_latest, desk_demand_hist, dfdemand, dfsupplyplant
    

        
        
    def hist_data_supply_plant(rundate, desk_supply_hist_plant, start_date, end_date):
        
        
        desk_supply_hist = desk_supply_hist_plant.loc[desk_supply_hist_plant[desk_supply_hist_plant['ForecastDate'] == str(rundate)].index,['CurveId','ValueDate','Value']]
        #print(desk_supply_hist)
        #change data format
        dfsupplynew=desk_supply_hist.groupby(['ValueDate','CurveId'], as_index=False).sum()
        dfsupplynew.loc[:,'ValueDate'] = pd.to_datetime(dfsupplynew.loc[:,'ValueDate'])
        dfsupplynew=pd.pivot(dfsupplynew, index='ValueDate', columns='CurveId')
        dfsupplynew.index.name='Date'
        dfsupplynew.columns=dfsupplynew.columns.droplevel(0)
        dfsupplynew.rename_axis(None, axis=1, inplace=True)
        for i in dfsupplynew.index:
            days=calendar.monthrange(i.year,i.month)
            dfsupplynew.loc[i]=dfsupplynew.loc[i]*1397/days[1]
        #change to full date data
        dfalldesksupply=pd.DataFrame(index=pd.date_range(start=start_date,end=end_date))
        dfalldesksupply.index.name='Date'
        dfdeskplanthistfull = pd.merge(dfalldesksupply, dfsupplynew, on='Date', how='outer')
        dfdeskplanthistfull.fillna(method='ffill', inplace=True) 
        dfdeskplanthistfull=dfdeskplanthistfull.loc[start_date:end_date]
        #print(dfdeskplanthistfull.columns)
        SupplyCategories=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'CategoriesSupply').sql_to_df()
        #DemandCategories=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'CategoriesDemand').sql_to_df()
        
        
        #Basin plants list, and remove 
        SupplyCategories.set_index('Basin', inplace=True)
        PB = SupplyCategories['Plant'].loc['PB'].values.tolist()
        #PB.remove('Mozambique Area 1')
        AB = SupplyCategories['Plant'].loc['AB'].values.tolist()
        #AB.remove('Calcasieu Pass')
        MENA = SupplyCategories['Plant'].loc['MENA_Bas'].values.tolist()

        
        dfdeskplanthistfull['PB'] = dfdeskplanthistfull[PB].sum(axis=1)
        dfdeskplanthistfull['AB'] = dfdeskplanthistfull[AB].sum(axis=1)
        dfdeskplanthistfull['MENA'] = dfdeskplanthistfull[MENA].sum(axis=1)
        dfdeskplanthistfull['Global'] = dfdeskplanthistfull.loc[:,['AB', 'PB','MENA']].sum(axis=1)
        #print(dfdeskplanthistfull)
        
        return dfdeskplanthistfull
    
    
    def hist_data_supply_country(rundate, desk_supply_hist_country, start_date, end_date):
        
        desk_supply_hist_country = desk_supply_hist_country.loc[desk_supply_hist_country[desk_supply_hist_country['ForecastDate'] == str(rundate)].index,['CurveId','ValueDate','Value']]
        #print(desk_supply_hist_country)
        #change data format
        dfsupplynew=desk_supply_hist_country.groupby(['ValueDate','CurveId'], as_index=False).sum()
        dfsupplynew.loc[:,'ValueDate'] = pd.to_datetime(dfsupplynew.loc[:,'ValueDate'])
        dfsupplynew=pd.pivot(dfsupplynew, index='ValueDate', columns='CurveId')
        dfsupplynew.index.name='Date'
        dfsupplynew.columns=dfsupplynew.columns.droplevel(0)
        dfsupplynew.rename_axis(None, axis=1, inplace=True)
        

        for i in dfsupplynew.index:
            days=calendar.monthrange(i.year,i.month)
            dfsupplynew.loc[i]=dfsupplynew.loc[i]*1397/days[1]
        #print(dfsupplynew)
      
        #change to full date data
        dfalldesksupply=pd.DataFrame(index=pd.date_range(start=start_date,end=end_date))
        dfalldesksupply.index.name='Date'
        dfdeskcountryhistfull = pd.merge(dfalldesksupply, dfsupplynew, on='Date', how='outer')
        dfdeskcountryhistfull.fillna(method='ffill', inplace=True) 
        dfdeskcountryhistfull = dfdeskcountryhistfull.loc[start_date:end_date]
        #print(dfdeskcountryhistfull)
        
        

        return dfdeskcountryhistfull
    
        
    def hist_data_demand(rundate, desk_demand_hist, start_date, end_date):
    
        #demand
        demand_hist = desk_demand_hist.loc[desk_demand_hist[desk_demand_hist['ForecastDate'] == str(rundate)].index]
        #change data format
        dfdemandnew=demand_hist.groupby(['ValueDate','CurveId'], as_index=False).sum()
        dfdemandnew.loc[:,'ValueDate'] = pd.to_datetime(dfdemandnew.loc[:,'ValueDate'])
        dfdemandnew=pd.pivot(dfdemandnew, index='ValueDate', columns='CurveId')
        dfdemandnew.index.name='Date'
        dfdemandnew.columns=dfdemandnew.columns.droplevel(0)
        dfdemandnew.rename_axis(None, axis=1, inplace=True)
        no_china_cols = [x for x in dfdemandnew.columns if x != 'China']
        for i in dfdemandnew.index:
            days=calendar.monthrange(i.year,i.month)
            #China conversion 1376
            dfdemandnew.loc[i,'China']=dfdemandnew.loc[i, 'China']*1376/days[1]
            #all others 1397
            dfdemandnew.loc[i,no_china_cols]=dfdemandnew.loc[i,no_china_cols]*1397/days[1]
        #change to full date data
        dfalldeskdemand=pd.DataFrame(index=pd.date_range(start=start_date,end=end_date))
        dfalldeskdemand.index.name='Date'
        
        dfdeskmarkethistfull = pd.merge(dfalldeskdemand, dfdemandnew, on='Date', how='outer')
        dfdeskmarkethistfull.fillna(method='ffill', inplace=True) 
        dfdeskmarkethistfull=dfdeskmarkethistfull.loc[start_date:end_date]
        #print(dfdeskmarkethistfull)
        
        DemandCategories=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'CategoriesDemand').sql_to_df()

        droplist = ['Algeria','Angola','Bahamas','Brunei','Cameroon','Equatorial Guinea','Libya','Nigeria','Papua New Guinea','Peru','Philipines','Qatar','Russian Federation','Trinidad and Tobago','Yemen']
        for i in droplist:
            DemandCategories.drop(DemandCategories[DemandCategories['Country']==i].index, inplace=True)
        
        DemandCategories.set_index('Suez', inplace=True)
        EoS = DemandCategories['Country'].loc['EoS'].values.tolist()
        #EoS.remove('Mozambique Area 1')
        WoS = DemandCategories['Country'].loc['WoS'].values.tolist()
        #WoS.remove('Calcasieu Pass')

        DemandCategories.set_index('Basin', inplace=True)
        
        #Basin plants list, and remove 
        PB = DemandCategories['Country'].loc['PB'].values.tolist()
        #PB.remove('Mozambique Area 1')
        AB = DemandCategories['Country'].loc['AB'].values.tolist()
        #AB.remove('Calcasieu Pass')
        MENA = DemandCategories['Country'].loc['MENA'].values.tolist()
        
        #group fpr region
        DemandCategories.set_index('Region', inplace=True)

        EurDesk = DemandCategories['Country'].loc['Eur Desk'].values.tolist()
        #MedEur = DemandCategories['Country'].loc['Med Eur'].values.tolist()
        JKTC = DemandCategories['Country'].loc['JKTC'].values.tolist()
        MEIP = DemandCategories['Country'].loc['MEIP'].values.tolist()
        OtherEur = DemandCategories['Country'].loc['Other Eur'].values.tolist()
        LatAm = DemandCategories['Country'].loc['Lat Am'].values.tolist()
        OtherRoW = DemandCategories['Country'].loc['Other RoW'].values.tolist()
        
        dfdeskmarkethistfull['Global'] = dfdeskmarkethistfull.sum(axis=1)
        
        dfdeskmarkethistfull['EoS'] = dfdeskmarkethistfull[EoS].sum(axis=1)
        dfdeskmarkethistfull['WoS'] = dfdeskmarkethistfull[WoS].sum(axis=1)
        
        dfdeskmarkethistfull['PB'] = dfdeskmarkethistfull[PB].sum(axis=1)
        dfdeskmarkethistfull['AB'] = dfdeskmarkethistfull[AB].sum(axis=1)
        dfdeskmarkethistfull['MENA'] = dfdeskmarkethistfull[MENA].sum(axis=1)
        
        dfdeskmarkethistfull['EurDesk'] = dfdeskmarkethistfull[EurDesk].sum(axis=1)
        #dfdeskmarkethistfull['MedEur'] = dfdeskmarkethistfull[MedEur].sum(axis=1)
        dfdeskmarkethistfull['JKTC'] = dfdeskmarkethistfull[JKTC].sum(axis=1)
        dfdeskmarkethistfull['MEIP'] = dfdeskmarkethistfull[MEIP].sum(axis=1)
        dfdeskmarkethistfull['OtherEur'] = dfdeskmarkethistfull[OtherEur].sum(axis=1)
        dfdeskmarkethistfull['LatAm'] = dfdeskmarkethistfull[LatAm].sum(axis=1)
        dfdeskmarkethistfull['OtherRoW'] = dfdeskmarkethistfull[OtherRoW].sum(axis=1)
        
        #print(dfdeskmarkethistfull)
        

        return dfdeskmarkethistfull
        
    
        
    def supply_delta_data(rundate, option, desk_country_latest, desk_supplybasin_latest, dfdeskplanthistfull, dfdeskcountryhistfull):
        
        today = datetime.datetime.today()
        
        desk_country_latest.set_index('Date', inplace=True)
        desk_country_latest.sort_index(inplace=True)
        for i in desk_country_latest.columns:
            desk_country_latest.rename(columns={i: i+' Desk'}, inplace=True)

        desk_supplybasin_latest.set_index('Date', inplace=True)
        desk_supplybasin_latest.sort_index(inplace=True)
        desk_latest = pd.concat([desk_country_latest, desk_supplybasin_latest], axis = 1)
        #print(desk_latest.columns)
        
             
        desk_pre = pd.concat([dfdeskplanthistfull, dfdeskcountryhistfull], axis = 1)
        #print(desk_pre[option])
        
        dfdelta_supply = desk_latest[[option+' Desk']]
        dfdelta_supply.loc[:,rundate] = desk_pre[option]
        dfdelta_supply.loc[:,'Delta'] = desk_latest[option+' Desk'] - desk_pre[option]
        dfdelta_supply.rename(columns={option+' Desk':option+' Desk latest'}, inplace=True)
        dfdelta_supply = dfdelta_supply.resample('M').mean().round(1)
        dfdelta_supply = dfdelta_supply.loc[str(today.year)+'-01-31':]
        dfdelta_supply.index = dfdelta_supply.index.strftime('%b-%Y')
        dfdelta_supply = dfdelta_supply.astype('int')
        #print(dfdelta_supply)
        return dfdelta_supply
    
    def demand_delta_data(rundate, option, desk_demandbasin_latest, dfdeskmarkethistfull):
        
        today = datetime.datetime.today()
        
        desk_demandbasin_latest.set_index('Date', inplace=True)
        desk_demandbasin_latest.sort_index(inplace=True)
        
        for i in dfdeskmarkethistfull.columns:
            dfdeskmarkethistfull.rename(columns={i: i+' Desk'}, inplace=True)
        #print(desk_demandbasin_latest.columns)
        #print(dfdeskmarkethistfull.columns)
        
        dfdelta_demand = desk_demandbasin_latest[[option+' Desk']]
        #print(dfdelta_demand)
        dfdelta_demand.loc[:,rundate] = dfdeskmarkethistfull.loc[:, option+' Desk']
        dfdelta_demand.loc[:,'Delta'] = desk_demandbasin_latest[option+' Desk'] - dfdeskmarkethistfull[option+' Desk']
        dfdelta_demand.rename(columns={option+' Desk':option+' Desk latest'}, inplace=True)
        dfdelta_demand = dfdelta_demand.resample('M').mean().round(1)
        dfdelta_demand = dfdelta_demand.loc[str(today.year)+'-01-31':]
        dfdelta_demand.index = dfdelta_demand.index.strftime('%b-%Y')
        dfdelta_demand = dfdelta_demand.astype('int')
        #print(dfdelta_demand)
        return dfdelta_demand
        
        
   
    def chart_plant_data(option, desk_supply_hist_plant, start_date, end_date):
        
        update_date = DBTOPD.get_model_run_date()
        
        #print(update_date.loc[0:6])
        
        run_date = update_date.loc[0:6,'ForecastDate'].to_list()
        m3 = update_date.loc[update_date[update_date['ForecastDate'] > (datetime.datetime.today() - relativedelta(months = 3))].index[-1],'ForecastDate']
        m6 = update_date.loc[update_date[update_date['ForecastDate'] > (datetime.datetime.today() - relativedelta(months = 6))].index[-1],'ForecastDate']
        m12 = update_date.loc[update_date[update_date['ForecastDate'] > (datetime.datetime.today() - relativedelta(months = 12))].index[-1],'ForecastDate']
        #print(run_date)
        run_date.append(m3)
        run_date.append(m6)
        run_date.append(m12)
        
        #print(run_date)
        SupplyCategories=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'CategoriesSupply').sql_to_df()
        
        #print(SupplyCategories.loc[SupplyCategories[SupplyCategories['Market'] == 'Australia'].index, 'Plant'].values)
        #Basin plants list, and remove 
        SupplyCategories.set_index('Basin', inplace=True)
        PB = SupplyCategories['Plant'].loc['PB'].values.tolist()
        #PB.remove('Mozambique Area 1')
        AB = SupplyCategories['Plant'].loc['AB'].values.tolist()
        #AB.remove('Calcasieu Pass')
        MENA = SupplyCategories['Plant'].loc['MENA_Bas'].values.tolist()
        Global = PB + AB + MENA
        
        name_dict = {
                     'PB':PB,
                     'AB':AB,
                     'MENA':MENA,
                     'Global':Global
            }
        #print(SupplyCategories)
        market_list = SupplyCategories['Market'].dropna().drop_duplicates().to_list()
        SupplyCategories.reset_index(inplace=True)
        #print(SupplyCategories)
        #print(SupplyCategories.loc['Yemen', 'Plant'].to_list())
        
        for i in market_list:
            #print(i)
            name_dict[i] = SupplyCategories.loc[SupplyCategories[SupplyCategories['Market'] == i].index, 'Plant'].to_list()
        
        for i in SupplyCategories['Plant'].tolist():
            name_dict[i] = [i]
        #print(name_dict)
        dfplant = pd.DataFrame(index=pd.date_range(start=start_date,end=end_date))
        #print(dfplant)
        for date in run_date:      
            #print(str((i)))
            desk_supply_hist = desk_supply_hist_plant.loc[desk_supply_hist_plant[desk_supply_hist_plant['ForecastDate'] == str(date)].index,['CurveId','ValueDate','Value']]
            #print(desk_supply_hist)
            #change data format
            dfsupplynew=desk_supply_hist.groupby(['ValueDate','CurveId'], as_index=False).sum()
            dfsupplynew.loc[:,'ValueDate'] = pd.to_datetime(dfsupplynew.loc[:,'ValueDate'])
            dfsupplynew=pd.pivot(dfsupplynew, index='ValueDate', columns='CurveId')
            dfsupplynew.index.name='Date'
            dfsupplynew.columns=dfsupplynew.columns.droplevel(0)
            dfsupplynew.rename_axis(None, axis=1, inplace=True)
            for i in dfsupplynew.index:
                days=calendar.monthrange(i.year,i.month)
                dfsupplynew.loc[i]=dfsupplynew.loc[i]*1397/days[1]
            #change to full date data
            dfalldesksupply=pd.DataFrame(index=pd.date_range(start=start_date,end=end_date))
            dfalldesksupply.index.name='Date'
            dfdeskplanthistfull = pd.merge(dfalldesksupply, dfsupplynew, on='Date', how='outer')
            dfdeskplanthistfull.fillna(method='ffill', inplace=True) 
            dfdeskplanthistfull=dfdeskplanthistfull.loc[start_date:end_date]
            #print(dfdeskplanthistfull[[plant]])
            #print(date)
            #dfplant.loc[:,str(date.date())] = dfdeskplanthistfull[name_dict[option]].values
            if len(name_dict[option]) == 1:
                try:
                    dfplant.loc[:,str(date.date())] = dfdeskplanthistfull.loc[:,name_dict[option]].values
                except KeyError:
                    dfplant.loc[:,str(date.date())] = 0
            else:
                try:
                    dfplant.loc[:,str(date.date())] = dfdeskplanthistfull.loc[:,name_dict[option]].sum(axis=1).values
                except KeyError as e:
                    #print(e)
                    #name_dict[option].remove('Germany')
                    name_dict[option].remove('Calcasieu Pass')
                    dfplant.loc[:,str(date.date())] = dfdeskplanthistfull.loc[:,name_dict[option]].sum(axis=1).values 
                    
        today = datetime.datetime.today()
        dfplant = dfplant.resample('M').mean().round(1)
        dfplant = dfplant.loc[str(today.year)+'-01-31':]
        dfplant.index = dfplant.index.strftime('%b-%Y')
        #print(dfplant)
        return dfplant
    
    def chart_market_data(option, desk_demand_hist, start_date, end_date):
        
        update_date = DBTOPD.get_model_run_date()
        
        #print(update_date.loc[0:6])
        
        run_date = update_date.loc[0:6,'ForecastDate'].to_list()
        m3 = update_date.loc[update_date[update_date['ForecastDate'] > (datetime.datetime.today() - relativedelta(months = 3))].index[-1],'ForecastDate']
        m6 = update_date.loc[update_date[update_date['ForecastDate'] > (datetime.datetime.today() - relativedelta(months = 6))].index[-1],'ForecastDate']
        m12 = update_date.loc[update_date[update_date['ForecastDate'] > (datetime.datetime.today() - relativedelta(months = 12))].index[-1],'ForecastDate']
        #print(run_date)
        run_date.append(m3)
        run_date.append(m6)
        run_date.append(m12)
        
        #print(run_date)
        DemandCategories=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'CategoriesDemand').sql_to_df()
        #DemandCategories.drop(DemandCategories[DemandCategories['Country'] == 'Russian Federation'].index, inplace=True)
        droplist = ['Algeria','Angola','Bahamas','Brunei','Cameroon','Equatorial Guinea','Libya','Nigeria','Papua New Guinea','Peru','Philipines','Qatar','Russian Federation','Trinidad and Tobago','Yemen']
        for i in droplist:
            DemandCategories.drop(DemandCategories[DemandCategories['Country']==i].index, inplace=True)
        
        DemandCategories.set_index('Suez', inplace=True)
        EoS = DemandCategories['Country'].loc['EoS'].values.tolist()
        #EoS.remove('Mozambique Area 1')
        WoS = DemandCategories['Country'].loc['WoS'].values.tolist()
        #WoS.remove('Calcasieu Pass')

        DemandCategories.set_index('Basin', inplace=True)
        
        #Basin plants list, and remove 
        PB = DemandCategories['Country'].loc['PB'].values.tolist()
        #PB.remove('Mozambique Area 1')
        AB = DemandCategories['Country'].loc['AB'].values.tolist()
        #AB.remove('Calcasieu Pass')
        MENA = DemandCategories['Country'].loc['MENA'].values.tolist()
        
        Global = PB + AB + MENA
        #group fpr region
        DemandCategories.set_index('Region', inplace=True)

        EurDesk = DemandCategories['Country'].loc['Eur Desk'].values.tolist()
        #MedEur = DemandCategories['Country'].loc['Med Eur'].values.tolist()
        JKTC = DemandCategories['Country'].loc['JKTC'].values.tolist()
        MEIP = DemandCategories['Country'].loc['MEIP'].values.tolist()
        OtherEur = DemandCategories['Country'].loc['Other Eur'].values.tolist()
        LatAm = DemandCategories['Country'].loc['Lat Am'].values.tolist()
        OtherRoW = DemandCategories['Country'].loc['Other RoW'].values.tolist()
        
        name_dict = {'EoS':EoS,
                     'WoS':WoS,
                     'PB':PB,
                     'AB':AB,
                     'MENA':MENA,
                     'EurDesk':EurDesk,
                     #'MedEur':MedEur,
                     'JKTC':JKTC,
                     'MEIP':MEIP,
                     'OtherEur':OtherEur,
                     'LatAm':LatAm,
                     'OtherRoW':OtherRoW,
                     'Global':Global
            }
        
        for i in DemandCategories['Country'].dropna().drop_duplicates().to_list():
            name_dict[i] = [i]
        
        #print(name_dict[option])
        
        dfmarket = pd.DataFrame(index=pd.date_range(start=start_date,end=end_date))
        #print(dfplant)
        for date in run_date:      
            #print(str((date)))
            #demand
            demand_hist = desk_demand_hist.loc[desk_demand_hist[desk_demand_hist['ForecastDate'] == str(date)].index]
            #change data format
            dfdemandnew=demand_hist.groupby(['ValueDate','CurveId'], as_index=False).sum()
            dfdemandnew.loc[:,'ValueDate'] = pd.to_datetime(dfdemandnew.loc[:,'ValueDate'])
            dfdemandnew=pd.pivot(dfdemandnew, index='ValueDate', columns='CurveId')
            dfdemandnew.index.name='Date'
            dfdemandnew.columns=dfdemandnew.columns.droplevel(0)
            dfdemandnew.rename_axis(None, axis=1, inplace=True)
            no_china_cols = [x for x in dfdemandnew.columns if x != 'China']
            for i in dfdemandnew.index:
                days=calendar.monthrange(i.year,i.month)
                #China conversion 1376
                dfdemandnew.loc[i,'China']=dfdemandnew.loc[i, 'China']*1376/days[1]
                #all others 1397
                dfdemandnew.loc[i,no_china_cols]=dfdemandnew.loc[i,no_china_cols]*1397/days[1]
            #print(dfdemandnew)
            #change to full date data
            dfalldeskdemand=pd.DataFrame(index=pd.date_range(start=start_date,end=end_date))
            dfalldeskdemand.index.name='Date'
            
            dfdeskmarkethistfull = pd.merge(dfalldeskdemand, dfdemandnew, on='Date', how='outer')
            dfdeskmarkethistfull.fillna(method='ffill', inplace=True) 
            dfdeskmarkethistfull=dfdeskmarkethistfull.loc[start_date:end_date]
            #print(dfdeskmarkethistfull.columns)
            #print(dfdeskmarkethistfull.loc[:])
            #print(len(name_dict[option]))
            if len(name_dict[option]) == 1:
                try:
                    dfmarket.loc[:,str(date.date())] = dfdeskmarkethistfull.loc[:,name_dict[option]].values
                except KeyError:
                    dfmarket.loc[:,str(date.date())] = 0
            else:
                try:
                    dfmarket.loc[:,str(date.date())] = dfdeskmarkethistfull.loc[:,name_dict[option]].sum(axis=1).values
                except KeyError as e:
                    #print(e)
                    try:
                        name_dict[option].remove('Germany')
                        dfmarket.loc[:,str(date.date())] = dfdeskmarkethistfull.loc[:,name_dict[option]].sum(axis=1).values
                    except (KeyError, ValueError):
                        try:
                            name_dict[option].remove('El Salvador')
                            dfmarket.loc[:,str(date.date())] = dfdeskmarkethistfull.loc[:,name_dict[option]].sum(axis=1).values
                        except KeyError:
                            name_dict[option].remove('El Salvador')
                            name_dict[option].remove('Germany')
                            
                            dfmarket.loc[:,str(date.date())] = dfdeskmarkethistfull.loc[:,name_dict[option]].sum(axis=1).values
        #print(dfmarket)
        today = datetime.datetime.today()
        dfmarket = dfmarket.resample('M').mean().round(1)
        
        dfmarket = dfmarket.loc[str(today.year)+'-01-31':]
        dfmarket.index = dfmarket.index.strftime('%b-%Y')
        #print(dfmarket)
        return dfmarket
        
    def dropdown_options():
        
        SupplyCategories=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'CategoriesSupply').sql_to_df()
        DemandCategories=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'CategoriesDemand').sql_to_df()
        
        #DemandCategories.drop(DemandCategories[DemandCategories['Country'] == 'Russian Federation'].index, inplace=True)
        droplist = ['Algeria','Angola','Bahamas','Brunei','Cameroon','Equatorial Guinea','Libya','Nigeria','Papua New Guinea','Peru','Philipines','Qatar','Russian Federation','Trinidad and Tobago','Yemen']
        for i in droplist:
            DemandCategories.drop(DemandCategories[DemandCategories['Country']==i].index, inplace=True)
        #print(DemandCategories['Suez'].dropna().drop_duplicates().to_list())
        return SupplyCategories, DemandCategories
    
    
    def minmaxsupply(dfsupplyplant, start_date, end_date):
        
        #print(run_date)
        SupplyCategories=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'CategoriesSupply').sql_to_df()
        #DemandCategories=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'CategoriesDemand').sql_to_df()
        
        #print(SupplyCategories.loc[SupplyCategories[SupplyCategories['Market'] == 'Australia'].index, 'Plant'].values)
        #Basin plants list, and remove 
        SupplyCategories.set_index('Basin', inplace=True)
        PB = SupplyCategories['Plant'].loc['PB'].values.tolist()
        #PB.remove('Mozambique Area 1')
        AB = SupplyCategories['Plant'].loc['AB'].values.tolist()
        #AB.remove('Calcasieu Pass')
        MENA = SupplyCategories['Plant'].loc['MENA_Bas'].values.tolist()
        Global = PB + AB + MENA
        
        name_dict = {
                     'PB':PB,
                     'AB':AB,
                     'MENA':MENA,
                     'Global':Global
            }
        #print(SupplyCategories)
        market_list = SupplyCategories['Market'].dropna().drop_duplicates().to_list()
        SupplyCategories.reset_index(inplace=True)
        #print(SupplyCategories)
        #print(SupplyCategories.loc['Yemen', 'Plant'].to_list())
        
        for i in market_list:
            #print(i)
            name_dict[i] = SupplyCategories.loc[SupplyCategories[SupplyCategories['Market'] == i].index, 'Plant'].to_list()
        
        for i in SupplyCategories['Plant'].tolist():
            name_dict[i] = [i]
        
        
        today = datetime.datetime.today()
        current_year_start = str(today.year)+'-01-01'
        current_year_end = str(today.year)+'-12-31'
        
        #print(current_year_start-relativedelta(years=5), current_year_end-relativedelta(years=5))
        #print(dfsupplyplant.loc[start_date-relativedelta(years=2):end_date-relativedelta(years=2), name_dict[i]].sum(axis = 1))
        
        dfsupply_range = pd.DataFrame(index=pd.date_range(start=start_date, end=end_date, freq='MS'))
        
        
        for i in name_dict.keys():
            
            try:
                supply_yoy=pd.DataFrame(index=pd.date_range(start=current_year_start, end=current_year_end, freq = 'MS'), columns=['Kpler 5','Kpler 4','Kpler 3','Kpler 2','Kpler 1','max','min','Kpler 2022','Desk View','Desk View 2023','Difference','Kpler ETA'])
                #print(supply_yoy.shape)
                supply_yoy['Kpler 5'].loc[current_year_start:current_year_end] = dfsupplyplant.loc[str(today.year-5)+'-01-01':str(today.year-5)+'-12-31', name_dict[i]].sum(axis = 1).resample('MS').mean().values
                supply_yoy['Kpler 4'].loc[current_year_start:current_year_end] = dfsupplyplant.loc[str(today.year-4)+'-01-01':str(today.year-4)+'-12-31', name_dict[i]].sum(axis = 1).resample('MS').mean().values
                supply_yoy['Kpler 3'].loc[current_year_start:current_year_end] = dfsupplyplant.loc[str(today.year-3)+'-01-01':str(today.year-3)+'-12-31', name_dict[i]].sum(axis = 1).resample('MS').mean().values
                supply_yoy['Kpler 2'].loc[current_year_start:current_year_end] = dfsupplyplant.loc[str(today.year-2)+'-01-01':str(today.year-2)+'-12-31', name_dict[i]].sum(axis = 1).resample('MS').mean().values
                supply_yoy['Kpler 1'].loc[current_year_start:current_year_end] = dfsupplyplant.loc[str(today.year-1)+'-01-01':str(today.year-1)+'-12-31', name_dict[i]].sum(axis = 1).resample('MS').mean().values
                supply_yoy['max']=supply_yoy[['Kpler 5','Kpler 4','Kpler 3','Kpler 2','Kpler 1']].max(axis=1)
                supply_yoy['min']=supply_yoy[['Kpler 5','Kpler 4','Kpler 3','Kpler 2','Kpler 1']].min(axis=1)
                
                #supply_yoy = supply_yoy.resample('MS').mean()
                supply_yoy=supply_yoy.round()
                #print(supply_yoy['max'].to_list() +  supply_yoy['max'].to_list()  +  supply_yoy['max'].to_list() )
                dfsupply_range[i+' max'] = supply_yoy['max'].to_list() +  supply_yoy['max'].to_list()  +  supply_yoy['max'].to_list() +  supply_yoy['max'].to_list()
                dfsupply_range[i+' min'] = supply_yoy['min'].to_list() +  supply_yoy['min'].to_list()  +  supply_yoy['min'].to_list() +  supply_yoy['min'].to_list()
            except KeyError:
                dfsupply_range[i+' max'] = 0
                dfsupply_range[i+' min'] = 0
        #print(dfsupply_range)     
        dfsupply_range = dfsupply_range.loc[str(today.year)+'-01-01':]
        dfsupply_range.index = dfsupply_range.index.strftime('%b-%Y')
        #print(dfsupply_range)  
        
        return dfsupply_range
    

    def minmaxdemand(dfdemand, start_date, end_date):

                
    #print(run_date)
        DemandCategories=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'CategoriesDemand').sql_to_df()
        #DemandCategories.drop(DemandCategories[DemandCategories['Country'] == 'Russian Federation'].index, inplace=True)
        droplist = ['Algeria','Angola','Bahamas','Brunei','Cameroon','Equatorial Guinea','Libya','Nigeria','Papua New Guinea','Peru','Philipines','Qatar','Russian Federation','Trinidad and Tobago','Yemen']
        for i in droplist:
            DemandCategories.drop(DemandCategories[DemandCategories['Country']==i].index, inplace=True)
        
        DemandCategories.set_index('Suez', inplace=True)
        EoS = DemandCategories['Country'].loc['EoS'].values.tolist()
        #EoS.remove('Mozambique Area 1')
        WoS = DemandCategories['Country'].loc['WoS'].values.tolist()
        #WoS.remove('Calcasieu Pass')

        DemandCategories.set_index('Basin', inplace=True)
        
        #Basin plants list, and remove 
        PB = DemandCategories['Country'].loc['PB'].values.tolist()
        #PB.remove('Mozambique Area 1')
        AB = DemandCategories['Country'].loc['AB'].values.tolist()
        #AB.remove('Calcasieu Pass')
        MENA = DemandCategories['Country'].loc['MENA'].values.tolist()
        
        Global = PB + AB + MENA
        #group fpr region
        DemandCategories.set_index('Region', inplace=True)

        EurDesk = DemandCategories['Country'].loc['Eur Desk'].values.tolist()
        #MedEur = DemandCategories['Country'].loc['Med Eur'].values.tolist()
        JKTC = DemandCategories['Country'].loc['JKTC'].values.tolist()
        MEIP = DemandCategories['Country'].loc['MEIP'].values.tolist()
        OtherEur = DemandCategories['Country'].loc['Other Eur'].values.tolist()
        LatAm = DemandCategories['Country'].loc['Lat Am'].values.tolist()
        OtherRoW = DemandCategories['Country'].loc['Other RoW'].values.tolist()
        
        name_dict = {'EoS':EoS,
                     'WoS':WoS,
                     'PB':PB,
                     'AB':AB,
                     'MENA':MENA,
                     'EurDesk':EurDesk,
                     #'MedEur':MedEur,
                     'JKTC':JKTC,
                     'MEIP':MEIP,
                     'OtherEur':OtherEur,
                     'LatAm':LatAm,
                     'OtherRoW':OtherRoW,
                     'Global':Global
            }
        
        for i in DemandCategories['Country'].dropna().drop_duplicates().to_list():
            name_dict[i] = [i]
            
            
        today = datetime.datetime.today()
        current_year_start = str(today.year)+'-01-01'
        current_year_end = str(today.year)+'-12-31'
        
        #print(current_year_start-relativedelta(years=5), current_year_end-relativedelta(years=5))
        #print(dfsupplyplant.loc[start_date-relativedelta(years=2):end_date-relativedelta(years=2), name_dict[i]].sum(axis = 1))
        
        dfdemand_range = pd.DataFrame(index=pd.date_range(start=start_date, end=end_date, freq='MS'))
        
        
        for i in name_dict.keys():
            
            try:
                demand_yoy=pd.DataFrame(index=pd.date_range(start=current_year_start, end=current_year_end, freq = 'MS'), columns=['Kpler 5','Kpler 4','Kpler 3','Kpler 2','Kpler 1','max','min','Kpler 2022','Desk View','Desk View 2023','Difference','Kpler ETA'])
                #print(demand_yoy.shape)
                demand_yoy['Kpler 5'].loc[current_year_start:current_year_end] = dfdemand.loc[str(today.year-5)+'-01-01':str(today.year-5)+'-12-31', name_dict[i]].sum(axis = 1).resample('MS').mean().values
                demand_yoy['Kpler 4'].loc[current_year_start:current_year_end] = dfdemand.loc[str(today.year-4)+'-01-01':str(today.year-4)+'-12-31', name_dict[i]].sum(axis = 1).resample('MS').mean().values
                demand_yoy['Kpler 3'].loc[current_year_start:current_year_end] = dfdemand.loc[str(today.year-3)+'-01-01':str(today.year-3)+'-12-31', name_dict[i]].sum(axis = 1).resample('MS').mean().values
                demand_yoy['Kpler 2'].loc[current_year_start:current_year_end] = dfdemand.loc[str(today.year-2)+'-01-01':str(today.year-2)+'-12-31', name_dict[i]].sum(axis = 1).resample('MS').mean().values
                demand_yoy['Kpler 1'].loc[current_year_start:current_year_end] = dfdemand.loc[str(today.year-1)+'-01-01':str(today.year-1)+'-12-31', name_dict[i]].sum(axis = 1).resample('MS').mean().values
                demand_yoy['max']=demand_yoy[['Kpler 5','Kpler 4','Kpler 3','Kpler 2','Kpler 1']].max(axis=1)
                demand_yoy['min']=demand_yoy[['Kpler 5','Kpler 4','Kpler 3','Kpler 2','Kpler 1']].min(axis=1)
                
                #supply_yoy = supply_yoy.resample('MS').mean()
                demand_yoy=demand_yoy.round()
                #print(supply_yoy['max'].to_list() +  supply_yoy['max'].to_list()  +  supply_yoy['max'].to_list() )
                dfdemand_range[i+' max'] = demand_yoy['max'].to_list() +  demand_yoy['max'].to_list()  +  demand_yoy['max'].to_list() +  demand_yoy['max'].to_list()
                dfdemand_range[i+' min'] = demand_yoy['min'].to_list() +  demand_yoy['min'].to_list()  +  demand_yoy['min'].to_list() +  demand_yoy['min'].to_list()
            except KeyError:
                dfdemand_range[i+' max'] = 0
                dfdemand_range[i+' min'] = 0
        #print(dfdemand_range)    
        dfdemand_range = dfdemand_range.loc[str(today.year)+'-01-01':]
        dfdemand_range.index = dfdemand_range.index.strftime('%b-%Y')
        
        return dfdemand_range
            
            
#model_delta_chart.dropdown_options()        
'''
desk_plant_latest, desk_country_latest, desk_supplybasin_latest, desk_supply_hist_plant, desk_supply_hist_country, desk_market_latest, desk_demandbasin_latest, desk_demand_hist, dfdemand, dfsupplyplant = model_delta_chart.get_data()
start_date = desk_demandbasin_latest['Date'].iloc[0]
end_date = desk_demandbasin_latest['Date'].iloc[-1]
rundate = '2022-08-31 00:00:00'
option = 'APLNG'

dfdeskplanthistfull = model_delta_chart.hist_data_supply_plant(rundate, desk_supply_hist_plant, start_date, end_date)
#print(desk_plant_latest)
dfdeskcountryhistfull = model_delta_chart.hist_data_supply_country(rundate, desk_supply_hist_country, start_date, end_date)
#print(dfdeskcountryhistfull)
dfdeskmarkethistfull = model_delta_chart.hist_data_demand(rundate, desk_demand_hist, start_date, end_date)
#print(dfdeskmarkethistfull)
dfdelta_supply = model_delta_chart.supply_delta_data(rundate, option, desk_country_latest, desk_supplybasin_latest, dfdeskplanthistfull, dfdeskcountryhistfull)
#print(dfdelta_supply)
#model_delta_chart.demand_delta_data(rundate, option, desk_demandbasin_latest, dfdeskmarkethistfull)

dfplant = model_delta_chart.chart_plant_data(option, desk_supply_hist_plant, start_date, end_date)
dfmarket = model_delta_chart.chart_market_data('Bangladesh', desk_demand_hist, start_date, end_date)
'''
#model_delta_chart.minmaxsupply(dfsupplyplant, start_date, end_date)
#model_delta_chart.minmaxdemand(dfdemand, start_date, end_date)

'''
fig_plant = go.Figure()
for i in dfplant.columns:
    fig_plant.add_trace(go.Scatter(x=dfplant.index, y=dfplant[i],
                        mode='lines',
                        name=i,
                        ))
fig_plant.update_layout(
             autosize=True,
             showlegend=True,
             #colorscale='RdBu',
             legend=dict(x=0, y=-0.2),
             legend_orientation="h",
             title_text='Yamal past 7 runs and 3M, 6M, 1Y ',
             #xaxis = dict(dtick="M2"),
             #xaxis_tickformat = '%B',
             hovermode='x unified',
             plot_bgcolor = 'white',
             template='ggplot2'  ,

         )

py.plot(fig_plant, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/Yamal scenarios.html', auto_open=True)
'''
'''
hdlistsupply = []

for i in df_supply_sum.columns:
    hd={"name":i,"id": i}
    hdlistsupply.append(hd)
    
#for j in hdlistsupply:
#print(hdlistsupply)    
hdlistsupply[0]['name'] = 'Supply'
#print(df_supply_sum.to_dict('records'))
print(df_supply_delta.to_dict('records'))

app = dash.Dash(__name__)

app.layout =  dash_table.DataTable(
                
                id='table3supply',
                columns = hdlistsupply,
                data = df_supply_delta.to_dict('records'),
        
                style_cell={
                    'whiteSpace': 'normal',
                    'height': 'auto',
                    #'border': '3px solid grey'
                },
                
                 style_data={
                        'color': 'black',
                        'backgroundColor': '#D9D9D9'
                    },
                style_header={
                    'backgroundColor': '#D9D9D9',
                    #'fontWeight': 'bold',
                    'textAlign': 'center',
                    #'border': '3px solid black'
                    #'color': 'white'
                },
                
                style_data_conditional=[
                    {
                        'if': {
                            'filter_query': '{{{}}} >= 10'.format(col),
                            'column_id': col
                        },
                        'backgroundColor': '#98FB98',
                        'color': 'blue'
                    } for col in df_supply_sum.columns
                ],
                
                #style_data_conditional=stylessupplyup,
                style_as_list_view=True,
            
            )

if __name__ == '__main__':
    app.run_server(debug=False, host='0.0.0.0', port=8060)
'''