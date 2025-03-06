

# -*- coding: utf-8 -*-
"""
Created on Wed Nov 23 16:30:24 2022

@author: SVC-GASQuant2-Prod
"""

'''
model delta
8 tables

1. supply & demand sum until dec23 latest
2. same with drop down for prv
3. 1 & 2 delta color
4. plant latest
5. market latest
6. plant delta same with 2
7. market delta same with 2
8. sum table


2 chart
    1.  plant dropdown, past 7 runs, 1y, 6m, 3m
    2. market dropdown, past 10 runs, 1y, 6m, 3m
'''
#V1 add US feed gas data from US feedgas; add JKC demand 1 std temp above the US 
#V2 

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
import requests
import json
import dash
import dash_html_components as html
import dash_core_components as dcc
from dash.dependencies import Input, Output
import dash_table 
from ModelDeltachart import model_delta_chart


class model_delta():
    
    def get_data():
        
        
        #latest data
        desk_plant_latest = DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'DeskSupplyPlant').sql_to_df()
        desk_country_latest = DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'DeskSupplyCountry').sql_to_df()
        desk_supplybasin_latest = DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'BasinSupply').sql_to_df()
        #print(desk_supplybasin_latest)
        
        desk_market_latest = DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'DeskDemand').sql_to_df()
        desk_demandbasin_latest = DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'BasinDemand').sql_to_df()
        desk_demandbasin_latest.sort_values(by='Date', inplace=True)
        #print(desk_demandbasin_latest)
        #desk_demandbasin_latest.set_index('Date', inplace=True)
        #print(desk_demandbasin_latest)
        
        #hist data
        curveid = DBTOPD.get_curve_id()
        dfcurveid = curveid.loc[:,['CurveId','Type','Location','Country']]
        #print(dfcurveid)
        SupplyCurveId = dfcurveid.loc[dfcurveid[dfcurveid['Type']=='Supply'].index]
        SupplyCurveId = SupplyCurveId[['CurveId','Country','Location']]
        SupplyCurveId.rename(columns={'CurveId':'CurveID','Country':'Country','Location':'plant'}, inplace=True)
        SupplyCurveId.drop(SupplyCurveId[SupplyCurveId['plant']=='Arctic LNG 2'].index, inplace=True)
        #print(SupplyCurveId)
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
       # print(desk_supply_hist.columns)
        #print(SupplyCurveId_plant)
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
        
        
        #get marked curve, curve_id = quote('TTF Fwd')
        #TTF FWD Mwh-G EUR, TTF USD MMBtu USD, JKM MMBtu USD, BRENT USD BBL, EUA MT EUR
        curve_id = ['TTF USD FWD', 'JKM FWD'] 
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
        
        #print(dfdeskmarkethistfull)
        usfeedgas = DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'USBentekFeedgas').sql_to_df()
        
        capa =  DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'IHScapa').sql_to_df()
        
        jkctemp = DBTOPD('PRD-DB-SQL-211','LNG', 'ana', '[JKC Temp Hist]').sql_to_df() 
        
        
        
        return desk_plant_latest, desk_country_latest, desk_supplybasin_latest, desk_supply_hist_plant, desk_supply_hist_country, desk_market_latest, desk_demandbasin_latest, desk_demand_hist, dfcurvefull, usfeedgas, capa, jkctemp
        
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
        #SupplyCategories = pd.read_excel('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/Categories.xlsx',header=(0),sheet_name='Index_Supply')
        #SupplyCategories = SupplyCategories.iloc[:44,0:5]
        
        SupplyCategories=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'CategoriesSupply').sql_to_df()
        #DemandCategories=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'CategoriesDemand').sql_to_df()
        drop_list_supply = ['Vysotsk','Kollsnes', 'Stavanger']
        for i in drop_list_supply:
            SupplyCategories.drop(SupplyCategories[SupplyCategories['Plant'] == i].index, inplace=True)
        #Basin plants list, and remove 
        SupplyCategories.set_index('Basin', inplace=True)
        PB = SupplyCategories['Plant'].loc['PB'].values.tolist()
        #PB.remove('Mozambique Area 1')
        AB = SupplyCategories['Plant'].loc['AB'].values.tolist()
        #AB.remove('Calcasieu Pass')
        MENA = SupplyCategories['Plant'].loc['MENA_Bas'].values.tolist()

        
        dfdeskplanthistfull['PB Desk'] = dfdeskplanthistfull[PB].sum(axis=1)
        dfdeskplanthistfull['AB Desk'] = dfdeskplanthistfull[AB].sum(axis=1)
        dfdeskplanthistfull['MENA Desk'] = dfdeskplanthistfull[MENA].sum(axis=1)
        dfdeskplanthistfull['Global Desk'] = dfdeskplanthistfull.loc[:,['AB Desk', 'PB Desk','MENA Desk']].sum(axis=1)
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
        #DemandCategories.drop(DemandCategories[DemandCategories['Country'] == 'Russian Federation'].index, inplace=True)
        droplist = ['Algeria','Angola','Bahamas','Brunei','Cameroon','Equatorial Guinea','Libya','Nigeria','Papua New Guinea','Peru','Philipines','Qatar','Russian Federation','Trinidad and Tobago','Yemen']
        for i in droplist:
            DemandCategories.drop(DemandCategories[DemandCategories['Country']==i].index, inplace=True)
        
        DemandCategories.set_index('Suez', inplace=True)
        EoS = DemandCategories['Country'].loc['EoS'].values.tolist()
        #EoS.remove('Mozambique Area 1')
        WoS = DemandCategories['Country'].loc['WoS'].values.tolist()
        #WoS.remove('Calcasieu Pass')


        #group fpr region
        DemandCategories.set_index('Region', inplace=True)

        
        EurDesk = DemandCategories['Country'].loc['Eur Desk'].values.tolist()
        #MedEur = DemandCategories['Country'].loc['Med Eur'].values.tolist()
        
        dfdeskmarkethistfull['Global Desk'] = dfdeskmarkethistfull.sum(axis=1)
        
        dfdeskmarkethistfull['EoS Desk'] = dfdeskmarkethistfull[EoS].sum(axis=1)
        dfdeskmarkethistfull['WoS Desk'] = dfdeskmarkethistfull[WoS].sum(axis=1)
        dfdeskmarkethistfull['EurDesk Desk'] = dfdeskmarkethistfull[EurDesk].sum(axis=1)
        #dfdeskmarkethistfull['MedEur Desk'] = dfdeskmarkethistfull[MedEur].sum(axis=1)
        
        #print(dfdeskmarkethistfull)
        

        return dfdeskmarkethistfull
        
        
    def table_data(desk_plant_latest, desk_country_latest, desk_supplybasin_latest, dfdeskplanthistfull, dfdeskcountryhistfull, desk_market_latest, desk_demandbasin_latest, dfdeskmarkethistfull):
        
        today = datetime.datetime.today()
        #table 1 supply sum
        
        df_supply_sum = desk_country_latest.loc[:,['Date','Qatar','Australia','United States','Malaysia']]
        df_supply_sum['AB other'] = desk_supplybasin_latest.loc[:,'AB Desk'] - desk_country_latest.loc[:,'United States']
        df_supply_sum['PB other'] = desk_supplybasin_latest.loc[:,'PB Desk'] - desk_country_latest.loc[:,'Australia'] - desk_country_latest.loc[:,'Malaysia']
        df_supply_sum['MENA Other'] = desk_supplybasin_latest.loc[:,'MENA Desk'] - desk_country_latest.loc[:,'Qatar']
        df_supply_sum['Total Supply'] =  desk_supplybasin_latest.loc[:,'Global Desk']
        df_supply_sum.set_index('Date', inplace = True)
        df_supply_sum = df_supply_sum.resample('M').mean().round(1)
        df_supply_sum = df_supply_sum.loc[str(today.year)+'-01-31':]
        df_supply_sum.index = df_supply_sum.index.strftime('%b-%Y')
        #df_supply_sum = df_supply_sum.iloc[:,::-1]
        df_supply_sum = df_supply_sum.T
        
        #print(df_supply_sum)
        #table 1 demand sum - old
        # df_demand_sum = desk_market_latest.loc[:,['Date','Japan','South Korea','China','India']]
        # df_demand_sum['Eur Desk'] = desk_demandbasin_latest.loc[:,'EurDesk Desk']
        # df_demand_sum['Med Eur'] = desk_demandbasin_latest.loc[:,'MedEur Desk']
        # df_demand_sum['WoS Other'] = desk_demandbasin_latest.loc[:,'WoS Desk'] - desk_demandbasin_latest.loc[:,'EurDesk Desk'] - desk_demandbasin_latest.loc[:,'MedEur Desk']
        # df_demand_sum['EoS Other'] = desk_demandbasin_latest.loc[:,'EoS Desk'] - desk_market_latest.loc[:,['Japan','South Korea','China','India']].sum(axis=1)
        # df_demand_sum['Total Demand'] = desk_demandbasin_latest.loc[:,'Global Desk']
        
        # df_demand_sum.set_index('Date', inplace = True)
        # df_demand_sum = df_demand_sum.resample('M').mean().round(1)
        # df_demand_sum = df_demand_sum.loc[str(today.year)+'-01-31':]
        # df_demand_sum.index = df_demand_sum.index.strftime('%b-%Y')
        # df_demand_sum = df_demand_sum.T
    
        #table 1 demand sum - new
        df_demand_sum = desk_market_latest.loc[:,['Date','Japan','South Korea','China','India','Italy','Spain','Poland']].set_index('Date')
        df_demand_sum = df_demand_sum.merge(how='outer', left_index=True, right_index=True, right=desk_demandbasin_latest.set_index('Date')[['EurDesk Desk','WoS Desk', 'EoS Desk']])
        df_demand_sum = df_demand_sum[df_demand_sum.index >= datetime.datetime(today.year, 1, 1)]  
        df_demand_sum['WoS Other'] = df_demand_sum['WoS Desk'] - df_demand_sum['EurDesk Desk']# - df_demand_sum['MedEur Desk']
        df_demand_sum['EoS Other'] = df_demand_sum['EoS Desk'] - df_demand_sum[['Japan','South Korea','China','India']].sum(axis=1) 
        df_demand_sum['Total Demand'] = desk_demandbasin_latest.set_index('Date')['Global Desk']
        df_demand_sum = df_demand_sum.rename({'EurDesk Desk' : 'Eur Desk'}, axis=1)     
        print(df_demand_sum)
        df_demand_sum = df_demand_sum.resample('M').mean().round(1)        
        df_demand_sum.index = df_demand_sum.index.strftime('%b-%Y')
        #df_demand_sum = df_demand_sum.iloc[:,::-1]
        df_demand_sum = df_demand_sum.T
        #print(df_demand_sum)
        #a = desk_demandbasin_latest.loc[:,['Date', 'EurDesk Desk']]        

        #print(dfdeskcountryhistfull)
        #table 2 spply previous
        df_supply_sum_pre = dfdeskcountryhistfull.loc[:,['Qatar','Australia','United States','Malaysia']]
        df_supply_sum_pre['AB other'] = dfdeskplanthistfull.loc[:,'AB Desk'] - dfdeskcountryhistfull.loc[:,'United States']
        df_supply_sum_pre['PB other'] = dfdeskplanthistfull.loc[:,'PB Desk'] - dfdeskcountryhistfull.loc[:,'Australia'] - dfdeskcountryhistfull.loc[:,'Malaysia']
        df_supply_sum_pre['MENA Other'] = dfdeskplanthistfull.loc[:,'MENA Desk'] - dfdeskcountryhistfull.loc[:,'Qatar']
        df_supply_sum_pre['Total Supply'] =  dfdeskplanthistfull.loc[:,'Global Desk']
        df_supply_sum_pre = df_supply_sum_pre.resample('M').mean().round(1)
        df_supply_sum_pre = df_supply_sum_pre.loc[str(today.year)+'-01-31':]
        df_supply_sum_pre.index = df_supply_sum_pre.index.strftime('%b-%Y')
        #df_supply_sum_pre = df_supply_sum_pre.iloc[:,::-1]
        df_supply_sum_pre = df_supply_sum_pre.T

        #table 2 demand previous
        df_demand_sum_pre = dfdeskmarkethistfull.loc[:,['Japan','South Korea','China','India','Italy','Spain','Poland']]
        df_demand_sum_pre['Eur Desk'] = dfdeskmarkethistfull.loc[:,'EurDesk Desk']
        #df_demand_sum_pre['Med Eur'] = dfdeskmarkethistfull.loc[:,'MedEur Desk']
        df_demand_sum_pre['WoS Desk'] = dfdeskmarkethistfull.loc[:,'WoS Desk']
        df_demand_sum_pre['EoS Desk'] = dfdeskmarkethistfull.loc[:,'EoS Desk']
        df_demand_sum_pre['WoS Other'] = dfdeskmarkethistfull.loc[:,'WoS Desk'] - dfdeskmarkethistfull.loc[:,'EurDesk Desk']# - dfdeskmarkethistfull.loc[:,'MedEur Desk']
        df_demand_sum_pre['EoS Other'] = dfdeskmarkethistfull.loc[:,'EoS Desk'] - dfdeskmarkethistfull.loc[:,['Japan','South Korea','China','India']].sum(axis=1)
        df_demand_sum_pre['Total Demand'] = dfdeskmarkethistfull.loc[:,'Global Desk']
        df_demand_sum_pre = df_demand_sum_pre.resample('M').mean().round(1)
        df_demand_sum_pre = df_demand_sum_pre.loc[str(today.year)+'-01-31':]
        df_demand_sum_pre.index = df_demand_sum_pre.index.strftime('%b-%Y')
        #df_demand_sum_pre = df_demand_sum_pre.iloc[:,::-1]
        df_demand_sum_pre = df_demand_sum_pre.T

        #table 3 supply delta
        df_supply_delta = df_supply_sum - df_supply_sum_pre
        
        #table 3 demand delta
        df_demand_delta = df_demand_sum.loc[:] - df_demand_sum_pre.loc[:]
        df_demand_delta = df_demand_delta.reindex(index = df_demand_sum_pre.index)
        #print(df_demand_sum)
        #print(df_demand_sum_pre)
        #print(df_demand_delta)
        #table 4 plant latest
        df_plant_latest = desk_plant_latest.copy()
        df_plant_latest.set_index('Date', inplace=True)
        df_plant_latest = df_plant_latest.resample('M').mean().round(1)
        df_plant_latest = df_plant_latest.loc[str(today.year)+'-01-31':]
        df_plant_latest.index = df_plant_latest.index.strftime('%b-%Y')
        df_plant_latest = df_plant_latest.T
        

        #table 5 market latest        
        df_market_latest = desk_market_latest.copy()
        df_market_latest.set_index('Date', inplace=True)
        df_market_latest = df_market_latest.resample('M').mean().round(1)
        df_market_latest = df_market_latest.loc[str(today.year)+'-01-31':]
        df_market_latest.index = df_market_latest.index.strftime('%b-%Y')
        df_market_latest = df_market_latest.T
        
        #table 6 plant delta
        dfdeskplanthistfull_m = dfdeskplanthistfull.resample('M').mean().round(1)
        dfdeskplanthistfull_m = dfdeskplanthistfull_m.loc[str(today.year)+'-01-31':]
        dfdeskplanthistfull_m.index = dfdeskplanthistfull_m.index.strftime('%b-%Y')
        dfdeskplanthistfull_m = dfdeskplanthistfull_m.T
        #print(df_plant_latest)
        #print(dfdeskplanthistfull_m.loc[df_plant_latest.index])
        df_plant_delta = df_plant_latest - dfdeskplanthistfull_m.loc[df_plant_latest.index]
        df_plant_delta = df_plant_delta.round(1)
        #print(df_plant_delta)
        #table 7 market delta
        dfdeskmarkethistfull_m = dfdeskmarkethistfull.resample('M').mean().round(1)
        dfdeskmarkethistfull_m = dfdeskmarkethistfull_m.loc[str(today.year)+'-01-31':]
        dfdeskmarkethistfull_m.index = dfdeskmarkethistfull_m.index.strftime('%b-%Y')
        dfdeskmarkethistfull_m = dfdeskmarkethistfull_m.T
        df_market_delta = df_market_latest - dfdeskmarkethistfull_m.loc[df_market_latest.index]
        df_market_delta = df_market_delta.round(1)
        #print(df_market_delta)
        
        return df_supply_sum, df_demand_sum, df_supply_sum_pre, df_demand_sum_pre, df_supply_delta, df_demand_delta, df_plant_latest, df_market_latest, df_plant_delta, df_market_delta
        
        
    def chart_plant_data(plant, desk_supply_hist_plant, start_date, end_date):
        
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
            dfplant.loc[:,str(date.date())] = dfdeskplanthistfull[[plant]].values
        
        today = datetime.datetime.today()
        dfplant = dfplant.resample('M').mean().round(1)
        dfplant = dfplant.loc[str(today.year)+'-01-31':]
        dfplant.index = dfplant.index.strftime('%b-%Y')
        #print(dfplant)
        return dfplant
    
    def chart_market_data(market, desk_demand_hist, start_date, end_date):
        
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
            #print(dfdeskmarkethistfull.loc[:,market])
            dfmarket.loc[:,str(date.date())] = dfdeskmarkethistfull.loc[:,market].values
        #print(dfmarket)
        today = datetime.datetime.today()
        dfmarket = dfmarket.resample('M').mean().round(1)
        
        dfmarket = dfmarket.loc[str(today.year)+'-01-31':]
        dfmarket.index = dfmarket.index.strftime('%b-%Y')
        #print(dfmarket)
        return dfmarket
    


    def jkcdemand(jkctemp, start_date, end_date):
        
        jkctemp.set_index('index', inplace=True)
        #find win start and end
        today = datetime.date.today()
        
        #create fcst temp df
        dfmodel = pd.DataFrame(index=pd.date_range(start=start_date, end=end_date, freq='D').date, columns=['China','Japan','South Korea'])
        dfmodel.index = pd.to_datetime(dfmodel.index)
        #print(start_date)
        
        for i in dfmodel.index:
            dfmodel.loc[i, 'normal China'] = jkctemp.loc['2020-'+str(i.month)+'-'+str(i.day),'China normal']
            dfmodel.loc[i, 'normal Japan'] = jkctemp.loc['2020-'+str(i.month)+'-'+str(i.day),'Japan normal']
            dfmodel.loc[i, 'normal South Korea'] = jkctemp.loc['2020-'+str(i.month)+'-'+str(i.day),'South Korea normal']
        dfmodel = dfmodel.round(2)
        #print(dfmodel)
        
        #create winter df
        dfchina = pd.DataFrame(index=pd.date_range(start=start_date, end=end_date))       
        dfchina['normal'] = dfmodel['normal China']
        dfchina['winsum-std'] = np.nan
        dfchina['winsum+std'] =np.nan 
        
        dfjapan = pd.DataFrame(index=pd.date_range(start=start_date, end=end_date))       
        dfjapan['normal'] = dfmodel['normal Japan']
        dfjapan['winsum-std'] = np.nan
        dfjapan['winsum+std'] =np.nan 
        
        dfsk = pd.DataFrame(index=pd.date_range(start=start_date, end=end_date))       
        dfsk['normal'] = dfmodel['normal South Korea']
        dfsk['winsum-std'] = np.nan
        dfsk['winsum+std'] =np.nan 
        
        normalyoy = jkctemp.copy()
        normalyoy['year'] = normalyoy.index.year
        normalyoy['date'] = normalyoy.index.strftime('%m-%d')
        dfchinayoy = normalyoy.set_index(['year', 'date'])['China'].unstack(0)
        #print(dfchinayoy)
        dfchinastd = dfchinayoy.loc[:,:].std(axis=1)
        dfjapanyoy = normalyoy.set_index(['year', 'date'])['Japan'].unstack(0)
        dfjapanstd = dfjapanyoy.std(axis=1)
        dfskyoy = normalyoy.set_index(['year', 'date'])['South Korea'].unstack(0)
        dfskstd = dfskyoy.std(axis=1)
        #print(dfchinastd)
        #print(dfjapanstd)
        #print(dfskstd)
        
        
        for i in dfchina.index:
            #print(str(i)[5:10])
            dfchina.loc[i,'normal-std'] = dfchina.loc[i,'normal']-dfchinastd.loc[str(i)[5:10]]
            dfchina.loc[i,'normal+std'] = dfchina.loc[i,'normal']+dfchinastd.loc[str(i)[5:10]]
            
            dfjapan.loc[i,'normal-std'] = dfjapan.loc[i,'normal']-dfjapanstd.loc[str(i)[5:10]]
            dfjapan.loc[i,'normal+std'] = dfjapan.loc[i,'normal']+dfjapanstd.loc[str(i)[5:10]]
            
            dfsk.loc[i,'normal-std'] = dfsk.loc[i,'normal']-dfskstd.loc[str(i)[5:10]]
            dfsk.loc[i,'normal+std'] = dfsk.loc[i,'normal']+dfskstd.loc[str(i)[5:10]]
        
        #model china
        dfmodelchina = pd.DataFrame(index=pd.date_range(start=start_date, end=end_date))
        dfmodelchina['normal-std'] = 0.6*dfchina['normal-std']**2 - 30.9*dfchina['normal-std'] + 781.6
        dfmodelchina['normal+std'] = 0.6*dfchina['normal+std']**2 - 30.9*dfchina['normal+std'] + 781.6
        dfmodelchina['normal'] = 0.6*dfchina['normal']**2 - 30.9*dfchina['normal'] + 781.6
        dfmodelchina['normal+std vs. normal'] = dfmodelchina['normal+std'] - dfmodelchina['normal']
        dfmodelchina['normal-std vs. normal'] = dfmodelchina['normal-std'] - dfmodelchina['normal']
        #print(dfmodelchina)
        
        dfmodeljapan = pd.DataFrame(index=pd.date_range(start=start_date, end=end_date))
        dfmodeljapan['normal-std'] = 0.3*dfjapan['normal-std']**2 - 11.1*dfjapan['normal-std'] + 214.6
        dfmodeljapan['normal+std'] = 0.3*dfjapan['normal-std']**2 - 11.1*dfjapan['normal-std'] + 214.6
        dfmodeljapan['normal'] = 0.3*dfjapan['normal']**2 - 11.1*dfjapan['normal'] + 214.6
        dfmodeljapan['normal+std vs. normal'] = dfmodeljapan['normal+std'] - dfmodeljapan['normal']
        dfmodeljapan['normal-std vs. normal'] = dfmodeljapan['normal-std'] - dfmodeljapan['normal']
        
        dfmodelsk = pd.DataFrame(index=pd.date_range(start=start_date, end=end_date))
        dfmodelsk['normal-std'] = 0.1*dfsk['normal-std']**2 - 6.4*dfsk['normal-std'] + 181.9
        dfmodelsk['normal+std'] = 0.1*dfsk['normal-std']**2 - 6.4*dfsk['normal-std'] + 181.9
        dfmodelsk['normal'] = 0.1*dfsk['normal']**2 - 6.4*dfsk['normal'] + 181.9
        dfmodelsk['normal+std vs. normal'] = dfmodelsk['normal+std'] - dfmodelsk['normal']
        dfmodelsk['normal-std vs. normal'] = dfmodelsk['normal-std'] - dfmodelsk['normal']
        
        dfmodelstd = pd.DataFrame(index=pd.date_range(start=start_date, end=end_date))
        dfmodelstd['JK normal+std vs. normal'] = dfmodeljapan['normal+std vs. normal']+dfmodelsk['normal+std vs. normal']
        dfmodelstd['JK normal-std vs. normal'] =  dfmodeljapan['normal-std vs. normal']+dfmodelsk['normal-std vs. normal']
        '''
        for i in dfchina.index:
            #print(str(i)[5:10])
            dfchina.loc[i,'std'] = dfchinastd.loc[str(i)[5:10]]
            
            dfjapan.loc[i,'std'] = dfjapanstd.loc[str(i)[5:10]]
            
            dfsk.loc[i,'std'] = dfskstd.loc[str(i)[5:10]]
            
        #print(dfchina)
        dfmodelchina = pd.DataFrame(index=pd.date_range(start=start_date, end=end_date))
        dfmodelchina['std'] = 0.6*dfchina['std']**2 - 30.9*dfchina['std'] + 781.6
        #print(dfmodelchina)
        
        dfmodeljapan = pd.DataFrame(index=pd.date_range(start=start_date, end=end_date))
        dfmodeljapan['std'] = 0.3*dfjapan['std']**2 - 11.1*dfjapan['std'] + 214.6
        
        dfmodelsk = pd.DataFrame(index=pd.date_range(start=start_date, end=end_date))
        dfmodelsk['std'] = 0.1*dfsk['std']**2 - 6.4*dfsk['std'] + 181.9
        
        dfmodelstd = pd.DataFrame(index=pd.date_range(start=start_date, end=end_date))
        dfmodelstd['std'] = dfmodelchina['std'] + dfmodeljapan['std']+dfmodelsk['std']
        '''
        
        
        #print(dfmodelstd)
        return dfmodelstd
        
        
    def sum_table(dfcurvefull, desk_country_latest, desk_supply_hist_country, desk_demand_hist, desk_demandbasin_latest, usfeedgas, capa, dfmodelstd, start_date, end_date):
        
        #print(dfdeskmarkethistfull)
        today = datetime.datetime.today()
        update_date = DBTOPD.get_model_run_date()
        m12 = update_date.loc[update_date[update_date['ForecastDate'] > (datetime.datetime.today() - relativedelta(months = 12))].index[-1],'ForecastDate']

        lastdemand = model_delta.hist_data_demand(str(update_date.loc[1,'ForecastDate']), desk_demand_hist, start_date, end_date)
        y1demandeur = model_delta_chart.chart_market_data('EurDesk', desk_demand_hist, start_date, end_date)
        y1demandjktc = model_delta_chart.chart_market_data('JKTC', desk_demand_hist, start_date, end_date)
        lastsupply = model_delta.hist_data_supply_country(str(update_date.loc[1,'ForecastDate']), desk_supply_hist_country, start_date, end_date)
        #print(lastsupply)
        dfsumprice = pd.DataFrame(index=pd.date_range(start=start_date,end=end_date, freq='MS'), columns = ['JKM','JKM-TTF'])
        dfsumprice.loc[:,'JKM'] = (dfcurvefull.loc[start_date:end_date,'JKM FWD']).round(1)
        dfsumprice.loc[:,'JKM-TTF'] = (dfcurvefull.loc[start_date:end_date,'JKM FWD'] - dfcurvefull.loc[start_date:end_date,'TTF USD FWD']).round(1)
        dfsumprice = dfsumprice.loc[str(today.year)+'-01-01':]
        dfsumprice.index = dfsumprice.index.strftime('%b-%Y')
        dfsumprice = dfsumprice.T
        #print(dfsumprice)
                     
        dfsumeur = pd.DataFrame(index=pd.date_range(start=start_date,end=end_date), columns = ['Eur Desk','Base','Last View','Delta','Y-1'])
        #print(dfsumeur)
        dfsumeur.loc[:,'Eur Desk'] = np.nan
        dfsumeur.loc[:,'Base'] = desk_demandbasin_latest.loc[:,'EurDesk Desk'].values
        dfsumeur.loc[:,'Last View'] = lastdemand.loc[:,'EurDesk Desk']
        dfsumeur.loc[:,'Delta'] = dfsumeur.loc[:,'Base'] -  dfsumeur.loc[:,'Last View'] 
        #print(dfsumeur)
        dfsumeur = dfsumeur.resample('M').mean().round(1)
        dfsumeur = dfsumeur.loc[str(today.year)+'-01-31':]
        dfsumeur.index = dfsumeur.index.strftime('%b-%Y')
        dfsumeur.loc[:,'Y-1'] = y1demandeur.loc[:,str(m12.date())]
        dfsumeur = dfsumeur.T
        #print(dfsumeur)
                                  
        dfsumjktc = pd.DataFrame(index=pd.date_range(start=start_date,end=end_date), columns = ['JKTC','Base','Last View','Delta','Y-1'])
        dfsumjktc.loc[:,'JKTC'] = np.nan
        dfsumjktc.loc[:,'Base'] = desk_demandbasin_latest.loc[:,'JKTC Desk'].values
        dfsumjktc.loc[:,'Last View'] = lastdemand.loc[:,['China','Japan','South Korea','Taiwan']].sum(axis=1)
        dfsumjktc.loc[:,'Delta'] = dfsumjktc.loc[:,'Base'] -  dfsumjktc.loc[:,'Last View'] 
        dfsumjktc.loc[:,'JK normal-std vs. normal'] = dfmodelstd['JK normal-std vs. normal']
        dfsumjktc.loc[:,'JK normal+std vs. normal'] = dfmodelstd['JK normal+std vs. normal']
        dfsumjktc = dfsumjktc.resample('M').mean().round(1)
        dfsumjktc = dfsumjktc.loc[str(today.year)+'-01-31':]
        dfsumjktc.index = dfsumjktc.index.strftime('%b-%Y')
        dfsumjktc.loc[:,'Y-1'] = y1demandjktc.loc[:,str(m12.date())]
        
        dfsumjktc = dfsumjktc.T
        #print(desk_country_latest.loc[start_date:])
        #print(dfsumjktc)
        
        capa.set_index('index', inplace=True)
        capa = capa.loc[start_date:end_date]
        for i in capa.index:
            days=calendar.monthrange(i.year,i.month)
            capa.loc[i]=capa.loc[i]/12*1397/days[1]
        #print( capa.loc[start_date:end_date, ['Cameron (Liqu.)','Cove Point','Sabine Pass','Corpus Christi','Elba Island Liq.','Freeport','Calcasieu Pass']].sum(axis=1))
        #ma30.set_index('Date', inplace=True)
        dfus = desk_country_latest[['Date','United States']].copy()
        dfus.set_index('Date', inplace=True)
        usfeedgas.set_index('MEASUREMENT_DATE', inplace=True)
        #print(usfeedgas)
        dfsumus = pd.DataFrame(index=pd.date_range(start=start_date,end=end_date), columns = ['United States','Base','US Utilisation*','Last View','Delta','US Feed.'])
        dfsumus.loc[:,'United States'] = np.nan
        dfsumus.loc[:,'Base'] = dfus.loc[:,'United States']
        dfsumus.loc[:,'US Utilisation*'] = (dfsumus.loc[:,'Base'] / capa.loc[start_date:end_date, ['Cameron (Liqu.)','Cove Point','Sabine Pass','Corpus Christi','Elba Island Liq.','Freeport','Calcasieu Pass']].sum(axis=1))#.apply(lambda x: format(x, '.2%'))
        dfsumus.loc[:,'Last View'] = lastsupply.loc[:,'United States']
        dfsumus.loc[:,'Delta'] = dfsumus.loc[:,'Base'] -  dfsumus.loc[:,'Last View'] 
        dfsumus.loc[:today,'US Feed.'] = usfeedgas.loc[:today,'LNG Exports Feedgas']/1000
        dfsumus.loc[today:,'US Feed.'] = dfsumus.loc[today:,'Base']*(1+0.12)*12.89/365
        dfsumus.loc[:,'US Feed.'] = dfsumus.loc[:,'US Feed.'].astype('float')
        #print(dfsumus.info())
        dfsumus = dfsumus.resample('M').mean()
        #print(dfsumus)
        dfsumus = dfsumus.loc[str(today.year)+'-01-31':]
        dfsumus.index = dfsumus.index.strftime('%b-%Y')
        dfsumus.loc[:,'US Utilisation*'] = dfsumus.loc[:,'US Utilisation*'].apply(lambda x: format(x, '.2%'))
        dfsumus = dfsumus.round(1).T
        
        
        dfsum = pd.concat([dfsumprice, dfsumeur, dfsumjktc, dfsumus])
        dfsum = dfsum.round(1)
        #print(dfsum)
        return dfsum

'''
desk_plant_latest, desk_country_latest, desk_supplybasin_latest, desk_supply_hist_plant, desk_supply_hist_country, desk_market_latest, desk_demandbasin_latest, desk_demand_hist, dfcurvefull, usfeedgas, capa, jkctemp = model_delta.get_data()
start_date = desk_demandbasin_latest['Date'].iloc[0]
end_date = desk_demandbasin_latest['Date'].iloc[-1]
rundate = '2022-08-31 00:00:00'
dfdeskplanthistfull = model_delta.hist_data_supply_plant(rundate, desk_supply_hist_plant, start_date, end_date)
dfdeskcountryhistfull = model_delta.hist_data_supply_country(rundate, desk_supply_hist_country, start_date, end_date)
dfdeskmarkethistfull = model_delta.hist_data_demand(rundate, desk_demand_hist, start_date, end_date)
dfmodelstd = model_delta.jkcdemand(jkctemp, start_date, end_date)

model_delta.sum_table(dfcurvefull, desk_country_latest, desk_supply_hist_country, desk_demand_hist, desk_demandbasin_latest, usfeedgas, capa, dfmodelstd, start_date, end_date)



df_supply_sum, df_demand_sum, df_supply_sum_pre, df_demand_sum_pre, df_supply_delta, df_demand_delta, df_plant_latest, df_market_latest, df_plant_delta, df_market_delta = model_delta.table_data(desk_plant_latest, desk_country_latest, desk_supplybasin_latest, dfdeskplanthistfull, dfdeskcountryhistfull, desk_market_latest, desk_demandbasin_latest, dfdeskmarkethistfull)
'''

