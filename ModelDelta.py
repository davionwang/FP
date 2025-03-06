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
        
        
        desk_market_latest = DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'DeskDemand').sql_to_df()
        desk_demandbasin_latest = DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'BasinDemand').sql_to_df()
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
        
        return desk_plant_latest, desk_country_latest, desk_supplybasin_latest, desk_supply_hist_plant, desk_supply_hist_country, desk_market_latest, desk_demandbasin_latest, desk_demand_hist, dfcurvefull
        
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

        
        NWEur = DemandCategories['Country'].loc['NW Eur'].values.tolist()
        MedEur = DemandCategories['Country'].loc['Med Eur'].values.tolist()
        
        dfdeskmarkethistfull['Global Desk'] = dfdeskmarkethistfull.sum(axis=1)
        
        dfdeskmarkethistfull['EoS Desk'] = dfdeskmarkethistfull[EoS].sum(axis=1)
        dfdeskmarkethistfull['WoS Desk'] = dfdeskmarkethistfull[WoS].sum(axis=1)
        dfdeskmarkethistfull['NWEur Desk'] = dfdeskmarkethistfull[NWEur].sum(axis=1)
        dfdeskmarkethistfull['MedEur Desk'] = dfdeskmarkethistfull[MedEur].sum(axis=1)
        
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
        # df_demand_sum['NW Eur'] = desk_demandbasin_latest.loc[:,'NWEur Desk']
        # df_demand_sum['Med Eur'] = desk_demandbasin_latest.loc[:,'MedEur Desk']
        # df_demand_sum['WoS Other'] = desk_demandbasin_latest.loc[:,'WoS Desk'] - desk_demandbasin_latest.loc[:,'NWEur Desk'] - desk_demandbasin_latest.loc[:,'MedEur Desk']
        # df_demand_sum['EoS Other'] = desk_demandbasin_latest.loc[:,'EoS Desk'] - desk_market_latest.loc[:,['Japan','South Korea','China','India']].sum(axis=1)
        # df_demand_sum['Total Demand'] = desk_demandbasin_latest.loc[:,'Global Desk']
        
        # df_demand_sum.set_index('Date', inplace = True)
        # df_demand_sum = df_demand_sum.resample('M').mean().round(1)
        # df_demand_sum = df_demand_sum.loc[str(today.year)+'-01-31':]
        # df_demand_sum.index = df_demand_sum.index.strftime('%b-%Y')
        # df_demand_sum = df_demand_sum.T
    
        #table 1 demand sum - new
        df_demand_sum = desk_market_latest.loc[:,['Date','Japan','South Korea','China','India']].set_index('Date')
        df_demand_sum = df_demand_sum.merge(how='outer', left_index=True, right_index=True, right=desk_demandbasin_latest.set_index('Date')[['NWEur Desk', 'MedEur Desk','WoS Desk', 'EoS Desk']])
        df_demand_sum = df_demand_sum[df_demand_sum.index >= datetime.datetime(today.year, 1, 1)]        
        df_demand_sum['WoS Other'] = df_demand_sum['WoS Desk'] - df_demand_sum['NWEur Desk'] - df_demand_sum['MedEur Desk']
        df_demand_sum['EoS Other'] = df_demand_sum['EoS Desk'] - df_demand_sum[['Japan','South Korea','China','India']].sum(axis=1) 
        df_demand_sum['Total Demand'] = desk_demandbasin_latest.set_index('Date')['Global Desk']
        df_demand_sum = df_demand_sum.rename({'NWEur Desk' : 'NW Eur', 'MedEur Desk' : 'Med Eur'}, axis=1)        
        df_demand_sum = df_demand_sum.resample('M').mean().round(1)        
        df_demand_sum.index = df_demand_sum.index.strftime('%b-%Y')
        #df_demand_sum = df_demand_sum.iloc[:,::-1]
        df_demand_sum = df_demand_sum.T
        #print(df_demand_sum)
        #a = desk_demandbasin_latest.loc[:,['Date', 'NWEur Desk']]        

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
        df_demand_sum_pre = dfdeskmarkethistfull.loc[:,['Japan','South Korea','China','India']]
        df_demand_sum_pre['NW Eur'] = dfdeskmarkethistfull.loc[:,'NWEur Desk']
        df_demand_sum_pre['Med Eur'] = dfdeskmarkethistfull.loc[:,'MedEur Desk']
        df_demand_sum_pre['WoS Desk'] = dfdeskmarkethistfull.loc[:,'WoS Desk']
        df_demand_sum_pre['EoS Desk'] = dfdeskmarkethistfull.loc[:,'EoS Desk']
        df_demand_sum_pre['WoS Other'] = dfdeskmarkethistfull.loc[:,'WoS Desk'] - dfdeskmarkethistfull.loc[:,'NWEur Desk'] - dfdeskmarkethistfull.loc[:,'MedEur Desk']
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
        
    def sum_table(dfcurvefull, desk_country_latest, desk_supply_hist_country, desk_demand_hist, desk_demandbasin_latest, start_date, end_date):
        
        #print(dfdeskmarkethistfull)
        today = datetime.datetime.today()
        update_date = DBTOPD.get_model_run_date()
        m12 = update_date.loc[update_date[update_date['ForecastDate'] > (datetime.datetime.today() - relativedelta(months = 12))].index[-1],'ForecastDate']

        lastdemand = model_delta.hist_data_demand(str(update_date.loc[1,'ForecastDate']), desk_demand_hist, start_date, end_date)
        y1demandeur = model_delta_chart.chart_market_data('NWEur', desk_demand_hist, start_date, end_date)
        y1demandjktc = model_delta_chart.chart_market_data('JKTC', desk_demand_hist, start_date, end_date)
        lastsupply = model_delta.hist_data_supply_country(str(update_date.loc[1,'ForecastDate']), desk_supply_hist_country, start_date, end_date)
        #print(lastsupply)
        dfsumprice = pd.DataFrame(index=pd.date_range(start=start_date,end=end_date, freq='MS'), columns = ['JKM','JKM-TTF'])
        dfsumprice.loc[:,'JKM'] = dfcurvefull.loc[start_date:end_date,'JKM FWD']
        dfsumprice.loc[:,'JKM-TTF'] = dfcurvefull.loc[start_date:end_date,'JKM FWD'] - dfcurvefull.loc[start_date:end_date,'TTF USD FWD']
        dfsumprice = dfsumprice.loc[str(today.year)+'-01-01':]
        dfsumprice.index = dfsumprice.index.strftime('%b-%Y')
        dfsumprice = dfsumprice.T
        #print(dfsumprice)
                     
        dfsumeur = pd.DataFrame(index=pd.date_range(start=start_date,end=end_date), columns = ['NW Eur','Base','Last View','Delta','Y-1'])
        #print(dfsumeur)
        dfsumeur.loc[:,'NW Eur'] = np.nan
        dfsumeur.loc[:,'Base'] = desk_demandbasin_latest.loc[:,'NWEur Desk'].values
        dfsumeur.loc[:,'Last View'] = lastdemand.loc[:,'NWEur Desk']
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
        dfsumjktc = dfsumjktc.resample('M').mean().round(1)
        dfsumjktc = dfsumjktc.loc[str(today.year)+'-01-31':]
        dfsumjktc.index = dfsumjktc.index.strftime('%b-%Y')
        dfsumjktc.loc[:,'Y-1'] = y1demandjktc.loc[:,str(m12.date())]
        dfsumjktc = dfsumjktc.T
        #print(desk_country_latest.loc[start_date:])
        #print(start_date)
        
        dfsumus = pd.DataFrame(index=pd.date_range(start=start_date,end=end_date), columns = ['United States','Base','US Utilisation*','Last View','Delta','US Feed.'])
        dfsumus.loc[:,'United States'] = np.nan
        dfsumus.loc[:,'Base'] = desk_country_latest.loc[:,'United States']
        dfsumus.loc[:,'US Utilisation*'] = np.nan
        dfsumus.loc[:,'Last View'] = lastsupply.loc[:,'United States']
        dfsumus.loc[:,'Delta'] = dfsumus.loc[:,'Base'] -  dfsumus.loc[:,'Last View'] 
        dfsumus.loc[:,'US Feed.'] = np.nan
        dfsumus = dfsumus.resample('M').mean().round(1)
        dfsumus = dfsumus.loc[str(today.year)+'-01-31':]
        dfsumus.index = dfsumus.index.strftime('%b-%Y')
        dfsumus = dfsumus.T
        #print(dfsumus)
        
        dfsum = pd.concat([dfsumprice, dfsumeur, dfsumjktc, dfsumus])
        dfsum = dfsum.round(1)
        #print(dfsum)
        return dfsum

'''
desk_plant_latest, desk_country_latest, desk_supplybasin_latest, desk_supply_hist_plant, desk_supply_hist_country, desk_market_latest, desk_demandbasin_latest, desk_demand_hist, dfcurvefull = model_delta.get_data()
start_date = desk_demandbasin_latest['Date'].iloc[0]
end_date = desk_demandbasin_latest['Date'].iloc[-1]
rundate = '2022-08-31 00:00:00'
dfdeskplanthistfull = model_delta.hist_data_supply_plant(rundate, desk_supply_hist_plant, start_date, end_date)
#print(desk_plant_latest)
dfdeskcountryhistfull = model_delta.hist_data_supply_country(rundate, desk_supply_hist_country, start_date, end_date)
#print(dfdeskcountryhistfull)
dfdeskmarkethistfull = model_delta.hist_data_demand(rundate, desk_demand_hist, start_date, end_date)
#print(dfdeskmarkethistfull)

model_delta.sum_table(dfcurvefull, desk_country_latest, desk_supply_hist_country, desk_demand_hist, desk_demandbasin_latest, start_date, end_date)



df_supply_sum, df_demand_sum, df_supply_sum_pre, df_demand_sum_pre, df_supply_delta, df_demand_delta, df_plant_latest, df_market_latest, df_plant_delta, df_market_delta = model_delta.table_data(desk_plant_latest, desk_country_latest, desk_supplybasin_latest, dfdeskplanthistfull, dfdeskcountryhistfull, desk_market_latest, desk_demandbasin_latest, dfdeskmarkethistfull)

'''
