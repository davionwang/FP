# -*- coding: utf-8 -*-
"""
Created on Mon Jun 28 20:45:51 2021

@author: ywang1
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
import calendar
import numpy as np
import pandas as pd
from plotly.subplots import make_subplots
import matplotlib.pyplot as plt


sys.path.append('C:\\Users\SVC-GASQuant2-Prod\YWwebsite\Python Scripts\class') 
from DBtoDF import DBTOPD

pd.set_option('display.max_columns',10)

class SD_Data_Country():
    
    def Kpler_data(SupplyCurveId, DemandCurveId):
        
        #read data from Kpler
        Kpler=DBTOPD('PRD-DB-SQL-211','LNG', 'dbo', 'KplerLNGTrades')
        dfkpler=Kpler.sql_to_df()
        
        #get supply and demand df
        supply_columns=['EndOrigin','CountryOrigin','VolumeOriginM3']
        demand_columns=['StartDestination','CountryDestination','VolumeDestinationM3']
        df_supply = dfkpler[supply_columns]
        #df_supply = df_supply.sort_values(by = 'EndOrigin')
        df_demand = dfkpler[demand_columns]
        
        #remove repeat country and get country list
        SupplyCurveId.drop_duplicates(inplace=True)
        supply_country_list=SupplyCurveId['Country']
        supply_country_list.drop_duplicates(inplace=True)
        supply_country_list=supply_country_list.values.tolist()
        #print(supply_country_list)
        DemandCurveId.drop_duplicates(inplace=True)
        demand_country_list = DemandCurveId['Country']
        demand_country_list.drop_duplicates(inplace=True)
        demand_country_list=demand_country_list.values.tolist()
        #print(df_supply)
        
        KplerETA=DBTOPD('PRD-DB-SQL-208','Scrape', 'dbo', 'KplerLNGTradesForecasViewByDay')
        df_supply_eta=KplerETA.get_eta_supply()
        df_demand_eta=KplerETA.get_eta_demand()
    
        return df_supply, df_demand, df_supply_eta, df_demand_eta, supply_country_list, demand_country_list
    
    
    
    def create_df_supply_MA (df_supply, country_list, rolling_period, start_date, end_date):
        
        prestart_date= datetime.datetime.strptime(start_date, '%Y-%m-%d') - rolling_period*datetime.timedelta(days=1)
        dfMA=pd.DataFrame(index=pd.date_range(start=start_date,end=end_date))
        dfMA.index.name='Date'
        #print(df_supply)
        for i in country_list:
            
            dffulldate=pd.DataFrame(index=pd.date_range(start=prestart_date,end=end_date)) 
            dffulldate.index.name='Date'
            dfcountry = df_supply[df_supply['CountryOrigin']==i].copy()
            dfcountry['EndOrigin']=pd.to_datetime(dfcountry['EndOrigin'].copy()).dt.strftime('%Y-%m-%d') 
            
            dfcountry=dfcountry.groupby(['EndOrigin']).sum()*0.000612
            dfcountry.index.name='Date'
            dfcountry.index=pd.DatetimeIndex(dfcountry.index)
            
            merged = pd.merge(dffulldate, dfcountry.loc[prestart_date: str(end_date)], on='Date', how='outer')
            merged.fillna(0, inplace=True)
            
            npMA=merged.loc[prestart_date: str(end_date)].rolling(rolling_period).mean()
            dfMA[i]=npMA.loc[start_date:end_date].values
        
        return dfMA
    
    def create_df_demand_MA (df_demand, country_list, rolling_period, start_date, end_date):
        
        prestart_date= datetime.datetime.strptime(start_date, '%Y-%m-%d') - rolling_period*datetime.timedelta(days=1)
        dfMA=pd.DataFrame(index=pd.date_range(start=start_date,end=end_date))
        dfMA.index.name='Date'
        for i in country_list:
            
            dffulldate=pd.DataFrame(index=pd.date_range(start=prestart_date,end=end_date)) 
            dffulldate.index.name='Date'
            dfcountry = df_demand[df_demand['CountryDestination']==i].copy()
            dfcountry['StartDestination']=pd.to_datetime(dfcountry['StartDestination'].copy()).dt.strftime('%Y-%m-%d') 
            
            dfcountry=dfcountry.groupby(['StartDestination']).sum()*0.000612
            dfcountry.index.name='Date'
            dfcountry.index=pd.DatetimeIndex(dfcountry.index)
            
            merged = pd.merge(dffulldate, dfcountry.loc[prestart_date: str(end_date)], on='Date', how='outer')
            merged.fillna(0, inplace=True)
            
            npMA=merged.loc[prestart_date: str(end_date)].rolling(rolling_period).mean()
            dfMA[i]=npMA.loc[start_date:end_date].values
        
        return dfMA
    
    def create_df_etasupply_MA (df_supply_eta, country_list, rolling_period, start_date, end_date):
        
        #prestart_date= datetime.datetime.strptime(start_date, '%Y-%m-%d') - rolling_period*datetime.timedelta(days=1)
        dfMA=pd.DataFrame(index=pd.date_range(start=start_date,end=end_date))
        dfMA.index.name='Date'
        #print(df_supply_eta)
        for i in country_list:
            
            dffulldate=pd.DataFrame(index=pd.date_range(start=df_supply_eta['Date'].iloc[0].strftime('%Y-%m-%d'),end=end_date)) 
            dffulldate.index.name='Date'
            dfcountry = df_supply_eta[df_supply_eta['CountryOrigin']==i].copy()
            
            dfcountry['Date']=pd.to_datetime(dfcountry['Date']).dt.strftime('%Y-%m-%d').copy()
            
            dfcountry=dfcountry.groupby(['Date']).sum()
            dfcountry['VolumeOriginM3']=dfcountry['VolumeOriginM3']*0.000612
            dfcountry.index.name='Date'
            dfcountry.index=pd.DatetimeIndex(dfcountry.index)
            #print(dfcountry)
            merged = pd.merge(dffulldate, dfcountry['VolumeOriginM3'], on='Date', how='outer')
            merged.fillna(0, inplace=True)
            #print(merged)
            
            #npMA=merged.loc[prestart_date: str(end_date)].rolling(rolling_period).mean()
            npMA=merged.rolling(10).mean()
            #print(npMA)
            #npMA.to_csv('H:/Yuefeng/LNG Flows/Deskdatatestcom.csv')
            dfMA[i]=npMA.loc[start_date:end_date].values
            #print(dfMA[i])
        return dfMA
    
    def create_df_etademand_MA (df_demand_eta, country_list, rolling_period, start_date, end_date):
        
        #prestart_date= datetime.datetime.strptime(start_date, '%Y-%m-%d') - rolling_period*datetime.timedelta(days=1)
        dfMA=pd.DataFrame(index=pd.date_range(start=start_date,end=end_date))
        dfMA.index.name='Date'
        for i in country_list:
            #print(end_date)
            dffulldate=pd.DataFrame(index=pd.date_range(start=df_demand_eta['Date'].iloc[0].strftime('%Y-%m-%d'),end=end_date)) 
            dffulldate.index.name='Date'
            dfcountry = df_demand_eta[df_demand_eta['ForecastedDestination']==i].copy()
            dfcountry['Date']=pd.to_datetime(dfcountry['Date']).dt.strftime('%Y-%m-%d').copy() 
            
            dfcountry=dfcountry.groupby(['Date']).sum()*0.000612
            dfcountry.index.name='Date'
            dfcountry.index=pd.DatetimeIndex(dfcountry.index)
            #print(dfcountry)
            merged = pd.merge(dffulldate, dfcountry, on='Date', how='outer')
            merged.fillna(0, inplace=True)
            #print(dffulldate.tail())
            #print(merged)
            
            #npMA=merged.loc[prestart_date: str(end_date)].rolling(rolling_period).mean()
            npMA=merged.rolling(10).mean()
            #print(npMA.loc[start_date:end_date])
            #print(dfMA)
            #npMA.to_csv('H:/Yuefeng/LNG Flows/Deskdatatestcom.csv')
            dfMA[i]=npMA.loc[start_date:end_date].values
        #dfMA.to_csv('H:/Yuefeng/LNG Flows/Deskdatatest.csv')
        return dfMA
    
    
    
    def supply_desk_view(supplyCurveId, start_date, end_date,mttomcmd):
        
        #read curveID and to dict, use for change id to country name
        dfsupplyCurveId=supplyCurveId.iloc[:,0:2]
        SupplyCurveId=dfsupplyCurveId.set_index('CurveID').T.to_dict('list')
        
        #read desk view data
        desksupply=DBTOPD('PRD-DB-SQL-209','AnalyticsModel', 'ts', 'AnalyticsLatest')
        dfdesksupply=desksupply.desksupply_to_df()
        #print(SupplyCurveId)
        #replace curve id to country name
        dfdesksupply['CurveId'].replace(SupplyCurveId, inplace=True)
        #change data format
        dfsupplynew=dfdesksupply.groupby(['ValueDate','CurveId'], as_index=False).sum()
        dfsupplynew['ValueDate'] = pd.to_datetime(dfsupplynew['ValueDate'])
        dfsupplynew=pd.pivot(dfsupplynew, index='ValueDate', columns='CurveId')
        dfsupplynew.index.name='Date'
        dfsupplynew.columns=dfsupplynew.columns.droplevel(0)
        dfsupplynew.rename_axis(None, axis=1, inplace=True)
        for i in dfsupplynew.index:
            days=calendar.monthrange(i.year,i.month)
            dfsupplynew.loc[i]=dfsupplynew.loc[i]*mttomcmd/days[1]
            
        #change to full date data
        dfalldesksupply=pd.DataFrame(index=pd.date_range(start=start_date,end=end_date))
        dfalldesksupply.index.name='Date'
        
        merged = pd.merge(dfalldesksupply, dfsupplynew, on='Date', how='outer')
        merged.fillna(method='ffill', inplace=True) 
        merged=merged.loc[start_date:end_date]
        #merged.to_csv('H:/Yuefeng/LNG Flows/Deskdatatest.csv')
    
        return merged
    
    def demand_desk_view(demandCurveId, start_date, end_date, mttomcmd):
        
        #read curveID and to dict, use for change id to country name
        dfdemandCurveId=demandCurveId
        DemandCurveId=dfdemandCurveId.set_index('CurveID').T.to_dict('list')
        
        #read desk view data
        deskdemand=DBTOPD('PRD-DB-SQL-209','AnalyticsModel', 'ts', 'AnalyticsLatest')
        dfdeskdemand=deskdemand.deskdemand_to_df()
        #replace curve id to country name
        dfdeskdemand['CurveId'].replace(DemandCurveId, inplace=True)
        #change data format
        dfdemandnew=dfdeskdemand.groupby(['ValueDate','CurveId'], as_index=False).sum()
        dfdemandnew['ValueDate'] = pd.to_datetime(dfdemandnew['ValueDate'])
        dfdemandnew=pd.pivot(dfdemandnew, index='ValueDate', columns='CurveId')
        dfdemandnew.index.name='Date'
        dfdemandnew.columns=dfdemandnew.columns.droplevel(0)
        dfdemandnew.rename_axis(None, axis=1, inplace=True)
        #print(dfdemandnew)
        no_china_cols = [x for x in dfdemandnew.columns if x != 'China']
        for i in dfdemandnew.index:
            days=calendar.monthrange(i.year,i.month)
            #China conversion 1376
            dfdemandnew.loc[i,'China']=dfdemandnew.loc[i, 'China']*1376/days[1]
            #all others 1397
            dfdemandnew.loc[i,no_china_cols]=dfdemandnew.loc[i,no_china_cols]*mttomcmd/days[1]
            
        #change to full date data
        dfalldeskdemand=pd.DataFrame(index=pd.date_range(start=start_date,end=end_date))
        dfalldeskdemand.index.name='Date'
        
        merged = pd.merge(dfalldeskdemand, dfdemandnew, on='Date', how='outer')
        merged.fillna(method='ffill', inplace=True) 
        merged=merged.loc[start_date:end_date]
        #print(merged['China'].loc['2021-08'])
        return merged
    
    def IHScontract_data(start_date, end_date):
        
        dfIHS = pd.read_excel('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/IHS Contract.xlsx',header=(0), sheet_name='demand')
        fulldate=pd.DataFrame(index=pd.date_range(start=start_date,end=end_date))
        fulldate.index.name='Date'
        
        merged = pd.merge(fulldate, dfIHS, on='Date', how='outer')
        merged.fillna(method='ffill', inplace=True)
        merged.set_index('Date', inplace=True)
     
        return merged
        
    def full_data(rolling_period):
        
        #SD_Data_Country1=SD_Data_Country()
        #MA period
        #rolling_period=10
        #date
        today=datetime.date.today()
        delta = datetime.timedelta(days=1)
        start_date='2016-01-01' 
        current_year_start = '2021-01-01'
        current_year_end = '2021-12-31' 
        #Mt to Mcm/d
        Mt_to_Mcmd = 1397
        #read curveID
        SupplyCurveId = pd.read_excel('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/DeskViewID.xlsx',header=(0),sheet_name='supply')
        DemandCurveId = pd.read_excel('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/DeskViewID.xlsx',header=(0),sheet_name='demand')
        #read Kplar data
        df_supply, df_demand, df_supply_eta, df_demand_eta, supply_country_list, demand_country_list=SD_Data_Country.Kpler_data(SupplyCurveId, DemandCurveId) 
        #create Kplar data for each country 
        dfsupplyMA=SD_Data_Country.create_df_supply_MA(df_supply, supply_country_list,rolling_period, start_date, today) 
        dfdemandMA=SD_Data_Country.create_df_demand_MA(df_demand, demand_country_list,rolling_period, start_date, today)
        #create desk view data for all countries
        desk_supply_view = SD_Data_Country.supply_desk_view(SupplyCurveId, current_year_start,current_year_end,Mt_to_Mcmd) 
        desk_demand_view = SD_Data_Country.demand_desk_view(DemandCurveId, current_year_start,current_year_end,Mt_to_Mcmd)
        #create ihs full table
        ihscontract = SD_Data_Country.IHScontract_data(current_year_start, current_year_end) 
        #eta data
        #supply_country_list=['United States']
        dfsupplyMA_eta=SD_Data_Country.create_df_etasupply_MA(df_supply_eta, supply_country_list,rolling_period, str(today+delta), current_year_end) 
        dfdemandMA_eta=SD_Data_Country.create_df_etademand_MA(df_demand_eta, demand_country_list,rolling_period, str(today+delta), current_year_end)
        #dfdemandMA_eta.to_csv('H:/Yuefeng/LNG Flows/Deskdatatestcom.csv')
        return dfsupplyMA, dfdemandMA, dfsupplyMA_eta, dfdemandMA_eta, desk_supply_view, desk_demand_view, ihscontract
    
    #if __name__ == '__main__':
        #supply_country_list, demand_country_list, dfsupplyMA, dfdemandMA, dfsupplyMA_eta, dfdemandMA_eta, desk_supply_view, desk_demand_view, ihscontract=full_data()
    
df=SD_Data_Country.full_data(10)

#print(df)


#///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

class SD_Data_Basin():
    
    def Kpler_data():
        
        #read data from Kpler
        Kpler=DBTOPD('PRD-DB-SQL-211','LNG', 'dbo', 'KplerLNGTrades')
        dfkpler=Kpler.sql_to_df()
        
        #get supply and demand df
        supply_columns=['EndOrigin','InstallationOrigin','VolumeOriginM3']
        demand_columns=['StartDestination','CountryDestination','VolumeDestinationM3']
        df_supply = dfkpler[supply_columns]
        df_demand = dfkpler[demand_columns]
        
        SupplyCategories = pd.read_excel('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/Categories.xlsx',header=(0),sheet_name='Index_Supply')
        SupplyCategories = SupplyCategories.iloc[:44,0:5]
        DemandCategories = pd.read_excel('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/Categories.xlsx',header=(0),sheet_name='Index_Demand')
        DemandCategories = DemandCategories.iloc[:,0:6]
        
        #print(SupplyCategories)
        return df_supply, df_demand, SupplyCategories, DemandCategories
    #Kpler_data()
    def create_df_supply_MA (df_supply, SupplyCurveId, rolling_period, start_date, end_date):
        
        #create supply list
        supply_plant_list=SupplyCurveId['plant'].values.tolist()
        #rolling start date
        prestart_date= datetime.datetime.strptime(start_date, '%Y-%m-%d') - rolling_period*datetime.timedelta(days=1)
        dfMA=pd.DataFrame(index=pd.date_range(start=start_date,end=end_date))
        dfMA.index.name='Date'
        for i in supply_plant_list:
            
            dffulldate=pd.DataFrame(index=pd.date_range(start=prestart_date,end=end_date)) 
            dffulldate.index.name='Date'
            dfplant = df_supply[df_supply['InstallationOrigin']==i]
            dfplant['EndOrigin']=pd.to_datetime(dfplant['EndOrigin']).dt.strftime('%Y-%m-%d') 
            
            dfplant=dfplant.groupby(['EndOrigin']).sum()*0.000612
            dfplant.index.name='Date'
            dfplant.index=pd.DatetimeIndex(dfplant.index)
            
            merged = pd.merge(dffulldate, dfplant.loc[prestart_date: str(end_date)], on='Date', how='outer')
            merged.fillna(0, inplace=True)
            
            npMA=merged.loc[prestart_date: str(end_date)].rolling(rolling_period).mean()
            dfMA[i]=npMA.loc[start_date:end_date].values
        
        return dfMA
    
    def create_df_demand_MA (df_demand, DemandCurveId, rolling_period, start_date, end_date):
        
        demand_country_list = DemandCurveId['Country'].values.tolist()
        prestart_date= datetime.datetime.strptime(start_date, '%Y-%m-%d') - rolling_period*datetime.timedelta(days=1)
        dfMA=pd.DataFrame(index=pd.date_range(start=start_date,end=end_date))
        dfMA.index.name='Date'
        for i in demand_country_list:
            
            dffulldate=pd.DataFrame(index=pd.date_range(start=prestart_date,end=end_date)) 
            dffulldate.index.name='Date'
            dfcountry = df_demand[df_demand['CountryDestination']==i]
            dfcountry['StartDestination']=pd.to_datetime(dfcountry['StartDestination']).dt.strftime('%Y-%m-%d') 
            
            dfcountry=dfcountry.groupby(['StartDestination']).sum()*0.000612
            dfcountry.index.name='Date'
            dfcountry.index=pd.DatetimeIndex(dfcountry.index)
            
            merged = pd.merge(dffulldate, dfcountry.loc[prestart_date: str(end_date)], on='Date', how='outer')
            merged.fillna(0, inplace=True)
            
            npMA=merged.loc[prestart_date: str(end_date)].rolling(rolling_period).mean()
            dfMA[i]=npMA.loc[start_date:end_date].values
        
        return dfMA
    
    
    def supply_desk_view(SupplyCurveId,start_date, end_date,mttomcmd):
    
        #read curveID and to dict, use for change id to country name
        SupplyCurveId=SupplyCurveId.set_index('CurveID').T.to_dict('list')
        #print(SupplyCurveId)
        #read desk view data
        desksupply=DBTOPD('PRD-DB-SQL-209','AnalyticsModel', 'ts', 'AnalyticsLatest')
        dfdesksupply=desksupply.desksupply_to_df()
        #print(dfdesksupply)
        #replace curve id to country name
        dfdesksupply['CurveId'].replace(SupplyCurveId, inplace=True)
        #print(dfdesksupply)
        #change data format
        dfsupplynew=dfdesksupply.groupby(['ValueDate','CurveId'], as_index=False).sum()
        dfsupplynew['ValueDate'] = pd.to_datetime(dfsupplynew['ValueDate'])
        dfsupplynew=pd.pivot(dfsupplynew, index='ValueDate', columns='CurveId')
        dfsupplynew.index.name='Date'
        dfsupplynew.columns=dfsupplynew.columns.droplevel(0)
        dfsupplynew.rename_axis(None, axis=1, inplace=True)
        for i in dfsupplynew.index:
            days=calendar.monthrange(i.year,i.month)
            dfsupplynew.loc[i]=dfsupplynew.loc[i]*mttomcmd/days[1]
        #change to full date data
        dfalldesksupply=pd.DataFrame(index=pd.date_range(start=start_date,end=end_date))
        dfalldesksupply.index.name='Date'
        #print(dfsupplynew.loc[start_date:end_date])
        merged = pd.merge(dfalldesksupply, dfsupplynew, on='Date', how='outer')
        merged.fillna(method='ffill', inplace=True) 
        merged=merged.loc[start_date:end_date]
        #print('read desk data', merged)
        #dfsupplynewDAY=dfsupplynew.resample('d')
        #print(dfsupplynewDAY)
        #merged.to_csv('H:/Yuefeng/LNG Flows/Deskdatatestcom.csv')
        
        return merged
    #supply_desk_view('2016-01-01','2021-06-01',1397)
    
    def demand_desk_view(DemandCurveId, start_date, end_date, mttomcmd):
        
    
        #read curveID and to dict, use for change id to country name
        
        DemandCurveId=DemandCurveId.set_index('CurveID').T.to_dict('list')
        #print(DemandCurveId)
        #read desk view data
        deskdemand=DBTOPD('PRD-DB-SQL-209','AnalyticsModel', 'ts', 'AnalyticsLatest')
        dfdeskdemand=deskdemand.deskdemand_to_df()
        #print(dfdeskdemand)
        #replace curve id to country name
        dfdeskdemand['CurveId'].replace(DemandCurveId, inplace=True)
        #print(dfdeskdemand)
        #change data format
        dfdemandnew=dfdeskdemand.groupby(['ValueDate','CurveId'], as_index=False).sum()
        dfdemandnew['ValueDate'] = pd.to_datetime(dfdemandnew['ValueDate'])
        dfdemandnew=pd.pivot(dfdemandnew, index='ValueDate', columns='CurveId')
        dfdemandnew.index.name='Date'
        dfdemandnew.columns=dfdemandnew.columns.droplevel(0)
        dfdemandnew.rename_axis(None, axis=1, inplace=True)
        for i in dfdemandnew.index:
            days=calendar.monthrange(i.year,i.month)
            #dfdemandnew.loc[i]=dfdemandnew.loc[i]*mttomcmd/days[1]
            if i == 'China':
                dfdemandnew.loc[i]=dfdemandnew.loc[i]*1376/days[1]
            else:
                dfdemandnew.loc[i]=dfdemandnew.loc[i]*mttomcmd/days[1]
        #change to full date data
        dfalldeskdemand=pd.DataFrame(index=pd.date_range(start=start_date,end=end_date))
        dfalldeskdemand.index.name='Date'
        
        merged = pd.merge(dfalldeskdemand, dfdemandnew, on='Date', how='outer')
        merged.fillna(method='ffill', inplace=True) 
        merged=merged.loc[start_date:end_date]
        #print(merged)
        return merged
    #demand_desk_view('2016-01-01','2021-06-01',1397)
    
    
    def full_data(rolling_period):
        #MA period
        #rolling_period=10
        #date
        today=datetime.date.today()
        delta = datetime.timedelta(days=1)
        start_date='2016-01-01' 
        current_year_start = '2021-01-01'
        current_year_end = '2021-12-31' 
        #Mt to Mcm/d
        Mt_to_Mcmd = 1397
        #read curveID
        SupplyCurveId = pd.read_excel('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/DeskViewID.xlsx',header=(0),sheet_name='supply')
        #SupplyCurveId = pd.read_excel('H:/Yuefeng/LNG Flows/DeskViewID.xlsx',header=(0),sheet_name='supply')
        SupplyCurveId=SupplyCurveId[['CurveID','plant']]
        DemandCurveId = pd.read_excel('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/DeskViewID.xlsx',header=(0),sheet_name='demandwithRU')
        DemandCurveId=DemandCurveId[['CurveID','Country']]
        #read Kplar data
        df_supply, df_demand, SupplyCategories, DemandCategories=SD_Data_Basin.Kpler_data() 
        
        #create Kplar data for each country 
        dfsupplyMA=SD_Data_Basin.create_df_supply_MA(df_supply, SupplyCurveId, rolling_period, start_date, today) 
        dfdemandMA=SD_Data_Basin.create_df_demand_MA(df_demand, DemandCurveId, rolling_period, start_date, today)
    
        #dfsupplyMA.to_csv('H:/Yuefeng/LNG Flows/Deskdatatestcom.csv')
        #create desk view data for all countries
        desk_supply_view = SD_Data_Basin.supply_desk_view(SupplyCurveId, current_year_start, current_year_end, Mt_to_Mcmd) 
        desk_demand_view = SD_Data_Basin.demand_desk_view(DemandCurveId, current_year_start, current_year_end, Mt_to_Mcmd)
        #create ihs full table
        #ihscontract = IHScontract_data(current_year_start, current_year_end) 
        #print(SupplyCategories)
        
        return SupplyCategories, DemandCategories, dfsupplyMA, dfdemandMA, desk_supply_view, desk_demand_view
    
    
    #create dict of date
    date_dict={
               'last_year_start' : '2020-01-01',
               'last_year_end' : '2020-12-31',
               'start_date' : '2020-01-01',
               'end_date' : '2021-12-31',
               'current_year_start' : '2021-01-01',
               'current_year_end' : '2021-12-31',
               'last_day' : datetime.date.today() - datetime.timedelta(days=1)*1,
               'today' : datetime.date.today(),
               'last_month': str(datetime.date.today().year)+'-'+str(datetime.date.today().month-1)
               }
    #supply//////////////////////////////////////////////////////////////////////////////////////////////
    def Basin_data_supply(dfsupplyMA, desk_supply_view, SupplyCategories, date_dict):
        
        
        SupplyCategories.set_index('Basin', inplace=True)
        
        #Basin plants list, and remove 
        PB = SupplyCategories['Plant'].loc['PB'].values.tolist()
        PB.remove('Mozambique Area 1')
        AB = SupplyCategories['Plant'].loc['AB'].values.tolist()
        AB.remove('Calcasieu Pass')
        MENA = SupplyCategories['Plant'].loc['MENA_Bas'].values.tolist()
        
        SupplyCategories.set_index('Suez', inplace=True)
        EoS = SupplyCategories['Plant'].loc['EoS'].values.tolist()
        EoS.remove('Mozambique Area 1')
        WoS = SupplyCategories['Plant'].loc['WoS'].values.tolist()
        WoS.remove('Calcasieu Pass')
        #print(EoS)
        #basin column name
        PB2021 = list(map(lambda x:x+' 2021',PB))
        PB2020 = list(map(lambda x:x+' 2020',PB))
        PBdesk = list(map(lambda x:x+' Desk 2021',PB))
        AB2021 = list(map(lambda x:x+' 2021',AB))
        AB2020 = list(map(lambda x:x+' 2020',AB))
        ABdesk = list(map(lambda x:x+' Desk 2021',AB))
        MENA2021 = list(map(lambda x:x+' 2021',MENA))
        MENA2020 = list(map(lambda x:x+' 2020',MENA))
        MENAdesk = list(map(lambda x:x+' Desk 2021',MENA))
        
        allplant=SupplyCategories['Plant'].values.tolist()
        allplant.remove('Mozambique Area 1')
        allplant.remove('Calcasieu Pass')
        
        #full time df
        df_fulltime = pd.DataFrame(index=pd.date_range(start=date_dict['current_year_start'], end=date_dict['current_year_end']))
        df_fulltime[['MENA 2021','PB 2021','AB 2021']] = pd.concat([df_fulltime,
                                 dfsupplyMA[MENA].loc[date_dict['current_year_start']:date_dict['current_year_end']].sum(axis=1),
                                 dfsupplyMA[PB].loc[date_dict['current_year_start']:date_dict['current_year_end']].sum(axis=1),
                                 dfsupplyMA[AB].loc[date_dict['current_year_start']:date_dict['current_year_end']].sum(axis=1)], axis=1)
        df_fulltime[MENA2021] = dfsupplyMA[MENA].loc[date_dict['current_year_start']:date_dict['current_year_end']]
        df_fulltime[MENA2020] = np.delete(dfsupplyMA[MENA].loc['2020-01-01':'2020-12-31'].values, 59,axis=0)
        df_fulltime[MENAdesk] = desk_supply_view[MENA].loc[date_dict['current_year_start']:date_dict['current_year_end']].values 
        
        df_fulltime[PB2021] = dfsupplyMA[PB].loc[date_dict['current_year_start']:date_dict['current_year_end']]
        df_fulltime[PB2020] = np.delete(dfsupplyMA[PB].loc['2020-01-01':'2020-12-31'].values, 59,axis=0)
        df_fulltime[PBdesk] = desk_supply_view[PB].loc[date_dict['current_year_start']:date_dict['current_year_end']].values 
        
        df_fulltime[AB2021] = dfsupplyMA[AB].loc[date_dict['current_year_start']:date_dict['current_year_end']]
        df_fulltime[AB2020] = np.delete(dfsupplyMA[AB].loc['2020-01-01':'2020-12-31'].values, 59,axis=0)
        df_fulltime[AB2021] = desk_supply_view[AB].loc[date_dict['current_year_start']:date_dict['current_year_end']].values 
        
        #yoy basin
        df_basin = pd.DataFrame(index=pd.date_range(start=date_dict['current_year_start'], end=date_dict['current_year_end']))
        df_basin.index.name='Date'
        df_basin['MENA 2021'] = df_fulltime['MENA 2021'].values
        df_basin['MENA 2020'] = np.delete(dfsupplyMA[MENA].loc['2020-01-01':'2020-12-31'].sum(axis=1).values, 59)
        df_basin['MENA Desk 2021'] = desk_supply_view[MENA].loc[date_dict['current_year_start']:date_dict['current_year_end']].sum(axis=1).values   
    
        df_basin['PB 2021'] = df_fulltime['PB 2021'].values
        df_basin['PB 2020'] = np.delete(dfsupplyMA[PB].loc['2020-01-01':'2020-12-31'].sum(axis=1).values, 59)
        df_basin['PB Desk 2021'] = desk_supply_view[PB].loc[date_dict['current_year_start']:date_dict['current_year_end']].sum(axis=1).values   
        
        df_basin['AB 2021'] = df_fulltime['AB 2021'].values
        df_basin['AB 2020'] = np.delete(dfsupplyMA[AB].loc['2020-01-01':'2020-12-31'].sum(axis=1).values, 59)
        df_basin['AB Desk 2021'] = desk_supply_view[AB].loc[date_dict['current_year_start']:date_dict['current_year_end']].sum(axis=1).values   
        
        df_basin[MENA2021] = dfsupplyMA[MENA].loc[date_dict['current_year_start']:date_dict['current_year_end']]
        df_basin[MENA2020] = np.delete(dfsupplyMA[MENA].loc['2020-01-01':'2020-12-31'].values, 59,axis=0)
        df_basin[MENAdesk] = desk_supply_view[MENA].loc[date_dict['current_year_start']:date_dict['current_year_end']].values 
        
        df_basin[PB2021] = dfsupplyMA[PB].loc[date_dict['current_year_start']:date_dict['current_year_end']]
        df_basin[PB2020] = np.delete(dfsupplyMA[PB].loc['2020-01-01':'2020-12-31'].values, 59,axis=0)
        df_basin[PBdesk] = desk_supply_view[PB].loc[date_dict['current_year_start']:date_dict['current_year_end']].values 
        
        df_basin[AB2021] = dfsupplyMA[AB].loc[date_dict['current_year_start']:date_dict['current_year_end']]
        df_basin[AB2020] = np.delete(dfsupplyMA[AB].loc['2020-01-01':'2020-12-31'].values, 59,axis=0)
        df_basin[ABdesk] = desk_supply_view[AB].loc[date_dict['current_year_start']:date_dict['current_year_end']].values 
        
        df_basin['Global 2021'] = df_basin[['AB 2021', 'PB 2021','MENA 2021']].loc[date_dict['current_year_start']:date_dict['today']].sum(axis=1)
        df_basin['Global 2020'] = df_basin[['AB 2020', 'PB 2020','MENA 2020']].sum(axis=1)
        df_basin['Global Desk 2021'] = df_basin[['AB Desk 2021', 'PB Desk 2021','MENA Desk 2021']].sum(axis=1)
        
        
        df_basin['EoS 2021'] = dfsupplyMA[EoS].loc[date_dict['current_year_start']:date_dict['current_year_end']].sum(axis=1)
        df_basin['EoS 2020'] = np.delete(dfsupplyMA[EoS].loc['2020-01-01':'2020-12-31'].sum(axis=1).values, 59)
        df_basin['EoS Desk 2021'] = desk_supply_view[EoS].loc[date_dict['current_year_start']:date_dict['current_year_end']].sum(axis=1).values
        df_basin['WoS 2021'] = dfsupplyMA[WoS].loc[date_dict['current_year_start']:date_dict['current_year_end']].sum(axis=1)
        df_basin['WoS 2020'] = np.delete(dfsupplyMA[WoS].loc['2020-01-01':'2020-12-31'].sum(axis=1).values, 59)
        df_basin['WoS Desk 2021'] = desk_supply_view[WoS].loc[date_dict['current_year_start']:date_dict['current_year_end']].sum(axis=1).values
        df_basin['EoS Difference']=(df_basin['EoS 2021'] - df_basin['EoS Desk 2021']).resample('M').sum()
        df_basin['EoS Difference'].fillna(method='bfill', inplace=True)
        df_basin['WoS Difference']=(df_basin['WoS 2021'] - df_basin['WoS Desk 2021']).resample('M').sum()
        df_basin['WoS Difference'].fillna(method='bfill', inplace=True)
        for day in pd.date_range(start=date_dict['current_year_start'], end=date_dict['today']):
                days=calendar.monthrange(day.year,day.month)
                df_basin.loc[day,'EoS Difference']=df_basin.loc[day,'EoS Difference']/days[1]
                df_basin.loc[day,'WoS Difference']=df_basin.loc[day,'WoS Difference']/days[1]
        
       
        
        for plant in allplant:  
            df_basin[plant+' Difference'] = (df_basin[plant+' 2021'] - df_basin[plant+' Desk 2021']).resample('M').sum()
            df_basin[plant+' Difference'].fillna(method='bfill', inplace=True)
            for day2 in pd.date_range(start=date_dict['current_year_start'], end=date_dict['today']):
                days2=calendar.monthrange(day2.year,day.month)
                df_basin.loc[day2,plant+' Difference']=df_basin.loc[day2,plant+' Difference']/days2[1]
            
        #desk_supply_view.to_csv('H:/Yuefeng/LNG Flows/Deskdatatestcom.csv')
        #print('basin desk data', desk_supply_view)
        #print(df_basin['EoS Desk 2021'])
       #create cumsum df 
        df_cumulative = pd.DataFrame(index=pd.date_range(start=date_dict['current_year_start'], end=date_dict['current_year_end'], freq='MS'))
    
        mttomcmd = 1397
        for i in df_basin.columns:
            df_cumulative[i+' Cum']=df_basin[i].resample('M').sum().cumsum().values/mttomcmd
        
        df_cumulative['AB Cum Difference']=(df_cumulative['AB Desk 2021 Cum'] - df_cumulative['AB 2021 Cum'])
        df_cumulative['PB Cum Difference']=(df_cumulative['PB Desk 2021 Cum'] - df_cumulative['PB 2021 Cum'])
        df_cumulative['MENA Cum Difference']=(df_cumulative['MENA Desk 2021 Cum'] - df_cumulative['MENA 2021 Cum'])
        df_cumulative['Global Cum Difference']=(df_cumulative['Global Desk 2021 Cum'] - df_cumulative['Global 2021 Cum'])
        df_cumulative['EoS Cum Difference']=(df_cumulative['EoS Desk 2021 Cum'] - df_cumulative['EoS 2021 Cum'])
        df_cumulative['WoS Cum Difference']=(df_cumulative['WoS Desk 2021 Cum'] - df_cumulative['WoS 2021 Cum'])
    
        #all plant cumsum and remove
        
        for j in allplant:
            df_cumulative[j+' Cum Difference']=(df_cumulative[j+' Desk 2021 Cum'] - df_cumulative[j+' 2021 Cum'])
    
        #reindex to get market plant index, and list    
        SupplyCategories.reset_index(inplace=True)
        SupplyCategories=SupplyCategories[['Plant','Market']]
        
        SupplyCategories.drop(SupplyCategories[SupplyCategories['Plant']=='Calcasieu Pass'].index, inplace=True)
        SupplyCategories.drop(SupplyCategories[SupplyCategories['Plant']=='Mozambique Area 1'].index, inplace=True)
        market_list = SupplyCategories['Market']
        market_list.drop_duplicates(inplace=True)
        market_list = market_list.tolist()
        
        #all market cumsum
        for market in market_list:
            
            market_plants=SupplyCategories['Plant'].loc[SupplyCategories[SupplyCategories['Market'] == market].index.tolist()].values.tolist()
            #print(market_plants)
            
            market_plants2021 = map(lambda x: x+' 2021 Cum' ,market_plants)
            market_plants2020 = map(lambda x: x+' 2020 Cum' ,market_plants)
            market_plantsdesk = map(lambda x: x+' Desk 2021 Cum' ,market_plants)
            
            df_cumulative[market +' 2021 Cum'] = df_cumulative[market_plants2021].sum(axis=1)
            df_cumulative[market +' 2020 Cum'] = df_cumulative[market_plants2020].sum(axis=1)
            df_cumulative[market +' Desk 2021 Cum'] = df_cumulative[market_plantsdesk].sum(axis=1)
            df_cumulative[market +' Cum Difference'] = df_cumulative[market +' Desk 2021 Cum'] - df_cumulative[market +' 2021 Cum']
            
        
        df_cumulative = df_cumulative.round(2)
        df_basin = df_basin.round(2)
        return df_basin, df_cumulative

    #Demand///////////////////////////////////////////////////////////////////////////////////////////////
    def Basin_data_demand(dfdemandMA, desk_demand_view, DemandCategories, date_dict):
        
        
        DemandCategories.set_index('Basin', inplace=True)
    
        #Basin plants list, and remove 
        PB = DemandCategories['Country'].loc['PB'].values.tolist()
        #PB.remove('Mozambique Area 1')
        AB = DemandCategories['Country'].loc['AB'].values.tolist()
        #AB.remove('Calcasieu Pass')
        MENA = DemandCategories['Country'].loc['MENA'].values.tolist()
        
        DemandCategories.set_index('Suez', inplace=True)
        EoS = DemandCategories['Country'].loc['EoS'].values.tolist()
        #EoS.remove('Mozambique Area 1')
        WoS = DemandCategories['Country'].loc['WoS'].values.tolist()
        #use curve id instead of categories, no need move countries in categories
        DemandCurveId = pd.read_excel('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/DeskViewID.xlsx',header=(0),sheet_name='demand')
        DemandCurveId=DemandCurveId[['CurveID','Country']]
        allcountry = DemandCurveId['Country'].values.tolist()
 
         #group fpr region
        DemandCategories.set_index('Region', inplace=True)
        JKTC = DemandCategories['Country'].loc['JKTC'].values.tolist()
        LatAm = DemandCategories['Country'].loc['Lat Am'].values.tolist()
        MEIP = DemandCategories['Country'].loc['MEIP'].values.tolist()
        NWEur = DemandCategories['Country'].loc['NW Eur'].values.tolist()
        OtherEur = DemandCategories['Country'].loc['Other Eur'].values.tolist()
        MedEur = DemandCategories['Country'].loc['Med Eur'].values.tolist()
        OtherRoW = DemandCategories['Country'].loc['Other RoW'].values.tolist()

        #yoy basin
        df_basin = pd.DataFrame(index=pd.date_range(start=date_dict['current_year_start'], end=date_dict['current_year_end']))
        df_basin.index.name='Date'
        df_basin['MENA 2021'] =  dfdemandMA[MENA].loc[date_dict['current_year_start']:date_dict['current_year_end']].sum(axis=1)
        df_basin['MENA 2020'] = np.delete(dfdemandMA[MENA].loc['2020-01-01':'2020-12-31'].sum(axis=1).values, 59)
        df_basin['MENA Desk 2021'] = desk_demand_view[MENA].loc[date_dict['current_year_start']:date_dict['current_year_end']].sum(axis=1).values   
    
        df_basin['PB 2021'] =  dfdemandMA[PB].loc[date_dict['current_year_start']:date_dict['current_year_end']].sum(axis=1)
        df_basin['PB 2020'] = np.delete(dfdemandMA[PB].loc['2020-01-01':'2020-12-31'].sum(axis=1).values, 59)
        df_basin['PB Desk 2021'] = desk_demand_view[PB].loc[date_dict['current_year_start']:date_dict['current_year_end']].sum(axis=1).values   
        
        df_basin['AB 2021'] = dfdemandMA[AB].loc[date_dict['current_year_start']:date_dict['current_year_end']].sum(axis=1)
        df_basin['AB 2020'] = np.delete(dfdemandMA[AB].loc['2020-01-01':'2020-12-31'].sum(axis=1).values, 59)
        df_basin['AB Desk 2021'] = desk_demand_view[AB].loc[date_dict['current_year_start']:date_dict['current_year_end']].sum(axis=1).values   
        
        for country in allcountry:  
            df_basin[country+' 2021'] = dfdemandMA[country].loc[date_dict['current_year_start']:date_dict['current_year_end']]
            df_basin[country+' 2020'] = np.delete(dfdemandMA[country].loc['2020-01-01':'2020-12-31'].values, 59)
            df_basin[country+' Desk 2021'] = desk_demand_view[country].loc[date_dict['current_year_start']:date_dict['current_year_end']]
            df_basin[country+' Difference'] = (df_basin[country+' 2021'] - df_basin[country+' Desk 2021']).resample('M').sum()
            df_basin[country+' Difference'].fillna(method='bfill', inplace=True)
        
            for day2 in pd.date_range(start=date_dict['current_year_start'], end=date_dict['today']):
                days2=calendar.monthrange(day2.year,day2.month)
                df_basin.loc[day2,country+' Difference']=df_basin.loc[day2,country+' Difference']/days2[1]
       
        
        df_basin['Global 2021'] = df_basin[['AB 2021', 'PB 2021','MENA 2021']].loc[date_dict['current_year_start']:date_dict['today']].sum(axis=1)
        df_basin['Global 2020'] = df_basin[['AB 2020', 'PB 2020','MENA 2020']].sum(axis=1)
        df_basin['Global Desk 2021'] = df_basin[['AB Desk 2021', 'PB Desk 2021','MENA Desk 2021']].sum(axis=1)
        
        
        df_basin['EoS 2021'] = dfdemandMA[EoS].loc[date_dict['current_year_start']:date_dict['current_year_end']].sum(axis=1)
        df_basin['EoS 2020'] = np.delete(dfdemandMA[EoS].loc['2020-01-01':'2020-12-31'].sum(axis=1).values, 59)
        df_basin['EoS Desk 2021'] = desk_demand_view[EoS].loc[date_dict['current_year_start']:date_dict['current_year_end']].sum(axis=1).values
        df_basin['WoS 2021'] = dfdemandMA[WoS].loc[date_dict['current_year_start']:date_dict['current_year_end']].sum(axis=1)
        df_basin['WoS 2020'] = np.delete(dfdemandMA[WoS].loc['2020-01-01':'2020-12-31'].sum(axis=1).values, 59)
        df_basin['WoS Desk 2021'] = desk_demand_view[WoS].loc[date_dict['current_year_start']:date_dict['current_year_end']].sum(axis=1).values
        df_basin['EoS Difference']=(df_basin['EoS 2021'] - df_basin['EoS Desk 2021']).resample('M').sum()
        df_basin['EoS Difference'].fillna(method='bfill', inplace=True)
        df_basin['WoS Difference']=(df_basin['WoS 2021'] - df_basin['WoS Desk 2021']).resample('M').sum()
        df_basin['WoS Difference'].fillna(method='bfill', inplace=True)
        df_basin['PB Difference']=(df_basin['PB 2021'] - df_basin['PB Desk 2021']).resample('M').sum()
        df_basin['PB Difference'].fillna(method='bfill', inplace=True)
        df_basin['AB Difference']=(df_basin['AB 2021'] - df_basin['AB Desk 2021']).resample('M').sum()
        df_basin['AB Difference'].fillna(method='bfill', inplace=True)
        df_basin['MENA Difference']=(df_basin['MENA 2021'] - df_basin['MENA Desk 2021']).resample('M').sum()
        df_basin['MENA Difference'].fillna(method='bfill', inplace=True)
        for day in pd.date_range(start=date_dict['current_year_start'], end=date_dict['today']):
            days=calendar.monthrange(day.year,day.month)
            df_basin.loc[day,'EoS Difference']=df_basin.loc[day,'EoS Difference']/days[1]
            df_basin.loc[day,'WoS Difference']=df_basin.loc[day,'WoS Difference']/days[1]
            df_basin.loc[day,'PB Difference']=df_basin.loc[day,'PB Difference']/days[1]
            df_basin.loc[day,'AB Difference']=df_basin.loc[day,'AB Difference']/days[1]
            df_basin.loc[day,'MENA Difference']=df_basin.loc[day,'MENA Difference']/days[1]

        #get region data
        regionlist = [JKTC, LatAm, MEIP, NWEur, OtherEur, MedEur, OtherRoW]
        #print(region1)
        region = pd.DataFrame(regionlist).T
        region.columns=['JKTC', 'LatAm', 'MEIP', 'NWEur', 'OtherEur', 'MedEur', 'OtherRoW']
        #print(region)
        #print(dfdemandMA['Taiwan'])
        for i in region.columns:
            #print(i)
            #print(region[i].dropna(axis=0))
            j=region[i].dropna(axis=0)
            #print(region[region.index(i)])
            df_basin[i+' 2021'] = dfdemandMA[j].loc[date_dict['current_year_start']:date_dict['current_year_end']].sum(axis=1)
            df_basin[i+' 2020'] = np.delete(dfdemandMA[j].loc['2020-01-01':'2020-12-31'].sum(axis=1).values, 59)
            df_basin[i+' Desk 2021'] = desk_demand_view[j].loc[date_dict['current_year_start']:date_dict['current_year_end']].sum(axis=1)
            df_basin[i+' Difference'] = (df_basin[i+' 2021'] - df_basin[i+' Desk 2021']).resample('M').sum()
            df_basin[i+' Difference'].fillna(method='bfill', inplace=True)
            
            for day in pd.date_range(start=date_dict['current_year_start'], end=date_dict['today']):
                days=calendar.monthrange(day.year,day.month)
                df_basin.loc[day,i+' Difference']=df_basin.loc[day,i+' Difference']/days[1]
                    
            #print(df_basin[i +' 2020'])    
       

        #create cumsum df 
        df_cumulative = pd.DataFrame(index=pd.date_range(start=date_dict['current_year_start'], end=date_dict['current_year_end'], freq='MS'))
        
        mttomcmd = 1397
        for i in df_basin.columns:
            df_cumulative[i+' Cum']=df_basin[i].resample('M').sum().cumsum().values/mttomcmd
        
        df_cumulative['AB Cum Difference']=(df_cumulative['AB Desk 2021 Cum'] - df_cumulative['AB 2021 Cum'])
        df_cumulative['PB Cum Difference']=(df_cumulative['PB Desk 2021 Cum'] - df_cumulative['PB 2021 Cum'])
        df_cumulative['MENA Cum Difference']=(df_cumulative['MENA Desk 2021 Cum'] - df_cumulative['MENA 2021 Cum'])
        df_cumulative['Global Cum Difference']=(df_cumulative['Global Desk 2021 Cum'] - df_cumulative['Global 2021 Cum'])
        df_cumulative['EoS Cum Difference']=(df_cumulative['EoS Desk 2021 Cum'] - df_cumulative['EoS 2021 Cum'])
        df_cumulative['WoS Cum Difference']=(df_cumulative['WoS Desk 2021 Cum'] - df_cumulative['WoS 2021 Cum'])
        
        for i in region.columns:
            df_cumulative[i+' Cum Difference']=(df_cumulative[i+' Desk 2021 Cum'] - df_cumulative[i+' 2021 Cum'])
    
        #all plant cumsum and remove
        
        for j in allcountry:
            df_cumulative[j+' Cum Difference']=(df_cumulative[j+' Desk 2021 Cum'] - df_cumulative[j+' 2021 Cum'])

        #dfdemandMA.to_csv('H:/Yuefeng/LNG Flows/Deskdatatestcom.csv')
        #df_cumulative_demand.to_csv('H:/Yuefeng/LNG Flows/Deskdatatestcom.csv')
        df_cumulative = df_cumulative.round(2)
        df_basin = df_basin.round(2)
        #print(df_basin)
        #print(df_cumulative)    
        return df_basin, df_cumulative
    
    def basin_data_full(rolling_period):
        date_dict = SD_Data_Basin.date_dict
        #df_supply, df_demand, SupplyCategories, DemandCategories = SD_Data_Basin.Kpler_data()
        SupplyCategories, DemandCategories,dfsupplyMA, dfdemandMA, desk_supply_view, desk_demand_view = SD_Data_Basin.full_data(rolling_period)
        supply_basin, supply_cumulative = SD_Data_Basin.Basin_data_supply(dfsupplyMA, desk_supply_view, SupplyCategories, date_dict)
        demand_basin, demand_cumulative = SD_Data_Basin.Basin_data_demand(dfdemandMA, desk_demand_view, DemandCategories, date_dict)
        #print(SupplyCategories)
        return supply_basin, supply_cumulative, demand_basin, demand_cumulative
    
#df=SD_Data_Basin.basin_data_full()
#print(df)