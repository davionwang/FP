# -*- coding: utf-8 -*-
"""
Created on Thu Jul  7 10:52:11 2022

@author: SVC-GASQuant2-Prod
"""



#V0 flow MA30 to DB
#v1 add reload for net import
#v2 remove reload if start and end country same
#V3 add Germany and El Salvador
#v4 add Mozambique and Portovaya 



import pandas as pd
import sqlalchemy
import datetime
import numpy as np
from dateutil.relativedelta import relativedelta

import calendar
# from apscheduler.schedulers.background import BackgroundScheduler
# from apscheduler.triggers.combining import OrTrigger
# from apscheduler.triggers.cron import CronTrigger
import sys
#sys.path.append('C:\\Users\SVC-GASQuant2-Prod\YWwebsite\Python Scripts\class') 
sys.path.append(r'\\prd-pwr-web-22\YWwebsite\Python Scripts\class')
from DBtoDF import DBTOPD

pd.set_option('display.max_columns',10)



class LNG_MA30():
    
    def Kpler_data():
        
        #read data from Kpler
        Kpler=DBTOPD('PRD-DB-SQL-211','LNG', 'dbo', 'KplerLNGTrades')
        dfkpler=Kpler.sql_to_df()
        
        #get supply and demand df
        supply_plant_columns=['EndOrigin','InstallationOrigin','VolumeOriginM3']
        supply_country_columns=['EndOrigin','CountryOrigin','VolumeOriginM3']
        demand_columns=['StartDestination','CountryDestination','VolumeDestinationM3']
        df_supply_plant = dfkpler[supply_plant_columns]
        df_supply_country = dfkpler[supply_country_columns]
  
        df_demand = dfkpler[demand_columns]
        df_demand_regas = dfkpler[supply_country_columns]

        dfabpb = dfkpler[['EndOrigin','StartDestination','CountryOrigin','CountryDestination', 'VolumeOriginM3']]
        #get ETA data
        #KplerETA=DBTOPD('PRD-DB-SQL-211','LNG', 'dbo', 'KplerLNGTrades')
        df_supply_eta=Kpler.get_eta_supply()
        df_supply_plant_eta=Kpler.get_eta_supply_plant()
        df_demand_eta=Kpler.get_eta_demand()
        
        #reload
        dfreload=Kpler.get_reload_data()
        dfreload = dfreload[supply_country_columns+['CountryDestination']]
        for i in dfreload.index:
            if dfreload.loc[i,'CountryOrigin'] == dfreload.loc[i,'CountryDestination']:
                dfreload.drop(i, inplace=True)

        return df_supply_plant, df_supply_country, df_demand, df_demand_regas, df_supply_eta, df_supply_plant_eta, df_demand_eta, dfabpb,dfreload
    
    def create_df_supply_MA_Plant (df_supply, SupplyCurveId, rolling_period, start_date, end_date): #use Supply curve id to get all plant supply
        
        #create supply list
        SupplyCurveId=SupplyCurveId[['CurveID','plant']]
        supply_plant_list=SupplyCurveId['plant'].values.tolist()
        #rolling start date
        prestart_date= datetime.datetime.strptime(start_date, '%Y-%m-%d') - rolling_period*datetime.timedelta(days=1)
        dfMA=pd.DataFrame(index=pd.date_range(start=start_date,end=end_date))
        dfMA.index.name='Date'
        for i in supply_plant_list:
            #i=supply_plant_list[0]
            dffulldate=pd.DataFrame(index=pd.date_range(start=prestart_date,end=end_date)) 
            dffulldate.index.name='Date'
            dfplant = df_supply[df_supply['InstallationOrigin']==i].copy()
            dfplant.loc[:,'EndOrigin']=pd.to_datetime(dfplant.loc[:,'EndOrigin'].copy()).dt.strftime('%Y-%m-%d') 
            
            dfplant=dfplant.groupby(['EndOrigin']).sum()*0.000612
            dfplant.index.name='Date'
            dfplant.index=pd.DatetimeIndex(dfplant.index)
            
            merged = pd.merge(dffulldate, dfplant.loc[prestart_date: str(end_date)], on='Date', how='outer')
            merged.fillna(0, inplace=True)
            
            npMA=merged.loc[prestart_date: str(end_date)].rolling(rolling_period).mean()
            dfMA[i]=npMA.loc[start_date:end_date].values
            
        return dfMA
    
    def create_df_supply_MA_Country (dfsupply_plant, SupplyCurveId):
    
        #get market plant index, and list    
        SupplyCurveId.drop_duplicates(inplace=True)
        market_list=SupplyCurveId['Country']
        market_list.drop_duplicates(inplace=True)
        market_list=market_list.values.tolist()
        #print(SupplyCurveId)
        dfMA=pd.DataFrame(index=dfsupply_plant.index)

        #all market cumsum
        for market in market_list:
            #market='United States' #market_list[0]
            market_plants=SupplyCurveId['plant'].loc[SupplyCurveId[SupplyCurveId['Country'] == market].index.tolist()].values.tolist()
            #print(market_plants)
            dfMA[market] = dfsupply_plant[market_plants].sum(axis=1)
            
            
        #print(dfMA.tail())
        return dfMA
    
    
    def create_df_demand_MA_County (df_demand, dfreload, DemandCurveId, rolling_period, start_date, end_date):
    
        DemandCurveId.drop_duplicates(inplace=True)
        demand_country_list = DemandCurveId['Country']
        demand_country_list.drop_duplicates(inplace=True)
        demand_country_list=demand_country_list.values.tolist()   
        prestart_date= datetime.datetime.strptime(start_date, '%Y-%m-%d') - rolling_period*datetime.timedelta(days=1)
        dfMA=pd.DataFrame(index=pd.date_range(start=start_date,end=end_date))
        dfMA.index.name='Date'
        for i in demand_country_list:
            #i = 'China'
            dffulldate=pd.DataFrame(index=pd.date_range(start=prestart_date,end=end_date)) 
            dffulldate.index.name='Date'
            dfcountry = df_demand[df_demand['CountryDestination']==i]
            dfcountry.loc[:,'StartDestination']=pd.to_datetime(dfcountry.loc[:,'StartDestination']).dt.strftime('%Y-%m-%d') 
            
            dfcountry=dfcountry.groupby(['StartDestination']).sum()*0.000612
            dfcountry.index.name='Date'
            dfcountry.index=pd.DatetimeIndex(dfcountry.index) 
            
            merged = pd.merge(dffulldate, dfcountry.loc[prestart_date: str(end_date)], on='Date', how='outer')
            merged.fillna(0, inplace=True)
            
            try:
                dfreloadcountry=dfreload[dfreload['CountryOrigin']==i]
                #print(dfreload)
                dfreloadcountry.loc[:,'EndOrigin']=pd.to_datetime(dfreloadcountry.loc[:,'EndOrigin']).dt.strftime('%Y-%m-%d') 
            
                dfreloadcountry=dfreloadcountry.groupby(['EndOrigin']).sum()*0.000612
                dfreloadcountry.index.name='Date'
                dfreloadcountry.index=pd.DatetimeIndex(dfreloadcountry.index)
                dfreloadcountry.rename(columns={'VolumeOriginM3':'VolumeDestinationM3'}, inplace=True)
                dfreloadfull=pd.DataFrame(index=merged.index)
                dfreloadfull=pd.merge(dfreloadfull, dfreloadcountry.loc[prestart_date: str(end_date)], on='Date', how='outer')
                dfreloadfull.fillna(0, inplace=True)
                #print(dfreloadfull)
                merged=merged-dfreloadfull
                for j in merged.index:
                    #print(merged.loc[j])
                    if merged.loc[j,'VolumeDestinationM3'] < 0:
                        merged.loc[j,'VolumeDestinationM3'] = merged.loc[j,'VolumeDestinationM3'] + dfreloadfull.loc[j,'VolumeDestinationM3']
            
            except KeyError:
                continue
            
            npMA=merged.loc[prestart_date: str(end_date)].rolling(rolling_period).mean()
            dfMA[i]=npMA.loc[start_date:end_date].values
        
        
        return dfMA


#dfcountry[(dfcountry.index >= '2023-01-21') & (dfcountry.index < '2023-02-21')]['VolumeDestinationM3'].sum() / 30



    def supply_desk_plant_view(SupplyCurveId,start_date, end_date,mttomcmd):
    
        #read curveID and to dict, use for change id to country name
        SupplyCurveId=SupplyCurveId[['CurveID','plant']]
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
        
        return merged
    
    def supply_desk_country_view(SupplyCurveId,start_date, end_date,mttomcmd):
    
        #read curveID and to dict, use for change id to country name
        dfsupplyCurveId=SupplyCurveId.iloc[:,0:2]
        SupplyCurveId=dfsupplyCurveId.set_index('CurveID').T.to_dict('list')
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
        
        return merged
    
    def demand_desk_view(DemandCurveId, start_date, end_date, mttomcmd):
        
        #read curveID and to dict, use for change id to country name
        DemandCurveId=DemandCurveId[['CurveID','Country']]
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
        #print(merged)
        return merged
    
    def create_df_etasupply_MA (df_supply_eta, SupplyCurveId, rolling_period, start_date, end_date):
    
        SupplyCurveId.drop_duplicates(inplace=True)
        supply_country_list=SupplyCurveId['Country']
        supply_country_list.drop_duplicates(inplace=True)
        supply_country_list=supply_country_list.values.tolist()
        #prestart_date= datetime.datetime.strptime(start_date, '%Y-%m-%d') - rolling_period*datetime.timedelta(days=1)
        dfMA=pd.DataFrame(index=pd.date_range(start=start_date,end=end_date))
        dfMA.index.name='Date'
        #print(df_supply_eta)
        for i in supply_country_list:
            
            dffulldate=pd.DataFrame(index=pd.date_range(start=df_supply_eta['Date'].iloc[0].strftime('%Y-%m-%d'),end=end_date)) 
            dffulldate.index.name='Date'
            dfcountry = df_supply_eta[df_supply_eta['CountryOrigin']==i].copy()
            
            dfcountry['Date']=pd.to_datetime(dfcountry['Date']).dt.strftime('%Y-%m-%d') 
            
            dfcountry=dfcountry.groupby(['Date']).sum()
            dfcountry['VolumeOriginM3']=dfcountry['VolumeOriginM3']*0.000612
            dfcountry.index.name='Date'
            dfcountry.index=pd.DatetimeIndex(dfcountry.index)
            #print(dfcountry)
            merged = pd.merge(dffulldate, dfcountry['VolumeOriginM3'], on='Date', how='outer')
            merged.fillna(0, inplace=True)
            #print(merged)
            
            #npMA=merged.loc[prestart_date: str(end_date)].rolling(rolling_period).mean()
            npMA=merged.rolling(rolling_period).mean()
            #print(npMA)
            #npMA.to_csv('H:/Yuefeng/LNG Flows/Deskdatatestcom.csv')
            dfMA[i]=npMA.loc[start_date:end_date].values
            #print(dfMA[i])
        return dfMA
    
    
    def create_df_etasupply_MA_plant (df_supply_eta_plant, SupplyCurveId, rolling_period, start_date, end_date):
    
        SupplyCurveId=SupplyCurveId.loc[:,['CurveID','plant']]
        supply_plant_list=SupplyCurveId.loc[:,'plant'].values.tolist()
        #prestart_date= datetime.datetime.strptime(start_date, '%Y-%m-%d') - rolling_period*datetime.timedelta(days=1)
        dfMA=pd.DataFrame(index=pd.date_range(start=start_date,end=end_date))
        dfMA.index.name='Date'
        #print(df_supply_eta)
        for i in supply_plant_list:
            
            dffulldate=pd.DataFrame(index=pd.date_range(start=df_supply_eta_plant['Date'].iloc[0].strftime('%Y-%m-%d'),end=end_date)) 
            dffulldate.index.name='Date'
            dfcountry = df_supply_eta_plant[df_supply_eta_plant['InstallationOrigin']==i]
            
            dfcountry.loc[:,'Date']=pd.to_datetime(dfcountry.loc[:,'Date']).dt.strftime('%Y-%m-%d') 
            
            dfcountry=dfcountry.groupby(['Date']).sum()
            dfcountry.loc[:,'VolumeOriginM3']=dfcountry.loc[:,'VolumeOriginM3']*0.000612
            dfcountry.index.name='Date'
            dfcountry.index=pd.DatetimeIndex(dfcountry.index)
            #print(dfcountry)
            merged = pd.merge(dffulldate, dfcountry['VolumeOriginM3'], on='Date', how='outer')
            merged.fillna(0, inplace=True)
            #print(merged)
            
            #npMA=merged.loc[prestart_date: str(end_date)].rolling(rolling_period).mean()
            npMA=merged.rolling(rolling_period).mean()
            #print(npMA)
            #npMA.to_csv('H:/Yuefeng/LNG Flows/Deskdatatestcom.csv')
            dfMA[i]=npMA.loc[start_date:end_date].values
            #print(dfMA[i])
        #print(dfMA)
        return dfMA

    def create_df_etademand_MA (df_demand_eta, DemandCurveId, rolling_period, start_date, end_date):
    
        DemandCurveId.drop_duplicates(inplace=True)
        demand_country_list = DemandCurveId['Country']
        demand_country_list.drop_duplicates(inplace=True)
        demand_country_list=demand_country_list.values.tolist()
        #prestart_date= datetime.datetime.strptime(start_date, '%Y-%m-%d') - rolling_period*datetime.timedelta(days=1)
        dfMA=pd.DataFrame(index=pd.date_range(start=start_date,end=end_date))
        dfMA.index.name='Date'
        for i in demand_country_list:
            #print(end_date)
            dffulldate=pd.DataFrame(index=pd.date_range(start=df_demand_eta['Date'].iloc[0].strftime('%Y-%m-%d'),end=end_date)) 
            dffulldate.index.name='Date'
            dfcountry = df_demand_eta[df_demand_eta['ForecastedDestination']==i].copy()
            dfcountry['Date']=pd.to_datetime(dfcountry['Date']).dt.strftime('%Y-%m-%d') 
            
            dfcountry=dfcountry.groupby(['Date']).sum()*0.000612
            dfcountry.index.name='Date'
            dfcountry.index=pd.DatetimeIndex(dfcountry.index)
            #print(dfcountry)
            merged = pd.merge(dffulldate, dfcountry, on='Date', how='outer')
            merged.fillna(0, inplace=True)
            #print(dffulldate.tail())
            #print(merged)
            
            #npMA=merged.loc[prestart_date: str(end_date)].rolling(rolling_period).mean()
            npMA=merged.rolling(rolling_period).mean()
            #print(npMA.loc[start_date:end_date])
            #print(dfMA)
            #npMA.to_csv('H:/Yuefeng/LNG Flows/Deskdatatestcom.csv')
            dfMA[i]=npMA.loc[start_date:end_date].values
        #dfMA.to_csv('H:/Yuefeng/LNG Flows/Deskdatatest.csv')
        return dfMA
    
    def IHScontract_data(start_date, end_date, SupplyCurveId):
    
        #demand    
        dfIHSdemand = pd.read_excel('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/IHS Contract.xlsx',header=(0), sheet_name='demand')
        fulldatedemand=pd.DataFrame(index=pd.date_range(start=start_date,end=end_date))
        fulldatedemand.index.name='Date'
        
        mergeddemand = pd.merge(fulldatedemand, dfIHSdemand, on='Date', how='outer')
        mergeddemand.fillna(method='ffill', inplace=True)
        mergeddemand.set_index('Date', inplace=True)
        
        #supply    
        dfIHSsupply = pd.read_excel('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/IHS Contract.xlsx',header=(0), sheet_name='supply')
        fulldatesupply=pd.DataFrame(index=pd.date_range(start=start_date,end=end_date))
        fulldatesupply.index.name='Date'
        
        mergedsupply = pd.merge(fulldatesupply, dfIHSsupply, on='Date', how='outer')
        mergedsupply.fillna(method='ffill', inplace=True)
        mergedsupply.set_index('Date', inplace=True)
        mergedsupply.rename(columns={'PFLNG 1':'PFLNG 1 Sabah', 'Elba Island':'Elba Island Liq.','Mozambique Coral FLNG':'Coral South FLNG'}, inplace=True)
        mergedsupply[['Kollsnes','Stavanger','Vysotsk','Portovaya']] =0
        #get market plant index, and list   
        #print(SupplyCurveId)
        #SupplyCurveId.drop_duplicates(inplace=True)
        #SupplyCurveId.drop([SupplyCurveId[SupplyCurveId['plant']=='Kollsnes'].index], inplace=True)
        #SupplyCurveId.drop([SupplyCurveId[SupplyCurveId['plant']=='Stavanger'].index], inplace=True)

        market_list=SupplyCurveId['Country']
        market_list.drop_duplicates(inplace=True)
        market_list=market_list.values.tolist()
        #print(SupplyCurveId[SupplyCurveId['Country'] == 'United States'].index.tolist())
        #dfMA=pd.DataFrame(index=dfsupply_plant.index)
        #print(market_list)
        #all market cumsum
        for market in market_list:
            #print(market)
            try:
                market_plants=SupplyCurveId['plant'].loc[SupplyCurveId[SupplyCurveId['Country'] == market].index.tolist()].values.tolist()
                mergedsupply[market] = mergedsupply[market_plants].sum(axis=1)
            except KeyError as e:
                print(e)
                #mergedsupply[e] = 0
                #mergedsupply[market] = mergedsupply[market_plants].sum(axis=1)
     
        #flat all contrace by year
        supplyflat = mergedsupply.resample('Y').mean()
        demandflat = mergeddemand.resample('Y').mean()
        #print(supplyflat)
        for i in mergedsupply.index:
            mergedsupply.loc[i,:] = supplyflat.loc[str(i.year)+'-12-31',:]
            mergeddemand.loc[i,:] = demandflat.loc[str(i.year)+'-12-31',:]
            
        return mergeddemand, mergedsupply
    
    def globaldf(SupplyCurveId, DemandCurveId, dfsupply_country, dfdemand_country, dfsupplyMA_eta, dfdemandMA_eta, desk_supply_country_view, desk_demand_view, ihscontractdemand, ihscontractsupply ):
    
        
        SupplyCurveId.drop_duplicates(inplace=True)
        supply_country_list=SupplyCurveId['Country']
        supply_country_list.drop_duplicates(inplace=True)
        supply_country_list=supply_country_list.values.tolist()
        #print(supply_country_list)
        DemandCurveId.drop_duplicates(inplace=True)
        demand_country_list = DemandCurveId['Country']
        demand_country_list.drop_duplicates(inplace=True)
        demand_country_list=demand_country_list.values.tolist()
        
        today=datetime.date.today()
        start_date='2016-01-01' 
        current_year_start = str(today.year)+'-01-01'
        current_year_end = str(today.year)+'-12-31' 
        
        
        #Supply global
        supply_global = pd.DataFrame(index=pd.date_range(start=start_date, end = current_year_end))
        supply_global['Kpler'] = dfsupply_country.sum(axis=1)
        supply_global['eta'] = dfsupplyMA_eta.sum(axis=1)
        supply_global['Desk View'] = desk_supply_country_view.sum(axis=1)
        supply_global['IHS Contract'] = ihscontractsupply.sum(axis=1)
        supply_global['Difference']=(supply_global['Kpler'] - supply_global['Desk View']).resample('M').sum()
        supply_global['Difference'].fillna(method='bfill', inplace=True)
        for day in pd.date_range(start=current_year_start, end=today):
                days=calendar.monthrange(day.year,day.month)
                supply_global.loc[day,'Difference']=supply_global.loc[day,'Difference']/days[1]
    
        supply_global_yoy=pd.DataFrame(index=pd.date_range(start=current_year_start, end=current_year_end))
        supply_global_yoy['Kpler 2016'] = np.delete(supply_global.loc['2016-01-01':'2016-12-31', 'Kpler'].values, 59)
        supply_global_yoy['Kpler 2017'] = supply_global.loc['2017-01-01':'2017-12-31', 'Kpler'].values
        supply_global_yoy['Kpler 2018'] = supply_global.loc['2018-01-01':'2018-12-31', 'Kpler'].values
        supply_global_yoy['Kpler 2019'] = supply_global.loc['2019-01-01':'2019-12-31', 'Kpler'].values
        supply_global_yoy['Kpler 2020'] = np.delete(supply_global.loc['2020-01-01':'2020-12-31', 'Kpler'].values, 59)
        supply_global_yoy['max']=supply_global_yoy[['Kpler 2016','Kpler 2017','Kpler 2018','Kpler 2019','Kpler 2020']].max(axis=1)
        supply_global_yoy['min']=supply_global_yoy[['Kpler 2016','Kpler 2017','Kpler 2018','Kpler 2019','Kpler 2020']].min(axis=1)
        supply_global_yoy['Kpler '+str(today.year)] = supply_global.loc[current_year_start:current_year_end, 'Kpler'].values
        
        supply_global_yoy['Kpler ETA'] = supply_global.loc['2021-01-01':'2021-12-31', 'eta'].values
        supply_global_yoy['Desk View'] = supply_global.loc[current_year_start:current_year_end, 'Desk View'].values
        supply_global_yoy['Difference'] = supply_global.loc[current_year_start:current_year_end, 'Difference'].values
        supply_global_yoy['IHS Contract'] = supply_global.loc[current_year_start:current_year_end, 'IHS Contract'].values

        
        supply_global_yoy=supply_global_yoy.round()   
        
        #Demand global
        demand_global = pd.DataFrame(index=pd.date_range(start=start_date, end = current_year_end))
        demand_global['Kpler'] = dfdemand_country.sum(axis=1)
        demand_global['eta'] = dfdemandMA_eta.sum(axis=1)
        demand_global['Desk View'] = desk_demand_view.sum(axis=1)
        demand_global['IHS Contract'] = ihscontractdemand.sum(axis=1)
        demand_global['Difference']=(demand_global['Kpler'] - demand_global['Desk View']).resample('M').sum()
        demand_global['Difference'].fillna(method='bfill', inplace=True)
        for day in pd.date_range(start=current_year_start, end=today):
                days=calendar.monthrange(day.year,day.month)
                demand_global.loc[day,'Difference']=demand_global.loc[day,'Difference']/days[1]
    
        demand_global_yoy=pd.DataFrame(index=pd.date_range(start=current_year_start, end=current_year_end))
        demand_global_yoy['Kpler 2016'] = np.delete(demand_global.loc['2016-01-01':'2016-12-31', 'Kpler'].values, 59)
        demand_global_yoy['Kpler 2017'] = demand_global.loc['2017-01-01':'2017-12-31', 'Kpler'].values
        demand_global_yoy['Kpler 2018'] = demand_global.loc['2018-01-01':'2018-12-31', 'Kpler'].values
        demand_global_yoy['Kpler 2019'] = demand_global.loc['2019-01-01':'2019-12-31', 'Kpler'].values
        demand_global_yoy['Kpler 2020'] = np.delete(demand_global.loc['2020-01-01':'2020-12-31', 'Kpler'].values, 59)
        demand_global_yoy['max']=demand_global_yoy[['Kpler 2016','Kpler 2017','Kpler 2018','Kpler 2019','Kpler 2020']].max(axis=1)
        demand_global_yoy['min']=demand_global_yoy[['Kpler 2016','Kpler 2017','Kpler 2018','Kpler 2019','Kpler 2020']].min(axis=1)
        demand_global_yoy['Kpler '+str(today.year)] = demand_global.loc[current_year_start:current_year_end, 'Kpler'].values
        demand_global_yoy['Kpler ETA'] = demand_global.loc['2021-01-01':'2021-12-31', 'eta'].values
        demand_global_yoy['Desk View'] = demand_global.loc[current_year_start:current_year_end, 'Desk View'].values
        demand_global_yoy['Difference'] = demand_global.loc[current_year_start:current_year_end, 'Difference'].values
        demand_global_yoy['IHS Contract'] = demand_global.loc[current_year_start:current_year_end, 'IHS Contract'].values
        demand_global_yoy=demand_global_yoy.round()
        
        return supply_global_yoy, demand_global_yoy

    
    #supply//////////////////////////////////////////////////////////////////////////////////////////////
    def Basin_data_supply(dfsupplyMA, desk_supply_view, SupplyCategories, date_dict):
        
        
        SupplyCategories.set_index('Basin', inplace=True)
        
        #Basin plants list, and remove 
        PB = SupplyCategories['Plant'].loc['PB'].values.tolist()
        #PB.remove('Mozambique Area 1')
        AB = SupplyCategories['Plant'].loc['AB'].values.tolist()
        #AB.remove('Calcasieu Pass')
        MENA = SupplyCategories['Plant'].loc['MENA_Bas'].values.tolist()
        
        SupplyCategories.set_index('Suez', inplace=True)
        EoS = SupplyCategories['Plant'].loc['EoS'].values.tolist()
        #EoS.remove('Mozambique Area 1')
        WoS = SupplyCategories['Plant'].loc['WoS'].values.tolist()
        #WoS.remove('Calcasieu Pass')
        #print(EoS)
        #basin column name
        #PB2021 = list(map(lambda x:x+' 2021',PB))
        #PB2020 = list(map(lambda x:x+' 2020',PB))
        PBdesk = list(map(lambda x:x+' Desk',PB))
        #AB2021 = list(map(lambda x:x+' 2021',AB))
        #AB2020 = list(map(lambda x:x+' 2020',AB))
        ABdesk = list(map(lambda x:x+' Desk',AB))
        #MENA2021 = list(map(lambda x:x+' 2021',MENA))
        #MENA2020 = list(map(lambda x:x+' 2020',MENA))
        MENAdesk = list(map(lambda x:x+' Desk',MENA))
        
        allplant=SupplyCategories['Plant'].values.tolist()
        #allplant.remove('Mozambique Area 1')
        #allplant.remove('Calcasieu Pass')
        
        #print(desk_supply_view[PB])
        #print(desk_supply_view['APLNG'].loc[date_dict['current_year_start']:date_dict['next_year_end']].values)
        
        #full time df
        df_fulltime = pd.DataFrame(index=pd.date_range(start=date_dict['last_year_start'], end=date_dict['next_year_end']))
        df_fulltime[['MENA','PB','AB']] = pd.concat([df_fulltime,
                                 dfsupplyMA[MENA].loc[date_dict['last_year_start']:date_dict['next_year_end']].sum(axis=1),
                                 dfsupplyMA[PB].loc[date_dict['last_year_start']:date_dict['next_year_end']].sum(axis=1),
                                 dfsupplyMA[AB].loc[date_dict['last_year_start']:date_dict['next_year_end']].sum(axis=1)], axis=1)
        df_fulltime[MENA] = dfsupplyMA[MENA].loc[date_dict['last_year_start']:date_dict['next_year_end']]
        #df_fulltime[MENA2020] = np.delete(dfsupplyMA[MENA].loc['2020-01-01':'2020-12-31'].values, 59,axis=0)
        df_fulltime[MENAdesk] = np.nan
        df_fulltime.loc[date_dict['current_year_start']:date_dict['next_year_end'],MENAdesk] = desk_supply_view[MENA].loc[date_dict['current_year_start']:date_dict['next_year_end']].copy().values
        
        df_fulltime[PB] = dfsupplyMA[PB].loc[date_dict['last_year_start']:date_dict['next_year_end']]
        #df_fulltime[PB2020] = np.delete(dfsupplyMA[PB].loc['2020-01-01':'2020-12-31'].values, 59,axis=0)
        #print(date_dict['current_year_start'],date_dict['next_year_end'])
        df_fulltime[PBdesk] = np.nan
        df_fulltime.loc[date_dict['current_year_start']:date_dict['next_year_end'],PBdesk] = desk_supply_view[PB].loc[date_dict['current_year_start']:date_dict['next_year_end']].copy().values
        #print(df_fulltime.loc[date_dict['current_year_start']:date_dict['next_year_end'],PBdesk])
        df_fulltime[AB] = dfsupplyMA[AB].loc[date_dict['last_year_start']:date_dict['next_year_end']]
        #df_fulltime[AB2020] = np.delete(dfsupplyMA[AB].loc['2020-01-01':'2020-12-31'].values, 59,axis=0)
        df_fulltime[ABdesk] = np.nan
        df_fulltime.loc[date_dict['current_year_start']:date_dict['next_year_end'],ABdesk] = desk_supply_view[AB].loc[date_dict['current_year_start']:date_dict['next_year_end']].copy().values
        #print(df_fulltime['APLNG'].loc[date_dict['current_year_start']:date_dict['next_year_end']])
        #yoy basin
        df_basin = pd.DataFrame(index=pd.date_range(start=date_dict['last_year_start'], end=date_dict['next_year_end']))
        df_basin.index.name='Date'
        df_basin['MENA'] = df_fulltime['MENA'].values
        #df_basin['MENA 2020'] = np.delete(dfsupplyMA[MENA].loc['2020-01-01':'2020-12-31'].sum(axis=1).values, 59)
        df_basin['MENA Desk'] = np.nan
        df_basin['MENA Desk'].loc[date_dict['current_year_start']:date_dict['next_year_end']] = desk_supply_view[MENA].loc[date_dict['current_year_start']:date_dict['next_year_end']].sum(axis=1).values   
        
        df_basin['PB'] = df_fulltime['PB'].values
        #df_basin['PB 2020'] = np.delete(dfsupplyMA[PB].loc['2020-01-01':'2020-12-31'].sum(axis=1).values, 59)
        df_basin['PB Desk'] = np.nan
        df_basin['PB Desk'].loc[date_dict['current_year_start']:date_dict['next_year_end']] = desk_supply_view[PB].loc[date_dict['current_year_start']:date_dict['next_year_end']].sum(axis=1).values   
        
        df_basin['AB'] = df_fulltime['AB'].values
        #df_basin['AB 2020'] = np.delete(dfsupplyMA[AB].loc['2020-01-01':'2020-12-31'].sum(axis=1).values, 59)
        df_basin['AB Desk'] = np.nan
        df_basin['AB Desk'].loc[date_dict['current_year_start']:date_dict['next_year_end']] = desk_supply_view[AB].loc[date_dict['current_year_start']:date_dict['next_year_end']].sum(axis=1).values   
        
        df_basin[MENA] = dfsupplyMA[MENA].loc[date_dict['last_year_start']:date_dict['next_year_end']]
        #df_basin[MENA2020] = np.delete(dfsupplyMA[MENA].loc['2020-01-01':'2020-12-31'].values, 59,axis=0)
        df_basin[MENAdesk] = df_fulltime[MENAdesk].copy()
        #df_basin[MENAdesk].loc[date_dict['current_year_start']:date_dict['next_year_end']] = desk_supply_view[MENA].loc[date_dict['current_year_start']:date_dict['next_year_end']]
        
        #print(desk_supply_view[PB].loc[date_dict['current_year_start']:date_dict['next_year_end']])
        df_basin[PB] = dfsupplyMA[PB].loc[date_dict['last_year_start']:date_dict['next_year_end']]
        #df_basin[PB2020] = np.delete(dfsupplyMA[PB].loc['2020-01-01':'2020-12-31'].values, 59,axis=0)
        df_basin[PBdesk] = df_fulltime[PBdesk].copy()
        #df_basin[PBdesk].loc[date_dict['current_year_start']:date_dict['next_year_end']] = desk_supply_view[PB].loc[date_dict['current_year_start']:date_dict['next_year_end']].values 
        #print(df_basin[PBdesk])
        
        df_basin[AB] = dfsupplyMA[AB].loc[date_dict['last_year_start']:date_dict['next_year_end']]
        #df_basin[AB2020] = np.delete(dfsupplyMA[AB].loc['2020-01-01':'2020-12-31'].values, 59,axis=0)
        df_basin[ABdesk] = df_fulltime[ABdesk].copy()
        #df_basin[ABdesk].loc[date_dict['current_year_start']:date_dict['next_year_end']] = desk_supply_view[AB].loc[date_dict['current_year_start']:date_dict['next_year_end']].values 
        
        df_basin['Global'] = df_basin[['AB', 'PB','MENA']].loc[date_dict['last_year_start']:date_dict['today']].sum(axis=1)
        #df_basin['Global 2020'] = df_basin[['AB 2020', 'PB 2020','MENA 2020']].sum(axis=1)
        df_basin['Global Desk'] = df_basin[['AB Desk', 'PB Desk','MENA Desk']].sum(axis=1)
        

        
        df_basin['EoS'] = dfsupplyMA[EoS].loc[date_dict['last_year_start']:date_dict['next_year_end']].sum(axis=1)
        #df_basin['EoS 2020'] = np.delete(dfsupplyMA[EoS].loc['2020-01-01':'2020-12-31'].sum(axis=1).values, 59)
        df_basin['EoS Desk'] = np.nan
        df_basin['EoS Desk'].loc[date_dict['current_year_start']:date_dict['next_year_end']] = desk_supply_view[EoS].loc[date_dict['current_year_start']:date_dict['next_year_end']].sum(axis=1).values
        df_basin['WoS'] = dfsupplyMA[WoS].loc[date_dict['last_year_start']:date_dict['next_year_end']].sum(axis=1)
        #df_basin['WoS 2020'] = np.delete(dfsupplyMA[WoS].loc['2020-01-01':'2020-12-31'].sum(axis=1).values, 59)
        df_basin['WoS Desk'] = np.nan
        df_basin['WoS Desk'].loc[date_dict['current_year_start']:date_dict['next_year_end']] = desk_supply_view[WoS].loc[date_dict['current_year_start']:date_dict['next_year_end']].sum(axis=1).values
        df_basin['EoS Difference']=(df_basin['EoS'] - df_basin['EoS Desk']).resample('M').sum()
        df_basin['EoS Difference'].fillna(method='bfill', inplace=True)
        df_basin['WoS Difference']=(df_basin['WoS'] - df_basin['WoS Desk']).resample('M').sum()
        df_basin['WoS Difference'].fillna(method='bfill', inplace=True)
        for day in pd.date_range(start=date_dict['current_year_start'], end=date_dict['today']):
                days=calendar.monthrange(day.year,day.month)
                df_basin.loc[day,'EoS Difference']=df_basin.loc[day,'EoS Difference']/days[1]
                df_basin.loc[day,'WoS Difference']=df_basin.loc[day,'WoS Difference']/days[1]
        
        for plant in allplant:  
            df_basin[plant+' Difference'] = (df_basin[plant] - df_basin[plant+' Desk']).resample('M').sum()
            df_basin[plant+' Difference'].fillna(method='bfill', inplace=True)
            for day2 in pd.date_range(start=date_dict['last_year_start'], end=date_dict['today']):
                days2=calendar.monthrange(day2.year,day.month)
                df_basin.loc[day2,plant+' Difference']=df_basin.loc[day2,plant+' Difference']/days2[1]
        #print(df_basin)    
        #create cumsum df 
        mttomcmd = 1397
        #current year cum
        df_cumulative = pd.DataFrame(index=pd.date_range(start=date_dict['last_year_start'], end=date_dict['next_year_end'], freq='MS'))
    
        for i in df_basin.columns:
            df_cumulative[i+' Cum'] = np.nan
            df_cumulative[i+' Cum'].loc[date_dict['last_year_start']:date_dict['last_year_end']]=df_basin[i].loc[date_dict['last_year_start']:date_dict['last_year_end']].resample('M').sum().cumsum().values/mttomcmd
            df_cumulative[i+' Cum'].loc[date_dict['current_year_start']:date_dict['current_year_end']]=df_basin[i].loc[date_dict['current_year_start']:date_dict['current_year_end']].resample('M').sum().cumsum().values/mttomcmd
            df_cumulative[i+' Cum'].loc[date_dict['next_year_start']:date_dict['next_year_end']]=df_basin[i].loc[date_dict['next_year_start']:date_dict['next_year_end']].resample('M').sum().cumsum().values/mttomcmd        
        
        df_cumulative['AB Cum Difference']=(df_cumulative['AB Desk Cum'] - df_cumulative['AB Cum'])
        df_cumulative['PB Cum Difference']=(df_cumulative['PB Desk Cum'] - df_cumulative['PB Cum'])
        df_cumulative['MENA Cum Difference']=(df_cumulative['MENA Desk Cum'] - df_cumulative['MENA Cum'])
        df_cumulative['Global Cum Difference']=(df_cumulative['Global Desk Cum'] - df_cumulative['Global Cum'])
        df_cumulative['EoS Cum Difference']=(df_cumulative['EoS Desk Cum'] - df_cumulative['EoS Cum'])
        df_cumulative['WoS Cum Difference']=(df_cumulative['WoS Desk Cum'] - df_cumulative['WoS Cum'])
    
        
        #all plant cumsum and remove
        #print(allplant)
        for j in allplant:
            df_cumulative[j+' Cum Difference']=(df_cumulative[j+' Desk Cum'] - df_cumulative[j+' Cum'])
            #df_cumulative_last[j+' Cum Difference']=(df_cumulative_last[j+' Desk Cum'] - df_cumulative_last[j+' Cum'])
        #reindex to get market plant index, and list    
        SupplyCategories.reset_index(inplace=True)
        SupplyCategories=SupplyCategories[['Plant','Market']].copy()
        
        #SupplyCategories.drop(SupplyCategories[SupplyCategories['Plant']=='Calcasieu Pass'].copy().index, inplace=True)
        #SupplyCategories.drop(SupplyCategories[SupplyCategories['Plant']=='Mozambique Area 1'].copy().index, inplace=True)
        market_list = SupplyCategories['Market'].copy()
        market_list.drop_duplicates(inplace=True)
        market_list = market_list.tolist()
        #print(df_cumulative)
        #all market cumsum
        for market in market_list:
            
            market_plants=SupplyCategories['Plant'].loc[SupplyCategories[SupplyCategories['Market'] == market].index.tolist()].values.tolist()
            #print(market_plants)
            
            market_plants_cum = map(lambda x: x+' Cum' ,market_plants)
            #market_plants_last = map(lambda x: x+date_dict['last_year_start'][0:4]+' Cum' ,market_plants)
            market_plantsdesk = map(lambda x: x+' Desk Cum' ,market_plants)
            
            df_cumulative[market +' Cum'] = df_cumulative[market_plants_cum].sum(axis=1)
            #df_cumulative_last[market +' last Cum'] = df_cumulative_last[market_plants].sum(axis=1)
            df_cumulative[market +' Desk Cum'] = df_cumulative[market_plantsdesk].sum(axis=1)
            df_cumulative[market +' Cum Difference'] = df_cumulative[market +' Desk Cum'] - df_cumulative[market +' Cum']
            
        #print(df_cumulative['Qatar Desk Cum'])
        #print(df_cumulative_last)
        df_cumulative = df_cumulative.round(2)
        df_basin = df_basin.round()
        return df_basin, df_cumulative
      
    
    
    #Demand/////////////////////////////////////////////////////////////////////////////////////////////////////////////
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
        #WoS.remove('Calcasieu Pass')
        #print(EoS)
        #basin column name
        
        #use curve id instead of categories, no need move countries in categories
        #DemandCurveId = pd.read_excel('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/DeskViewID.xlsx',header=(0),sheet_name='demand')
        #DemandCurveId=DemandCurveId[['CurveID','Country']]
        #allcountry = DemandCurveId['Country'].values.tolist()
        curveid = DBTOPD.get_curve_id()
        dfcurveid = curveid.loc[:,['CurveId','Type','Location','Country']]
        DemandCurveId = dfcurveid.loc[dfcurveid[dfcurveid['Type']=='Demand'].index]
        DemandCurveId = DemandCurveId[['CurveId','Country','Location']]
        DemandCurveId.rename(columns={'CurveId':'CurveID','Country':'Country','Location':'plant'}, inplace=True)
        DemandCurveId=DemandCurveId[['CurveID','Country']]
        #DemandCurveId=DemandCurveId.set_index('CurveID').T.to_dict('list')
        
        allcountry = DemandCurveId['Country'].values.tolist()
        allcountry.remove('Russian Federation')
        allcountry.remove('South Africa') #new market
        allcountry.remove('Sri Lanka')
        
        
        
        #group fpr region
        DemandCategories.set_index('Region', inplace=True)
        JKTC = DemandCategories['Country'].loc['JKTC'].values.tolist()
        #JKTC.remove('Taiwan')
        
        LatAm = DemandCategories['Country'].loc['Lat Am'].values.tolist()
       
        MEIP = DemandCategories['Country'].loc['MEIP'].values.tolist()
        
        EurDesk = DemandCategories['Country'].loc['Eur Desk'].values.tolist()
        
        OtherEur = DemandCategories['Country'].loc['Other Eur'].values.tolist()
        
        #MedEur = DemandCategories['Country'].loc['Med Eur'].values.tolist()
        
        OtherRoW = DemandCategories['Country'].loc['Other RoW'].values.tolist()
        
        #print(dfdemandMA)
        #yoy basin
        df_basin = pd.DataFrame(index=pd.date_range(start=date_dict['last_year_start'], end=date_dict['next_year_end']))
        df_basin.index.name='Date'
        df_basin['MENA'] =  dfdemandMA[MENA].loc[date_dict['last_year_start']:date_dict['next_year_end']].sum(axis=1)
        #df_basin['MENA 2020'] = np.delete(dfdemandMA[MENA].loc['2020-01-01':'2020-12-31'].sum(axis=1).values, 59)
        df_basin['MENA Desk'] = np.nan
        df_basin['MENA Desk'].loc[date_dict['current_year_start']:date_dict['next_year_end']] = desk_demand_view[MENA].loc[date_dict['current_year_start']:date_dict['next_year_end']].sum(axis=1).values   
    
        df_basin['PB'] =  dfdemandMA[PB].loc[date_dict['last_year_start']:date_dict['next_year_end']].sum(axis=1)
        #df_basin['PB 2020'] = np.delete(dfdemandMA[PB].loc['2020-01-01':'2020-12-31'].sum(axis=1).values, 59)
        df_basin['PB Desk'] = np.nan
        df_basin['PB Desk'].loc[date_dict['current_year_start']:date_dict['next_year_end']] = desk_demand_view[PB].loc[date_dict['current_year_start']:date_dict['next_year_end']].sum(axis=1).values   
        
        df_basin['AB'] = dfdemandMA[AB].loc[date_dict['last_year_start']:date_dict['next_year_end']].sum(axis=1)
        #df_basin['AB'] = np.delete(dfdemandMA[AB].loc['2020-01-01':'2020-12-31'].sum(axis=1).values, 59)
        df_basin['AB Desk'] = np.nan
        df_basin['AB Desk'].loc[date_dict['current_year_start']:date_dict['next_year_end']] = desk_demand_view[AB].loc[date_dict['current_year_start']:date_dict['next_year_end']].sum(axis=1).values   
        
        for country in allcountry:  
            df_basin[country] = dfdemandMA[country].loc[date_dict['last_year_start']:date_dict['next_year_end']]
            #df_basin[country+' 2020'] = np.delete(dfdemandMA[country].loc['2020-01-01':'2020-12-31'].values, 59)
            df_basin[country+' Desk'] = np.nan
            df_basin[country+' Desk'].loc[date_dict['current_year_start']:date_dict['next_year_end']] = desk_demand_view[country].loc[date_dict['current_year_start']:date_dict['next_year_end']]
            df_basin[country+' Difference'] = (df_basin[country] - df_basin[country+' Desk']).resample('M').sum()
            df_basin[country+' Difference'].fillna(method='bfill', inplace=True)
        
            for day2 in pd.date_range(start=date_dict['last_year_start'], end=date_dict['today']):
                days2=calendar.monthrange(day2.year,day2.month)
                df_basin.loc[day2,country+' Difference']=df_basin.loc[day2,country+' Difference']/days2[1]

        df_basin['Global'] = df_basin[['AB', 'PB','MENA']].loc[date_dict['last_year_start']:date_dict['today']].sum(axis=1)
        #df_basin['Global'] = df_basin[['AB', 'PB','MENA']].loc[date_dict['last_year_start']:date_dict['last_year_end']].sum(axis=1)
        df_basin['Global Desk'] = df_basin[['AB Desk', 'PB Desk','MENA Desk']].sum(axis=1)
        #print( df_basin['Global Desk'])
        
        df_basin['EoS'] = dfdemandMA[EoS].loc[date_dict['last_year_start']:date_dict['next_year_end']].sum(axis=1)
        #df_basin['EoS 2020'] = np.delete(dfdemandMA[EoS].loc['2020-01-01':'2020-12-31'].sum(axis=1).values, 59)
        df_basin['EoS Desk'] = np.nan
        df_basin['EoS Desk'].loc[date_dict['current_year_start']:date_dict['next_year_end']] = desk_demand_view[EoS].loc[date_dict['current_year_start']:date_dict['next_year_end']].sum(axis=1).values
        df_basin['WoS'] = dfdemandMA[WoS].loc[date_dict['last_year_start']:date_dict['next_year_end']].sum(axis=1)
        #df_basin['WoS 2020'] = np.delete(dfdemandMA[WoS].loc['2020-01-01':'2020-12-31'].sum(axis=1).values, 59)
        df_basin['WoS Desk'] = np.nan
        df_basin['WoS Desk'].loc[date_dict['current_year_start']:date_dict['next_year_end']] = desk_demand_view[WoS].loc[date_dict['current_year_start']:date_dict['next_year_end']].sum(axis=1).values
        df_basin['EoS Difference']=(df_basin['EoS'] - df_basin['EoS Desk']).resample('M').sum()
        df_basin['EoS Difference'].fillna(method='bfill', inplace=True)
        df_basin['WoS Difference']=(df_basin['WoS'] - df_basin['WoS Desk']).resample('M').sum()
        df_basin['WoS Difference'].fillna(method='bfill', inplace=True)
        df_basin['PB Difference']=(df_basin['PB'] - df_basin['PB Desk']).resample('M').sum()
        df_basin['PB Difference'].fillna(method='bfill', inplace=True)
        df_basin['AB Difference']=(df_basin['AB'] - df_basin['AB Desk']).resample('M').sum()
        df_basin['AB Difference'].fillna(method='bfill', inplace=True)
        df_basin['MENA Difference']=(df_basin['MENA'] - df_basin['MENA Desk']).resample('M').sum()
        df_basin['MENA Difference'].fillna(method='bfill', inplace=True)
        for day in pd.date_range(start=date_dict['last_year_start'], end=date_dict['today']):
            days=calendar.monthrange(day.year,day.month)
            df_basin.loc[day,'EoS Difference']=df_basin.loc[day,'EoS Difference']/days[1]
            df_basin.loc[day,'WoS Difference']=df_basin.loc[day,'WoS Difference']/days[1]
            df_basin.loc[day,'PB Difference']=df_basin.loc[day,'PB Difference']/days[1]
            df_basin.loc[day,'AB Difference']=df_basin.loc[day,'AB Difference']/days[1]
            df_basin.loc[day,'MENA Difference']=df_basin.loc[day,'MENA Difference']/days[1]
        
        #get region data
        regionlist = [JKTC, LatAm, MEIP, EurDesk, OtherEur, OtherRoW]
        #print(region1)
        region = pd.DataFrame(regionlist).T
        region.columns=['JKTC', 'LatAm', 'MEIP', 'EurDesk', 'OtherEur', 'OtherRoW']
        #print(region)
        #print(dfdemandMA['Taiwan'])
        for i in region.columns:
            #print(i)
            #print(region[i].dropna(axis=0))
            j=region[i].dropna(axis=0)
            #print(region[region.index(i)])
            df_basin[i] = dfdemandMA[j].loc[date_dict['last_year_start']:date_dict['next_year_end']].sum(axis=1)
            #df_basin[i+' 2020'] = np.delete(dfdemandMA[j].loc['2020-01-01':'2020-12-31'].sum(axis=1).values, 59)
            df_basin[i+' Desk'] = desk_demand_view[j].loc[date_dict['last_year_start']:date_dict['next_year_end']].sum(axis=1)
            df_basin[i+' Difference'] = (df_basin[i] - df_basin[i+' Desk']).resample('M').sum()
            df_basin[i+' Difference'].fillna(method='bfill', inplace=True)
            
            for day in pd.date_range(start=date_dict['last_year_start'], end=date_dict['today']):
                days=calendar.monthrange(day.year,day.month)
                df_basin.loc[day,i+' Difference']=df_basin.loc[day,i+' Difference']/days[1]
                    
           
        #create cumsum df 
        df_cumulative = pd.DataFrame(index=pd.date_range(start=date_dict['last_year_start'], end=date_dict['next_year_end'], freq='MS'))
        
        mttomcmd = 1397
        #for i in df_basin.columns:
        #    df_cumulative[i+' Cum']=df_basin[i].resample('M').sum().cumsum().values/mttomcmd
        
        for i in df_basin.columns:
            df_cumulative[i+' Cum'] = np.nan
            df_cumulative[i+' Cum'].loc[date_dict['last_year_start']:date_dict['last_year_end']]=df_basin[i].loc[date_dict['last_year_start']:date_dict['last_year_end']].resample('M').sum().cumsum().values/mttomcmd
            df_cumulative[i+' Cum'].loc[date_dict['current_year_start']:date_dict['current_year_end']]=df_basin[i].loc[date_dict['current_year_start']:date_dict['current_year_end']].resample('M').sum().cumsum().values/mttomcmd
            df_cumulative[i+' Cum'].loc[date_dict['next_year_start']:date_dict['next_year_end']]=df_basin[i].loc[date_dict['next_year_start']:date_dict['next_year_end']].resample('M').sum().cumsum().values/mttomcmd        
        
        #get net import
        #for c in df_cumulative.columns:
            #if c in df_cumulative_supply.columns:
            #print(df_cumulative[c])
            #print(df_cumulative_supply[c])
                #df_cumulative[c]=df_cumulative[c]-df_cumulative_supply[c]
        
        df_cumulative['AB Cum Difference']=(df_cumulative['AB Desk Cum'] - df_cumulative['AB Cum'])
        df_cumulative['PB Cum Difference']=(df_cumulative['PB Desk Cum'] - df_cumulative['PB Cum'])
        df_cumulative['MENA Cum Difference']=(df_cumulative['MENA Desk Cum'] - df_cumulative['MENA Cum'])
        df_cumulative['Global Cum Difference']=(df_cumulative['Global Desk Cum'] - df_cumulative['Global Cum'])
        df_cumulative['EoS Cum Difference']=(df_cumulative['EoS Desk Cum'] - df_cumulative['EoS Cum'])
        df_cumulative['WoS Cum Difference']=(df_cumulative['WoS Desk Cum'] - df_cumulative['WoS Cum'])
        
        for i in region.columns:
            df_cumulative[i+' Cum Difference']=(df_cumulative[i+' Desk Cum'] - df_cumulative[i+' Cum'])
    
        #all plant cumsum and remove
        
        for j in allcountry:
            df_cumulative[j+' Cum Difference']=(df_cumulative[j+' Desk Cum'] - df_cumulative[j+' Cum'])
       
        df_cumulative = df_cumulative.round(2)
        df_basin = df_basin.round(2)
        #print(df_basin)
        #print(df_cumulative)    
        return df_basin, df_cumulative
    
    
    def Region_Flex(df_basin_demand, date_dict, DemandCategories, mttomcmd, rolling_period, file_path):
        
        today = datetime.date.today()
        Kpler=DBTOPD('PRD-DB-SQL-209','AnalyticsModel', 'ts', 'AnalyticsHistory')
        dflastdeskview=Kpler.lastdeskdemand_to_df()
        
        #DemandCurveId = pd.read_excel('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/DeskViewID.xlsx',header=(0),sheet_name='demandwithRU')
        #DemandCurveId=DemandCurveId[['CurveID','Country']]
        #DemandCurveId=DemandCurveId.set_index('CurveID').T.to_dict('list')
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
        newdflastdeskview = pd.DataFrame(dflastdeskview[['ValueDate','CurveId', 'Value']].values, columns=['ValueDate','CurveId','Value'])
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
        df_flex['Previous View mcm'] = merged[EurDesk].loc[date_dict['current_year_start']:date_dict['current_year_end']].sum(axis=1)
        df_flex['Previous View mcm'].loc[:today] = None 
        df_flex['ALL Flex mcm'] = df_basin_demand['EurDesk Desk']
        df_flex['Eur Desk 2021 mcm'] = df_basin_demand['EurDesk'].loc[date_dict['current_year_start']:date_dict['current_year_end']]
        df_flex['Eur Desk 2020 mcm'] = df_basin_demand['EurDesk'].loc[date_dict['last_year_start']:date_dict['last_year_end']]
        
        return df_flex
    
    
    def NWE_cargo(df_basin_demand, df_cumulative_demand, date_dict, DemandCategories, mttomcmd,file_path):
        today = datetime.date.today()
        Kpler=DBTOPD('PRD-DB-SQL-209','AnalyticsModel', 'ts', 'AnalyticsHistory')
        dflastdeskview=Kpler.lastdeskdemand_to_df()
        
        #DemandCurveId = pd.read_excel('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/DeskViewID.xlsx',header=(0),sheet_name='demandwithRU')
        #DemandCurveId=DemandCurveId[['CurveID','Country']]
        #DemandCurveId=DemandCurveId.set_index('CurveID').T.to_dict('list')
        
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
        newdflastdeskview = pd.DataFrame(dflastdeskview[['ValueDate','CurveId', 'Value']].values, columns=['ValueDate','CurveId','Value'])
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
        df_flex['Previous View'].loc[:today] = None 
        df_flex['ALL Flex'] = df_basin_demand['EurDesk Desk']
        df_flex['Eur Desk 2021'] = df_basin_demand['EurDesk'].loc[date_dict['current_year_start']:date_dict['current_year_end']]
        df_flex['Eur Desk 2020'] = df_basin_demand['EurDesk'].loc[date_dict['last_year_start']:date_dict['last_year_end']]
        #mcm/d to mt/y =0.2613,
        df_flex_mt = df_flex*0.2613
        #mcm/d to cm/y = 596379, standard cargo = 150000cm
        df_flex_cargo = df_flex*596379/150000
        for j in df_flex_mt.index:
            days=calendar.monthrange(j.year,j.month)
            df_flex_mt.loc[j]=df_flex_mt.loc[j]/365*days[1]
            df_flex_cargo.loc[j]=df_flex_cargo.loc[j]/365*days[1]
        df_flex_mt = df_flex_mt.round(2)
        df_flex_cargo = df_flex_cargo.round(2)

        EurCum=df_cumulative_demand[['EurDesk Cum','EurDesk Desk Cum', 'EurDesk Cum Difference']]
        EurCumCargo=EurCum/0.062
        #for k in EurCumCargo.index:
         #   days=calendar.monthrange(k.year,k.month)
          #  EurCumCargo.loc[k]=EurCumCargo.loc[k]/365*days[1]
        EurCumBcm=EurCum*1.397
        EurCumCargo = EurCumCargo.round(2)
        EurCumBcm = EurCumBcm.round(2)
        
        df_flex_mt.columns = ['Previous View mt','ALL Flex mt','Eur Desk 2021 mt','Eur Desk 2020 mt']
        df_flex_cargo.columns = ['Previous View cargo','ALL Flex cargo','Eur Desk 2021 cargo','Eur Desk 2020 cargo']
        
        return df_flex_mt, df_flex_cargo, EurCumBcm

    def ABtoPB(dfabpb, SupplyCategories, DemandCategories, date_dict):
        
        start_date = date_dict['start_date']
        end_date= date_dict['end_date']
        
        dfabpb['EndOrigin']=pd.to_datetime(dfabpb['EndOrigin'])
        dfabpb.set_index('EndOrigin',inplace=True)
        dfabpb.sort_values(by='EndOrigin',inplace=True)
        dfabpb = dfabpb.loc[start_date:]
        #df = df.groupby(['EndOrigin']).sum()
        #print(df)
        
        SupplyCategories = SupplyCategories[['Market', 'Basin']]
        SupplyCategories.set_index('Market', inplace=True)
        #supply_dict= SupplyCategories.to_dict()
        supply_dict = {'Australia': 'PB', 'Yemen': 'MENA_Bas', 'Algeria': 'MENA_Bas', 'Malaysia': 'PB', 'Equatorial Guinea': 'AB', 'Nigeria': 'AB', 'Indonesia': 'PB', 'United States': 'AB', 'Cameroon': 'AB', 'Egypt': 'MENA_Bas', 'United Arab Emirates': 'MENA_Bas', 'Brunei': 'PB', 'Libya': 'MENA_Bas', 'Peru': 'PB', 'Papua New Guinea': 'PB', 'Oman': 'MENA_Bas', 'Qatar': 'MENA_Bas', 'Russian Federation': 'AB', 'Norway': 'AB', 'Angola': 'AB', 'Mozambique': 'PB', 'Trinidad and Tobago': 'AB', 'Argentina': 'AB'}
        
        #AB-PB
        dfabpb.replace(supply_dict, inplace=True)
        
        DemandCategories = DemandCategories[['Country', 'Basin']]
        DemandCategories.set_index('Country', inplace=True)
        Demand_dict=  {'Argentina': 'AB', 'Australia': 'PB', 'Bahrain': 'MENA', 'Bangladesh': 'MENA', 'Belgium': 'AB', 'Brazil': 'AB', 'Canada': 'AB', 'Chile': 'PB', 'China': 'PB', 'Colombia': 'AB', 'Croatia': 'AB', 'Cyprus': 'MENA', 'Dominican Republic': 'AB', 'Egypt': 'MENA', 'Finland': 'AB', 'France': 'AB', 'Ghana': 'AB', 'Greece': 'AB', 'India': 'MENA', 'Indonesia': 'PB', 'Israel': 'MENA', 'Italy': 'AB', 'Jamaica': 'AB', 'Japan': 'PB', 'Jordan': 'MENA', 'Kuwait': 'MENA', 'Lithuania': 'AB', 'Malaysia': 'PB', 'Malta': 'AB', 'Myanmar': 'PB', 'Mexico': 'PB', 'Netherlands': 'AB', 'Norway': 'AB', 'Oman': 'MENA', 'Pakistan': 'MENA', 'Panama': 'AB', 'Poland': 'AB', 'Portugal': 'AB', 'Puerto Rico': 'AB', 'Singapore Republic': 'PB', 'South Korea': 'PB', 'Spain': 'AB', 'Sweden': 'AB', 'Taiwan': 'PB', 'Thailand': 'PB', 'Turkey': 'AB', 'United Arab Emirates': 'MENA', 'United Kingdom': 'AB', 'United States': 'AB'}
        dfabpb.replace(Demand_dict, inplace=True)
        dfabpb.reset_index(inplace=True)
            
        
        ABPB=pd.DataFrame(index=dfabpb.index, columns=['EndOrigin','StartDestination','CountryOrigin','CountryDestination', 'VolumeOriginM3'])
        for i in dfabpb.index:
            if dfabpb.loc[i,'CountryOrigin'] == 'AB' and dfabpb.loc[i, 'CountryDestination']=='PB':
                ABPB.loc[i] = dfabpb.loc[i]
        
        ABPB.dropna(axis=0,inplace=True)
        
        #shipping_days = pd.DataFrame((ABPB['StartDestination'] - ABPB['EndOrigin']).map(lambda x:x.days).values, /pd.Timedelta(1, 'H')).fillna(0).astype(int)
         #                            index=ABPB.index ,columns=['shipping days'])
        shipping_days = pd.DataFrame(((ABPB['StartDestination'] - ABPB['EndOrigin'])/pd.Timedelta(1, 'D')).astype(int),
                                     index=ABPB.index ,columns=['shipping days'])
        #print(shipping_days)
        shipping_days['StartDestination'] = pd.to_datetime(ABPB['StartDestination']).dt.strftime('%Y-%m-%d')
        shipping_days.drop(shipping_days[shipping_days['shipping days']==0].index,inplace=True)
        #print(shipping_days)
        shipping_days=shipping_days.groupby(['StartDestination']).mean()
        #print(shipping_days.loc['2021-04-01':'2021-05-01','shipping days'])
        #print(shipping_days.loc['2021-04-01':'2021-05-01','shipping days'].mean())
        ABPB['StartDestination']=pd.to_datetime(ABPB['StartDestination']).dt.strftime('%Y-%m-%d') 
        #ABPB['StartDestination']=pd.to_datetime(ABPB['StartDestination']).dt.strftime('%Y-%m-%d') 
        
        
        ABPB=ABPB.groupby(['StartDestination']).sum()
        ABPB['VolumeOriginM3'] = ABPB['VolumeOriginM3']*0.000000438
        ABPB['shipping days'] = shipping_days['shipping days']
        ABPB.index.name='Date'
        ABPB.index=pd.DatetimeIndex(ABPB.index)
        #print(ABPB)
        
        
        dffulldate=pd.DataFrame(index=pd.date_range(start=start_date,end=end_date)) 
        dffulldate.index.name='Date'
        merged = pd.merge(dffulldate, ABPB, on='Date', how='outer')
        merged.fillna(0, inplace=True)
        #merged['VolumeOriginM3'] = merged['VolumeOriginM3'].rolling(10).mean()
        #merged['shipping days'] = merged['shipping days'].rolling(10).mean()
        #print(merged)
        
        return ABPB
        

    
    def full_data(rolling_period, date_dict):
        #MA period
        rolling_period=30
        
        #date
        today = date_dict['today']
        delta = datetime.timedelta(days=1)
        start_date='2010-01-01' 
        current_year_start = date_dict['current_year_start']
        current_year_end = date_dict['current_year_end']
        next_year_end = date_dict['next_year_end']
        
        
        #Mt to Mcm/d
        
        Mt_to_Mcmd = 1397
        #read curveID
        #SupplyCurveId = pd.read_excel('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/DeskViewID.xlsx',header=(0),sheet_name='supply')
        #DemandCurveId = pd.read_excel('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/DeskViewID.xlsx',header=(0),sheet_name='demandwithRU')
        
        curveid = DBTOPD.get_curve_id()
        dfcurveid = curveid.loc[:,['CurveId','Type','Location','Country']]
        #print(dfcurveid)
        SupplyCurveId = dfcurveid.loc[dfcurveid[dfcurveid['Type']=='Supply'].index]
        SupplyCurveId = SupplyCurveId[['CurveId','Country','Location']]
        SupplyCurveId.rename(columns={'CurveId':'CurveID','Country':'Country','Location':'plant'}, inplace=True)
        SupplyCurveId.drop(SupplyCurveId[SupplyCurveId['plant']=='Arctic LNG 2'].index, inplace=True)
        SupplyCurveId.loc[SupplyCurveId[SupplyCurveId['Country']=='Mozambique'].index, 'plant'] = 'Coral South FLNG' #rename Mozambique
        SupplyCurveId.loc[SupplyCurveId[SupplyCurveId['plant']=='Portovaya LNG'].index, 'plant'] = 'Portovaya'

        DemandCurveId = dfcurveid.loc[dfcurveid[dfcurveid['Type']=='Demand'].index]
        DemandCurveId = DemandCurveId[['CurveId','Country','Location']]
        DemandCurveId.rename(columns={'CurveId':'CurveID','Country':'Country','Location':'plant'}, inplace=True)
        DemandCurveId.drop(DemandCurveId[DemandCurveId['Country']=='Russian Federation'].index, inplace=True)
        
        
        
        
        SupplyCategories=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'CategoriesSupply').sql_to_df()
        DemandCategories=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'CategoriesDemand').sql_to_df()
        drop_list_supply = ['Vysotsk','Kollsnes', 'Stavanger']
        for i in drop_list_supply:
            SupplyCategories.drop(SupplyCategories[SupplyCategories['Plant'] == i].index, inplace=True)
        #read Kplar data
        df_supply_plant, df_supply_country, df_demand, df_demand_regas, df_supply_eta, df_supply_plant_eta, df_demand_eta, dfabpb, dfreload = LNG_MA30.Kpler_data() 
        
        #create Kplar data for each country 
        dfsupply_plant = LNG_MA30.create_df_supply_MA_Plant(df_supply_plant, SupplyCurveId, rolling_period, start_date, today) 
        dfsupply_country = LNG_MA30.create_df_supply_MA_Country(dfsupply_plant, SupplyCurveId)
        
        dfdemand_country = LNG_MA30.create_df_demand_MA_County(df_demand, dfreload, DemandCurveId, rolling_period, start_date, today)
        
        dfsupplyMA_eta=LNG_MA30.create_df_etasupply_MA(df_supply_eta, SupplyCurveId,rolling_period, str(today+delta), next_year_end) 
        dfsupplyMA_eta_plant=LNG_MA30.create_df_etasupply_MA_plant(df_supply_plant_eta, SupplyCurveId,rolling_period, str(today+delta), next_year_end) 
        dfdemandMA_eta=LNG_MA30.create_df_etademand_MA(df_demand_eta, DemandCurveId,rolling_period, str(today+delta), next_year_end)
        #create desk view data for all countries
        desk_supply_plant_view = LNG_MA30.supply_desk_plant_view(SupplyCurveId, current_year_start, next_year_end, Mt_to_Mcmd) 
        desk_supply_country_view = LNG_MA30.supply_desk_country_view(SupplyCurveId,current_year_start, next_year_end,Mt_to_Mcmd)
        desk_demand_view = LNG_MA30.demand_desk_view(DemandCurveId, current_year_start, next_year_end, Mt_to_Mcmd)
        desk_supply_country_view = LNG_MA30.supply_desk_country_view(SupplyCurveId, current_year_start, next_year_end, Mt_to_Mcmd) 

        #create ihs full table
        ihscontractdemand, ihscontractsupply = LNG_MA30.IHScontract_data(current_year_start, next_year_end, SupplyCurveId) 
        
        supply_global_yoy, demand_global_yoy = LNG_MA30.globaldf(SupplyCurveId, DemandCurveId, dfsupply_country, dfdemand_country, dfsupplyMA_eta, dfdemandMA_eta, desk_supply_country_view, desk_demand_view, ihscontractdemand, ihscontractsupply )
        
        
        return SupplyCategories, DemandCategories, dfsupply_plant, dfsupply_country, dfdemand_country, desk_supply_plant_view, desk_supply_country_view, desk_demand_view, ihscontractdemand, ihscontractsupply , supply_global_yoy, demand_global_yoy, dfabpb,dfsupplyMA_eta,dfsupplyMA_eta_plant, dfdemandMA_eta

    def dftodb():  
        
        rolling_period=30
        mttomcmd = 1397
        #create dict of date
        date_dict={
                   'last_year_start' : str(datetime.date.today().year-1)+'-01-01',
                   'last_year_end' : str(datetime.date.today().year-1)+'-12-31',
                   'start_date' : '2010-01-01',
                   'end_date' : '2021-12-31',
                   'current_year_start' : str(datetime.date.today().year)+'-01-01',
                   'current_year_end' : str(datetime.date.today().year)+'-12-31',
                   'next_year_start':str(datetime.date.today().year+1)+'-01-01',
                   'next_year_end' : str(datetime.date.today().year+2)+'-12-31',  #go to 2024
                   'last_day' : datetime.date.today() - datetime.timedelta(days=1)*1,
                   'today' : datetime.date.today(),
                   'last_month': str(datetime.date.today().year)+'-'+str((datetime.date.today()-relativedelta(month=1)).month)
                   }
        
        file_path='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/Flow30/'
        #fulldata=LNG_MA10
        #get full data
        SupplyCategories, DemandCategories, dfsupply_plant, dfsupply_country, dfdemand_country, desk_supply_plant_view, desk_supply_country_view, desk_demand_view, ihscontractdemand, ihscontractsupply, supply_global_yoy, demand_global_yoy, dfabpb,dfsupplyMA_eta,dfsupplyMA_eta_plant, dfdemandMA_eta = LNG_MA30.full_data(rolling_period, date_dict)
        #print(SupplyCategories)
        supply_global_yoy.reset_index(inplace=True)
        demand_global_yoy.reset_index(inplace=True)
        #AB to PB
        #print(SupplyCategories)
        dfabpb = LNG_MA30.ABtoPB(dfabpb, SupplyCategories, DemandCategories, date_dict)
        dfabpb.reset_index(inplace=True)
        #get supply data
        df_basin_supply, df_cumulative_supply = LNG_MA30.Basin_data_supply(dfsupply_plant, desk_supply_plant_view, SupplyCategories, date_dict)
        #print(df_basin_supply.loc[date_dict['today']:,'APLNG Desk'])
        df_basin_supply.reset_index(inplace=True)
        df_cumulative_supply.reset_index(inplace=True)
        #demand
        df_basin_demand, df_cumulative_demand = LNG_MA30.Basin_data_demand(dfdemand_country, desk_demand_view, DemandCategories, date_dict)
        
        df_flex = LNG_MA30.Region_Flex(df_basin_demand, date_dict, DemandCategories, mttomcmd,  rolling_period, file_path)
        df_flex_mt, df_flex_cargo, EurCumBcm = LNG_MA30.NWE_cargo(df_basin_demand, df_cumulative_demand, date_dict, DemandCategories, 1397, file_path)
        df_basin_demand.reset_index(inplace=True)
        df_cumulative_demand.reset_index(inplace=True)
        EurCumBcm.reset_index(inplace=True)
        
        
        df_flex = pd.concat([df_flex, df_flex_mt,df_flex_cargo], axis=1)
        df_flex.reset_index(inplace=True)
        
        #print(df_flex)
        
        db_server_lng = "PRD-DB-SQL-211"
        db_name_lng = "LNG"
        sql_engine_lng = sqlalchemy.create_engine("mssql+pyodbc://" + db_server_lng + "/" + db_name_lng + "?trusted_connection=yes&driver=ODBC+Driver+13+for+SQL+Server")
        
        dfsupply_plant.to_sql(name='SupplyPlant30', con=sql_engine_lng, index=True, if_exists='replace', schema='ana')
        dfsupply_country.to_sql(name='SupplyCountry30', con=sql_engine_lng, index=True, if_exists='replace', schema='ana')
        dfdemand_country.to_sql(name='DemandCountry30', con=sql_engine_lng, index=True, if_exists='replace', schema='ana')
        df_basin_supply.to_sql(name='BasinSupply30', con=sql_engine_lng, index=False, if_exists='replace', schema='ana')
        df_cumulative_supply.to_sql(name='CumulativeSupply30', con=sql_engine_lng, index=False, if_exists='replace', schema='ana')
        df_basin_demand.to_sql(name='BasinDemand30', con=sql_engine_lng, index=False, if_exists='replace', schema='ana')
        df_cumulative_demand.to_sql(name='CumulativeDemand30', con=sql_engine_lng, index=False, if_exists='replace', schema='ana')
        df_flex.to_sql(name='EURflex30', con=sql_engine_lng, index=False, if_exists='replace', schema='ana')
        EurCumBcm.to_sql(name='EURflexCum30', con=sql_engine_lng, index=False, if_exists='replace', schema='ana')
        supply_global_yoy.to_sql(name='SupplyGlobal30', con=sql_engine_lng, index=False, if_exists='replace', schema='ana')
        demand_global_yoy.to_sql(name='DemandGlobal30', con=sql_engine_lng, index=False, if_exists='replace', schema='ana')
        #dfabpb.to_sql(name='ABtoPB', con=sql_engine_lng, index=False, if_exists='replace', schema='ana')
        #desk_supply_plant_view.to_sql(name='DeskSupplyPlant', con=sql_engine_lng, index=True, if_exists='replace', schema='ana')
        #desk_supply_country_view.to_sql(name='DeskSupplyCountry', con=sql_engine_lng, index=True, if_exists='replace', schema='ana')
        #desk_demand_view.to_sql(name='DeskDemand', con=sql_engine_lng, index=True, if_exists='replace', schema='ana')
        dfsupplyMA_eta.to_sql(name='SupplyCountryETA30', con=sql_engine_lng, index=True, if_exists='replace', schema='ana')
        dfsupplyMA_eta_plant.to_sql(name='SupplyPlantETA30', con=sql_engine_lng, index=True, if_exists='replace', schema='ana')
        dfdemandMA_eta.to_sql(name='DemandETA30', con=sql_engine_lng, index=True, if_exists='replace', schema='ana')
        #ihscontractdemand.to_sql(name='IHSContractdemand', con=sql_engine_lng, index=True, if_exists='replace', schema='ana')
        #ihscontractsupply.to_sql(name='IHSContractsupply', con=sql_engine_lng, index=True, if_exists='replace', schema='ana') 
        
        
LNG_MA30.dftodb()

'''

date_dict={
                   'last_year_start' : '2020-01-01',
                   'last_year_end' : '2020-12-31',
                   'start_date' : '2010-01-01',
                   'end_date' : '2021-12-31',
                   'current_year_start' : '2021-01-01',
                   'current_year_end' : '2021-12-31',
                   'last_day' : datetime.date.today() - datetime.timedelta(days=1)*1,
                   'today' : datetime.date.today(),
                   'last_month': str(datetime.date.today().year)+'-'+str(datetime.date.today().month-1)
                   }
a=LNG_MA10     
a.dftodb()

'''
#scheduler = BackgroundScheduler(job_defaults={'misfire_grace_time': 15*60})
#trigger = OrTrigger([CronTrigger(day_of_week='0-6', hour='06, 12, 16', minute='22')])
#scheduler.add_job(func=dftodb,trigger=trigger,id='LNGMA10')
#scheduler.start()
#runtime = datetime.datetime.now()
#print (scheduler.get_jobs(), runtime)
#scheduler.remove_job('LNGMA10') 
