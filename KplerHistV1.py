# -*- coding: utf-8 -*-
"""
Created on Tue May 30 11:51:51 2023

@author: SVC-GASQuant2-Prod
"""

# -*- coding: utf-8 -*-
"""
Created on Wed Dec  8 09:55:04 2021

@author: SVC-GASQuant2-Prod
"""


#V1 Fixed negative demand MA10 when reload is > demand. If demand = 0, reload >0, MA10 = 0, if demand >0, reload >0 and reload >demand, MA10 = demand.



import pandas as pd
import datetime
import numpy as np
import calendar
import sys
import sqlalchemy

sys.path.append('C:\\Users\SVC-GASQuant2-Prod\YWwebsite\Python Scripts\class') 
from DBtoDF import DBTOPD

pd.set_option('display.max_columns',10)

class Kpler_hist_data():
    
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

        #reload
        dfreload=Kpler.get_reload_data()
        dfreload = dfreload[supply_country_columns+['CountryDestination']]
        for i in dfreload.index:
            if dfreload.loc[i,'CountryOrigin'] == dfreload.loc[i,'CountryDestination']:
                dfreload.drop(i, inplace=True)

        return df_supply_plant, df_supply_country, df_demand, dfreload
    
    def create_df_supply_Plant (df_supply, SupplyCurveId): #use Supply curve id to get all plant supply
    
        #create supply list
        #SupplyCurveId.loc[SupplyCurveId[SupplyCurveId['Country']=='Mozambique'].index, 'plant'] = 'Mozambique Area 1' #rename Mozambique
        SupplyCurveId=SupplyCurveId[['CurveID','plant']]
        
        supply_plant_list=SupplyCurveId['plant'].values.tolist()
        
        #print(supply_plant_list)
        #rolling start date
        start_date='2019-01-01'
        today=datetime.date.today()
        dfMA=pd.DataFrame(index=pd.date_range(start=start_date,end=today))
        dfMA.index.name='Date'
        for i in supply_plant_list:
            
            dffulldate=pd.DataFrame(index=pd.date_range(start=start_date,end=today)) 
            dffulldate.index.name='Date'
            dfplant = df_supply[df_supply['InstallationOrigin']==i]
            dfplant.loc[:,'EndOrigin']=pd.to_datetime(dfplant.loc[:,'EndOrigin']).dt.strftime('%Y-%m-%d') 
            
            dfplant=dfplant.groupby(['EndOrigin']).sum()*0.000612
            dfplant.index.name='Date'
            dfplant.index=pd.DatetimeIndex(dfplant.index)
            
            merged = pd.merge(dffulldate, dfplant.loc[start_date: str(today)], on='Date', how='outer')
            merged.fillna(0, inplace=True)
            
            npMA=merged.loc[start_date: str(today)]
            dfMA[i]=npMA.loc[start_date:today].values
        
        #print(dfMA)
        
        return dfMA
        
    
    def create_df_supply_Country (dfsupply_plant, SupplyCurveId):
    
        #get market plant index, and list    
        SupplyCurveId.drop_duplicates(inplace=True)
        market_list=SupplyCurveId['Country']
        market_list.drop_duplicates(inplace=True)
        market_list=market_list.values.tolist()
        #print(dfsupply_plant)
        dfMA=pd.DataFrame(index=dfsupply_plant.index)
        
        #all market cumsum
        for market in market_list:
            #print(market)
            market_plants=SupplyCurveId['plant'].loc[SupplyCurveId[SupplyCurveId['Country'] == market].index.tolist()].values.tolist()
            #print(market_plants)
            dfMA[market] = dfsupply_plant[market_plants].sum(axis=1)

        return dfMA
    
    def create_df_demand_County (df_demand, dfreload, DemandCurveId):
    
        DemandCurveId.drop_duplicates(inplace=True)
        DemandCurveId.sort_values(by='Country', inplace=True)
        #print(DemandCurveId)
        demand_country_list = DemandCurveId['Country']
        
        #print(demand_country_list)
        demand_country_list.drop_duplicates(inplace=True)
        demand_country_list=demand_country_list.values.tolist() 
        #print(demand_country_list)
        start_date='2019-01-01' 
        today=datetime.date.today()
        dfMA=pd.DataFrame(index=pd.date_range(start=start_date,end=today))
        dfMA.index.name='Date'
        for i in demand_country_list:
            
            dffulldate=pd.DataFrame(index=pd.date_range(start=start_date,end=today)) 
            dffulldate.index.name='Date'
            dfcountry = df_demand[df_demand['CountryDestination']==i]
            dfcountry.loc[:,'StartDestination']=pd.to_datetime(dfcountry.loc[:,'StartDestination']).dt.strftime('%Y-%m-%d') 
            
            dfcountry=dfcountry.groupby(['StartDestination']).sum()*0.000612
            dfcountry.index.name='Date'
            dfcountry.index=pd.DatetimeIndex(dfcountry.index) 
            
            merged = pd.merge(dffulldate, dfcountry.loc[start_date: str(today)], on='Date', how='outer')
            merged.fillna(0, inplace=True)
            
            try:
                dfreloadcountry=dfreload[dfreload['CountryOrigin']==i]
                dfreloadcountry.loc[:,'EndOrigin']=pd.to_datetime(dfreloadcountry.loc[:,'EndOrigin']).dt.strftime('%Y-%m-%d') 
            
                dfreloadcountry=dfreloadcountry.groupby(['EndOrigin']).sum()*0.000612
                dfreloadcountry.index.name='Date'
                dfreloadcountry.index=pd.DatetimeIndex(dfreloadcountry.index)
                dfreloadcountry.rename(columns={'VolumeOriginM3':'VolumeDestinationM3'}, inplace=True)
                dfreloadfull=pd.DataFrame(index=merged.index)
                dfreloadfull=pd.merge(dfreloadfull, dfreloadcountry.loc[start_date: str(today)], on='Date', how='outer')
                dfreloadfull.fillna(0, inplace=True)
                #print(dfreloadfull)
                merged=merged-dfreloadfull
                #print(merged)
                #print(dfcountry)
                for j in merged.index:
                    #print(merged.loc[j])
                    if merged.loc[j,'VolumeDestinationM3'] < 0:
                        merged.loc[j,'VolumeDestinationM3'] = merged.loc[j,'VolumeDestinationM3'] + dfreloadfull.loc[j,'VolumeDestinationM3']
            
            except KeyError:
                continue
            
            npMA=merged.loc[start_date: str(today)]
            dfMA[i]=npMA.loc[start_date:today].values
        #print(dfMA.columns)
        #print(dfMA['Indonesia'].tail(20))
        
        return dfMA
    
    #supply//////////////////////////////////////////////////////////////////////////////////////////////
    def Basin_data_supply(dfsupplyMA, SupplyCategories, date_dict):
        
        SupplyCategories.set_index('Basin', inplace=True)
        
        #Basin plants list, and remove 
        PB = SupplyCategories['Plant'].loc['PB'].values.tolist()
        #PB.remove('Mozambique Area 1')
        AB = SupplyCategories['Plant'].loc['AB'].values.tolist()
        AB.remove('Calcasieu Pass')
        MENA = SupplyCategories['Plant'].loc['MENA_Bas'].values.tolist()
        
        SupplyCategories.set_index('Suez', inplace=True)
        EoS = SupplyCategories['Plant'].loc['EoS'].values.tolist()
        #EoS.remove('Mozambique Area 1')
        WoS = SupplyCategories['Plant'].loc['WoS'].values.tolist()
        WoS.remove('Calcasieu Pass')
        #print(EoS)
        
        allplant=SupplyCategories['Plant'].values.tolist()
        #allplant.remove('Mozambique Area 1')
        allplant.remove('Calcasieu Pass')
        
        
        #full time df
        df_fulltime = pd.DataFrame(index=pd.date_range(start=date_dict['last_year_start'], end=date_dict['next_year_end']))
        df_fulltime[['MENA','PB','AB']] = pd.concat([df_fulltime,
                                 dfsupplyMA[MENA].loc[date_dict['last_year_start']:date_dict['next_year_end']].sum(axis=1),
                                 dfsupplyMA[PB].loc[date_dict['last_year_start']:date_dict['next_year_end']].sum(axis=1),
                                 dfsupplyMA[AB].loc[date_dict['last_year_start']:date_dict['next_year_end']].sum(axis=1)], axis=1)
        
        #yoy basin
        df_basin = pd.DataFrame(index=pd.date_range(start=date_dict['last_year_start'], end=date_dict['next_year_end']))
        df_basin.index.name='Date'
        df_basin['MENA'] = df_fulltime['MENA'].values
        
        df_basin['PB'] = df_fulltime['PB'].values
        
        df_basin['AB'] = df_fulltime['AB'].values
        
        
        
        df_basin['Global'] = df_basin[['AB', 'PB','MENA']].loc[date_dict['last_year_start']:date_dict['today']].sum(axis=1)        

        
        df_basin['EoS'] = dfsupplyMA[EoS].loc[date_dict['last_year_start']:date_dict['next_year_end']].sum(axis=1)
        df_basin['WoS'] = dfsupplyMA[WoS].loc[date_dict['last_year_start']:date_dict['next_year_end']].sum(axis=1)
 
        
        df_basin = df_basin.round()
        #print(df_basin)
        
        return df_basin
      
    
    
    #Demand/////////////////////////////////////////////////////////////////////////////////////////////////////////////
    def Basin_data_demand(dfdemandMA, DemandCategories, date_dict):
        
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
        '''
        #use curve id instead of categories, no need move countries in categories
        curveid = DBTOPD.get_curve_id()
        dfcurveid = curveid.loc[:,['CurveId','Type','Location','Country']]
        DemandCurveId = dfcurveid.loc[dfcurveid[dfcurveid['Type']=='Demand'].index]
        DemandCurveId = DemandCurveId[['CurveId','Country','Location']]
        DemandCurveId.rename(columns={'CurveId':'CurveID','Country':'Country','Location':'plant'}, inplace=True)
        DemandCurveId.drop(DemandCurveId[DemandCurveId['Country']=='Russian Federation'].index, inplace=True)
        
        
        #DemandCurveId = pd.read_excel('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/DeskViewID.xlsx',header=(0),sheet_name='demand')
        #DemandCurveId=DemandCurveId[['CurveID','Country']]
        allcountry = DemandCurveId['Country'].values.tolist()
        '''
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
    
        df_basin['PB'] =  dfdemandMA[PB].loc[date_dict['last_year_start']:date_dict['next_year_end']].sum(axis=1)
        
        df_basin['AB'] = dfdemandMA[AB].loc[date_dict['last_year_start']:date_dict['next_year_end']].sum(axis=1)

        df_basin['Global'] = df_basin[['AB', 'PB','MENA']].loc[date_dict['last_year_start']:date_dict['today']].sum(axis=1)
        
        df_basin['EoS'] = dfdemandMA[EoS].loc[date_dict['last_year_start']:date_dict['next_year_end']].sum(axis=1)
        df_basin['WoS'] = dfdemandMA[WoS].loc[date_dict['last_year_start']:date_dict['next_year_end']].sum(axis=1)
        
        
        #get region data
        regionlist = [JKTC, LatAm, MEIP, EurDesk, OtherEur, OtherRoW]
        #print(region1)
        region = pd.DataFrame(regionlist).T
        region.columns=['JKTC', 'LatAm', 'MEIP', 'EurDesk', 'OtherEur', 'OtherRoW']
        #print(region)
        #print(dfdemandMA['Taiwan'])
        for i in region.columns:
            #print(i)
            j=region[i].dropna(axis=0)
            #print(region[region.index(i)])
            df_basin[i] = dfdemandMA[j].loc[date_dict['last_year_start']:date_dict['next_year_end']].sum(axis=1)
                    
           
        return df_basin
    
    
    
    
    
    def full_data():
       
        date_dict={
                   'last_year_start' : str(datetime.date.today().year-2)+'-01-01', #2years
                   'last_year_end' : str(datetime.date.today().year-1)+'-12-31',
                   'start_date' : '2010-01-01',
                   'end_date' : '2021-12-31',
                   'current_year_start' : str(datetime.date.today().year)+'-01-01',
                   'current_year_end' : str(datetime.date.today().year)+'-12-31',
                   'next_year_end' : str(datetime.date.today().year+1)+'-12-31',
                   'last_day' : datetime.date.today() - datetime.timedelta(days=1)*1,
                   'today' : datetime.date.today(),
                   'last_month': str(datetime.date.today().year)+'-'+str(datetime.date.today().month-1)
                   }
        
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
        SupplyCurveId.loc[SupplyCurveId[SupplyCurveId['Country']=='Mozambique'].index, 'plant'] = 'Coral South FLNG' #rename Mozambique
        SupplyCurveId.loc[SupplyCurveId[SupplyCurveId['plant']=='Portovaya LNG'].index, 'plant'] = 'Portovaya'
        #print(SupplyCurveId)
        
        DemandCurveId = dfcurveid.loc[dfcurveid[dfcurveid['Type']=='Demand'].index]
        DemandCurveId = DemandCurveId[['CurveId','Country','Location']]
        DemandCurveId.rename(columns={'CurveId':'CurveID','Country':'Country','Location':'plant'}, inplace=True)
        DemandCurveId.drop(DemandCurveId[DemandCurveId['Country']=='Russian Federation'].index, inplace=True)
        
        
        SupplyCategories=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'CategoriesSupply').sql_to_df()
        DemandCategories=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'CategoriesDemand').sql_to_df()
        #read Kplar data
        df_supply_plant, df_supply_country, df_demand, dfreload = Kpler_hist_data.Kpler_data() 
    
        #create Kplar data for each country 
        dfsupply_plant = Kpler_hist_data.create_df_supply_Plant (df_supply_plant, SupplyCurveId)
        dfsupply_country = Kpler_hist_data.create_df_supply_Country(dfsupply_plant, SupplyCurveId)
        dfdemand_country = Kpler_hist_data.create_df_demand_County(df_demand, dfreload, DemandCurveId)
        #print()
        
        dfbasinsupply = Kpler_hist_data.Basin_data_supply(dfsupply_plant, SupplyCategories, date_dict)
        dfbasindemand = Kpler_hist_data.Basin_data_demand(dfdemand_country, DemandCategories, date_dict)
        
        db_server_lng = "PRD-DB-SQL-211"
        db_name_lng = "LNG"
        sql_engine_lng = sqlalchemy.create_engine("mssql+pyodbc://" + db_server_lng + "/" + db_name_lng + "?trusted_connection=yes&driver=ODBC+Driver+13+for+SQL+Server")
        
        dfsupply_plant.to_sql(name='SupplyPlantHist', con=sql_engine_lng, index=True, if_exists='replace', schema='ana')
        dfsupply_country.to_sql(name='SupplyCountryHist', con=sql_engine_lng, index=True, if_exists='replace', schema='ana')
        dfdemand_country.to_sql(name='DemandCountryHist', con=sql_engine_lng, index=True, if_exists='replace', schema='ana')
        dfbasinsupply.to_sql(name='SupplyBasinHist', con=sql_engine_lng, index=True, if_exists='replace', schema='ana')
        dfbasindemand.to_sql(name='DemandBasinHist', con=sql_engine_lng, index=True, if_exists='replace', schema='ana')

        

        

#Kpler_hist_data.full_data()