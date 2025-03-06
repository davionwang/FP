# -*- coding: utf-8 -*-
"""
Created on Mon Mar  7 13:38:01 2022

@author: SVC-GASQuant2-Prod
"""

#V1 net demand of Kpler basin data




import sys
sys.path.append('C:\\Users\SVC-GASQuant2-Prod\YWwebsite\Python Scripts\class') 
from DBtoDF import DBTOPD
import datetime
import pandas as pd
import sqlalchemy


class KplerGroupData():
    
    def Kpler_data():
            
            #read data from Kpler
            Kpler=DBTOPD('PRD-DB-SQL-211','LNG', 'dbo', 'KplerLNGTrades')
            dfkpler=Kpler.sql_to_df()
            
            #get supply and demand df
            demand_columns=['StartDestination','CountryDestination','VolumeDestinationM3']
            supply_country_columns=['EndOrigin','CountryOrigin','VolumeOriginM3']

            df_demand = dfkpler[demand_columns]
                
            #reload
            dfreload = Kpler.get_reload_data()
            dfreload = dfreload[supply_country_columns+['CountryDestination']]
            for i in dfreload.index:
                if dfreload.loc[i,'CountryOrigin'] == dfreload.loc[i,'CountryDestination']:
                    dfreload.drop(i, inplace=True)
    
            return df_demand, dfreload

    def create_df_demand_County (df_demand, dfreload, DemandCurveId, start_date, end_date, cmtomt):
    
        DemandCurveId.drop_duplicates(inplace=True)
        demand_country_list = DemandCurveId['Country']
        demand_country_list.drop_duplicates(inplace=True)
        demand_country_list=demand_country_list.values.tolist()   
        #Actual data no MA
        dfact=pd.DataFrame(index=pd.date_range(start=start_date,end=end_date))
        dfact.index.name='Date'
        
        for i in demand_country_list:
            
            dffulldate=pd.DataFrame(index=pd.date_range(start=start_date,end=end_date)) 
            dffulldate.index.name='Date'
            dfcountry = df_demand[df_demand['CountryDestination']==i]
            dfcountry.loc[:,'StartDestination']=pd.to_datetime(dfcountry.loc[:,'StartDestination']).dt.strftime('%Y-%m-%d') 
            
            dfcountry=dfcountry.groupby(['StartDestination']).sum()*cmtomt 
            dfcountry.index.name='Date'
            dfcountry.index=pd.DatetimeIndex(dfcountry.index) 
            
            merged = pd.merge(dffulldate, dfcountry.loc[start_date: str(end_date)], on='Date', how='outer')
            merged.fillna(0, inplace=True)
            
            try:
                dfreloadcountry=dfreload[dfreload['CountryOrigin']==i]
                dfreloadcountry.loc[:,'EndOrigin']=pd.to_datetime(dfreloadcountry.loc[:,'EndOrigin']).dt.strftime('%Y-%m-%d') 
            
                dfreloadcountry=dfreloadcountry.groupby(['EndOrigin']).sum()*cmtomt
                dfreloadcountry.index.name='Date'
                dfreloadcountry.index=pd.DatetimeIndex(dfreloadcountry.index)
                dfreloadcountry.rename(columns={'VolumeOriginM3':'VolumeDestinationM3'}, inplace=True)
                dfreloadfull=pd.DataFrame(index=merged.index)
                dfreloadfull=pd.merge(dfreloadfull, dfreloadcountry.loc[start_date: str(end_date)], on='Date', how='outer')
                dfreloadfull.fillna(0, inplace=True)
                
                merged=merged-dfreloadfull
            
            except KeyError:
                continue
            
            dfact[i] = merged.loc[start_date: str(end_date)] #for no MA real data
        
        
        
        
        return dfact
    
    def Basin_data_demand(dfact, DemandCategories, date_dict):
        
        
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
        DemandCurveId = pd.read_excel('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/DeskViewID.xlsx',header=(0),sheet_name='demand')
        DemandCurveId=DemandCurveId[['CurveID','Country']]
        allcountry = DemandCurveId['Country'].values.tolist()
        
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
        
        #print(dfact)
        #yoy basin
        df_basin = pd.DataFrame(index=pd.date_range(start=date_dict['last_year_start'], end=date_dict['next_year_end']))
        df_basin.index.name='Date'
        df_basin['MENA'] =  dfact[MENA].loc[date_dict['last_year_start']:date_dict['next_year_end']].sum(axis=1)
    
        df_basin['PB'] =  dfact[PB].loc[date_dict['last_year_start']:date_dict['next_year_end']].sum(axis=1)
        
        df_basin['AB'] = dfact[AB].loc[date_dict['last_year_start']:date_dict['next_year_end']].sum(axis=1)
        
    

        df_basin['Global'] = df_basin[['AB', 'PB','MENA']].loc[date_dict['last_year_start']:date_dict['today']].sum(axis=1)
        
        df_basin['EoS'] = dfact[EoS].loc[date_dict['last_year_start']:date_dict['next_year_end']].sum(axis=1)
        df_basin['WoS'] = dfact[WoS].loc[date_dict['last_year_start']:date_dict['next_year_end']].sum(axis=1)
        
        #get region data
        regionlist = [JKTC, LatAm, MEIP, EurDesk, OtherEur, OtherRoW]
        #print(region1)
        region = pd.DataFrame(regionlist).T
        region.columns=['JKTC', 'LatAm', 'MEIP', 'EurDesk', 'OtherEur', 'OtherRoW']
        #print(region)
        #print(dfact['Taiwan'])
        for i in region.columns:
            #print(i)
            #print(region[i].dropna(axis=0))
            j=region[i].dropna(axis=0)
            #print(region[region.index(i)])
            df_basin[i] = dfact[j].loc[date_dict['last_year_start']:date_dict['next_year_end']].sum(axis=1)
            
        
        df_basin = df_basin.round(2)
        #print(df_basin)
        return df_basin
    
    
    def dftodb():
        
            
        cmtomt = 0.000000438060800930124
        #create dict of date
        date_dict={
                   'last_year_start' : str(datetime.date.today().year-1)+'-01-01',
                   'last_year_end' : str(datetime.date.today().year-1)+'-12-31',
                   'start_date' : '2010-01-01',
                   'end_date' : '2022-12-31',
                   'current_year_start' : str(datetime.date.today().year)+'-01-01',
                   'current_year_end' : str(datetime.date.today().year)+'-12-31',
                   'next_year_end' : str(datetime.date.today().year+1)+'-12-31',
                   'last_day' : datetime.date.today() - datetime.timedelta(days=1)*1,
                   'today' : datetime.date.today(),
                   'last_month': str(datetime.date.today().year)+'-'+str(datetime.date.today().month-1)
                   }
        
        
        
        
        #date
        #today = date_dict['today']
        #delta = datetime.timedelta(days=1)
        start_date='2010-01-01' 
        end_date = date_dict['last_year_end']
        #current_year_start = date_dict['current_year_start']
        #current_year_end = date_dict['current_year_end']
        #next_year_end = date_dict['next_year_end']
        #last_year_start=date_dict['last_year_start']
                
        #read curveID
        #SupplyCurveId = pd.read_excel('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/DeskViewID.xlsx',header=(0),sheet_name='supply')
        #DemandCurveId = pd.read_excel('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/DeskViewID.xlsx',header=(0),sheet_name='demandwithRU')
        
        curveid = DBTOPD.get_curve_id()
        dfcurveid = curveid.loc[:,['CurveId','Type','Location','Country']]
        
        DemandCurveId = dfcurveid.loc[dfcurveid[dfcurveid['Type']=='Demand'].index]
        DemandCurveId = DemandCurveId[['CurveId','Country','Location']]
        DemandCurveId.rename(columns={'CurveId':'CurveID','Country':'Country','Location':'plant'}, inplace=True)
        #print(DemandCurveId)
        
        SupplyCategories=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'CategoriesSupply').sql_to_df()
        DemandCategories=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'CategoriesDemand').sql_to_df()
        
        #print(end_date)
        
        df_demand, dfreload = KplerGroupData.Kpler_data()
        dfact = KplerGroupData.create_df_demand_County (df_demand, dfreload, DemandCurveId, start_date, end_date, cmtomt)
        df_basin = KplerGroupData.Basin_data_demand(dfact, DemandCategories, date_dict)
        
        #print(dfact)
        
        db_server_lng = "PRD-DB-SQL-211"
        db_name_lng = "LNG"
        sql_engine_lng = sqlalchemy.create_engine("mssql+pyodbc://" + db_server_lng + "/" + db_name_lng + "?trusted_connection=yes&driver=ODBC+Driver+13+for+SQL+Server")
        
        dfact.to_sql(name='KplerNetDemandCountryMt', con=sql_engine_lng, index=True, if_exists='replace', schema='ana')
        df_basin.to_sql(name='KplerNetDemandBasinMt', con=sql_engine_lng, index=True, if_exists='replace', schema='ana')
        
        
#KplerGroupData.dftodb()