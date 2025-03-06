# -*- coding: utf-8 -*-
"""
Created on Wed May 19 12:58:54 2021

@author: ywang1
"""

import pyodbc
import pandas as pd
import datetime
from pandas.tseries.offsets import BDay
from dateutil.relativedelta import relativedelta


class DBTOPD(object):
    
    #which Database and folder
    dbserverScrape = ''
    dbnameScrape = ''
    tabletitle = ''
    tablename = ''
    #RIClist = ''
    
    #ini input
    def __init__(self,dbserverScrape, dbnameScrape, tabletitle, tablename):
        self.dbserverScrape = dbserverScrape
        self.dbnameScrape = dbnameScrape
        self.tabletitle = tabletitle
        self.tablename = tablename
        #self.RIClist = RIClist
        
    #output pandas df
    def sql_to_df(self):
        """
        'PRD-DB-SQL-211','LNG', 'ana', 'tablename'
        """
        
        sqlConnScrape = pyodbc.connect('DRIVER={SQL SERVER};SERVER=' + self.dbserverScrape + ';Trusted_Connection=yes')
        
        sql = '''
                select * from {database}.{tabletitle}.{table} 
    
              '''.format(database = self.dbnameScrape,
                          tabletitle = self.tabletitle,
                          table = self.tablename)

        df = pd.read_sql(sql, sqlConnScrape)
        return df
    
    def get_curve_id():
        sqlConnScrape = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER=prd-db-sql-209;Trusted_Connection=yes')
        
        sql = '''
               select c.*
                from AnalyticsModel.aux.CurveDetail c
                where c.Source = 'swasti'
                		and c.Commodity = 'LNG'
                		and c.Type in ('Supply', 'Demand')
                		and c.Scenario = 'Base'
    
              '''

        df = pd.read_sql(sql, sqlConnScrape)
        
        
        df.loc[df[df['Country']=='Mozambique'].index, 'Location'] = 'Coral South FLNG' #rename Mozambique
        df.loc[df[df['Location']=='Portovaya LNG'].index, 'Location'] = 'Portovaya'
        #print('index:',df[(df['CurveId']==59333) | (df['CurveId']==67646) | (df['CurveId']==67647) | (df['CurveId']==67648)| (df['CurveId']==67644)| (df['CurveId']==67645)].index)
        df.drop(index = df[(df['CurveId']==59333) | (df['CurveId']==67646) | (df['CurveId']==67647) | (df['CurveId']==67648)| (df['CurveId']==67644)| (df['CurveId']==67645)].index, inplace=True)
        #print(df)
        
        return df
    
    def desksupply_to_df(self):
        sqlConnScrape = pyodbc.connect('DRIVER={SQL SERVER};SERVER=' + self.dbserverScrape + ';Trusted_Connection=yes')
        
        curveid = DBTOPD.get_curve_id()
        dfcurveid = curveid.loc[:,['CurveId','Type','Location','Country']]
        
        SupplyCurveId = dfcurveid.loc[dfcurveid[dfcurveid['Type']=='Supply'].index]
        SupplyCurveId = SupplyCurveId[['CurveId','Country','Location']]
        #print(SupplyCurveId)
        list_curveId = SupplyCurveId['CurveId'].to_list()
        
        tuple_curveId = tuple(list_curveId)
        
        
        
        #print(list_curveId)
        sql = '''
                select CurveId, ValueDate, Value
                from {database}.{tabletitle}.{table} 
                where CurveId in {listids}
    
              '''.format(database = self.dbnameScrape,
                          tabletitle = self.tabletitle,
                          table = self.tablename,
                          listids = tuple_curveId
                          )

        df = pd.read_sql(sql, sqlConnScrape)
        #print(df)
        return df
    
    def deskdemand_to_df(self):
        sqlConnScrape = pyodbc.connect('DRIVER={SQL SERVER};SERVER=' + self.dbserverScrape + ';Trusted_Connection=yes')
        
        sql = '''
                select CurveId, ValueDate, Value
                from {database}.{tabletitle}.{table} 
                where CurveId >= 43414 and CurveId <= 43466 or CurveId = 67202 or CurveId = 67203
    
              '''.format(database = self.dbnameScrape,
                          tabletitle = self.tabletitle,
                          table = self.tablename
                          )

        df = pd.read_sql(sql, sqlConnScrape)
        return df
        
    def lastdeskdemand_to_df(self):
        sqlConnScrape = pyodbc.connect('DRIVER={SQL SERVER};SERVER=' + self.dbserverScrape + ';Trusted_Connection=yes')
        
        sql = '''
                select CurveId, ForecastDate, ValueDate, Value
                from {database}.{tabletitle}.{table} 
                where CurveId >= 43414 and CurveId <= 43466 or CurveId = 67202 or CurveId = 67203
    
              '''.format(database = self.dbnameScrape,
                          tabletitle = self.tabletitle,
                          table = self.tablename
                          )

        df = pd.read_sql(sql, sqlConnScrape)
        return df
    
    
    def get_eta_today(self):
        today=datetime.date.today()
        rundate_start=today-datetime.timedelta(days=0)
        rundate_start="'"+rundate_start.strftime('%Y-%m-%d')+"'"
        sqlConnScrape = pyodbc.connect('DRIVER={SQL SERVER};SERVER=' + self.dbserverScrape + ';Trusted_Connection=yes')
        
        """sql = '''
                select * from {database}.{tabletitle}.{table} 
                
                where RunDate >= cast({getdate} as date)
    
              '''.format(database = self.dbnameScrape,
                          tabletitle = self.tabletitle,
                          table = self.tablename,
                          getdate = rundate_start
                          )"""
        sql = '''
                select * from {database}.{tabletitle}.{table} 
                
    
              '''.format(database = self.dbnameScrape,
                          tabletitle = self.tabletitle,
                          table = self.tablename
                          )
    
        df = pd.read_sql(sql, sqlConnScrape)
        return df
    
    def get_eta_supply(self):
        
        '''
        #Kpler=DBTOPD('TST-DB-SQL-208','Scrape', 'dbo', 'KplerLNGTradesForecasViewByDay') old DB
        Kpler=DBTOPD('PRD-DB-SQL-211','LNG', 'dbo', 'KplerLNGTrades') #new DB 211
        dfkpler=Kpler.sql_to_df()
        
        #get supply and demand df
        supply_columns=['EndOrigin','CountryOrigin','VolumeOriginM3']
        df_supply = dfkpler[supply_columns]
        #print(df_supply)
        today=datetime.date.today()
        #KplerETA=DBTOPD('TST-DB-SQL-208','Scrape', 'dbo', 'KplerLNGTradesForecasViewByDay') #old DB
        KplerETA=DBTOPD('PRD-DB-SQL-211','LNG', 'dbo', 'KplerLNGTrades') #new DB 211
        dfkplerETA=KplerETA.get_eta_today()
        
        #eta_supply_columns=['RunDate','EtaOrigin','ForecastedOriginETA','CountryOrigin','VolumeOriginM3','StartOrigin','EndOrigin'] old
        eta_supply_columns=['EtaOrigin','ForecastedOriginETA','CountryOrigin','VolumeOriginM3','StartOrigin','EndOrigin'] #new

        supply_eta = dfkplerETA[eta_supply_columns]
        #delete endorigin not null and before today startorigin is null. 
        supply_eta = supply_eta.drop(supply_eta[(supply_eta['EndOrigin'].notnull()) | (supply_eta['StartOrigin'].loc[:today.strftime('%Y-%m-%d')].isna())].index)
        #remove etaorigin and fcstorigin <=today
        supply_eta = supply_eta.drop(supply_eta[(supply_eta['EtaOrigin']<=pd.to_datetime('today')) | (supply_eta['ForecastedOriginETA']<=pd.to_datetime('today'))].index)
        #supply_eta.set_index('RunDate',inplace=True)
        
        supplybydate = supply_eta#.loc[today:]
        
        EtaOrigin=supplybydate[['EtaOrigin','CountryOrigin','VolumeOriginM3']]
        EtaOrigin.set_index('EtaOrigin',inplace=True)
        ForecastedOriginETA=supplybydate[['ForecastedOriginETA','CountryOrigin','VolumeOriginM3']]
        ForecastedOriginETA.set_index('ForecastedOriginETA',inplace=True)
        df_supply_eta=pd.concat([EtaOrigin,ForecastedOriginETA])
        df_supply_eta.reset_index(inplace=True)
        df_supply_eta.columns = ['Date','CountryOrigin','VolumeOriginM3']
        df_supply_eta = df_supply_eta[pd.notnull(df_supply_eta['Date'])]
        df_supply_eta = df_supply_eta.sort_values(by = 'Date')
        df_supply_eta.set_index('Date', inplace=True)
        df_supply_eta = df_supply_eta.loc[today:]
        df_supply_eta.reset_index(inplace=True)
        df_supply_eta.columns = ['Date','CountryOrigin','VolumeOriginM3']
        etasupplystart = df_supply
        etasupplystart = etasupplystart.sort_values(by = 'EndOrigin')
        etasupplystart.set_index('EndOrigin', inplace=True)
        #print(etasupplystart)
        etasupplystart = etasupplystart.loc[:today.strftime('%Y-%m-%d')]
        etasupplystart.reset_index(inplace=True)
        etasupplystart.columns=['Date','CountryOrigin','VolumeOriginM3']
        df_supply_eta = pd.concat([etasupplystart,df_supply_eta])
        '''
        #get hist 
        today = datetime.datetime.today()
        Kpler=DBTOPD('PRD-DB-SQL-211','LNG', 'dbo', 'KplerLNGTrades') #new DB
        dfkpler=Kpler.sql_to_df()
        #get supply and demand df
        supply_columns = ['EndOrigin','CountryOrigin','VolumeOriginM3']
        #df_supply = df_supply.sort_values(by = 'EndOrigin')
        df_supply = dfkpler[supply_columns]
        etasupplystart = df_supply.copy()
        etasupplystart = etasupplystart.sort_values(by = 'EndOrigin')
        etasupplystart.set_index('EndOrigin', inplace=True)
        etasupplystart = etasupplystart.loc[:today.strftime('%Y-%m-%d')]
        etasupplystart.reset_index(inplace=True)
        etasupplystart.columns=['Date','CountryOrigin','VolumeOriginM3']
        #print(etademandstart)
        supply_columns_eta=['EtaOrigin','CountryOrigin','VolumeOriginM3']
        #df_supply = df_supply.sort_values(by = 'EndOrigin')
        df_supply_eta = dfkpler[supply_columns_eta]
        df_supply_eta = df_supply_eta.sort_values(by = 'EtaOrigin')
        df_supply_eta.set_index('EtaOrigin', inplace=True)
        df_supply_eta = df_supply_eta.loc[(today+relativedelta(days=1)).strftime('%Y-%m-%d'):]
        df_supply_eta.reset_index(inplace=True)
        df_supply_eta.columns=['Date','CountryOrigin','VolumeOriginM3']
        #print(df_demand_eta)
        df_supply_eta = pd.concat([etasupplystart,df_supply_eta])
        
        
        return df_supply_eta
    
    
    def get_eta_supply_plant(self):
        
        #get hist 
        today = datetime.datetime.today()
        Kpler=DBTOPD('PRD-DB-SQL-211','LNG', 'dbo', 'KplerLNGTrades') #new DB
        dfkpler=Kpler.sql_to_df()
        #get supply and demand df
        supply_columns = ['EndOrigin','InstallationOrigin','VolumeOriginM3']
        #df_supply = df_supply.sort_values(by = 'EndOrigin')
        df_supply = dfkpler[supply_columns]
        etasupplystart = df_supply.copy()
        etasupplystart = etasupplystart.sort_values(by = 'EndOrigin')
        etasupplystart.set_index('EndOrigin', inplace=True)
        etasupplystart = etasupplystart.loc[:today.strftime('%Y-%m-%d')]
        etasupplystart.reset_index(inplace=True)
        etasupplystart.columns=['Date','InstallationOrigin','VolumeOriginM3']
        #print(etademandstart)
        supply_columns_eta=['EtaOrigin','InstallationOrigin','VolumeOriginM3']
        #df_supply = df_supply.sort_values(by = 'EndOrigin')
        df_supply_eta = dfkpler[supply_columns_eta]
        df_supply_eta = df_supply_eta.sort_values(by = 'EtaOrigin')
        df_supply_eta.set_index('EtaOrigin', inplace=True)
        df_supply_eta = df_supply_eta.loc[(today+relativedelta(days=1)).strftime('%Y-%m-%d'):]
        df_supply_eta.reset_index(inplace=True)
        df_supply_eta.columns=['Date','InstallationOrigin','VolumeOriginM3']
        #print(df_demand_eta)
        df_supply_eta_plant = pd.concat([etasupplystart,df_supply_eta])
        
        
        return df_supply_eta_plant


    def get_eta_demand(self):
          
        '''
        #old eta using forecastdestination        
        Kpler=DBTOPD('PRD-DB-SQL-211','LNG', 'dbo', 'KplerLNGTrades') #new DB
        dfkpler=Kpler.sql_to_df()
        
        #get supply and demand df
        demand_columns=['StartDestination','CountryDestination','VolumeDestinationM3']
        #df_supply = df_supply.sort_values(by = 'EndOrigin')
        df_demand = dfkpler[demand_columns]
        
        today=datetime.date.today()
        #KplerETA=DBTOPD('TST-DB-SQL-208','Scrape', 'dbo', 'KplerLNGTradesForecasViewByDay') #old DB
        KplerETA=DBTOPD('PRD-DB-SQL-211','LNG', 'dbo', 'KplerLNGTrades') #new DB211
        dfkplerETA=KplerETA.get_eta_today()
        #eta_demand_columns=['RunDate','ForecastedETA','EtaDestination','ForecastedDestination','VolumeDestinationM3'] old
        eta_demand_columns=['ForecastedETA','EtaDestination','ForecastedDestination','VolumeDestinationM3'] #new 211

        demand_eta = dfkplerETA[eta_demand_columns]
        #demand_eta.set_index('RunDate',inplace=True)
        demandbydate = demand_eta.loc[today.strftime('%Y-%m-%d'):]
        ForecastedETA=demandbydate[['ForecastedETA','ForecastedDestination','VolumeDestinationM3']]
        ForecastedETA.set_index('ForecastedETA',inplace=True)
        EtaDestination=demandbydate[['EtaDestination','ForecastedDestination','VolumeDestinationM3']]
        EtaDestination.set_index('EtaDestination',inplace=True)
        df_demand_eta=pd.concat([ForecastedETA,EtaDestination])
        df_demand_eta.reset_index(inplace=True)
        df_demand_eta.columns = ['Date','ForecastedDestination','VolumeDestinationM3']
        df_demand_eta = df_demand_eta[pd.notnull(df_demand_eta['Date'])]
        df_demand_eta = df_demand_eta.sort_values(by = 'Date')
        df_demand_eta.set_index('Date', inplace=True)
        df_demand_eta = df_demand_eta.loc[today:]
        df_demand_eta.reset_index(inplace=True)
        df_demand_eta.columns = ['Date','ForecastedDestination','VolumeDestinationM3']
        etademandstart = df_demand
        etademandstart = etademandstart.sort_values(by = 'StartDestination')
        etademandstart.set_index('StartDestination', inplace=True)
        etademandstart = etademandstart.loc[:today.strftime('%Y-%m-%d')]
        etademandstart.reset_index(inplace=True)
        etademandstart.columns=['Date','ForecastedDestination','VolumeDestinationM3']
        #print(etademandstart)
        df_demand_eta = pd.concat([etademandstart,df_demand_eta])
        '''
        '''
        demand=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'DemandCountryHist')
        dfDemand=demand.sql_to_df()
        dfDemand.set_index('Date', inplace=True)
        column=dfDemand.columns.tolist()
        today = datetime.datetime.today()
        #get ETA data
        KplerETA=DBTOPD('PRD-DB-SQL-211','LNG', 'dbo', 'KplerLNGTrades') #new DB 211
        dfkplerETA=KplerETA.get_eta_today()
        eta_demand_columns=['EtaDestination','CountryDestination','VolumeOriginM3'] #new
        demand_eta = dfkplerETA[eta_demand_columns]
        dz=demand_eta.groupby(['EtaDestination', 'CountryDestination'],as_index=False).sum()
        da=pd.pivot(dz, index='EtaDestination', columns='CountryDestination')
        da.columns=da.columns.droplevel(0)
        da=da.resample('D').sum()
        dfeta=pd.DataFrame(index=pd.date_range(start=today+datetime.timedelta(1),periods=10).date, columns=column)
        for i in da.columns:
            dfeta.loc[:,i]=da.loc[:,i]
        dfeta=dfeta.iloc[::-1]
        dfeta=dfeta[column]
        #dfeta=dfeta*0.000612027
        dfeta=round(dfeta,2)
        dfeta.reset_index(inplace=True)
        '''
        #print(dfeta)
        #get hist 
        today = datetime.datetime.today()
        Kpler=DBTOPD('PRD-DB-SQL-211','LNG', 'dbo', 'KplerLNGTrades') #new DB
        dfkpler=Kpler.sql_to_df()
        #get supply and demand df
        demand_columns=['StartDestination','CountryDestination','VolumeDestinationM3']
        #df_supply = df_supply.sort_values(by = 'EndOrigin')
        df_demand = dfkpler[demand_columns]
        etademandstart = df_demand.copy()
        etademandstart = etademandstart.sort_values(by = 'StartDestination')
        etademandstart.set_index('StartDestination', inplace=True)
        etademandstart = etademandstart.loc[:today.strftime('%Y-%m-%d')]
        etademandstart.reset_index(inplace=True)
        etademandstart.columns=['Date','ForecastedDestination','VolumeDestinationM3']
        #print(etademandstart)
        demand_columns_eta=['EtaDestination','CountryDestination','VolumeDestinationM3']
        #df_supply = df_supply.sort_values(by = 'EndOrigin')
        df_demand_eta = dfkpler[demand_columns_eta]
        df_demand_eta = df_demand_eta.sort_values(by = 'EtaDestination')
        df_demand_eta.set_index('EtaDestination', inplace=True)
        df_demand_eta = df_demand_eta.loc[(today+relativedelta(days=1)).strftime('%Y-%m-%d'):]
        df_demand_eta.reset_index(inplace=True)
        df_demand_eta.columns=['Date','ForecastedDestination','VolumeDestinationM3']
        #print(df_demand_eta)
        df_demand_eta = pd.concat([etademandstart,df_demand_eta])
        #print(df_demand_eta)
    
        return df_demand_eta
    
    # z=df_demand_eta[df_demand_eta['ForecastedDestination'] == 'China']
    # z['Date'] = z['Date'].dt.normalize()
    # zz=(z.set_index('Date').resample('d').sum() / 612 / 30).rolling(10).mean()
    
    def get_platts_index(self):
        
        sqlConnScrape = pyodbc.connect('DRIVER={SQL SERVER};SERVER=' + self.dbserverScrape + ';Trusted_Connection=yes')
        
        sql =  '''
                select * from {database}.{tabletitle}.{table} 
                where RIC = 'AAOVQ00'
                          order by [Timestamp]
              '''.format(database = self.dbnameScrape,
                          tabletitle = self.tabletitle,
                          table = self.tablename)
              
        df = pd.read_sql(sql, sqlConnScrape)
        return df
    
    def get_freight(self):
        sqlConnScrape = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER=' + self.dbserverScrape + ';Trusted_Connection=yes')
        
        sql = '''
                select * from {database}.{tabletitle}.{table} r
                where r.RIC in ('AARXT00', 'AASYC00')
                order by r.[Timestamp]
              '''.format(database = self.dbnameScrape,
                          tabletitle = self.tabletitle,
                          table = self.tablename)

        df = pd.read_sql(sql, sqlConnScrape)
        return df
    
    def get_freight_spark(self):
        sqlConnScrape = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER=' + self.dbserverScrape + ';Trusted_Connection=yes')
        
        sql = '''
                select al.*, c.* 
                from AnalyticsModel.ts.AnalyticsLatest al 
                inner join 
                	(	select al.CurveId, max(al.ValueDate) as MaxValueDate
                		from AnalyticsModel.ts.AnalyticsLatest al
                		where al.CurveId in (52744,52756,52768,52780,52792,52804,52816,52828,52840,52852,52864,52876) 
                		group by al.CurveId
                	) z
                on al.CurveId = z.CurveId and al.ValueDate = z.MaxValueDate 
                inner join AnalyticsModel.aux.CurveDetail c 
                on al.CurveId = c.CurveId 
                order by al.CurveId
              '''.format(database = self.dbnameScrape,
                          tabletitle = self.tabletitle,
                          table = self.tablename)

        df = pd.read_sql(sql, sqlConnScrape)
        return df
    
    def get_price(self):
        sqlConnScrape = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER=' + self.dbserverScrape + ';Trusted_Connection=yes')
        
        sql = '''
                select * from {database}.{tabletitle}.{table} r
                where r.RIC in ('TRNLTTFMc1','TRNLTTFMc2')
              '''.format(database = self.dbnameScrape,
                          tabletitle = self.tabletitle,
                          table = self.tablename)

        df = pd.read_sql(sql, sqlConnScrape)
        return df
    
    def get_suez(self):
        
        sqlConnScrape = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER=' + self.dbserverScrape + ';Trusted_Connection=yes')
        sql='''
               SELECT k.InstallationOrigin, k.CountryDestination, m.Suez, datepart(month, k.StartDestination) as MonthDelivered, round(avg(k.VolumeDestinationM3), 2) as AvgVolDelivered, count(k.VolumeDestinationM3) as NumShips, round(sum(k.VolumeDestinationM3), 2) as SumVolDelivered
                FROM {database}.{tabletitle}.{table} k
                inner join LNG.[dbo].[RefKplerMappingsRegions] m
                on k.CountryDestination = m.Country
                where	k.StartDestination is not null
                		and k.CountryDestination <> ''
                		and (
                		(k.InstallationOrigin in ('Atlantic LNG','Bethioua','Elba Island Liq.', 'Cameroon FLNG Terminal','Cameron (Liqu.)', 'Corpus Christi' , 'Cove Point', 'Freeport', 'Sabine Pass', 'Yamal', 'Idku'  ) and k.StartDestination >= '2018-01-01')
                		or (k.InstallationOrigin in ('Bonny LNG', 'Bioko', 'Soyo', 'Skikda', 'Qalhat', 'Das Island' ) and k.StartDestination >= '2015-01-01')
                		or (k.InstallationOrigin in ('Atlantic LNG', 'Snohvit', 'Ras Laffan', 'Damietta', 'Marsa El Brega' ))
                		)
                		and m.Country is not null
                		and m.Country <> ''
                group by k.InstallationOrigin, k.CountryDestination, m.Suez, datepart(month, k.StartDestination)
                order by k.InstallationOrigin, k.CountryDestination, MonthDelivered, m.Suez
            '''.format(database = self.dbnameScrape,
                        tabletitle = self.tabletitle,
                        table = self.tablename)
    
        df = pd.read_sql(sql, sqlConnScrape)
        return df
    
    def get_dash_hist(self):
        
        sqlConnScrape = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER=' + self.dbserverScrape + ';Trusted_Connection=yes')
        sql='''
                select k.InstallationOrigin, k.CountryDestination, datepart(YEAR, k.StartDestination) as YearDelivered, datepart(month, k.StartDestination) as MonthDelivered, round(avg(k.VolumeDestinationM3), 2) as AvgVolDelivered, count(k.VolumeDestinationM3) as NumShips, round(sum(k.VolumeDestinationM3), 2) as SumVolDelivered
                from {database}.{tabletitle}.{table} k
                where k.StartDestination is not null
                and k.CountryDestination <> ''
                and (
                	(k.InstallationOrigin in ('Atlantic LNG','Bethioua','Elba Island Liq.', 'Cameroon FLNG Terminal','Cameron (Liqu.)', 'Corpus Christi' , 'Cove Point', 'Freeport', 'Sabine Pass', 'Yamal', 'Idku'  ) and k.StartDestination >= '2018-01-01')
                	or (k.InstallationOrigin in ('Bonny LNG', 'Bioko', 'Soyo', 'Skikda', 'Qalhat', 'Das Island' ) and k.StartDestination >= '2015-01-01')
                	or (k.InstallationOrigin in ('Atlantic LNG', 'Snohvit', 'Ras Laffan', 'Damietta', 'Marsa El Brega' ))
                )
                group by k.InstallationOrigin, k.CountryDestination, datepart(YEAR, k.StartDestination), datepart(month, k.StartDestination)
                order by k.CountryDestination, k.InstallationOrigin, YearDelivered, MonthDelivered
            '''.format(database = self.dbnameScrape,
                        tabletitle = self.tabletitle,
                        table = self.tablename)
    
        df = pd.read_sql(sql, sqlConnScrape)
        return df
    
    def get_curve(self):
        
        today=datetime.date.today()
        rundate_start=today-datetime.timedelta(days=7)
        
        datelist=pd.bdate_range(rundate_start, today,freq='B')
        rundate_start="'"+datelist[-2].strftime('%Y-%m-%d')+"'"
        #print(datelist)
        sqlConnScrape = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER=' + self.dbserverScrape + ';Trusted_Connection=yes')
        
        sql = '''
                select * from {database}.{tabletitle}.{table}
                where CURVE_ID in ('TTF FWD','JKM FWD','EUA FWD','ICE BRENT FWD') and CURVE_DT between '2020-11-01' and '2023-12-01' and COB_DT >= cast({getdate} as date)
              '''.format(database = self.dbnameScrape,
                          tabletitle = self.tabletitle,
                          table = self.tablename,
                          getdate = rundate_start
                          )

        df = pd.read_sql(sql, sqlConnScrape)
        return df
    
    def get_jodi_data(self):
        
        sqlConnScrape = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER=' + self.dbserverScrape + ';Trusted_Connection=yes')
        
        sql = '''
                select * from {database}.{tabletitle}.{table} j
                where j.REF_AREA in ('KR', 'JP', 'CN')
                and j.FLOW_BREAKDOWN = 'CLOSTLV'
                and j.UNIT_MEASURE = 'Million Standard Cubic Metres'
                order by j.TIME_PERIOD desc, JODI_COUNTRY
              '''.format(database = self.dbnameScrape,
                        tabletitle = self.tabletitle,
                        table = self.tablename)

        df = pd.read_sql(sql, sqlConnScrape)
        return df
        
    def get_reload_data(self):
        
        sqlConnScrape = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER=' + self.dbserverScrape + ';Trusted_Connection=yes')
        
        sql = '''
                select *
                from   {database}.{tabletitle}.{table} t
                where 
              (
                     (t.ReloadSTSPartialOrigin is not null and t.ReloadSTSPartialOrigin not in ('Partial'))
                     or
                     (t.ReloadSTSPartialDestination is not null and t.ReloadSTSPartialDestination not in ('Partial'))
              )
              
              order by t.EndOrigin desc

              '''.format(database = self.dbnameScrape,
                        tabletitle = self.tabletitle,
                        table = self.tablename)

        df = pd.read_sql(sql, sqlConnScrape)
        return df
        
    def get_temp_data(self):
        
        sqlConnScrape = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER=' + self.dbserverScrape + ';Trusted_Connection=yes')
        
        sql = '''
                select r.synthetic_curve_id, r.continent, r.country, r.wmo_id, r.wmo_weighting_type, r.wmo_number, r.wmo_weighting, m.parameter, m.model_source, cast(m.value_date as date) as value_date, avg(m.value) as average_value
                from {database}.{tabletitle}.{table} m
                inner join Meteorology.dbo.RefInternalMeteomaticCoordinatesLNGMarketsCurves r
                on m.synthetic_curve_id = r.synthetic_curve_id
                where m.IsLatest = 1
                	and m.model_source in ('ecmwf-ens', 'ecmwf-mmsf', 'ecmwf-vareps')
                	and m.parameter = 't_2m:C'
                	and m.value not in (-666, -777, -888, -999)
                group by r.synthetic_curve_id, r.continent, r.country, r.wmo_id, r.wmo_weighting_type, r.wmo_number, r.wmo_weighting, m.parameter, m.model_source, cast(m.value_date as date)
                order by r.synthetic_curve_id, r.continent, r.country, r.wmo_id, r.wmo_weighting_type, r.wmo_number, r.wmo_weighting, m.parameter, m.model_source, value_date
                 

              '''.format(database = self.dbnameScrape,
                        tabletitle = self.tabletitle,
                        table = self.tablename)

        df = pd.read_sql(sql, sqlConnScrape)
        return df
    
    def get_temp_data_last(self):
        
        
        sqlConnScrape = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER=' + self.dbserverScrape + ';Trusted_Connection=yes')
        
        sql = '''
                select r.synthetic_curve_id, r.continent, r.country, r.wmo_id, r.wmo_weighting_type, r.wmo_number, r.wmo_weighting, m.parameter, m.model_source, cast(m.value_date as date) as [value_date], avg(m.value) as [average_value]
                from {database}.{tabletitle}.{table} m
                inner join
                (
                       select m.synthetic_curve_id, m.model_source, m.parameter, m.value_date, max(m.forecast_date) as MaxFDBeforeLatest
                       from Meteorology.dbo.MeteomaticsPointDataTimeseries m
                       inner join
                       (
                              select z.model_source, z.forecast_date, z.row_num
                              from (
                                     select m.model_source, m.forecast_date, ROW_NUMBER() over (partition by m.model_source order by m.forecast_date desc) as row_num
                                     from   Meteorology.dbo.MeteomaticsPointDataTimeseries m
                                     where  m.model_source in ('ecmwf-ens', 'ecmwf-mmsf', 'ecmwf-vareps')
                                                  and m.parameter = 't_2m:C'
                                                  and m.value not in (-666, -777, -888, -999)
                                     group by m.model_source, m.forecast_date
                              ) z
                              where z.row_num = 2
                       ) z
                       on m.model_source = z.model_source and m.forecast_date <= z.forecast_date
                       where  m.model_source in ('ecmwf-ens', 'ecmwf-mmsf', 'ecmwf-vareps')
                                     and m.parameter = 't_2m:C'
                                     and m.value not in (-666, -777, -888, -999)
                       group by m.synthetic_curve_id, m.model_source, m.parameter, m.value_date
                       --order by m.synthetic_curve_id, m.model_source, m.parameter, m.value_date
                ) z
                on m.synthetic_curve_id = z.synthetic_curve_id and m.model_source = z.model_source and m.parameter = z.parameter and m.value_date = z.value_date and m.forecast_date = z.MaxFDBeforeLatest
                inner join Meteorology.dbo.RefInternalMeteomaticCoordinatesLNGMarketsCurves r
                on m.synthetic_curve_id = r.synthetic_curve_id
                group by r.synthetic_curve_id, r.continent, r.country, r.wmo_id, r.wmo_weighting_type, r.wmo_number, r.wmo_weighting, m.parameter, m.model_source, cast(m.value_date as date)
                --order by r.synthetic_curve_id, r.continent, r.country, r.wmo_id, r.wmo_weighting_type, r.wmo_number, r.wmo_weighting, m.parameter, m.model_source, value_date
                
              '''.format(database = self.dbnameScrape,
                        tabletitle = self.tabletitle,
                        table = self.tablename)

        df = pd.read_sql(sql, sqlConnScrape)
        return df
    
    
    def get_wind_wave_data(self):
        
        sqlConnScrape = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER=' + self.dbserverScrape + ';Trusted_Connection=yes')
        
        sql = '''
                select r.synthetic_curve_id, r.country, r.installation, m.parameter, m.model_source, cast(m.value_date as date) as value_date, min(m.value) as min_value, avg(m.value) as average_value, max(m.value) as max_value
                from {database}.{tabletitle}.{table} m
                inner join Meteorology.dbo.RefInternalMeteomaticCoordinatesLNGTerminalsCurves r
                on m.synthetic_curve_id = r.synthetic_curve_id
                where m.IsLatest = 1
                	and m.model_source = 'mix'
                	and m.parameter in ('mean_wave_direction:d', 'significant_wave_height:m', 'wind_dir_10m:d', 'wind_speed_10m:kn')
                	and m.value not in (-666, -777, -888, -999)
                group by r.synthetic_curve_id, r.country, r.installation, m.parameter, m.model_source, cast(m.value_date as date)
                order by r.synthetic_curve_id, r.country, r.installation, m.parameter, m.model_source, value_date

              '''.format(database = self.dbnameScrape,
                        tabletitle = self.tabletitle,
                        table = self.tablename)

        df = pd.read_sql(sql, sqlConnScrape)
        return df
    

    def get_meti_data(self):
        
        sqlConnScrape = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER=' + self.dbserverScrape + ';Trusted_Connection=yes')
        
        sql = '''
                select c.CurveId, c.Commodity, c.Type, c.PeriodType, c.UnitOfMeasure, c.Source, c.Location, c.Country, c.[FreeText], al.ForecastDate, al.ValueDate, al.Value
                from {database}.{tabletitle}.{table} al
                inner join AnalyticsModel.aux.CurveDetail c
                on al.CurveId = c.CurveId
                where al.CurveId in (59328, 59329)
                order by al.CurveId, al.ValueDate

              '''.format(database = self.dbnameScrape,
                        tabletitle = self.tabletitle,
                        table = self.tablename)

        df = pd.read_sql(sql, sqlConnScrape)
        return df
        
    
    def get_live_fx(self):
        
        sqlConnScrape = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER=' + self.dbserverScrape + ';Trusted_Connection=yes')
        
        sql = '''
               SELECT *
               FROM {database}.{tabletitle}.{table} 

              '''.format(database = self.dbnameScrape,
                        tabletitle = self.tabletitle,
                        table = self.tablename)

        df = pd.read_sql(sql, sqlConnScrape)
        return df

               
    def get_ytd_fx(self):
        
        yesterday=datetime.date.today()-BDay(1)
        sqlConnScrape = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER=' + self.dbserverScrape + ';Trusted_Connection=yes')
        #print(yesterday)
        sql = '''
               SELECT*
                  FROM {database}.{tabletitle}.{table}
                  where ToCurrency = 'USD' and FromCurrency in('EUR','GBP') and COBDate = cast({getdate} as date)

              '''.format(database = self.dbnameScrape,
                        tabletitle = self.tabletitle,
                        table = self.tablename,
                        getdate = "'"+str(yesterday)+"'")

        df = pd.read_sql(sql, sqlConnScrape)
        return df
    
    def get_EU_inventory_fcst():
        
        today = datetime.date.today()
        sqlConnScrape = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER=prd-db-sql-201;Trusted_Connection=yes')
        #print(yesterday)
        sql = '''
                select GasDay, ForecastDate, Conti_IT_CEE_stock
                from [Sandpit].[dbo].[RN_Balances_forecasts]
                where ForecastDate = (select max(ForecastDate) from [Sandpit].[dbo].[RN_Balances_forecasts])
                order by GasDay
                 

              '''
              
              
        
        df = pd.read_sql(sql, sqlConnScrape)
        return df  

    def get_EU_inventory_hist_fcst():
        
        today = datetime.date.today()
        sqlConnScrape = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER=prd-db-sql-201;Trusted_Connection=yes')
        #print(yesterday)
        sql = '''
                SELECT [GasDay], max([ForecastDate]) as [MaxFD]
                FROM [Sandpit].[dbo].[RN_Balances_forecasts]
                group by GasDay
                order by GasDay

              '''
              
              
        df = pd.read_sql(sql, sqlConnScrape)
        return df 


    def get_JP_power():
        
        sqlConnScrape = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER=prd-db-sql-211;Trusted_Connection=yes')
        #print(yesterday)
        sql = '''
                select *
                from LNG.dbo.OutputsJepxMaintenanceByTypeAndCategory m
                --order by m.Date desc, m.StopCategory, m.PowerGenerationFormat

              '''
              
              
        df = pd.read_sql(sql, sqlConnScrape)
        return df 
    
    def get_JP_nuk():
  
        sqlConnScrape = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER=prd-db-sql-209;Trusted_Connection=yes')
            #print(yesterday)
        sql = '''
             select *
             FROM [AnalyticsModel].[ts].[AnalyticsLatest]
             where CurveId in (14952,	14953,	14954,	14955,	14956,	14959,	14960,	14946,	14947,	14948,	14949,	14945,	14950,	14951,	14957,	14958) 
    
              '''
              
              
        df = pd.read_sql(sql, sqlConnScrape)
        return df 
    
    def get_JP_nuk_previous():
  
        today = datetime.date.today()-relativedelta(months = 1)
        
        sqlConnScrape = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER=prd-db-sql-209;Trusted_Connection=yes')
            #print(yesterday)
        sql = '''select ah.*
                from
                (
                	SELECT ah.CurveId, ah.ValueDate, max(ah.ForecastDate) as MaxFD
                	FROM [AnalyticsModel].[ts].[AnalyticsHistory] ah
                	where CurveId in (14952,	14953,	14954,	14955,	14956,	14959,	14960,	14946,	14947,	14948,	14949,	14945,	14950,	14951,	14957,	14958)
                		and ForecastDate < cast({getdate} as date)
                	group by ah.CurveId, ah.ValueDate
                ) z
                inner join [AnalyticsModel].[ts].[AnalyticsHistory] ah
                on z.CurveId = ah.CurveId and z.ValueDate = ah.ValueDate
                where ah.ForecastDate = z.MaxFD
                order by ah.ValueDate, ah.CurveId
                '''.format(getdate = "'"+str(today)+"'")
              
              
        df = pd.read_sql(sql, sqlConnScrape)
        
        return df 
    
    
    def get_JP_nuk_outage_date():
  
        #today = datetime.date.today()
        
        sqlConnScrape = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER=prd-db-sql-208;Trusted_Connection=yes')
            #print(yesterday)
        sql = '''
             select [CreatedDate],
                		[PowerPlantName]
                		,[UnitName]
                		,[StopDateAndTime]
                      ,[ScheduledRecoveryDate],
                      [PowerGenerationFormat],
                      [AuthorizationOutput],
                      [ReductionAmount]
             FROM [Scrape].[dbo].[MaintenanceCalendarJapanJepxOutages]
             where IsLatest = '1' and PowerGenerationFormat = 'Nuclear power' --and StopDateAndTime >= '2022-01-01' and StopDateAndTime <'2023-04-30'
             order by CreatedDate
              '''
              
              
        df = pd.read_sql(sql, sqlConnScrape)
        return df
    
    def get_JP_nuk_outage_date_previous():
  
        today = datetime.date.today()-relativedelta(months = 1)
        
        sqlConnScrape = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER=prd-db-sql-208;Trusted_Connection=yes')
            #print(yesterday)
        sql = '''
             select [CreatedDate]
                		,[PowerPlantName]
                		,[UnitName]
                		,[StopDateAndTime]
                    ,[ScheduledRecoveryDate]
                    ,[PowerGenerationFormat]
             FROM [Scrape].[dbo].[MaintenanceCalendarJapanJepxOutages]
             where IsLatest = '0' and PowerGenerationFormat = 'Nuclear power' --and CreatedDate >= cast({getdate} as date)
             order by CreatedDate
    
              '''.format(getdate = "'"+str(today)+"'")
              
              
        df = pd.read_sql(sql, sqlConnScrape)
        return df
    
    def get_JPEX_outage():
  
        
        sqlConnScrape = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER=prd-db-sql-208;Trusted_Connection=yes')
        sql = '''
                SET NOCOUNT ON;
                exec scrape.[dbo].[Jepx_MaintenanceScheduleAndOutages]
              '''
              
              
        df = pd.read_sql(sql, sqlConnScrape)
        
        return df
    
    def get_JPEX_outage_comments():
  
        
        sqlConnScrape = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER=prd-db-sql-208;Trusted_Connection=yes')
        sql = '''
                SELECT * FROM [Scrape].[dbo].[MaintenanceCalendarJapanKyudenView]
                order by PlantName, UnitName
              '''
              
        df = pd.read_sql(sql, sqlConnScrape)
        
        return df
    
    def get_SK_nuk():
  
        sqlConnScrape = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER=prd-db-sql-209;Trusted_Connection=yes')
            #print(yesterday)
        sql = '''
             select *
             FROM [AnalyticsModel].[ts].[AnalyticsLatest]
             where CurveId in (14919,	14920,	14921,	14922,	14923,	14924,	14925,	14926,	14927,	14928,	14929,	14930,	14931,	14932,	14933,	14934,	14935,	14936,	14937,	14938,	14939,	14940,	14941,	14942,	14943,	14944,71449)
                 
              '''

              
        df = pd.read_sql(sql, sqlConnScrape)
        return df 
    
    def get_KHNP_outage():
  
        
        sqlConnScrape = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER=prd-db-sql-208;Trusted_Connection=yes')
        sql = '''
                SET NOCOUNT ON;
                exec Scrape.[dbo].[KHNP_MaintenanceSchedule]
              '''
              
              
        df = pd.read_sql(sql, sqlConnScrape)
        
        return df
    
    
    def get_ttf_curve():
        
        sqlConnScrape = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER=PRD-DB-SQL-214;Trusted_Connection=yes')
            #print(yesterday)
        sql = '''
                    DECLARE @today DATETIME
                    SET @today = GETDATE()
                    DECLARE @monthAheadContract CHAR(6)
                    SET @monthAheadContract = FORMAT(DATEADD(MONTH, 1, @today), 'MMM') + '-' + CAST(YEAR(DATEADD(MONTH, 1, @today)) % 2000 AS CHAR)
                    DECLARE @currentMonth INT
                    SET @currentMonth = MONTH(@today)
                    DECLARE @currentYear INT
                    SET @currentYear = YEAR(@today)
                    DECLARE @curentYearForSeason CHAR(2)
                    DECLARE @nextYearForSeason CHAR(2)
                    SET @curentYearForSeason = CAST(@currentYear % 2000 AS CHAR(2))
                    SET @nextYearForSeason = CAST((@currentYear + 1) % 2000 AS CHAR(2))
                    DECLARE @seasonAheadContract CHAR(6)
                    SET @seasonAheadContract = 
                        CASE
                            WHEN @currentMonth >= 4 AND @currentMonth <= 9 THEN 'Win ' + @curentYearForSeason
                            WHEN @currentMonth >= 10 AND @currentMonth <= 12 THEN 'Sum ' + @nextYearForSeason
                            ELSE 'Sum '+ @curentYearForSeason
                        END  
                    DECLARE @nextCal CHAR(4)
                    DECLARE @nextNextCal CHAR(4)
                    SET @nextCal = CAST(@currentYear + 1 AS CHAR(4))
                    SET @nextNextCal = CAST(@currentYear + 2 AS CHAR(4))
                    SELECT [Id]
                        ,[DateTime]
                        ,[LastUpdate]
                        ,[TradeID]
                        ,[OrderID]
                        ,[Action]
                        ,[InstrumentID]
                        ,[InstrumentName]
                        ,[FirstSequenceItemName]
                        ,[ExecutionVenueID]
                        ,[Price]
                        ,[Volume]
                        ,[Timestamp]
                    FROM [Trayport].[ts].[Trade]
                    WHERE [DateTime] >= '2022-04-13' and [SeqSpan] = 'Single'
                    AND ([InstrumentName] = 'TTF Hi Cal 51.6' or [InstrumentName] = 'TTF Hi Cal 51.6 1MW'
                    or [InstrumentName] = 'TTF Hi Cal 51.6 EEX' or [InstrumentName] = 'TTF Hi Cal 51.6 EEX OTF'
                    or [InstrumentName] = 'TTF Hi Cal 51.6 1MW EEX Non-MTF' or [InstrumentName] = 'TTF Hi Cal 51.6 PEGAS'
                    or [InstrumentName] = 'TTF Hi Cal 51.6 PEGAS OTF' or [InstrumentName] = 'TTF Hi Cal 51.6 1MW PEGAS Non-MTF'
                    or [InstrumentName] = 'TTF Hi Cal 51.6 ICE ENDEX' or [InstrumentName] = 'TTF Hi Cal 51.6 ICE')
                    AND [Volume] >= 5
                    and FirstSequenceItemName IN (
                        @monthAheadContract, 
                        @seasonAheadContract,
                        @nextCal,
                        @nextNextCal
                    )

    
              '''
              
              
        df = pd.read_sql(sql, sqlConnScrape)
        return df 
    
        
    def get_FreightAtlantic():
        
        
        sqlConnScrape = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER=prd-db-sql-211;Trusted_Connection=yes')
        sql = '''
                select [RIC], [Timestamp], [Close]
                from [LNG].[dbo].[ReutersActualPrices] p
                where p.[RIC] in ('AARXT00')
                order by p.[RIC], p.[Timestamp]
              '''
              
              
        df = pd.read_sql(sql, sqlConnScrape)
        
        
        return df
        
        
    def get_ce_regas_stock():
        
        sqlConnScrape = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER=PRD-DB-SQL-209;Trusted_Connection=yes')
        sql = '''
                select *
                from [AnalyticsModel].[ts].[CommodityEssentialsTimeSeries]
                where SeriesId in ('20882',	'26493',	'26543',	'26513',	'26523',	'26533',	'26503',	'20035',	'20032',	'20027',	'19507',	'19514',	'19509',	'45515',	'55324',	'25199',	'25219',	'25209',	'45534',	'18837',	'28238',	'33945','60381','51854','60735','61026','60736')
              '''
              
        df = pd.read_sql(sql, sqlConnScrape)
        
        return df
    
    def get_ce_regas_sendout():
        
        sqlConnScrape = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER=PRD-DB-SQL-209;Trusted_Connection=yes')
        sql = '''
                select *
                from [AnalyticsModel].[ts].[CommodityEssentialsTimeSeries]
                where SeriesId in ('20881',	'26492',	'26542',	'26512',	'26522',	'26532',	'26502',	'20034',	'20033',	'20026',	'19506',	'19513',	'19508',	'45514',	'55325',	'25198',	'25218',	'25208',	'45533',	'59909',	'18836',	'28237',	'33944','60384','50851','50845','60733','61025','60734')
              '''
              
        df = pd.read_sql(sql, sqlConnScrape)
        
        return df
    
    def get_ce_regas_capa():
        
        sqlConnScrape = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER=PRD-DB-SQL-209;Trusted_Connection=yes')
        sql = '''
                select *
                from [AnalyticsModel].[ts].[CommodityEssentialsTimeSeries]
                where SeriesId in ('29046',	'29047',	'29052',	'29049',	'29050','29051',	'29048',	'28987',	'29122',	'28984',	'29021',	'29022',	'28959',	'45553',	'55329',	'29007',	'29011',	'29009',	'45552',	'28995',	'29038',	'34084','60399','60823',	'61033',	'60824')
              '''                   
              
        df = pd.read_sql(sql, sqlConnScrape)
        
        return df
        
    def get_regas_arrivel():
        
        today= datetime.date.today()
        start = '2012-04-01'#today - relativedelta(days = 15)
        end = '2022-08-10'#today + relativedelta(days = 30)
        
        sqlConnScrape = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER=PRD-DB-SQL-211;Trusted_Connection=yes')
        sql = '''
                SELECT 
                	
                	[Vessel],
                	[CountryOrigin],
                	[InstallationOrigin],
                	[EndOrigin],
                	[CountryDestination],
                	[InstallationDestination],
                	[StartDestination],
                	[VolumeDestinationM3],
                	[EtaDestination]
                
                  FROM [LNG].[dbo].[KplerLNGTrades]
                  where StartDestination >= {start} and ContinentDestination = 'Europe'
                  '''.format(start = "'"+str(start)+"'"
                        )
              
              
        df = pd.read_sql(sql, sqlConnScrape)
        
        return df
        
    def get_brizal_hydro():
        
        sqlConnScrape = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER=ENTFUND-DB;Trusted_Connection=yes')
        sql = '''
                 select b.DAT1, replace(left(right(REF2, 6), 5), ',', '.') as [value]
                        from [fpEntFundamental].cleardox.BRAZIL_HYDRO_PARSER b
                        inner join
                        (
                               select DAT1, min(CREATE_DATE) as MIN_CREATE_DATE
                               from [fpEntFundamental].cleardox.BRAZIL_HYDRO_PARSER
                               where REF1 = 'Subsistema Sudeste / Centro-Oeste'
                               group by DAT1
                        ) z
                        on z.DAT1 = b.DAT1 and z.MIN_CREATE_DATE = b.CREATE_DATE
                        where b.REF1 = 'Subsistema Sudeste / Centro-Oeste'
                        group by b.DAT1, replace(left(right(b.REF2, 6), 5), ',', '.')
                        order by b.DAT1
            '''
              
              
        df = pd.read_sql(sql, sqlConnScrape)
        
        return df
    
    def get_model_run_date():
        
        sqlConnScrape = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER=PRD-DB-SQL-209;Trusted_Connection=yes')
        sql = '''
                select a.ForecastDate, count(*)
                    from AnalyticsModel.ts.AnalyticsHistory a
                    where a.CurveId = 43466
                    group by a.ForecastDate
                    order by a.ForecastDate desc
            '''
              
              
        df = pd.read_sql(sql, sqlConnScrape)
        
        return df
    
    def get_desk_supply_hist():
        
        
        curveid = DBTOPD.get_curve_id()
        dfcurveid = curveid.loc[:,['CurveId','Type','Location','Country']]
        
        SupplyCurveId = dfcurveid.loc[dfcurveid[dfcurveid['Type']=='Supply'].index]
        SupplyCurveId = SupplyCurveId[['CurveId','Country','Location']]
        #print(SupplyCurveId)
        list_curveId = SupplyCurveId['CurveId'].to_list()
      
        tuple_curveId = tuple(list_curveId)

        
        start = datetime.date.today()-relativedelta(days = 365+8)
        
        sqlConnScrape = pyodbc.connect('DRIVER={SQL SERVER};SERVER=PRD-DB-SQL-209;Trusted_Connection=yes')
        
        sql = '''
                select CurveId, ForecastDate, ValueDate, Value
                from [AnalyticsModel].[ts].[AnalyticsHistory] 
                where CurveId in {listids} and ForecastDate > {date}
    
              '''.format(listids = tuple_curveId,
                          date = '\''+str(start)+'\''
                              )

        df = pd.read_sql(sql, sqlConnScrape)

        
        return df
    
    def get_desk_demand_hist():
        
        start = datetime.date.today()-relativedelta(days = 365+8)
        
        sqlConnScrape = pyodbc.connect('DRIVER={SQL SERVER};SERVER=PRD-DB-SQL-209;Trusted_Connection=yes')
        
        sql = '''
                select CurveId, ForecastDate, ValueDate, Value
                from [AnalyticsModel].[ts].[AnalyticsHistory] 
                where ((CurveId >= 43414 and CurveId <= 43466) or CurveId = 67202 or CurveId = 67203) and ForecastDate > {}
    
              '''.format('\''+str(start)+'\'')

        df = pd.read_sql(sql, sqlConnScrape)
        return df
    
    def get_japan_power_price():
        
        today = datetime.datetime.today()
        
        sqlConnScrape = pyodbc.connect('DRIVER={SQL SERVER};SERVER=PRD-DB-SQL-211;Trusted_Connection=yes')
        
        sql = '''
                select r.PRODUCT, r.CONTRACT_DATE, r.VALUE_DATE, r.VALUE, r.CREATED_DATE
                from [LNG].[dbo].[ReutersPrices] r
                inner join
                (
                	select r.PRODUCT, r.CONTRACT_DATE, max(r.VALUE_DATE) as [MaxVD]
                	from [LNG].[dbo].[ReutersPrices] r
                	where r.PRODUCT in ('JEPD', 'JEBD', 'JWBD', 'JWPD')
                	group by r.PRODUCT, r.CONTRACT_DATE
                ) z
                on r.PRODUCT = z.PRODUCT and r.CONTRACT_DATE = z.CONTRACT_DATE and r.VALUE_DATE = z.MaxVD
    
              '''
        sql1 = '''
                SELECT *
                      FROM [LNG].[ana].[BBGJPPOWERPRICE] r
                      order by [timestamp]
            '''
        
        
        
        df = pd.read_sql(sql1, sqlConnScrape)
        #print(df)
        return df
        
    def get_us_feed():
        
        now = datetime.datetime.now()


        facility_points = {
                'Sabine Pass' : [77083, 72084, 72100, 73221, 74842, 77395,79489, 80303],
                'Corpus' : [76869,76875],
                'Cameron' : [76126,44495,76582],
                'Freeport' : [76510,77826,79123],
                'Cove' : [25046,74893],
                'Elba' : [24717,77227],
                'Calcasieu Pass': [80030, 80031, 80549]
            }
        points_to_facility = {y:k for k,v in facility_points.items() for y in v}
        points_list = ','.join([str(x) for x in points_to_facility])
        db_server_fpentfund = 'entfund-db'
        db_name_fpentfund = 'fpEntFundamental'
        sql_conn_fpentfund = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER=' + db_server_fpentfund + ';DATABASE=' + db_name_fpentfund + ';Trusted_Connection=yes')
        
        def sum_location_types(x):
            if x.name[1] == 'Cove':
                return x['D'] + x['G'] - x['R']
            elif x.name[1] == 'Elba':
                return x['D'] - x['W']
            return x.sum()
        
        def Run():
            #try:
            df = pd.read_sql('''
                    
                    select	F.MEASUREMENT_DATE, Pt.POINTS_ID, F.SCHEDULED_VOLUME, F.NOMINATION_CYCLE, F.LOCATION_TYPE, p.NAME as [PIPELINE_NAME]
                    from	bentek.FLOW_DATA F, bentek.PIPELINES P, bentek.POINTS Pt
                    where	F.Pipeline_ID = P.PIPELINE_ID
                    		and F.Point_ID = Pt.POINTS_ID
                    		and Pt.POINTS_ID IN (''' + points_list + ''')
                    		and f.IS_ACTIVE = 1
                    		and f.Expiration_date = '31-DEC-9999'
                ''', sql_conn_fpentfund)
            df['FACILITY'] = df['POINTS_ID'].map(points_to_facility)
            df = df.sort_values(['FACILITY', 'PIPELINE_NAME', 'MEASUREMENT_DATE', 'NOMINATION_CYCLE', 'LOCATION_TYPE'])
            df_grp = df.groupby(['FACILITY', 'PIPELINE_NAME', 'MEASUREMENT_DATE', 'NOMINATION_CYCLE', 'LOCATION_TYPE'])
            sum_vol_df = df_grp['SCHEDULED_VOLUME'].sum().reset_index()
            sum_vol_df_pivot = sum_vol_df.pivot_table(values='SCHEDULED_VOLUME', index=['MEASUREMENT_DATE', 'FACILITY', 'PIPELINE_NAME', 'NOMINATION_CYCLE'], columns='LOCATION_TYPE')
            sum_vol_df_pivot['Sum'] = sum_vol_df_pivot.apply(lambda x: sum_location_types(x), axis=1)
            sum_vol_df_melt = sum_vol_df_pivot.reset_index().melt(id_vars=['MEASUREMENT_DATE', 'FACILITY', 'PIPELINE_NAME', 'NOMINATION_CYCLE'], value_name='SCHEDULED_VOLUME')
            sum_vol_df_melt['SCHEDULED_VOLUME'] = round(sum_vol_df_melt['SCHEDULED_VOLUME'] / 1000.0, 0)
            sum_vol_df_melt['CREATED_DATE'] = now
            sum_vol_df_melt = sum_vol_df_melt.sort_values(['CREATED_DATE', 'MEASUREMENT_DATE', 'FACILITY', 'PIPELINE_NAME', 'NOMINATION_CYCLE'])
            #print(sum_vol_df_melt)
            return sum_vol_df_melt
        
        sum_vol_df_melt = Run()
        
        return sum_vol_df_melt
    
    
    
    def get_aus_feedgas():
        
    
        sqlConnScrape = pyodbc.connect('DRIVER={SQL SERVER};SERVER=PRD-DB-SQL-208;Trusted_Connection=yes')
        
        sql = '''
                   SELECT  [CreatedDate]
                      ,[GasDate]
                      ,[FacilityId]
                      ,[FacilityName]
                      ,[LocationId]
                      ,[LocationName]
                      ,[Demand]
                      ,[Supply]
                      ,[TransferIn]
                      ,[TransferOut]
                      ,[HeldInStorage]
                      ,[LastUpdated]
                      ,[NomsAndForecasted]
                  FROM [Scrape].[dbo].[ActualNomsAndForecastedFlowsAustraliaAEMOView]
                  where FacilityName in ('APLNG Pipeline', 'GLNG Gas Transmission Pipeline','Queensland Gas Pipeline','Wallumbilla to Gladstone Pipeline')
    
              '''

        df = pd.read_sql(sql, sqlConnScrape)
        
        return df
    

        
    def get_temp_hist_data():
        
        today = datetime.datetime.today()
        
        sqlConnScrape = pyodbc.connect('DRIVER={SQL SERVER};SERVER=PRD-DB-SQL-211;Trusted_Connection=yes')
        
        country_list = ['Finland', 'South Korea', 'Serbia and Montenegro', 'New Zealand', 'Egypt',
                                             'Italy', 'Brazil', 'Netherlands', 'Malaysia', 'Jamaica', 'Indonesia', 'Germany',
                                             'Uzbekistan', 'Lithuania', 'Iceland', 'Kuwait', 'Hungary', 'Switzerland',
                                             'United States', 'Estonia', 'Algeria', 'Bosnia and Herzegovina',
                                             'Australia', 'United Kingdom', 'Mexico', 'Albania', 'Myanmar', 'Sweden',
                                             'Ukraine', 'Argentina', 'China', 'Peru', 'India', 'Croatia', 'Greece', 'Austria',
                                             'Poland', 'Bulgaria', 'Canada', 'Slovenia', 'Ireland', 'Latvia', 'Slovakia',
                                             'Republic of Macedonia', 'Thailand', 'Norway', 'France', 'Luxembourg',
                                             'Belgium', 'Japan', 'Turkmenistan', 'Spain' ,'Turkey', 'Bangladesh', 'Romania',
                                             'Denmark', 'Pakistan', 'Colombia', 'Iran', 
                                             'Czech Republic', 'Portugal', 'Taiwan', 'Kazakhstan' ,'Paraguay','Vietnam','Philippines','Iceland']
        
        dfhist = pd.DataFrame(columns=country_list)
        for i in dfhist:
        
            sql = '''
                        SELECT * FROM [Meteorology].[dbo].[WeatherStationTimeSeriesLatest] ws
                            WHERE ModelSourceName = 'ecmwf-era5'
            						and ParameterName ='t_2m:C'
                                    AND ValueDate >= {startdate}
            						and CountryName = {country}
        
                          '''.format(startdate = '\''+str(today.year-5)+'-01-01'+'\'',
                          country='\''+i+'\'')
            
            df = pd.read_sql(sql, sqlConnScrape)
            
            df=df[['ValueDate','WMOId','Weighting','Value']]
            df['weighted'] = df['Weighting']*df['Value']
            df['ValueDate'] = pd.to_datetime(df['ValueDate'])
            #print(i, df)
            dfpivot = df.pivot_table(values='weighted',index='ValueDate',columns='WMOId')
            dfpivot['sum'] = dfpivot.sum(axis=1)
            #print(dfpivot)
            #df.set_index('ValueDate', inplace=True)
            #df.sort_index(inplace=True)
            #print(df)
            dfpivot = dfpivot.resample('D').mean()
            #print(dfpivot)
            dfhist[i] = dfpivot['sum']
            
        dfhist.drop(index = dfhist.index[-1], inplace=True)
        #print(dfhist)
        
        return dfhist
  
    
    
    