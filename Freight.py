# -*- coding: utf-8 -*-
"""
Created on Tue Aug 10 16:32:34 2021

@author: SVC-PowerUK-Test
"""

import pandas as pd
import sys
sys.path.append('C:\\Users\SVC-PowerUK-Test\YWwebsite\Python Scripts\class') 
from DBtoDF import DBTOPD
import sqlalchemy
pd.set_option('display.max_columns',10)


class Freight():
    
    def get_freight():
        #read freight data from DB
        freight=DBTOPD('PRD-DB-SQL-208','Scrape', 'dbo', 'ReutersActualPrices')
        dffreight=freight.sql_to_df()
        dffreight.set_index('RIC', inplace=True)
        #print(dffreight)
        #dffreight["Timestamp"] =pd.to_datetime(dffreight["Timestamp"],format='%Y-%m-%d')
        #dffreight["Timestamp"] = dffreight["Timestamp"].dt.date
        start_date =dffreight["Timestamp"].iloc[0]
        end_date =dffreight["Timestamp"].iloc[-1]
        
        freight_Pacific = dffreight[['Timestamp','Close']].loc['AARXT00']
        #freight_Pacific.set_index('Timestamp', inplace=True)
        #print(freight_Pacific)
        
        freight_Atlantic = dffreight[['Timestamp','Close']].loc['AASYC00']
        #freight_Atlantic.set_index('Timestamp', inplace=True)
        #print(freight_Atlantic)
        db_server_lng = "PRD-DB-SQL-211"
        db_name_lng = "LNG"
        sql_engine_lng = sqlalchemy.create_engine("mssql+pyodbc://" + db_server_lng + "/" + db_name_lng + "?trusted_connection=yes&driver=ODBC+Driver+13+for+SQL+Server")
        
        freight_Pacific.to_sql(name='FreightPacific', con=sql_engine_lng, index=False, if_exists='replace', schema='ana')
        freight_Atlantic.to_sql(name='FreightAtlantic', con=sql_engine_lng, index=False, if_exists='replace', schema='ana')
        return freight_Pacific, freight_Atlantic

#a=Freight
#pa,at=a.get_freight()
#print(pa, at)