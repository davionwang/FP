import pandas as pd
import sqlalchemy as sa
import urllib


#Inputs
startp = '2022-01-01'
endp = '2022-02-25'



#%% Read in data and merge

#Read in Trayport Data
params = urllib.parse.quote_plus(r'DRIVER={SQL Server};SERVER=PRD-DB-SQL-214;DATABASE=Trayport;Trusted_Connection=yes')
conn_str = 'mssql+pyodbc:///?odbc_connect={}'.format(params)
engine = sa.create_engine(conn_str)
trayportdb = pd.read_sql("""SELECT [Id]
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
                                WHERE [DateTime] >= {} AND [DateTime] <= {} and [SeqSpan] = 'Single' 
                                
                                
                                AND ([InstrumentName] = 'TTF Hi Cal 51.6' or [InstrumentName] = 'TTF Hi Cal 51.6 1MW'                                                                                                                           
                                or [InstrumentName] = 'TTF Hi Cal 51.6 EEX' or [InstrumentName] = 'TTF Hi Cal 51.6 EEX OTF'
                                or [InstrumentName] = 'TTF Hi Cal 51.6 1MW EEX Non-MTF' or [InstrumentName] = 'TTF Hi Cal 51.6 PEGAS' 
                                or [InstrumentName] = 'TTF Hi Cal 51.6 PEGAS OTF' or [InstrumentName] = 'TTF Hi Cal 51.6 1MW PEGAS Non-MTF' 
                                or [InstrumentName] = 'TTF Hi Cal 51.6 ICE ENDEX' or [InstrumentName] = 'TTF Hi Cal 51.6 ICE')
                                
                                AND [Volume] >= 5
                                
                                AND (CHARINDEX('Jan', [FirstSequenceItemName]) > 0
                                 or CHARINDEX('Feb', [FirstSequenceItemName]) > 0
                                 or CHARINDEX('Mar', [FirstSequenceItemName]) > 0
                                 or CHARINDEX('Apr', [FirstSequenceItemName]) > 0
                                 or CHARINDEX('May', [FirstSequenceItemName]) > 0
                                 or CHARINDEX('Jun', [FirstSequenceItemName]) > 0
                                 or CHARINDEX('Jul', [FirstSequenceItemName]) > 0
                                 or CHARINDEX('Aug', [FirstSequenceItemName]) > 0
                                 or CHARINDEX('Sep', [FirstSequenceItemName]) > 0
                                 or CHARINDEX('Oct', [FirstSequenceItemName]) > 0
                                 or CHARINDEX('Nov', [FirstSequenceItemName]) > 0
                                 or CHARINDEX('Dec', [FirstSequenceItemName]) > 0)
                                
                                AND CHARINDEX('BOM', [FirstSequenceItemName]) = 0
                              
                                    """.format('\''+startp+'\'','\''+endp+'\''), engine)
                                    
print(trayportdb)