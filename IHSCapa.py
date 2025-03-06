# -*- coding: utf-8 -*-
"""
Created on Tue Nov 23 15:10:43 2021

@author: SVC-GASQuant2-Prod
"""

import sqlalchemy

import calendar

import sys

import datetime
import pandas as pd

sys.path.append('C:\\Users\SVC-GASQuant2-Prod\YWwebsite\Python Scripts\class') 


pd.set_option('display.max_columns',20)
class IHS_capa():
    
    def regas_capa_data():
            
        today=datetime.date.today()
        
        #capa data        
        capa=pd.read_excel('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/IHS Markit Regasification Database 11_22_2021.xlsm',header=(5),sheet_name='Terminal Details') #mt
        capa=capa[['Market','Status','IHS Markit Estimated Start','Capacity (MMtpa)']]
        capaexist=capa[capa['Status']=='Existing']
        capacons=capa[capa['Status']=='Under Construction']
        #print(capaexist)
        #print(capacons)
        capa=pd.concat([capaexist,capacons])
        #print(capa)
        capa['monthly capacity'] = capa['Capacity (MMtpa)']/12 #mt/month
        capa.reset_index(inplace=True)
        capa.sort_values(by='IHS Markit Estimated Start', inplace=True)

        dbcapagroup=capa[['Market','IHS Markit Estimated Start','Capacity (MMtpa)']]
        
        IHSname = capa['Market'].drop_duplicates()
        #print(IHSname)
        dbcapa=pd.DataFrame(index=pd.date_range(start='1964-01-01', end='2027-12-31'), columns=IHSname.to_list())
        for i in IHSname:
            #dbcapa[i]=dbcapagroup.loc['Initial_Capacity'].cumsum()
            df1=dbcapagroup.loc[dbcapagroup[dbcapagroup['Market']==i].index]
            df1=df1[['IHS Markit Estimated Start','Capacity (MMtpa)']]
            df1['cum capa']=df1['Capacity (MMtpa)'].cumsum()
            df1=df1.groupby('IHS Markit Estimated Start').sum()
            #print(df1)
            for j in df1.index:
                dbcapa.loc[j, i]=df1.loc[j,'cum capa']
        dbcapa.fillna(0, inplace=True)
        
        dbcapa.replace(to_replace=0, method='ffill', inplace=True)
        #print(dbcapa)
        db_server_lng = "PRD-DB-SQL-211"
        db_name_lng = "LNG"
        sql_engine_lng = sqlalchemy.create_engine("mssql+pyodbc://" + db_server_lng + "/" + db_name_lng + "?trusted_connection=yes&driver=ODBC+Driver+13+for+SQL+Server")
        dbcapa.to_sql(name='IHSregascapa', con=sql_engine_lng, index=True, if_exists='replace', schema='ana')
        '''        
        dfcapa = pd.DataFrame(index=pd.date_range(start='1964-01-01', end=today), columns=IHSname.to_list())
    
        for i in IHSname:
            df=capa.loc[capa[capa['Market']==i].index]
            df=df[['IHS Markit Estimated Start','Capacity (MMtpa)']]
            df['cum capa']=df['Capacity (MMtpa)'].cumsum()
            df=df.groupby('IHS Markit Estimated Start').sum()
            for j in df.index:
                dfcapa.loc[j, i]=df.loc[j,'cum capa']
            #print(i,df)
        dfcapa.fillna(0, inplace=True)
        
        dfcapa=dfcapa.resample('MS').sum()
        dfcapa.replace(to_replace=0, method='ffill', inplace=True)
        dfcapa=dfcapa/0.063 #mt/month to 
        dfcapa=dfcapa.loc['2018-01-01':,:]
        #get capa day cargo rage
        for i in dfcapa.index:
            days=calendar.monthrange(i.year,i.month)[1]
            dfcapa.loc[i]=dfcapa.loc[i]/days
        
        return dfcapa
        '''

    def regas_capa_data23():
            
        today=datetime.date.today()
        
        #capa data        
        capa=pd.read_excel('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/SP Global Regasification Projects and Contracts Data Analysis Tool 2023_09_01.xlsm',header=(6),sheet_name='Regasification Data') #mt
        #print(capa)
        #print(capa.columns)
        capa=capa[['Market','S&P Global estimated start date','Capacity (MMtpa)','Status','1990', '1991', '1992', '1993',
       '1994', '1995', '1996', '1997', '1998', '1999', '2000', '2001', '2002',
       '2003', '2004', '2005', '2006', '2007', '2008', '2009', '2010', '2011',
       '2012', '2013', '2014', '2015', '2016', '2017', '2018', '2019', '2020',
       '2021', '2022', '2023', '2024', '2025', '2026', '2027', '2028', '2029',
       '2030', '2031', '2032', '2033', '2034', '2035', '2036', '2037', '2038',
       '2039', '2040', '2041', '2042', '2043', '2044', '2045', '2046', '2047',
       '2048', '2049', '2050']]
        capaexist=capa[capa['Status']=='Existing']
        capacons=capa[capa['Status']=='Under Construction']
        #print(capaexist)
        #print(capacons)
        capa=pd.concat([capaexist,capacons])
        #print(capa)
        #capa['monthly capacity'] = capa['Capacity (MMtpa)']/12 #mt/month
        capa.reset_index(inplace=True)
        capa.sort_values(by='S&P Global estimated start date', inplace=True)
        #capagroup = capa.groupby('Market').sum()
        
        #print(capagroup)
        #print(capagroup.T)
        
        dbcapagroup=capa[['Market','S&P Global estimated start date','Capacity (MMtpa)']]
        
        IHSname = capa['Market'].drop_duplicates()
        #print(IHSname)
        dbcapa=pd.DataFrame(index=pd.date_range(start='1964-01-01', end='2027-12-31'), columns=IHSname.to_list())
        for i in IHSname:
            #dbcapa[i]=dbcapagroup.loc['Initial_Capacity'].cumsum()
            df1=dbcapagroup.loc[dbcapagroup[dbcapagroup['Market']==i].index]
            df1=df1[['S&P Global estimated start date','Capacity (MMtpa)']]
            df1['cum capa']=df1['Capacity (MMtpa)'].cumsum()
            df1=df1.groupby('S&P Global estimated start date').sum()
            #print(df1)
            for j in df1.index:
                dbcapa.loc[j, i]=df1.loc[j,'cum capa']
        dbcapa.fillna(0, inplace=True)
        
        dbcapa.replace(to_replace=0, method='ffill', inplace=True)
        #print(dbcapa)
        db_server_lng = "PRD-DB-SQL-211"
        db_name_lng = "LNG"
        sql_engine_lng = sqlalchemy.create_engine("mssql+pyodbc://" + db_server_lng + "/" + db_name_lng + "?trusted_connection=yes&driver=ODBC+Driver+13+for+SQL+Server")
        dbcapa.to_sql(name='IHSregascapa', con=sql_engine_lng, index=True, if_exists='replace', schema='ana')
        
        
    def teminal_capa_data():
        
        today=datetime.date.today()
        
        #pull IHS name to kpler name
        IHSname=pd.read_excel('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/IHS project names.xlsx',header=(0),sheet_name=1) 
        IHSname.set_index('IHS Markit', inplace=True)
        IHSnamedict = IHSname['Plant'].to_dict()
        #capa data        
        capa=pd.read_excel('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/IHS_Liquefaction Project Details.xlsx',header=(2),sheet_name=0) #mt
        capa=capa[['Liquefaction Project','Status','Initial_Capacity','IHS Markit Start Date']]
        #capa=capa[capa['Status']=='Existing'] 
        capaexist=capa[capa['Status']=='Existing'] 
        capacons=capa[capa['Status']=='Under Construction']
        capa=pd.concat([capaexist,capacons])
        
        capa['Liquefaction Project'].replace(IHSnamedict, inplace=True)
        capa['monthly capacity'] = capa['Initial_Capacity']/12 #mt/month
        capa.reset_index(inplace=True)
        capa.sort_values(by='IHS Markit Start Date', inplace=True)
        #print(capa.loc[capa['Liquefaction Project']=='Sabine Pass'])
        dbcapagroup=capa[['Liquefaction Project','IHS Markit Start Date','Initial_Capacity']]
        #dbcapagroup.set_index('IHS Markit Start Date', inplace=True)
        #print(dbcapagroup.loc[dbcapagroup[dbcapagroup['Liquefaction Project']=='Sabine Pass'].index])
        dbcapa=pd.DataFrame(index=pd.date_range(start='1964-01-01', end='2027-12-31'), columns=IHSname['Plant'].to_list())
        for i in IHSname['Plant']:
            #dbcapa[i]=dbcapagroup.loc['Initial_Capacity'].cumsum()
            
            
            df1=dbcapagroup.loc[dbcapagroup[dbcapagroup['Liquefaction Project']==i].index]
            df1=df1[['IHS Markit Start Date','Initial_Capacity']]
            
            df1 = df1.groupby(by='IHS Markit Start Date').sum()
            
            #df1['cum capa']=df1['Initial_Capacity'].cumsum()
            
            #print(df1)
            df1['cum capa']=df1['Initial_Capacity'].cumsum()
            #df1.set_index('IHS Markit Start Date', inplace=True)
            #df1=df1.groupby('IHS Markit Start Date').sum()
            #print(df1)
            for j in df1.index:
                dbcapa.loc[j, i]=df1.loc[j,'cum capa']
        #print(dbcapa.loc['2022-04-01':,'Sabine Pass'])
        dbcapa.fillna(0, inplace=True)
        
        dbcapa.replace(to_replace=0, method='ffill', inplace=True)
        #print(dbcapa['Coral South FLNG'])
        db_server_lng = "PRD-DB-SQL-211"
        db_name_lng = "LNG"
        sql_engine_lng = sqlalchemy.create_engine("mssql+pyodbc://" + db_server_lng + "/" + db_name_lng + "?trusted_connection=yes&driver=ODBC+Driver+13+for+SQL+Server")
        dbcapa.to_sql(name='IHScapa', con=sql_engine_lng, index=True, if_exists='replace', schema='ana')
         
        '''
        dfcapa = pd.DataFrame(index=pd.date_range(start='1964-01-01', end=today), columns=IHSname['Plant'].to_list())
        #dfcapa.fillna(0, inplace=True)
        #print(IHSname['Plant'])
        #print(dfcapa.loc['2021-02-01'])
        for i in IHSname['Plant']:
            df=capa.loc[capa[capa['Liquefaction Project']==i].index]
            df=df[['IHS Markit Start Date','monthly capacity']]
            df['cum capa']=df['monthly capacity'].cumsum()
            df=df.groupby('IHS Markit Start Date').sum()
            for j in df.index:
                dfcapa.loc[j, i]=df.loc[j,'cum capa']#dfcapa.loc[j,i]+df.loc[df[df['Liquefaction Project']==i].index]
            #print(i,df)
        dfcapa.fillna(0, inplace=True)
        #print(dfcapa['Sabine Pass'])
        
        dfcapa=dfcapa.resample('MS').sum()
        dfcapa.replace(to_replace=0, method='ffill', inplace=True)
        dfcapa=dfcapa/0.063 #mt/month to 
        dfcapa=dfcapa.loc['2018-01-01':,:'Calcasieu Pass']
        #get capa day cargo rage
        for i in dfcapa.index:
            days=calendar.monthrange(i.year,i.month)[1]
            dfcapa.loc[i]=dfcapa.loc[i]/days
        
       
        #print(dfcapa)        
        #print(capa.index[0]<dfcapa.index[0])
        return dfcapa
        '''
        
    def update_capa():

        a=IHS_capa
        a.regas_capa_data23()
        a.teminal_capa_data()
        
#IHS_capa.update_capa()