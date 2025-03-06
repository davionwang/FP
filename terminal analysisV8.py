# -*- coding: utf-8 -*-
"""
Created on Wed Mar  9 13:05:26 2022

@author: SVC-GASQuant2-Prod
"""

# -*- coding: utf-8 -*-



#V1 fixed error and unit
#V2 use daily Kpler data, not 10 days ave.Add 365 days utilisation
#V3 use capa from DB, and Sabine pass increase capa
#V4 use new dataframe with 2022
#V5 color delta and uti(can't compare %), supply uses DateOrigin includes Loading, 
#V6 add demand table, add desk view and ave.day vol for demand table, change cargos to mcm
#V7 button to download
#V8 supply + desk view, ave week, 1 year delta in Mcm/d, + Delta-current month, + current month. demand +delta = desk -current month

import time

import dash
import dash_html_components as html
import dash_core_components as dcc
from dash.dependencies import Input, Output
from dash.exceptions import PreventUpdate

import numpy as np

import sys

import datetime
import numpy as np
import pandas as pd

from dateutil.relativedelta import relativedelta
sys.path.append('C:\\Users\SVC-GASQuant2-Prod\YWwebsite\Python Scripts\class') 
import dash_table 
from DBtoDF import DBTOPD

pd.set_option('display.max_columns',20)

class AnalysisData():
    
    def supply_data():
        #read data from Kpler
        Kpler=DBTOPD('PRD-DB-SQL-211','LNG', 'dbo', 'KplerLNGTrades')
        dfkpler=Kpler.sql_to_df()

        #get supply and demand df
        supply_plant_columns=['DateOrigin','InstallationOrigin','VolumeOriginM3']
        df_supply_plant = dfkpler[supply_plant_columns]
        
        SupplyCurveId = pd.read_excel('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/DeskViewID.xlsx',header=(0),sheet_name='supply')
        #create supply list
        SupplyCurveId=SupplyCurveId[['CurveID','plant']]
        supply_plant_list=SupplyCurveId['plant'].values.tolist()
        
        start_date='2019-01-01' 
        today=datetime.date.today()
        dfsupply=pd.DataFrame(index=pd.date_range(start=start_date,end=today))
        dfsupply.index.name='Date'
        for i in supply_plant_list:
            
            dffulldate=pd.DataFrame(index=pd.date_range(start=start_date,end=today)) 
            dffulldate.index.name='Date'
            dfplant = df_supply_plant[df_supply_plant['InstallationOrigin']==i]
            dfplant.loc[:,'DateOrigin']=pd.to_datetime(dfplant.loc[:,'DateOrigin']).dt.strftime('%Y-%m-%d') 
            
            dfplant=dfplant.groupby(['DateOrigin']).sum()*0.000612027 #m3 to mcm EndOrigin
            dfplant.index.name='Date'
            dfplant.index=pd.DatetimeIndex(dfplant.index)
            
            dfsupply[i] = pd.merge(dffulldate, dfplant.loc[start_date: today], on='Date', how='outer')
            dfsupply[i].fillna(0, inplace=True)
            
        #print(dfsupply.tail())
        SupplyCategories = pd.read_excel('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/Categories.xlsx',header=(0),sheet_name='Index_Supply')
        SupplyCategories = SupplyCategories.iloc[:44,0:5]
        SupplyCategories.set_index('Plant', inplace=True)
        
        #get desk view
        desk=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'DeskSupplyPlant')
        dfsupplydesk=desk.sql_to_df()
        dfsupplydesk.set_index('Date', inplace=True)
        
        return dfsupply,SupplyCategories,dfsupplydesk
    
    def terminal_data(dfsupply):
        
        capa= pd.read_csv('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/IHSCapa.csv',header=(0))
        capa.set_index('Liquefaction Project', inplace=True)
        '''get supply plant and desk supply'''
        #supply=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'SupplyPlant')
        #dfsupply=supply.sql_to_df()
        #dfsupply.set_index('Date', inplace=True)
        dfsupply=dfsupply/0.000612027 #mcm to m3
        #get date time
        today=dfsupply.index[-1]
        start_date=today-datetime.timedelta(1)*365
        start=today-datetime.timedelta(1)*14

        dfsupplyplant=dfsupply.loc[start:]*0.000000438 #change m3 to mt
        dfsupplyplant15=dfsupply.loc[today-datetime.timedelta(1)*15:]*0.000000438
        column=dfsupplyplant.columns.tolist()
        column.remove('Kollsnes')
        column.remove('Marsa El Brega')
        column.remove('Senkang')
        column.remove('Stavanger')
        column.remove('Tango FLNG')
        column.remove('Vysotsk')
        
        df=pd.DataFrame(index=column)
        df['Avg. Loading 14 days (cargos)']=np.nan
        df['1 year Weekly average (cargos)']=np.nan
        #print(dfsupply)
        for i in df.index:
            df.loc[i,'Avg. Loading 14 days (cargos)']=dfsupplyplant[i].sum()/0.063/14*7
            df.loc[i,'1 year Weekly average (cargos)'] = dfsupply.loc[start_date:today,i].sum()*0.000000438/0.063/365*7
            df.loc[i,'Capacity (Mt)'] = capa.loc[i,'Initial_Capacity']
            df.loc[i,'Avg. Loading 15 days (cargos)']=dfsupplyplant15[i].sum()/0.063/15*7

        df['Delta (cargos)']=df['Avg. Loading 14 days (cargos)'] - df['1 year Weekly average (cargos)']
        df['Per week Capacity (cargos)'] = df['Capacity (Mt)']/52/0.0625
        df['Per Month Capacity (cargos)'] = df['Per week Capacity (cargos)']*4
        df['15 days Utilisation'] = (df['Avg. Loading 15 days (cargos)']/df['Per week Capacity (cargos)']).apply(lambda x:'{:.0%}'.format(x)) #cargo/mt????????
        column_order=df.columns.tolist()
        column_order.remove('Avg. Loading 15 days (cargos)')
        column_order.remove('Capacity (Mt)')
        column_order.insert(0, 'Capacity (Mt)')
        df=df[column_order]
        df=df.T

        df=df.round(2)
        df.reset_index(inplace=True)
        
        dfhist=dfsupplyplant.iloc[-14:,]
        dfhist.reset_index(inplace=True)
        dfhist.rename(columns={'Date':'index'}, inplace=True)
        dfhist.loc[:,'index']=dfhist.loc[:,'index'].copy().dt.strftime('%Y-%m-%d')
        dfhist=dfhist.iloc[::-1]
        
        #get ETA data
        KplerETA=DBTOPD('PRD-DB-SQL-211','LNG', 'dbo', 'KplerLNGTrades') #new DB 211
        dfkplerETA=KplerETA.get_eta_today()
        eta_supply_columns=['EtaOrigin','InstallationOrigin','VolumeOriginM3'] #new
        supply_eta = dfkplerETA[eta_supply_columns]
        dz=supply_eta.groupby(['EtaOrigin', 'InstallationOrigin'],as_index=False).sum()
        da=pd.pivot(dz, index='EtaOrigin', columns='InstallationOrigin')
        da.columns=da.columns.droplevel(0)
        da=da.resample('D').sum()
        dfeta=pd.DataFrame(index=pd.date_range(start=today+datetime.timedelta(1),periods=5).date, columns=column)
        for i in da.columns:
            dfeta.loc[:,i]=da.loc[:,i]
        dfeta=dfeta.iloc[::-1]*0.000000438
        dfeta=dfeta[column]
        dfeta=round(dfeta,2)
        dfeta.reset_index(inplace=True)
        
        dfchart=pd.concat([dfeta,dfhist, df])
        dfchart.iloc[:-1,1:]=round(dfchart.iloc[:-1,1:].astype(float),2)
        dfchart.reset_index(inplace=True)
        dfchart.drop(columns=['level_0'],axis=1, inplace=True)
        #dfchart.fillna(0,inplace=True)
        #print(dfchart)
        #print(dfchart['Sabine Pass'])
        
        return dfchart
    
    def terminalmcm_data(dfsupply,dfsupplydesk):
        
        capa = pd.read_csv('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/IHSCapa.csv',header=(0)) #check
        capa.set_index('Liquefaction Project', inplace=True)
        #print(capa.loc['Sabine Pass'])
        #Kpler=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'IHScapa')
        #dfcapa=Kpler.sql_to_df()
        #dfcapa.set_index('index', inplace=True)
        #print(dfcapa)
        #capa=capa.T
        #capa.columns=['Initial_Capacity']
        #capa=capa.rename(columns ={capa.columns: 'Initial_Capacity'})
        
        #print(capa)
        
        '''get supply plant and desk supply'''
        #supply=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'SupplyPlant')
        #dfsupply=supply.sql_to_df()
        #dfsupply.set_index('Date', inplace=True)
        #dfsupply=dfsupply/0.000612027 #mcm to m3
        #get date time
        today=dfsupply.index[-1]
        start_date=today-datetime.timedelta(1)*365
        start=today-datetime.timedelta(1)*14

        dfsupplyplant=dfsupply.loc[start:] #change m3 to mt
        dfsupplyplant15=dfsupply.loc[today-datetime.timedelta(1)*15:]

        column=dfsupplyplant.columns.tolist()
        column.remove('Kollsnes')
        column.remove('Marsa El Brega')
        column.remove('Senkang')
        column.remove('Stavanger')
        column.remove('Tango FLNG')
        column.remove('Vysotsk')
        
        df=pd.DataFrame(index=column)
        
        df['Current month desk view (Mcm/d)'] = dfsupplydesk.loc[str(today)]
        df['Current month average EndOrigin (Mcm/d)'] = dfsupply.loc[str(today.year)+'-'+str(today.month)+'-01':today].sum()/today.day
        #print(df)
        df['Desk - Current Month Delta (Mcm/d)'] = df['Current month desk view (Mcm/d)'] - df['Current month average EndOrigin (Mcm/d)']
        df['Avg. Loading 14 days (Mcm/d)']=np.nan
        df['1 year Weekly average (Mcm/d)']=np.nan
        df['365 days Utilisation']=np.nan
        #print(dfsupply)
        for i in df.index:
            df.loc[i,'Avg. Loading 14 days (Mcm/d)']=dfsupplyplant[i].sum()/14 # Mcm/d
            df.loc[i,'Avg. Loading 15 days (cargos)']=dfsupplyplant15[i].sum()/0.000612027/0.063/15*7*0.000000438
            df.loc[i,'1 year Weekly average (Mcm/d)'] = dfsupply.loc[start_date:today,i].sum()/365
            df.loc[i,'Capacity (Mt)'] = capa.loc[i,'Initial_Capacity']
            df.loc[i,'365 days Utilisation'] = (dfsupply.loc[start_date:today,i].copy().sum()/0.000612027*0.000000438/df.loc[i,'Capacity (Mt)'].copy())

        df['Delta (Mcm/d)']=df['Avg. Loading 14 days (Mcm/d)'] - df['1 year Weekly average (Mcm/d)']
        df['Per week Capacity (cargos)'] = df['Capacity (Mt)']/52/0.0625
        df['Per Month Capacity (cargos)'] = df['Per week Capacity (cargos)']*4
        df['15 days Utilisation'] = (df['Avg. Loading 15 days (cargos)']/df['Per week Capacity (cargos)']).apply(lambda x:'{:.0%}'.format(x)) #cargo/mt????????
        
        column_order=df.columns.tolist()
        column_order.remove('Avg. Loading 15 days (cargos)')
        column_order.remove('Capacity (Mt)')
        column_order.remove('365 days Utilisation')
        column_order.insert(0, '365 days Utilisation')
        column_order.insert(0, 'Capacity (Mt)')
        df=df[column_order]
        df=df.T

        df=df.round(2)
        df.reset_index(inplace=True)
        #get hist 14 days actual
        dfhist=dfsupplyplant.iloc[-14:,]
        dfhist=dfhist#*0.000612027/0.000612027
        dfhist.reset_index(inplace=True)
        dfhist.rename(columns={'Date':'index'}, inplace=True)
        dfhist['index']=dfhist['index'].dt.strftime('%Y-%m-%d')
        dfhist=dfhist.iloc[::-1]
        
        #get ETA data
        KplerETA=DBTOPD('PRD-DB-SQL-211','LNG', 'dbo', 'KplerLNGTrades') #new DB 211
        dfkplerETA=KplerETA.get_eta_today()
        eta_supply_columns=['EtaOrigin','InstallationOrigin','VolumeOriginM3'] #new
        supply_eta = dfkplerETA[eta_supply_columns]
        dz=supply_eta.groupby(['EtaOrigin', 'InstallationOrigin'],as_index=False).sum()
        da=pd.pivot(dz, index='EtaOrigin', columns='InstallationOrigin')
        da.columns=da.columns.droplevel(0)
        da=da.resample('D').sum()
        dfeta=pd.DataFrame(index=pd.date_range(start=today+datetime.timedelta(1),periods=5).date, columns=column)
        for i in da.columns:
            dfeta.loc[:,i]=da.loc[:,i]
        dfeta=dfeta.iloc[::-1]
        dfeta=dfeta[column]
        dfeta=dfeta*0.000612027
        dfeta=round(dfeta,2)
        dfeta.reset_index(inplace=True)
        
        #table data        
        dfchart=pd.concat([dfeta,dfhist, df])
        #print(dfchart)
        dfchart.iloc[:-1,1:]=round(dfchart.iloc[:-1,1:].astype(float),2)
        #print(dfchart)
        dfchart.iloc[-10,1:] = dfchart.iloc[-10,1:].apply(lambda x:'{:.0%}'.format(x))
        dfchart.reset_index(inplace=True)
        dfchart.drop(columns=['level_0'],axis=1, inplace=True)
        rename={'Avg. Loading 14 days (cargos)':'Avg. Weekly Loading 14 days (cargos)', 'Delta (Mcm/d)':'Weekly Delta (Mcm/d)'}
        dfchart.replace(rename, inplace=True)
        #print(dfchart)
        dfchart=dfchart.reindex(index=[19,20,29,0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,21,22,23,24,25,26,27,28])
      
        #print(dfchart['Sabine Pass'])
        
        return dfchart

    
    def terminal_uti_data(dfsupply, dfterminal,SupplyCategories):
        
        dfsupply=dfsupply/0.000612027 #mcm to m3
        dfsupplyplant=dfsupply*0.000000438 #m3 to mt
        
        today=dfsupply.index[-1].date()
        #print(today)
        c_week=today-datetime.timedelta(1)*11
        l_week=c_week-datetime.timedelta(1)*7
        yesterday=today-datetime.timedelta(1)
        month_before=str((today-relativedelta(months=1)).year)+'-'+str((today-relativedelta(months=1)).month)
        year_before =str(today.year-1)+'-'+str(today.month)
        current_year_start=str(today.year)+'-01-01'
        current_year_end=str(today.year)+'-12-31'
        last_year_start=str(today.year-1)+'-01-01'
        #print(dfsupplyplant)
        #dfterminal=AnalysisData.terminal_data()
        dfterminal.set_index('index',inplace=True)
        
        
        
        #print(SupplyCategories)
        dfuti = pd.DataFrame(dfterminal.loc['Capacity (Mt)'])
        dfuti['Basin']=np.nan
        dfuti['Country']=np.nan
        dfuti['Current Week 10 Day Rolling Avg '+str(yesterday)]=dfsupplyplant.loc[str(c_week):str(yesterday)].mean()/(dfuti.loc[:,'Capacity (Mt)']/365)
        dfuti['Last Week 10 Day Rolling Avg '+str(yesterday-datetime.timedelta(1)*7)]=dfsupplyplant.loc[str(l_week):str(yesterday-datetime.timedelta(1)*7)].mean()/(dfuti.loc[:,'Capacity (Mt)']/365)
        dfuti['M-1 '+month_before] = dfsupplyplant.loc[month_before].sum()*12/dfuti.loc[:,'Capacity (Mt)']
        dfuti['Y-1 '+year_before] = dfsupplyplant.loc[year_before].sum()*12/dfuti.loc[:,'Capacity (Mt)']
        dfuti['Loads in a Week last 14 days(Cargos)']=dfterminal.loc['Avg. Loading 14 days (cargos)']  #mt not cargo
        dfuti['1 year Weekly average (cargos)']=dfterminal.loc['1 year Weekly average (cargos)']
        dfuti['Delta(Cargos)']=dfterminal.loc['Delta (cargos)']
        dfuti['Mcm/d (Cargos)']=dfsupply.loc[today-datetime.timedelta(1)*10:today].mean()*0.000612027 #m3 to mcm
        dfuti['W-o-W % (Cargos)']=np.nan
        
        for i in dfuti.index:
            if dfuti.loc[i,'1 year Weekly average (cargos)'] !=0:
                #print(i)
                dfuti.loc[i,'W-o-W % (Cargos)']=(dfuti.loc[i,'Delta(Cargos)']/dfuti.loc[i,'1 year Weekly average (cargos)'])
        
        #dfuti['Delta signal']=dfterminal.loc['Delta (cargos)']/dfterminal.loc['1 year Weekly average (cargos)']
        #dfuti['Delta signal'] = dfuti['Delta signal']#.apply(lambda x: '↗️' if x > 0 else '↘️')
        for i in dfuti.index:
            try:
                dfuti.loc[i,'Basin'] = SupplyCategories.loc[i,'Basin']
                dfuti.loc[i,'Country'] = SupplyCategories.loc[i,'Market']
            except KeyError:
                continue
        dfuti=dfuti.round(2)
        dfuti.reset_index(inplace=True)
        dfuti.rename(columns={'index':'Terminal'},inplace=True)  
        #print('dftui',dfuti.head())
        
        dfutisum=dfuti.copy()
        dfutisum.replace('MENA_Bas','MENA',inplace=True)
        dfutisum.set_index('Basin', inplace=True)
        #print(dfsupplyplant)
        supplybasin=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'BasinSupply')
        dfsupplybasin=supplybasin.sql_to_df()
        dfsupplybasin.set_index('Date', inplace=True)
        dfsupplybasin=dfsupplybasin/0.000612027*0.000000438 #mcm to m3
        
        dfutiregion=pd.DataFrame(index=['Global','AB','MENA','PB'], columns=dfuti.columns)
        for i in ['AB','MENA','PB']:
            dfutiregion.loc[i, 'Capacity (Mt)']=dfutisum.loc[i,'Capacity (Mt)'].sum().round(2)
            dfutiregion.loc[i,'Current Week 10 Day Rolling Avg '+str(yesterday)]=round(dfsupplybasin[i].loc[str(c_week):str(yesterday)].mean()/(dfutiregion.loc[i,'Capacity (Mt)']/365),2)
            dfutiregion.loc[i,'Last Week 10 Day Rolling Avg '+str(yesterday-datetime.timedelta(1)*7)]=round(dfsupplybasin[i].loc[str(l_week):str(yesterday-datetime.timedelta(1)*7)].mean()/(dfutiregion.loc[i,'Capacity (Mt)']/365),2)
            dfutiregion.loc[i,'M-1 '+month_before] = round(dfsupplybasin[i].loc[month_before].sum()/(dfutiregion.loc[i,'Capacity (Mt)']/12),2)
            dfutiregion.loc[i,'Y-1 '+year_before] = round(dfsupplybasin[i].loc[str(today.year-1)].sum()/(dfutiregion.loc[i,'Capacity (Mt)']),2)
            dfutiregion.loc[i,'Loads in a Week last 14 days(Cargos)'] = round(dfutisum.loc[i,'Loads in a Week last 14 days(Cargos)'].sum(),2)
            dfutiregion.loc[i,'1 year Weekly average (cargos)'] = round(dfutisum.loc[i,'1 year Weekly average (cargos)'].sum(),2)
            dfutiregion.loc[i,'Delta(Cargos)'] = round((dfutiregion.loc[i,'Loads in a Week last 14 days(Cargos)'] - dfutiregion.loc[i,'1 year Weekly average (cargos)']),2)
            dfutiregion.loc[i,'Mcm/d (Cargos)']=round(dfsupplybasin[i].loc[today-datetime.timedelta(1)*10:today].mean()*0.000612027/0.000000438,2) #m3 to mcm
            dfutiregion.loc[i,'W-o-W % (Cargos)']=round((dfutiregion.loc[i,'Delta(Cargos)']/dfutiregion.loc[i,'1 year Weekly average (cargos)']),2)
            
        dfutiregion.loc['Global','Capacity (Mt)']=dfutisum.loc[:,'Capacity (Mt)'].sum().round(2)
        dfsupplyglobal=dfsupplybasin[['AB','PB','MENA']].loc[last_year_start:current_year_end].sum(axis=1)
        #print(dfsupplyglobal)
        dfutiregion.loc['Global','Current Week 10 Day Rolling Avg '+str(yesterday)]=(dfsupplyglobal.loc[str(c_week):str(yesterday)].mean()/(dfutiregion.loc['Global','Capacity (Mt)']/365)).round(2)
        dfutiregion.loc['Global','Last Week 10 Day Rolling Avg '+str(yesterday-datetime.timedelta(1)*7)]=(dfsupplyglobal.loc[str(l_week):str(yesterday-datetime.timedelta(1)*7)].mean()/(dfutiregion.loc['Global','Capacity (Mt)']/365)).round(2)
        dfutiregion.loc['Global','M-1 '+month_before]= dfsupplyglobal.loc[month_before].sum()*12/dfutiregion.loc['Global','Capacity (Mt)']
        dfutiregion.loc['Global','Y-1 '+year_before]=dfsupplybasin[['AB','MENA','PB']].sum(axis=1).loc[str(today.year-1)+'-'+str(today.month)].sum()*12/dfutiregion.loc['Global','Capacity (Mt)']
        dfutiregion.loc['Global','Loads in a Week last 14 days(Cargos)']=dfutisum.loc[:,'Loads in a Week last 14 days(Cargos)'].sum().round(2)
        dfutiregion.loc['Global','1 year Weekly average (cargos)']=dfutisum.loc[:,'1 year Weekly average (cargos)'].sum().round(2)
        dfutiregion.loc['Global','Delta(Cargos)']=round((dfutiregion.loc['Global','Loads in a Week last 14 days(Cargos)'] - dfutiregion.loc['Global','1 year Weekly average (cargos)']),2)
        dfutiregion.loc['Global','Mcm/d (Cargos)']=round(dfsupplyglobal.loc[today-datetime.timedelta(1)*10:today].mean()/0.000000438*0.000612027,2) #m3 to mcm
        dfutiregion.loc['Global','W-o-W % (Cargos)']=round((dfutiregion.loc['Global','Delta(Cargos)']/dfutiregion.loc['Global','1 year Weekly average (cargos)']),2)
       
        #dfutiregion.loc['Global':'PB','Delta signal'] = dfutiregion.loc['Global':'PB','Delta(Cargos)']/ dfutiregion.loc['Global':'PB','1 year Weekly average (cargos)']#.apply(lambda x: '↗️' if x > 0 else '↘️')
        dfutiregion.reset_index(inplace=True)
        #print(dfutiregion.head())
        
        dfutiregion.loc[:,'Country']=dfutiregion.loc[:,'index']
        dfutiregion.loc[:,'Terminal']=dfutiregion.loc[:,'index']
        dfutiregion.drop(columns='index', inplace=True)
        dfutiregion=dfutiregion.round(2)
        #print(dfutiregion.tail())
        #print(dfuti.tail())
        
        dfutiall=pd.concat([dfutiregion, dfuti])
        dfutiall['W-o-W % (Cargos)']=dfutiall['W-o-W % (Cargos)'].fillna(0).apply(lambda x:format(x, '.0%'))
        #dfutiall['W-o-W % (Cargos)']=(dfutiall['Delta(Cargos)']/dfutiall['1 year Weekly average (cargos)'])#.fillna(0).apply(lambda x:format(x, '.2%'))
        #dfutiall['Delta signal']=dfutiall.loc['Delta']
        #dfutiall['Delta signal'] = dfutiall['Delta signal'].apply(lambda x: '↗️' if x > 0 else '↘️')
        
        #dfutiall.reset_index(inplace=True)
        #print(dfutiall.head())
        #dfutiall.rename(columns={'index':'Basin'},inplace=True) 
        #print(dfutiall.columns)
        dfutiall['Basin'].iloc[0:3]=np.nan
        #print(dfutiall.columns)
        column=['Terminal','Capacity (Mt)','Country','Basin']
        column=column+dfutiall.columns.tolist()[4:]
        #print(column)
        dfutiall=dfutiall[column]
        dfutiall=round(dfutiall,2)
        #print('1',dfutiall)
        
        dfutiallP=dfutiall.copy()
        columnP=['Current Week 10 Day Rolling Avg '+str(yesterday),'Last Week 10 Day Rolling Avg '+str(yesterday-datetime.timedelta(1)*7),'M-1 '+month_before,'Y-1 '+year_before]
        #print(dfutiallP.columns)
        for c in columnP:
            #print(c)
            #print(dfutiallP.columns)
            dfutiallP.loc[:,c] = dfutiallP.loc[:, c].fillna(0).apply(lambda x:'{:.0%}'.format(x)) # 
        #dfutiall.loc[:,columnP]=dfutiall.loc[:,columnP].fillna(0).apply('{:.2%}'.format)
        #print(dfutiallP.columns)
        #print(dfutiallP.head())
        return dfutiall, dfutiallP
    
    def terminal_cargo_data(dfsupply, dfterminal,SupplyCategories):
    
        dfsupply=dfsupply/0.000612027 #mcm to m3
        dfsupplyplant=dfsupply*0.000000438/0.063 #m3 to mt to cargo
        #print(dfsupplyplant)
        today=dfsupply.index[-1].date()
        c_week=today-datetime.timedelta(1)*11
        l_week=c_week-datetime.timedelta(1)*7
        yesterday=today-datetime.timedelta(1)
        month_before=str((today-relativedelta(months=1)).year)+'-'+str((today-relativedelta(months=1)).month)
        year_before =str(today.year-1)+'-'+str(today.month)
        current_year_start=str(today.year)+'-01-01'
        current_year_end=str(today.year)+'-12-31'
        last_year_start=str(today.year-1)+'-01-01'
        #dfterminal=AnalysisData.terminal_data()
        #print(dfterminal.head())
        #dfterminal.set_index('index',inplace=True)
        
        
        #print(SupplyCategories)
        dfcargo= pd.DataFrame(dfterminal.loc['Capacity (Mt)'])
        dfcargo['Basin']=np.nan
        dfcargo['Country']=np.nan
        dfcargo['Current Week Cargoes Count '+str(yesterday)]=dfsupplyplant.loc[str(c_week):str(yesterday)].sum()
        dfcargo['Last Week Cargoes Count '+str(yesterday-datetime.timedelta(1)*7)]=dfsupplyplant.loc[str(l_week):str(yesterday-datetime.timedelta(1)*7)].sum()
        dfcargo['M-1 '+month_before] = round(dfsupplyplant.loc[month_before].sum(),2)
        dfcargo['Y-1 '+year_before] = round(dfsupplyplant.loc[year_before].sum(),2)
        dfcargo['W-o-W (Cargos)']= round((dfcargo['Current Week Cargoes Count '+str(yesterday)] - dfcargo['Last Week Cargoes Count '+str(yesterday-datetime.timedelta(1)*7)]),2)
        
        #dfcargo['Delta signal']=dfcargo['W-o-W (Cargos)']
        #dfcargo['Delta signal'] = dfcargo['Delta signal']#.apply(lambda x: '↗️' if x > 0 else '↘️')
        for i in dfcargo.index:
            try:
                dfcargo.loc[i,'Basin'] = SupplyCategories.loc[i,'Basin']
                dfcargo.loc[i,'Country'] = SupplyCategories.loc[i,'Market']
            except KeyError:
                continue
        dfcargo=dfcargo.round(2)
        dfcargo.reset_index(inplace=True)
        dfcargo.rename(columns={'index':'Terminal'},inplace=True)  
        #print('dftui',dfcargo.head())
        
        dfcargosum=dfcargo.copy()
        dfcargosum.replace('MENA_Bas','MENA',inplace=True)
        dfcargosum.set_index('Basin', inplace=True)
        #print(dfsupplyplant)
        supplybasin=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'BasinSupply')
        dfsupplybasin=supplybasin.sql_to_df()
        dfsupplybasin.set_index('Date', inplace=True)
        dfsupplybasin=dfsupplybasin/0.000612027*0.000000438/0.063 #mcm to m3 to cargo
        
        dfcargoregion=pd.DataFrame(index=['Global','AB','MENA','PB'], columns=dfcargo.columns)
        for i in ['AB','MENA','PB']:
            dfcargoregion.loc[i, 'Capacity (Mt)']=dfcargosum.loc[i,'Capacity (Mt)'].sum().round(2)
            dfcargoregion.loc[i,'Current Week Cargoes Count '+str(yesterday)]=round(dfsupplybasin[i].loc[str(c_week):str(yesterday)].sum(),2)
            dfcargoregion.loc[i,'Last Week Cargoes Count '+str(yesterday-datetime.timedelta(1)*7)]=round(dfsupplybasin[i].loc[str(l_week):str(yesterday-datetime.timedelta(1)*7)].sum(),2)
            dfcargoregion.loc[i,'M-1 '+month_before] = round(dfsupplybasin[i].loc[month_before].sum(),2)
            dfcargoregion.loc[i,'Y-1 '+year_before] = round(dfsupplybasin[i].loc[str(today.year-1)+'-'+str(today.month)].sum(),2)
            dfcargoregion.loc[i,'W-o-W (Cargos)']=round((dfcargoregion.loc[i,'Current Week Cargoes Count '+str(yesterday)] - dfcargoregion.loc[i,'Last Week Cargoes Count '+str(yesterday-datetime.timedelta(1)*7)]),2)
            
        dfcargoregion.loc['Global','Capacity (Mt)']=dfcargosum.loc[:,'Capacity (Mt)'].sum().round(2)
        dfsupplyglobal=dfsupplybasin[['AB','PB','MENA']].loc[last_year_start:current_year_end].sum(axis=1)
        #print(dfsupplyglobal)
        dfcargoregion.loc['Global','Current Week Cargoes Count '+str(yesterday)] = (dfsupplyglobal.loc[str(c_week):str(yesterday)].sum()).round(2)
        dfcargoregion.loc['Global','Last Week Cargoes Count '+str(yesterday-datetime.timedelta(1)*7)]=(dfsupplyglobal.loc[str(l_week):str(yesterday-datetime.timedelta(1)*7)].sum()).round(2)
        dfcargoregion.loc['Global','M-1 '+month_before]= round(dfsupplyglobal.loc[month_before].sum(),2)
        dfcargoregion.loc['Global','Y-1 '+year_before]=round(dfsupplybasin[['AB','MENA','PB']].sum(axis=1).loc[str(today.year-1)+'-'+str(today.month)].sum(),2)
        dfcargoregion.loc['Global','W-o-W (Cargos)'] = round((dfcargoregion.loc['Global','Current Week Cargoes Count '+str(yesterday)] - dfcargoregion.loc['Global','Last Week Cargoes Count '+str(yesterday-datetime.timedelta(1)*7)]),2)
        #dfcargoregion.loc['Global':'PB','Delta signal'] = dfcargoregion.loc['Global':'PB','W-o-W (Cargos)']#.apply(lambda x: '↗️' if x > 0 else '↘️')
        dfcargoregion.reset_index(inplace=True)
        #print(dfcargoregion.head())
        
        dfcargoregion.loc[:,'Country']=dfcargoregion.loc[:,'index']
        dfcargoregion.loc[:,'Terminal']=dfcargoregion.loc[:,'index']
        dfcargoregion.drop(columns='index', inplace=True)
        dfcargoregion=dfcargoregion.round(2)
        dfcargoall=pd.concat([dfcargoregion, dfcargo])

        dfcargoall.reset_index(inplace=True)
        #dfcargoall.rename(columns={'index':'Basin'},inplace=True) 
        #print(dfcargoall)
        dfcargoall['Basin'].iloc[0:3]=np.nan
        
        column=['Terminal','Capacity (Mt)','Country','Basin']
        column=column+dfcargoall.columns.tolist()[5:]
        
        dfcargoall=dfcargoall[column]
        dfcargoall=round(dfcargoall,2)
        
        
        return dfcargoall
    
    def terminal_vol_data(dfsupply, dfterminal,SupplyCategories):
        
        dfsupply=dfsupply/0.000612027 #mcm to m3
        dfsupplyplant=dfsupply*0.000000438 #m3 to mt
        
        today=dfsupply.index[-1].date()
        c_week=today-datetime.timedelta(1)*11
        l_week=c_week-datetime.timedelta(1)*7
        yesterday=today-datetime.timedelta(1)
        month_before=str((today-relativedelta(months=1)).year)+'-'+str((today-relativedelta(months=1)).month)
        year_before =str(today.year-1)+'-'+str(today.month)
        current_year_start=str(today.year)+'-01-01'
        current_year_end=str(today.year)+'-12-31'
        last_year_start=str(today.year-1)+'-01-01'
        #print(dfsupplyplant)
        
        
        #print(SupplyCategories)
        dfvol = pd.DataFrame(dfterminal.loc['Capacity (Mt)'])
        dfvol['Basin']=np.nan
        dfvol['Country']=np.nan
        dfvol['Current Week Volumes '+str(yesterday)]=dfsupplyplant.loc[str(c_week):str(yesterday)].sum()
        dfvol['Last Week Volumes '+str(yesterday-datetime.timedelta(1)*7)]=dfsupplyplant.loc[str(l_week):str(yesterday-datetime.timedelta(1)*7)].sum()
        dfvol['M-1 '+month_before] = dfsupplyplant.loc[month_before].sum()
        dfvol['Y-1 '+year_before] = dfsupplyplant.loc[year_before].sum()
        dfvol['W-o-W']=dfvol['Current Week Volumes '+str(yesterday)] - dfvol['Last Week Volumes '+str(yesterday-datetime.timedelta(1)*7)]
        
        #dfvol['Delta signal'] = dfvol['W-o-W']
        #dfvol['Delta signal'] = dfvol['Delta signal']#.apply(lambda x: '↗️' if x > 0 else '↘️')
        for i in dfvol.index:
            try:
                dfvol.loc[i,'Basin'] = SupplyCategories.loc[i,'Basin']
                dfvol.loc[i,'Country'] = SupplyCategories.loc[i,'Market']
            except KeyError:
                continue
        dfvol=dfvol.round(2)
        dfvol.reset_index(inplace=True)
        dfvol.rename(columns={'index':'Terminal'},inplace=True)  
        #print('dftui',dfvol.head())
        
        dfvolsum=dfvol.copy()
        dfvolsum.replace('MENA_Bas','MENA',inplace=True)
        dfvolsum.set_index('Basin', inplace=True)
        #print(dfsupplyplant)
        supplybasin=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'BasinSupply')
        dfsupplybasin=supplybasin.sql_to_df()
        dfsupplybasin.set_index('Date', inplace=True)
        dfsupplybasin=dfsupplybasin/0.000612027*0.000000438 #mcm to m3 to mt
        
        dfvolregion=pd.DataFrame(index=['Global','AB','MENA','PB'], columns=dfvol.columns)
        for i in ['AB','MENA','PB']:
            dfvolregion.loc[i, 'Capacity (Mt)']=dfvolsum.loc[i,'Capacity (Mt)'].sum().round(2)
            dfvolregion.loc[i,'Current Week Volumes '+str(yesterday)]=round(dfsupplybasin[i].loc[str(c_week):str(yesterday)].sum(),2)
            dfvolregion.loc[i,'Last Week Volumes '+str(yesterday-datetime.timedelta(1)*7)]=round(dfsupplybasin[i].loc[str(l_week):str(yesterday-datetime.timedelta(1)*7)].sum(),2)
            dfvolregion.loc[i,'M-1 '+month_before] = round(dfsupplybasin[i].loc[month_before].sum(),2)
            dfvolregion.loc[i,'Y-1 '+year_before] = round(dfsupplybasin[i].loc[str(today.year-1)+'-'+str(today.month)].sum(),2)
            
            dfvolregion.loc[i,'W-o-W'] = round((dfvolregion.loc[i,'Current Week Volumes '+str(yesterday)] - dfvolregion.loc[i,'Last Week Volumes '+str(yesterday-datetime.timedelta(1)*7)]),2)
            
        dfvolregion.loc['Global','Capacity (Mt)']=dfvolsum.loc[:,'Capacity (Mt)'].sum().round(2)
        dfsupplyglobal=dfsupplybasin[['AB','PB','MENA']].loc[last_year_start:current_year_end].sum(axis=1)
        #print(dfsupplyglobal)
        dfvolregion.loc['Global','Current Week Volumes '+str(yesterday)]=(dfsupplyglobal.loc[str(c_week):str(yesterday)].sum()).round(2)
        dfvolregion.loc['Global','Last Week Volumes '+str(yesterday-datetime.timedelta(1)*7)]=(dfsupplyglobal.loc[str(l_week):str(yesterday-datetime.timedelta(1)*7)].sum()).round(2)
        dfvolregion.loc['Global','M-1 '+month_before]= round(dfsupplyglobal.loc[month_before].sum(),2)
        dfvolregion.loc['Global','Y-1 '+year_before] = round(dfsupplybasin[['AB','MENA','PB']].sum(axis=1).loc[str(today.year-1)+'-'+str(today.month)].sum(),2)
        
        dfvolregion.loc['Global','W-o-W'] = round((dfvolregion.loc['Global','Current Week Volumes '+str(yesterday)] - dfvolregion.loc['Global','Last Week Volumes '+str(yesterday-datetime.timedelta(1)*7)]),2)
       
        #dfvolregion.loc['Global':'PB','Delta signal'] = dfvolregion.loc['Global':'PB','W-o-W']#.apply(lambda x: '↗️' if x > 0 else '↘️')
        dfvolregion.reset_index(inplace=True)
        #print(dfvolregion.head())
        
        dfvolregion.loc[:,'Country']=dfvolregion.loc[:,'index']
        dfvolregion.loc[:,'Terminal']=dfvolregion.loc[:,'index']
        dfvolregion.drop(columns='index', inplace=True)
        dfvolregion=dfvolregion.round(2)
        dfvolall=pd.concat([dfvolregion, dfvol])
       
        
        dfvolall.reset_index(inplace=True)
        #dfvolall.rename(columns={'index':'Basin'},inplace=True) 
        #print(dfvolall)
        dfvolall['Basin'].iloc[0:3]=np.nan
        
        column=['Terminal','Capacity (Mt)','Country','Basin']
        column=column+dfvolall.columns.tolist()[5:]
        
        dfvolall=dfvolall[column]
        dfvolall=round(dfvolall,2)
        #print('1',dfvolall)
        return dfvolall


    def demand_data():
      #read data from Kpler
      Kpler=DBTOPD('PRD-DB-SQL-211','LNG', 'dbo', 'KplerLNGTrades')
      dfkpler=Kpler.sql_to_df()
      
      #get desk view
      desk=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'DeskDemand')
      dfdesk=desk.sql_to_df()
      dfdesk.set_index('Date', inplace=True)
      #get supply and demand df
      demand_country_columns=['StartDestination','CountryDestination','VolumeOriginM3']
      df_demand_country = dfkpler[demand_country_columns]
      
      DemandCurveId = pd.read_excel('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/DeskViewID.xlsx',header=(0),sheet_name='demand')
      #create supply list
      DemandCurveId=DemandCurveId[['CurveID','Country']]
      Demand_country_list=DemandCurveId['Country'].values.tolist()
      
      start_date='2019-01-01' 
      today=datetime.date.today()
      dfDemand=pd.DataFrame(index=pd.date_range(start=start_date,end=today))
      dfDemand.index.name='Date'
      for i in Demand_country_list:
          
          dffulldate=pd.DataFrame(index=pd.date_range(start=start_date,end=today)) 
          dffulldate.index.name='Date'
          #print(1)
          #print(i)
          dfcountry = df_demand_country.loc[df_demand_country['CountryDestination']==i]
          #print(2)
          dfcountry.loc[:,'StartDestination']=pd.to_datetime(dfcountry.loc[:,'StartDestination']).dt.strftime('%Y-%m-%d') 
          
          dfcountry=dfcountry.groupby(['StartDestination']).sum()*0.000612 #m3 to mcm 
          dfcountry.index.name='Date'
          dfcountry.index=pd.DatetimeIndex(dfcountry.index)
          
          dfDemand[i] = pd.merge(dffulldate, dfcountry.loc[start_date: today], on='Date', how='outer')
          dfDemand[i].fillna(0, inplace=True)
          
      
      #DemandCategories = pd.read_excel('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/Categories.xlsx',header=(0),sheet_name='Index_Demand')
      #DemandCategories = DemandCategories.iloc[:64,0:6]
      #DemandCategories.set_index('Country', inplace=True)
      
      
      return dfDemand,dfdesk #,DemandCategories
  
    def countrymcm_data(dfDemand,dfdesk):
   
       #get date time
       today=datetime.date.today()
       start_date=today-datetime.timedelta(1)*365
       start=today-datetime.timedelta(1)*14
       #get capa
       Kpler=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'IHSregascapa')
       dfcapa=Kpler.sql_to_df()
       dfcapa.set_index('index', inplace=True)
       #print(dfcapa)
       dfcapa.rename(columns={'China (Mainland)':'China','Singapore':'Singapore Republic'}, inplace=True)
       #capa=dfcapa.iloc[[-1]].T
       #print([today.strftime('%Y-%m-%d')])
       capa=dfcapa.loc[[today]].T
       capa.columns=['Initial_Capacity']
       capa.drop(['Gibraltar','Cambodia','Senegal','Nicaragua','Hong Kong','El Salvador','Mozambique'],inplace=True) #delete no curve id
       
       #print(dfdesk)
       
       '''get demand country and desk'''
       
       #dfDemand=dfDemand
       
       
       dfDemandmarket=dfDemand.loc[start:] 
       dfDemanddemand15=dfDemand.loc[today-datetime.timedelta(1)*15:]
       
       column=dfDemandmarket.columns.tolist()
       
       
       df=pd.DataFrame(index=column)
       df['Current month desk view (Mcm/d)'] = dfdesk.loc[str(today)]
       df['Current month average delivered (Mcm/d)'] = dfDemand.loc[str(today.year)+'-'+str(today.month)+'-01':today].sum()/today.day
       df['Desk - Current month average delivered (Mcm/d)'] = df['Current month desk view (Mcm/d)'] - df['Current month average delivered (Mcm/d)']
       df['Avg. Loading 14 days (Mcm/d)']=np.nan
       df['1 year average (Mcm/d)']=np.nan
       df['365 days Utilisation']=np.nan
       #print(dfDemand)
       for i in capa.index[1:]:#df.index:
           df.loc[i,'Avg. Loading 14 days (Mcm/d)']=dfDemandmarket[i].sum()/14  #Mcm/d
           df.loc[i,'Avg. Loading 15 days (Mcm/d)']=dfDemanddemand15[i].sum()/15
           df.loc[i,'1 year average (Mcm/d)'] = dfDemand.loc[start_date:today,i].sum()/365
           df.loc[i,'Capacity (Mt)'] = capa.loc[i,'Initial_Capacity']
           df.loc[i,'365 days Utilisation'] = (dfDemand.loc[start_date:today,i].copy().sum()/0.000612*0.000000438/df.loc[i,'Capacity (Mt)'].copy())
    
       df['Delta (Mcm/d)']=df['Avg. Loading 14 days (Mcm/d)'] - df['1 year average (Mcm/d)']
       df['Capacity (Mcm/d)'] = df['Capacity (Mt)']/365/0.000000438*0.000612
       #df['Per Month Capacity (Mcm)'] = df['Per week Capacity (Mcm)']*4
       df['15 days Utilisation'] = (df['Avg. Loading 15 days (Mcm/d)']/df['Capacity (Mcm/d)']).apply(lambda x:'{:.0%}'.format(x)) 
       
       column_order=df.columns.tolist()
       column_order.remove('Avg. Loading 15 days (Mcm/d)')
       column_order.remove('Capacity (Mt)')
       column_order.remove('365 days Utilisation')
       column_order.insert(0, '365 days Utilisation')
       column_order.insert(0, 'Capacity (Mt)')
       df=df[column_order]
       df=df.T
    
       df=df.round(2)
       df.reset_index(inplace=True)
       #print(df[['index','China']])
       #get hist 14 days actual
       dfhist=dfDemandmarket.iloc[-14:,]
       dfhist=dfhist#*0.000612027
       dfhist.reset_index(inplace=True)
       dfhist.rename(columns={'Date':'index'}, inplace=True)
       dfhist.loc[:,'index']=dfhist.loc[:,'index'].dt.strftime('%Y-%m-%d')
       dfhist=dfhist.iloc[::-1]
       #print(dfhist)
       
       #get ETA data
       KplerETA=DBTOPD('PRD-DB-SQL-211','LNG', 'dbo', 'KplerLNGTrades') #new DB 211
       dfkplerETA=KplerETA.get_eta_today()
       eta_demand_columns=['EtaDestination','CountryDestination','VolumeOriginM3'] #new
       demand_eta = dfkplerETA[eta_demand_columns]
       dz=demand_eta.groupby(['EtaDestination', 'CountryDestination'],as_index=False).sum()
       da=pd.pivot(dz, index='EtaDestination', columns='CountryDestination')
       da.columns=da.columns.droplevel(0)
       da=da.resample('D').sum()
       dfeta=pd.DataFrame(index=pd.date_range(start=today+datetime.timedelta(1),periods=5).date, columns=column)
       for i in da.columns:
           dfeta.loc[:,i]=da.loc[:,i]
       dfeta=dfeta.iloc[::-1]
       dfeta=dfeta[column]
       dfeta=dfeta*0.000612027
       dfeta=round(dfeta,2)
       dfeta.reset_index(inplace=True)
       
       #table data        
       dfchart=pd.concat([dfeta,dfhist, df])
       dfchart.iloc[:-1,1:]=round(dfchart.iloc[:-1,1:].astype(float),2)
       #print(dfchart)
       dfchart.iloc[-9,1:] = dfchart.iloc[-9,1:].apply(lambda x:'{:.0%}'.format(x)) #365 days uti
       dfchart.reset_index(inplace=True)
       dfchart.drop(columns=['level_0'],axis=1, inplace=True)
       rename={'Avg. Loading 14 days (cargos)':'Avg. Weekly Loading 14 days (cargos)', 'Delta (cargos)':'Weekly Delta (cargos)'}
       dfchart.replace(rename, inplace=True)
       #print(dfchart)
       dfchart=dfchart.reindex(index=[19,20,28,0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,21,22,23,24,25,26,27])
     
       #print(dfchart)
       return dfchart
 
    def get_new_data():
        
        dfsupply,SupplyCategories,dfsupplydesk = AnalysisData.supply_data()
        #get terminal data
        dfterminal= AnalysisData.terminal_data(dfsupply)
        #print(dfterminal['Sabine Pass'])
        dfterminalmcm= AnalysisData.terminalmcm_data(dfsupply,dfsupplydesk)
        #print(dfterminalmcm['Sabine Pass'])
        dfDemand,dfdesk = AnalysisData.demand_data()
        dfdemandcountry=AnalysisData.countrymcm_data(dfDemand,dfdesk)
       
        column = [] #columns for cal table
        for i in dfterminalmcm.columns:
            hd={"name":i,"id": i,'deletable' : True }
            column.append(hd)
            
        column_demand = [] #columns for cal table
        for i in dfdemandcountry.columns:
            hd={"name":i,"id": i,'deletable' : True }
            column_demand.append(hd)
        #print( datetime.datetime.strptime(dfterminal.loc[5,'index'], '%Y-%m-%d').date()-datetime.timedelta(1))
        #print('get_new_data at ', datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        #(styles, legend) = discrete_background_color_bins(dfterminal)
        
        dfutiall, dfutiallP = AnalysisData.terminal_uti_data(dfsupply, dfterminal,SupplyCategories)
        dfcargo = AnalysisData.terminal_cargo_data(dfsupply, dfterminal,SupplyCategories)
        dfvol = AnalysisData.terminal_vol_data(dfsupply, dfterminal,SupplyCategories)
        
        return dfterminal, dfterminalmcm, column, dfutiall, dfutiallP, dfcargo, dfvol, dfDemand,dfdemandcountry,column_demand
    
    
        
        

app = dash.Dash(__name__)




app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-content')
])


index_page = html.Div([
    dcc.Link('terminal', href='/terminal'),
    html.Br(),
    dcc.Link('terminal_uti', href='/terminal_uti'),
    html.Br(),
    dcc.Link('terminal_cargo', href='/terminal_cargo'),
    html.Br(),
    dcc.Link('terminal_vol', href='/terminal_vol'),
    
])

# Update the index
@app.callback(dash.dependencies.Output('page-content', 'children'),
              [dash.dependencies.Input('url', 'pathname')])
def display_page(pathname):
    if pathname == '/terminal':
        
        terminal_layout = html.Div([
        
        #html.Div([html.I("End of Day PnL")]), #page header
        #html.Hr(),
        
            html.Button('Download Terminal Table (need about 60s)', id='refrash'),
            dcc.Loading(id="loading-terminal", 
                            children=[
            html.Div(id='body-div-terminal'), ])       
        ])
        
        return terminal_layout
    
    if pathname == '/terminal_uti':
        
        terminal_uti_layout = html.Div([
        
        #html.Div([html.I("End of Day PnL")]), #page header
        #html.Hr(),
        
            html.Button('Download Terminal Utilization Table (need about 60s)', id='refrash_uti'),
            dcc.Loading(id="loading-uti", 
                            children=[
            html.Div(id='body-div-uti'), ])       
        ])
        
        return terminal_uti_layout
    
    if pathname == '/terminal_cargo':
        
        terminal_cargo_layout = html.Div([
        
        #html.Div([html.I("End of Day PnL")]), #page header
        #html.Hr(),
        
            html.Button('Download Terminal Cargo Table (need about 60s)', id='refrash_cargo'),
            dcc.Loading(id="loading-cargo", 
                            children=[
            html.Div(id='body-div-cargo'), ])       
        ])
        
        return terminal_cargo_layout
    
    if pathname == '/terminal_vol':
        
        terminal_vol_layout = html.Div([
        
        #html.Div([html.I("End of Day PnL")]), #page header
        #html.Hr(),
        
            html.Button('Download Terminal Volume Table (need about 60s)', id='refrash_vol'),
            dcc.Loading(id="loading-vol", 
                            children=[
            html.Div(id='body-div-vol'), ])       
        ])
        
        return terminal_vol_layout
    
    else:
        return index_page


@app.callback(
    Output(component_id='body-div-terminal', component_property='children'),
    Input(component_id='refrash', component_property='n_clicks'),
)
def update_output(n_clicks):
    
    if n_clicks is None:
        
        raise PreventUpdate
    
    else:

        dfterminal, dfterminalmcm, column, dfutiall, dfutiallP, dfcargo, dfvol, dfDemand,dfdemandcountry,column_demand = AnalysisData.get_new_data()
        
        terminal_table_layout = html.Div([
        #html.H1('Data updated at: '+ datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')),
        html.H3('Supply Data updated at: '+ datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')),
        html.Hr(),
        #supply table
        dash_table.DataTable(
            
            id='terminal',
            columns=column,
            #data=dfsupply.loc[3:].to_dict('records'),
            data =dfterminalmcm.to_dict('records'),
            #column_deletable=True,
            #editable=True,
            #sort_action='native',
            #filter_action="native",
            
            #row_deletable=True,
    
            style_cell={
                'whiteSpace': 'normal',
                'height': 'auto',
                'textAlign': 'center',
                #'border': '3px solid grey'
                #'backgroundColor': '#D9D9D9',
                'backgroundColor': '#DDEBF7',
                'minWidth': '200px',
            },
            style_cell_conditional=[
                
                {
                    'if': {
                        'row_index': 3,
                        },
                    'backgroundColor': '#92D050'
                },
                {
                    'if': {
                        'row_index': 4,
                        },
                    'backgroundColor': '#92D050'
                },
                {
                    'if': {
                        'row_index': 5,
                        },
                    'backgroundColor': '#92D050'
                },
                {
                    'if': {
                        'row_index': 6,
                        },
                    'backgroundColor': '#92D050'
                },
                {
                    'if': {
                        'row_index': 7,
                        },
                    'backgroundColor': '#92D050'
                },
                
                {
                    'if': {
                        'row_index': 22,
                        },
                    'backgroundColor': 'white'
                },
                {
                    'if': {
                        
                        'row_index': 23,
                        },
                    'backgroundColor': 'white'
                },
                
                
                {
                    'if': {
                        'row_index': 24,
                        },
                    'backgroundColor': 'white'
                },
                {
                    'if': {
                        'row_index': 25,
                        },
                    'backgroundColor': 'white'
                },
                {
                    'if': {
                        'row_index': 26,
                        },
                    'backgroundColor': 'white'
                },  
                {
                    'if': {
                        'row_index': 27,
                        },
                    'backgroundColor': 'white'
                },  
                {
                    'if': {
                        'row_index': 28,
                        },
                    'backgroundColor': 'white'
                },  
                {
                    'if': {
                        'row_index': 29,
                        },
                    'backgroundColor': 'white'
                },  
                
            ],
            
            style_data_conditional= ( 
                #color the delta row
                [
                {
                    'if': {
                        'filter_query': '{{{col}}} > {value}'.format(col=i.rstrip('\"'), value=0),
                        'column_id': i,
                         'row_index': 24,
                        
                    },
                    'backgroundColor': 'green',
                    'color': 'white'
                } for i in dfterminalmcm.columns

            ]+
                 [
                {
                    'if': {
                        'filter_query': '{{{col}}} < {value}'.format(col=i.rstrip('\"'), value=0),
                        'column_id': i,
                         'row_index': 24,  
                    },
                    'backgroundColor': 'Pink',
                    'color': 'white'
                } for i in dfterminalmcm.columns

            ]+
                 [
                {
                    'if': {
                        'filter_query': '{{{col}}} > {value}'.format(col=i.rstrip('\"'), value=0),
                        'column_id': i,
                         'row_index': 2,
                        
                    },
                    'backgroundColor': 'green',
                    'color': 'white'
                } for i in dfterminalmcm.columns

            ]+
                 [
                {
                    'if': {
                        'filter_query': '{{{col}}} < {value}'.format(col=i.rstrip('\"'), value=0),
                        'column_id': i,
                         'row_index': 2,  
                    },
                    'backgroundColor': 'Pink',
                    'color': 'white'
                } for i in dfterminalmcm.columns

            ]+
                [
                {
                    'if': {
                        'filter_query': '{{{col}}} > {value}'.format(col=i.rstrip('\"'), value=0),
                        'column_id': i,
                         'row_index': 27,
                        
                    },
                    'backgroundColor': 'green',
                    'color': 'white'
                } for i in dfterminalmcm.columns

            ]+
                 [
                {
                    'if': {
                        'filter_query': '{{{col}}} < {value}'.format(col=i.rstrip('\"'), value=0),
                        'column_id': i,
                         'row_index': 27,  
                    },
                    'backgroundColor': 'Pink',
                    'color': 'white'
                } for i in dfterminalmcm.columns

            ]
  
                
                ),
            
            style_header={
                'backgroundColor': 'royalblue',
                'fontWeight': 'bold',
                'textAlign': 'center',
                #'border': '3px solid black'
                'color': 'white'
            },
             
            fixed_columns = {'headers': True, 'data': 1},
            #style_data_conditional=stylessupplyup,
            style_as_list_view=False,
            style_table={'minWidth': '100%'},
            #style_data_conditional=styles
            #page_size=45
        ),
        
        #demand table
        html.H3('Demand Data updated at: '+ datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')),
        html.Hr(),
        dash_table.DataTable(
            
            id='terminal_demand',
            columns=column_demand,
            #data=dfsupply.loc[3:].to_dict('records'),
            data =dfdemandcountry.to_dict('records'),
            #column_deletable=True,
            #editable=True,
            #sort_action='native',
            #filter_action="native",
            
            #row_deletable=True,
    
            style_cell={
                'whiteSpace': 'normal',
                'height': 'auto',
                'textAlign': 'center',
                #'border': '3px solid grey'
                #'backgroundColor': '#D9D9D9',
                'backgroundColor': '#DDEBF7',
                'minWidth': '200px',
            },
            style_cell_conditional=[
                
                {
                    'if': {
                        'row_index': 3,
                        },
                    'backgroundColor': '#92D050'
                },
                {
                    'if': {
                        'row_index': 4,
                        },
                    'backgroundColor': '#92D050'
                },
                {
                    'if': {
                        'row_index': 5,
                        },
                    'backgroundColor': '#92D050'
                },
                {
                    'if': {
                        'row_index': 6,
                        },
                    'backgroundColor': '#92D050'
                },
                {
                    'if': {
                        'row_index': 7,
                        },
                    'backgroundColor': '#92D050'
                },
                
                {
                    'if': {
                        'row_index': 22,
                        },
                    'backgroundColor': 'white'
                },
                {
                    'if': {
                        
                        'row_index': 23,
                        },
                    'backgroundColor': 'white'
                },
                
                
                {
                    'if': {
                        'row_index': 24,
                        },
                    'backgroundColor': 'white'
                },
                {
                    'if': {
                        'row_index': 25,
                        },
                    'backgroundColor': 'white'
                },
                {
                    'if': {
                        'row_index': 26,
                        },
                    'backgroundColor': 'white'
                },  
                {
                    'if': {
                        'row_index': 27,
                        },
                    'backgroundColor': 'white'
                },  
                {
                    'if': {
                        'row_index': 28,
                        },
                    'backgroundColor': 'white'
                },  
                
                
            ],
            
            style_data_conditional= ( 
                #color the delta row
                [
                {
                    'if': {
                        'filter_query': '{{{col}}} > {value}'.format(col=i.rstrip('\"'), value=0),
                        'column_id': i,
                         'row_index': 24,
                        
                    },
                    'backgroundColor': 'green',
                    'color': 'white'
                } for i in dfdemandcountry.columns

            ]+
                 [
                {
                    'if': {
                        'filter_query': '{{{col}}} < {value}'.format(col=i.rstrip('\"'), value=0),
                        'column_id': i,
                         'row_index': 24,  
                    },
                    'backgroundColor': 'Pink',
                    'color': 'white'
                } for i in dfdemandcountry.columns

            ]+
                  [
                {
                    'if': {
                        'filter_query': '{{{col}}} > {value}'.format(col=i.rstrip('\"'), value=0),
                        'column_id': i,
                         'row_index': 2,
                        
                    },
                    'backgroundColor': 'green',
                    'color': 'white'
                } for i in dfdemandcountry.columns

            ]+
                 [
                {
                    'if': {
                        'filter_query': '{{{col}}} < {value}'.format(col=i.rstrip('\"'), value=0),
                        'column_id': i,
                         'row_index': 2,  
                    },
                    'backgroundColor': 'Pink',
                    'color': 'white'
                } for i in dfdemandcountry.columns

            ]+
                 [
                {
                    'if': {
                        'filter_query': '{{{col}}} > {value}'.format(col=i.rstrip('\"'), value=0),
                        'column_id': i,
                         'row_index': 27,
                        
                    },
                    'backgroundColor': 'green',
                    'color': 'white'
                } for i in dfdemandcountry.columns

            ]+
                 [
                {
                    'if': {
                        'filter_query': '{{{col}}} < {value}'.format(col=i.rstrip('\"'), value=0),
                        'column_id': i,
                         'row_index': 27,  
                    },
                    'backgroundColor': 'Pink',
                    'color': 'white'
                } for i in dfdemandcountry.columns

            ]
  
                
                ),
            
            style_header={
                'backgroundColor': 'royalblue',
                'fontWeight': 'bold',
                'textAlign': 'center',
                #'border': '3px solid black'
                'color': 'white'
            },
             
            fixed_columns = {'headers': True, 'data': 1},
            #style_data_conditional=stylessupplyup,
            style_as_list_view=False,
            style_table={'minWidth': '100%'},
            #style_data_conditional=styles
            #page_size=45
        ),
        
        ])

        
        return terminal_table_layout
    
    
@app.callback(
    Output(component_id='body-div-uti', component_property='children'),
    Input(component_id='refrash_uti', component_property='n_clicks'),
)
def update_output(n_clicks):
    
    if n_clicks is None:
        
        raise PreventUpdate
    
    else:

        dfterminal, dfterminalmcm, column, dfutiall, dfutiallP, dfcargo, dfvol, dfDemand,dfdemandcountry,column_demand = AnalysisData.get_new_data()
        
        terminal_uti_table_layout = dash_table.DataTable(
            id='terminal_uti',
            data=dfutiallP.to_dict('records'),
            #sort_action='native',
            columns=[{'name': i, 'id': i} for i in dfutiallP.columns],
            #columns.append({'id': 'W-o-W % (Cargos)','name': 'W-o-W % (Cargos)','format': FormatTemplate.percentage(1).sign(Sign.positive)}),
            style_header={
                'backgroundColor': '#538DD5',
                'fontWeight': 'bold',
                'textAlign': 'center',
                #'border': '3px solid black'
                'color': 'white'
            },
            fixed_rows={'headers': True},
            fixed_columns = {'headers': True, 'data': 1},
            style_as_list_view=False,
            style_table={'minWidth': '100%'},
            style_data_conditional=(
                #AnalysisData.data_bars(dfutiall, 'Current Week 10 Day Rolling Avg '+str(yesterday))+
                #AnalysisData.data_bars(dfutiall, 'Last Week 10 Day Rolling Avg '+str(yesterday-datetime.timedelta(1)*7))+
                #AnalysisData.data_bars(dfutiall, 'M-1 '+month_before)+
                #AnalysisData.data_bars(dfutiall, 'Y-1 '+year_before)+
                [
                    {
                        'if': {
                            'filter_query': '{{{}}} > {}'.format('W-o-W % (Cargos)', 0.1),
                            'column_id': 'W-o-W % (Cargos)'
                        },
                        'backgroundColor': '#3D9970',
                        'color': 'white'
                    } 
                ] +
                [
                    {
                        'if': {
                            'filter_query': '{{{}}} < {}'.format('W-o-W % (Cargos)', -0.1),
                            'column_id': 'W-o-W % (Cargos)'
                        },
                        'backgroundColor': '#FF4136',
                        'color': 'white'
                    } 
                ] +
                [
                    {
                        'if': {
                            'filter_query': '{{{}}} > {}'.format('Delta(Cargos)', 0),
                            'column_id': 'Delta(Cargos)'
                        },
                        'backgroundColor': '#3D9970',
                        'color': 'white'
                    } 
                ] +
                [
                    {
                        'if': {
                            'filter_query': '{{{}}} < {}'.format('Delta(Cargos)', 0),
                            'column_id': 'Delta(Cargos)'
                        },
                        'backgroundColor': '#FF4136',
                        'color': 'white'
                    } 
                ]
               
            ),
            
            
            style_cell_conditional=[
                {
                    'if': {'column_id': 'Capacity (Mt)'},
                    'fontWeight': 'bold'
                },
                {
                    'if': {'column_id': 'Basin'},
                    'fontWeight': 'bold'
                },
                {
                    'if': {'column_id': 'Country'},
                    'fontWeight': 'bold'
                },
                {
                    'if': {'column_id': 'Terminal'},
                    'fontWeight': 'bold'
                },
                
                ],
            
            style_cell={
                'minWidth': '180px', 
                'width': '180px', 
                'maxWidth': '180px',
                'whiteSpace': 'normal',
                'overflow': 'hidden',
                'textOverflow': 'ellipsis',
                'textAlign': 'center',
                'height': 'auto',
            },
            
               
            )

        
        return terminal_uti_table_layout
    
@app.callback(
    Output(component_id='body-div-cargo', component_property='children'),
    Input(component_id='refrash_cargo', component_property='n_clicks'),
)
def update_output(n_clicks):
    
    if n_clicks is None:
        
        raise PreventUpdate
    
    else:

        dfterminal, dfterminalmcm, column, dfutiall, dfutiallP, dfcargo, dfvol, dfDemand,dfdemandcountry,column_demand = AnalysisData.get_new_data()
        
        terminal_cargo_table_layout = dash_table.DataTable(
            id='terminal_cargo',
            data=dfcargo.to_dict('records'),
            #sort_action='native',
            columns=[{'name': i, 'id': i} for i in dfcargo.columns],
            #columns.append({'id': 'W-o-W % (Cargos)','name': 'W-o-W % (Cargos)','format': FormatTemplate.percentage(1).sign(Sign.positive)}),
            style_header={
                'backgroundColor': '#538DD5',
                'fontWeight': 'bold',
                'textAlign': 'center',
                #'border': '3px solid black'
                'color': 'white'
            },
            fixed_rows={'headers': True},
            fixed_columns = {'headers': True, 'data': 1},
            style_as_list_view=False,
            style_table={'minWidth': '100%'},
            style_data_conditional=(
                #AnalysisData.data_bars(dfcargo, 'Current Week Cargoes Count'+str(yesterday))+
                #AnalysisData.data_bars(dfcargo, 'Last Week Cargoes Count'+str(yesterday-datetime.timedelta(1)*7))+
                #AnalysisData.data_bars(dfcargo, 'M-1'+month_before)+
                #AnalysisData.data_bars(dfcargo, 'Y-1'+year_before)+
                [
                    {
                        'if': {
                            'filter_query': '{{{}}} > {}'.format('W-o-W (Cargos)', 0.1),
                            'column_id': 'W-o-W (Cargos)'
                        },
                        'backgroundColor': '#3D9970',
                        'color': 'white'
                    } 
                ] +
                [
                    {
                        'if': {
                            'filter_query': '{{{}}} < {}'.format('W-o-W (Cargos)', -0.1),
                            'column_id': 'W-o-W (Cargos)'
                        },
                        'backgroundColor': '#FF4136',
                        'color': 'white'
                    } 
                ]+
                [
                    {
                        'if': {
                            'filter_query': '{{{}}} > {}'.format('Delta signal', 0),
                            'column_id': 'Delta signal'
                        },
                        'backgroundColor': 'green',
                        'color': 'white'
                    } 
                ] +
                [
                    {
                        'if': {
                            'filter_query': '{{{}}} < {}'.format('Delta signal', 0),
                            'column_id': 'Delta signal'
                        },
                        'backgroundColor': 'pink',
                        'color': 'white'
                    } 
                ]
               
            ),
            style_cell_conditional=[
                {
                    'if': {'column_id': 'Capacity (Mt)'},
                    'fontWeight': 'bold'
                },
                {
                    'if': {'column_id': 'Basin'},
                    'fontWeight': 'bold'
                },
                {
                    'if': {'column_id': 'Country'},
                    'fontWeight': 'bold'
                },
                {
                    'if': {'column_id': 'Terminal'},
                    'fontWeight': 'bold'
                },
                
                ],
            style_cell={
                'minWidth': '180px', 
                'width': '180px', 
                'maxWidth': '180px',
                'whiteSpace': 'normal',
                'overflow': 'hidden',
                'textOverflow': 'ellipsis',
                'textAlign': 'center',
                'height': 'auto',
            },
            
               
            )

        
        return terminal_cargo_table_layout
    
@app.callback(
    Output(component_id='body-div-vol', component_property='children'),
    Input(component_id='refrash_vol', component_property='n_clicks'),
)
def update_output(n_clicks):
    
    if n_clicks is None:
        
        raise PreventUpdate
    
    else:

        dfterminal, dfterminalmcm, column, dfutiall, dfutiallP, dfcargo, dfvol, dfDemand,dfdemandcountry,column_demand = AnalysisData.get_new_data()
        
        terminal_vol_table_layout = dash_table.DataTable(
            id='terminal_vol',
            data=dfvol.to_dict('records'),
            #sort_action='native',
            columns=[{'name': i, 'id': i} for i in dfvol.columns],
            #columns.append({'id': 'W-o-W % (Cargos)','name': 'W-o-W % (Cargos)','format': FormatTemplate.percentage(1).sign(Sign.positive)}),
            style_header={
                'backgroundColor': '#538DD5',
                'fontWeight': 'bold',
                'textAlign': 'center',
                #'border': '3px solid black'
                'color': 'white'
            },
            fixed_rows={'headers': True},
            fixed_columns = {'headers': True, 'data': 1},
            style_as_list_view=False,
            style_table={'minWidth': '100%'},
            style_data_conditional=(
                #AnalysisData.data_bars(dfvol, 'Current Week Volumes'+str(yesterday))+
                #AnalysisData.data_bars(dfvol, 'Last Week Volumes'+str(yesterday-datetime.timedelta(1)*7))+
                #AnalysisData.data_bars(dfvol, 'M-1'+month_before)+
                #AnalysisData.data_bars(dfvol, 'Y-1'+year_before)+
                [
                    {
                        'if': {
                            'filter_query': '{{{}}} > {}'.format('W-o-W', 0),
                            'column_id': 'W-o-W'
                        },
                        'backgroundColor': '#3D9970',
                        'color': 'white'
                    } 
                ] +
                [
                    {
                        'if': {
                            'filter_query': '{{{}}} < {}'.format('W-o-W', 0),
                            'column_id': 'W-o-W'
                        },
                        'backgroundColor': '#FF4136',
                        'color': 'white'
                    } 
                ]+
                [
                    {
                        'if': {
                            'filter_query': '{{{}}} > {}'.format('Delta signal', 0),
                            'column_id': 'Delta signal'
                        },
                        'backgroundColor': 'green',
                        'color': 'white'
                    } 
                ] +
                [
                    {
                        'if': {
                            'filter_query': '{{{}}} < {}'.format('Delta signal', 0),
                            'column_id': 'Delta signal'
                        },
                        'backgroundColor': 'pink',
                        'color': 'white'
                    } 
                ]
               
            ),
            style_cell_conditional=[
                {
                    'if': {'column_id': 'Capacity (Mt)'},
                    'fontWeight': 'bold'
                },
                {
                    'if': {'column_id': 'Basin'},
                    'fontWeight': 'bold'
                },
                {
                    'if': {'column_id': 'Country'},
                    'fontWeight': 'bold'
                },
                {
                    'if': {'column_id': 'Terminal'},
                    'fontWeight': 'bold'
                },
                
                ],
            style_cell={
                'minWidth': '180px', 
                'width': '180px', 
                'maxWidth': '180px',
                'whiteSpace': 'normal',
                'overflow': 'hidden',
                'textOverflow': 'ellipsis',
                'textAlign': 'center',
                'height': 'auto',
            },
            
               
            )
        
        return terminal_vol_table_layout
        
if __name__ == '__main__':
    app.run_server(debug=False, host='0.0.0.0', port=8060)
