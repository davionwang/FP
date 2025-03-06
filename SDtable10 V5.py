# -*- coding: utf-8 -*-
"""
Created on Tue Mar  8 11:35:57 2022

@author: SVC-GASQuant2-Prod
"""


#MA10 and MA30 
# use DB data
#V3 fixed M+1 desk to 2022
#V4 click buttom to update
#v5 button to download


import time

import dash
import dash_html_components as html
#from dash import html
#from dash import dcc
import dash_core_components as dcc
from dash.dependencies import Input, Output
from dash.exceptions import PreventUpdate

import sys

import datetime
import numpy as np
import pandas as pd

from dateutil.relativedelta import relativedelta
sys.path.append('C:\\Users\SVC-GASQuant2-Prod\YWwebsite\Python Scripts\class') 
#from SDdata import SD_Data_Country
#from SDdata import SD_Data_Basin
import dash_table 
from DBtoDF import DBTOPD


#MA10 Data
class SD_Table_10():
    
    
    #create dict of date
    def refresh_date():
        date_dict={
               'today' : str(datetime.date.today()),
               'last_day' : str(datetime.date.today() - datetime.timedelta(days=1)*1),
               'last_week' : str(datetime.date.today() - datetime.timedelta(weeks=1)*1),
               'last_month' : str(datetime.date.today() - relativedelta(months=1)),
               'next_1month' : str(datetime.date.today() + relativedelta(months=1)),
               'next_2month' : str(datetime.date.today() + relativedelta(months=2)),
               'last_year' : str(datetime.date.today() - relativedelta(months=12)),
               'now' : str(datetime.datetime.now())
               }
        return date_dict
    
    def get_sd_data():
        print("ori data updated at: ",  datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        #get data from DB
        demand=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'DemandCountry')
        dfdemandMA=demand.sql_to_df()
        dfdemandMA.set_index('Date', inplace=True)
        
        supplyMA=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'SupplyCountry')
        dfsupplyMA=supplyMA.sql_to_df()
        dfsupplyMA.set_index('Date', inplace=True)
        
        supplyMA_eta=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'SupplyPlantETA')
        dfsupplyMA_eta=supplyMA_eta.sql_to_df()
        dfsupplyMA_eta.set_index('Date', inplace=True)
        
        demandMA_eta=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'DemandETA')
        dfdemandMA_eta=demandMA_eta.sql_to_df()
        dfdemandMA_eta.set_index('Date', inplace=True)
        
        desk_supply_plant_view=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'DeskSupplyPlant')
        desk_supply_plant_view=desk_supply_plant_view.sql_to_df()
        desk_supply_plant_view.set_index('Date', inplace=True)
        
        desksupplyview=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'DeskSupplyCountry')
        desk_supply_view=desksupplyview.sql_to_df()
        desk_supply_view.set_index('Date', inplace=True)
        
        deskdemandview=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'DeskDemand')
        desk_demand_view=deskdemandview.sql_to_df()
        desk_demand_view.set_index('Date', inplace=True)
        
        ihscontract=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'IHSContract')
        dfihscontract=ihscontract.sql_to_df()
        dfihscontract.set_index('Date', inplace=True)
        
        #categories index
        SupplyCategories = pd.read_excel('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/Categories.xlsx',header=(0),sheet_name='Index_Supply')
        SupplyCategories = SupplyCategories.iloc[:44,0:5]
        #SupplyCategories.drop(SupplyCategories[SupplyCategories['Plant']=='Calcasieu Pass'].index, inplace=True)
        SupplyCategories.drop(SupplyCategories[SupplyCategories['Plant']=='Mozambique Area 1'].index, inplace=True)
        
        DemandCategories = pd.read_excel('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/Categories.xlsx',header=(0),sheet_name='Index_Demand')
        DemandCategories = DemandCategories.iloc[:64,0:6]
        droplist = ['Algeria','Angola','Bahamas','Brunei','Cameroon','Equatorial Guinea','Libya','Nigeria','Papua New Guinea','Peru','Philipines','Qatar','Russian Federation','Trinidad and Tobago','Yemen']
        for i in droplist:
            DemandCategories.drop(DemandCategories[DemandCategories['Country']==i].index, inplace=True)
            
        #print(df_supply)
        basinsupply=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'BasinSupply')
        dfbasinsupply=basinsupply.sql_to_df()
        dfbasinsupply.set_index('Date', inplace=True)
        
        basindemand=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'BasinDemand')
        dfbasindemand=basindemand.sql_to_df()
        dfbasindemand.set_index('Date', inplace=True)
        
        cumsupply=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'CumulativeSupply')
        dfcumsupply=cumsupply.sql_to_df()
        dfcumsupply.rename(columns=({'index':'Date'}), inplace=True)
        dfcumsupply.set_index('Date', inplace=True)
        
        cumdemand=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'CumulativeDemand')
        dfcumdemand=cumdemand.sql_to_df()
        dfcumdemand.rename(columns=({'index':'Date'}), inplace=True)
        dfcumdemand.set_index('Date', inplace=True)
       
        return dfsupplyMA, dfdemandMA, dfsupplyMA_eta, dfdemandMA_eta, desk_supply_view, desk_demand_view, dfihscontract, SupplyCategories, DemandCategories,dfbasinsupply, dfcumsupply, dfbasindemand, dfcumdemand
      
  
    def gpam_supply_table(supply_basin,dfsupplyMA, desk_supply_view, SupplyCategories):
        
        dfsupplyMA, desk_supply_view = dfsupplyMA.round(2), desk_supply_view.round(2)
        date_dict = SD_Table_10.refresh_date()
        #
        date_columns = ['',date_dict['last_month'], date_dict['last_month'],date_dict['today'],date_dict['today'],date_dict['next_1month'],date_dict['next_2month'],'','','','',date_dict['last_year'],date_dict['last_day'],date_dict['last_week'],'','']
        
        dfsupply = pd.DataFrame(columns=['Year','SupplyMA10','M-1',' M-1 ','M',' M ','M+1','M+2','YoY','MoM','Actual vs. Forecast','Actual vs. Forecast %', 'Y-1','D-1','D-7','DoD','WoW'])

        dfsupply['Year']=['Date','Source','Unit','Global','PB','AB','MENA_Bas','Algeria', 'Australia','Nigeria','Qatar','Russian Federation','Trinidad and Tobago','United States']
        dfsupply.loc[0,'SupplyMA10':] = date_columns
        dfsupply.loc[1,'SupplyMA10':] = ['','Kpler','Desk','Kpler','Desk','Desk','Desk','Kpler','Kpler','Indicator','Indicator','Kpler','Kpler','Kpler','Kpler','Kpler']
        dfsupply.loc[2,'SupplyMA10':] = ['','Mcm/d','Mcm/d','Mcm/d','Mcm/d','Mcm/d','Mcm/d','Mcm/d','Mcm/d','Mcm/d','%','','','','','']
        dfsupply['SupplyMA10'] = ''
        #M-1 Kpler
        dfsupply.loc[3,'M-1'] = supply_basin.loc[date_dict['last_month'],'Global']
        dfsupply.loc[4,'M-1'] = supply_basin.loc[date_dict['last_month'],'PB']
        dfsupply.loc[5,'M-1'] = supply_basin.loc[date_dict['last_month'],'AB']
        dfsupply.loc[6,'M-1'] = supply_basin.loc[date_dict['last_month'],'MENA']
        dfsupply.loc[7,'M-1'] = dfsupplyMA.loc[date_dict['last_month'],'Algeria']
        dfsupply.loc[8,'M-1'] = dfsupplyMA.loc[date_dict['last_month'],'Australia']
        dfsupply.loc[9,'M-1'] = dfsupplyMA.loc[date_dict['last_month'],'Nigeria']
        dfsupply.loc[10,'M-1'] = dfsupplyMA.loc[date_dict['last_month'],'Qatar']
        dfsupply.loc[11,'M-1'] = dfsupplyMA.loc[date_dict['last_month'],'Russian Federation']
        dfsupply.loc[12,'M-1'] = dfsupplyMA.loc[date_dict['last_month'],'Trinidad and Tobago']
        dfsupply.loc[13,'M-1'] = dfsupplyMA.loc[date_dict['last_month'],'United States']
        #M Kpler
        dfsupply.loc[3,'M'] = supply_basin.loc[date_dict['today'],'Global']
        dfsupply.loc[4,'M'] = supply_basin.loc[date_dict['today'],'PB']
        dfsupply.loc[5,'M'] = supply_basin.loc[date_dict['today'],'AB']
        dfsupply.loc[6,'M'] = supply_basin.loc[date_dict['today'],'MENA']
        dfsupply.loc[7,'M'] = dfsupplyMA.loc[date_dict['today'],'Algeria']
        dfsupply.loc[8,'M'] = dfsupplyMA.loc[date_dict['today'],'Australia']
        dfsupply.loc[9,'M'] = dfsupplyMA.loc[date_dict['today'],'Nigeria']
        dfsupply.loc[10,'M'] = dfsupplyMA.loc[date_dict['today'],'Qatar']
        dfsupply.loc[11,'M'] = dfsupplyMA.loc[date_dict['today'],'Russian Federation']
        dfsupply.loc[12,'M'] = dfsupplyMA.loc[date_dict['today'],'Trinidad and Tobago']
        dfsupply.loc[13,'M'] = dfsupplyMA.loc[date_dict['today'],'United States']
        #M Desk
        dfsupply.loc[3,' M '] = supply_basin.loc[date_dict['today'],'Global Desk']
        dfsupply.loc[4,' M '] = supply_basin.loc[date_dict['today'],'PB Desk']
        dfsupply.loc[5,' M '] = supply_basin.loc[date_dict['today'],'AB Desk']
        dfsupply.loc[6,' M '] = supply_basin.loc[date_dict['today'],'MENA Desk']
        dfsupply.loc[7,' M '] = desk_supply_view.loc[date_dict['today'],'Algeria']
        dfsupply.loc[8,' M '] = desk_supply_view.loc[date_dict['today'],'Australia']
        dfsupply.loc[9,' M '] = desk_supply_view.loc[date_dict['today'],'Nigeria']
        dfsupply.loc[10,' M '] = desk_supply_view.loc[date_dict['today'],'Qatar']
        dfsupply.loc[11,' M '] = desk_supply_view.loc[date_dict['today'],'Russian Federation']
        dfsupply.loc[12,' M '] = desk_supply_view.loc[date_dict['today'],'Trinidad and Tobago']
        dfsupply.loc[13,' M '] = desk_supply_view.loc[date_dict['today'],'United States']
        #M+1 Desk
        dfsupply.loc[3,'M+1'] = supply_basin.loc[date_dict['next_1month'],'Global Desk']
        dfsupply.loc[4,'M+1'] = supply_basin.loc[date_dict['next_1month'],'PB Desk']
        dfsupply.loc[5,'M+1'] = supply_basin.loc[date_dict['next_1month'],'AB Desk']
        dfsupply.loc[6,'M+1'] = supply_basin.loc[date_dict['next_1month'],'MENA Desk']
        dfsupply.loc[7,'M+1'] = desk_supply_view.loc[date_dict['next_1month'],'Algeria']
        dfsupply.loc[8,'M+1'] = desk_supply_view.loc[date_dict['next_1month'],'Australia']
        dfsupply.loc[9,'M+1'] = desk_supply_view.loc[date_dict['next_1month'],'Nigeria']
        dfsupply.loc[10,'M+1'] = desk_supply_view.loc[date_dict['next_1month'],'Qatar']
        dfsupply.loc[11,'M+1'] = desk_supply_view.loc[date_dict['next_1month'],'Russian Federation']
        dfsupply.loc[12,'M+1'] = desk_supply_view.loc[date_dict['next_1month'],'Trinidad and Tobago']
        dfsupply.loc[13,'M+1'] = desk_supply_view.loc[date_dict['next_1month'],'United States']
        #M+2 Desk
        dfsupply.loc[3,'M+2'] = supply_basin.loc[date_dict['next_2month'],'Global Desk']
        dfsupply.loc[4,'M+2'] = supply_basin.loc[date_dict['next_2month'],'PB Desk']
        dfsupply.loc[5,'M+2'] = supply_basin.loc[date_dict['next_2month'],'AB Desk']
        dfsupply.loc[6,'M+2'] = supply_basin.loc[date_dict['next_2month'],'MENA Desk']
        dfsupply.loc[7,'M+2'] = desk_supply_view.loc[date_dict['next_2month'],'Algeria']
        dfsupply.loc[8,'M+2'] = desk_supply_view.loc[date_dict['next_2month'],'Australia']
        dfsupply.loc[9,'M+2'] = desk_supply_view.loc[date_dict['next_2month'],'Nigeria']
        dfsupply.loc[10,'M+2'] = desk_supply_view.loc[date_dict['next_2month'],'Qatar']
        dfsupply.loc[11,'M+2'] = desk_supply_view.loc[date_dict['next_2month'],'Russian Federation']
        dfsupply.loc[12,'M+2'] = desk_supply_view.loc[date_dict['next_2month'],'Trinidad and Tobago']
        dfsupply.loc[13,'M+2'] = desk_supply_view.loc[date_dict['next_2month'],'United States']
        #M-1 Desk
        dfsupply.loc[3,' M-1 '] = supply_basin.loc[date_dict['last_month'],'Global Desk']
        dfsupply.loc[4,' M-1 '] = supply_basin.loc[date_dict['last_month'],'PB Desk']
        dfsupply.loc[5,' M-1 '] = supply_basin.loc[date_dict['last_month'],'AB Desk']
        dfsupply.loc[6,' M-1 '] = supply_basin.loc[date_dict['last_month'],'MENA Desk']
        dfsupply.loc[7,' M-1 '] = desk_supply_view.loc[date_dict['last_month'],'Algeria']
        dfsupply.loc[8,' M-1 '] = desk_supply_view.loc[date_dict['last_month'],'Australia']
        dfsupply.loc[9,' M-1 '] = desk_supply_view.loc[date_dict['last_month'],'Nigeria']
        dfsupply.loc[10,' M-1 '] = desk_supply_view.loc[date_dict['last_month'],'Qatar']
        dfsupply.loc[11,' M-1 '] = desk_supply_view.loc[date_dict['last_month'],'Russian Federation']
        dfsupply.loc[12,' M-1 '] = desk_supply_view.loc[date_dict['last_month'],'Trinidad and Tobago']
        dfsupply.loc[13,' M-1 '] = desk_supply_view.loc[date_dict['last_month'],'United States']
        #YoY Kpler
        dfsupply.loc[3,'YoY'] = (supply_basin.loc[date_dict['today'],'Global'] - supply_basin.loc[date_dict['last_year'],'Global']).round(2)
        dfsupply.loc[4,'YoY'] = (supply_basin.loc[date_dict['today'],'PB'] - supply_basin.loc[date_dict['last_year'],'PB']).round(2) 
        dfsupply.loc[5,'YoY'] = (supply_basin.loc[date_dict['today'],'AB'] - supply_basin.loc[date_dict['last_year'],'AB']).round(2)  
        dfsupply.loc[6,'YoY'] = (supply_basin.loc[date_dict['today'],'MENA'] - supply_basin.loc[date_dict['last_year'],'AB']).round(2) 
        dfsupply.loc[7,'YoY'] = (dfsupplyMA.loc[date_dict['today'],'Algeria'] - dfsupplyMA.loc[date_dict['last_year'],'Algeria']).round(2) 
        dfsupply.loc[8,'YoY'] = (dfsupplyMA.loc[date_dict['today'],'Australia'] - dfsupplyMA.loc[date_dict['last_year'],'Australia']).round(2) 
        dfsupply.loc[9,'YoY'] = (dfsupplyMA.loc[date_dict['today'],'Nigeria'] - dfsupplyMA.loc[date_dict['last_year'],'Nigeria']).round(2) 
        dfsupply.loc[10,'YoY'] = (dfsupplyMA.loc[date_dict['today'],'Qatar'] - dfsupplyMA.loc[date_dict['last_year'],'Qatar']).round(2) 
        dfsupply.loc[11,'YoY'] = (dfsupplyMA.loc[date_dict['today'],'Russian Federation'] - dfsupplyMA.loc[date_dict['last_year'],'Russian Federation']).round(2) 
        dfsupply.loc[12,'YoY'] = (dfsupplyMA.loc[date_dict['today'],'Trinidad and Tobago'] - dfsupplyMA.loc[date_dict['last_year'],'Trinidad and Tobago']).round(2) 
        dfsupply.loc[13,'YoY'] = (dfsupplyMA.loc[date_dict['today'],'United States'] - dfsupplyMA.loc[date_dict['last_year'],'United States']).round(2) 
        #MoM Kpler
        dfsupply.loc[3,'MoM'] = (supply_basin.loc[date_dict['today'],'Global'] - supply_basin.loc[date_dict['last_month'],'Global']).round(2) 
        dfsupply.loc[4,'MoM'] = (supply_basin.loc[date_dict['today'],'PB'] - supply_basin.loc[date_dict['last_month'],'PB']).round(2)  
        dfsupply.loc[5,'MoM'] = (supply_basin.loc[date_dict['today'],'AB'] - supply_basin.loc[date_dict['last_month'],'AB']).round(2)  
        dfsupply.loc[6,'MoM'] = (supply_basin.loc[date_dict['today'],'MENA'] - supply_basin.loc[date_dict['last_month'],'AB']).round(2) 
        dfsupply.loc[7,'MoM'] = (dfsupplyMA.loc[date_dict['today'],'Algeria'] - dfsupplyMA.loc[date_dict['last_month'],'Algeria']).round(2) 
        dfsupply.loc[8,'MoM'] = (dfsupplyMA.loc[date_dict['today'],'Australia'] - dfsupplyMA.loc[date_dict['last_month'],'Australia']).round(2) 
        dfsupply.loc[9,'MoM'] = (dfsupplyMA.loc[date_dict['today'],'Nigeria'] - dfsupplyMA.loc[date_dict['last_month'],'Nigeria']).round(2) 
        dfsupply.loc[10,'MoM'] = (dfsupplyMA.loc[date_dict['today'],'Qatar'] - dfsupplyMA.loc[date_dict['last_month'],'Qatar']).round(2) 
        dfsupply.loc[11,'MoM'] = (dfsupplyMA.loc[date_dict['today'],'Russian Federation'] - dfsupplyMA.loc[date_dict['last_month'],'Russian Federation']).round(2) 
        dfsupply.loc[12,'MoM'] = (dfsupplyMA.loc[date_dict['today'],'Trinidad and Tobago'] - dfsupplyMA.loc[date_dict['last_month'],'Trinidad and Tobago']).round(2) 
        dfsupply.loc[13,'MoM'] = (dfsupplyMA.loc[date_dict['today'],'United States'] - dfsupplyMA.loc[date_dict['last_month'],'United States']).round(2) 
        #Actual vs. Forecast
        dfsupply.loc[3,'Actual vs. Forecast'] = dfsupply.loc[3,'M'] - dfsupply.loc[3,' M '] 
        dfsupply.loc[4,'Actual vs. Forecast'] = dfsupply.loc[4,'M'] - dfsupply.loc[4,' M ']  
        dfsupply.loc[5,'Actual vs. Forecast'] = dfsupply.loc[5,'M'] - dfsupply.loc[5,' M '] 
        dfsupply.loc[6,'Actual vs. Forecast'] = dfsupply.loc[6,'M'] - dfsupply.loc[6,' M ']
        dfsupply.loc[7,'Actual vs. Forecast'] = dfsupply.loc[7,'M'] - dfsupply.loc[7,' M ']
        dfsupply.loc[8,'Actual vs. Forecast'] = dfsupply.loc[8,'M'] - dfsupply.loc[8,' M ']
        dfsupply.loc[9,'Actual vs. Forecast'] = dfsupply.loc[9,'M'] - dfsupply.loc[9,' M ']
        dfsupply.loc[10,'Actual vs. Forecast'] = dfsupply.loc[10,'M'] - dfsupply.loc[10,' M ']
        dfsupply.loc[11,'Actual vs. Forecast'] = dfsupply.loc[11,'M'] - dfsupply.loc[11,' M ']
        dfsupply.loc[12,'Actual vs. Forecast'] = dfsupply.loc[12,'M'] - dfsupply.loc[12,' M ']
        dfsupply.loc[13,'Actual vs. Forecast'] = dfsupply.loc[13,'M'] - dfsupply.loc[13,' M ']
        dfsupply.loc[3:13, 'Actual vs. Forecast'] = dfsupply.loc[3:13, 'Actual vs. Forecast'].astype('float').round(2)
        #Actual vs. Forecast %
        dfsupply.loc[3,'Actual vs. Forecast %'] = (dfsupply.loc[3,'M'] - dfsupply.loc[3,' M ']) /dfsupply.loc[3,'M']
        dfsupply.loc[4,'Actual vs. Forecast %'] = (dfsupply.loc[4,'M'] - dfsupply.loc[4,' M ']) /dfsupply.loc[4,'M']
        dfsupply.loc[5,'Actual vs. Forecast %'] = (dfsupply.loc[5,'M'] - dfsupply.loc[5,' M ']) /dfsupply.loc[5,'M']
        dfsupply.loc[6,'Actual vs. Forecast %'] = (dfsupply.loc[6,'M'] - dfsupply.loc[6,' M ']) /dfsupply.loc[6,'M']
        dfsupply.loc[7,'Actual vs. Forecast %'] = (dfsupply.loc[7,'M'] - dfsupply.loc[7,' M ']) /dfsupply.loc[7,'M']
        dfsupply.loc[8,'Actual vs. Forecast %'] = (dfsupply.loc[8,'M'] - dfsupply.loc[8,' M ']) /dfsupply.loc[8,'M']
        dfsupply.loc[9,'Actual vs. Forecast %'] = (dfsupply.loc[9,'M'] - dfsupply.loc[9,' M ']) /dfsupply.loc[9,'M']
        dfsupply.loc[10,'Actual vs. Forecast %'] = (dfsupply.loc[10,'M'] - dfsupply.loc[10,' M ']) /dfsupply.loc[10,'M']
        dfsupply.loc[11,'Actual vs. Forecast %'] = (dfsupply.loc[11,'M'] - dfsupply.loc[11,' M ']) /dfsupply.loc[11,'M']
        dfsupply.loc[12,'Actual vs. Forecast %'] = (dfsupply.loc[12,'M'] - dfsupply.loc[12,' M ']) /dfsupply.loc[12,'M']
        dfsupply.loc[13,'Actual vs. Forecast %'] = (dfsupply.loc[13,'M'] - dfsupply.loc[13,' M ']) /dfsupply.loc[13,'M']
        dfsupply.loc[3:13,'Actual vs. Forecast %'] = dfsupply.loc[3:13,'Actual vs. Forecast %'].map(lambda x:format(x,'.0%'))
        #Y-1 Kpler
        dfsupply.loc[3:,'Y-1'] = supply_basin.loc[date_dict['last_year'],'Global']
        dfsupply.loc[4:,'Y-1'] = supply_basin.loc[date_dict['last_year'],'PB']
        dfsupply.loc[5:,'Y-1'] = supply_basin.loc[date_dict['last_year'],'AB']
        dfsupply.loc[6:,'Y-1'] = supply_basin.loc[date_dict['last_year'],'MENA']
        dfsupply.loc[7,'Y-1'] = dfsupplyMA.loc[date_dict['last_year'],'Algeria']
        dfsupply.loc[8,'Y-1'] = dfsupplyMA.loc[date_dict['last_year'],'Australia']
        dfsupply.loc[9,'Y-1'] = dfsupplyMA.loc[date_dict['last_year'],'Nigeria']
        dfsupply.loc[10,'Y-1'] = dfsupplyMA.loc[date_dict['last_year'],'Qatar']
        dfsupply.loc[11,'Y-1'] = dfsupplyMA.loc[date_dict['last_year'],'Russian Federation']
        dfsupply.loc[12,'Y-1'] = dfsupplyMA.loc[date_dict['last_year'],'Trinidad and Tobago']
        dfsupply.loc[13,'Y-1'] = dfsupplyMA.loc[date_dict['last_year'],'United States']
        #D-1 Kpler
        dfsupply.loc[3:,'D-1'] = supply_basin.loc[date_dict['last_day'],'Global']
        dfsupply.loc[4:,'D-1'] = supply_basin.loc[date_dict['last_day'],'PB']
        dfsupply.loc[5:,'D-1'] = supply_basin.loc[date_dict['last_day'],'AB']
        dfsupply.loc[6:,'D-1'] = supply_basin.loc[date_dict['last_day'],'MENA']
        dfsupply.loc[7,'D-1'] = dfsupplyMA.loc[date_dict['last_day'],'Algeria']
        dfsupply.loc[8,'D-1'] = dfsupplyMA.loc[date_dict['last_day'],'Australia']
        dfsupply.loc[9,'D-1'] = dfsupplyMA.loc[date_dict['last_day'],'Nigeria']
        dfsupply.loc[10,'D-1'] = dfsupplyMA.loc[date_dict['last_day'],'Qatar']
        dfsupply.loc[11,'D-1'] = dfsupplyMA.loc[date_dict['last_day'],'Russian Federation']
        dfsupply.loc[12,'D-1'] = dfsupplyMA.loc[date_dict['last_day'],'Trinidad and Tobago']
        dfsupply.loc[13,'D-1'] = dfsupplyMA.loc[date_dict['last_day'],'United States']
        #D-8 Kpler
        dfsupply.loc[3:,'D-7'] = supply_basin.loc[date_dict['last_week'],'Global']
        dfsupply.loc[4:,'D-7'] = supply_basin.loc[date_dict['last_week'],'PB']
        dfsupply.loc[5:,'D-7'] = supply_basin.loc[date_dict['last_week'],'AB']
        dfsupply.loc[6:,'D-7'] = supply_basin.loc[date_dict['last_week'],'MENA']
        dfsupply.loc[7,'D-7'] = dfsupplyMA.loc[date_dict['last_week'],'Algeria']
        dfsupply.loc[8,'D-7'] = dfsupplyMA.loc[date_dict['last_week'],'Australia']
        dfsupply.loc[9,'D-7'] = dfsupplyMA.loc[date_dict['last_week'],'Nigeria']
        dfsupply.loc[10,'D-7'] = dfsupplyMA.loc[date_dict['last_week'],'Qatar']
        dfsupply.loc[11,'D-7'] = dfsupplyMA.loc[date_dict['last_week'],'Russian Federation']
        dfsupply.loc[12,'D-7'] = dfsupplyMA.loc[date_dict['last_week'],'Trinidad and Tobago']
        dfsupply.loc[13,'D-7'] = dfsupplyMA.loc[date_dict['last_week'],'United States']
        #DoD Kpler
        dfsupply.loc[3,'DoD'] = dfsupply.loc[3,'M'] - dfsupply.loc[3,'D-1']
        dfsupply.loc[4,'DoD'] = dfsupply.loc[4,'M'] - dfsupply.loc[4,'D-1']
        dfsupply.loc[5,'DoD'] = dfsupply.loc[5,'M'] - dfsupply.loc[5,'D-1']
        dfsupply.loc[6,'DoD'] = dfsupply.loc[6,'M'] - dfsupply.loc[6,'D-1']
        dfsupply.loc[7,'DoD'] = dfsupply.loc[7,'M'] - dfsupply.loc[7,'D-1']
        dfsupply.loc[8,'DoD'] = dfsupply.loc[8,'M'] - dfsupply.loc[8,'D-1']
        dfsupply.loc[9,'DoD'] = dfsupply.loc[9,'M'] - dfsupply.loc[9,'D-1']
        dfsupply.loc[10,'DoD'] = dfsupply.loc[10,'M'] - dfsupply.loc[10,'D-1']
        dfsupply.loc[11,'DoD'] = dfsupply.loc[11,'M'] - dfsupply.loc[11,'D-1']
        dfsupply.loc[12,'DoD'] = dfsupply.loc[12,'M'] - dfsupply.loc[12,'D-1']
        dfsupply.loc[13,'DoD'] = dfsupply.loc[13,'M'] - dfsupply.loc[13,'D-1']
        dfsupply.loc[3:13, 'DoD'] = dfsupply.loc[3:13, 'DoD'].astype('float').round(2)
        #WoW Kpler
        dfsupply.loc[3,'WoW'] = dfsupply.loc[3,'M'] - dfsupply.loc[3,'D-7']
        dfsupply.loc[4,'WoW'] = dfsupply.loc[4,'M'] - dfsupply.loc[4,'D-7']
        dfsupply.loc[5,'WoW'] = dfsupply.loc[5,'M'] - dfsupply.loc[5,'D-7']
        dfsupply.loc[6,'WoW'] = dfsupply.loc[6,'M'] - dfsupply.loc[6,'D-7']
        dfsupply.loc[7,'WoW'] = dfsupply.loc[7,'M'] - dfsupply.loc[7,'D-7']
        dfsupply.loc[8,'WoW'] = dfsupply.loc[8,'M'] - dfsupply.loc[8,'D-7']
        dfsupply.loc[9,'WoW'] = dfsupply.loc[9,'M'] - dfsupply.loc[9,'D-7']
        dfsupply.loc[10,'WoW'] = dfsupply.loc[10,'M'] - dfsupply.loc[10,'D-7']
        dfsupply.loc[11,'WoW'] = dfsupply.loc[11,'M'] - dfsupply.loc[11,'D-7']
        dfsupply.loc[12,'WoW'] = dfsupply.loc[12,'M'] - dfsupply.loc[12,'D-7']
        dfsupply.loc[13,'WoW'] = dfsupply.loc[13,'M'] - dfsupply.loc[13,'D-7']
        dfsupply.loc[3:13, 'WoW'] = dfsupply.loc[3:13, 'WoW'].astype('float').round(2)
        #dfsupply = dfsupply.round(2)
        #dfsupply.loc[6,:]=dfsupply[6:,].append(pd.Series(), ignore_index=True)
        
        
        df_supply_plant = pd.DataFrame(columns=['Suez','Basin','Market','Plant','M-1 Kpler','M-1 Desk','M Kpler','M Desk','M+1 Desk','M+2 Desk','YoY Kpler','MoM Kpler','Actual vs. Forecast','Actual vs. Forecast %', 'Y-1 Kpler','D-1 Kpler','D-7 Kpler','DoD Kpler','WoW Kpler'])
        df_supply_plant[['Suez', 'Basin','Market','Plant']] = SupplyCategories[['Suez', 'Basin','Market','Plant']]
        
        df_supply_plant.set_index('Plant', inplace=True)
        
        #print(SupplyCategories)
        
        for i in df_supply_plant.index:
            df_supply_plant.loc[i,'M-1 Kpler'] = supply_basin.loc[date_dict['last_month'],i]
            df_supply_plant.loc[i,'M Kpler'] = supply_basin.loc[date_dict['today'],i]
            df_supply_plant.loc[i,'M Desk'] = supply_basin.loc[date_dict['today'],i+' Desk']
            df_supply_plant.loc[i,'M+1 Desk'] = supply_basin.loc[date_dict['next_1month'],i+' Desk']
            df_supply_plant.loc[i,'M+2 Desk'] = supply_basin.loc[date_dict['next_2month'],i+' Desk']
            df_supply_plant.loc[i,'M-1 Desk'] = supply_basin.loc[date_dict['last_month'],i+' Desk']
            df_supply_plant.loc[i,'YoY Kpler'] = (supply_basin.loc[date_dict['today'],i] - supply_basin.loc[date_dict['last_year'],i]).round(2)
            df_supply_plant.loc[i,'MoM Kpler'] = (supply_basin.loc[date_dict['today'],i] - supply_basin.loc[date_dict['last_month'],i]).round(2)
            #print(i,df_supply_plant.loc[i,'M Kpler'])
            #print(i,df_supply_plant.loc[i,'M Desk'])
            df_supply_plant.loc[i,'Actual vs. Forecast'] = (df_supply_plant.loc[i,'M Kpler'] - df_supply_plant.loc[i,'M Desk']).round(2)
            df_supply_plant.loc[i,'Actual vs. Forecast %'] = (df_supply_plant.loc[i,'M Kpler'] - df_supply_plant.loc[i,'M Desk']) /df_supply_plant.loc[i,'M Kpler']
            
            df_supply_plant.loc[i,'Y-1 Kpler'] = supply_basin.loc[date_dict['last_year'],i]
            df_supply_plant.loc[i,'D-1 Kpler'] = supply_basin.loc[date_dict['last_day'],i]
            df_supply_plant.loc[i,'D-7 Kpler'] = supply_basin.loc[date_dict['last_week'],i]
            df_supply_plant.loc[i,'DoD Kpler'] = (df_supply_plant.loc[i,'M Kpler'] - df_supply_plant.loc[i,'D-1 Kpler']).round(2)
            df_supply_plant.loc[i,'WoW Kpler'] = (df_supply_plant.loc[i,'M Kpler'] - df_supply_plant.loc[i,'D-7 Kpler']).round(2)
        df_supply_plant.loc[:,'Actual vs. Forecast %'] = df_supply_plant.loc[:,'Actual vs. Forecast %'].map(lambda x:format(x,'.0%'))
        df_supply_plant.sort_values(by='Basin', inplace=True)
        df_supply_plant.reset_index(inplace=True)
        df_supply_plant = df_supply_plant[['Suez','Basin','Market','Plant','M-1 Kpler','M-1 Desk','M Kpler','M Desk','M+1 Desk','M+2 Desk','YoY Kpler','MoM Kpler','Actual vs. Forecast','Actual vs. Forecast %', 'Y-1 Kpler','D-1 Kpler','D-7 Kpler','DoD Kpler','WoW Kpler']]
        
        return dfsupply, df_supply_plant
    
    #demand
    def gpam_demand_table(demand_basin, dfdemandMA, desk_demand_view,DemandCategories):
        
        desk_demand_view, dfdemandMA = desk_demand_view.round(2), dfdemandMA.round(2)
        date_dict = SD_Table_10.refresh_date()
        #demand_basin.to_csv('H:/Yuefeng/LNG Flows/Deskdatatestcom.csv')
        date_columns = ['',date_dict['last_month'], date_dict['last_month'],date_dict['today'],date_dict['today'],date_dict['next_1month'],date_dict['next_2month'],'','','','',date_dict['last_year'],date_dict['last_day'],date_dict['last_week'],'','']
        
        dfdemand = pd.DataFrame(columns=['Year','DemandMA10','M-1',' M-1 ','M',' M ','M+1','M+2','YoY','MoM','Actual vs. Forecast','Actual vs. Forecast %', 'Y-1','D-1','D-7','DoD','WoW'])
        dfdemand['Year']=['Date','Source','Unit','Global','PB','AB','MENA_Bas','JKTC','MEIP', 'NW Eur','MedEur','Other Eur','Lat Am','Other RoW']
        dfdemand.loc[0,'DemandMA10':] = date_columns
        dfdemand.loc[1,'DemandMA10':] = ['','Kpler','Desk','Kpler','Desk','Desk','Desk','Kpler','Kpler','Indicator','Indicator','Kpler','Kpler','Kpler','Kpler','Kpler']
        dfdemand.loc[2,'DemandMA10':] = ['','Mcm/d','Mcm/d','Mcm/d','Mcm/d','Mcm/d','Mcm/d','Mcm/d','Mcm/d','Mcm/d','%','','','','','']
        dfdemand['DemandMA10'] = ''
        #M-1 Kpler
        dfdemand.loc[3,'M-1'] = demand_basin.loc[date_dict['last_month'],'Global']
        dfdemand.loc[4,'M-1'] = demand_basin.loc[date_dict['last_month'],'PB']
        dfdemand.loc[5,'M-1'] = demand_basin.loc[date_dict['last_month'],'AB']
        dfdemand.loc[6,'M-1'] = demand_basin.loc[date_dict['last_month'],'MENA']
        dfdemand.loc[7,'M-1'] = demand_basin.loc[date_dict['last_month'],'JKTC']
        dfdemand.loc[8,'M-1'] = demand_basin.loc[date_dict['last_month'],'MEIP']
        dfdemand.loc[9,'M-1'] = demand_basin.loc[date_dict['last_month'],'NWEur']
        dfdemand.loc[10,'M-1'] = demand_basin.loc[date_dict['last_month'],'MedEur']
        dfdemand.loc[11,'M-1'] = demand_basin.loc[date_dict['last_month'],'OtherEur']
        dfdemand.loc[12,'M-1'] = demand_basin.loc[date_dict['last_month'],'LatAm']
        dfdemand.loc[13,'M-1'] = demand_basin.loc[date_dict['last_month'],'OtherRoW']
        #M Kpler
        dfdemand.loc[3,'M'] = demand_basin.loc[date_dict['today'],'Global']
        dfdemand.loc[4,'M'] = demand_basin.loc[date_dict['today'],'PB']
        dfdemand.loc[5,'M'] = demand_basin.loc[date_dict['today'],'AB']
        dfdemand.loc[6,'M'] = demand_basin.loc[date_dict['today'],'MENA']
        dfdemand.loc[7,'M'] = demand_basin.loc[date_dict['today'],'JKTC']
        dfdemand.loc[8,'M'] = demand_basin.loc[date_dict['today'],'MEIP']
        dfdemand.loc[9,'M'] = demand_basin.loc[date_dict['today'],'NWEur']
        dfdemand.loc[10,'M'] = demand_basin.loc[date_dict['today'],'MedEur']
        dfdemand.loc[11,'M'] = demand_basin.loc[date_dict['today'],'OtherEur']
        dfdemand.loc[12,'M'] = demand_basin.loc[date_dict['today'],'LatAm']
        dfdemand.loc[13,'M'] = demand_basin.loc[date_dict['today'],'OtherRoW']
        #M Desk
        dfdemand.loc[3,' M '] = demand_basin.loc[date_dict['today'],'Global Desk']
        dfdemand.loc[4,' M '] = demand_basin.loc[date_dict['today'],'PB Desk']
        dfdemand.loc[5,' M '] = demand_basin.loc[date_dict['today'],'AB Desk']
        dfdemand.loc[6,' M '] = demand_basin.loc[date_dict['today'],'MENA Desk']
        dfdemand.loc[7,' M '] = demand_basin.loc[date_dict['today'],'JKTC Desk']
        dfdemand.loc[8,' M '] = demand_basin.loc[date_dict['today'],'MEIP Desk']
        dfdemand.loc[9,' M '] = demand_basin.loc[date_dict['today'],'NWEur Desk']
        dfdemand.loc[10,' M '] = demand_basin.loc[date_dict['today'],'MedEur Desk']
        dfdemand.loc[11,' M '] = demand_basin.loc[date_dict['today'],'OtherEur Desk']
        dfdemand.loc[12,' M '] = demand_basin.loc[date_dict['today'],'LatAm Desk']
        dfdemand.loc[13,' M '] = demand_basin.loc[date_dict['today'],'OtherRoW Desk']
        #M+1 Desk
        dfdemand.loc[3,'M+1'] = demand_basin.loc[date_dict['next_1month'],'Global Desk']
        dfdemand.loc[4,'M+1'] = demand_basin.loc[date_dict['next_1month'],'PB Desk']
        dfdemand.loc[5,'M+1'] = demand_basin.loc[date_dict['next_1month'],'AB Desk']
        dfdemand.loc[6,'M+1'] = demand_basin.loc[date_dict['next_1month'],'MENA Desk']
        dfdemand.loc[7,'M+1'] = demand_basin.loc[date_dict['next_1month'],'JKTC Desk']
        dfdemand.loc[8,'M+1'] = demand_basin.loc[date_dict['next_1month'],'MEIP Desk']
        dfdemand.loc[9,'M+1'] = demand_basin.loc[date_dict['next_1month'],'NWEur Desk']
        dfdemand.loc[10,'M+1'] = demand_basin.loc[date_dict['next_1month'],'MedEur Desk']
        dfdemand.loc[11,'M+1'] = demand_basin.loc[date_dict['next_1month'],'OtherEur Desk']
        dfdemand.loc[12,'M+1'] = demand_basin.loc[date_dict['next_1month'],'LatAm Desk']
        dfdemand.loc[13,'M+1'] = demand_basin.loc[date_dict['next_1month'],'OtherRoW Desk']
        #M+2 Desk
        dfdemand.loc[3,'M+2'] = demand_basin.loc[date_dict['next_2month'],'Global Desk']
        dfdemand.loc[4,'M+2'] = demand_basin.loc[date_dict['next_2month'],'PB Desk']
        dfdemand.loc[5,'M+2'] = demand_basin.loc[date_dict['next_2month'],'AB Desk']
        dfdemand.loc[6,'M+2'] = demand_basin.loc[date_dict['next_2month'],'MENA Desk']
        dfdemand.loc[7,'M+2'] = demand_basin.loc[date_dict['next_2month'],'JKTC Desk']
        dfdemand.loc[8,'M+2'] = demand_basin.loc[date_dict['next_2month'],'MEIP Desk']
        dfdemand.loc[9,'M+2'] = demand_basin.loc[date_dict['next_2month'],'NWEur Desk']
        dfdemand.loc[10,'M+2'] = demand_basin.loc[date_dict['next_2month'],'MedEur Desk']
        dfdemand.loc[11,'M+2'] = demand_basin.loc[date_dict['next_2month'],'OtherEur Desk']
        dfdemand.loc[12,'M+2'] = demand_basin.loc[date_dict['next_2month'],'LatAm Desk']
        dfdemand.loc[13,'M+2'] = demand_basin.loc[date_dict['next_2month'],'OtherRoW Desk']
        #M-1 Desk
        dfdemand.loc[3,' M-1 '] = demand_basin.loc[date_dict['last_month'],'Global Desk']
        dfdemand.loc[4,' M-1 '] = demand_basin.loc[date_dict['last_month'],'PB Desk']
        dfdemand.loc[5,' M-1 '] = demand_basin.loc[date_dict['last_month'],'AB Desk']
        dfdemand.loc[6,' M-1 '] = demand_basin.loc[date_dict['last_month'],'MENA Desk']
        dfdemand.loc[7,' M-1 '] = demand_basin.loc[date_dict['last_month'],'JKTC Desk']
        dfdemand.loc[8,' M-1 '] = demand_basin.loc[date_dict['last_month'],'MEIP Desk']
        dfdemand.loc[9,' M-1 '] = demand_basin.loc[date_dict['last_month'],'NWEur Desk']
        dfdemand.loc[10,' M-1 '] = demand_basin.loc[date_dict['last_month'],'MedEur Desk']
        dfdemand.loc[11,' M-1 '] = demand_basin.loc[date_dict['last_month'],'OtherEur Desk']
        dfdemand.loc[12,' M-1 '] = demand_basin.loc[date_dict['last_month'],'LatAm Desk']
        dfdemand.loc[13,' M-1 '] = demand_basin.loc[date_dict['last_month'],'OtherRoW Desk']
        #YoY Kpler
        dfdemand.loc[3,'YoY'] = demand_basin.loc[date_dict['today'],'Global'] - demand_basin.loc[date_dict['last_year'],'Global'] 
        dfdemand.loc[4,'YoY'] = demand_basin.loc[date_dict['today'],'PB'] - demand_basin.loc[date_dict['last_year'],'PB'] 
        dfdemand.loc[5,'YoY'] = demand_basin.loc[date_dict['today'],'AB'] - demand_basin.loc[date_dict['last_year'],'AB'] 
        dfdemand.loc[6,'YoY'] = demand_basin.loc[date_dict['today'],'MENA'] - demand_basin.loc[date_dict['last_year'],'AB']
        dfdemand.loc[7,'YoY'] = demand_basin.loc[date_dict['today'],'JKTC'] - demand_basin.loc[date_dict['last_year'],'JKTC']
        dfdemand.loc[8,'YoY'] = demand_basin.loc[date_dict['today'],'MEIP'] - demand_basin.loc[date_dict['last_year'],'MEIP']
        dfdemand.loc[9,'YoY'] = demand_basin.loc[date_dict['today'],'NWEur'] - demand_basin.loc[date_dict['last_year'],'NWEur']
        dfdemand.loc[10,'YoY'] = demand_basin.loc[date_dict['today'],'MedEur'] - demand_basin.loc[date_dict['last_year'],'MedEur']
        dfdemand.loc[11,'YoY'] = demand_basin.loc[date_dict['today'],'OtherEur'] - demand_basin.loc[date_dict['last_year'],'OtherEur']
        dfdemand.loc[12,'YoY'] = demand_basin.loc[date_dict['today'],'LatAm'] - demand_basin.loc[date_dict['last_year'],'LatAm']
        dfdemand.loc[13,'YoY'] = demand_basin.loc[date_dict['today'],'OtherRoW'] - demand_basin.loc[date_dict['last_year'],'OtherRoW']
        dfdemand.loc[3:13, 'YoY'] = dfdemand.loc[3:13, 'YoY'].astype('float').round(2)
        #MoM Kpler
        dfdemand.loc[3,'MoM'] = demand_basin.loc[date_dict['today'],'Global'] - demand_basin.loc[date_dict['last_month'],'Global'] 
        dfdemand.loc[4,'MoM'] = demand_basin.loc[date_dict['today'],'PB'] - demand_basin.loc[date_dict['last_month'],'PB'] 
        dfdemand.loc[5,'MoM'] = demand_basin.loc[date_dict['today'],'AB'] - demand_basin.loc[date_dict['last_month'],'AB'] 
        dfdemand.loc[6,'MoM'] = demand_basin.loc[date_dict['today'],'MENA'] - demand_basin.loc[date_dict['last_month'],'AB']
        dfdemand.loc[7,'MoM'] = demand_basin.loc[date_dict['today'],'JKTC'] - demand_basin.loc[date_dict['last_month'],'JKTC']
        dfdemand.loc[8,'MoM'] = demand_basin.loc[date_dict['today'],'MEIP'] - demand_basin.loc[date_dict['last_month'],'MEIP']
        dfdemand.loc[9,'MoM'] = demand_basin.loc[date_dict['today'],'NWEur'] - demand_basin.loc[date_dict['last_month'],'NWEur']
        dfdemand.loc[10,'MoM'] = demand_basin.loc[date_dict['today'],'MedEur'] - demand_basin.loc[date_dict['last_month'],'MedEur']
        dfdemand.loc[11,'MoM'] = demand_basin.loc[date_dict['today'],'OtherEur'] - demand_basin.loc[date_dict['last_month'],'OtherEur']
        dfdemand.loc[12,'MoM'] = demand_basin.loc[date_dict['today'],'LatAm'] - demand_basin.loc[date_dict['last_month'],'LatAm']
        dfdemand.loc[13,'MoM'] = demand_basin.loc[date_dict['today'],'OtherRoW'] - demand_basin.loc[date_dict['last_month'],'OtherRoW']
        dfdemand.loc[3:13, 'MoM'] = dfdemand.loc[3:13, 'MoM'].astype('float').round(2)
        #Actual vs. Forecast
        dfdemand.loc[3,'Actual vs. Forecast'] = dfdemand.loc[3,'M'] - dfdemand.loc[3,' M '] 
        dfdemand.loc[4,'Actual vs. Forecast'] = dfdemand.loc[4,'M'] - dfdemand.loc[4,' M ']  
        dfdemand.loc[5,'Actual vs. Forecast'] = dfdemand.loc[5,'M'] - dfdemand.loc[5,' M '] 
        dfdemand.loc[6,'Actual vs. Forecast'] = dfdemand.loc[6,'M'] - dfdemand.loc[6,' M ']
        dfdemand.loc[7,'Actual vs. Forecast'] = dfdemand.loc[7,'M'] - dfdemand.loc[7,' M ']
        dfdemand.loc[8,'Actual vs. Forecast'] = dfdemand.loc[8,'M'] - dfdemand.loc[8,' M ']
        dfdemand.loc[9,'Actual vs. Forecast'] = dfdemand.loc[9,'M'] - dfdemand.loc[9,' M ']
        dfdemand.loc[10,'Actual vs. Forecast'] = dfdemand.loc[10,'M'] - dfdemand.loc[10,' M ']
        dfdemand.loc[11,'Actual vs. Forecast'] = dfdemand.loc[11,'M'] - dfdemand.loc[11,' M ']
        dfdemand.loc[12,'Actual vs. Forecast'] = dfdemand.loc[12,'M'] - dfdemand.loc[12,' M ']
        dfdemand.loc[13,'Actual vs. Forecast'] = dfdemand.loc[13,'M'] - dfdemand.loc[13,' M ']
        dfdemand.loc[3:13, 'Actual vs. Forecast'] = dfdemand.loc[3:13, 'Actual vs. Forecast'].astype('float').round(2)
        #Actual vs. Forecast %
        dfdemand.loc[3,'Actual vs. Forecast %'] = (dfdemand.loc[3,'M'] - dfdemand.loc[3,' M ']) /dfdemand.loc[3,'M']
        dfdemand.loc[4,'Actual vs. Forecast %'] = (dfdemand.loc[4,'M'] - dfdemand.loc[4,' M ']) /dfdemand.loc[4,'M']
        dfdemand.loc[5,'Actual vs. Forecast %'] = (dfdemand.loc[5,'M'] - dfdemand.loc[5,' M ']) /dfdemand.loc[5,'M']
        dfdemand.loc[6,'Actual vs. Forecast %'] = (dfdemand.loc[6,'M'] - dfdemand.loc[6,' M ']) /dfdemand.loc[6,'M']
        dfdemand.loc[7,'Actual vs. Forecast %'] = (dfdemand.loc[7,'M'] - dfdemand.loc[7,' M ']) /dfdemand.loc[7,'M']
        dfdemand.loc[8,'Actual vs. Forecast %'] = (dfdemand.loc[8,'M'] - dfdemand.loc[8,' M ']) /dfdemand.loc[8,'M']
        dfdemand.loc[9,'Actual vs. Forecast %'] = (dfdemand.loc[9,'M'] - dfdemand.loc[9,' M ']) /dfdemand.loc[9,'M']
        dfdemand.loc[10,'Actual vs. Forecast %'] = (dfdemand.loc[10,'M'] - dfdemand.loc[10,' M ']) /dfdemand.loc[10,'M']
        dfdemand.loc[11,'Actual vs. Forecast %'] = (dfdemand.loc[11,'M'] - dfdemand.loc[11,' M ']) /dfdemand.loc[11,'M']
        dfdemand.loc[12,'Actual vs. Forecast %'] = (dfdemand.loc[12,'M'] - dfdemand.loc[12,' M ']) /dfdemand.loc[12,'M']
        dfdemand.loc[13,'Actual vs. Forecast %'] = (dfdemand.loc[13,'M'] - dfdemand.loc[13,' M ']) /dfdemand.loc[13,'M']
        dfdemand.loc[3:13,'Actual vs. Forecast %'] = dfdemand.loc[3:13,'Actual vs. Forecast %'].map(lambda x:format(x,'.0%'))
        #Y-1 Kpler
        dfdemand.loc[3:,'Y-1'] = demand_basin.loc[date_dict['last_year'],'Global']
        dfdemand.loc[4:,'Y-1'] = demand_basin.loc[date_dict['last_year'],'PB']
        dfdemand.loc[5:,'Y-1'] = demand_basin.loc[date_dict['last_year'],'AB']
        dfdemand.loc[6:,'Y-1'] = demand_basin.loc[date_dict['last_year'],'MENA']
        dfdemand.loc[7,'Y-1'] = demand_basin.loc[date_dict['last_year'],'JKTC']
        dfdemand.loc[8,'Y-1'] = demand_basin.loc[date_dict['last_year'],'MEIP']
        dfdemand.loc[9,'Y-1'] = demand_basin.loc[date_dict['last_year'],'NWEur']
        dfdemand.loc[10,'Y-1'] = demand_basin.loc[date_dict['last_year'],'MedEur']
        dfdemand.loc[11,'Y-1'] = demand_basin.loc[date_dict['last_year'],'OtherEur']
        dfdemand.loc[12,'Y-1'] = demand_basin.loc[date_dict['last_year'],'LatAm']
        dfdemand.loc[13,'Y-1'] = demand_basin.loc[date_dict['last_year'],'OtherRoW']
        #D-1 Kpler
        dfdemand.loc[3:,'D-1'] = demand_basin.loc[date_dict['last_day'],'Global']
        dfdemand.loc[4:,'D-1'] = demand_basin.loc[date_dict['last_day'],'PB']
        dfdemand.loc[5:,'D-1'] = demand_basin.loc[date_dict['last_day'],'AB']
        dfdemand.loc[6:,'D-1'] = demand_basin.loc[date_dict['last_day'],'MENA']
        dfdemand.loc[7,'D-1'] = demand_basin.loc[date_dict['last_day'],'JKTC']
        dfdemand.loc[8,'D-1'] = demand_basin.loc[date_dict['last_day'],'MEIP']
        dfdemand.loc[9,'D-1'] = demand_basin.loc[date_dict['last_day'],'NWEur']
        dfdemand.loc[10,'D-1'] = demand_basin.loc[date_dict['last_day'],'MedEur']
        dfdemand.loc[11,'D-1'] = demand_basin.loc[date_dict['last_day'],'OtherEur']
        dfdemand.loc[12,'D-1'] = demand_basin.loc[date_dict['last_day'],'LatAm']
        dfdemand.loc[13,'D-1'] = demand_basin.loc[date_dict['last_day'],'OtherRoW']
        #D-7 Kpler
        dfdemand.loc[3:,'D-7'] = demand_basin.loc[date_dict['last_week'],'Global']
        dfdemand.loc[4:,'D-7'] = demand_basin.loc[date_dict['last_week'],'PB']
        dfdemand.loc[5:,'D-7'] = demand_basin.loc[date_dict['last_week'],'AB']
        dfdemand.loc[6:,'D-7'] = demand_basin.loc[date_dict['last_week'],'MENA']
        dfdemand.loc[7,'D-7'] = demand_basin.loc[date_dict['last_week'],'JKTC']
        dfdemand.loc[8,'D-7'] = demand_basin.loc[date_dict['last_week'],'MEIP']
        dfdemand.loc[9,'D-7'] = demand_basin.loc[date_dict['last_week'],'NWEur']
        dfdemand.loc[10,'D-7'] = demand_basin.loc[date_dict['last_week'],'MedEur']
        dfdemand.loc[11,'D-7'] = demand_basin.loc[date_dict['last_week'],'OtherEur']
        dfdemand.loc[12,'D-7'] = demand_basin.loc[date_dict['last_week'],'LatAm']
        dfdemand.loc[13,'D-7'] = demand_basin.loc[date_dict['last_week'],'OtherRoW']
        #DoD Kpler
        dfdemand.loc[3,'DoD'] = dfdemand.loc[3,'M'] - dfdemand.loc[3,'D-1']
        dfdemand.loc[4,'DoD'] = dfdemand.loc[4,'M'] - dfdemand.loc[4,'D-1']
        dfdemand.loc[5,'DoD'] = dfdemand.loc[5,'M'] - dfdemand.loc[5,'D-1']
        dfdemand.loc[6,'DoD'] = dfdemand.loc[6,'M'] - dfdemand.loc[6,'D-1']
        dfdemand.loc[7,'DoD'] = dfdemand.loc[7,'M'] - dfdemand.loc[7,'D-1']
        dfdemand.loc[8,'DoD'] = dfdemand.loc[8,'M'] - dfdemand.loc[8,'D-1']
        dfdemand.loc[9,'DoD'] = dfdemand.loc[9,'M'] - dfdemand.loc[9,'D-1']
        dfdemand.loc[10,'DoD'] = dfdemand.loc[10,'M'] - dfdemand.loc[10,'D-1']
        dfdemand.loc[11,'DoD'] = dfdemand.loc[11,'M'] - dfdemand.loc[11,'D-1']
        dfdemand.loc[12,'DoD'] = dfdemand.loc[12,'M'] - dfdemand.loc[12,'D-1']
        dfdemand.loc[13,'DoD'] = dfdemand.loc[13,'M'] - dfdemand.loc[13,'D-1']
        dfdemand.loc[3:13, 'DoD'] = dfdemand.loc[3:13, 'DoD'].astype('float').round(2)
        #WoW Kpler
        dfdemand.loc[3,'WoW'] = dfdemand.loc[3,'M'] - dfdemand.loc[3,'D-7']
        dfdemand.loc[4,'WoW'] = dfdemand.loc[4,'M'] - dfdemand.loc[4,'D-7']
        dfdemand.loc[5,'WoW'] = dfdemand.loc[5,'M'] - dfdemand.loc[5,'D-7']
        dfdemand.loc[6,'WoW'] = dfdemand.loc[6,'M'] - dfdemand.loc[6,'D-7']
        dfdemand.loc[7,'WoW'] = dfdemand.loc[7,'M'] - dfdemand.loc[7,'D-7']
        dfdemand.loc[8,'WoW'] = dfdemand.loc[8,'M'] - dfdemand.loc[8,'D-7']
        dfdemand.loc[9,'WoW'] = dfdemand.loc[9,'M'] - dfdemand.loc[9,'D-7']
        dfdemand.loc[10,'WoW'] = dfdemand.loc[10,'M'] - dfdemand.loc[10,'D-7']
        dfdemand.loc[11,'WoW'] = dfdemand.loc[11,'M'] - dfdemand.loc[11,'D-7']
        dfdemand.loc[12,'WoW'] = dfdemand.loc[12,'M'] - dfdemand.loc[12,'D-7']
        dfdemand.loc[13,'WoW'] = dfdemand.loc[13,'M'] - dfdemand.loc[13,'D-7']
        dfdemand.loc[3:13, 'WoW'] = dfdemand.loc[3:13, 'WoW'].astype('float').round(2)
        #dfdemand = dfdemand.round(2)
        #print(dfdemand)
        
        #print(DemandCategories)
            
        df_demand_country = pd.DataFrame(columns=['Suez','Basin','Region','Market','M-1 Kpler','M-1 Desk','M Kpler','M Desk','M+1 Desk','M+2 Desk','YoY Kpler','MoM Kpler','Actual vs. Forecast','Actual vs. Forecast %', 'Y-1 Kpler','D-1 Kpler','D-7 Kpler','DoD Kpler','WoW Kpler'])
        df_demand_country[['Suez','Basin','Region','Market']] = DemandCategories[['Suez','Basin','Region','Country']]
        
        df_demand_country.set_index('Market', inplace=True)
        #print(df_demand_country)
        
        for i in df_demand_country.index:
            #print(i)
            df_demand_country.loc[i,'M-1 Kpler'] = demand_basin.loc[date_dict['last_month'],i]
            df_demand_country.loc[i,'M Kpler'] = demand_basin.loc[date_dict['today'],i]
            df_demand_country.loc[i,'M Desk'] = demand_basin.loc[date_dict['today'],i+' Desk']
            df_demand_country.loc[i,'M+1 Desk'] = demand_basin.loc[date_dict['next_1month'],i+' Desk']
            df_demand_country.loc[i,'M+2 Desk'] = demand_basin.loc[date_dict['next_2month'],i+' Desk']
            df_demand_country.loc[i,'M-1 Desk'] = demand_basin.loc[date_dict['last_month'],i+' Desk']
            df_demand_country.loc[i,'YoY Kpler'] = (demand_basin.loc[date_dict['today'],i] - demand_basin.loc[date_dict['last_year'],i]).round(2)
            df_demand_country.loc[i,'MoM Kpler'] = (demand_basin.loc[date_dict['today'],i] - demand_basin.loc[date_dict['last_month'],i]).round(2)
            df_demand_country.loc[i,'Actual vs. Forecast'] = (df_demand_country.loc[i,'M Kpler'] - df_demand_country.loc[i,'M Desk']).round(2)
            df_demand_country.loc[i,'Actual vs. Forecast %'] = (df_demand_country.loc[i,'M Kpler'] - df_demand_country.loc[i,'M Desk']) /df_demand_country.loc[i,'M Kpler']
            
            df_demand_country.loc[i,'Y-1 Kpler'] = demand_basin.loc[date_dict['last_year'],i]
            df_demand_country.loc[i,'D-1 Kpler'] = demand_basin.loc[date_dict['last_day'],i]
            df_demand_country.loc[i,'D-7 Kpler'] = demand_basin.loc[date_dict['last_week'],i]
            df_demand_country.loc[i,'DoD Kpler'] = (df_demand_country.loc[i,'M Kpler'] - df_demand_country.loc[i,'D-1 Kpler']).round(2)
            df_demand_country.loc[i,'WoW Kpler'] = (df_demand_country.loc[i,'M Kpler'] - df_demand_country.loc[i,'D-7 Kpler']).round(2)
        df_demand_country.loc[:,'Actual vs. Forecast %'] = df_demand_country.loc[:,'Actual vs. Forecast %'].map(lambda x:format(x,'.0%'))
        df_demand_country.sort_values(by='Region', inplace=True)
        df_demand_country.reset_index(inplace=True)
        df_demand_country = df_demand_country[['Suez','Basin','Region','Market','M-1 Kpler','M-1 Desk','M Kpler','M Desk','M+1 Desk','M+2 Desk','YoY Kpler','MoM Kpler','Actual vs. Forecast','Actual vs. Forecast %', 'Y-1 Kpler','D-1 Kpler','D-7 Kpler','DoD Kpler','WoW Kpler']]
        #print(df_demand_country)
        return dfdemand, df_demand_country




    def discrete_background_color_bins(df, n_bins=10, columns='all'):
        import colorlover
        bounds = [i * (1.0 / n_bins) for i in range(n_bins + 1)]
        if columns == 'all':
            if 'id' in df:
                df_numeric_columns = df.select_dtypes('number').drop(['id'], axis=1)
            else:
                df_numeric_columns = df.select_dtypes('number')
        else:
            df_numeric_columns = df[columns]
        df_max = df_numeric_columns.max().max()
        df_min = df_numeric_columns.min().min()
        ranges = [
            ((df_max - df_min) * i) + df_min
            for i in bounds
        ]
        styles = []
        legend = []
        for i in range(1, len(bounds)):
            min_bound = ranges[i - 1]
            max_bound = ranges[i]
            backgroundColor = colorlover.scales[str(n_bins)]['div']['RdYlGn'][i - 1]  # 'seq' or 'div' color in https://plotly.com/python/builtin-colorscales/
            color = 'white' if i > len(bounds) / 2. else 'inherit'
    
            for column in df_numeric_columns:
                styles.append({
                    'if': {
                        'filter_query': (
                            '{{{column}}} >= {min_bound}' +
                            (' && {{{column}}} < {max_bound}' if (i < len(bounds) - 1) else '')
                        ).format(column=column, min_bound=min_bound, max_bound=max_bound),
                        'column_id': column
                    },
                    'backgroundColor': backgroundColor,
                    'color': color
                })
            legend.append(
                html.Div(style={'display': 'inline-block', 'width': '60px'}, children=[
                    html.Div(
                        style={
                            'backgroundColor': backgroundColor,
                            'borderLeft': '1px rgb(50, 50, 50) solid',
                            'height': '10px'
                        }
                    ),
                    html.Small(round(min_bound, 2), style={'paddingLeft': '2px'})
                ])
            )
    
        return (styles, html.Div(legend, style={'padding': '5px 0 5px 0'}))



    def get_new_data():

        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        #get data
        dfsupplyMA, dfdemandMA, dfsupplyMA_eta, dfdemandMA_eta, desk_supply_view, desk_demand_view, ihscontract, SupplyCategories, DemandCategories,supply_basin, supply_cumulative, demand_basin, demand_cumulative = SD_Table_10.get_sd_data()
        dfsupply, df_supply_plant = SD_Table_10.gpam_supply_table(supply_basin, dfsupplyMA, desk_supply_view, SupplyCategories)
        dfdemand, df_demand_country = SD_Table_10.gpam_demand_table(demand_basin, dfdemandMA, desk_demand_view, DemandCategories)#this
        #supply data and style
        dfdatasupply=pd.DataFrame(columns=['M-1',' M-1 ','M', ' M ', 'M+1', 'M+2', 'YoY', 'MoM', 'Actual vs. Forecast', '2020', '2021', ' 2021 ', 'DoD', 'WoW'])
        dfdatasupply=dfsupply.loc[3:13]
        dfdatasupplyplant=df_supply_plant
        headersupply=[list(dfsupply.columns[0:]), list(dfsupply.iloc[0,]),list(dfsupply.loc[1]),list(dfsupply.loc[2])]
        headersupply=list(map(list, zip(*headersupply)))
        hdlistsuply = []
        print('get new data time', now)
        
        for i in headersupply:
            hd={"name":i,"id": i[0],'deletable' : True }
            hdlistsuply.append(hd)
        
        hdlistsupply2=[{"name": i, "id": i,'deletable' : True } for i in df_supply_plant.columns]
        
        dfcolorsupply1=pd.DataFrame(dfsupply[['Actual vs. Forecast','DoD','WoW']].loc[3:6].astype('float').values, columns=['Actual vs. Forecast','DoD','WoW'])
        dfcolorsupply2=pd.DataFrame(dfsupply[['Actual vs. Forecast','DoD','WoW']].loc[8:14].astype('float').values, columns=['Actual vs. Forecast','DoD','WoW'])
        dfcolorsupply3=pd.DataFrame(df_supply_plant[['Actual vs. Forecast','DoD Kpler', 'WoW Kpler']].astype('float').values, columns=['Actual vs. Forecast','DoD Kpler', 'WoW Kpler'])
        
        (styles1s, legend) = SD_Table_10.discrete_background_color_bins(dfcolorsupply1, columns=['Actual vs. Forecast'])
        (styles2s, legend) = SD_Table_10.discrete_background_color_bins(dfcolorsupply1, columns=['DoD'])
        (styles3s, legend) = SD_Table_10.discrete_background_color_bins(dfcolorsupply1, columns=['WoW'])
        (styles4s, legend) = SD_Table_10.discrete_background_color_bins(dfcolorsupply2, columns=['Actual vs. Forecast'])
        (styles5s, legend) = SD_Table_10.discrete_background_color_bins(dfcolorsupply2, columns=['DoD'])
        (styles6s, legend) = SD_Table_10.discrete_background_color_bins(dfcolorsupply2, columns=['WoW'])
        (styles7s, legend) = SD_Table_10.discrete_background_color_bins(dfcolorsupply3, columns=['Actual vs. Forecast'])
        (styles8s, legend) = SD_Table_10.discrete_background_color_bins(dfcolorsupply3, columns=['DoD Kpler'])
        (styles9s, legend) = SD_Table_10.discrete_background_color_bins(dfcolorsupply3, columns=['WoW Kpler'])
        stylessupplylow = styles7s+ styles8s+styles9s
        stylessupplyup = styles1s+ styles2s+styles3s+styles4s+styles5s+styles6s
        #demand data and style
        dfdatademand=pd.DataFrame(columns=['M-1',' M-1 ', 'M', ' M ', 'M+1', 'M+2', 'YoY', 'MoM', 'Actual vs. Forecast', '2020', '2021', ' 2021 ', 'DoD', 'WoW'])
        dfdatademand=dfdemand.loc[3:13]
        
        dfdatademandcountry=df_demand_country
        headerdemand=[list(dfdemand.columns[0:]), list(dfdemand.iloc[0,]),list(dfdemand.loc[1]),list(dfdemand.loc[2])]
        headerdemand=list(map(list, zip(*headerdemand)))
        hdlistdemand = []
        
        for i in headerdemand:
            hd={"name":i,"id": i[0],'deletable' : True }
            
            hdlistdemand.append(hd)
        
        hdlistdemand2=[{"name": i, "id": i, 'deletable' : True } for i in df_demand_country.columns]
        dfcolordemand1=pd.DataFrame(dfdemand[['Actual vs. Forecast','DoD','WoW']].loc[3:6].astype('float').values, columns=['Actual vs. Forecast','DoD','WoW'])
        dfcolordemand2=pd.DataFrame(dfdemand[['Actual vs. Forecast','DoD','WoW']].loc[8:14].astype('float').values, columns=['Actual vs. Forecast','DoD','WoW'])
        dfcolordemand3=pd.DataFrame(df_demand_country[['Actual vs. Forecast','DoD Kpler', 'WoW Kpler']].astype('float').values, columns=['Actual vs. Forecast','DoD Kpler', 'WoW Kpler'])
        
        (styles1d, legend) = SD_Table_10.discrete_background_color_bins(dfcolordemand1, columns=['Actual vs. Forecast'])
        (styles2d, legend) = SD_Table_10.discrete_background_color_bins(dfcolordemand1, columns=['DoD'])
        (styles3d, legend) = SD_Table_10.discrete_background_color_bins(dfcolordemand1, columns=['WoW'])
        (styles4d, legend) = SD_Table_10.discrete_background_color_bins(dfcolordemand2, columns=['Actual vs. Forecast'])
        (styles5d, legend) = SD_Table_10.discrete_background_color_bins(dfcolordemand2, columns=['DoD'])
        (styles6d, legend) = SD_Table_10.discrete_background_color_bins(dfcolordemand2, columns=['WoW'])
        (styles7d, legend) = SD_Table_10.discrete_background_color_bins(dfcolordemand3, columns=['Actual vs. Forecast'])
        (styles8d, legend) = SD_Table_10.discrete_background_color_bins(dfcolordemand3, columns=['DoD Kpler'])
        (styles9d, legend) = SD_Table_10.discrete_background_color_bins(dfcolordemand3, columns=['WoW Kpler'])
        stylesdemandlow = styles7d+ styles8d+styles9d
        stylesdemandup = styles1d+ styles2d+styles3d+styles4d+ styles5d+styles6d
        
        
        
        return dfsupply, df_supply_plant,hdlistsuply,dfdatasupply,stylessupplyup,hdlistsupply2,dfdatasupplyplant,stylessupplylow, hdlistdemand, dfdatademand,stylesdemandup, hdlistdemand2, dfdatademandcountry,stylesdemandlow


#MA30 data

class SD_Table_30():
    
    
    #create dict of date
    def refresh_date():
        date_dict={
               'today' : str(datetime.date.today()),
               'last_day' : str(datetime.date.today() - datetime.timedelta(days=1)*1),
               'last_week' : str(datetime.date.today() - datetime.timedelta(weeks=1)*1),
               'last_month' : str(datetime.date.today() - relativedelta(months=1)),
               'next_1month' : str(datetime.date.today() + relativedelta(months=1)),
               'next_2month' : str(datetime.date.today() + relativedelta(months=2)),
               'last_year' : str(datetime.date.today() - relativedelta(months=12)),
               'now' : str(datetime.datetime.now())
               }
        return date_dict
    
    def get_sd_data():
        
        #get data from DB
        demand=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'DemandCountry30')
        dfdemandMA=demand.sql_to_df()
        dfdemandMA.set_index('Date', inplace=True)
        
        supplyMA=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'SupplyCountry30')
        dfsupplyMA=supplyMA.sql_to_df()
        dfsupplyMA.set_index('Date', inplace=True)
        
        supplyMA_eta=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'SupplyPlantETA30')
        dfsupplyMA_eta=supplyMA_eta.sql_to_df()
        dfsupplyMA_eta.set_index('Date', inplace=True)
        
        demandMA_eta=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'DemandETA30')
        dfdemandMA_eta=demandMA_eta.sql_to_df()
        dfdemandMA_eta.set_index('Date', inplace=True)
        
        desk_supply_plant_view=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'DeskSupplyPlant')
        desk_supply_plant_view=desk_supply_plant_view.sql_to_df()
        desk_supply_plant_view.set_index('Date', inplace=True)
        
        desksupplyview=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'DeskSupplyCountry')
        desk_supply_view=desksupplyview.sql_to_df()
        desk_supply_view.set_index('Date', inplace=True)
        
        deskdemandview=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'DeskDemand')
        desk_demand_view=deskdemandview.sql_to_df()
        desk_demand_view.set_index('Date', inplace=True)
        
        
        ihscontract=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'IHSContract')
        dfihscontract=ihscontract.sql_to_df()
        dfihscontract.set_index('Date', inplace=True)
        
        #categories index
        SupplyCategories = pd.read_excel('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/Categories.xlsx',header=(0),sheet_name='Index_Supply')
        SupplyCategories = SupplyCategories.iloc[:44,0:5]
        #SupplyCategories.drop(SupplyCategories[SupplyCategories['Plant']=='Calcasieu Pass'].index, inplace=True)
        SupplyCategories.drop(SupplyCategories[SupplyCategories['Plant']=='Mozambique Area 1'].index, inplace=True)
        
        DemandCategories = pd.read_excel('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/Categories.xlsx',header=(0),sheet_name='Index_Demand')
        DemandCategories = DemandCategories.iloc[:64,0:6]
        droplist = ['Algeria','Angola','Bahamas','Brunei','Cameroon','Equatorial Guinea','Libya','Nigeria','Papua New Guinea','Peru','Philipines','Qatar','Russian Federation','Trinidad and Tobago','Yemen']
        for i in droplist:
            DemandCategories.drop(DemandCategories[DemandCategories['Country']==i].index, inplace=True)
            
        #print(df_supply)
        basinsupply=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'BasinSupply30')
        dfbasinsupply=basinsupply.sql_to_df()
        dfbasinsupply.set_index('Date', inplace=True)
        
        basindemand=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'BasinDemand30')
        dfbasindemand=basindemand.sql_to_df()
        dfbasindemand.set_index('Date', inplace=True)
        
        cumsupply=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'CumulativeSupply30')
        dfcumsupply=cumsupply.sql_to_df()
        dfcumsupply.rename(columns=({'index':'Date'}), inplace=True)
        dfcumsupply.set_index('Date', inplace=True)
        
        cumdemand=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'CumulativeDemand30')
        dfcumdemand=cumdemand.sql_to_df()
        dfcumdemand.rename(columns=({'index':'Date'}), inplace=True)
        dfcumdemand.set_index('Date', inplace=True)
       
        return dfsupplyMA, dfdemandMA, dfsupplyMA_eta, dfdemandMA_eta, desk_supply_view, desk_demand_view, dfihscontract, SupplyCategories, DemandCategories,dfbasinsupply, dfcumsupply, dfbasindemand, dfcumdemand
      
  
    def gpam_supply_table(supply_basin,dfsupplyMA, desk_supply_view, SupplyCategories):
        
        dfsupplyMA, desk_supply_view = dfsupplyMA.round(2), desk_supply_view.round(2)
        date_dict = SD_Table_30.refresh_date()
        #
        date_columns = ['',date_dict['last_month'], date_dict['last_month'],date_dict['today'],date_dict['today'],date_dict['next_1month'],date_dict['next_2month'],'','','','',date_dict['last_year'],date_dict['last_day'],date_dict['last_week'],'','']
        
        dfsupply = pd.DataFrame(columns=['Year','SupplyMA10','M-1',' M-1 ','M',' M ','M+1','M+2','YoY','MoM','Actual vs. Forecast','Actual vs. Forecast %', 'Y-1','D-1','D-7','DoD','WoW'])

        dfsupply['Year']=['Date','Source','Unit','Global','PB','AB','MENA_Bas','Algeria', 'Australia','Nigeria','Qatar','Russian Federation','Trinidad and Tobago','United States']
        dfsupply.loc[0,'SupplyMA10':] = date_columns
        dfsupply.loc[1,'SupplyMA10':] = ['','Kpler','Desk','Kpler','Desk','Desk','Desk','Kpler','Kpler','Indicator','Indicator','Kpler','Kpler','Kpler','Kpler','Kpler']
        dfsupply.loc[2,'SupplyMA10':] = ['','Mcm/d','Mcm/d','Mcm/d','Mcm/d','Mcm/d','Mcm/d','Mcm/d','Mcm/d','Mcm/d','%','','','','','']
        dfsupply['SupplyMA10'] = ''
        #M-1 Kpler
        dfsupply.loc[3,'M-1'] = supply_basin.loc[date_dict['last_month'],'Global']
        dfsupply.loc[4,'M-1'] = supply_basin.loc[date_dict['last_month'],'PB']
        dfsupply.loc[5,'M-1'] = supply_basin.loc[date_dict['last_month'],'AB']
        dfsupply.loc[6,'M-1'] = supply_basin.loc[date_dict['last_month'],'MENA']
        dfsupply.loc[7,'M-1'] = dfsupplyMA.loc[date_dict['last_month'],'Algeria']
        dfsupply.loc[8,'M-1'] = dfsupplyMA.loc[date_dict['last_month'],'Australia']
        dfsupply.loc[9,'M-1'] = dfsupplyMA.loc[date_dict['last_month'],'Nigeria']
        dfsupply.loc[10,'M-1'] = dfsupplyMA.loc[date_dict['last_month'],'Qatar']
        dfsupply.loc[11,'M-1'] = dfsupplyMA.loc[date_dict['last_month'],'Russian Federation']
        dfsupply.loc[12,'M-1'] = dfsupplyMA.loc[date_dict['last_month'],'Trinidad and Tobago']
        dfsupply.loc[13,'M-1'] = dfsupplyMA.loc[date_dict['last_month'],'United States']
        #M Kpler
        dfsupply.loc[3,'M'] = supply_basin.loc[date_dict['today'],'Global']
        dfsupply.loc[4,'M'] = supply_basin.loc[date_dict['today'],'PB']
        dfsupply.loc[5,'M'] = supply_basin.loc[date_dict['today'],'AB']
        dfsupply.loc[6,'M'] = supply_basin.loc[date_dict['today'],'MENA']
        dfsupply.loc[7,'M'] = dfsupplyMA.loc[date_dict['today'],'Algeria']
        dfsupply.loc[8,'M'] = dfsupplyMA.loc[date_dict['today'],'Australia']
        dfsupply.loc[9,'M'] = dfsupplyMA.loc[date_dict['today'],'Nigeria']
        dfsupply.loc[10,'M'] = dfsupplyMA.loc[date_dict['today'],'Qatar']
        dfsupply.loc[11,'M'] = dfsupplyMA.loc[date_dict['today'],'Russian Federation']
        dfsupply.loc[12,'M'] = dfsupplyMA.loc[date_dict['today'],'Trinidad and Tobago']
        dfsupply.loc[13,'M'] = dfsupplyMA.loc[date_dict['today'],'United States']
        #M Desk
        dfsupply.loc[3,' M '] = supply_basin.loc[date_dict['today'],'Global Desk']
        dfsupply.loc[4,' M '] = supply_basin.loc[date_dict['today'],'PB Desk']
        dfsupply.loc[5,' M '] = supply_basin.loc[date_dict['today'],'AB Desk']
        dfsupply.loc[6,' M '] = supply_basin.loc[date_dict['today'],'MENA Desk']
        dfsupply.loc[7,' M '] = desk_supply_view.loc[date_dict['today'],'Algeria']
        dfsupply.loc[8,' M '] = desk_supply_view.loc[date_dict['today'],'Australia']
        dfsupply.loc[9,' M '] = desk_supply_view.loc[date_dict['today'],'Nigeria']
        dfsupply.loc[10,' M '] = desk_supply_view.loc[date_dict['today'],'Qatar']
        dfsupply.loc[11,' M '] = desk_supply_view.loc[date_dict['today'],'Russian Federation']
        dfsupply.loc[12,' M '] = desk_supply_view.loc[date_dict['today'],'Trinidad and Tobago']
        dfsupply.loc[13,' M '] = desk_supply_view.loc[date_dict['today'],'United States']
        #M+1 Desk
        dfsupply.loc[3,'M+1'] = supply_basin.loc[date_dict['next_1month'],'Global Desk']
        dfsupply.loc[4,'M+1'] = supply_basin.loc[date_dict['next_1month'],'PB Desk']
        dfsupply.loc[5,'M+1'] = supply_basin.loc[date_dict['next_1month'],'AB Desk']
        dfsupply.loc[6,'M+1'] = supply_basin.loc[date_dict['next_1month'],'MENA Desk']
        dfsupply.loc[7,'M+1'] = desk_supply_view.loc[date_dict['next_1month'],'Algeria']
        dfsupply.loc[8,'M+1'] = desk_supply_view.loc[date_dict['next_1month'],'Australia']
        dfsupply.loc[9,'M+1'] = desk_supply_view.loc[date_dict['next_1month'],'Nigeria']
        dfsupply.loc[10,'M+1'] = desk_supply_view.loc[date_dict['next_1month'],'Qatar']
        dfsupply.loc[11,'M+1'] = desk_supply_view.loc[date_dict['next_1month'],'Russian Federation']
        dfsupply.loc[12,'M+1'] = desk_supply_view.loc[date_dict['next_1month'],'Trinidad and Tobago']
        dfsupply.loc[13,'M+1'] = desk_supply_view.loc[date_dict['next_1month'],'United States']
        #M+2 Desk
        dfsupply.loc[3,'M+2'] = supply_basin.loc[date_dict['next_2month'],'Global Desk']
        dfsupply.loc[4,'M+2'] = supply_basin.loc[date_dict['next_2month'],'PB Desk']
        dfsupply.loc[5,'M+2'] = supply_basin.loc[date_dict['next_2month'],'AB Desk']
        dfsupply.loc[6,'M+2'] = supply_basin.loc[date_dict['next_2month'],'MENA Desk']
        dfsupply.loc[7,'M+2'] = desk_supply_view.loc[date_dict['next_2month'],'Algeria']
        dfsupply.loc[8,'M+2'] = desk_supply_view.loc[date_dict['next_2month'],'Australia']
        dfsupply.loc[9,'M+2'] = desk_supply_view.loc[date_dict['next_2month'],'Nigeria']
        dfsupply.loc[10,'M+2'] = desk_supply_view.loc[date_dict['next_2month'],'Qatar']
        dfsupply.loc[11,'M+2'] = desk_supply_view.loc[date_dict['next_2month'],'Russian Federation']
        dfsupply.loc[12,'M+2'] = desk_supply_view.loc[date_dict['next_2month'],'Trinidad and Tobago']
        dfsupply.loc[13,'M+2'] = desk_supply_view.loc[date_dict['next_2month'],'United States']
        #M-1 Desk
        dfsupply.loc[3,' M-1 '] = supply_basin.loc[date_dict['last_month'],'Global Desk']
        dfsupply.loc[4,' M-1 '] = supply_basin.loc[date_dict['last_month'],'PB Desk']
        dfsupply.loc[5,' M-1 '] = supply_basin.loc[date_dict['last_month'],'AB Desk']
        dfsupply.loc[6,' M-1 '] = supply_basin.loc[date_dict['last_month'],'MENA Desk']
        dfsupply.loc[7,' M-1 '] = desk_supply_view.loc[date_dict['last_month'],'Algeria']
        dfsupply.loc[8,' M-1 '] = desk_supply_view.loc[date_dict['last_month'],'Australia']
        dfsupply.loc[9,' M-1 '] = desk_supply_view.loc[date_dict['last_month'],'Nigeria']
        dfsupply.loc[10,' M-1 '] = desk_supply_view.loc[date_dict['last_month'],'Qatar']
        dfsupply.loc[11,' M-1 '] = desk_supply_view.loc[date_dict['last_month'],'Russian Federation']
        dfsupply.loc[12,' M-1 '] = desk_supply_view.loc[date_dict['last_month'],'Trinidad and Tobago']
        dfsupply.loc[13,' M-1 '] = desk_supply_view.loc[date_dict['last_month'],'United States']
        #YoY Kpler
        dfsupply.loc[3,'YoY'] = (supply_basin.loc[date_dict['today'],'Global'] - supply_basin.loc[date_dict['last_year'],'Global']).round(2)
        dfsupply.loc[4,'YoY'] = (supply_basin.loc[date_dict['today'],'PB'] - supply_basin.loc[date_dict['last_year'],'PB']).round(2) 
        dfsupply.loc[5,'YoY'] = (supply_basin.loc[date_dict['today'],'AB'] - supply_basin.loc[date_dict['last_year'],'AB']).round(2)  
        dfsupply.loc[6,'YoY'] = (supply_basin.loc[date_dict['today'],'MENA'] - supply_basin.loc[date_dict['last_year'],'AB']).round(2) 
        dfsupply.loc[7,'YoY'] = (dfsupplyMA.loc[date_dict['today'],'Algeria'] - dfsupplyMA.loc[date_dict['last_year'],'Algeria']).round(2) 
        dfsupply.loc[8,'YoY'] = (dfsupplyMA.loc[date_dict['today'],'Australia'] - dfsupplyMA.loc[date_dict['last_year'],'Australia']).round(2) 
        dfsupply.loc[9,'YoY'] = (dfsupplyMA.loc[date_dict['today'],'Nigeria'] - dfsupplyMA.loc[date_dict['last_year'],'Nigeria']).round(2) 
        dfsupply.loc[10,'YoY'] = (dfsupplyMA.loc[date_dict['today'],'Qatar'] - dfsupplyMA.loc[date_dict['last_year'],'Qatar']).round(2) 
        dfsupply.loc[11,'YoY'] = (dfsupplyMA.loc[date_dict['today'],'Russian Federation'] - dfsupplyMA.loc[date_dict['last_year'],'Russian Federation']).round(2) 
        dfsupply.loc[12,'YoY'] = (dfsupplyMA.loc[date_dict['today'],'Trinidad and Tobago'] - dfsupplyMA.loc[date_dict['last_year'],'Trinidad and Tobago']).round(2) 
        dfsupply.loc[13,'YoY'] = (dfsupplyMA.loc[date_dict['today'],'United States'] - dfsupplyMA.loc[date_dict['last_year'],'United States']).round(2) 
        #MoM Kpler
        dfsupply.loc[3,'MoM'] = (supply_basin.loc[date_dict['today'],'Global'] - supply_basin.loc[date_dict['last_month'],'Global']).round(2) 
        dfsupply.loc[4,'MoM'] = (supply_basin.loc[date_dict['today'],'PB'] - supply_basin.loc[date_dict['last_month'],'PB']).round(2)  
        dfsupply.loc[5,'MoM'] = (supply_basin.loc[date_dict['today'],'AB'] - supply_basin.loc[date_dict['last_month'],'AB']).round(2)  
        dfsupply.loc[6,'MoM'] = (supply_basin.loc[date_dict['today'],'MENA'] - supply_basin.loc[date_dict['last_month'],'AB']).round(2) 
        dfsupply.loc[7,'MoM'] = (dfsupplyMA.loc[date_dict['today'],'Algeria'] - dfsupplyMA.loc[date_dict['last_month'],'Algeria']).round(2) 
        dfsupply.loc[8,'MoM'] = (dfsupplyMA.loc[date_dict['today'],'Australia'] - dfsupplyMA.loc[date_dict['last_month'],'Australia']).round(2) 
        dfsupply.loc[9,'MoM'] = (dfsupplyMA.loc[date_dict['today'],'Nigeria'] - dfsupplyMA.loc[date_dict['last_month'],'Nigeria']).round(2) 
        dfsupply.loc[10,'MoM'] = (dfsupplyMA.loc[date_dict['today'],'Qatar'] - dfsupplyMA.loc[date_dict['last_month'],'Qatar']).round(2) 
        dfsupply.loc[11,'MoM'] = (dfsupplyMA.loc[date_dict['today'],'Russian Federation'] - dfsupplyMA.loc[date_dict['last_month'],'Russian Federation']).round(2) 
        dfsupply.loc[12,'MoM'] = (dfsupplyMA.loc[date_dict['today'],'Trinidad and Tobago'] - dfsupplyMA.loc[date_dict['last_month'],'Trinidad and Tobago']).round(2) 
        dfsupply.loc[13,'MoM'] = (dfsupplyMA.loc[date_dict['today'],'United States'] - dfsupplyMA.loc[date_dict['last_month'],'United States']).round(2) 
        #Actual vs. Forecast
        dfsupply.loc[3,'Actual vs. Forecast'] = dfsupply.loc[3,'M'] - dfsupply.loc[3,' M '] 
        dfsupply.loc[4,'Actual vs. Forecast'] = dfsupply.loc[4,'M'] - dfsupply.loc[4,' M ']  
        dfsupply.loc[5,'Actual vs. Forecast'] = dfsupply.loc[5,'M'] - dfsupply.loc[5,' M '] 
        dfsupply.loc[6,'Actual vs. Forecast'] = dfsupply.loc[6,'M'] - dfsupply.loc[6,' M ']
        dfsupply.loc[7,'Actual vs. Forecast'] = dfsupply.loc[7,'M'] - dfsupply.loc[7,' M ']
        dfsupply.loc[8,'Actual vs. Forecast'] = dfsupply.loc[8,'M'] - dfsupply.loc[8,' M ']
        dfsupply.loc[9,'Actual vs. Forecast'] = dfsupply.loc[9,'M'] - dfsupply.loc[9,' M ']
        dfsupply.loc[10,'Actual vs. Forecast'] = dfsupply.loc[10,'M'] - dfsupply.loc[10,' M ']
        dfsupply.loc[11,'Actual vs. Forecast'] = dfsupply.loc[11,'M'] - dfsupply.loc[11,' M ']
        dfsupply.loc[12,'Actual vs. Forecast'] = dfsupply.loc[12,'M'] - dfsupply.loc[12,' M ']
        dfsupply.loc[13,'Actual vs. Forecast'] = dfsupply.loc[13,'M'] - dfsupply.loc[13,' M ']
        dfsupply.loc[3:13, 'Actual vs. Forecast'] = dfsupply.loc[3:13, 'Actual vs. Forecast'].astype('float').round(2)
        #Actual vs. Forecast %
        dfsupply.loc[3,'Actual vs. Forecast %'] = (dfsupply.loc[3,'M'] - dfsupply.loc[3,' M ']) /dfsupply.loc[3,'M']
        dfsupply.loc[4,'Actual vs. Forecast %'] = (dfsupply.loc[4,'M'] - dfsupply.loc[4,' M ']) /dfsupply.loc[4,'M']
        dfsupply.loc[5,'Actual vs. Forecast %'] = (dfsupply.loc[5,'M'] - dfsupply.loc[5,' M ']) /dfsupply.loc[5,'M']
        dfsupply.loc[6,'Actual vs. Forecast %'] = (dfsupply.loc[6,'M'] - dfsupply.loc[6,' M ']) /dfsupply.loc[6,'M']
        dfsupply.loc[7,'Actual vs. Forecast %'] = (dfsupply.loc[7,'M'] - dfsupply.loc[7,' M ']) /dfsupply.loc[7,'M']
        dfsupply.loc[8,'Actual vs. Forecast %'] = (dfsupply.loc[8,'M'] - dfsupply.loc[8,' M ']) /dfsupply.loc[8,'M']
        dfsupply.loc[9,'Actual vs. Forecast %'] = (dfsupply.loc[9,'M'] - dfsupply.loc[9,' M ']) /dfsupply.loc[9,'M']
        dfsupply.loc[10,'Actual vs. Forecast %'] = (dfsupply.loc[10,'M'] - dfsupply.loc[10,' M ']) /dfsupply.loc[10,'M']
        dfsupply.loc[11,'Actual vs. Forecast %'] = (dfsupply.loc[11,'M'] - dfsupply.loc[11,' M ']) /dfsupply.loc[11,'M']
        dfsupply.loc[12,'Actual vs. Forecast %'] = (dfsupply.loc[12,'M'] - dfsupply.loc[12,' M ']) /dfsupply.loc[12,'M']
        dfsupply.loc[13,'Actual vs. Forecast %'] = (dfsupply.loc[13,'M'] - dfsupply.loc[13,' M ']) /dfsupply.loc[13,'M']
        dfsupply.loc[3:13,'Actual vs. Forecast %'] = dfsupply.loc[3:13,'Actual vs. Forecast %'].map(lambda x:format(x,'.0%'))
        #Y-1 Kpler
        dfsupply.loc[3:,'Y-1'] = supply_basin.loc[date_dict['last_year'],'Global']
        dfsupply.loc[4:,'Y-1'] = supply_basin.loc[date_dict['last_year'],'PB']
        dfsupply.loc[5:,'Y-1'] = supply_basin.loc[date_dict['last_year'],'AB']
        dfsupply.loc[6:,'Y-1'] = supply_basin.loc[date_dict['last_year'],'MENA']
        dfsupply.loc[7,'Y-1'] = dfsupplyMA.loc[date_dict['last_year'],'Algeria']
        dfsupply.loc[8,'Y-1'] = dfsupplyMA.loc[date_dict['last_year'],'Australia']
        dfsupply.loc[9,'Y-1'] = dfsupplyMA.loc[date_dict['last_year'],'Nigeria']
        dfsupply.loc[10,'Y-1'] = dfsupplyMA.loc[date_dict['last_year'],'Qatar']
        dfsupply.loc[11,'Y-1'] = dfsupplyMA.loc[date_dict['last_year'],'Russian Federation']
        dfsupply.loc[12,'Y-1'] = dfsupplyMA.loc[date_dict['last_year'],'Trinidad and Tobago']
        dfsupply.loc[13,'Y-1'] = dfsupplyMA.loc[date_dict['last_year'],'United States']
        #D-1 Kpler
        dfsupply.loc[3:,'D-1'] = supply_basin.loc[date_dict['last_day'],'Global']
        dfsupply.loc[4:,'D-1'] = supply_basin.loc[date_dict['last_day'],'PB']
        dfsupply.loc[5:,'D-1'] = supply_basin.loc[date_dict['last_day'],'AB']
        dfsupply.loc[6:,'D-1'] = supply_basin.loc[date_dict['last_day'],'MENA']
        dfsupply.loc[7,'D-1'] = dfsupplyMA.loc[date_dict['last_day'],'Algeria']
        dfsupply.loc[8,'D-1'] = dfsupplyMA.loc[date_dict['last_day'],'Australia']
        dfsupply.loc[9,'D-1'] = dfsupplyMA.loc[date_dict['last_day'],'Nigeria']
        dfsupply.loc[10,'D-1'] = dfsupplyMA.loc[date_dict['last_day'],'Qatar']
        dfsupply.loc[11,'D-1'] = dfsupplyMA.loc[date_dict['last_day'],'Russian Federation']
        dfsupply.loc[12,'D-1'] = dfsupplyMA.loc[date_dict['last_day'],'Trinidad and Tobago']
        dfsupply.loc[13,'D-1'] = dfsupplyMA.loc[date_dict['last_day'],'United States']
        #D-8 Kpler
        dfsupply.loc[3:,'D-7'] = supply_basin.loc[date_dict['last_week'],'Global']
        dfsupply.loc[4:,'D-7'] = supply_basin.loc[date_dict['last_week'],'PB']
        dfsupply.loc[5:,'D-7'] = supply_basin.loc[date_dict['last_week'],'AB']
        dfsupply.loc[6:,'D-7'] = supply_basin.loc[date_dict['last_week'],'MENA']
        dfsupply.loc[7,'D-7'] = dfsupplyMA.loc[date_dict['last_week'],'Algeria']
        dfsupply.loc[8,'D-7'] = dfsupplyMA.loc[date_dict['last_week'],'Australia']
        dfsupply.loc[9,'D-7'] = dfsupplyMA.loc[date_dict['last_week'],'Nigeria']
        dfsupply.loc[10,'D-7'] = dfsupplyMA.loc[date_dict['last_week'],'Qatar']
        dfsupply.loc[11,'D-7'] = dfsupplyMA.loc[date_dict['last_week'],'Russian Federation']
        dfsupply.loc[12,'D-7'] = dfsupplyMA.loc[date_dict['last_week'],'Trinidad and Tobago']
        dfsupply.loc[13,'D-7'] = dfsupplyMA.loc[date_dict['last_week'],'United States']
        #DoD Kpler
        dfsupply.loc[3,'DoD'] = dfsupply.loc[3,'M'] - dfsupply.loc[3,'D-1']
        dfsupply.loc[4,'DoD'] = dfsupply.loc[4,'M'] - dfsupply.loc[4,'D-1']
        dfsupply.loc[5,'DoD'] = dfsupply.loc[5,'M'] - dfsupply.loc[5,'D-1']
        dfsupply.loc[6,'DoD'] = dfsupply.loc[6,'M'] - dfsupply.loc[6,'D-1']
        dfsupply.loc[7,'DoD'] = dfsupply.loc[7,'M'] - dfsupply.loc[7,'D-1']
        dfsupply.loc[8,'DoD'] = dfsupply.loc[8,'M'] - dfsupply.loc[8,'D-1']
        dfsupply.loc[9,'DoD'] = dfsupply.loc[9,'M'] - dfsupply.loc[9,'D-1']
        dfsupply.loc[10,'DoD'] = dfsupply.loc[10,'M'] - dfsupply.loc[10,'D-1']
        dfsupply.loc[11,'DoD'] = dfsupply.loc[11,'M'] - dfsupply.loc[11,'D-1']
        dfsupply.loc[12,'DoD'] = dfsupply.loc[12,'M'] - dfsupply.loc[12,'D-1']
        dfsupply.loc[13,'DoD'] = dfsupply.loc[13,'M'] - dfsupply.loc[13,'D-1']
        dfsupply.loc[3:13, 'DoD'] = dfsupply.loc[3:13, 'DoD'].astype('float').round(2)
        #WoW Kpler
        dfsupply.loc[3,'WoW'] = dfsupply.loc[3,'M'] - dfsupply.loc[3,'D-7']
        dfsupply.loc[4,'WoW'] = dfsupply.loc[4,'M'] - dfsupply.loc[4,'D-7']
        dfsupply.loc[5,'WoW'] = dfsupply.loc[5,'M'] - dfsupply.loc[5,'D-7']
        dfsupply.loc[6,'WoW'] = dfsupply.loc[6,'M'] - dfsupply.loc[6,'D-7']
        dfsupply.loc[7,'WoW'] = dfsupply.loc[7,'M'] - dfsupply.loc[7,'D-7']
        dfsupply.loc[8,'WoW'] = dfsupply.loc[8,'M'] - dfsupply.loc[8,'D-7']
        dfsupply.loc[9,'WoW'] = dfsupply.loc[9,'M'] - dfsupply.loc[9,'D-7']
        dfsupply.loc[10,'WoW'] = dfsupply.loc[10,'M'] - dfsupply.loc[10,'D-7']
        dfsupply.loc[11,'WoW'] = dfsupply.loc[11,'M'] - dfsupply.loc[11,'D-7']
        dfsupply.loc[12,'WoW'] = dfsupply.loc[12,'M'] - dfsupply.loc[12,'D-7']
        dfsupply.loc[13,'WoW'] = dfsupply.loc[13,'M'] - dfsupply.loc[13,'D-7']
        dfsupply.loc[3:13, 'WoW'] = dfsupply.loc[3:13, 'WoW'].astype('float').round(2)
        #dfsupply = dfsupply.round(2)
        #dfsupply.loc[6,:]=dfsupply[6:,].append(pd.Series(), ignore_index=True)
        
        
        df_supply_plant = pd.DataFrame(columns=['Suez','Basin','Market','Plant','M-1 Kpler','M-1 Desk','M Kpler','M Desk','M+1 Desk','M+2 Desk','YoY Kpler','MoM Kpler','Actual vs. Forecast','Actual vs. Forecast %', 'Y-1 Kpler','D-1 Kpler','D-7 Kpler','DoD Kpler','WoW Kpler'])
        df_supply_plant[['Suez', 'Basin','Market','Plant']] = SupplyCategories[['Suez', 'Basin','Market','Plant']]
        
        df_supply_plant.set_index('Plant', inplace=True)
        
        #print(SupplyCategories)
        
        for i in df_supply_plant.index:
            df_supply_plant.loc[i,'M-1 Kpler'] = supply_basin.loc[date_dict['last_month'],i]
            df_supply_plant.loc[i,'M Kpler'] = supply_basin.loc[date_dict['today'],i]
            df_supply_plant.loc[i,'M Desk'] = supply_basin.loc[date_dict['today'],i+' Desk']
            df_supply_plant.loc[i,'M+1 Desk'] = supply_basin.loc[date_dict['next_1month'],i+' Desk']
            df_supply_plant.loc[i,'M+2 Desk'] = supply_basin.loc[date_dict['next_2month'],i+' Desk']
            df_supply_plant.loc[i,'M-1 Desk'] = supply_basin.loc[date_dict['last_month'],i+' Desk']
            df_supply_plant.loc[i,'YoY Kpler'] = (supply_basin.loc[date_dict['today'],i] - supply_basin.loc[date_dict['last_year'],i]).round(2)
            df_supply_plant.loc[i,'MoM Kpler'] = (supply_basin.loc[date_dict['today'],i] - supply_basin.loc[date_dict['last_month'],i]).round(2)
            #print(i,df_supply_plant.loc[i,'M Kpler'])
            #print(i,df_supply_plant.loc[i,'M Desk'])
            df_supply_plant.loc[i,'Actual vs. Forecast'] = (df_supply_plant.loc[i,'M Kpler'] - df_supply_plant.loc[i,'M Desk']).round(2)
            df_supply_plant.loc[i,'Actual vs. Forecast %'] = (df_supply_plant.loc[i,'M Kpler'] - df_supply_plant.loc[i,'M Desk']) /df_supply_plant.loc[i,'M Kpler']
            
            df_supply_plant.loc[i,'Y-1 Kpler'] = supply_basin.loc[date_dict['last_year'],i]
            df_supply_plant.loc[i,'D-1 Kpler'] = supply_basin.loc[date_dict['last_day'],i]
            df_supply_plant.loc[i,'D-7 Kpler'] = supply_basin.loc[date_dict['last_week'],i]
            df_supply_plant.loc[i,'DoD Kpler'] = (df_supply_plant.loc[i,'M Kpler'] - df_supply_plant.loc[i,'D-1 Kpler']).round(2)
            df_supply_plant.loc[i,'WoW Kpler'] = (df_supply_plant.loc[i,'M Kpler'] - df_supply_plant.loc[i,'D-7 Kpler']).round(2)
        df_supply_plant.loc[:,'Actual vs. Forecast %'] = df_supply_plant.loc[:,'Actual vs. Forecast %'].map(lambda x:format(x,'.0%'))
        df_supply_plant.sort_values(by='Basin', inplace=True)
        df_supply_plant.reset_index(inplace=True)
        df_supply_plant = df_supply_plant[['Suez','Basin','Market','Plant','M-1 Kpler','M-1 Desk','M Kpler','M Desk','M+1 Desk','M+2 Desk','YoY Kpler','MoM Kpler','Actual vs. Forecast','Actual vs. Forecast %', 'Y-1 Kpler','D-1 Kpler','D-7 Kpler','DoD Kpler','WoW Kpler']]
        
        return dfsupply, df_supply_plant
    
    #demand
    def gpam_demand_table(demand_basin, dfdemandMA, desk_demand_view,DemandCategories):
        
        desk_demand_view, dfdemandMA = desk_demand_view.round(2), dfdemandMA.round(2)
        
        date_dict = SD_Table_30.refresh_date()
        #demand_basin.to_csv('H:/Yuefeng/LNG Flows/Deskdatatestcom.csv')
        #print(date_dict['last_month'])
        #print(supply_basin.columns)
        date_columns = ['',date_dict['last_month'], date_dict['last_month'],date_dict['today'],date_dict['today'],date_dict['next_1month'],date_dict['next_2month'],'','','','',date_dict['last_year'],date_dict['last_day'],date_dict['last_week'],'','']
        
        dfdemand = pd.DataFrame(columns=['Year','DemandMA10','M-1',' M-1 ','M',' M ','M+1','M+2','YoY','MoM','Actual vs. Forecast','Actual vs. Forecast %', 'Y-1','D-1','D-7','DoD','WoW'])

        dfdemand['Year']=['Date','Source','Unit','Global','PB','AB','MENA_Bas','JKTC','MEIP', 'NW Eur','MedEur','Other Eur','Lat Am','Other RoW']
        dfdemand.loc[0,'DemandMA10':] = date_columns
        dfdemand.loc[1,'DemandMA10':] = ['','Kpler','Desk','Kpler','Desk','Desk','Desk','Kpler','Kpler','Indicator','Indicator','Kpler','Kpler','Kpler','Kpler','Kpler']
        dfdemand.loc[2,'DemandMA10':] = ['','Mcm/d','Mcm/d','Mcm/d','Mcm/d','Mcm/d','Mcm/d','Mcm/d','Mcm/d','Mcm/d','%','','','','','']
        dfdemand['DemandMA10'] = ''
        #M-1 Kpler
        dfdemand.loc[3,'M-1'] = demand_basin.loc[date_dict['last_month'],'Global']
        dfdemand.loc[4,'M-1'] = demand_basin.loc[date_dict['last_month'],'PB']
        dfdemand.loc[5,'M-1'] = demand_basin.loc[date_dict['last_month'],'AB']
        dfdemand.loc[6,'M-1'] = demand_basin.loc[date_dict['last_month'],'MENA']
        dfdemand.loc[7,'M-1'] = demand_basin.loc[date_dict['last_month'],'JKTC']
        dfdemand.loc[8,'M-1'] = demand_basin.loc[date_dict['last_month'],'MEIP']
        dfdemand.loc[9,'M-1'] = demand_basin.loc[date_dict['last_month'],'NWEur']
        dfdemand.loc[10,'M-1'] = demand_basin.loc[date_dict['last_month'],'MedEur']
        dfdemand.loc[11,'M-1'] = demand_basin.loc[date_dict['last_month'],'OtherEur']
        dfdemand.loc[12,'M-1'] = demand_basin.loc[date_dict['last_month'],'LatAm']
        dfdemand.loc[13,'M-1'] = demand_basin.loc[date_dict['last_month'],'OtherRoW']
        #M Kpler
        dfdemand.loc[3,'M'] = demand_basin.loc[date_dict['today'],'Global']
        dfdemand.loc[4,'M'] = demand_basin.loc[date_dict['today'],'PB']
        dfdemand.loc[5,'M'] = demand_basin.loc[date_dict['today'],'AB']
        dfdemand.loc[6,'M'] = demand_basin.loc[date_dict['today'],'MENA']
        dfdemand.loc[7,'M'] = demand_basin.loc[date_dict['today'],'JKTC']
        dfdemand.loc[8,'M'] = demand_basin.loc[date_dict['today'],'MEIP']
        dfdemand.loc[9,'M'] = demand_basin.loc[date_dict['today'],'NWEur']
        dfdemand.loc[10,'M'] = demand_basin.loc[date_dict['today'],'MedEur']
        dfdemand.loc[11,'M'] = demand_basin.loc[date_dict['today'],'OtherEur']
        dfdemand.loc[12,'M'] = demand_basin.loc[date_dict['today'],'LatAm']
        dfdemand.loc[13,'M'] = demand_basin.loc[date_dict['today'],'OtherRoW']
        #M Desk
        dfdemand.loc[3,' M '] = demand_basin.loc[date_dict['today'],'Global Desk']
        dfdemand.loc[4,' M '] = demand_basin.loc[date_dict['today'],'PB Desk']
        dfdemand.loc[5,' M '] = demand_basin.loc[date_dict['today'],'AB Desk']
        dfdemand.loc[6,' M '] = demand_basin.loc[date_dict['today'],'MENA Desk']
        dfdemand.loc[7,' M '] = demand_basin.loc[date_dict['today'],'JKTC Desk']
        dfdemand.loc[8,' M '] = demand_basin.loc[date_dict['today'],'MEIP Desk']
        dfdemand.loc[9,' M '] = demand_basin.loc[date_dict['today'],'NWEur Desk']
        dfdemand.loc[10,' M '] = demand_basin.loc[date_dict['today'],'MedEur Desk']
        dfdemand.loc[11,' M '] = demand_basin.loc[date_dict['today'],'OtherEur Desk']
        dfdemand.loc[12,' M '] = demand_basin.loc[date_dict['today'],'LatAm Desk']
        dfdemand.loc[13,' M '] = demand_basin.loc[date_dict['today'],'OtherRoW Desk']
        #M+1 Desk
        dfdemand.loc[3,'M+1'] = demand_basin.loc[date_dict['next_1month'],'Global Desk']
        dfdemand.loc[4,'M+1'] = demand_basin.loc[date_dict['next_1month'],'PB Desk']
        dfdemand.loc[5,'M+1'] = demand_basin.loc[date_dict['next_1month'],'AB Desk']
        dfdemand.loc[6,'M+1'] = demand_basin.loc[date_dict['next_1month'],'MENA Desk']
        dfdemand.loc[7,'M+1'] = demand_basin.loc[date_dict['next_1month'],'JKTC Desk']
        dfdemand.loc[8,'M+1'] = demand_basin.loc[date_dict['next_1month'],'MEIP Desk']
        dfdemand.loc[9,'M+1'] = demand_basin.loc[date_dict['next_1month'],'NWEur Desk']
        dfdemand.loc[10,'M+1'] = demand_basin.loc[date_dict['next_1month'],'MedEur Desk']
        dfdemand.loc[11,'M+1'] = demand_basin.loc[date_dict['next_1month'],'OtherEur Desk']
        dfdemand.loc[12,'M+1'] = demand_basin.loc[date_dict['next_1month'],'LatAm Desk']
        dfdemand.loc[13,'M+1'] = demand_basin.loc[date_dict['next_1month'],'OtherRoW Desk']
        #M+2 Desk
        dfdemand.loc[3,'M+2'] = demand_basin.loc[date_dict['next_2month'],'Global Desk']
        dfdemand.loc[4,'M+2'] = demand_basin.loc[date_dict['next_2month'],'PB Desk']
        dfdemand.loc[5,'M+2'] = demand_basin.loc[date_dict['next_2month'],'AB Desk']
        dfdemand.loc[6,'M+2'] = demand_basin.loc[date_dict['next_2month'],'MENA Desk']
        dfdemand.loc[7,'M+2'] = demand_basin.loc[date_dict['next_2month'],'JKTC Desk']
        dfdemand.loc[8,'M+2'] = demand_basin.loc[date_dict['next_2month'],'MEIP Desk']
        dfdemand.loc[9,'M+2'] = demand_basin.loc[date_dict['next_2month'],'NWEur Desk']
        dfdemand.loc[10,'M+2'] = demand_basin.loc[date_dict['next_2month'],'MedEur Desk']
        dfdemand.loc[11,'M+2'] = demand_basin.loc[date_dict['next_2month'],'OtherEur Desk']
        dfdemand.loc[12,'M+2'] = demand_basin.loc[date_dict['next_2month'],'LatAm Desk']
        dfdemand.loc[13,'M+2'] = demand_basin.loc[date_dict['next_2month'],'OtherRoW Desk']
        #M-1 Desk
        dfdemand.loc[3,' M-1 '] = demand_basin.loc[date_dict['last_month'],'Global Desk']
        dfdemand.loc[4,' M-1 '] = demand_basin.loc[date_dict['last_month'],'PB Desk']
        dfdemand.loc[5,' M-1 '] = demand_basin.loc[date_dict['last_month'],'AB Desk']
        dfdemand.loc[6,' M-1 '] = demand_basin.loc[date_dict['last_month'],'MENA Desk']
        dfdemand.loc[7,' M-1 '] = demand_basin.loc[date_dict['last_month'],'JKTC Desk']
        dfdemand.loc[8,' M-1 '] = demand_basin.loc[date_dict['last_month'],'MEIP Desk']
        dfdemand.loc[9,' M-1 '] = demand_basin.loc[date_dict['last_month'],'NWEur Desk']
        dfdemand.loc[10,' M-1 '] = demand_basin.loc[date_dict['last_month'],'MedEur Desk']
        dfdemand.loc[11,' M-1 '] = demand_basin.loc[date_dict['last_month'],'OtherEur Desk']
        dfdemand.loc[12,' M-1 '] = demand_basin.loc[date_dict['last_month'],'LatAm Desk']
        dfdemand.loc[13,' M-1 '] = demand_basin.loc[date_dict['last_month'],'OtherRoW Desk']
        #YoY Kpler
        dfdemand.loc[3,'YoY'] = demand_basin.loc[date_dict['today'],'Global'] - demand_basin.loc[date_dict['last_year'],'Global']
        dfdemand.loc[4,'YoY'] = demand_basin.loc[date_dict['today'],'PB'] - demand_basin.loc[date_dict['last_year'],'PB']
        dfdemand.loc[5,'YoY'] = demand_basin.loc[date_dict['today'],'AB'] - demand_basin.loc[date_dict['last_year'],'AB'] 
        dfdemand.loc[6,'YoY'] = demand_basin.loc[date_dict['today'],'MENA'] - demand_basin.loc[date_dict['last_year'],'AB']
        dfdemand.loc[7,'YoY'] = demand_basin.loc[date_dict['today'],'JKTC'] - demand_basin.loc[date_dict['last_year'],'JKTC']
        dfdemand.loc[8,'YoY'] = demand_basin.loc[date_dict['today'],'MEIP'] - demand_basin.loc[date_dict['last_year'],'MEIP']
        dfdemand.loc[9,'YoY'] = demand_basin.loc[date_dict['today'],'NWEur'] - demand_basin.loc[date_dict['last_year'],'NWEur']
        dfdemand.loc[10,'YoY'] = demand_basin.loc[date_dict['today'],'MedEur'] - demand_basin.loc[date_dict['last_year'],'MedEur']
        dfdemand.loc[11,'YoY'] = demand_basin.loc[date_dict['today'],'OtherEur'] - demand_basin.loc[date_dict['last_year'],'OtherEur']
        dfdemand.loc[12,'YoY'] = demand_basin.loc[date_dict['today'],'LatAm'] - demand_basin.loc[date_dict['last_year'],'LatAm']
        dfdemand.loc[13,'YoY'] = demand_basin.loc[date_dict['today'],'OtherRoW'] - demand_basin.loc[date_dict['last_year'],'OtherRoW']
        dfdemand.loc[3:13, 'YoY'] = dfdemand.loc[3:13, 'YoY'].astype('float').round(2)
        #MoM Kpler
        dfdemand.loc[3,'MoM'] = demand_basin.loc[date_dict['today'],'Global'] - demand_basin.loc[date_dict['last_month'],'Global'] 
        dfdemand.loc[4,'MoM'] = demand_basin.loc[date_dict['today'],'PB'] - demand_basin.loc[date_dict['last_month'],'PB'] 
        dfdemand.loc[5,'MoM'] = demand_basin.loc[date_dict['today'],'AB'] - demand_basin.loc[date_dict['last_month'],'AB'] 
        dfdemand.loc[6,'MoM'] = demand_basin.loc[date_dict['today'],'MENA'] - demand_basin.loc[date_dict['last_month'],'AB']
        dfdemand.loc[7,'MoM'] = demand_basin.loc[date_dict['today'],'JKTC'] - demand_basin.loc[date_dict['last_month'],'JKTC']
        dfdemand.loc[8,'MoM'] = demand_basin.loc[date_dict['today'],'MEIP'] - demand_basin.loc[date_dict['last_month'],'MEIP']
        dfdemand.loc[9,'MoM'] = demand_basin.loc[date_dict['today'],'NWEur'] - demand_basin.loc[date_dict['last_month'],'NWEur']
        dfdemand.loc[10,'MoM'] = demand_basin.loc[date_dict['today'],'MedEur'] - demand_basin.loc[date_dict['last_month'],'MedEur']
        dfdemand.loc[11,'MoM'] = demand_basin.loc[date_dict['today'],'OtherEur'] - demand_basin.loc[date_dict['last_month'],'OtherEur']
        dfdemand.loc[12,'MoM'] = demand_basin.loc[date_dict['today'],'LatAm'] - demand_basin.loc[date_dict['last_month'],'LatAm']
        dfdemand.loc[13,'MoM'] = demand_basin.loc[date_dict['today'],'OtherRoW'] - demand_basin.loc[date_dict['last_month'],'OtherRoW']
        dfdemand.loc[3:13, 'MoM'] = dfdemand.loc[3:13, 'MoM'].astype('float').round(2)
        #Actual vs. Forecast
        dfdemand.loc[3,'Actual vs. Forecast'] = dfdemand.loc[3,'M'] - dfdemand.loc[3,' M '] 
        dfdemand.loc[4,'Actual vs. Forecast'] = dfdemand.loc[4,'M'] - dfdemand.loc[4,' M ']
        dfdemand.loc[5,'Actual vs. Forecast'] = dfdemand.loc[5,'M'] - dfdemand.loc[5,' M '] 
        dfdemand.loc[6,'Actual vs. Forecast'] = dfdemand.loc[6,'M'] - dfdemand.loc[6,' M ']
        dfdemand.loc[7,'Actual vs. Forecast'] = dfdemand.loc[7,'M'] - dfdemand.loc[7,' M ']
        dfdemand.loc[8,'Actual vs. Forecast'] = dfdemand.loc[8,'M'] - dfdemand.loc[8,' M ']
        dfdemand.loc[9,'Actual vs. Forecast'] = dfdemand.loc[9,'M'] - dfdemand.loc[9,' M ']
        dfdemand.loc[10,'Actual vs. Forecast'] = dfdemand.loc[10,'M'] - dfdemand.loc[10,' M ']
        dfdemand.loc[11,'Actual vs. Forecast'] = dfdemand.loc[11,'M'] - dfdemand.loc[11,' M ']
        dfdemand.loc[12,'Actual vs. Forecast'] = dfdemand.loc[12,'M'] - dfdemand.loc[12,' M ']
        dfdemand.loc[13,'Actual vs. Forecast'] = dfdemand.loc[13,'M'] - dfdemand.loc[13,' M ']
        dfdemand.loc[3:13, 'Actual vs. Forecast'] = dfdemand.loc[3:13, 'Actual vs. Forecast'].astype('float').round(2)
        #Actual vs. Forecast %
        dfdemand.loc[3,'Actual vs. Forecast %'] = (dfdemand.loc[3,'M'] - dfdemand.loc[3,' M ']) /dfdemand.loc[3,'M']
        dfdemand.loc[4,'Actual vs. Forecast %'] = (dfdemand.loc[4,'M'] - dfdemand.loc[4,' M ']) /dfdemand.loc[4,'M']
        dfdemand.loc[5,'Actual vs. Forecast %'] = (dfdemand.loc[5,'M'] - dfdemand.loc[5,' M ']) /dfdemand.loc[5,'M']
        dfdemand.loc[6,'Actual vs. Forecast %'] = (dfdemand.loc[6,'M'] - dfdemand.loc[6,' M ']) /dfdemand.loc[6,'M']
        dfdemand.loc[7,'Actual vs. Forecast %'] = (dfdemand.loc[7,'M'] - dfdemand.loc[7,' M ']) /dfdemand.loc[7,'M']
        dfdemand.loc[8,'Actual vs. Forecast %'] = (dfdemand.loc[8,'M'] - dfdemand.loc[8,' M ']) /dfdemand.loc[8,'M']
        dfdemand.loc[9,'Actual vs. Forecast %'] = (dfdemand.loc[9,'M'] - dfdemand.loc[9,' M ']) /dfdemand.loc[9,'M']
        dfdemand.loc[10,'Actual vs. Forecast %'] = (dfdemand.loc[10,'M'] - dfdemand.loc[10,' M ']) /dfdemand.loc[10,'M']
        dfdemand.loc[11,'Actual vs. Forecast %'] = (dfdemand.loc[11,'M'] - dfdemand.loc[11,' M ']) /dfdemand.loc[11,'M']
        dfdemand.loc[12,'Actual vs. Forecast %'] = (dfdemand.loc[12,'M'] - dfdemand.loc[12,' M ']) /dfdemand.loc[12,'M']
        dfdemand.loc[13,'Actual vs. Forecast %'] = (dfdemand.loc[13,'M'] - dfdemand.loc[13,' M ']) /dfdemand.loc[13,'M']
        dfdemand.loc[3:13,'Actual vs. Forecast %'] = dfdemand.loc[3:13,'Actual vs. Forecast %'].map(lambda x:format(x,'.0%'))
        #Y-1 Kpler
        dfdemand.loc[3:,'Y-1'] = demand_basin.loc[date_dict['last_year'],'Global']
        dfdemand.loc[4:,'Y-1'] = demand_basin.loc[date_dict['last_year'],'PB']
        dfdemand.loc[5:,'Y-1'] = demand_basin.loc[date_dict['last_year'],'AB']
        dfdemand.loc[6:,'Y-1'] = demand_basin.loc[date_dict['last_year'],'MENA']
        dfdemand.loc[7,'Y-1'] = demand_basin.loc[date_dict['last_year'],'JKTC']
        dfdemand.loc[8,'Y-1'] = demand_basin.loc[date_dict['last_year'],'MEIP']
        dfdemand.loc[9,'Y-1'] = demand_basin.loc[date_dict['last_year'],'NWEur']
        dfdemand.loc[10,'Y-1'] = demand_basin.loc[date_dict['last_year'],'MedEur']
        dfdemand.loc[11,'Y-1'] = demand_basin.loc[date_dict['last_year'],'OtherEur']
        dfdemand.loc[12,'Y-1'] = demand_basin.loc[date_dict['last_year'],'LatAm']
        dfdemand.loc[13,'Y-1'] = demand_basin.loc[date_dict['last_year'],'OtherRoW']
        #D-1 Kpler
        dfdemand.loc[3:,'D-1'] = demand_basin.loc[date_dict['last_day'],'Global']
        dfdemand.loc[4:,'D-1'] = demand_basin.loc[date_dict['last_day'],'PB']
        dfdemand.loc[5:,'D-1'] = demand_basin.loc[date_dict['last_day'],'AB']
        dfdemand.loc[6:,'D-1'] = demand_basin.loc[date_dict['last_day'],'MENA']
        dfdemand.loc[7,'D-1'] = demand_basin.loc[date_dict['last_day'],'JKTC']
        dfdemand.loc[8,'D-1'] = demand_basin.loc[date_dict['last_day'],'MEIP']
        dfdemand.loc[9,'D-1'] = demand_basin.loc[date_dict['last_day'],'NWEur']
        dfdemand.loc[10,'D-1'] = demand_basin.loc[date_dict['last_day'],'MedEur']
        dfdemand.loc[11,'D-1'] = demand_basin.loc[date_dict['last_day'],'OtherEur']
        dfdemand.loc[12,'D-1'] = demand_basin.loc[date_dict['last_day'],'LatAm']
        dfdemand.loc[13,'D-1'] = demand_basin.loc[date_dict['last_day'],'OtherRoW']
        #D-7 Kpler
        dfdemand.loc[3:,'D-7'] = demand_basin.loc[date_dict['last_week'],'Global']
        dfdemand.loc[4:,'D-7'] = demand_basin.loc[date_dict['last_week'],'PB']
        dfdemand.loc[5:,'D-7'] = demand_basin.loc[date_dict['last_week'],'AB']
        dfdemand.loc[6:,'D-7'] = demand_basin.loc[date_dict['last_week'],'MENA']
        dfdemand.loc[7,'D-7'] = demand_basin.loc[date_dict['last_week'],'JKTC']
        dfdemand.loc[8,'D-7'] = demand_basin.loc[date_dict['last_week'],'MEIP']
        dfdemand.loc[9,'D-7'] = demand_basin.loc[date_dict['last_week'],'NWEur']
        dfdemand.loc[10,'D-7'] = demand_basin.loc[date_dict['last_week'],'MedEur']
        dfdemand.loc[11,'D-7'] = demand_basin.loc[date_dict['last_week'],'OtherEur']
        dfdemand.loc[12,'D-7'] = demand_basin.loc[date_dict['last_week'],'LatAm']
        dfdemand.loc[13,'D-7'] = demand_basin.loc[date_dict['last_week'],'OtherRoW']
        #DoD Kpler
        dfdemand.loc[3,'DoD'] = dfdemand.loc[3,'M'] - dfdemand.loc[3,'D-1']
        dfdemand.loc[4,'DoD'] = dfdemand.loc[4,'M'] - dfdemand.loc[4,'D-1']
        dfdemand.loc[5,'DoD'] = dfdemand.loc[5,'M'] - dfdemand.loc[5,'D-1']
        dfdemand.loc[6,'DoD'] = dfdemand.loc[6,'M'] - dfdemand.loc[6,'D-1']
        dfdemand.loc[7,'DoD'] = dfdemand.loc[7,'M'] - dfdemand.loc[7,'D-1']
        dfdemand.loc[8,'DoD'] = dfdemand.loc[8,'M'] - dfdemand.loc[8,'D-1']
        dfdemand.loc[9,'DoD'] = dfdemand.loc[9,'M'] - dfdemand.loc[9,'D-1']
        dfdemand.loc[10,'DoD'] = dfdemand.loc[10,'M'] - dfdemand.loc[10,'D-1']
        dfdemand.loc[11,'DoD'] = dfdemand.loc[11,'M'] - dfdemand.loc[11,'D-1']
        dfdemand.loc[12,'DoD'] = dfdemand.loc[12,'M'] - dfdemand.loc[12,'D-1']
        dfdemand.loc[13,'DoD'] = dfdemand.loc[13,'M'] - dfdemand.loc[13,'D-1']
        dfdemand.loc[3:13, 'DoD'] = dfdemand.loc[3:13, 'DoD'].astype('float').round(2)
        #WoW Kpler
        dfdemand.loc[3,'WoW'] = dfdemand.loc[3,'M'] - dfdemand.loc[3,'D-7']
        dfdemand.loc[4,'WoW'] = dfdemand.loc[4,'M'] - dfdemand.loc[4,'D-7']
        dfdemand.loc[5,'WoW'] = dfdemand.loc[5,'M'] - dfdemand.loc[5,'D-7']
        dfdemand.loc[6,'WoW'] = dfdemand.loc[6,'M'] - dfdemand.loc[6,'D-7']
        dfdemand.loc[7,'WoW'] = dfdemand.loc[7,'M'] - dfdemand.loc[7,'D-7']
        dfdemand.loc[8,'WoW'] = dfdemand.loc[8,'M'] - dfdemand.loc[8,'D-7']
        dfdemand.loc[9,'WoW'] = dfdemand.loc[9,'M'] - dfdemand.loc[9,'D-7']
        dfdemand.loc[10,'WoW'] = dfdemand.loc[10,'M'] - dfdemand.loc[10,'D-7']
        dfdemand.loc[11,'WoW'] = dfdemand.loc[11,'M'] - dfdemand.loc[11,'D-7']
        dfdemand.loc[12,'WoW'] = dfdemand.loc[12,'M'] - dfdemand.loc[12,'D-7']
        dfdemand.loc[13,'WoW'] = dfdemand.loc[13,'M'] - dfdemand.loc[13,'D-7']
        dfdemand.loc[3:13, 'WoW'] = dfdemand.loc[3:13, 'WoW'].astype('float').round(2)
        #dfdemand = dfdemand.round(2)
        #print(dfdemand)
        
        #print(DemandCategories)
            
        df_demand_country = pd.DataFrame(columns=['Suez','Basin','Region','Market','M-1 Kpler','M-1 Desk','M Kpler','M Desk','M+1 Desk','M+2 Desk','YoY Kpler','MoM Kpler','Actual vs. Forecast','Actual vs. Forecast %', 'Y-1 Kpler','D-1 Kpler','D-7 Kpler','DoD Kpler','WoW Kpler'])
        df_demand_country[['Suez','Basin','Region','Market']] = DemandCategories[['Suez','Basin','Region','Country']]
        
        df_demand_country.set_index('Market', inplace=True)
        
        #print(df_demand_country)
        
        for i in df_demand_country.index:
            #print(i)
            df_demand_country.loc[i,'M-1 Kpler'] = demand_basin.loc[date_dict['last_month'],i]
            df_demand_country.loc[i,'M Kpler'] = demand_basin.loc[date_dict['today'],i]
            df_demand_country.loc[i,'M Desk'] = demand_basin.loc[date_dict['today'],i+' Desk']
            df_demand_country.loc[i,'M+1 Desk'] = demand_basin.loc[date_dict['next_1month'],i+' Desk']
            df_demand_country.loc[i,'M+2 Desk'] = demand_basin.loc[date_dict['next_2month'],i+' Desk']
            df_demand_country.loc[i,'M-1 Desk'] = demand_basin.loc[date_dict['last_month'],i+' Desk']
            df_demand_country.loc[i,'YoY Kpler'] = (demand_basin.loc[date_dict['today'],i] - demand_basin.loc[date_dict['last_year'],i]).round(2)
            df_demand_country.loc[i,'MoM Kpler'] = (demand_basin.loc[date_dict['today'],i] - demand_basin.loc[date_dict['last_month'],i]).round(2)
            df_demand_country.loc[i,'Actual vs. Forecast'] = (df_demand_country.loc[i,'M Kpler'] - df_demand_country.loc[i,'M Desk']).round(2)
            df_demand_country.loc[i,'Actual vs. Forecast %'] = (df_demand_country.loc[i,'M Kpler'] - df_demand_country.loc[i,'M Desk']) /df_demand_country.loc[i,'M Kpler']
            
            df_demand_country.loc[i,'Y-1 Kpler'] = demand_basin.loc[date_dict['last_year'],i]
            df_demand_country.loc[i,'D-1 Kpler'] = demand_basin.loc[date_dict['last_day'],i]
            df_demand_country.loc[i,'D-7 Kpler'] = demand_basin.loc[date_dict['last_week'],i]
            df_demand_country.loc[i,'DoD Kpler'] = (df_demand_country.loc[i,'M Kpler'] - df_demand_country.loc[i,'D-1 Kpler']).round(2)
            df_demand_country.loc[i,'WoW Kpler'] = (df_demand_country.loc[i,'M Kpler'] - df_demand_country.loc[i,'D-7 Kpler']).round(2)
        df_demand_country.loc[:,'Actual vs. Forecast %'] = df_demand_country.loc[:,'Actual vs. Forecast %'].map(lambda x:format(x,'.0%'))
        df_demand_country.sort_values(by='Region', inplace=True)
        df_demand_country.reset_index(inplace=True)
        df_demand_country = df_demand_country[['Suez','Basin','Region','Market','M-1 Kpler','M-1 Desk','M Kpler','M Desk','M+1 Desk','M+2 Desk','YoY Kpler','MoM Kpler','Actual vs. Forecast','Actual vs. Forecast %', 'Y-1 Kpler','D-1 Kpler','D-7 Kpler','DoD Kpler','WoW Kpler']]
        #print(df_demand_country)
        
        return dfdemand, df_demand_country

    def discrete_background_color_bins(df, n_bins=10, columns='all'):
        import colorlover
        bounds = [i * (1.0 / n_bins) for i in range(n_bins + 1)]
        if columns == 'all':
            if 'id' in df:
                df_numeric_columns = df.select_dtypes('number').drop(['id'], axis=1)
            else:
                df_numeric_columns = df.select_dtypes('number')
        else:
            df_numeric_columns = df[columns]
        df_max = df_numeric_columns.max().max()
        df_min = df_numeric_columns.min().min()
        ranges = [
            ((df_max - df_min) * i) + df_min
            for i in bounds
        ]
        styles = []
        legend = []
        for i in range(1, len(bounds)):
            min_bound = ranges[i - 1]
            max_bound = ranges[i]
            backgroundColor = colorlover.scales[str(n_bins)]['div']['RdYlGn'][i - 1]  # 'seq' or 'div' color in https://plotly.com/python/builtin-colorscales/
            color = 'white' if i > len(bounds) / 2. else 'inherit'
    
            for column in df_numeric_columns:
                styles.append({
                    'if': {
                        'filter_query': (
                            '{{{column}}} >= {min_bound}' +
                            (' && {{{column}}} < {max_bound}' if (i < len(bounds) - 1) else '')
                        ).format(column=column, min_bound=min_bound, max_bound=max_bound),
                        'column_id': column
                    },
                    'backgroundColor': backgroundColor,
                    'color': color
                })
            legend.append(
                html.Div(style={'display': 'inline-block', 'width': '60px'}, children=[
                    html.Div(
                        style={
                            'backgroundColor': backgroundColor,
                            'borderLeft': '1px rgb(50, 50, 50) solid',
                            'height': '10px'
                        }
                    ),
                    html.Small(round(min_bound, 2), style={'paddingLeft': '2px'})
                ])
            )
    
        return (styles, html.Div(legend, style={'padding': '5px 0 5px 0'}))

    def get_new_data():

        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        #get data
        dfsupplyMA, dfdemandMA, dfsupplyMA_eta, dfdemandMA_eta, desk_supply_view, desk_demand_view, ihscontract, SupplyCategories, DemandCategories,supply_basin, supply_cumulative, demand_basin, demand_cumulative = SD_Table_30.get_sd_data()
        dfsupply, df_supply_plant = SD_Table_30.gpam_supply_table(supply_basin, dfsupplyMA, desk_supply_view, SupplyCategories)
        dfdemand, df_demand_country = SD_Table_30.gpam_demand_table(demand_basin, dfdemandMA, desk_demand_view, DemandCategories) #this
        #supply data and style
        dfdatasupply=pd.DataFrame(columns=['M-1',' M-1 ','M', ' M ', 'M+1', 'M+2', 'YoY', 'MoM', 'Actual vs. Forecast', '2020', '2021', ' 2021 ', 'DoD', 'WoW'])
        dfdatasupply=dfsupply.loc[3:13]
        dfdatasupplyplant=df_supply_plant
        headersupply=[list(dfsupply.columns[0:]), list(dfsupply.iloc[0,]),list(dfsupply.loc[1]),list(dfsupply.loc[2])]
        headersupply=list(map(list, zip(*headersupply)))
        hdlistsuply = []
        print('get new data time', now)
        
        for i in headersupply:
            hd={"name":i,"id": i[0],'deletable' : True }
            hdlistsuply.append(hd)
        
        hdlistsupply2=[{"name": i, "id": i,'deletable' : True } for i in df_supply_plant.columns]
        
        dfcolorsupply1=pd.DataFrame(dfsupply[['Actual vs. Forecast','DoD','WoW']].loc[3:6].astype('float').values, columns=['Actual vs. Forecast','DoD','WoW'])
        dfcolorsupply2=pd.DataFrame(dfsupply[['Actual vs. Forecast','DoD','WoW']].loc[8:14].astype('float').values, columns=['Actual vs. Forecast','DoD','WoW'])
        dfcolorsupply3=pd.DataFrame(df_supply_plant[['Actual vs. Forecast','DoD Kpler', 'WoW Kpler']].astype('float').values, columns=['Actual vs. Forecast','DoD Kpler', 'WoW Kpler'])
        
        (styles1s, legend) = SD_Table_30.discrete_background_color_bins(dfcolorsupply1, columns=['Actual vs. Forecast'])
        (styles2s, legend) = SD_Table_30.discrete_background_color_bins(dfcolorsupply1, columns=['DoD'])
        (styles3s, legend) = SD_Table_30.discrete_background_color_bins(dfcolorsupply1, columns=['WoW'])
        (styles4s, legend) = SD_Table_30.discrete_background_color_bins(dfcolorsupply2, columns=['Actual vs. Forecast'])
        (styles5s, legend) = SD_Table_30.discrete_background_color_bins(dfcolorsupply2, columns=['DoD'])
        (styles6s, legend) = SD_Table_30.discrete_background_color_bins(dfcolorsupply2, columns=['WoW'])
        (styles7s, legend) = SD_Table_30.discrete_background_color_bins(dfcolorsupply3, columns=['Actual vs. Forecast'])
        (styles8s, legend) = SD_Table_30.discrete_background_color_bins(dfcolorsupply3, columns=['DoD Kpler'])
        (styles9s, legend) = SD_Table_30.discrete_background_color_bins(dfcolorsupply3, columns=['WoW Kpler'])
        stylessupplylow = styles7s+ styles8s+styles9s
        stylessupplyup = styles1s+ styles2s+styles3s+styles4s+styles5s+styles6s
        #demand data and style
        dfdatademand=pd.DataFrame(columns=['M-1',' M-1 ', 'M', ' M ', 'M+1', 'M+2', 'YoY', 'MoM', 'Actual vs. Forecast', '2020', '2021', ' 2021 ', 'DoD', 'WoW'])
        dfdatademand=dfdemand.loc[3:13]
        
        dfdatademandcountry=df_demand_country
        headerdemand=[list(dfdemand.columns[0:]), list(dfdemand.iloc[0,]),list(dfdemand.loc[1]),list(dfdemand.loc[2])]
        headerdemand=list(map(list, zip(*headerdemand)))
        hdlistdemand = []
        
        for i in headerdemand:
            hd={"name":i,"id": i[0],'deletable' : True }
            
            hdlistdemand.append(hd)
        
        hdlistdemand2=[{"name": i, "id": i, 'deletable' : True } for i in df_demand_country.columns]
        dfcolordemand1=pd.DataFrame(dfdemand[['Actual vs. Forecast','DoD','WoW']].loc[3:6].astype('float').values, columns=['Actual vs. Forecast','DoD','WoW'])
        dfcolordemand2=pd.DataFrame(dfdemand[['Actual vs. Forecast','DoD','WoW']].loc[8:14].astype('float').values, columns=['Actual vs. Forecast','DoD','WoW'])
        dfcolordemand3=pd.DataFrame(df_demand_country[['Actual vs. Forecast','DoD Kpler', 'WoW Kpler']].astype('float').values, columns=['Actual vs. Forecast','DoD Kpler', 'WoW Kpler'])
        
        (styles1d, legend) = SD_Table_30.discrete_background_color_bins(dfcolordemand1, columns=['Actual vs. Forecast'])
        (styles2d, legend) = SD_Table_30.discrete_background_color_bins(dfcolordemand1, columns=['DoD'])
        (styles3d, legend) = SD_Table_30.discrete_background_color_bins(dfcolordemand1, columns=['WoW'])
        (styles4d, legend) = SD_Table_30.discrete_background_color_bins(dfcolordemand2, columns=['Actual vs. Forecast'])
        (styles5d, legend) = SD_Table_30.discrete_background_color_bins(dfcolordemand2, columns=['DoD'])
        (styles6d, legend) = SD_Table_30.discrete_background_color_bins(dfcolordemand2, columns=['WoW'])
        (styles7d, legend) = SD_Table_30.discrete_background_color_bins(dfcolordemand3, columns=['Actual vs. Forecast'])
        (styles8d, legend) = SD_Table_30.discrete_background_color_bins(dfcolordemand3, columns=['DoD Kpler'])
        (styles9d, legend) = SD_Table_30.discrete_background_color_bins(dfcolordemand3, columns=['WoW Kpler'])
        stylesdemandlow = styles7d+ styles8d+styles9d
        stylesdemandup = styles1d+ styles2d+styles3d+styles4d+ styles5d+styles6d
        
        return dfsupply, df_supply_plant,hdlistsuply,dfdatasupply,stylessupplyup,hdlistsupply2,dfdatasupplyplant,stylessupplylow, hdlistdemand, dfdatademand,stylesdemandup, hdlistdemand2, dfdatademandcountry,stylesdemandlow

app = dash.Dash(__name__)

app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-content')
])


index_page = html.Div([
    dcc.Link('supply', href='/supply'),
    html.Br(),
    dcc.Link('demand', href='/demand'),
    html.Br(),
    dcc.Link('supply30', href='/supply30'),
    html.Br(),
    dcc.Link('demand30', href='/demand30'),
])


# Update the index
@app.callback(dash.dependencies.Output('page-content', 'children'),
              [dash.dependencies.Input('url', 'pathname')])
def display_page(pathname):
    if pathname == '/supply':
        
        supply_layout = html.Div([
                
        html.Button('Download Supply MA10 Table (need about 10s)', id='refrash'),
        dcc.Loading(id="loading_supply", 
                        children=[
        html.Div(id='body-div-supply') ])       ])
        
        return supply_layout
    
    if pathname == '/demand':
        
        demand_layout = html.Div([
        
        #html.Div([html.I("End of Day PnL")]), #page header
        #html.Hr(),
        
            html.Button('Download Demand MA10 Table (need about 60s)', id='refrash'),
            dcc.Loading(id="loading-demand", 
                            children=[
            html.Div(id='body-div-demand'), ])       
        ])
        
        return demand_layout
    
    if pathname == '/supply30':
        
        supply_layout30 = html.Div([
        
        #html.Div([html.I("End of Day PnL")]), #page header
        #html.Hr(),
        
            html.Button('Download Supply MA30 Table (need about 60s)', id='refrash30'),
            dcc.Loading(id="loading-supply30", 
                            children=[
            html.Div(id='body-div-supply30'), ])       
        ])
        
        return supply_layout30
    
    if pathname == '/demand30':
        
        demand_layout30 = html.Div([
        
        #html.Div([html.I("End of Day PnL")]), #page header
        #html.Hr(),
        
            html.Button('Download Demand MA30 Table (need about 60s)', id='refrash30'),
            dcc.Loading(id="loading-demand30", 
                            children=[
            html.Div(id='body-div-demand30'), ])       
        ])
        
        return demand_layout30
    
    else:
        return index_page

#update MA10 layout
@app.callback(
    Output(component_id='body-div-supply', component_property='children'),
    #Output(component_id='body-div-demand', component_property='children'),
    Input(component_id='refrash', component_property='n_clicks'),
    #Input(component_id='refrash_supply', component_property='n_clicks')
)
def update_output(n_clicks):
    
    if n_clicks is None:
        
        raise PreventUpdate
    
    else:
        print(1)
        dfsupply, df_supply_plant,hdlistsuply,dfdatasupply,stylessupplyup,hdlistsupply2,dfdatasupplyplant,stylessupplylow, hdlistdemand, dfdatademand,stylesdemandup, hdlistdemand2, dfdatademandcountry,stylesdemandlow = SD_Table_10.get_new_data()
        
        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print("data updated at: ", now)
        supply_table_layout = html.Div([
            html.H1('Data updated at: '+ datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')),
            html.Hr(),
            dash_table.DataTable(
                
                id='table1',
                columns=hdlistsuply,
                #data=dfsupply.loc[3:].to_dict('records'),
                data=dfdatasupply.to_dict('records'),
                editable=True,
                sort_action='native',
                filter_action="native",
                style_filter = {'height':'25px'},
                #column_deletable=True,
                row_deletable=True,
        
                style_cell={
                    'whiteSpace': 'normal',
                    'height': 'auto',
                    #'border': '3px solid grey'
                },
                
                style_cell_conditional=[
                    {
                        'if': {'column_id': 'M'},
                        'fontWeight': 'bold'
                    },
                    {
                        'if': {'column_id': ' M '},
                        'fontWeight': 'bold'
                    },
                    {
                        'if': {'column_id': 'Year'},
                        'fontWeight': 'bold'
                    },
                    {
                        'if': {'column_id': 'Year'},
                        'textAlign': 'left'
                    },
                    {
                        'if': {'column_id': 'Supply'},
                        'textAlign': 'left'
                    },
                   {
                        'if': {
                            'row_index': 4,
                            },
                        'backgroundColor': '#DDEBF7'
                    },
                    {
                        'if': {
                            'row_index': 5,
                            },
                        'backgroundColor': '#DDEBF7'
                    },
                    {
                        'if': {
                            'row_index': 6,
                            },
                        'backgroundColor': '#DDEBF7'
                    },
                    {
                        'if': {
                            'row_index': 7,
                            },
                        'backgroundColor': '#DDEBF7'
                    },
                    {
                        'if': {
                            'row_index': 8,
                            },
                        'backgroundColor': '#DDEBF7'
                    },
                    {
                        'if': {
                            'row_index': 9,
                            },
                        'backgroundColor': '#DDEBF7'
                    },
                    {
                        'if': {
                            'row_index': 10,
                            },
                        'backgroundColor': '#DDEBF7'
                    },
                    
                ],
                style_header={
                    'backgroundColor': '#D9D9D9',
                    #'fontWeight': 'bold',
                    'textAlign': 'center',
                    #'border': '3px solid black'
                    #'color': 'white'
                },
                style_header_conditional=[
                    {
                        'if': {'column_id': 'Year'},
                        'textAlign': 'left'
                    },
                    {
                        'if': {'column_id': 'Supply'},
                        'textAlign': 'left'
                    },
                    {
                        'if': {
                            'header_index': 1,
                            'column_id': 'M'},
                        'backgroundColor': '#FFFF00'
                    },
                    {
                        'if': {
                            'header_index': 1,
                            'column_id': ' M '},
                        'backgroundColor': '#FFFF00'
                    },
                    {
                        'if': {'column_id': 'M'},
                        'fontWeight': 'bold'
                    },
                    {
                        'if': {'column_id': ' M '},
                        'fontWeight': 'bold'
                    },
                    
                ],
                
                style_data_conditional=stylessupplyup,
                style_as_list_view=True,
            ),
            html.Hr(),
            html.Br(),
            html.Hr(),
            dash_table.DataTable(
            
                id='table2',
                columns=hdlistsupply2,
                #data=dfsupply.loc[3:].to_dict('records'),
                data=dfdatasupplyplant.to_dict('records'),
                editable=True,
                sort_action='native',
                filter_action="native",
                style_filter = {'height':'25px'},
                #column_deletable=True,
                row_deletable=True,
                style_cell={
                    'whiteSpace': 'normal',
                    'height': 'auto',
                },
                
                style_cell_conditional=[
                    {
                        'if': {'column_id': 'M Kpler'},
                        'fontWeight': 'bold'
                    },
                    {
                        'if': {'column_id': 'M Desk'},
                        'fontWeight': 'bold'
                    },
                    {
                        'if': {'column_id': 'Suez'},
                        'textAlign': 'left'
                    },
                    {
                        'if': {'column_id': 'Basin'},
                        'textAlign': 'left'
                    },
                    {
                        'if': {'column_id': 'Market'},
                        'textAlign': 'left'
                    },
                    {
                        'if': {'column_id': 'Plant'},
                        'textAlign': 'left'
                    },
                    
                ],
                style_header={
                    'backgroundColor': '#D9D9D9',
                    'fontWeight': 'bold',
                    'textAlign': 'right',
                    #'color': 'white'
                },
                style_header_conditional=[
                    {
                        'if': {'column_id': 'Suez'},
                        'textAlign': 'left'
                    },
                    {
                        'if': {'column_id': 'Basin'},
                        'textAlign': 'left'
                    },
                    {
                        'if': {'column_id': 'Market'},
                        'textAlign': 'left'
                    },
                    {
                        'if': {'column_id': 'Plant'},
                        'textAlign': 'left'
                    },
                    {
                        'if': {
                            'header_index': 0,
                            'column_id': 'M Kpler'},
                        'backgroundColor': '#FFFF00'
                    },
                    {
                        'if': {
                            'header_index': 0,
                            'column_id': 'M Desk'},
                        'backgroundColor': '#FFFF00'
                    },
                    
                ],
                
                style_data_conditional=stylessupplylow,
                style_as_list_view=True,
            ),
            html.Hr(),
            
            ])
    
    
        demand_table_layout = html.Div([
        html.H1('Data updated at: '+now),
        html.Hr(),
        dash_table.DataTable(
        
            id='table3',
            columns=hdlistdemand,
            #data=dfsupply.loc[3:].to_dict('records'),
            data=dfdatademand.to_dict('records'),
            editable=True,
            sort_action='native',
            filter_action="native",
            style_filter = {'height':'25px'},
            #column_deletable=True,
            row_deletable=True,
    
            style_cell={
                'whiteSpace': 'normal',
                'height': 'auto',
                #'border': '3px solid grey'
            },
            
            style_cell_conditional=[
                {
                    'if': {'column_id': 'M'},
                    'fontWeight': 'bold'
                },
                {
                    'if': {'column_id': ' M '},
                    'fontWeight': 'bold'
                },
                {
                    'if': {'column_id': 'Year'},
                    'fontWeight': 'bold'
                },
                {
                    'if': {'column_id': 'Year'},
                    'textAlign': 'left'
                },
                {
                    'if': {'column_id': 'Demand'},
                    'textAlign': 'left'
                },
                {
                    'if': {
                        'row_index': 4,
                        },
                    'backgroundColor': '#DDEBF7'
                },
                {
                    'if': {
                        'row_index': 5,
                        },
                    'backgroundColor': '#DDEBF7'
                },
                {
                    'if': {
                        'row_index': 6,
                        },
                    'backgroundColor': '#DDEBF7'
                },
                {
                    'if': {
                        'row_index': 7,
                        },
                    'backgroundColor': '#DDEBF7'
                },
                {
                    'if': {
                        'row_index': 8,
                        },
                    'backgroundColor': '#DDEBF7'
                },
                {
                    'if': {
                        'row_index': 9,
                        },
                    'backgroundColor': '#DDEBF7'
                },
                {
                    'if': {
                        'row_index': 10,
                        },
                    'backgroundColor': '#DDEBF7'
                },
                
            ],
            style_header={
                'backgroundColor': '#D9D9D9',
                #'fontWeight': 'bold',
                'textAlign': 'center',
                #'border': '3px solid black'
                #'color': 'white'
            },
            style_header_conditional=[
                {
                    'if': {'column_id': 'Year'},
                    'textAlign': 'left'
                },
                {
                    'if': {'column_id':'Demand'},
                    'textAlign': 'left'
                },
                {
                    'if': {
                        'header_index': 1,
                        'column_id': 'M'},
                    'backgroundColor': '#FFFF00'
                },
                {
                    'if': {
                        'header_index': 1,
                        'column_id': ' M '},
                    'backgroundColor': '#FFFF00'
                },
                {
                    'if': {'column_id': 'M'},
                    'fontWeight': 'bold'
                },
                {
                    'if': {'column_id': ' M '},
                    'fontWeight': 'bold'
                },
                
                
            ],
            
            style_data_conditional=stylesdemandup,
            style_as_list_view=True,
        ),
        html.Hr(),
        html.Br(),
        html.Hr(),
        dash_table.DataTable(
        
            id='table4',
            columns=hdlistdemand2,
            #data=dfsupply.loc[3:].to_dict('records'),
            data=dfdatademandcountry.to_dict('records'),
            #editable=True,
            sort_action='native',
            filter_action="native",
            style_filter = {'height':'25px'},
            #column_deletable=True,
            row_deletable=True,
            style_cell={
                'whiteSpace': 'normal',
                'height': 'auto',
            },
            
            style_cell_conditional=[
                {
                    'if': {'column_id': 'M Kpler'},
                    'fontWeight': 'bold'
                },
                {
                    'if': {'column_id': 'M Desk'},
                    'fontWeight': 'bold'
                },
                {
                    'if': {'column_id': 'Suez'},
                    'textAlign': 'left'
                },
                {
                    'if': {'column_id': 'Basin'},
                    'textAlign': 'left'
                },
                {
                    'if': {'column_id': 'Region'},
                    'textAlign': 'left'
                },
                {
                    'if': {'column_id': 'Market'},
                    'textAlign': 'left'
                },
                
            ],
            style_header={
                'backgroundColor': '#D9D9D9',
                'fontWeight': 'bold',
                'textAlign': 'right',
                #'color': 'white'
            },
            style_header_conditional=[
                {
                    'if': {'column_id': 'Suez'},
                    'textAlign': 'left'
                },
                {
                    'if': {'column_id': 'Basin'},
                    'textAlign': 'left'
                },
                {
                    'if': {'column_id': 'Region'},
                    'textAlign': 'left'
                },
                {
                    'if': {'column_id': 'Market'},
                    'textAlign': 'left'
                },
                {
                    'if': {
                        'header_index': 0,
                        'column_id': 'M Kpler'},
                    'backgroundColor': '#FFFF00'
                },
                {
                    'if': {
                        'header_index': 0,
                        'column_id': 'M Desk'},
                    'backgroundColor': '#FFFF00'
                },
                
            ],
            
            style_data_conditional=stylesdemandlow,
            style_as_list_view=True,
        ),
        
        ])    
        
        return supply_table_layout,# demand_table_layout

@app.callback(
    #Output(component_id='body-div-supply', component_property='children'),
    Output(component_id='body-div-demand', component_property='children'),
    Input(component_id='refrash', component_property='n_clicks'),
    #Input(component_id='refrash_supply', component_property='n_clicks')
)
def update_output(n_clicks1):
    
    if n_clicks1 is None:
        
        raise PreventUpdate
    
    else:
        print(1)
        dfsupply, df_supply_plant,hdlistsuply,dfdatasupply,stylessupplyup,hdlistsupply2,dfdatasupplyplant,stylessupplylow, hdlistdemand, dfdatademand,stylesdemandup, hdlistdemand2, dfdatademandcountry,stylesdemandlow = SD_Table_10.get_new_data()
        
        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print("data updated at: ", now)
        supply_table_layout = html.Div([
            html.H1('Data updated at: '+ datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')),
            html.Hr(),
            dash_table.DataTable(
                
                id='table1',
                columns=hdlistsuply,
                #data=dfsupply.loc[3:].to_dict('records'),
                data=dfdatasupply.to_dict('records'),
                editable=True,
                sort_action='native',
                filter_action="native",
                style_filter = {'height':'25px'},
                #column_deletable=True,
                row_deletable=True,
        
                style_cell={
                    'whiteSpace': 'normal',
                    'height': 'auto',
                    #'border': '3px solid grey'
                },
                
                style_cell_conditional=[
                    {
                        'if': {'column_id': 'M'},
                        'fontWeight': 'bold'
                    },
                    {
                        'if': {'column_id': ' M '},
                        'fontWeight': 'bold'
                    },
                    {
                        'if': {'column_id': 'Year'},
                        'fontWeight': 'bold'
                    },
                    {
                        'if': {'column_id': 'Year'},
                        'textAlign': 'left'
                    },
                    {
                        'if': {'column_id': 'Supply'},
                        'textAlign': 'left'
                    },
                   {
                        'if': {
                            'row_index': 4,
                            },
                        'backgroundColor': '#DDEBF7'
                    },
                    {
                        'if': {
                            'row_index': 5,
                            },
                        'backgroundColor': '#DDEBF7'
                    },
                    {
                        'if': {
                            'row_index': 6,
                            },
                        'backgroundColor': '#DDEBF7'
                    },
                    {
                        'if': {
                            'row_index': 7,
                            },
                        'backgroundColor': '#DDEBF7'
                    },
                    {
                        'if': {
                            'row_index': 8,
                            },
                        'backgroundColor': '#DDEBF7'
                    },
                    {
                        'if': {
                            'row_index': 9,
                            },
                        'backgroundColor': '#DDEBF7'
                    },
                    {
                        'if': {
                            'row_index': 10,
                            },
                        'backgroundColor': '#DDEBF7'
                    },
                    
                ],
                style_header={
                    'backgroundColor': '#D9D9D9',
                    #'fontWeight': 'bold',
                    'textAlign': 'center',
                    #'border': '3px solid black'
                    #'color': 'white'
                },
                style_header_conditional=[
                    {
                        'if': {'column_id': 'Year'},
                        'textAlign': 'left'
                    },
                    {
                        'if': {'column_id': 'Supply'},
                        'textAlign': 'left'
                    },
                    {
                        'if': {
                            'header_index': 1,
                            'column_id': 'M'},
                        'backgroundColor': '#FFFF00'
                    },
                    {
                        'if': {
                            'header_index': 1,
                            'column_id': ' M '},
                        'backgroundColor': '#FFFF00'
                    },
                    {
                        'if': {'column_id': 'M'},
                        'fontWeight': 'bold'
                    },
                    {
                        'if': {'column_id': ' M '},
                        'fontWeight': 'bold'
                    },
                    
                ],
                
                style_data_conditional=stylessupplyup,
                style_as_list_view=True,
            ),
            html.Hr(),
            html.Br(),
            html.Hr(),
            dash_table.DataTable(
            
                id='table2',
                columns=hdlistsupply2,
                #data=dfsupply.loc[3:].to_dict('records'),
                data=dfdatasupplyplant.to_dict('records'),
                editable=True,
                sort_action='native',
                filter_action="native",
                style_filter = {'height':'25px'},
                #column_deletable=True,
                row_deletable=True,
                style_cell={
                    'whiteSpace': 'normal',
                    'height': 'auto',
                },
                
                style_cell_conditional=[
                    {
                        'if': {'column_id': 'M Kpler'},
                        'fontWeight': 'bold'
                    },
                    {
                        'if': {'column_id': 'M Desk'},
                        'fontWeight': 'bold'
                    },
                    {
                        'if': {'column_id': 'Suez'},
                        'textAlign': 'left'
                    },
                    {
                        'if': {'column_id': 'Basin'},
                        'textAlign': 'left'
                    },
                    {
                        'if': {'column_id': 'Market'},
                        'textAlign': 'left'
                    },
                    {
                        'if': {'column_id': 'Plant'},
                        'textAlign': 'left'
                    },
                    
                ],
                style_header={
                    'backgroundColor': '#D9D9D9',
                    'fontWeight': 'bold',
                    'textAlign': 'right',
                    #'color': 'white'
                },
                style_header_conditional=[
                    {
                        'if': {'column_id': 'Suez'},
                        'textAlign': 'left'
                    },
                    {
                        'if': {'column_id': 'Basin'},
                        'textAlign': 'left'
                    },
                    {
                        'if': {'column_id': 'Market'},
                        'textAlign': 'left'
                    },
                    {
                        'if': {'column_id': 'Plant'},
                        'textAlign': 'left'
                    },
                    {
                        'if': {
                            'header_index': 0,
                            'column_id': 'M Kpler'},
                        'backgroundColor': '#FFFF00'
                    },
                    {
                        'if': {
                            'header_index': 0,
                            'column_id': 'M Desk'},
                        'backgroundColor': '#FFFF00'
                    },
                    
                ],
                
                style_data_conditional=stylessupplylow,
                style_as_list_view=True,
            ),
            html.Hr(),
            
            ])
    
    
        demand_table_layout = html.Div([
        html.H1('Data updated at: '+now),
        html.Hr(),
        dash_table.DataTable(
        
            id='table3',
            columns=hdlistdemand,
            #data=dfsupply.loc[3:].to_dict('records'),
            data=dfdatademand.to_dict('records'),
            editable=True,
            sort_action='native',
            filter_action="native",
            style_filter = {'height':'25px'},
            #column_deletable=True,
            row_deletable=True,
    
            style_cell={
                'whiteSpace': 'normal',
                'height': 'auto',
                #'border': '3px solid grey'
            },
            
            style_cell_conditional=[
                {
                    'if': {'column_id': 'M'},
                    'fontWeight': 'bold'
                },
                {
                    'if': {'column_id': ' M '},
                    'fontWeight': 'bold'
                },
                {
                    'if': {'column_id': 'Year'},
                    'fontWeight': 'bold'
                },
                {
                    'if': {'column_id': 'Year'},
                    'textAlign': 'left'
                },
                {
                    'if': {'column_id': 'Demand'},
                    'textAlign': 'left'
                },
                {
                    'if': {
                        'row_index': 4,
                        },
                    'backgroundColor': '#DDEBF7'
                },
                {
                    'if': {
                        'row_index': 5,
                        },
                    'backgroundColor': '#DDEBF7'
                },
                {
                    'if': {
                        'row_index': 6,
                        },
                    'backgroundColor': '#DDEBF7'
                },
                {
                    'if': {
                        'row_index': 7,
                        },
                    'backgroundColor': '#DDEBF7'
                },
                {
                    'if': {
                        'row_index': 8,
                        },
                    'backgroundColor': '#DDEBF7'
                },
                {
                    'if': {
                        'row_index': 9,
                        },
                    'backgroundColor': '#DDEBF7'
                },
                {
                    'if': {
                        'row_index': 10,
                        },
                    'backgroundColor': '#DDEBF7'
                },
                
            ],
            style_header={
                'backgroundColor': '#D9D9D9',
                #'fontWeight': 'bold',
                'textAlign': 'center',
                #'border': '3px solid black'
                #'color': 'white'
            },
            style_header_conditional=[
                {
                    'if': {'column_id': 'Year'},
                    'textAlign': 'left'
                },
                {
                    'if': {'column_id':'Demand'},
                    'textAlign': 'left'
                },
                {
                    'if': {
                        'header_index': 1,
                        'column_id': 'M'},
                    'backgroundColor': '#FFFF00'
                },
                {
                    'if': {
                        'header_index': 1,
                        'column_id': ' M '},
                    'backgroundColor': '#FFFF00'
                },
                {
                    'if': {'column_id': 'M'},
                    'fontWeight': 'bold'
                },
                {
                    'if': {'column_id': ' M '},
                    'fontWeight': 'bold'
                },
                
                
            ],
            
            style_data_conditional=stylesdemandup,
            style_as_list_view=True,
        ),
        html.Hr(),
        html.Br(),
        html.Hr(),
        dash_table.DataTable(
        
            id='table4',
            columns=hdlistdemand2,
            #data=dfsupply.loc[3:].to_dict('records'),
            data=dfdatademandcountry.to_dict('records'),
            #editable=True,
            sort_action='native',
            filter_action="native",
            style_filter = {'height':'25px'},
            #column_deletable=True,
            row_deletable=True,
            style_cell={
                'whiteSpace': 'normal',
                'height': 'auto',
            },
            
            style_cell_conditional=[
                {
                    'if': {'column_id': 'M Kpler'},
                    'fontWeight': 'bold'
                },
                {
                    'if': {'column_id': 'M Desk'},
                    'fontWeight': 'bold'
                },
                {
                    'if': {'column_id': 'Suez'},
                    'textAlign': 'left'
                },
                {
                    'if': {'column_id': 'Basin'},
                    'textAlign': 'left'
                },
                {
                    'if': {'column_id': 'Region'},
                    'textAlign': 'left'
                },
                {
                    'if': {'column_id': 'Market'},
                    'textAlign': 'left'
                },
                
            ],
            style_header={
                'backgroundColor': '#D9D9D9',
                'fontWeight': 'bold',
                'textAlign': 'right',
                #'color': 'white'
            },
            style_header_conditional=[
                {
                    'if': {'column_id': 'Suez'},
                    'textAlign': 'left'
                },
                {
                    'if': {'column_id': 'Basin'},
                    'textAlign': 'left'
                },
                {
                    'if': {'column_id': 'Region'},
                    'textAlign': 'left'
                },
                {
                    'if': {'column_id': 'Market'},
                    'textAlign': 'left'
                },
                {
                    'if': {
                        'header_index': 0,
                        'column_id': 'M Kpler'},
                    'backgroundColor': '#FFFF00'
                },
                {
                    'if': {
                        'header_index': 0,
                        'column_id': 'M Desk'},
                    'backgroundColor': '#FFFF00'
                },
                
            ],
            
            style_data_conditional=stylesdemandlow,
            style_as_list_view=True,
        ),
        
        ])    
        
        return  demand_table_layout


#update MA30 layout
@app.callback(
    Output(component_id='body-div-supply30', component_property='children'),
    #Output(component_id='body-div-demand30', component_property='children'),
    Input(component_id='refrash30', component_property='n_clicks'),
    #Input(component_id='refrash_supply', component_property='n_clicks')
)
def update_output30(n_clicks3):
    
    if n_clicks3 is None:
        
        raise PreventUpdate
    
    else:
        
        dfsupply, df_supply_plant,hdlistsuply,dfdatasupply,stylessupplyup,hdlistsupply2,dfdatasupplyplant,stylessupplylow, hdlistdemand, dfdatademand,stylesdemandup, hdlistdemand2, dfdatademandcountry,stylesdemandlow = SD_Table_30.get_new_data()
        
        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print("MA30 data updated at: ", now)
        supply_table_layout = html.Div([
            html.H1('Data updated at: '+ datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')),
            html.Hr(),
            dash_table.DataTable(
                
                id='table1',
                columns=hdlistsuply,
                #data=dfsupply.loc[3:].to_dict('records'),
                data=dfdatasupply.to_dict('records'),
                editable=True,
                sort_action='native',
                filter_action="native",
                style_filter = {'height':'25px'},
                #column_deletable=True,
                row_deletable=True,
        
                style_cell={
                    'whiteSpace': 'normal',
                    'height': 'auto',
                    #'border': '3px solid grey'
                },
                
                style_cell_conditional=[
                    {
                        'if': {'column_id': 'M'},
                        'fontWeight': 'bold'
                    },
                    {
                        'if': {'column_id': ' M '},
                        'fontWeight': 'bold'
                    },
                    {
                        'if': {'column_id': 'Year'},
                        'fontWeight': 'bold'
                    },
                    {
                        'if': {'column_id': 'Year'},
                        'textAlign': 'left'
                    },
                    {
                        'if': {'column_id': 'Supply'},
                        'textAlign': 'left'
                    },
                   {
                        'if': {
                            'row_index': 4,
                            },
                        'backgroundColor': '#DDEBF7'
                    },
                    {
                        'if': {
                            'row_index': 5,
                            },
                        'backgroundColor': '#DDEBF7'
                    },
                    {
                        'if': {
                            'row_index': 6,
                            },
                        'backgroundColor': '#DDEBF7'
                    },
                    {
                        'if': {
                            'row_index': 7,
                            },
                        'backgroundColor': '#DDEBF7'
                    },
                    {
                        'if': {
                            'row_index': 8,
                            },
                        'backgroundColor': '#DDEBF7'
                    },
                    {
                        'if': {
                            'row_index': 9,
                            },
                        'backgroundColor': '#DDEBF7'
                    },
                    {
                        'if': {
                            'row_index': 10,
                            },
                        'backgroundColor': '#DDEBF7'
                    },
                    
                ],
                style_header={
                    'backgroundColor': '#D9D9D9',
                    #'fontWeight': 'bold',
                    'textAlign': 'center',
                    #'border': '3px solid black'
                    #'color': 'white'
                },
                style_header_conditional=[
                    {
                        'if': {'column_id': 'Year'},
                        'textAlign': 'left'
                    },
                    {
                        'if': {'column_id': 'Supply'},
                        'textAlign': 'left'
                    },
                    {
                        'if': {
                            'header_index': 1,
                            'column_id': 'M'},
                        'backgroundColor': '#FFFF00'
                    },
                    {
                        'if': {
                            'header_index': 1,
                            'column_id': ' M '},
                        'backgroundColor': '#FFFF00'
                    },
                    {
                        'if': {'column_id': 'M'},
                        'fontWeight': 'bold'
                    },
                    {
                        'if': {'column_id': ' M '},
                        'fontWeight': 'bold'
                    },
                    
                ],
                
                style_data_conditional=stylessupplyup,
                style_as_list_view=True,
            ),
            html.Hr(),
            html.Br(),
            html.Hr(),
            dash_table.DataTable(
            
                id='table2',
                columns=hdlistsupply2,
                #data=dfsupply.loc[3:].to_dict('records'),
                data=dfdatasupplyplant.to_dict('records'),
                editable=True,
                sort_action='native',
                filter_action="native",
                style_filter = {'height':'25px'},
                #column_deletable=True,
                row_deletable=True,
                style_cell={
                    'whiteSpace': 'normal',
                    'height': 'auto',
                },
                
                style_cell_conditional=[
                    {
                        'if': {'column_id': 'M Kpler'},
                        'fontWeight': 'bold'
                    },
                    {
                        'if': {'column_id': 'M Desk'},
                        'fontWeight': 'bold'
                    },
                    {
                        'if': {'column_id': 'Suez'},
                        'textAlign': 'left'
                    },
                    {
                        'if': {'column_id': 'Basin'},
                        'textAlign': 'left'
                    },
                    {
                        'if': {'column_id': 'Market'},
                        'textAlign': 'left'
                    },
                    {
                        'if': {'column_id': 'Plant'},
                        'textAlign': 'left'
                    },
                    
                ],
                style_header={
                    'backgroundColor': '#D9D9D9',
                    'fontWeight': 'bold',
                    'textAlign': 'right',
                    #'color': 'white'
                },
                style_header_conditional=[
                    {
                        'if': {'column_id': 'Suez'},
                        'textAlign': 'left'
                    },
                    {
                        'if': {'column_id': 'Basin'},
                        'textAlign': 'left'
                    },
                    {
                        'if': {'column_id': 'Market'},
                        'textAlign': 'left'
                    },
                    {
                        'if': {'column_id': 'Plant'},
                        'textAlign': 'left'
                    },
                    {
                        'if': {
                            'header_index': 0,
                            'column_id': 'M Kpler'},
                        'backgroundColor': '#FFFF00'
                    },
                    {
                        'if': {
                            'header_index': 0,
                            'column_id': 'M Desk'},
                        'backgroundColor': '#FFFF00'
                    },
                    
                ],
                
                style_data_conditional=stylessupplylow,
                style_as_list_view=True,
            ),
            html.Hr(),
            
            ])
    
    
        demand_table_layout = html.Div([
        html.H1('Data updated at: '+now),
        html.Hr(),
        dash_table.DataTable(
        
            id='table3',
            columns=hdlistdemand,
            #data=dfsupply.loc[3:].to_dict('records'),
            data=dfdatademand.to_dict('records'),
            editable=True,
            sort_action='native',
            filter_action="native",
            style_filter = {'height':'25px'},
            #column_deletable=True,
            row_deletable=True,
    
            style_cell={
                'whiteSpace': 'normal',
                'height': 'auto',
                #'border': '3px solid grey'
            },
            
            style_cell_conditional=[
                {
                    'if': {'column_id': 'M'},
                    'fontWeight': 'bold'
                },
                {
                    'if': {'column_id': ' M '},
                    'fontWeight': 'bold'
                },
                {
                    'if': {'column_id': 'Year'},
                    'fontWeight': 'bold'
                },
                {
                    'if': {'column_id': 'Year'},
                    'textAlign': 'left'
                },
                {
                    'if': {'column_id': 'Demand'},
                    'textAlign': 'left'
                },
                {
                    'if': {
                        'row_index': 4,
                        },
                    'backgroundColor': '#DDEBF7'
                },
                {
                    'if': {
                        'row_index': 5,
                        },
                    'backgroundColor': '#DDEBF7'
                },
                {
                    'if': {
                        'row_index': 6,
                        },
                    'backgroundColor': '#DDEBF7'
                },
                {
                    'if': {
                        'row_index': 7,
                        },
                    'backgroundColor': '#DDEBF7'
                },
                {
                    'if': {
                        'row_index': 8,
                        },
                    'backgroundColor': '#DDEBF7'
                },
                {
                    'if': {
                        'row_index': 9,
                        },
                    'backgroundColor': '#DDEBF7'
                },
                {
                    'if': {
                        'row_index': 10,
                        },
                    'backgroundColor': '#DDEBF7'
                },
                
            ],
            style_header={
                'backgroundColor': '#D9D9D9',
                #'fontWeight': 'bold',
                'textAlign': 'center',
                #'border': '3px solid black'
                #'color': 'white'
            },
            style_header_conditional=[
                {
                    'if': {'column_id': 'Year'},
                    'textAlign': 'left'
                },
                {
                    'if': {'column_id':'Demand'},
                    'textAlign': 'left'
                },
                {
                    'if': {
                        'header_index': 1,
                        'column_id': 'M'},
                    'backgroundColor': '#FFFF00'
                },
                {
                    'if': {
                        'header_index': 1,
                        'column_id': ' M '},
                    'backgroundColor': '#FFFF00'
                },
                {
                    'if': {'column_id': 'M'},
                    'fontWeight': 'bold'
                },
                {
                    'if': {'column_id': ' M '},
                    'fontWeight': 'bold'
                },
                
                
            ],
            
            style_data_conditional=stylesdemandup,
            style_as_list_view=True,
        ),
        html.Hr(),
        html.Br(),
        html.Hr(),
        dash_table.DataTable(
        
            id='table4',
            columns=hdlistdemand2,
            #data=dfsupply.loc[3:].to_dict('records'),
            data=dfdatademandcountry.to_dict('records'),
            #editable=True,
            sort_action='native',
            filter_action="native",
            style_filter = {'height':'25px'},
            #column_deletable=True,
            row_deletable=True,
            style_cell={
                'whiteSpace': 'normal',
                'height': 'auto',
            },
            
            style_cell_conditional=[
                {
                    'if': {'column_id': 'M Kpler'},
                    'fontWeight': 'bold'
                },
                {
                    'if': {'column_id': 'M Desk'},
                    'fontWeight': 'bold'
                },
                {
                    'if': {'column_id': 'Suez'},
                    'textAlign': 'left'
                },
                {
                    'if': {'column_id': 'Basin'},
                    'textAlign': 'left'
                },
                {
                    'if': {'column_id': 'Region'},
                    'textAlign': 'left'
                },
                {
                    'if': {'column_id': 'Market'},
                    'textAlign': 'left'
                },
                
            ],
            style_header={
                'backgroundColor': '#D9D9D9',
                'fontWeight': 'bold',
                'textAlign': 'right',
                #'color': 'white'
            },
            style_header_conditional=[
                {
                    'if': {'column_id': 'Suez'},
                    'textAlign': 'left'
                },
                {
                    'if': {'column_id': 'Basin'},
                    'textAlign': 'left'
                },
                {
                    'if': {'column_id': 'Region'},
                    'textAlign': 'left'
                },
                {
                    'if': {'column_id': 'Market'},
                    'textAlign': 'left'
                },
                {
                    'if': {
                        'header_index': 0,
                        'column_id': 'M Kpler'},
                    'backgroundColor': '#FFFF00'
                },
                {
                    'if': {
                        'header_index': 0,
                        'column_id': 'M Desk'},
                    'backgroundColor': '#FFFF00'
                },
                
            ],
            
            style_data_conditional=stylesdemandlow,
            style_as_list_view=True,
        ),
        
        ])    
        
        return supply_table_layout, #demand_table_layout



@app.callback(
    #Output(component_id='body-div-supply30', component_property='children'),
    Output(component_id='body-div-demand30', component_property='children'),
    Input(component_id='refrash30', component_property='n_clicks'),
    #Input(component_id='refrash_supply', component_property='n_clicks')
)
def update_output30(n_clicks4):
    
    if n_clicks4 is None:
        
        raise PreventUpdate
    
    else:
        
        dfsupply, df_supply_plant,hdlistsuply,dfdatasupply,stylessupplyup,hdlistsupply2,dfdatasupplyplant,stylessupplylow, hdlistdemand, dfdatademand,stylesdemandup, hdlistdemand2, dfdatademandcountry,stylesdemandlow = SD_Table_30.get_new_data()
        
        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print("MA30 data updated at: ", now)
        supply_table_layout = html.Div([
            html.H1('Data updated at: '+ datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')),
            html.Hr(),
            dash_table.DataTable(
                
                id='table1',
                columns=hdlistsuply,
                #data=dfsupply.loc[3:].to_dict('records'),
                data=dfdatasupply.to_dict('records'),
                editable=True,
                sort_action='native',
                filter_action="native",
                style_filter = {'height':'25px'},
                #column_deletable=True,
                row_deletable=True,
        
                style_cell={
                    'whiteSpace': 'normal',
                    'height': 'auto',
                    #'border': '3px solid grey'
                },
                
                style_cell_conditional=[
                    {
                        'if': {'column_id': 'M'},
                        'fontWeight': 'bold'
                    },
                    {
                        'if': {'column_id': ' M '},
                        'fontWeight': 'bold'
                    },
                    {
                        'if': {'column_id': 'Year'},
                        'fontWeight': 'bold'
                    },
                    {
                        'if': {'column_id': 'Year'},
                        'textAlign': 'left'
                    },
                    {
                        'if': {'column_id': 'Supply'},
                        'textAlign': 'left'
                    },
                   {
                        'if': {
                            'row_index': 4,
                            },
                        'backgroundColor': '#DDEBF7'
                    },
                    {
                        'if': {
                            'row_index': 5,
                            },
                        'backgroundColor': '#DDEBF7'
                    },
                    {
                        'if': {
                            'row_index': 6,
                            },
                        'backgroundColor': '#DDEBF7'
                    },
                    {
                        'if': {
                            'row_index': 7,
                            },
                        'backgroundColor': '#DDEBF7'
                    },
                    {
                        'if': {
                            'row_index': 8,
                            },
                        'backgroundColor': '#DDEBF7'
                    },
                    {
                        'if': {
                            'row_index': 9,
                            },
                        'backgroundColor': '#DDEBF7'
                    },
                    {
                        'if': {
                            'row_index': 10,
                            },
                        'backgroundColor': '#DDEBF7'
                    },
                    
                ],
                style_header={
                    'backgroundColor': '#D9D9D9',
                    #'fontWeight': 'bold',
                    'textAlign': 'center',
                    #'border': '3px solid black'
                    #'color': 'white'
                },
                style_header_conditional=[
                    {
                        'if': {'column_id': 'Year'},
                        'textAlign': 'left'
                    },
                    {
                        'if': {'column_id': 'Supply'},
                        'textAlign': 'left'
                    },
                    {
                        'if': {
                            'header_index': 1,
                            'column_id': 'M'},
                        'backgroundColor': '#FFFF00'
                    },
                    {
                        'if': {
                            'header_index': 1,
                            'column_id': ' M '},
                        'backgroundColor': '#FFFF00'
                    },
                    {
                        'if': {'column_id': 'M'},
                        'fontWeight': 'bold'
                    },
                    {
                        'if': {'column_id': ' M '},
                        'fontWeight': 'bold'
                    },
                    
                ],
                
                style_data_conditional=stylessupplyup,
                style_as_list_view=True,
            ),
            html.Hr(),
            html.Br(),
            html.Hr(),
            dash_table.DataTable(
            
                id='table2',
                columns=hdlistsupply2,
                #data=dfsupply.loc[3:].to_dict('records'),
                data=dfdatasupplyplant.to_dict('records'),
                editable=True,
                sort_action='native',
                filter_action="native",
                style_filter = {'height':'25px'},
                #column_deletable=True,
                row_deletable=True,
                style_cell={
                    'whiteSpace': 'normal',
                    'height': 'auto',
                },
                
                style_cell_conditional=[
                    {
                        'if': {'column_id': 'M Kpler'},
                        'fontWeight': 'bold'
                    },
                    {
                        'if': {'column_id': 'M Desk'},
                        'fontWeight': 'bold'
                    },
                    {
                        'if': {'column_id': 'Suez'},
                        'textAlign': 'left'
                    },
                    {
                        'if': {'column_id': 'Basin'},
                        'textAlign': 'left'
                    },
                    {
                        'if': {'column_id': 'Market'},
                        'textAlign': 'left'
                    },
                    {
                        'if': {'column_id': 'Plant'},
                        'textAlign': 'left'
                    },
                    
                ],
                style_header={
                    'backgroundColor': '#D9D9D9',
                    'fontWeight': 'bold',
                    'textAlign': 'right',
                    #'color': 'white'
                },
                style_header_conditional=[
                    {
                        'if': {'column_id': 'Suez'},
                        'textAlign': 'left'
                    },
                    {
                        'if': {'column_id': 'Basin'},
                        'textAlign': 'left'
                    },
                    {
                        'if': {'column_id': 'Market'},
                        'textAlign': 'left'
                    },
                    {
                        'if': {'column_id': 'Plant'},
                        'textAlign': 'left'
                    },
                    {
                        'if': {
                            'header_index': 0,
                            'column_id': 'M Kpler'},
                        'backgroundColor': '#FFFF00'
                    },
                    {
                        'if': {
                            'header_index': 0,
                            'column_id': 'M Desk'},
                        'backgroundColor': '#FFFF00'
                    },
                    
                ],
                
                style_data_conditional=stylessupplylow,
                style_as_list_view=True,
            ),
            html.Hr(),
            
            ])
    
    
        demand_table_layout = html.Div([
        html.H1('Data updated at: '+now),
        html.Hr(),
        dash_table.DataTable(
        
            id='table3',
            columns=hdlistdemand,
            #data=dfsupply.loc[3:].to_dict('records'),
            data=dfdatademand.to_dict('records'),
            editable=True,
            sort_action='native',
            filter_action="native",
            style_filter = {'height':'25px'},
            #column_deletable=True,
            row_deletable=True,
    
            style_cell={
                'whiteSpace': 'normal',
                'height': 'auto',
                #'border': '3px solid grey'
            },
            
            style_cell_conditional=[
                {
                    'if': {'column_id': 'M'},
                    'fontWeight': 'bold'
                },
                {
                    'if': {'column_id': ' M '},
                    'fontWeight': 'bold'
                },
                {
                    'if': {'column_id': 'Year'},
                    'fontWeight': 'bold'
                },
                {
                    'if': {'column_id': 'Year'},
                    'textAlign': 'left'
                },
                {
                    'if': {'column_id': 'Demand'},
                    'textAlign': 'left'
                },
                {
                    'if': {
                        'row_index': 4,
                        },
                    'backgroundColor': '#DDEBF7'
                },
                {
                    'if': {
                        'row_index': 5,
                        },
                    'backgroundColor': '#DDEBF7'
                },
                {
                    'if': {
                        'row_index': 6,
                        },
                    'backgroundColor': '#DDEBF7'
                },
                {
                    'if': {
                        'row_index': 7,
                        },
                    'backgroundColor': '#DDEBF7'
                },
                {
                    'if': {
                        'row_index': 8,
                        },
                    'backgroundColor': '#DDEBF7'
                },
                {
                    'if': {
                        'row_index': 9,
                        },
                    'backgroundColor': '#DDEBF7'
                },
                {
                    'if': {
                        'row_index': 10,
                        },
                    'backgroundColor': '#DDEBF7'
                },
                
            ],
            style_header={
                'backgroundColor': '#D9D9D9',
                #'fontWeight': 'bold',
                'textAlign': 'center',
                #'border': '3px solid black'
                #'color': 'white'
            },
            style_header_conditional=[
                {
                    'if': {'column_id': 'Year'},
                    'textAlign': 'left'
                },
                {
                    'if': {'column_id':'Demand'},
                    'textAlign': 'left'
                },
                {
                    'if': {
                        'header_index': 1,
                        'column_id': 'M'},
                    'backgroundColor': '#FFFF00'
                },
                {
                    'if': {
                        'header_index': 1,
                        'column_id': ' M '},
                    'backgroundColor': '#FFFF00'
                },
                {
                    'if': {'column_id': 'M'},
                    'fontWeight': 'bold'
                },
                {
                    'if': {'column_id': ' M '},
                    'fontWeight': 'bold'
                },
                
                
            ],
            
            style_data_conditional=stylesdemandup,
            style_as_list_view=True,
        ),
        html.Hr(),
        html.Br(),
        html.Hr(),
        dash_table.DataTable(
        
            id='table4',
            columns=hdlistdemand2,
            #data=dfsupply.loc[3:].to_dict('records'),
            data=dfdatademandcountry.to_dict('records'),
            #editable=True,
            sort_action='native',
            filter_action="native",
            style_filter = {'height':'25px'},
            #column_deletable=True,
            row_deletable=True,
            style_cell={
                'whiteSpace': 'normal',
                'height': 'auto',
            },
            
            style_cell_conditional=[
                {
                    'if': {'column_id': 'M Kpler'},
                    'fontWeight': 'bold'
                },
                {
                    'if': {'column_id': 'M Desk'},
                    'fontWeight': 'bold'
                },
                {
                    'if': {'column_id': 'Suez'},
                    'textAlign': 'left'
                },
                {
                    'if': {'column_id': 'Basin'},
                    'textAlign': 'left'
                },
                {
                    'if': {'column_id': 'Region'},
                    'textAlign': 'left'
                },
                {
                    'if': {'column_id': 'Market'},
                    'textAlign': 'left'
                },
                
            ],
            style_header={
                'backgroundColor': '#D9D9D9',
                'fontWeight': 'bold',
                'textAlign': 'right',
                #'color': 'white'
            },
            style_header_conditional=[
                {
                    'if': {'column_id': 'Suez'},
                    'textAlign': 'left'
                },
                {
                    'if': {'column_id': 'Basin'},
                    'textAlign': 'left'
                },
                {
                    'if': {'column_id': 'Region'},
                    'textAlign': 'left'
                },
                {
                    'if': {'column_id': 'Market'},
                    'textAlign': 'left'
                },
                {
                    'if': {
                        'header_index': 0,
                        'column_id': 'M Kpler'},
                    'backgroundColor': '#FFFF00'
                },
                {
                    'if': {
                        'header_index': 0,
                        'column_id': 'M Desk'},
                    'backgroundColor': '#FFFF00'
                },
                
            ],
            
            style_data_conditional=stylesdemandlow,
            style_as_list_view=True,
        ),
        
        ])    
        
        return demand_table_layout


if __name__ == '__main__':
    app.run_server(debug=False, host='0.0.0.0', port=8050)
    



