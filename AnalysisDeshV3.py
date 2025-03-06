# -*- coding: utf-8 -*-
"""
Created on Fri Sep  1 10:03:19 2023

@author: SVC-GASQuant2-Prod
"""

# -*- coding: utf-8 -*-
"""
Created on Thu Jan  5 08:42:14 2023

@author: SVC-GASQuant2-Prod
"""




#Analysis, to save memory, use MA table V5 + terminalV8
#V1 add charter rate fcst
#v2 add tempdemand


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
import plotly.graph_objs as go

from dateutil.relativedelta import relativedelta
sys.path.append('C:\\Users\SVC-GASQuant2-Prod\YWwebsite\Python Scripts\class') 
#from SDdata import SD_Data_Country
#from SDdata import SD_Data_Basin
import dash_table 
from DBtoDF import DBTOPD
from ModelDeltaV2 import model_delta
from ModelDeltachartV1 import model_delta_chart
import colorlover
from CharterRateV4 import CharterRate
from tempdemandV0 import temp_demand

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
        dfdemandMA.sort_index(inplace=True)
        
        supplyMA=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'SupplyCountry')
        dfsupplyMA=supplyMA.sql_to_df()
        dfsupplyMA.set_index('Date', inplace=True)
        dfsupplyMA.sort_index(inplace=True)
        
        supplyMA_eta=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'SupplyPlantETA')
        dfsupplyMA_eta=supplyMA_eta.sql_to_df()
        dfsupplyMA_eta.set_index('Date', inplace=True)
        dfsupplyMA_eta.sort_index(inplace=True)
        
        demandMA_eta=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'DemandETA')
        dfdemandMA_eta=demandMA_eta.sql_to_df()
        dfdemandMA_eta.set_index('Date', inplace=True)
        dfdemandMA_eta.sort_index(inplace=True)
        
        desk_supply_plant_view=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'DeskSupplyPlant')
        desk_supply_plant_view=desk_supply_plant_view.sql_to_df()
        desk_supply_plant_view.set_index('Date', inplace=True)
        desk_supply_plant_view.sort_index(inplace=True)
        
        desksupplyview=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'DeskSupplyCountry')
        desk_supply_view=desksupplyview.sql_to_df()
        desk_supply_view.set_index('Date', inplace=True)
        desk_supply_view.sort_index(inplace=True)
        
        deskdemandview=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'DeskDemand')
        desk_demand_view=deskdemandview.sql_to_df()
        desk_demand_view.set_index('Date', inplace=True)
        desk_demand_view.sort_index(inplace=True)
        
        ihscontract=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'IHSContract')
        dfihscontract=ihscontract.sql_to_df()
        dfihscontract.set_index('Date', inplace=True)
        dfihscontract.sort_index(inplace=True)
        
        #categories index
        SupplyCategories=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'CategoriesSupply').sql_to_df()
        DemandCategories=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'CategoriesDemand').sql_to_df()
        
        droplist = ['Algeria','Angola','Bahamas','Brunei','Cameroon','Equatorial Guinea','Libya','Nigeria','Papua New Guinea','Peru','Qatar','Russian Federation','Trinidad and Tobago','Yemen']
        for i in droplist:
            DemandCategories.drop(DemandCategories[DemandCategories['Country']==i].index, inplace=True)
          
        droplist_supply = ['Kollsnes','Stavanger','Vysotsk']
        for i in droplist_supply:
            SupplyCategories.drop(SupplyCategories[SupplyCategories['Plant']==i].index, inplace=True)
          
        #print(df_supply)
        basinsupply=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'BasinSupply')
        dfbasinsupply=basinsupply.sql_to_df()
        dfbasinsupply.set_index('Date', inplace=True)
        dfbasinsupply.sort_index(inplace=True)
        
        basindemand=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'BasinDemand')
        dfbasindemand=basindemand.sql_to_df()
        dfbasindemand.set_index('Date', inplace=True)
        dfbasindemand.sort_index(inplace=True)
        
        cumsupply=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'CumulativeSupply')
        dfcumsupply=cumsupply.sql_to_df()
        dfcumsupply.rename(columns=({'index':'Date'}), inplace=True)
        dfcumsupply.set_index('Date', inplace=True)
        dfcumsupply.sort_index(inplace=True)
        
        cumdemand=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'CumulativeDemand')
        dfcumdemand=cumdemand.sql_to_df()
        dfcumdemand.rename(columns=({'index':'Date'}), inplace=True)
        dfcumdemand.set_index('Date', inplace=True)
        dfcumdemand.sort_index(inplace=True)
       
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
        dfsupply.loc[6,'YoY'] = (supply_basin.loc[date_dict['today'],'MENA'] - supply_basin.loc[date_dict['last_year'],'MENA']).round(2) 
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
        dfsupply.loc[6,'MoM'] = (supply_basin.loc[date_dict['today'],'MENA'] - supply_basin.loc[date_dict['last_month'],'MENA']).round(2) 
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
        dfdemand['Year']=['Date','Source','Unit','Global','PB','AB','MENA_Bas','JKTC','MEIP', 'Eur Desk','Other Eur','Lat Am','Other RoW']
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
        dfdemand.loc[9,'M-1'] = demand_basin.loc[date_dict['last_month'],'EurDesk']
        #dfdemand.loc[10,'M-1'] = demand_basin.loc[date_dict['last_month'],'MedEur']
        dfdemand.loc[10,'M-1'] = demand_basin.loc[date_dict['last_month'],'OtherEur']
        dfdemand.loc[11,'M-1'] = demand_basin.loc[date_dict['last_month'],'LatAm']
        dfdemand.loc[12,'M-1'] = demand_basin.loc[date_dict['last_month'],'OtherRoW']
        #M Kpler
        dfdemand.loc[3,'M'] = demand_basin.loc[date_dict['today'],'Global']
        dfdemand.loc[4,'M'] = demand_basin.loc[date_dict['today'],'PB']
        dfdemand.loc[5,'M'] = demand_basin.loc[date_dict['today'],'AB']
        dfdemand.loc[6,'M'] = demand_basin.loc[date_dict['today'],'MENA']
        dfdemand.loc[7,'M'] = demand_basin.loc[date_dict['today'],'JKTC']
        dfdemand.loc[8,'M'] = demand_basin.loc[date_dict['today'],'MEIP']
        dfdemand.loc[9,'M'] = demand_basin.loc[date_dict['today'],'EurDesk']
        #dfdemand.loc[10,'M'] = demand_basin.loc[date_dict['today'],'MedEur']
        dfdemand.loc[10,'M'] = demand_basin.loc[date_dict['today'],'OtherEur']
        dfdemand.loc[11,'M'] = demand_basin.loc[date_dict['today'],'LatAm']
        dfdemand.loc[12,'M'] = demand_basin.loc[date_dict['today'],'OtherRoW']
        #M Desk
        dfdemand.loc[3,' M '] = demand_basin.loc[date_dict['today'],'Global Desk']
        dfdemand.loc[4,' M '] = demand_basin.loc[date_dict['today'],'PB Desk']
        dfdemand.loc[5,' M '] = demand_basin.loc[date_dict['today'],'AB Desk']
        dfdemand.loc[6,' M '] = demand_basin.loc[date_dict['today'],'MENA Desk']
        dfdemand.loc[7,' M '] = demand_basin.loc[date_dict['today'],'JKTC Desk']
        dfdemand.loc[8,' M '] = demand_basin.loc[date_dict['today'],'MEIP Desk']
        dfdemand.loc[9,' M '] = demand_basin.loc[date_dict['today'],'EurDesk Desk']
        #dfdemand.loc[10,' M '] = demand_basin.loc[date_dict['today'],'MedEur Desk']
        dfdemand.loc[10,' M '] = demand_basin.loc[date_dict['today'],'OtherEur Desk']
        dfdemand.loc[11,' M '] = demand_basin.loc[date_dict['today'],'LatAm Desk']
        dfdemand.loc[12,' M '] = demand_basin.loc[date_dict['today'],'OtherRoW Desk']
        #M+1 Desk
        dfdemand.loc[3,'M+1'] = demand_basin.loc[date_dict['next_1month'],'Global Desk']
        dfdemand.loc[4,'M+1'] = demand_basin.loc[date_dict['next_1month'],'PB Desk']
        dfdemand.loc[5,'M+1'] = demand_basin.loc[date_dict['next_1month'],'AB Desk']
        dfdemand.loc[6,'M+1'] = demand_basin.loc[date_dict['next_1month'],'MENA Desk']
        dfdemand.loc[7,'M+1'] = demand_basin.loc[date_dict['next_1month'],'JKTC Desk']
        dfdemand.loc[8,'M+1'] = demand_basin.loc[date_dict['next_1month'],'MEIP Desk']
        dfdemand.loc[9,'M+1'] = demand_basin.loc[date_dict['next_1month'],'EurDesk Desk']
        #dfdemand.loc[10,'M+1'] = demand_basin.loc[date_dict['next_1month'],'MedEur Desk']
        dfdemand.loc[10,'M+1'] = demand_basin.loc[date_dict['next_1month'],'OtherEur Desk']
        dfdemand.loc[11,'M+1'] = demand_basin.loc[date_dict['next_1month'],'LatAm Desk']
        dfdemand.loc[12,'M+1'] = demand_basin.loc[date_dict['next_1month'],'OtherRoW Desk']
        #M+2 Desk
        dfdemand.loc[3,'M+2'] = demand_basin.loc[date_dict['next_2month'],'Global Desk']
        dfdemand.loc[4,'M+2'] = demand_basin.loc[date_dict['next_2month'],'PB Desk']
        dfdemand.loc[5,'M+2'] = demand_basin.loc[date_dict['next_2month'],'AB Desk']
        dfdemand.loc[6,'M+2'] = demand_basin.loc[date_dict['next_2month'],'MENA Desk']
        dfdemand.loc[7,'M+2'] = demand_basin.loc[date_dict['next_2month'],'JKTC Desk']
        dfdemand.loc[8,'M+2'] = demand_basin.loc[date_dict['next_2month'],'MEIP Desk']
        dfdemand.loc[9,'M+2'] = demand_basin.loc[date_dict['next_2month'],'EurDesk Desk']
        #dfdemand.loc[10,'M+2'] = demand_basin.loc[date_dict['next_2month'],'MedEur Desk']
        dfdemand.loc[10,'M+2'] = demand_basin.loc[date_dict['next_2month'],'OtherEur Desk']
        dfdemand.loc[11,'M+2'] = demand_basin.loc[date_dict['next_2month'],'LatAm Desk']
        dfdemand.loc[12,'M+2'] = demand_basin.loc[date_dict['next_2month'],'OtherRoW Desk']
        #M-1 Desk
        dfdemand.loc[3,' M-1 '] = demand_basin.loc[date_dict['last_month'],'Global Desk']
        dfdemand.loc[4,' M-1 '] = demand_basin.loc[date_dict['last_month'],'PB Desk']
        dfdemand.loc[5,' M-1 '] = demand_basin.loc[date_dict['last_month'],'AB Desk']
        dfdemand.loc[6,' M-1 '] = demand_basin.loc[date_dict['last_month'],'MENA Desk']
        dfdemand.loc[7,' M-1 '] = demand_basin.loc[date_dict['last_month'],'JKTC Desk']
        dfdemand.loc[8,' M-1 '] = demand_basin.loc[date_dict['last_month'],'MEIP Desk']
        dfdemand.loc[9,' M-1 '] = demand_basin.loc[date_dict['last_month'],'EurDesk Desk']
        #dfdemand.loc[10,' M-1 '] = demand_basin.loc[date_dict['last_month'],'MedEur Desk']
        dfdemand.loc[10,' M-1 '] = demand_basin.loc[date_dict['last_month'],'OtherEur Desk']
        dfdemand.loc[11,' M-1 '] = demand_basin.loc[date_dict['last_month'],'LatAm Desk']
        dfdemand.loc[12,' M-1 '] = demand_basin.loc[date_dict['last_month'],'OtherRoW Desk']
        #YoY Kpler
        dfdemand.loc[3,'YoY'] = demand_basin.loc[date_dict['today'],'Global'] - demand_basin.loc[date_dict['last_year'],'Global'] 
        dfdemand.loc[4,'YoY'] = demand_basin.loc[date_dict['today'],'PB'] - demand_basin.loc[date_dict['last_year'],'PB'] 
        dfdemand.loc[5,'YoY'] = demand_basin.loc[date_dict['today'],'AB'] - demand_basin.loc[date_dict['last_year'],'AB'] 
        dfdemand.loc[6,'YoY'] = demand_basin.loc[date_dict['today'],'MENA'] - demand_basin.loc[date_dict['last_year'],'MENA']
        dfdemand.loc[7,'YoY'] = demand_basin.loc[date_dict['today'],'JKTC'] - demand_basin.loc[date_dict['last_year'],'JKTC']
        dfdemand.loc[8,'YoY'] = demand_basin.loc[date_dict['today'],'MEIP'] - demand_basin.loc[date_dict['last_year'],'MEIP']
        dfdemand.loc[9,'YoY'] = demand_basin.loc[date_dict['today'],'EurDesk'] - demand_basin.loc[date_dict['last_year'],'EurDesk']
        #dfdemand.loc[10,'YoY'] = demand_basin.loc[date_dict['today'],'MedEur'] - demand_basin.loc[date_dict['last_year'],'MedEur']
        dfdemand.loc[10,'YoY'] = demand_basin.loc[date_dict['today'],'OtherEur'] - demand_basin.loc[date_dict['last_year'],'OtherEur']
        dfdemand.loc[11,'YoY'] = demand_basin.loc[date_dict['today'],'LatAm'] - demand_basin.loc[date_dict['last_year'],'LatAm']
        dfdemand.loc[12,'YoY'] = demand_basin.loc[date_dict['today'],'OtherRoW'] - demand_basin.loc[date_dict['last_year'],'OtherRoW']
        dfdemand.loc[3:12, 'YoY'] = dfdemand.loc[3:12, 'YoY'].astype('float').round(2)
        #MoM Kpler
        dfdemand.loc[3,'MoM'] = demand_basin.loc[date_dict['today'],'Global'] - demand_basin.loc[date_dict['last_month'],'Global'] 
        dfdemand.loc[4,'MoM'] = demand_basin.loc[date_dict['today'],'PB'] - demand_basin.loc[date_dict['last_month'],'PB'] 
        dfdemand.loc[5,'MoM'] = demand_basin.loc[date_dict['today'],'AB'] - demand_basin.loc[date_dict['last_month'],'AB'] 
        dfdemand.loc[6,'MoM'] = demand_basin.loc[date_dict['today'],'MENA'] - demand_basin.loc[date_dict['last_month'],'MENA']
        dfdemand.loc[7,'MoM'] = demand_basin.loc[date_dict['today'],'JKTC'] - demand_basin.loc[date_dict['last_month'],'JKTC']
        dfdemand.loc[8,'MoM'] = demand_basin.loc[date_dict['today'],'MEIP'] - demand_basin.loc[date_dict['last_month'],'MEIP']
        dfdemand.loc[9,'MoM'] = demand_basin.loc[date_dict['today'],'EurDesk'] - demand_basin.loc[date_dict['last_month'],'EurDesk']
        #dfdemand.loc[10,'MoM'] = demand_basin.loc[date_dict['today'],'MedEur'] - demand_basin.loc[date_dict['last_month'],'MedEur']
        dfdemand.loc[10,'MoM'] = demand_basin.loc[date_dict['today'],'OtherEur'] - demand_basin.loc[date_dict['last_month'],'OtherEur']
        dfdemand.loc[11,'MoM'] = demand_basin.loc[date_dict['today'],'LatAm'] - demand_basin.loc[date_dict['last_month'],'LatAm']
        dfdemand.loc[12,'MoM'] = demand_basin.loc[date_dict['today'],'OtherRoW'] - demand_basin.loc[date_dict['last_month'],'OtherRoW']
        dfdemand.loc[3:12, 'MoM'] = dfdemand.loc[3:12, 'MoM'].astype('float').round(2)
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
        #dfdemand.loc[13,'Actual vs. Forecast'] = dfdemand.loc[13,'M'] - dfdemand.loc[13,' M ']
        dfdemand.loc[3:12, 'Actual vs. Forecast'] = dfdemand.loc[3:12, 'Actual vs. Forecast'].astype('float').round(2)
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
        #dfdemand.loc[13,'Actual vs. Forecast %'] = (dfdemand.loc[13,'M'] - dfdemand.loc[13,' M ']) /dfdemand.loc[13,'M']
        dfdemand.loc[3:12,'Actual vs. Forecast %'] = dfdemand.loc[3:12,'Actual vs. Forecast %'].map(lambda x:format(x,'.0%'))
        #Y-1 Kpler
        dfdemand.loc[3:,'Y-1'] = demand_basin.loc[date_dict['last_year'],'Global']
        dfdemand.loc[4:,'Y-1'] = demand_basin.loc[date_dict['last_year'],'PB']
        dfdemand.loc[5:,'Y-1'] = demand_basin.loc[date_dict['last_year'],'AB']
        dfdemand.loc[6:,'Y-1'] = demand_basin.loc[date_dict['last_year'],'MENA']
        dfdemand.loc[7,'Y-1'] = demand_basin.loc[date_dict['last_year'],'JKTC']
        dfdemand.loc[8,'Y-1'] = demand_basin.loc[date_dict['last_year'],'MEIP']
        dfdemand.loc[9,'Y-1'] = demand_basin.loc[date_dict['last_year'],'EurDesk']
        #dfdemand.loc[10,'Y-1'] = demand_basin.loc[date_dict['last_year'],'MedEur']
        dfdemand.loc[10,'Y-1'] = demand_basin.loc[date_dict['last_year'],'OtherEur']
        dfdemand.loc[11,'Y-1'] = demand_basin.loc[date_dict['last_year'],'LatAm']
        dfdemand.loc[12,'Y-1'] = demand_basin.loc[date_dict['last_year'],'OtherRoW']
        #D-1 Kpler
        dfdemand.loc[3:,'D-1'] = demand_basin.loc[date_dict['last_day'],'Global']
        dfdemand.loc[4:,'D-1'] = demand_basin.loc[date_dict['last_day'],'PB']
        dfdemand.loc[5:,'D-1'] = demand_basin.loc[date_dict['last_day'],'AB']
        dfdemand.loc[6:,'D-1'] = demand_basin.loc[date_dict['last_day'],'MENA']
        dfdemand.loc[7,'D-1'] = demand_basin.loc[date_dict['last_day'],'JKTC']
        dfdemand.loc[8,'D-1'] = demand_basin.loc[date_dict['last_day'],'MEIP']
        dfdemand.loc[9,'D-1'] = demand_basin.loc[date_dict['last_day'],'EurDesk']
        #dfdemand.loc[10,'D-1'] = demand_basin.loc[date_dict['last_day'],'MedEur']
        dfdemand.loc[10,'D-1'] = demand_basin.loc[date_dict['last_day'],'OtherEur']
        dfdemand.loc[11,'D-1'] = demand_basin.loc[date_dict['last_day'],'LatAm']
        dfdemand.loc[12,'D-1'] = demand_basin.loc[date_dict['last_day'],'OtherRoW']
        #D-7 Kpler
        dfdemand.loc[3:,'D-7'] = demand_basin.loc[date_dict['last_week'],'Global']
        dfdemand.loc[4:,'D-7'] = demand_basin.loc[date_dict['last_week'],'PB']
        dfdemand.loc[5:,'D-7'] = demand_basin.loc[date_dict['last_week'],'AB']
        dfdemand.loc[6:,'D-7'] = demand_basin.loc[date_dict['last_week'],'MENA']
        dfdemand.loc[7,'D-7'] = demand_basin.loc[date_dict['last_week'],'JKTC']
        dfdemand.loc[8,'D-7'] = demand_basin.loc[date_dict['last_week'],'MEIP']
        dfdemand.loc[9,'D-7'] = demand_basin.loc[date_dict['last_week'],'EurDesk']
        #dfdemand.loc[10,'D-7'] = demand_basin.loc[date_dict['last_week'],'MedEur']
        dfdemand.loc[10,'D-7'] = demand_basin.loc[date_dict['last_week'],'OtherEur']
        dfdemand.loc[11,'D-7'] = demand_basin.loc[date_dict['last_week'],'LatAm']
        dfdemand.loc[12,'D-7'] = demand_basin.loc[date_dict['last_week'],'OtherRoW']
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
        #dfdemand.loc[13,'DoD'] = dfdemand.loc[13,'M'] - dfdemand.loc[13,'D-1']
        dfdemand.loc[3:12, 'DoD'] = dfdemand.loc[3:12, 'DoD'].astype('float').round(2)
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
        #dfdemand.loc[13,'WoW'] = dfdemand.loc[13,'M'] - dfdemand.loc[13,'D-7']
        dfdemand.loc[3:12, 'WoW'] = dfdemand.loc[3:12, 'WoW'].astype('float').round(2)
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
        dfdatademand=dfdemand.loc[3:12]
        
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
        dfdemandMA.sort_index(inplace=True)
        
        supplyMA=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'SupplyCountry30')
        dfsupplyMA=supplyMA.sql_to_df()
        dfsupplyMA.set_index('Date', inplace=True)
        dfsupplyMA.sort_index(inplace=True)
        
        supplyMA_eta=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'SupplyPlantETA30')
        dfsupplyMA_eta=supplyMA_eta.sql_to_df()
        dfsupplyMA_eta.set_index('Date', inplace=True)
        dfsupplyMA_eta.sort_index(inplace=True)
        
        demandMA_eta=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'DemandETA30')
        dfdemandMA_eta=demandMA_eta.sql_to_df()
        dfdemandMA_eta.set_index('Date', inplace=True)
        dfdemandMA_eta.sort_index(inplace=True)
        
        desk_supply_plant_view=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'DeskSupplyPlant')
        desk_supply_plant_view=desk_supply_plant_view.sql_to_df()
        desk_supply_plant_view.set_index('Date', inplace=True)
        desk_supply_plant_view.sort_index(inplace=True)
        
        desksupplyview=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'DeskSupplyCountry')
        desk_supply_view=desksupplyview.sql_to_df()
        desk_supply_view.set_index('Date', inplace=True)
        desk_supply_view.sort_index(inplace=True)
        
        deskdemandview=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'DeskDemand')
        desk_demand_view=deskdemandview.sql_to_df()
        desk_demand_view.set_index('Date', inplace=True)
        desk_demand_view.sort_index(inplace=True)
        
        
        ihscontract=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'IHSContract')
        dfihscontract=ihscontract.sql_to_df()
        dfihscontract.set_index('Date', inplace=True)
        dfihscontract.sort_index(inplace=True)
        
        #categories index
        SupplyCategories=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'CategoriesSupply').sql_to_df()
        DemandCategories=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'CategoriesDemand').sql_to_df()
        
        droplist = ['Algeria','Angola','Bahamas','Brunei','Cameroon','Equatorial Guinea','Libya','Nigeria','Papua New Guinea','Peru','Philipines','Qatar','Russian Federation','Trinidad and Tobago','Yemen']
        for i in droplist:
            DemandCategories.drop(DemandCategories[DemandCategories['Country']==i].index, inplace=True)
        
        droplist_supply = ['Kollsnes','Stavanger','Vysotsk']
        for i in droplist_supply:
            SupplyCategories.drop(SupplyCategories[SupplyCategories['Plant']==i].index, inplace=True)
        #print(df_supply)
        basinsupply=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'BasinSupply30')
        dfbasinsupply=basinsupply.sql_to_df()
        dfbasinsupply.set_index('Date', inplace=True)
        dfbasinsupply.sort_index(inplace=True)
        
        basindemand=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'BasinDemand30')
        dfbasindemand=basindemand.sql_to_df()
        dfbasindemand.set_index('Date', inplace=True)
        dfbasindemand.sort_index(inplace=True)
        
        cumsupply=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'CumulativeSupply30')
        dfcumsupply=cumsupply.sql_to_df()
        dfcumsupply.rename(columns=({'index':'Date'}), inplace=True)
        dfcumsupply.set_index('Date', inplace=True)
        dfcumsupply.sort_index(inplace=True)
        
        cumdemand=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'CumulativeDemand30')
        dfcumdemand=cumdemand.sql_to_df()
        dfcumdemand.rename(columns=({'index':'Date'}), inplace=True)
        dfcumdemand.set_index('Date', inplace=True)
        dfcumdemand.sort_index(inplace=True)
       
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
        dfsupply.loc[6,'YoY'] = (supply_basin.loc[date_dict['today'],'MENA'] - supply_basin.loc[date_dict['last_year'],'MENA']).round(2) 
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
        dfsupply.loc[6,'MoM'] = (supply_basin.loc[date_dict['today'],'MENA'] - supply_basin.loc[date_dict['last_month'],'MENA']).round(2) 
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

        dfdemand['Year']=['Date','Source','Unit','Global','PB','AB','MENA_Bas','JKTC','MEIP', 'Eur Desk','Other Eur','Lat Am','Other RoW']
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
        dfdemand.loc[9,'M-1'] = demand_basin.loc[date_dict['last_month'],'EurDesk']
        #dfdemand.loc[10,'M-1'] = demand_basin.loc[date_dict['last_month'],'MedEur']
        dfdemand.loc[10,'M-1'] = demand_basin.loc[date_dict['last_month'],'OtherEur']
        dfdemand.loc[11,'M-1'] = demand_basin.loc[date_dict['last_month'],'LatAm']
        dfdemand.loc[12,'M-1'] = demand_basin.loc[date_dict['last_month'],'OtherRoW']
        #M Kpler
        dfdemand.loc[3,'M'] = demand_basin.loc[date_dict['today'],'Global']
        dfdemand.loc[4,'M'] = demand_basin.loc[date_dict['today'],'PB']
        dfdemand.loc[5,'M'] = demand_basin.loc[date_dict['today'],'AB']
        dfdemand.loc[6,'M'] = demand_basin.loc[date_dict['today'],'MENA']
        dfdemand.loc[7,'M'] = demand_basin.loc[date_dict['today'],'JKTC']
        dfdemand.loc[8,'M'] = demand_basin.loc[date_dict['today'],'MEIP']
        dfdemand.loc[9,'M'] = demand_basin.loc[date_dict['today'],'EurDesk']
        #dfdemand.loc[10,'M'] = demand_basin.loc[date_dict['today'],'MedEur']
        dfdemand.loc[10,'M'] = demand_basin.loc[date_dict['today'],'OtherEur']
        dfdemand.loc[11,'M'] = demand_basin.loc[date_dict['today'],'LatAm']
        dfdemand.loc[12,'M'] = demand_basin.loc[date_dict['today'],'OtherRoW']
        #M Desk
        dfdemand.loc[3,' M '] = demand_basin.loc[date_dict['today'],'Global Desk']
        dfdemand.loc[4,' M '] = demand_basin.loc[date_dict['today'],'PB Desk']
        dfdemand.loc[5,' M '] = demand_basin.loc[date_dict['today'],'AB Desk']
        dfdemand.loc[6,' M '] = demand_basin.loc[date_dict['today'],'MENA Desk']
        dfdemand.loc[7,' M '] = demand_basin.loc[date_dict['today'],'JKTC Desk']
        dfdemand.loc[8,' M '] = demand_basin.loc[date_dict['today'],'MEIP Desk']
        dfdemand.loc[9,' M '] = demand_basin.loc[date_dict['today'],'EurDesk Desk']
        #dfdemand.loc[10,' M '] = demand_basin.loc[date_dict['today'],'MedEur Desk']
        dfdemand.loc[10,' M '] = demand_basin.loc[date_dict['today'],'OtherEur Desk']
        dfdemand.loc[11,' M '] = demand_basin.loc[date_dict['today'],'LatAm Desk']
        dfdemand.loc[12,' M '] = demand_basin.loc[date_dict['today'],'OtherRoW Desk']
        #M+1 Desk
        dfdemand.loc[3,'M+1'] = demand_basin.loc[date_dict['next_1month'],'Global Desk']
        dfdemand.loc[4,'M+1'] = demand_basin.loc[date_dict['next_1month'],'PB Desk']
        dfdemand.loc[5,'M+1'] = demand_basin.loc[date_dict['next_1month'],'AB Desk']
        dfdemand.loc[6,'M+1'] = demand_basin.loc[date_dict['next_1month'],'MENA Desk']
        dfdemand.loc[7,'M+1'] = demand_basin.loc[date_dict['next_1month'],'JKTC Desk']
        dfdemand.loc[8,'M+1'] = demand_basin.loc[date_dict['next_1month'],'MEIP Desk']
        dfdemand.loc[9,'M+1'] = demand_basin.loc[date_dict['next_1month'],'EurDesk Desk']
        #dfdemand.loc[10,'M+1'] = demand_basin.loc[date_dict['next_1month'],'MedEur Desk']
        dfdemand.loc[10,'M+1'] = demand_basin.loc[date_dict['next_1month'],'OtherEur Desk']
        dfdemand.loc[11,'M+1'] = demand_basin.loc[date_dict['next_1month'],'LatAm Desk']
        dfdemand.loc[12,'M+1'] = demand_basin.loc[date_dict['next_1month'],'OtherRoW Desk']
        #M+2 Desk
        dfdemand.loc[3,'M+2'] = demand_basin.loc[date_dict['next_2month'],'Global Desk']
        dfdemand.loc[4,'M+2'] = demand_basin.loc[date_dict['next_2month'],'PB Desk']
        dfdemand.loc[5,'M+2'] = demand_basin.loc[date_dict['next_2month'],'AB Desk']
        dfdemand.loc[6,'M+2'] = demand_basin.loc[date_dict['next_2month'],'MENA Desk']
        dfdemand.loc[7,'M+2'] = demand_basin.loc[date_dict['next_2month'],'JKTC Desk']
        dfdemand.loc[8,'M+2'] = demand_basin.loc[date_dict['next_2month'],'MEIP Desk']
        dfdemand.loc[9,'M+2'] = demand_basin.loc[date_dict['next_2month'],'EurDesk Desk']
        #dfdemand.loc[10,'M+2'] = demand_basin.loc[date_dict['next_2month'],'MedEur Desk']
        dfdemand.loc[10,'M+2'] = demand_basin.loc[date_dict['next_2month'],'OtherEur Desk']
        dfdemand.loc[11,'M+2'] = demand_basin.loc[date_dict['next_2month'],'LatAm Desk']
        dfdemand.loc[12,'M+2'] = demand_basin.loc[date_dict['next_2month'],'OtherRoW Desk']
        #M-1 Desk
        dfdemand.loc[3,' M-1 '] = demand_basin.loc[date_dict['last_month'],'Global Desk']
        dfdemand.loc[4,' M-1 '] = demand_basin.loc[date_dict['last_month'],'PB Desk']
        dfdemand.loc[5,' M-1 '] = demand_basin.loc[date_dict['last_month'],'AB Desk']
        dfdemand.loc[6,' M-1 '] = demand_basin.loc[date_dict['last_month'],'MENA Desk']
        dfdemand.loc[7,' M-1 '] = demand_basin.loc[date_dict['last_month'],'JKTC Desk']
        dfdemand.loc[8,' M-1 '] = demand_basin.loc[date_dict['last_month'],'MEIP Desk']
        dfdemand.loc[9,' M-1 '] = demand_basin.loc[date_dict['last_month'],'EurDesk Desk']
        #dfdemand.loc[10,' M-1 '] = demand_basin.loc[date_dict['last_month'],'MedEur Desk']
        dfdemand.loc[10,' M-1 '] = demand_basin.loc[date_dict['last_month'],'OtherEur Desk']
        dfdemand.loc[11,' M-1 '] = demand_basin.loc[date_dict['last_month'],'LatAm Desk']
        dfdemand.loc[12,' M-1 '] = demand_basin.loc[date_dict['last_month'],'OtherRoW Desk']
        #YoY Kpler
        dfdemand.loc[3,'YoY'] = demand_basin.loc[date_dict['today'],'Global'] - demand_basin.loc[date_dict['last_year'],'Global']
        dfdemand.loc[4,'YoY'] = demand_basin.loc[date_dict['today'],'PB'] - demand_basin.loc[date_dict['last_year'],'PB']
        dfdemand.loc[5,'YoY'] = demand_basin.loc[date_dict['today'],'AB'] - demand_basin.loc[date_dict['last_year'],'AB'] 
        dfdemand.loc[6,'YoY'] = demand_basin.loc[date_dict['today'],'MENA'] - demand_basin.loc[date_dict['last_year'],'MENA']
        dfdemand.loc[7,'YoY'] = demand_basin.loc[date_dict['today'],'JKTC'] - demand_basin.loc[date_dict['last_year'],'JKTC']
        dfdemand.loc[8,'YoY'] = demand_basin.loc[date_dict['today'],'MEIP'] - demand_basin.loc[date_dict['last_year'],'MEIP']
        dfdemand.loc[9,'YoY'] = demand_basin.loc[date_dict['today'],'EurDesk'] - demand_basin.loc[date_dict['last_year'],'EurDesk']
        #dfdemand.loc[10,'YoY'] = demand_basin.loc[date_dict['today'],'MedEur'] - demand_basin.loc[date_dict['last_year'],'MedEur']
        dfdemand.loc[10,'YoY'] = demand_basin.loc[date_dict['today'],'OtherEur'] - demand_basin.loc[date_dict['last_year'],'OtherEur']
        dfdemand.loc[11,'YoY'] = demand_basin.loc[date_dict['today'],'LatAm'] - demand_basin.loc[date_dict['last_year'],'LatAm']
        dfdemand.loc[12,'YoY'] = demand_basin.loc[date_dict['today'],'OtherRoW'] - demand_basin.loc[date_dict['last_year'],'OtherRoW']
        dfdemand.loc[3:12, 'YoY'] = dfdemand.loc[3:12, 'YoY'].astype('float').round(2)
        #MoM Kpler
        dfdemand.loc[3,'MoM'] = demand_basin.loc[date_dict['today'],'Global'] - demand_basin.loc[date_dict['last_month'],'Global'] 
        dfdemand.loc[4,'MoM'] = demand_basin.loc[date_dict['today'],'PB'] - demand_basin.loc[date_dict['last_month'],'PB'] 
        dfdemand.loc[5,'MoM'] = demand_basin.loc[date_dict['today'],'AB'] - demand_basin.loc[date_dict['last_month'],'AB'] 
        dfdemand.loc[6,'MoM'] = demand_basin.loc[date_dict['today'],'MENA'] - demand_basin.loc[date_dict['last_month'],'MENA']
        dfdemand.loc[7,'MoM'] = demand_basin.loc[date_dict['today'],'JKTC'] - demand_basin.loc[date_dict['last_month'],'JKTC']
        dfdemand.loc[8,'MoM'] = demand_basin.loc[date_dict['today'],'MEIP'] - demand_basin.loc[date_dict['last_month'],'MEIP']
        dfdemand.loc[9,'MoM'] = demand_basin.loc[date_dict['today'],'EurDesk'] - demand_basin.loc[date_dict['last_month'],'EurDesk']
        #dfdemand.loc[10,'MoM'] = demand_basin.loc[date_dict['today'],'MedEur'] - demand_basin.loc[date_dict['last_month'],'MedEur']
        dfdemand.loc[10,'MoM'] = demand_basin.loc[date_dict['today'],'OtherEur'] - demand_basin.loc[date_dict['last_month'],'OtherEur']
        dfdemand.loc[11,'MoM'] = demand_basin.loc[date_dict['today'],'LatAm'] - demand_basin.loc[date_dict['last_month'],'LatAm']
        dfdemand.loc[12,'MoM'] = demand_basin.loc[date_dict['today'],'OtherRoW'] - demand_basin.loc[date_dict['last_month'],'OtherRoW']
        dfdemand.loc[3:12, 'MoM'] = dfdemand.loc[3:12, 'MoM'].astype('float').round(2)
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
        #dfdemand.loc[13,'Actual vs. Forecast'] = dfdemand.loc[13,'M'] - dfdemand.loc[13,' M ']
        dfdemand.loc[3:12, 'Actual vs. Forecast'] = dfdemand.loc[3:12, 'Actual vs. Forecast'].astype('float').round(2)
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
        #dfdemand.loc[13,'Actual vs. Forecast %'] = (dfdemand.loc[13,'M'] - dfdemand.loc[13,' M ']) /dfdemand.loc[13,'M']
        dfdemand.loc[3:12,'Actual vs. Forecast %'] = dfdemand.loc[3:12,'Actual vs. Forecast %'].map(lambda x:format(x,'.0%'))
        #Y-1 Kpler
        dfdemand.loc[3:,'Y-1'] = demand_basin.loc[date_dict['last_year'],'Global']
        dfdemand.loc[4:,'Y-1'] = demand_basin.loc[date_dict['last_year'],'PB']
        dfdemand.loc[5:,'Y-1'] = demand_basin.loc[date_dict['last_year'],'AB']
        dfdemand.loc[6:,'Y-1'] = demand_basin.loc[date_dict['last_year'],'MENA']
        dfdemand.loc[7,'Y-1'] = demand_basin.loc[date_dict['last_year'],'JKTC']
        dfdemand.loc[8,'Y-1'] = demand_basin.loc[date_dict['last_year'],'MEIP']
        dfdemand.loc[9,'Y-1'] = demand_basin.loc[date_dict['last_year'],'EurDesk']
        #dfdemand.loc[10,'Y-1'] = demand_basin.loc[date_dict['last_year'],'MedEur']
        dfdemand.loc[10,'Y-1'] = demand_basin.loc[date_dict['last_year'],'OtherEur']
        dfdemand.loc[11,'Y-1'] = demand_basin.loc[date_dict['last_year'],'LatAm']
        dfdemand.loc[12,'Y-1'] = demand_basin.loc[date_dict['last_year'],'OtherRoW']
        #D-1 Kpler
        dfdemand.loc[3:,'D-1'] = demand_basin.loc[date_dict['last_day'],'Global']
        dfdemand.loc[4:,'D-1'] = demand_basin.loc[date_dict['last_day'],'PB']
        dfdemand.loc[5:,'D-1'] = demand_basin.loc[date_dict['last_day'],'AB']
        dfdemand.loc[6:,'D-1'] = demand_basin.loc[date_dict['last_day'],'MENA']
        dfdemand.loc[7,'D-1'] = demand_basin.loc[date_dict['last_day'],'JKTC']
        dfdemand.loc[8,'D-1'] = demand_basin.loc[date_dict['last_day'],'MEIP']
        dfdemand.loc[9,'D-1'] = demand_basin.loc[date_dict['last_day'],'EurDesk']
        #dfdemand.loc[10,'D-1'] = demand_basin.loc[date_dict['last_day'],'MedEur']
        dfdemand.loc[10,'D-1'] = demand_basin.loc[date_dict['last_day'],'OtherEur']
        dfdemand.loc[11,'D-1'] = demand_basin.loc[date_dict['last_day'],'LatAm']
        dfdemand.loc[12,'D-1'] = demand_basin.loc[date_dict['last_day'],'OtherRoW']
        #D-7 Kpler
        dfdemand.loc[3:,'D-7'] = demand_basin.loc[date_dict['last_week'],'Global']
        dfdemand.loc[4:,'D-7'] = demand_basin.loc[date_dict['last_week'],'PB']
        dfdemand.loc[5:,'D-7'] = demand_basin.loc[date_dict['last_week'],'AB']
        dfdemand.loc[6:,'D-7'] = demand_basin.loc[date_dict['last_week'],'MENA']
        dfdemand.loc[7,'D-7'] = demand_basin.loc[date_dict['last_week'],'JKTC']
        dfdemand.loc[8,'D-7'] = demand_basin.loc[date_dict['last_week'],'MEIP']
        dfdemand.loc[9,'D-7'] = demand_basin.loc[date_dict['last_week'],'EurDesk']
        #dfdemand.loc[10,'D-7'] = demand_basin.loc[date_dict['last_week'],'MedEur']
        dfdemand.loc[10,'D-7'] = demand_basin.loc[date_dict['last_week'],'OtherEur']
        dfdemand.loc[11,'D-7'] = demand_basin.loc[date_dict['last_week'],'LatAm']
        dfdemand.loc[12,'D-7'] = demand_basin.loc[date_dict['last_week'],'OtherRoW']
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
        #dfdemand.loc[13,'DoD'] = dfdemand.loc[13,'M'] - dfdemand.loc[13,'D-1']
        dfdemand.loc[3:12, 'DoD'] = dfdemand.loc[3:12, 'DoD'].astype('float').round(2)
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
        #dfdemand.loc[13,'WoW'] = dfdemand.loc[13,'M'] - dfdemand.loc[13,'D-7']
        dfdemand.loc[3:12, 'WoW'] = dfdemand.loc[3:12, 'WoW'].astype('float').round(2)
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
        dfdatademand=dfdemand.loc[3:12]
        
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


#Terminal Anaysis tables
class AnalysisData():
    
    def supply_data():
        #read data from Kpler
        Kpler=DBTOPD('PRD-DB-SQL-211','LNG', 'dbo', 'KplerLNGTrades')
        dfkpler=Kpler.sql_to_df()

        #get supply and demand df
        supply_plant_columns=['DateOrigin','InstallationOrigin','VolumeOriginM3']
        df_supply_plant = dfkpler[supply_plant_columns]
        '''
        SupplyCurveId = pd.read_excel('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/DeskViewID.xlsx',header=(0),sheet_name='supply')
        #create supply list
        SupplyCurveId=SupplyCurveId[['CurveID','plant']]
        supply_plant_list=SupplyCurveId['plant'].values.tolist()
        '''
        curveid = DBTOPD.get_curve_id()
        dfcurveid = curveid.loc[:,['CurveId','Type','Location','Country']]
        #print(dfcurveid)
        SupplyCurveId = dfcurveid.loc[dfcurveid[dfcurveid['Type']=='Supply'].index]
        SupplyCurveId = SupplyCurveId[['CurveId','Country','Location']]
        SupplyCurveId.rename(columns={'CurveId':'CurveID','Country':'Country','Location':'plant'}, inplace=True)
        SupplyCurveId.loc[SupplyCurveId[SupplyCurveId['Country']=='Mozambique'].index, 'plant'] = 'Coral South FLNG' #rename Mozambique
        SupplyCurveId.loc[SupplyCurveId[SupplyCurveId['plant']=='Portovaya LNG'].index, 'plant'] = 'Portovaya'
        supply_plant_list=SupplyCurveId['plant'].values.tolist()
        #print(supply_plant_list)
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
            
        #print(dfsupply.columns)
        SupplyCategories=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'CategoriesSupply').sql_to_df()
        #DemandCategories=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'CategoriesDemand').sql_to_df()
        SupplyCategories.set_index('Plant', inplace=True)
        
        #get desk view
        desk=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'DeskSupplyPlant')
        dfsupplydesk=desk.sql_to_df()
        dfsupplydesk.set_index('Date', inplace=True)
        dfsupplydesk.sort_index(inplace=True)
        
        return dfsupply,SupplyCategories,dfsupplydesk
    
    def terminal_data(dfsupply):
        
        #capa= pd.read_csv('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/IHSCapa.csv',header=(0))
        #capa.set_index('Liquefaction Project', inplace=True)
        #print(capa)
        capa=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'IHScapa').sql_to_df()
        capa.set_index('index', inplace=True)
        capa.rename(columns={'Mozambique Coral FLNG':'Coral South FLNG','Portovaya LNG':'Portovaya','Greater Tortue LNG':'Greater Tortue FLNG'}, inplace=True)
        
        #print(capa)
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
            df.loc[i,'Capacity (Mt)'] = capa.loc[today,i]
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
        
        capa=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'IHScapa').sql_to_df()
        capa.set_index('index', inplace=True)
        capa.rename(columns={'Mozambique Coral FLNG':'Coral South FLNG','Portovaya LNG':'Portovaya','Greater Tortue LNG':'Greater Tortue FLNG'}, inplace=True)
        
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
            df.loc[i,'Capacity (Mt)'] = capa.loc[today,i]
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
      
        #print(dfchart)
        
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
        dfterminal.dropna(axis=1, inplace=True)

        
        
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
      #Kpler=DBTOPD('PRD-DB-SQL-211','LNG', 'dbo', 'KplerLNGTrades')
      #dfkpler=Kpler.sql_to_df()
      
      
      #get desk view
      desk=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'DeskDemand')
      dfdesk=desk.sql_to_df()
      dfdesk.set_index('Date', inplace=True)
      '''
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
          
      '''
      #DemandCategories = pd.read_excel('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/Categories.xlsx',header=(0),sheet_name='Index_Demand')
      #DemandCategories = DemandCategories.iloc[:64,0:6]
      #DemandCategories.set_index('Country', inplace=True)
      
      demand=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'DemandCountryHist')
      dfDemand=demand.sql_to_df()
      dfDemand.set_index('Date', inplace=True)
      dfDemand.sort_index(inplace=True)
      #print(dfdemand)
      #print('dfDemand', dfDemand)
      
      
      
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
       capa.drop(['Gibraltar','Senegal','Nicaragua','Hong Kong','El Salvador','Russia','Estonia','Antigua And Barbuda'],inplace=True) #delete no curve id
       
       othereur = ['Croatia', 'Cyprus', 'Finland', 'Greece', 'Lithuania', 'Malta', 'Norway', 'Portugal', 'Sweden', 'Turkey']
       eurdesk = ['Belgium', 'France', 'Germany', 'Italy', 'Netherlands', 'Poland', 'Spain', 'United Kingdom']
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
       #print(capa.index)
       for i in capa.index[0:]:#df.index:
           df.loc[i,'Avg. Loading 14 days (Mcm/d)']=dfDemandmarket[i].sum()/14  #Mcm/d
           df.loc[i,'Avg. Loading 15 days (Mcm/d)']=dfDemanddemand15[i].sum()/15
           df.loc[i,'1 year average (Mcm/d)'] = dfDemand.loc[start_date:today,i].sum()/365
           df.loc[i,'Capacity (Mt)'] = capa.loc[i,'Initial_Capacity']
           df.loc[i,'365 days Utilisation'] = (dfDemand.loc[start_date:today,i].copy().sum()/0.000612*0.000000438/df.loc[i,'Capacity (Mt)'].copy())
       df.loc['Eur Desk','365 days Utilisation'] = df.loc[eurdesk,'365 days Utilisation'].mean()
       df.loc['Other Eur','365 days Utilisation'] = df.loc[othereur,'365 days Utilisation'].mean()
       #print(df)
       #print(dfDemanddemand15[eurdesk].sum().sum()/15)
       df.loc['Eur Desk','Avg. Loading 15 days (Mcm/d)']=dfDemanddemand15[eurdesk].sum().sum()/15
       df.loc['Other Eur','Avg. Loading 15 days (Mcm/d)']=dfDemanddemand15[othereur].sum().sum()/15
       df.loc['Eur Desk','Capacity (Mt)'] = capa.loc[eurdesk,'Initial_Capacity'].sum()
       df.loc['Other Eur','Capacity (Mt)'] = capa.loc[othereur,'Initial_Capacity'].sum()
       df.loc['Eur Desk','Avg. Loading 14 days (Mcm/d)'] = dfDemandmarket[eurdesk].sum().sum()/14
       df.loc['Other Eur','Avg. Loading 14 days (Mcm/d)'] = dfDemandmarket[othereur].sum().sum()/14
       
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
       
       dfchart.loc[0:18,'Eur Desk'] = dfchart.loc[0:18, eurdesk].sum(axis=1).round(2)
       dfchart.loc[0:18,'Other Eur'] = dfchart.loc[0:18, othereur].sum(axis=1).round(2)
       dfchart.loc[[21,22,23,25,26],'Eur Desk'] = dfchart.loc[[21,22,23,25,26], eurdesk].sum(axis=1).round(2)
       dfchart.loc[[21,22,23,25,26],'Other Eur'] = dfchart.loc[[21,22,23,25,26], othereur].sum(axis=1).round(2)
       
       dfchart=dfchart.reindex(index=[19,20,28,0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,21,22,23,24,25,26,27])
       #print(dfchart.columns.tolist()[:-3])
       dfchart = dfchart[['index','Eur Desk','Other Eur']+dfchart.columns.tolist()[1:-2]]
       #print(dfchart)
       #dfchart.insert(1, 'Other Eur',dfchart[othereur].sum(axis=1))
       #dfchart.insert(1, 'Eur Desk',dfchart[eurdesk].sum(axis=1))
       #print(dfDemand.loc[start_date:today,eurdesk].sum(axis=1))
       #print(df.loc[0,eurdesk].sum())
       #print(dfchart)
       #dfchart.loc[20,'Eur Desk'] = (dfDemand.loc[start_date:today,eurdesk].sum(axis=1)/0.000612*0.000000438/df.loc[0,eurdesk].sum())
       #dfchart.loc[28,'Eur Desk'] = (dfDemanddemand15[eurdesk].sum()/15/df.loc[0,eurdesk].sum()).apply(lambda x:'{:.0%}'.format(x)) 

       #dfchart.loc[20,'Other Eur'] = (dfDemand.loc[start_date:today,othereur].sum(axis=1)/0.000612*0.000000438/df.loc[0,othereur].sum())
       #dfchart.loc[28,'Other Eur'] = (dfDemanddemand15[othereur].sum()/15/df.loc[0,othereur].sum()).apply(lambda x:'{:.0%}'.format(x))

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
#AnalysisData.demand_data()    
    
       


#Dash 
app = dash.Dash(__name__)

app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-content')
])

#index page
index_page = html.Div([
    dcc.Link('supply', href='/supply'),
    html.Br(),
    dcc.Link('demand', href='/demand'),
    html.Br(),
    dcc.Link('supply30', href='/supply30'),
    html.Br(),
    dcc.Link('demand30', href='/demand30'),
    html.Br(),
    dcc.Link('terminal_supply', href='/terminal_supply'),
    html.Br(),
    dcc.Link('terminal_demand', href='/terminal_demand'),
    html.Br(),
    dcc.Link('terminal_uti', href='/terminal_uti'),
    html.Br(),
    dcc.Link('terminal_cargo', href='/terminal_cargo'),
    html.Br(),
    dcc.Link('terminal_vol', href='/terminal_vol'),
    html.Br(),
    dcc.Link('model_delta_table', href='/model_delta_table'),
    html.Br(),
    dcc.Link('model_delta_chart', href='/model_delta_chart'),
    html.Br(),
    dcc.Link('charter_rate', href='/charter_rate'),
    html.Br(),
    dcc.Link('demandtemp', href='/demandtemp'),
    html.Br(),
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
        
            html.Button('Download Demand MA10 Table (need about 10s)', id='refrash'),
            dcc.Loading(id="loading-demand", 
                            children=[
            html.Div(id='body-div-demand'), ])       
        ])
        
        return demand_layout
    
    if pathname == '/supply30':
        
        supply_layout30 = html.Div([
        
        #html.Div([html.I("End of Day PnL")]), #page header
        #html.Hr(),
        
            html.Button('Download Supply MA30 Table (need about 10s)', id='refrash30'),
            dcc.Loading(id="loading-supply30", 
                            children=[
            html.Div(id='body-div-supply30'), ])       
        ])
        
        return supply_layout30
    
    if pathname == '/demand30':
        
        demand_layout30 = html.Div([
        
        #html.Div([html.I("End of Day PnL")]), #page header
        #html.Hr(),
        
            html.Button('Download Demand MA30 Table (need about 10s)', id='refrash30'),
            dcc.Loading(id="loading-demand30", 
                            children=[
            html.Div(id='body-div-demand30'), ])       
        ])
        
        return demand_layout30
    
    if pathname == '/terminal_supply':
        
        terminal_layout_supply = html.Div([
        
        #html.Div([html.I("End of Day PnL")]), #page header
        #html.Hr(),
        
            html.Button('Download Terminal Table (need about 60s)', id='refrash_ana_supply'),
            dcc.Loading(id="loading-terminal", 
                            children=[
            html.Div(id='body-div-terminal_supply'), ])       
        ])
        
        return terminal_layout_supply
    
    if pathname == '/terminal_demand':
        
        terminal_layout_demand = html.Div([
        
        #html.Div([html.I("End of Day PnL")]), #page header
        #html.Hr(),
        
            html.Button('Download Terminal Table (need about 60s)', id='refrash_ana_demand'),
            dcc.Loading(id="loading-terminal", 
                            children=[
            html.Div(id='body-div-terminal_demand'), ])       
        ])
        
        return terminal_layout_demand
    
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
    
     
    if pathname == '/model_delta_table':
        
        update_date = DBTOPD.get_model_run_date()
        option = []
        for i in update_date.loc[0:59,'ForecastDate'].to_list(): #59 runs covers y-1
            option.append({'label':str(i), 'value':str(i)})
        
        model_layout = html.Div([
                        
                        #tables
                        dcc.Dropdown(
                            options = option,
                            value = option[1],
                            id='model_run_date'
                            , style={'width': '48%'}),
                        
                        dcc.Loading(id="loading_table", 
                                        children=[
                        html.Div(id='body-div-table') ]),
                        html.Hr(),
                        
                        
        ])
        
        
        return model_layout
    
    if pathname == '/model_delta_chart':
        
        SupplyCategories, DemandCategories = model_delta_chart.dropdown_options()
            
        update_date = DBTOPD.get_model_run_date()
        option_chart = []
        for i in update_date.loc[0:59,'ForecastDate'].to_list():
            option_chart.append({'label':str(i), 'value':str(i)})
        
        model_chart_layout = html.Div([
                        
                        #charts
                        html.H3('Hist run charts'),
                        html.Div([
                        dcc.Dropdown(
                            options = SupplyCategories['Plant'].drop_duplicates().to_list(),
                            #value = "Select a plant",
                            placeholder="Select a supply plant",
                            id='model_chart_plant',
                            style={'width': '230px', 'display': 'inline-block'}
                            ),
                                                
                        dcc.Dropdown(
                            options = SupplyCategories['Market'].drop_duplicates().to_list(),
                            #value = "Select a country",
                            placeholder="Select a supply country",
                            id='model_chart_country',
                            style={'width': '230px', 'display': 'inline-block'}),
                        dcc.Dropdown(
                            options = ['Global','PB','AB','MENA'],
                            #value = "Select a Basin",
                            placeholder="Select a supply basin",
                            id='model_chart_Sbasin',
                            style={'width': '230px', 'display': 'inline-block'}),
                        
                        ],style={'width': '48%', 'display': 'inline-block'}
                            ),
                        #demand dropdwon
                        html.Div([
                        dcc.Dropdown(
                            options = DemandCategories['Country'].dropna().drop_duplicates().to_list(),
                            placeholder="Select a demand market",
                            id='model_chart_market'
                            , style={'width': '230px', 'display': 'inline-block'}),
                                                
                        dcc.Dropdown(
                            options = ['EurDesk', 'JKTC', 'MEIP', 'OtherEur', 'LatAm', 'OtherRoW'],
                            placeholder="Select a demand region",
                            id='model_chart_region'
                            , style={'width': '230px', 'display': 'inline-block'}),
                        dcc.Dropdown(
                            options = ['Global','PB','AB','MENA','EoS', 'WoS'],
                            placeholder="Select a demand basin",
                            id='model_chart_Dbasin'
                            , style={'width': '230px', 'display': 'inline-block'}),
                        ], style={'width': '48%', 'float': 'right', 'display': 'inline-block'}
                            ),
                        
                        dcc.Loading(id="loading_plant_chart", 
                                    type= 'graph',
                                        children=[
                        html.Div([dcc.Graph(id='body-div-plant-chart')],  style={'display': 'inline-block', 'width': '48%'},),
                        html.Div([dcc.Graph(id='body-div-market-chart')],  style={'display': 'inline-block', 'float': 'right','width': '48%'},)
                        
                        #html.Div(id='body-div-plant-chart')
                        ]),
                        html.H3('Hist run delta tables'),
                        html.Hr(),
                        html.Div([
                            html.H4('Hist run delta date'),
                            dcc.Dropdown(
                                options = option_chart,
                                value = option_chart[1],
                                id='model_run_date_chart_supply'
                                , style={'width': '48%', 'display': 'inline-block'}),
                            ],style={'width': '48%', 'display': 'inline-block'}),
                        html.Div([
                            html.H4('Hist run delta date'),
                            dcc.Dropdown(
                                options = option_chart,
                                value = option_chart[1],
                                id='model_run_date_chart_demand'
                                , style={'width': '48%', 'display': 'inline-block'}),
                            ],style={'width': '48%', 'display': 'inline-block'}),
                        dcc.Loading(id="loading_table", 
                                        children=[
                        html.Div(id='body-div-chart-supplydeltatable',
                                 style={'width': '48%', 'display': 'inline-block'}),
                        html.Div(id='body-div-chart-demanddeltatable',
                                 style={'width': '48%', 'float': 'right','display': 'inline-block'})
                        ]),
                        
        ])
        
        
        return model_chart_layout
    

    
    if pathname == '/charter_rate':
        
        today = datetime.date.today()
        radiolist = []
        daterange = pd.date_range(start = str(today.year)+'-'+str(today.month)+'-01', end=str(today.year+1)+'-12-01', freq='MS').date #end date
        for i in daterange:
            radiolist.append(
                html.Div([
                                            str(i),
                                            dcc.RadioItems(
                                                id="'"+str(i)+"'",
                                                options=[{'label': option, 'value': option} for option in ['No US', 'US Cont','US 50%']],
                                                value='No US',
                                                labelStyle={'display': 'inline-block'}
                                            )
                                        ], style={'width': '4%', 'display': 'inline-block'}),
                )
        
        sen_layout = html.Div([
                                html.Div(radiolist
                            
                                    ),
                            
                                dcc.Graph(id='Sensitivity'),
                                dcc.Graph(id='test') 
                            
                            ])
        return sen_layout
    
    
    if pathname == '/demandtemp':
        
        dt_chart_layout = html.Div([
                        
                        #charts
                        html.H3('Total Gas Demand Delta vs Temperature'),
                        html.Div([
                            #dropdown select country China, Japan, SK   
                            html.Tr(['Select a country and start year']),
                            #html.H3(['Select a country']),
                            dcc.Dropdown(
                                options = ['China','Japan','South Korea'],
                                #value = "Select a country",
                                placeholder="Select a country",
                                id='dt_select_country',
                                style={'width': '230px', 'display': 'inline-block'}
                                ),
                            
                            
                            #dropdowm selecct start year, default 2012, 
                            #html.P(['Select a year']),
                            #html.H3(['Select a country']),
                            dcc.Dropdown(
                                options = list(range(1980,2023)),
                                value = 2012,
                                placeholder="Select start year",
                                id='dt_select_start_year',
                                style={'width': '230px', 'display': 'inline-block'}
                                ),
                            ],style={'width': '99%', 'display': 'inline-block'}),
                        html.Br(),
                        html.Hr(),#new row
                        #dropdown monthly temp normal or cell input base temp
                        html.Div([
                            html.Tr(['Select a month normal Temperature or input base temperature']),
                            dcc.Dropdown(
                                #options = ,
                                value = 'None',
                                placeholder="Select Normal Temperature",
                                id='dt_select_tempnormal',
                                style={'width': '230px', 'display': 'inline-block'}
                                ),
                            #html.Tr(['or input base temperature']),
                            dcc.Input(id="base_temp",value='None', type="number", placeholder="Input Temperature"),
                            ],style={'width': '99%', 'display': 'inline-block'}),
                        html.Br(),
                        html.Hr(),#new row
                        #cell input temp change or cell input how many std of months
                        html.Div([
                            html.Tr(['Input Temperature Scenario']),
                            dcc.Input(id="temp_scenario", value=0, type="number", placeholder="Input Temperature Scenario"),
                            html.Tr(['or']),
                            html.Tr(['Select std of month and input multiplier']),
                            
                            #html.Tr(['x']),
                            dcc.Dropdown(
                                #options = ,
                                value = 'None',
                                placeholder="Select Temperature std",
                                id='dt_select_tempstd',
                                style={'width': '230px', 'display': 'inline-block'}
                                ),
                            dcc.Input(id="std_multi",value=1, type="number", placeholder="Input Multi std"),
                             ],style={'width': '99%', 'display': 'inline-block'}),
                        #result table
                        html.Br(),
                        html.Div([
                                    html.Table([
                                    html.Tr([html.Td(['Country: ']), html.Td(id='dtselectedcountry')]),
                                    html.Tr([html.Td(['Base Temperature: ']), html.Td(id='dtbasetemp')]),
                                    html.Tr([html.Td(['Temperature Scenario: ']), html.Td(id='dttempsce')]),
                                    html.Tr([html.Td(['Gas demand Mcm/d: ']), html.Td(id='dtdemand')]),
                                            ]),
                                ]),
                                
                        html.Div([
                        # scatter chart       
                        dcc.Loading(id="loading_dt_chart", 
                                    type= 'graph',
                                        children=[
                                            html.Div([dcc.Graph(id='body-div-dt-chart')]),
                        ]),
                        
                        ],style={'display': 'inline-block'}),
                        #scenario table
                        dcc.Loading(id="loading_dt_table", 
                                    type= 'graph',
                                        children=[
                                            html.Div([dcc.Graph(id='body-div-dt-table'), ],style={ 'height': '1500vh'})
                        ]),
                        
                        
                   
                        ])
        
        
        return dt_chart_layout
    
    
    else:
        return index_page

#update MA10 layout
@app.callback(
    Output(component_id='body-div-supply', component_property='children'),
    #Output(component_id='body-div-demand', component_property='children'),
    Input(component_id='refrash', component_property='n_clicks'),
    #Input(component_id='refrash_supply', component_property='n_clicks')
)
def update_output_supply10(n_clicks):
    
    if n_clicks is None:
        
        raise PreventUpdate
    
    else:
        #print(1)
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
def update_output_demand10(n_clicks1):
    
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
def update_output_supply30(n_clicks3):
    
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
def update_output_demand30(n_clicks4):
    
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


#terminal supply
@app.callback(
    Output(component_id='body-div-terminal_supply', component_property='children'),
    Input(component_id='refrash_ana_supply', component_property='n_clicks'),
)
def update_output_ana_supply(n_clicks):
    
    if n_clicks is None:
        
        raise PreventUpdate
    
    else:

        dfterminal, dfterminalmcm, column, dfutiall, dfutiallP, dfcargo, dfvol, dfDemand,dfdemandcountry,column_demand = AnalysisData.get_new_data()
        
        terminal_table_layout_supply = html.Div([
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
        ),])
        
        
        return terminal_table_layout_supply

#terminal demand
@app.callback(
    Output(component_id='body-div-terminal_demand', component_property='children'),
    Input(component_id='refrash_ana_demand', component_property='n_clicks'),
)
def update_output_ana_demand(n_clicks):
    
    if n_clicks is None:
        
        raise PreventUpdate
    
    else:

        if n_clicks is None:
        
            raise PreventUpdate
    
        else:

            dfterminal, dfterminalmcm, column, dfutiall, dfutiallP, dfcargo, dfvol, dfDemand,dfdemandcountry,column_demand = AnalysisData.get_new_data()
            
            terminal_table_layout_demand = html.Div([
            #html.H1('Data updated at: '+ datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')),       
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

        
        return terminal_table_layout_demand
    
#uti    
@app.callback(
    Output(component_id='body-div-uti', component_property='children'),
    Input(component_id='refrash_uti', component_property='n_clicks'),
)
def update_output_uti(n_clicks):
    
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
def update_output_cargo(n_clicks):
    
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
def update_output_vol(n_clicks):
    
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
 

#model delta table and charts

@app.callback(
    Output(component_id='body-div-table', component_property='children'),
    Input(component_id='model_run_date', component_property='value'),
)
def update_model_delta(rundate):
    
    print(rundate)
    update_date = DBTOPD.get_model_run_date()
    latest_date = update_date.loc[0,'ForecastDate']
    
    desk_plant_latest, desk_country_latest, desk_supplybasin_latest, desk_supply_hist_plant, desk_supply_hist_country, desk_market_latest, desk_demandbasin_latest, desk_demand_hist, dfcurvefull, usfeedgas, capa, jkctemp = model_delta.get_data()
    start_date = desk_demandbasin_latest['Date'].iloc[0]
    end_date = desk_demandbasin_latest['Date'].iloc[-1]
    #rundate = '2022-08-31 00:00:00'
    #rundate = '2022-11-15 00:00:00'
    #rundate = '2022-11-08 00:00:00'
    #rundate={'label': '2022-11-08 00:00:00', 'value': '2022-11-08 00:00:00'}
    
    if type(rundate) == dict:
        rundate = rundate['value']
    
    #dfdeskplanthistfull = model_delta.hist_data_supply_plant(rundate, desk_supply_hist_plant, start_date, end_date)
    dfdeskplanthistfull = model_delta.hist_data_supply_plant(rundate, desk_supply_hist_plant, start_date, end_date)

    #print(dfdeskplanthistfull)
    dfdeskcountryhistfull = model_delta.hist_data_supply_country(rundate, desk_supply_hist_country, start_date, end_date)
    #print(dfdeskcountryhistfull)
    dfdeskmarkethistfull = model_delta.hist_data_demand(rundate, desk_demand_hist, start_date, end_date)
    #print(dfdeskmarkethistfull)
    dfmodelstd = model_delta.jkcdemand(jkctemp, start_date, end_date)

    df_supply_sum, df_demand_sum, df_supply_sum_pre, df_demand_sum_pre, df_supply_delta, df_demand_delta, df_plant_latest, df_market_latest, df_plant_delta, df_market_delta = model_delta.table_data(desk_plant_latest, desk_country_latest, desk_supplybasin_latest, dfdeskplanthistfull, dfdeskcountryhistfull, desk_market_latest, desk_demandbasin_latest, dfdeskmarkethistfull)
    #dfplant = model_delta.chart_plant_data('Yamal', desk_supply_hist_plant, start_date, end_date)
    #dfmarket = model_delta.chart_market_data('China', desk_demand_hist, start_date, end_date)
    dfsumtable = model_delta.sum_table(dfcurvefull, desk_country_latest, desk_supply_hist_country, desk_demand_hist, desk_demandbasin_latest, usfeedgas, capa, dfmodelstd, start_date, end_date)
    dfsumtable.reset_index(inplace=True)
    hdlistsum = []
    for i in dfsumtable.columns:
        hd={"name":i,"id": i,'deletable' : True}
        hdlistsum.append(hd)
    hdlistsum[0]['name'] = ' '
    #set for table
    #latest supply
    df_supply_sum.reset_index(inplace=True)
    hdlistsupply = []
    for i in df_supply_sum.columns:
        hd={"name":i,"id": i,'deletable' : True}
        hdlistsupply.append(hd)
    hdlistsupply[0]['name'] = 'Supply'
    #latest demand
    df_demand_sum.reset_index(inplace=True)
    hdlistdemand = []
    for i in df_demand_sum.columns:
        hd={"name":i,"id": i,'deletable' : True}
        hdlistdemand.append(hd)
    hdlistdemand[0]['name'] = 'Demand'
    
    #pre supply
    df_supply_sum_pre.reset_index(inplace=True)
    #pre demand
    df_demand_sum_pre.reset_index(inplace=True)
    #supply delta
    df_supply_delta.reset_index(inplace=True)
    df_supply_delta = df_supply_delta.round(1)
    #demand delta
    df_demand_delta.reset_index(inplace=True)
    df_demand_delta = df_demand_delta.round(1)
    #plant latest
    df_plant_latest.reset_index(inplace=True)
    #market latest
    df_market_latest.reset_index(inplace=True)
    #plant dalta
    df_plant_delta.reset_index(inplace=True)
    #market delta
    df_market_delta.reset_index(inplace=True)
    
    #print(df_supply_delta)
    #print(df_demand_delta)
    #layout
    model_delta_layout = html.Div([
            html.H3('Summary Table'),
            html.Hr(),
            
            dash_table.DataTable(
                
                id='tablesummary',
                columns = hdlistsum,
                data = dfsumtable.to_dict('records'),
                #row_deletable=True,
        
                style_cell={
                    'whiteSpace': 'normal',
                    'height': 'auto',
                    'textAlign': 'center'
                },
                
                 style_data={
                        'color': 'black',
                        'backgroundColor': '#F2F2F2'
                    },
                style_header={
                    'backgroundColor': '#F2F2F2',
                    #'fontWeight': 'bold',
                    'textAlign': 'center',
                    #'border': '3px solid black'
                    #'color': 'white'
                },
                
                style_data_conditional=(
                    [{
                        'if': {
                            'row_index': [5,10,18],
                            'filter_query': '{{{}}} >= 10'.format(col),
                            'column_id': col
                        },
                        'backgroundColor': '#F2F2F2',
                        'color': 'blue'
                    } for col in dfsumtable.columns]
                    
                    +
                    
                    [{
                        'if': {
                            'row_index': [5,10,18],
                            'filter_query': '{{{}}} <= -10'.format(col),
                            'column_id': col
                        },
                        'backgroundColor': '#F2F2F2',
                        'color': 'red'
                    } for col in dfsumtable.columns]
                    
                    +
                    
                    [{
                        'if': {
                            'row_index': [5,10,18],
                            'filter_query': '{{{}}} > 0'.format(col),
                            'column_id': col
                        },
        
                        'color': 'blue'
                    } for col in dfsumtable.columns]
                    
                    +
                    
                    [{
                        'if': {
                            'row_index': [5,10,18],
                            'filter_query': '{{{}}} < 0'.format(col),
                            'column_id': col
                        },
                        
                        'color': 'red'
                    } for col in dfsumtable.columns]
                    
                    
                ),
                style_as_list_view=True,
            ),
            html.H3('Latest run : '+ latest_date.strftime('%Y-%m-%d %H:%M:%S')),
            html.Hr(),
            dash_table.DataTable(
                
                id='table1supply',
                columns = hdlistsupply,
                #data=dfsupply.loc[3:].to_dict('records'),
                data = df_supply_sum.to_dict('records'),
                #row_deletable=True,
        
                style_cell={
                    'whiteSpace': 'normal',
                    'height': 'auto',
                    'textAlign': 'center'
                },
                
                 style_data={
                        'color': 'black',
                        'backgroundColor': '#92D050'
                    },
                style_header={
                    'backgroundColor': '#D9D9D9',
                    #'fontWeight': 'bold',
                    'textAlign': 'center',
                    #'border': '3px solid black'
                    #'color': 'white'
                },
                
                style_data_conditional=[
                    {
                        'if': {'row_index': 2},
                        'color': 'white',
                    }
                ],
                style_as_list_view=True,
            ),
            
            dash_table.DataTable(
                
                id='table1demand',
                columns = hdlistdemand,
                #data=dfsupply.loc[3:].to_dict('records'),
                data = df_demand_sum.to_dict('records'),
                #row_deletable=True,
        
                style_cell={
                    'whiteSpace': 'normal',
                    'height': 'auto',
                    'textAlign': 'center'
                },
                
                 style_data={
                        'color': 'black',
                        'backgroundColor': 'red'
                    },
                style_header={
                    'backgroundColor': '#D9D9D9',
                    #'fontWeight': 'bold',
                    'textAlign': 'center',
                    #'border': '3px solid black'
                    #'color': 'white'
                },
                style_data_conditional=[
                    {
                        'if': {'row_index': 7}, #Eur Desk
                        'color': 'white',
                    }
                ],
                
                style_as_list_view=True,
            ),
            
            html.H3('Previous run : '+ rundate),
            html.Hr(),
            dash_table.DataTable(
                
                id='table2supply',
                columns = hdlistsupply,
                #data=dfsupply.loc[3:].to_dict('records'),
                data = df_supply_sum_pre.to_dict('records'),
                #row_deletable=True,
        
                style_cell={
                    'whiteSpace': 'normal',
                    'height': 'auto',
                    'textAlign': 'center'
                },
                
                 style_data={
                        'color': 'black',
                        'backgroundColor': '#92D050'
                    },
                style_header={
                    'backgroundColor': '#D9D9D9',
                    #'fontWeight': 'bold',
                    'textAlign': 'center',
                    #'border': '3px solid black'
                    #'color': 'white'
                },
                style_data_conditional=[
                    {
                        'if': {'row_index': 2},
                        'color': 'white',
                    }
                ],
                
                #style_data_conditional=stylessupplyup,
                style_as_list_view=True,
            ),
            
            dash_table.DataTable(
                
                id='table2demand',
                columns = hdlistdemand,
                #data=dfsupply.loc[3:].to_dict('records'),
                data = df_demand_sum_pre.to_dict('records'),
                #row_deletable=True,
        
                style_cell={
                    'whiteSpace': 'normal',
                    'height': 'auto',
                    'textAlign': 'center'
                },
                
                 style_data={
                        'color': 'black',
                        'backgroundColor': 'red'
                    },
                style_header={
                    'backgroundColor': '#D9D9D9',
                    #'fontWeight': 'bold',
                    'textAlign': 'center',
                    #'border': '3px solid black'
                    #'color': 'white'
                },
                
                style_data_conditional=[
                    {
                        'if': {'row_index': 7}, #Eur Desk
                        'color': 'white',
                    }
                ],
                
                #style_data_conditional=stylessupplyup,
                style_as_list_view=True,
            ),
            
            html.H3('Delta'),
            html.Hr(),
            dash_table.DataTable(
                
                id='table3supply',
                columns = hdlistsupply,
                data = df_supply_delta.to_dict('records'),
                style_cell={
                    'whiteSpace': 'normal',
                    'height': 'auto',
                    'textAlign': 'center'
                },
                
                 style_data={
                        'color': 'black',
                        'backgroundColor': '#D9D9D9'
                    },
                style_header={
                    'backgroundColor': '#D9D9D9',
                    #'fontWeight': 'bold',
                    'textAlign': 'center',
                    #'border': '3px solid black'
                    #'color': 'white'
                },
                
                style_data_conditional=(
                    [{
                        'if': {
                            'filter_query': '{{{}}} >= 10'.format(col),
                            'column_id': col
                        },
                        'backgroundColor': '#98FB98',
                        'color': 'blue'
                    } for col in df_supply_sum.columns]
                    
                    +
                    
                    [{
                        'if': {
                            'filter_query': '{{{}}} <= -10'.format(col),
                            'column_id': col
                        },
                        'backgroundColor': '#FFB6C1',
                        'color': 'red'
                    } for col in df_supply_sum.columns]
                    
                    +
                    
                    [{
                        'if': {
                            'filter_query': '{{{}}} > 0'.format(col),
                            'column_id': col
                        },
        
                        'color': 'blue'
                    } for col in df_supply_sum.columns]
                    
                    +
                    
                    [{
                        'if': {
                            'filter_query': '{{{}}} < 0'.format(col),
                            'column_id': col
                        },
                        
                        'color': 'red'
                    } for col in df_supply_sum.columns]
                    
                    
                ),
                
                #style_data_conditional=stylessupplyup,
                style_as_list_view=True,
            ),
            html.Hr(),
            dash_table.DataTable(
                
                id='table3demand',
                columns = hdlistdemand,
                #data=dfsupply.loc[3:].to_dict('records'),
                data = df_demand_delta.to_dict('records'),
                #row_deletable=True,
        
                style_cell={
                    'whiteSpace': 'normal',
                    'height': 'auto',
                    'textAlign': 'center'
                },
                
                style_data={
                        'color': 'black',
                        'backgroundColor': '#D9D9D9'
                    },
                style_header={
                    'backgroundColor': '#D9D9D9',
                    #'fontWeight': 'bold',
                    'textAlign': 'center',
                    #'border': '3px solid black'
                    #'color': 'white'
                },
                
                style_data_conditional=(
                    [{
                        'if': {
                            'filter_query': '{{{}}} >= 10'.format(col),
                            'column_id': col
                        },
                        'backgroundColor': '#98FB98',
                        'color': 'blue'
                    } for col in df_supply_sum.columns]
                    
                    +
                    
                    [{
                        'if': {
                            'filter_query': '{{{}}} <= -10'.format(col),
                            'column_id': col
                        },
                        'backgroundColor': '#FFB6C1',
                        'color': 'red'
                    } for col in df_supply_sum.columns]
                    +
                    
                    [{
                        'if': {
                            'filter_query': '{{{}}} > 0'.format(col),
                            'column_id': col
                        },
        
                        'color': 'blue'
                    } for col in df_supply_sum.columns]
                    
                    +
                    
                    [{
                        'if': {
                            'filter_query': '{{{}}} < 0'.format(col),
                            'column_id': col
                        },
                        
                        'color': 'red'
                    } for col in df_supply_sum.columns]
                    
                ),
                style_as_list_view=True,
            ),
            
            html.H3('Plant Latest run : '+ latest_date.strftime('%Y-%m-%d %H:%M:%S')),
            html.Hr(),
            dash_table.DataTable(
                
                id='table4supply',
                columns = hdlistsupply,
                #data=dfsupply.loc[3:].to_dict('records'),
                data = df_plant_latest.to_dict('records'),
                #row_deletable=True,
        
                style_cell={
                    'whiteSpace': 'normal',
                    'height': 'auto',
                    'textAlign': 'center'
                },
                
                 style_data={
                        'color': 'black',
                        'backgroundColor': '#92D050'
                    },
                style_header={
                    'backgroundColor': '#D9D9D9',
                    #'fontWeight': 'bold',
                    'textAlign': 'center',
                    #'border': '3px solid black'
                    #'color': 'white'
                },
                
                
                #style_data_conditional=stylessupplyup,
                style_as_list_view=True,
            ),
            
            html.H3('Market Latest run : '+ latest_date.strftime('%Y-%m-%d %H:%M:%S')),
            html.Hr(),
            dash_table.DataTable(
                
                id='table5demand',
                columns = hdlistsupply,
                #data=dfsupply.loc[3:].to_dict('records'),
                data = df_market_latest.to_dict('records'),
                #row_deletable=True,
        
                style_cell={
                    'whiteSpace': 'normal',
                    'height': 'auto',
                    'textAlign': 'center'
                },
                
                 style_data={
                        'color': 'black',
                        'backgroundColor': 'red'
                    },
                style_header={
                    'backgroundColor': '#D9D9D9',
                    #'fontWeight': 'bold',
                    'textAlign': 'center',
                    #'border': '3px solid black'
                    #'color': 'white'
                },
                
                
                #style_data_conditional=stylessupplyup,
                style_as_list_view=True,
            ),
            
            html.H3('Plant delta'),
            html.Hr(),
            dash_table.DataTable(
                
                id='table6supply',
                columns = hdlistsupply,
                data = df_plant_delta.to_dict('records'),
                style_cell={
                    'whiteSpace': 'normal',
                    'height': 'auto',
                    'textAlign': 'center'
                },
                
                 style_data={
                        'color': 'black',
                        'backgroundColor': '#D9D9D9'
                    },
                style_header={
                    'backgroundColor': '#D9D9D9',
                    #'fontWeight': 'bold',
                    'textAlign': 'center',
                    #'border': '3px solid black'
                    #'color': 'white'
                },
                
                style_data_conditional=(
                    [{
                        'if': {
                            'filter_query': '{{{}}} > 0'.format(col),
                            'column_id': col
                        },
                        'backgroundColor': '#98FB98',
                        'color': 'blue'
                    } for col in df_plant_delta.columns]
                    
                    +
                    
                    [{
                        'if': {
                            'filter_query': '{{{}}} < 0'.format(col),
                            'column_id': col
                        },
                        'backgroundColor': '#FFB6C1',
                        'color': 'red'
                    } for col in df_plant_delta.columns]
                    
                    +
                    
                    [{
                        'if': {
                            'filter_query': '{{{}}} > 0'.format(col),
                            'column_id': col
                        },
        
                        'color': 'blue'
                    } for col in df_plant_delta.columns]
                    
                    +
                    
                    [{
                        'if': {
                            'filter_query': '{{{}}} < 0'.format(col),
                            'column_id': col
                        },
                        
                        'color': 'red'
                    } for col in df_plant_delta.columns]
                    
                    
                ),
                
                #style_data_conditional=stylessupplyup,
                style_as_list_view=True,
            ),
            html.H3('Market delta'),
            html.Hr(),
            dash_table.DataTable(
                
                id='table7demand',
                columns = hdlistdemand,
                #data=dfsupply.loc[3:].to_dict('records'),
                data = df_market_delta.to_dict('records'),
                #row_deletable=True,
        
                style_cell={
                    'whiteSpace': 'normal',
                    'height': 'auto',
                    'textAlign': 'center'
                },
                
                style_data={
                        'color': 'black',
                        'backgroundColor': '#D9D9D9'
                    },
                style_header={
                    'backgroundColor': '#D9D9D9',
                    #'fontWeight': 'bold',
                    'textAlign': 'center',
                    #'border': '3px solid black'
                    #'color': 'white'
                },
                
                style_data_conditional=(
                    [{
                        'if': {
                            'filter_query': '{{{}}} > 0'.format(col),
                            'column_id': col
                        },
                        'backgroundColor': '#98FB98',
                        'color': 'blue'
                    } for col in df_supply_sum.columns]
                    
                    +
                    
                    [{
                        'if': {
                            'filter_query': '{{{}}} < 0'.format(col),
                            'column_id': col
                        },
                        'backgroundColor': '#FFB6C1',
                        'color': 'red'
                    } for col in df_supply_sum.columns]
                    +
                    
                    [{
                        'if': {
                            'filter_query': '{{{}}} > 0'.format(col),
                            'column_id': col
                        },
        
                        'color': 'blue'
                    } for col in df_supply_sum.columns]
                    
                    +
                    
                    [{
                        'if': {
                            'filter_query': '{{{}}} < 0'.format(col),
                            'column_id': col
                        },
                        
                        'color': 'red'
                    } for col in df_supply_sum.columns]
                    
                ),
                style_as_list_view=True,
            ),
            
            
            
            ])
            
    return  model_delta_layout    
    

#set up for supply dropdown only output one value
@app.callback(Output(component_id='model_chart_plant', component_property='value'),
              Output(component_id='model_chart_country', component_property='value'),
              Output(component_id='model_chart_Sbasin', component_property='value'),
              Input(component_id='model_chart_plant', component_property='value'),
              Input(component_id='model_chart_country', component_property='value'),
              Input(component_id='model_chart_Sbasin', component_property='value'),)
def display_supply_dropdown(plant_dropdown, country_dropdown, Sbasin_dropdown):
    
    dropdown_id = dash.callback_context.triggered[0]['prop_id']# if not None else 'No clicks yet'
    #print(dropdown_id)

    if dropdown_id == 'model_chart_plant.value':
        
        new_plant = plant_dropdown
        new_country = 'None'#"Select a country"
        new_basin = 'None'#'Select a Basin'
        
    elif dropdown_id == 'model_chart_country.value':
        
        new_plant = 'None'#'Select a plant'
        new_country = country_dropdown
        new_basin = 'None'#'Select a Basin'
        
    elif dropdown_id == 'model_chart_Sbasin.value':
        
        new_plant = 'None'#'Select a plant'
        new_country = 'None'#"Select a country"
        new_basin = Sbasin_dropdown
    else:
        
        raise PreventUpdate
    

    return new_plant, new_country, new_basin


#set up for demand dropdown only output one value
@app.callback(Output(component_id='model_chart_market', component_property='value'),
              Output(component_id='model_chart_region', component_property='value'),
              Output(component_id='model_chart_Dbasin', component_property='value'),
              Input(component_id='model_chart_market', component_property='value'),
              Input(component_id='model_chart_region', component_property='value'),
              Input(component_id='model_chart_Dbasin', component_property='value'),)
def display_demand_dropdown(market_dropdown, region_dropdown, Dbasin_dropdown):
    
    dropdown_id = dash.callback_context.triggered[0]['prop_id']# if not None else 'No clicks yet'
    #print(dropdown_id)

    if dropdown_id == 'model_chart_market.value':
        
        new_market = market_dropdown
        new_region = 'None'#"Select a country"
        new_Dbasin = 'None'#'Select a Basin'
        
    elif dropdown_id == 'model_chart_region.value':
        
        new_market = 'None'#'Select a plant'
        new_region = region_dropdown
        new_Dbasin = 'None'#'Select a Basin'
        
    elif dropdown_id == 'model_chart_Dbasin.value':
        
        new_market = 'None'#'Select a plant'
        new_region = 'None'#"Select a country"
        new_Dbasin = Dbasin_dropdown
        
    else:
        
        raise PreventUpdate

    return new_market, new_region, new_Dbasin

    
@app.callback(
    Output(component_id='body-div-plant-chart', component_property='figure'),
    Input(component_id='model_chart_plant', component_property='value'),
    Input(component_id='model_chart_country', component_property='value'),
    Input(component_id='model_chart_Sbasin', component_property='value'),
)
def update_plant_chart(plant, country, Sbasin):
    
    option = [plant, country, Sbasin]
    option = [x for x in option if str(x) != 'None']
    #print(option)
    desk_plant_latest, desk_country_latest, desk_supplybasin_latest, desk_supply_hist_plant, desk_supply_hist_country, desk_market_latest, desk_demandbasin_latest, desk_demand_hist, dfdemand, dfsupplyplant,ma30supplycountry, ma30supplybasin, ma30supplyplant,ma30demandcountry, ma30demandbasin = model_delta_chart.get_data()
    start_date = desk_demandbasin_latest['Date'].iloc[0]
    end_date = desk_demandbasin_latest['Date'].iloc[-1]
    
    dfplant = model_delta_chart.chart_plant_data(option[0], desk_supply_hist_plant, start_date, end_date)
    dfsupplyrange = model_delta_chart.minmaxsupply(dfsupplyplant, start_date, end_date)
    dfsupply30, dfdemand30 = model_delta_chart.ma30data(ma30supplycountry, ma30supplybasin, ma30supplyplant,ma30demandcountry, ma30demandbasin)
    color = colorlover.scales[str(10)]['div']['RdYlGn']  
   
    fig_plant = go.Figure()
    for i in dfplant.columns:
        fig_plant.add_trace(go.Scatter(x=dfplant.index, y=dfplant[i],
                            mode='lines',
                            name=i,
                            line=dict(color=color[dfplant.columns.to_list().index(i)])
                            ))
    fig_plant.add_trace(
                go.Scatter(
                    x=dfsupplyrange.index,
                    y=dfsupplyrange[option[0]+' min'],
                    fill='tonexty',
                    fillcolor='rgba(65,105,225,0)',
                    line_color='rgba(65,105,225,0)',
                    showlegend=False,
                    name='5 Years'
                    )
                )
            
    fig_plant.add_trace(
        go.Scatter(
            x=dfsupplyrange.index,
            y=dfsupplyrange[option[0]+' max'],
            fill='tonexty',
            fillcolor='rgba(65,105,225,0.1)',
            line_color='rgba(65,105,225,0)',
            showlegend=True,
            name='5 Years'
            )
        )
             
    fig_plant.add_trace(
        go.Scatter(
            x=dfplant.index,
            y=dfsupply30.loc[dfplant.index[0]:,option[0]],
            line=dict(color='black', dash='dot'),
            showlegend=True,
            name='Kpler Actual'
            )
        )
    fig_plant.update_layout(
                 autosize=True,
                 showlegend=True,
                 #colorscale='RdBu',
                 legend=dict(x=0, y=-0.2),
                 legend_orientation="h",
                 title_text= option[0] +' past 7 runs and 3M, 6M, 1Y ',
                 #xaxis = dict(dtick="M2"),
                 #xaxis_tickformat = '%B',
                 hovermode='x unified',
                 plot_bgcolor = 'white',
                 template='ggplot2'  ,
                 )
    
    return fig_plant
    
    #py.plot(fig_plant, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/Yamal scenarios.html', auto


@app.callback(
    Output(component_id='body-div-market-chart', component_property='figure'),
    Input(component_id='model_chart_market', component_property='value'),
    Input(component_id='model_chart_region', component_property='value'),
    Input(component_id='model_chart_Dbasin', component_property='value'),
)
def update_market_chart(market, region, Dbasin):
    
    option = [market, region, Dbasin]
    option = [x for x in option if str(x) != 'None']
    
    desk_plant_latest, desk_country_latest, desk_supplybasin_latest, desk_supply_hist_plant, desk_supply_hist_country, desk_market_latest, desk_demandbasin_latest, desk_demand_hist, dfdemand, dfsupplyplant,ma30supplycountry, ma30supplybasin, ma30supplyplant,ma30demandcountry, ma30demandbasin = model_delta_chart.get_data()
    start_date = desk_demandbasin_latest['Date'].iloc[0]
    end_date = desk_demandbasin_latest['Date'].iloc[-1]
    
    color = colorlover.scales[str(10)]['div']['RdYlGn']  
   
    dfmarket = model_delta_chart.chart_market_data(option[0], desk_demand_hist, start_date, end_date)
    dfdemandrange = model_delta_chart.minmaxdemand(dfdemand, start_date, end_date)
    dfsupply30, dfdemand30 = model_delta_chart.ma30data(ma30supplycountry, ma30supplybasin, ma30supplyplant,ma30demandcountry, ma30demandbasin)

    fig_market = go.Figure()
    for i in dfmarket.columns:
        fig_market.add_trace(go.Scatter(x=dfmarket.index, y=dfmarket[i],
                            mode='lines',
                            name=i,
                            line=dict(color=color[dfmarket.columns.to_list().index(i)])
                            ))
    fig_market.add_trace(
                go.Scatter(
                    x=dfdemandrange.index,
                    y=dfdemandrange[option[0]+' min'],
                    fill='tonexty',
                    fillcolor='rgba(65,105,225,0)',
                    line_color='rgba(65,105,225,0)',
                    showlegend=False,
                    name='5 Years'
                    )
                )
            
    fig_market.add_trace(
        go.Scatter(
            x=dfdemandrange.index,
            y=dfdemandrange[option[0]+' max'],
            fill='tonexty',
            fillcolor='rgba(65,105,225,0.1)',
            line_color='rgba(65,105,225,0)',
            showlegend=True,
            name='5 Years'
            )
        )
    
    fig_market.add_trace(
        go.Scatter(
            x=dfmarket.index,
            y=dfdemand30.loc[dfmarket.index[0]:,option[0]],
            line=dict(color='black', dash='dot'),
            showlegend=True,
            name='Kpler Actual'
            )
        )
    fig_market.update_layout(
                 autosize=True,
                 showlegend=True,
                 #colorscale='RdBu',
                 legend=dict(x=0, y=-0.2),
                 legend_orientation="h",
                 title_text= option[0] + ' past 7 runs and 3M, 6M, 1Y ',
                 #xaxis = dict(dtick="M2"),
                 #xaxis_tickformat = '%B',
                 hovermode='x unified',
                 plot_bgcolor = 'white',
                 template='ggplot2'  ,

             )
    
    #py.plot(fig_plant, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/Yamal scenarios.html', auto_open=False)
    return fig_market

#chart supply table
@app.callback(
    Output(component_id='body-div-chart-supplydeltatable', component_property='children'),
    Input(component_id='model_chart_plant', component_property='value'),
    Input(component_id='model_chart_country', component_property='value'),
    Input(component_id='model_chart_Sbasin', component_property='value'),
    Input(component_id='model_run_date_chart_supply', component_property='value'),
)
def update_market_chart_Stable(plant, country, Sbasin, rundate_supply):
    
    option = [plant, country, Sbasin]
    option = [x for x in option if str(x) != 'None']
    
    desk_plant_latest, desk_country_latest, desk_supplybasin_latest, desk_supply_hist_plant, desk_supply_hist_country, desk_market_latest, desk_demandbasin_latest, desk_demand_hist, dfdemand, dfsupplyplant, ma30supplycountry, ma30supplybasin, ma30supplyplant,ma30demandcountry, ma30demandbasin = model_delta_chart.get_data()
    start_date = desk_demandbasin_latest['Date'].iloc[0]
    end_date = desk_demandbasin_latest['Date'].iloc[-1]
    
    dfdeskplanthistfull = model_delta_chart.hist_data_supply_plant(rundate_supply, desk_supply_hist_plant, start_date, end_date)
    dfdeskcountryhistfull = model_delta_chart.hist_data_supply_country(rundate_supply, desk_supply_hist_country, start_date, end_date)
    dfdelta_supply = model_delta_chart.supply_delta_data(rundate_supply, option[0], desk_country_latest, desk_supplybasin_latest, dfdeskplanthistfull, dfdeskcountryhistfull)
    dfsupply30, dfdemand30 = model_delta_chart.ma30data(ma30supplycountry, ma30supplybasin, ma30supplyplant,ma30demandcountry, ma30demandbasin)
    
    dfdelta_supply.reset_index(inplace=True)
    hdlistsupply = []
    for i in dfdelta_supply.columns:
        hd={"name":i,"id": i}
        hdlistsupply.append(hd)
    hdlistsupply[0]['name'] = 'Supply'
    
    
    supply_model_delta_layout = html.Div([      
            dash_table.DataTable(
                
                id='table1supplydelta',
                columns = hdlistsupply,
                data = dfdelta_supply.to_dict('records'),
        
                style_cell={
                    'whiteSpace': 'normal',
                    'height': 'auto',
                    'textAlign': 'center'
                },
                
                 style_data={
                        'color': 'black',
                        'backgroundColor': '#D9D9D9'
                    },
                style_header={
                    'backgroundColor': '#D9D9D9',
                    #'fontWeight': 'bold',
                    'textAlign': 'center',
                    #'border': '3px solid black'
                    #'color': 'white'
                },
                
                style_data_conditional=(
                    [{
                        'if': {
                            'filter_query': '{{{}}} >= 10'.format(dfdelta_supply.columns[-1]),
                            'column_id': dfdelta_supply.columns[-1]
                        },
                        'backgroundColor': '#98FB98',
                        'color': 'blue'
                    }]
                    
                    +
                    
                    [{
                        'if': {
                            'filter_query': '{{{}}} <= -10'.format(dfdelta_supply.columns[-1]),
                            'column_id': dfdelta_supply.columns[-1]
                        },
                        'backgroundColor': '#FFB6C1',
                        'color': 'red'
                    }]
                    
                    +
                    
                    [{
                        'if': {
                            'filter_query': '{{{}}} > 0'.format(dfdelta_supply.columns[-1]),
                            'column_id': dfdelta_supply.columns[-1]
                        },
        
                        'color': 'blue'
                    } ]
                    
                    +
                    
                    [{
                        'if': {
                            'filter_query': '{{{}}} < 0'.format(dfdelta_supply.columns[-1]),
                            'column_id': dfdelta_supply.columns[-1]
                        },
                        
                        'color': 'red'
                    }]
                    
                    
                ),
                
                #style_data_conditional=stylessupplyup,
                style_as_list_view=True,
            ),
            ])
    return supply_model_delta_layout
    

#chart demand table
@app.callback(
    Output(component_id='body-div-chart-demanddeltatable', component_property='children'),
    Input(component_id='model_chart_market', component_property='value'),
    Input(component_id='model_chart_region', component_property='value'),
    Input(component_id='model_chart_Dbasin', component_property='value'),
    Input(component_id='model_run_date_chart_demand', component_property='value'),
)
def update_market_chart_Dtable(market, region, Dbasin, rundate_demand):
    
    option = [market, region, Dbasin]
    option = [x for x in option if str(x) != 'None']
    
    desk_plant_latest, desk_country_latest, desk_supplybasin_latest, desk_supply_hist_plant, desk_supply_hist_country, desk_market_latest, desk_demandbasin_latest, desk_demand_hist, dfdemand, dfsupplyplant,ma30supplycountry, ma30supplybasin, ma30supplyplant,ma30demandcountry, ma30demandbasin = model_delta_chart.get_data()
    start_date = desk_demandbasin_latest['Date'].iloc[0]
    end_date = desk_demandbasin_latest['Date'].iloc[-1]
    
    dfdeskmarkethistfull = model_delta_chart.hist_data_demand(rundate_demand, desk_demand_hist, start_date, end_date)
    dfdelta_demand = model_delta_chart.demand_delta_data(rundate_demand, option[0], desk_demandbasin_latest, dfdeskmarkethistfull)

    dfdelta_demand.reset_index(inplace=True)
    hdlistdemand = []
    for i in dfdelta_demand.columns:
        hd={"name":i,"id": i}
        hdlistdemand.append(hd)
    hdlistdemand[0]['name'] = 'Demand'
    
    
    demand_model_delta_layout = html.Div([      
            dash_table.DataTable(
                
                id='table2demanddelta',
                columns = hdlistdemand,
                data = dfdelta_demand.to_dict('records'),
        
                style_cell={
                    'whiteSpace': 'normal',
                    'height': 'auto',
                    'textAlign': 'center'
                },
                
                 style_data={
                        'color': 'black',
                        'backgroundColor': '#D9D9D9'
                    },
                style_header={
                    'backgroundColor': '#D9D9D9',
                    #'fontWeight': 'bold',
                    'textAlign': 'center',
                    #'border': '3px solid black'
                    #'color': 'white'
                },
                
                style_data_conditional=(
                    [{
                        'if': {
                            'filter_query': '{{{}}} >= 10'.format(dfdelta_demand.columns[-1]),
                            'column_id': dfdelta_demand.columns[-1]
                        },
                        'backgroundColor': '#98FB98',
                        'color': 'blue'
                    }]
                    
                    +
                    
                    [{
                        'if': {
                            'filter_query': '{{{}}} <= -10'.format(dfdelta_demand.columns[-1]),
                            'column_id': dfdelta_demand.columns[-1]
                        },
                        'backgroundColor': '#FFB6C1',
                        'color': 'red'
                    }]
                    
                    +
                    
                    [{
                        'if': {
                            'filter_query': '{{{}}} > 0'.format(dfdelta_demand.columns[-1]),
                            'column_id': dfdelta_demand.columns[-1]
                        },
        
                        'color': 'blue'
                    }]
                    
                    +
                    
                    [{
                        'if': {
                            'filter_query': '{{{}}} < 0'.format(dfdelta_demand.columns[-1]),
                            'column_id': dfdelta_demand.columns[-1]
                        },
                        
                        'color': 'red'
                    }]
                    
                    
                ),
                
                #style_data_conditional=stylessupplyup,
                style_as_list_view=True,
            ),
            ])
    return demand_model_delta_layout



#creat charter rate call back list
def charter_rate_callback_list():
    today=datetime.date.today()
    callback_list = []
    daterange = pd.date_range(start = str(today.year)+'-'+str(today.month)+'-01', end=str(today.year+1)+'-12-01', freq='MS').date #change end
    for i in daterange:
        callback_list.append(
            Input("'"+str(i)+"'", 'value'),
            )
    return callback_list

callback_list = charter_rate_callback_list()
#charter rate call back
@app.callback(
    Output('Sensitivity', 'figure'),
    Output('test', 'figure'),
    callback_list
    )
def update_graph(Sep23, Oct23, Nov23, Dec23, Jan24,Feb24,Mar24,Apr24,May24,Jun24,Jul24,Aug24,Sep24,Oct24,Nov24,Dec24): #delete M-1
#def update_graph([input_list]):

    today=datetime.date.today()
    df, dfABPBdesk = CharterRate.get_new_data()  
    dfSenfull = CharterRate.new_model_out(df, dfABPBdesk)
    
    start_date = today-relativedelta(years=2)
    dfSen = dfSenfull.loc[start_date:]
    end_date = str(today.year+1)+'-12-31' #change date
    
    dfSen['option']=dfSen['No US']
    #print(dfSen)
    dfoption = pd.DataFrame([Sep23, Oct23, Nov23, Dec23, Jan24,Feb24,Mar24,Apr24,May24,Jun24,Jul24,Aug24,Sep24,Oct24,Nov24,Dec24],index = pd.date_range(start=str(today.year)+'-'+str(today.month)+'-01', end=end_date,freq='MS').date, columns=['option']) #change M   
    
    #create dfsen for US option plot in sen chart
    dfsen_new=pd.DataFrame(dfSen[['No US','US Cont','US 50%']].loc[start_date:].values, index=pd.date_range(start=start_date, end=end_date,freq='MS').date, columns=['No US','US Cont','US 50%'])
    dfsen_new['option']=dfsen_new['No US']
    
    #create dfreg for US out of sample in test chart
    dfreg_new=pd.DataFrame(dfSenfull['No US'].values, index=pd.date_range(start='2010-01-01', end=end_date,freq='MS').date, columns=['option'])
    #dfreg_new['option']=dfreg_new['No US']
    
    for i in dfoption.index:
        
        dfsen_new['option'].loc[i]=dfsen_new[dfoption.loc[i].values].loc[i].values
        dfreg_new['option'].loc[i]=dfsen_new[dfoption.loc[i].values].loc[i].values
       
    Sensitivity = go.Figure() 
            
    Sensitivity.add_trace(
            go.Scatter(
                x=dfSen.index,
                y=dfSen['Charter Rate History Platts'],
                mode="lines",
                name='Charter Rate History Platts',
                showlegend=True,
                line=dict(color='black', width=2, dash='solid')
                
                )
            )
    Sensitivity.add_trace(
            go.Scatter(
                x=dfSen.index,
                y=dfSen['Markov Switching'],
                mode="lines",
                name='In sample',
                showlegend=True,
                line=dict(color='red', width=2, dash='solid')
                
                )
            )
    Sensitivity.add_trace(
            go.Scatter(
                x=dfSen.index,
                y=dfSen['Charter Rate Fwd curve SPARK'],
                mode="lines",
                name='Charter Rate Fwd curve SPARK',
                showlegend=True,
                line=dict(color='black', width=2, dash='dash')
                
                )
            )
    Sensitivity.add_trace(
            go.Scatter(
                x=dfSen.index,
                y=dfsen_new['option'], #for option line
                mode="lines",
                name='Out sample',
                showlegend=True,
                line=dict(color='red', width=2, dash='dash')
                
                )
            )
    Sensitivity.add_trace(
            go.Scatter(
                x=dfSen.index,
                y=dfSen['No US'],
                mode="lines",
                name='No US',
                showlegend=True,
                line=dict(color='black', width=1, dash='dash')
                
                )
            )
    Sensitivity.add_trace(
            go.Scatter(
                x=dfSen.index,
                y=dfSen['US Cont'],
                mode="lines",
                name='US Cont',
                showlegend=True,
                line=dict(color='black', width=1, dash='dash')
                
                )
            )
    Sensitivity.add_trace(
            go.Scatter(
                x=dfSen.index,
                y=dfSen['US 50%'],
                mode="lines",
                name='US 50%',
                showlegend=True,
                line=dict(color='black', width=1, dash='dash')
                
                )
            )
    
    Sensitivity.add_trace(
               go.Scatter(
                   x=dfSen.index,
                   y=dfSen['errormin'],
                   fill='tonexty',
                   fillcolor='rgba(65,105,225,0)',
                   line_color='rgba(65,105,225,0)',
                   showlegend=False,
                   name='model error range'
                   )
               )           
    Sensitivity.add_trace(
            go.Scatter(
                x=dfSen.index,
                y=dfSen['errormax'],
                fill='tonexty',
                fillcolor='rgba(191,191,191,0.7)',
                line_color='rgba(65,105,225,0)',
                showlegend=True,
                name='model error range'
                )
            )
    
    Sensitivity.add_trace(
               go.Scatter(
                   x=dfSen.index,
                   y=dfSen['platts5yrsmin'],
                   fill='tonexty',
                   fillcolor='rgba(65,105,225,0)',
                   line_color='rgba(65,105,225,0)',
                   showlegend=False,
                   name='Platts 5yrs range'
                   )
               )           
    Sensitivity.add_trace(
            go.Scatter(
                x=dfSen.index,
                y=dfSen['platts5yrsmax'],
                fill='tonexty',
                fillcolor='rgba(173,216,230,0.7)',
                line_color='rgba(65,105,225,0)',
                showlegend=True,
                name='Platts 5yrs range'
                )
            )
        
    Sensitivity.update_layout(
                autosize=True,
                showlegend=True,
                legend=dict(x=0, y=-0.2),
                legend_orientation="h",
                title_text='Chart  fwd veiw vs. Scenarios',
                #yaxis_title="Export (Mcm/d)",
                #xaxis = dict(dtick="M1"),
                height=666,
                hovermode='x unified',
                plot_bgcolor = 'white',
                template='ggplot2'  
           )
    Sensitivity.update_xaxes(
                showline=True, 
                linewidth=1, 
                linecolor='LightGrey', 
                showgrid=True, 
                gridwidth=1, 
                gridcolor='LightGrey'
            )
    Sensitivity.update_yaxes(
            showline=True, 
            linewidth=1, 
            linecolor='LightGrey', 
            showgrid=True, 
            gridwidth=1, 
            gridcolor='LightGrey'
        )

    #plot test regression graph
    test = go.Figure() 
            
    test.add_trace(
            go.Scatter(
                x=dfSenfull.index,
                y=dfSenfull['Charter Rate History Platts'],
                mode="lines",
                name='Charter Rate History Platts',
                showlegend=True,
                line=dict(color='black', width=2, dash='solid')
                
                )
            )
    test.add_trace(
            go.Scatter(
                x=dfSenfull.index,
                y=dfSenfull['Markov Switching'],
                mode="lines",
                name='In sample',
                showlegend=True,
                line=dict(color='red', width=2, dash='solid')
                
                )
            )
    test.add_trace(
            go.Scatter(
                x=dfSenfull.index,
                y=dfSenfull['Charter Rate Fwd curve SPARK'],
                mode="lines",
                name='Charter Rate Fwd curve SPARK',
                showlegend=True,
                line=dict(color='black', width=2, dash='dash')
                
                )
            )
    test.add_trace(
            go.Scatter(
                x=dfreg_new.index,
                y=dfreg_new['option'],
                mode="lines",
                name='Out sample',
                showlegend=True,
                line=dict(color='red', width=2, dash='dash')
                
                )
            )
    
    test.add_trace(
               go.Scatter(
                   x=dfSenfull.index,
                   y=dfSenfull['errormin'],
                   fill='tonexty',
                   fillcolor='rgba(65,105,225,0)',
                   line_color='rgba(65,105,225,0)',
                   showlegend=False,
                   name='model error range'
                   )
               )           
    test.add_trace(
            go.Scatter(
                x=dfSenfull.index,
                y=dfSenfull['errormax'],
                fill='tonexty',
                fillcolor='rgba(191,191,191,0.7)',
                line_color='rgba(65,105,225,0)',
                showlegend=True,
                name='model error range'
                )
            )
    '''
    test.add_trace(
               go.Scatter(
                   x=dfSenfull.index,
                   y=dfSenfull['platts5yrsmin'],
                   fill='tonexty',
                   fillcolor='rgba(65,105,225,0)',
                   line_color='rgba(65,105,225,0)',
                   showlegend=False,
                   name='Platts 5yrs range'
                   )
               )           
    test.add_trace(
            go.Scatter(
                x=dfSenfull.index,
                y=dfSenfull['platts5yrsmax'],
                fill='tonexty',
                fillcolor='rgba(173,216,230,0.7)',
                line_color='rgba(65,105,225,0)',
                showlegend=True,
                name='Platts 5yrs range'
                )
            )
    '''
    test.update_layout(
                autosize=True,
                showlegend=True,
                legend=dict(x=0, y=-0.2),
                legend_orientation="h",
                title_text='Full Regression',
                height=666,
                hovermode='x unified',
                plot_bgcolor = 'white',
                template='ggplot2'  
           )
    test.update_xaxes(
                showline=True, 
                linewidth=1, 
                linecolor='LightGrey', 
                showgrid=True, 
                gridwidth=1, 
                gridcolor='LightGrey'
            )
    test.update_yaxes(
                showline=True, 
                linewidth=1, 
                linecolor='LightGrey', 
                showgrid=True, 
                gridwidth=1, 
                gridcolor='LightGrey'
            )
    
    
    return Sensitivity, test


@app.callback(
    Output(component_id='dt_select_tempnormal', component_property='options'),
    Input(component_id='dt_select_country', component_property='value'),
    
)
def get_temp_normal(country):
    
    #print(country)
    tempnormal_dict,dfnormal = temp_demand.normal_temp(country)
    
    dictnan = [{'label': 'None', 'value':'None'}]
    dictnormal = [{'label': i, 'value': tempnormal_dict[i]} for i in tempnormal_dict.keys()]
    dictoption = dictnan + dictnormal
    #print(dictoption)
    
    
    return dictoption

@app.callback(
    Output(component_id='dt_select_tempstd', component_property='options'),
    Input(component_id='dt_select_country', component_property='value'),
    Input(component_id='dt_select_start_year', component_property='value'),
    
)
def get_temp_std(country,start_year):
    
    #print(country)
    dftemphist,dfyoy =  temp_demand.get_temp_hist_data(country, start_year)   
    dfyoy.columns = dfyoy.columns.droplevel(1)
    #print(dfyoy)
    
    dictnan = [{'label': 'None', 'value':'None'}]
    dictstd = [{'label': i, 'value': dfyoy.loc[i, 'std']} for i in dfyoy.index]
    dictoption = dictnan + dictstd
    #print(dictoption)
    
    return dictoption
    
@app.callback(
    Output('dtselectedcountry', 'children'),
    Output('dtbasetemp', 'children'),
    Output('dttempsce', 'children'),
    Output('dtdemand', 'children'),
    Output('body-div-dt-chart', 'figure'),
    Output('body-div-dt-table', 'figure'),
    #Output('body-div-dt-Stable', 'figure'),

    
    Input(component_id='dt_select_country', component_property='value'),
    Input(component_id='dt_select_start_year', component_property='value'),
    Input(component_id='dt_select_tempnormal', component_property='value'),
    Input(component_id='base_temp', component_property='value'),
    Input(component_id='temp_scenario', component_property='value'),
    Input(component_id='std_multi', component_property='value'),
    Input(component_id='dt_select_tempstd', component_property='value'),
    )
def callback_a(country,start_year,tempnormal,basetemp,tempscenario,stdmulti,tempstd):    
    #get temp hist and yoy
    dftemphist,dfyoy =  temp_demand.get_temp_hist_data(country, start_year)    
    #get ihd data
    gas_demand_by_sector_m = temp_demand.get_ihs_data(start_year, country)
    #df full
    dffull = temp_demand.data_full(country, gas_demand_by_sector_m, dftemphist, dfyoy)
    # parameters of model
    p, dfmodel = temp_demand.model_para(dffull)
    
    figchart = temp_demand.model_chart_scatter(dfmodel)
    # dfsta
    dfsta = temp_demand.statistic(dffull, p) 
    #table data and plot
    dftable = temp_demand.table_data(dfsta)
    figtable = temp_demand.plot_table(dftable, start_year)     
    
    tempnormal_dict,dfnormal = temp_demand.normal_temp(country)
    #dfStable =  temp_demand.temp_table(dfnormal, dfyoy, p, country)   Stable
    #figS = temp_demand.plot_Stable(dfStable)
    
    #calculate demand and scatter chart
    if tempnormal != 'None':
        dtbasetemp = tempnormal
        y1 = p[0]*tempnormal*tempnormal+p[1]*tempnormal+p[2]
        if tempscenario != 0:
            dttempsce = tempscenario
            y2 = p[0]*(tempnormal+tempscenario)*(tempnormal+tempscenario)+p[1]*(tempnormal+tempscenario)+p[2]
            yd = y2-y1
        if tempscenario == 0 and tempstd != 'None':
            dttempsce = tempstd
            y2 = p[0]*(tempnormal+tempstd*stdmulti)*(tempnormal+tempstd*stdmulti)+p[1]*(tempnormal+tempstd*stdmulti)+p[2]
            yd = y2-y1
    
    if tempnormal == 'None' and basetemp != 'None':
        dtbasetemp = basetemp
        y1 = p[0]*basetemp*basetemp+p[1]*basetemp+p[2]
        if tempscenario != 0:
            dttempsce = tempscenario
            y2 = p[0]*(basetemp+tempscenario)*(basetemp+tempscenario)+p[1]*(basetemp+tempscenario)+p[2]
            yd = y2-y1
        if tempscenario == 0 and tempstd != 'None':
            dttempsce = tempstd
            y2 = p[0]*(basetemp+tempstd*stdmulti)*(basetemp+tempstd*stdmulti)+p[1]*(basetemp+tempstd*stdmulti)+p[2]
            yd = y2-y1
            
    #print(yd)
    #print(dttempsce)
    
        
    return country, dtbasetemp, round(dttempsce, 2), round(yd,2), figchart, figtable#,figS


if __name__ == '__main__':
    app.run_server(debug=False, host='0.0.0.0', port=8050)
    



