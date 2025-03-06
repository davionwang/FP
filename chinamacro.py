# -*- coding: utf-8 -*-
"""
Created on Fri Jul 28 15:26:27 2023

@author: SVC-GASQuant2-Prod
"""


import CErename
import pandas as pd
from CEtools import CEtools
import sys
sys.path.append('C:\\Users\SVC-GASQuant2-Prod\YWwebsite\Python Scripts\class') 
import numpy as np
import datetime
import sqlalchemy
import pyodbc
import requests
import json
import math
from dateutil.relativedelta import relativedelta
from sklearn.neighbors import KNeighborsRegressor
from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score
from dateutil.relativedelta import relativedelta
import plotly.graph_objs as go
import plotly.offline as py
import calendar


#import CEtools
from CEtools import *
ce=CEtools.CElink('swasti@freepoint.com','kfGa5uwL1+')
pd.set_option('display.max_columns',20)
from DBtoDF import DBTOPD


class ChinaMacro():
    
    def get_LNG_data():
        
        df = DBTOPD('PRD-DB-SQL-211','[LNG]','[ana]','KplerNetDemandCountryMt').sql_to_df()
        df.set_index('Date', inplace=True)
        df = df[['Japan']]
        dfquarter = df.resample('D').sum()
        print(dfquarter)
        dfquarter.to_csv('C:\\Users\SVC-GASQuant2-Prod\YWwebsite\data\china.csv')
        
        
        
        
ChinaMacro.get_LNG_data()