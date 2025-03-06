# -*- coding: utf-8 -*-
"""
Created on Fri Jun 17 15:22:41 2022

@author: SVC-GASQuant2-Prod
"""

#V3 use db data
#v4 add error term and 5yrs range


import sys
import plotly.offline as py
import plotly.graph_objs as go
import dash
import dash_core_components as dcc
#import dash_html_components as html
from dash import html
from dash.dependencies import Input, Output
import numpy as np
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import time
import datetime
from sklearn.neighbors import KNeighborsRegressor
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import ExtraTreesRegressor
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import GridSearchCV
from sklearn.model_selection import ShuffleSplit
from sklearn.model_selection import train_test_split
from sklearn.metrics import r2_score
from sklearn.preprocessing import StandardScaler
from dateutil.relativedelta import relativedelta

import math
import matplotlib.pyplot as plt
import json
import requests
import statsmodels.api

import plotly.express as px
from kpler.sdk.configuration import Configuration
from kpler.sdk import Platform
from kpler.sdk.resources.fleet_metrics import FleetMetrics
from kpler.sdk import FleetMetricsAlgo, FleetMetricsPeriod
config = Configuration(Platform.LNG, "ywang1@freepoint.com", "Wsad1234")
fleet_metrics_client = FleetMetrics(config)
sys.path.append('C:\\Users\SVC-PowerUK-Test\YWwebsite\Python Scripts\class') 
from DBtoDF import DBTOPD
#from Freight import Freight

pd.set_option('display.max_columns',20)

''' Need to change date every month: '''

class CharterRate():

    def get_new_data():
        
        #global df, dfSen, start_date, end_date 
        today = datetime.date.today()
        start_date = '2022-09-01'
        end_date = str(today.year)+'-12-01' #
        
        '''get demand and desk demand'''
        demand=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'DemandCountry')
        dfdemand=demand.sql_to_df()
        dfdemand.set_index('Date', inplace=True)
        #print(dfkpler)
        demandall = dfdemand.sum(axis=1)
        demandall.columns=['Demand ALL']
        demandall = demandall.resample('MS').sum()/0.000612*0.000000438 #small diff with Salman's because m3 to mcm to mt. He use M3 to mt directly.
        #print(demandall)
        demanddesk = DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'DeskDemand')
        dfdemanddesk=demanddesk.sql_to_df()
        dfdemanddesk.set_index('Date', inplace=True)
        dfdemanddeskall = dfdemanddesk.sum(axis=1)
        dfdemanddeskall.columns=['Demand ALL']
        dfdemanddeskall = dfdemanddeskall.resample('MS').sum()/0.000612*0.000000438 
        #print(dfdemanddeskall.tail())
        
        '''get freight and spark'''
        #freightat = DBTOPD.get_FreightAtlantic()
        at =  DBTOPD.get_FreightAtlantic()
        at['Timestamp'] = pd.to_datetime(at['Timestamp'], format='%Y-%m-%d')
        at.set_index('Timestamp', inplace=True)
        at=at.loc['2010-01-01':]
        atM = at.resample('MS').mean()
        #print(atM)
        freight_spark = DBTOPD('PRD-DB-SQL-209','AnalyticsModel', 'ts', 'AnalyticsLatest')
        dffreight_spark = freight_spark.get_freight_spark()
        dffreight_spark = dffreight_spark[['ValueDate','Value']]
        dffreight_spark.set_index('ValueDate', inplace=True)
        #print(dffreight_spark)
        
        '''get shipping days and AB to PB'''
        ABPB=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'ABtoPB')
        dfabpb=ABPB.sql_to_df()
        dfabpb.set_index('Date', inplace=True)
        dfabpbM=dfabpb.resample('MS').sum()  #big diff
        #print(dfabpbM.tail(30))
        dfshippingdayM=dfabpb['shipping days'].resample('MS').mean()  #some diff
        #print(dfshippingdayM.tail(30))
        '''get fcst AB PB'''
        ABPBproduct = pd.read_excel('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/ABPBproduct.xlsx',header=(0))
        ABPBproduct.set_index('EoS %', inplace=True)
        #print(ABPBproduct)
        supplydesk = DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'DeskSupplyPlant')
        dfsupplydesk=supplydesk.sql_to_df()
        dfsupplydesk.set_index('Date', inplace=True)
        dfsupplydeskplant = dfsupplydesk[['Atlantic LNG','Bioko','Bonny LNG','Cameron (Liqu.)','Cameroon FLNG Terminal','Corpus Christi','Cove Point','Elba Island Liq.','Freeport','Sabine Pass','Snohvit','Soyo','Yamal']]
        dfsupplydeskplant=dfsupplydeskplant.resample('MS').sum()/0.000612*0.000000438 
        dfsupplydeskplant = dfsupplydeskplant.T
        
        dfsupplydeskplant2021 = dfsupplydeskplant.loc[:,'2021-01-01':'2021-12-01']
        dfsupplydeskplant2021.columns = dfsupplydeskplant2021.columns.month
        dfABPBdesk2021=dfsupplydeskplant2021*ABPBproduct
        dfABPBdesk2021 = dfABPBdesk2021.T
        #print(dfABPBdesk2021)
        dfsupplydeskplant2022 = dfsupplydeskplant.loc[:,'2022-01-01':'2022-12-01']
        dfsupplydeskplant2022.columns = dfsupplydeskplant2022.columns.month
        dfABPBdesk2022=dfsupplydeskplant2022*ABPBproduct
        dfABPBdesk2022=dfABPBdesk2022.T
        
        dfsupplydeskplant2023 = dfsupplydeskplant.loc[:,'2023-01-01':end_date]
        dfsupplydeskplant2023.columns = dfsupplydeskplant2023.columns.month
        dfABPBdesk2023=dfsupplydeskplant2023*ABPBproduct
        dfABPBdesk2023=dfABPBdesk2023.T
        #print(dfABPBdesk2022)
        dfABPBdesk = pd.concat([dfABPBdesk2021,dfABPBdesk2022, dfABPBdesk2023], axis=0)
        #print(dfABPBdesk)
        #dfABPBdesk.drop(['nan'],axis=1,inplace=True)
        dfABPBdesk.dropna(axis=0, how='all', inplace=True)
        #dfABPBdesk = dfABPBdesk.T
        dfABPBdesk.index=pd.date_range(start='2021-01-01',end=end_date, freq='MS')
        dfABPBdesk=dfABPBdesk[['Atlantic LNG','Bioko','Bonny LNG','Cameron (Liqu.)','Cameroon FLNG Terminal','Corpus Christi','Cove Point','Elba Island Liq.','Freeport','Sabine Pass','Snohvit','Soyo','Yamal']]
        dfABPBdesk['AB to PB'] = dfABPBdesk[['Atlantic LNG','Bioko','Bonny LNG','Cameroon FLNG Terminal','Snohvit','Yamal']].sum(axis=1)
        dfABPBdesk['US']=dfABPBdesk[['Cameron (Liqu.)','Corpus Christi','Cove Point','Elba Island Liq.','Freeport','Sabine Pass']].sum(axis=1)
        dfABPBdesk['50%']=dfABPBdesk['US']*0.5
        dfABPBdesk['US Contracts Asia']=dfABPBdesk['US']*0.2
        dfABPBdesk['No US'] = dfABPBdesk['AB to PB']
        dfABPBdesk['US Cont'] = dfABPBdesk['No US'] + dfABPBdesk['US Contracts Asia']
        dfABPBdesk['US 50%'] = dfABPBdesk['No US'] + dfABPBdesk['50%']
        dfABPBdesk['No US Li'] = dfABPBdesk['No US']*1.834266487+23.81115962
        dfABPBdesk['US Cont Li'] = dfABPBdesk['US Cont']*1.834266487+23.81115962
        dfABPBdesk['US 50% Li'] = dfABPBdesk['US 50%']*1.834266487+23.81115962
        #print(dfABPBdesk)
        '''read new build'''
        newbuild=pd.read_excel('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/new build.xlsx',header=(0))
        newbuild['date']=pd.to_datetime(newbuild['date'], format='%Y-%m-%d')
        newbuild.set_index('date', inplace=True)
        #print(newbuild)
        
        plattshist = atM.copy()
        plattshist['year'] = plattshist.index.year
        plattshist['date'] = plattshist.index.strftime('%m-%d')
        plattshistyoy = plattshist.set_index(['year','date']).Close.unstack(0)
        plattshistyoy['5yrsmin'] = plattshistyoy.loc[:,str(today.year-6):str(today.year-1)].min(axis=1)
        plattshistyoy['5yrsmax'] = plattshistyoy.loc[:,today.year-6:today.year-1].max(axis=1)
        #plattshistyoy.index = pd.to_datetime(plattshistyoy.index)
        #print(plattshistyoy)
        
        
        
        '''chart df'''
        df=pd.DataFrame(index=pd.date_range(start='2010-01-01', end='2024-12-31',freq='MS')) #change end2024
        df['Charter Rate History Platts'] = atM
        df['Demand ALL'] = demandall
        df['AB to PB'] = dfabpbM['VolumeOriginM3']
        df['shipping days'] = dfshippingdayM 
        df['new builds'] = newbuild['New Builds']
        df['Demand ALL'].loc[start_date:] = dfdemanddeskall.loc[start_date:]
        df['AB to PB'].loc[start_date:] = dfABPBdesk['AB to PB'].loc[start_date:]
        df['shipping days'].loc[start_date:] = df['AB to PB'].loc[start_date:]*1.834266487+23.81115962
        df['Charter Rate Fwd curve SPARK'] = np.NaN
        df['Charter Rate Fwd curve SPARK'].loc[start_date:dffreight_spark.index[-1]] = dffreight_spark['Value'].loc[start_date:]
        df['Regression'] = (df['Demand ALL']*-3443+df['AB to PB']*29178.5+df['shipping days']*3485.118+df['new builds']*-5363.84)+22665.41
        
        df['No US'] = np.NaN
        df['US Cont'] =np.NaN
        df['US 50%'] = np.NaN
        df['No US'].loc[start_date:] = (df['Demand ALL'].loc[start_date:]*-3443+df['AB to PB'].loc[start_date:]*29178.5+df['shipping days'].loc[start_date:]*3485.118+df['new builds'].loc[start_date:]*-5363.84)+22665.41
        df['US Cont'].loc[start_date:] =(df['Demand ALL'].loc[start_date:]*-3443+dfABPBdesk['US Cont'].loc[start_date:]*29178.5+dfABPBdesk['US Cont Li'].loc[start_date:]*3485.118+df['new builds'].loc[start_date:]*-5363.84)+22665.41
        df['US 50%'].loc[start_date:] = (df['Demand ALL'].loc[start_date:]*-3443+dfABPBdesk['US 50%'].loc[start_date:]*29178.5+dfABPBdesk['US 50% Li'].loc[start_date:]*3485.118+df['new builds'].loc[start_date:]*-5363.84)+22665.41
        
        for i in df.index:
            df.loc[i,'platts5yrsmin'] = plattshistyoy['5yrsmin'].iloc[i.month-1]
            df.loc[i,'platts5yrsmax'] = plattshistyoy['5yrsmax'].iloc[i.month-1]
        
        df=df.round(2)
        #print(df.loc['2021-09-01':])
        #dfSen=df.loc['2019-09-01':]
        return df, dfABPBdesk
     
    def floating_cargos():
        # misc variables
        today = datetime.date.today()
        
        # Kpler API function
        def floating(Country_or_Region):
            df_floating = fleet_metrics_client.get(
                metric=FleetMetricsAlgo.FloatingStorage,
                period=FleetMetricsPeriod.Daily,
                zones=[Country_or_Region],
                floating_storage_duration_min=20,
                floating_storage_duration_max=90, )
            
            #print(df_floating)
            return df_floating
        
        # Convert raw Kpler to mcm of nat gas
        def convert_to_mcm(df, country_or_region):
            df.index = df['Date']
            df = ((df['Total'] * 609) / 1000000).to_frame().rename(columns={'Total': str(country_or_region) + ' Floating Storage (20-90 Days)'})
            return df
        
        # Various dfs
        euro_floating = floating('Europe')
        euro_floating = convert_to_mcm(euro_floating, 'Europe')
        #print(euro_floating)
        #asia_floating = floating('Asia')
        #asia_floating = convert_to_mcm(asia_floating, 'Asia')
        
        #all_floating = pd.concat([euro_floating, asia_floating], axis=1)
        #euro_floating.to_csv('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/charter.csv')  
        # Chart date parameters
        start_date = '2010-01-01'
        end_date = today
        
        return euro_floating
    
    def floating_cargos_fcst(euro_floating):
        
        jkmttf=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'BBGJKMTTF').sql_to_df()
        jkmttf.set_index('date', inplace=True)
        #print(jkmttf)
        
        df = pd.concat([euro_floating, jkmttf['value']], axis=1)
        df=df.resample('MS').mean()
        #print(df)
        #df.fillna(method='ffill', inplace=True)
        df.dropna(axis=0, inplace=True)
        
        
        columns = ['value']
        
        test_start = '2021-11-18'
        test_end = '2022-12-12'
        target_start = '2021-01-01'
        target_end = '2022-06-30'
        
        train_X1, train_y1 = df.loc[test_start:test_end,columns], df.loc[test_start:test_end,'Europe Floating Storage (20-90 Days)']#(df.loc[:'2022-02-01','Y'] - df.loc[:'2022-02-01','JP demand'])
        
        
        mod_hamilton = statsmodels.tsa.regime_switching.markov_regression.MarkovRegression(
                endog=train_y1, 
                exog = train_X1 ,
                #switching_variance=True,
                k_regimes=2,
                trend='ct',
                #order=2,
                #switching_trend=True, 
                #switching_exog=True,
                )
    
        res_hamilton = mod_hamilton.fit()
        #print(train_X1)
        #print(res_hamilton.summary())
        #res_hamilton.smoothed_marginal_probabilities[1].plot(title="Probability of being in the high regime", figsize=(12, 3))
        pred = res_hamilton.predict(start='2021-12-01', end='2022-12-01')
        
        result2=pd.DataFrame()
        result2.loc[test_start:test_end,'act'] = train_y1
        result2.loc[test_start:test_end,'ms'] = pred
        #result2.to_csv('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/charter.csv')   
        result2.plot(legend=True)
        #print(result2)
        print('RMSE ms: ', round(math.sqrt(((result2['ms']-result2['act'])**2).mean()),2))
        
        print('r2 ms: ', round(r2_score(result2['act'], result2['ms']),2))
    
                
    def new_model(df, euro_floating):
        
        #print(df)
        price =  pd.read_excel('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/charter fcst.xlsx', sheet_name=2)
        price.dropna(inplace=True)
        price.set_index('date', inplace=True)
        price.index = pd.to_datetime(price.index)
        price.sort_index(inplace=True)
        price = price.resample('MS').mean()
        #print(price)
        
    
        df['brent'] = price['brent']
        df['floating'] = euro_floating['Europe Floating Storage (20-90 Days)'].resample('MS').mean()
        df.fillna(0, inplace=True)
        #print(df)
        #columns = ['Demand ALL','AB to PB','shipping days','new builds','brent','floating']
        columns = ['Demand ALL','AB to PB','shipping days','brent']
        #print(df)
        test_start = '2010-01-01'
        test_end = '2022-09-01'
        target_start = '2022-10-01'
        target_end = '2023-12-01'
        
        train_X1, train_y1 = df.loc[test_start:test_end,columns], df.loc[test_start:test_end,'Charter Rate History Platts']
        
        #print(train_X1)
        Target_x = df.loc[target_start:target_end,columns]
        #Traget_y = rf_grid.predict(Target_x)
        #print(Target_x)
        #test_y = dfmodel.loc[target_start:target_end,'Charter Rate History Platts']
        '''
        cv = ShuffleSplit(n_splits=5, test_size=0.2)
            
        rf_model = ExtraTreesRegressor()
        
        rf_param = {'n_estimators':range(1,100)}
        
        rf_grid = GridSearchCV(rf_model, rf_param, cv=cv)
        
        np.random.seed(50)
        
        rf_grid.fit(train_X1, train_y1)
        
        
        best_rf = rf_grid.best_estimator_
        print('Test score:', best_rf.score(train_X1, train_y1))
        scores = rf_grid.cv_results_['mean_test_score']
        plt.figure().set_size_inches(8, 6)
        plt.semilogx(scores)
        
        # feature importance scores
        features = train_X1.columns
        feature_importances = best_rf.feature_importances_
        
        features_df = pd.DataFrame({'Features': features, 'Importance Score': feature_importances*100})
        print(features_df.round(3))
        '''
        
        print('-------------------linear---------------------')
        reg = LinearRegression().fit(train_X1, train_y1)
        #print(reg.score(train_X1, train_y1))
        
        #print(reg.coef_)
        
        #print(reg.intercept_)
        
        reg_predit = reg.predict(train_X1)
        result1=pd.DataFrame(index = pd.date_range(start=test_start, end = test_end, freq = 'MS'))
        result1.loc[test_start:test_end,'act'] = train_y1
        result1.loc[test_start:test_end,'fcst'] = reg_predit
        #result['Meti'] = df['Y']
        result1.plot(legend=True)
        #print(result1)
        
        print('-------------------KNN---------------------')
        knn=KNeighborsRegressor(n_neighbors=3, #weights='distance')
                                weights='uniform')
        knn.fit(train_X1,train_y1)
        knn_y_predict = knn.predict(train_X1)
        
        print('-------------------ms---------------------')
        mod_hamilton = statsmodels.tsa.regime_switching.markov_regression.MarkovRegression(
                endog=train_y1, 
                exog = train_X1 ,
                #switching_variance=True,
                k_regimes=2,
                trend='ct',
                #order=2,
                #switching_trend=True, 
                #switching_exog=True,
                )
            
            
        
       
        res_hamilton = mod_hamilton.fit()
        
        #print(res_hamilton.summary())
        #res_hamilton.smoothed_marginal_probabilities[1].plot(title="Probability of being in the high regime", figsize=(12, 3))
        pred = res_hamilton.predict(start=test_start, end=test_end)
        ##########################################
        
        result2=pd.DataFrame(index = pd.date_range(start=test_start, end = test_end, freq = 'MS'))
        result2.loc[test_start:test_end,'act'] = train_y1
        result2.loc[test_start:test_end,'knnfcst'] = knn_y_predict
        result2.loc[test_start:test_end,'linearfcst'] = reg_predit
        result2.loc[test_start:test_end,'ms'] = pred
        result2.loc[test_start:test_end,'Old'] =  df.loc[test_start:test_end,'Regression']
        
        #result['Meti'] = df['Y']
        result2.to_csv('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/charter.csv')
        #print((result2['linearfcst']-result2['act'])**2)
        result2.plot(legend=True)
        print('RMSE linear: ', round(math.sqrt(((result2['linearfcst']-result2['act'])**2).mean()),2))
        print('RMSE knn: ', round(math.sqrt(((result2['knnfcst']-result2['act'])**2).mean()),2))
        print('RMSE ms: ', round(math.sqrt(((result2['ms']-result2['act'])**2).mean()),2))
        print('RMSE old: ', round(math.sqrt(((result2['Old']-result2['act'])**2).mean()),2))
        
        print('r2 linear: ', round(r2_score(result2['act'], result2['linearfcst']),2))
        print('r2 knn: ', round(r2_score(result2['act'], result2['knnfcst']),2))
        print('r2 ms: ', round(r2_score(result2['act'], result2['ms']),2))
        print('r2 old: ', round(r2_score(result2['act'], result2['Old']),2))
        
        
    def new_model_out(df, dfABPBdesk):
        
        #print(df)
        price =  pd.read_excel('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/charter fcst.xlsx', sheet_name=2)
        price.dropna(inplace=True)
        price.set_index('date', inplace=True)
        price.index = pd.to_datetime(price.index)
        price.sort_index(inplace=True)
        price = price.resample('MS').mean()
        #print(price)
        
        curve_id = ['ICE BRENT FWD'] 
        today=datetime.date.today()
        cob = today#'2021-12-15'
        time_period_id = 2
        start = '2022-01-01' #str((today+relativedelta(months=1)).year)+'-'+str((today+relativedelta(months=1)).month)+'-01'# 
        end = str(today.year+2)+'-12-01'#'2024-12-01'
        
        dfcurvefull=pd.DataFrame(index=pd.date_range(start=start, end=end, freq='MS'))
        for i in curve_id:
            url =\
                f'http://prd-rsk-app-06:28080/price-manager/service/curveJson?name={i}'\
                    + f'&cob={cob}&time_period_id={time_period_id}&start={start}&end={end}'
            
            response = requests.request('GET', url)
            data_json = json.loads(response.text)
            dfcurve=pd.DataFrame.from_dict(data_json)
            dfcurve.set_index('CURVE_START_DT', inplace=True)
            dfcurve.index=pd.to_datetime(dfcurve.index)
            #print(dfcurve)
            dfcurvefull[i] = dfcurve['PRICE'] 
        
        #print(dfcurvefull.loc['2022-07-01':'2022-10-01'])
        
        price = pd.concat([price,dfcurvefull.loc['2023-01-01':str(today.year+1)+'-12-01']],axis=0)
        price.loc['2023-01-01':str(today.year+1)+'-12-01','brent'] = price.loc['2023-01-01':str(today.year+1)+'-12-01','ICE BRENT FWD'].values
        #print(price)
    
        df['brent'] = price['brent']
        #df['floating'] = euro_floating['Europe Floating Storage (20-90 Days)'].resample('MS').mean()
        df.fillna(0, inplace=True)
        #print(df)
        #columns = ['Demand ALL','AB to PB','shipping days','new builds','brent','floating']
        
        #print(df)
        test_start = '2010-01-01'
        test_end = str(today.year)+'-'+str(today.month)+'-01'#2022-09-01'#
        target_start = str(today.year)+'-'+str(today.month)+'-01'#'2022-10-01'
        target_end = str(today.year+1)+'-12-01' #2024
        
        columns = ['Demand ALL','AB to PB','shipping days','brent']
        train_X1, train_y1 = df.loc[test_start:test_end,columns], df.loc[test_start:test_end,'Charter Rate History Platts']
        
        #print(train_X1)
        Target_x = df.loc[test_start:target_end,columns]
        Traget_y = df.loc[test_start:target_end,'Charter Rate History Platts']
        print(Target_x)
        #test_y = dfmodel.loc[target_start:target_end,'Charter Rate History Platts']
        '''
        cv = ShuffleSplit(n_splits=5, test_size=0.2)
            
        rf_model = ExtraTreesRegressor()
        
        rf_param = {'n_estimators':range(1,100)}
        
        rf_grid = GridSearchCV(rf_model, rf_param, cv=cv)
        
        np.random.seed(50)
        
        rf_grid.fit(train_X1, train_y1)
        
        
        best_rf = rf_grid.best_estimator_
        print('Test score:', best_rf.score(train_X1, train_y1))
        scores = rf_grid.cv_results_['mean_test_score']
        plt.figure().set_size_inches(8, 6)
        plt.semilogx(scores)
        
        # feature importance scores
        features = train_X1.columns
        feature_importances = best_rf.feature_importances_
        
        features_df = pd.DataFrame({'Features': features, 'Importance Score': feature_importances*100})
        print(features_df.round(3))
        '''
        ''' 
        print('-------------------linear---------------------')
        reg = LinearRegression().fit(train_X1, train_y1)
        #print(reg.score(train_X1, train_y1))
        
        #print(reg.coef_)
        
        #print(reg.intercept_)
        
        reg_predit = reg.predict(Target_x)
        result1=pd.DataFrame(index = pd.date_range(start=test_start, end = target_end, freq = 'MS'))
        #result1.loc[test_start:test_end,'act'] = train_y1
        result1.loc[test_start:target_end,'fcst'] = reg_predit
        #result['Meti'] = df['Y']
        #result1.plot(legend=True)
        print(reg_predit)
        
        print('-------------------KNN---------------------')
        knn=KNeighborsRegressor(n_neighbors=3, #weights='distance')
                                weights='uniform')
        knn.fit(train_X1,train_y1)
        knn_y_predict = knn.predict(Target_x)
        
        #print(train_X1.loc[test_start:test_end])
        Traget_y.loc[target_start:target_end] = result1.loc[target_start:target_end,'fcst']
        '''
        print('-------------------ms---------------------')
        mod_hamilton = statsmodels.tsa.regime_switching.markov_regression.MarkovRegression(
                endog = train_y1, 
                exog = train_X1 ,
                #exog_tvtp = train_X1 ,
                #switching_variance=True,
                k_regimes=2,
                trend='ct',
                #order=2,
                #switching_trend=True, 
                #switching_exog=True,
                )
        
       
        res_hamilton = mod_hamilton.fit()
        #mod_hamilton.fit()
        #print(res_hamilton.summary())
        #print(mod_hamilton.param_names)
        #print(mod_hamilton.exog_names)
        #print(res_hamilton())
        #res_hamilton.smoothed_marginal_probabilities[1].plot(title="Probability of being in the high regime", figsize=(12, 3))
        pred = res_hamilton.predict(start=test_start, end=test_end)
        #print(pred)
        #pred = mod_hamilton.forecast(steps=12) 
        ##########################################
        
        prob_matrix = np.array([[0.95055283,0.12980974],
                                [0.04944717, 0.87019026]])
                                
                                 
        r1c1 = 8083.1647
        r1x1 = -34.0925 
        r1x2 = -1147.8453
        r1x3 = 3548.6589
        r1x4 = 1894.9706
        r1x5 = 421.3788 
    
        r2c2 = -1.108e+05
        r2x1 = 460.6674 
        r2x2 = -330.5845
        r2x3 = 2.26e+04
        r2x4 = 1333.5222
        r2x5 = 1271.9360        
        
        #print(train_y1-.std())
        #print(columns)
        trend = list(range(1, Target_x.shape[0]+1,1))
        #print(trend)
        Target_x['t'] = trend
        #print(Target_x.loc[target_start:target_end,columns[0]])
        #error = np.random.normal(0, 16717.5)
        error = 0
        #print(error)
        #print(prob_matrix[0,0])
        
        fcst = pd.DataFrame(index = pd.date_range(start=target_start, end = target_end, freq = 'MS'))
        for i in fcst.index:
            
            fcst.loc[i,'00'] = prob_matrix[0,0]*(r1c1 + r1x1*Target_x.loc[i,'t'] +  r1x2*Target_x.loc[i,'Demand ALL'] + r1x3*Target_x.loc[i,'AB to PB'] + r1x4*Target_x.loc[i,'shipping days'] + r1x5*Target_x.loc[i,'brent']+error)
            fcst.loc[i,'01'] = (1-prob_matrix[0,0])*(r2c2 + r2x1*Target_x.loc[i,'t'] +  r2x2*Target_x.loc[i,'Demand ALL'] + r2x3*Target_x.loc[i,'AB to PB'] + r2x4*Target_x.loc[i,'shipping days'] + r2x5*Target_x.loc[i,'brent']+error)
            fcst.loc[i,'10'] = (1-prob_matrix[1,1])*(r1c1 + r1x1*Target_x.loc[i,'t'] +  r1x2*Target_x.loc[i,'Demand ALL'] + r1x3*Target_x.loc[i,'AB to PB'] + r1x4*Target_x.loc[i,'shipping days'] + r1x5*Target_x.loc[i,'brent']+error)
            fcst.loc[i,'11'] = prob_matrix[1,1]*(r2c2 + r2x1*Target_x.loc[i,'t'] +  r2x2*Target_x.loc[i,'Demand ALL'] + r2x3*Target_x.loc[i,'AB to PB'] + r2x4*Target_x.loc[i,'shipping days'] + r2x5*Target_x.loc[i,'brent']+error)
        fcst['fcst'] = fcst.sum(axis=1)/2
       
        #US scenarios
        dfnew = pd.concat([df[['Charter Rate History Platts','Demand ALL','brent']], dfABPBdesk[['US Cont','US Cont Li','US 50%','US 50% Li']]], axis=1)
        #print(dfnew)
        columns_uscount = ['Demand ALL','US Cont','US Cont Li','brent']
        
        Target_x_uscount = dfnew.loc[test_start:target_end,columns_uscount]
        Traget_y_uscount = dfnew.loc[test_start:target_end,'Charter Rate History Platts']
        Target_x_uscount['t'] = trend
        fcst_uscount = pd.DataFrame(index = pd.date_range(start=target_start, end = target_end, freq = 'MS'))
        for i in fcst_uscount.index:
            
            fcst_uscount.loc[i,'00'] = prob_matrix[0,0]*(r1c1 + r1x1*Target_x_uscount.loc[i,'t'] +  r1x2*Target_x_uscount.loc[i,'Demand ALL'] + r1x3*Target_x_uscount.loc[i,'US Cont'] + r1x4*Target_x_uscount.loc[i,'US Cont Li'] + r1x5*Target_x_uscount.loc[i,'brent']+error)
            fcst_uscount.loc[i,'01'] = (1-prob_matrix[0,0])*(r2c2 + r2x1*Target_x_uscount.loc[i,'t'] +  r2x2*Target_x_uscount.loc[i,'Demand ALL'] + r2x3*Target_x_uscount.loc[i,'US Cont'] + r2x4*Target_x_uscount.loc[i,'US Cont Li'] + r2x5*Target_x_uscount.loc[i,'brent']+error)
            fcst_uscount.loc[i,'10'] = (1-prob_matrix[1,1])*(r1c1 + r1x1*Target_x_uscount.loc[i,'t'] +  r1x2*Target_x_uscount.loc[i,'Demand ALL'] + r1x3*Target_x_uscount.loc[i,'US Cont'] + r1x4*Target_x_uscount.loc[i,'US Cont Li'] + r1x5*Target_x_uscount.loc[i,'brent']+error)
            fcst_uscount.loc[i,'11'] = prob_matrix[1,1]*(r2c2 + r2x1*Target_x_uscount.loc[i,'t'] +  r2x2*Target_x_uscount.loc[i,'Demand ALL'] + r2x3*Target_x_uscount.loc[i,'US Cont'] + r2x4*Target_x_uscount.loc[i,'US Cont Li'] + r2x5*Target_x_uscount.loc[i,'brent']+error)
        fcst_uscount['fcst'] = fcst_uscount.sum(axis=1)/2
        
        #US 50%
        columns_us50 = ['Demand ALL','US 50%','US 50% Li','brent']
        
        Target_x_us50 = dfnew.loc[test_start:target_end,columns_us50]
        Traget_y_us50 = dfnew.loc[test_start:target_end,'Charter Rate History Platts']
        Target_x_us50['t'] = trend
        fcst_us50 = pd.DataFrame(index = pd.date_range(start=target_start, end = target_end, freq = 'MS'))
        for i in fcst_us50.index:
            
            fcst_us50.loc[i,'00'] = prob_matrix[0,0]*(r1c1 + r1x1*Target_x_us50.loc[i,'t'] +  r1x2*Target_x_us50.loc[i,'Demand ALL'] + r1x3*Target_x_us50.loc[i,'US 50%'] + r1x4*Target_x_us50.loc[i,'US 50% Li'] + r1x5*Target_x_us50.loc[i,'brent']+error)
            fcst_us50.loc[i,'01'] = (1-prob_matrix[0,0])*(r2c2 + r2x1*Target_x_us50.loc[i,'t'] +  r2x2*Target_x_us50.loc[i,'Demand ALL'] + r2x3*Target_x_us50.loc[i,'US 50%'] + r2x4*Target_x_us50.loc[i,'US 50% Li'] + r2x5*Target_x_us50.loc[i,'brent']+error)
            fcst_us50.loc[i,'10'] = (1-prob_matrix[1,1])*(r1c1 + r1x1*Target_x_us50.loc[i,'t'] +  r1x2*Target_x_us50.loc[i,'Demand ALL'] + r1x3*Target_x_us50.loc[i,'US 50%'] + r1x4*Target_x_us50.loc[i,'US 50% Li'] + r1x5*Target_x_us50.loc[i,'brent']+error)
            fcst_us50.loc[i,'11'] = prob_matrix[1,1]*(r2c2 + r2x1*Target_x_us50.loc[i,'t'] +  r2x2*Target_x_us50.loc[i,'Demand ALL'] + r2x3*Target_x_us50.loc[i,'US 50%'] + r2x4*Target_x_us50.loc[i,'US 50% Li'] + r2x5*Target_x_us50.loc[i,'brent']+error)
        fcst_us50['fcst'] = fcst_us50.sum(axis=1)/2
        
        
        result2=pd.DataFrame(index = pd.date_range(start=test_start, end = target_end, freq = 'MS'))
        result2.loc[test_start:test_end,'Charter Rate History Platts'] = train_y1
        result2.loc[target_start:target_end,'Charter Rate Fwd curve SPARK'] = df['Charter Rate Fwd curve SPARK']
        #result2.loc[test_start:target_end,'knnfcst'] = knn_y_predict
        #result2.loc[test_start:target_end,'linearfcst'] = reg_predit
        result2.loc[test_start:test_end,'Markov Switching'] = pred
        result2.loc[target_start:target_end,'No US'] = fcst['fcst']
        result2.loc[target_start:target_end,'US Cont'] = fcst_uscount['fcst']
        result2.loc[target_start:target_end,'US 50%'] = fcst_us50['fcst']
        result2.loc[test_start:target_end,'errormax'] = result2.loc[target_start:target_end,'No US']+16717.5
        result2.loc[test_start:target_end,'errormin'] = result2.loc[target_start:target_end,'No US']-16717.5
        result2.loc[test_start:target_end,'platts5yrsmin'] = df.loc[test_start:target_end,'platts5yrsmin']
        result2.loc[test_start:target_end,'platts5yrsmax'] = df.loc[test_start:target_end,'platts5yrsmax']
        #result2.loc[test_start:test_end,'Old'] =  df.loc[test_start:test_end,'Regression']
        #print(result2)
        #result['Meti'] = df['Y']
        #result2.to_csv('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/charter.csv')
        #print((result2['linearfcst']-result2['act'])**2)
        result2.plot(legend=True)
        #print('RMSE linear: ', round(math.sqrt(((result2['linearfcst']-result2['act'])**2).mean()),2))
        #print('RMSE knn: ', round(math.sqrt(((result2['knnfcst']-result2['act'])**2).mean()),2))
        #print('RMSE ms: ', round(math.sqrt(((result2['ms']-result2['act'])**2).mean()),2))
        #print('RMSE old: ', round(math.sqrt(((result2['Old']-result2['act'])**2).mean()),2))
        
        #print('r2 linear: ', round(r2_score(result2['act'], result2['linearfcst']),2))
        #print('r2 knn: ', round(r2_score(result2['act'], result2['knnfcst']),2))
        #print('r2 ms: ', round(r2_score(result2.loc[test_start:test_end,'act'], result2.loc[test_start:test_end,'ms']),2))
        #print('r2 old: ', round(r2_score(result2['act'], result2['Old']),2))
        #print('res std: ', round((result2.loc[test_start:test_end,'act'] - result2.loc[test_start:test_end,'ms']).std(),2))
        return result2
        
'''
df, dfABPBdesk = CharterRate.get_new_data()  
euro_floating = CharterRate.floating_cargos()  
CharterRate.floating_cargos_fcst(euro_floating)
CharterRate.new_model(df, euro_floating)
CharterRate.new_model_out(df, dfABPBdesk)


today=datetime.date.today()
radiolist = []
daterange = pd.date_range(start = str(today.year)+'-'+str(today.month)+'-01', end=str(today.year+1)+'-12-01', freq='MS').date
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
print(radiolist)
   
'''
'''
def test_graph():
    regression = go.Figure() 
            
    regression.add_trace(
            go.Scatter(
                x=df.index,
                y=df['Charter Rate History Platts'].loc[:start_date],
                mode="lines",
                name='Charter Rate History Platts',
                showlegend=True,
                line=dict(color='black', width=2, dash='solid')
                
                )
            )
    regression.add_trace(
            go.Scatter(
                x=df.index,
                y=df['Regression'].loc[:start_date],
                mode="lines",
                name='In sample',
                showlegend=True,
                line=dict(color='red', width=2, dash='solid')
                
                )
            )
    regression.add_trace(
            go.Scatter(
                x=df.index,
                y=df['Charter Rate Fwd curve SPARK'],
                mode="lines",
                name='Charter Rate Fwd curve SPARK',
                showlegend=True,
                line=dict(color='black', width=2, dash='dash')
                
                )
            )
    regression.add_trace(
            go.Scatter(
                x=df.index,
                y=df['Regression'],
                mode="lines",
                name='Out sample',
                showlegend=True,
                line=dict(color='red', width=2, dash='dash')
                
                )
            )
    
    regression.update_layout(
                autosize=True,
                showlegend=True,
                legend=dict(x=0, y=-0.2),
                legend_orientation="h",
                title_text='Regression',
                height=666,
                hovermode='x unified',
                plot_bgcolor = 'white',
                template='ggplot2'  
           )
    regression.update_xaxes(
                showline=True, 
                linewidth=1, 
                linecolor='LightGrey', 
                showgrid=True, 
                gridwidth=1, 
                gridcolor='LightGrey'
            )
    regression.update_yaxes(
                showline=True, 
                linewidth=1, 
                linecolor='LightGrey', 
                showgrid=True, 
                gridwidth=1, 
                gridcolor='LightGrey'
            )
    return regression
'''
'''
UPDADE_INTERVAL = 3600*4
    
def get_new_data_every(period=UPDADE_INTERVAL):
    """Update the data every 'period' seconds"""
    global sen_layout, reg_layout
    
    radiolist = []
    daterange = pd.date_range(start = str(today.year)+'-'+str(today.month)+'-01', end=str(today.year)+'-12-01', freq='MS').date
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
    #print(radiolist) correct
    
    while True:
        CharterRate.get_new_data()
        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print("data updated at: ", now)
        sen_layout = html.Div([
                                html.Div(radiolist
                            
                                    ),
                            
                                dcc.Graph(id='Sensitivity'),
                                dcc.Graph(id='test') 
                            
                            ])

    
    
        reg_layout = html.Div([
                                dcc.Graph(id='test')                      
                            ])

        
        time.sleep(period)
        

app = dash.Dash(__name__)

# get initial data                                                                                                                                                            
#get_new_data()

# we need to set layout to be a function so that for each new page load                                                                                                       
# the layout is re-created with the current data, otherwise they will see                                                                                                     
# data that was generated when the Dash app was first initialised                                                                                                             
#app.layout = make_layout


app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-content')
])


index_page = html.Div([
    dcc.Link('sen', href='/sen'),
    html.Br(),
    dcc.Link('reg', href='/reg'),
])


today=datetime.date.today()
callback_list = []
input_list = []
daterange = pd.date_range(start = str(today.year)+'-'+str(today.month)+'-01', end=str(today.year)+'-12-01', freq='MS').date
for i in daterange:
    callback_list.append(
        Input("'"+str(i)+"'", 'value'),
        )
    input_list.append(i.strftime('%b%y'))
#print(callback_list)

    
input_list = ",".join(input_list)
#print(input_list)

@app.callback(
    Output('Sensitivity', 'figure'),
    Output('test', 'figure'),
    callback_list
    )
def update_graph( Aug23, Sep23, Oct23, Nov23, Dec23):
#def update_graph([input_list]):

    
    df, dfABPBdesk = CharterRate.get_new_data()  
    dfSenfull = CharterRate.new_model_out(df, dfABPBdesk)
    print( Aug23, Sep23, Oct23, Nov23, Dec23)
    start_date = today-relativedelta(years=2)
    dfSen = dfSenfull.loc[start_date:]
    end_date = str(today.year)+'-12-31'
    
    dfSen['option']=dfSen['No US']
    #print(dfSen)
    dfoption = pd.DataFrame([ Aug23, Sep23, Oct23, Nov23, Dec23],index = pd.date_range(start='2023-08-01', end=end_date,freq='MS').date, columns=['option'])    
    
    #create dfsen for US option plot in sen chart
    dfsen_new=pd.DataFrame(dfSen[['No US','US Cont','US 50%']].loc[start_date:].values, index=pd.date_range(start=start_date, end=end_date,freq='MS').date, columns=['No US','US Cont','US 50%'])
    dfsen_new['option']=dfsen_new['No US']
    print(dfsen_new.iloc[-1])
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
                y=dfSen['option'],
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



# Update the index
@app.callback(dash.dependencies.Output('page-content', 'children'),
              [dash.dependencies.Input('url', 'pathname')])
def display_page(pathname):
    if pathname == '/sen':
        return sen_layout
    elif pathname == '/reg':
        return reg_layout
    else:
        return index_page
    # You could also return a 404 "URL not found" page here

# Run the function in another thread
executor = ThreadPoolExecutor(max_workers=1)
executor.submit(get_new_data_every)


if __name__ == '__main__':
    app.run_server(debug=False, use_reloader=False, host='0.0.0.0', port=8060)

'''
   
    
"""
'''chart 1'''
regression = go.Figure() 
            
regression.add_trace(
        go.Scatter(
            x=df.index,
            y=df['Charter Rate History Platts'].loc[:'2021-09-01'],
            mode="lines",
            name='Charter Rate History Platts',
            showlegend=True,
            line=dict(color='black', width=2, dash='solid')
            
            )
        )
regression.add_trace(
        go.Scatter(
            x=df.index,
            y=df['Regression'].loc[:'2021-09-01'],
            mode="lines",
            name='In sample',
            showlegend=True,
            line=dict(color='red', width=2, dash='solid')
            
            )
        )
regression.add_trace(
        go.Scatter(
            x=df.index,
            y=df['Charter Rate Fwd curve SPARK'],
            mode="lines",
            name='Charter Rate Fwd curve SPARK',
            showlegend=True,
            line=dict(color='black', width=2, dash='dash')
            
            )
        )
regression.add_trace(
        go.Scatter(
            x=df.index,
            y=df['Regression'],
            mode="lines",
            name='Out sample',
            showlegend=True,
            line=dict(color='red', width=2, dash='dash')
            
            )
        )

regression.update_layout(
            autosize=True,
            showlegend=True,
            legend=dict(x=0, y=-0.2),
            legend_orientation="h",
            title_text='Regression',
            #yaxis_title="Export (Mcm/d)",
            #xaxis = dict(dtick="M1"),
            hovermode='x unified',
            plot_bgcolor = 'white',
            template='ggplot2'  
       )
regression.update_xaxes(
            showline=True, 
            linewidth=1, 
            linecolor='LightGrey', 
            showgrid=True, 
            gridwidth=1, 
            gridcolor='LightGrey'
        )
regression.update_yaxes(
            showline=True, 
            linewidth=1, 
            linecolor='LightGrey', 
            showgrid=True, 
            gridwidth=1, 
            gridcolor='LightGrey'
        )
py.plot(regression, filename='C:/Users/SVC-PowerUK-Test/YWwebsite/LNG website/analysis/regression.html', auto_open=False)   

'''chart Sensitivity'''
Sensitivity = go.Figure() 
            
Sensitivity.add_trace(
        go.Scatter(
            x=dfSen.index,
            y=dfSen['Charter Rate History Platts'].loc[:'2021-09-01'],
            mode="lines",
            name='Charter Rate History Platts',
            showlegend=True,
            line=dict(color='black', width=2, dash='solid')
            
            )
        )
Sensitivity.add_trace(
        go.Scatter(
            x=dfSen.index,
            y=dfSen['Regression'].loc[:'2021-09-01'],
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
            y=dfSen['Regression'],
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


Sensitivity.update_layout(
            autosize=True,
            showlegend=True,
            legend=dict(x=0, y=-0.2),
            legend_orientation="h",
            title_text='Chart  fwd veiw vs. Scenarios',
            #yaxis_title="Export (Mcm/d)",
            #xaxis = dict(dtick="M1"),
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
py.plot(Sensitivity, filename='C:/Users/SVC-PowerUK-Test/YWwebsite/LNG website/analysis/Sensitivity.html', auto_open=False)  
"""