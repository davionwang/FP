# -*- coding: utf-8 -*-
"""
Created on Tue Jun 14 13:27:36 2022

@author: SVC-GASQuant2-Prod
"""




#V0 Japan Meti forecast, Salman model
#V1 add MS and KNN forecast, YW model

import time
import sys
import numpy as np
import pandas as pd
import plotly.offline as py
import plotly.graph_objs as go
from plotly.subplots import make_subplots
import datetime
import calendar
import plotly.express as px


import numpy as np
import pandas as pd
import requests
import statsmodels.api

from sklearn.neighbors import KNeighborsRegressor


#from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import requests
sys.path.append('C:\\Users\SVC-GASQuant2-Prod\YWwebsite\Python Scripts\class') 
pd.set_option('display.max_columns',20)
from DBtoDF import DBTOPD
import sqlalchemy
import pyodbc

class Japan_METI_FCST():
    
    
    def get_data():
        #Japan Meti data
        meti = DBTOPD('PRD-DB-SQL-209','AnalyticsModel','ts','AnalyticsLatest' )
        dfmeti = meti.get_meti_data()
        dfmeti = dfmeti[['CurveId', 'ValueDate', 'Value']] #mt
        dfmetilast = dfmeti.loc[dfmeti[dfmeti['CurveId']==59328].index,['ValueDate','Value']]
        dfmetilast.set_index('ValueDate', inplace=True)
        dfmetilast.columns=['Meti Latest']
        dfmetilast = dfmetilast.loc['2021-12-05':]/10000
        #print(dfmetilast)
        #Japan Meti data
        
        dfmetiave = dfmeti.loc[dfmeti[dfmeti['CurveId']==59329].index, ['ValueDate','Value']]
        dfmetiave.set_index('ValueDate', inplace=True)
        dfmetiave.fillna(method = 'ffill', inplace=True)
        dfmetiave.replace(to_replace=0, method='bfill', inplace=True)
        demetifull = pd.DataFrame(index = pd.date_range(start = dfmetiave.index[0], end = datetime.date.today()), columns=['Meti Latest','Average Of Last 4 Years'])
        demetifull['Meti Latest'] = dfmetilast
        demetifull['Average Of Last 4 Years'] = dfmetiave
        demetifull.fillna(method = 'ffill', inplace=True)
        #print(dfmetiave)
        #Japan temp 
        temp_hist = DBTOPD('PRD-DB-SQL-211','LNG', 'ana','TempHistNormal' )
        dftemp_hist = temp_hist.sql_to_df()
        dftemp_hist = dftemp_hist[['ValueDate','Japan','Japan normal']]
        dftemp_hist.set_index('ValueDate', inplace=True)
        dftemp_hist.sort_index(inplace=True)
        #dftemp_hist = dftemp_hist.loc['2021-12-01':str(datetime.date.today() - datetime.timedelta(7))]
        
        #print(dftemp_hist)
        temp_hist_end = dftemp_hist.Japan.loc[~dftemp_hist.Japan.isnull()].index[-1]
        temp_fcst = DBTOPD('PRD-DB-SQL-211','LNG', 'ana','TempFcst' )
        dftemp_fcst = temp_fcst.sql_to_df()
        dftemp_fcst = dftemp_fcst[['ValueDate','ecmwf-ens Japan','ecmwf-vareps Japan','ecmwf-mmsf Japan']]
        dftemp_fcst.set_index('ValueDate', inplace=True)
        #dftemp_fcst = dftemp_fcst.loc[str(datetime.date.today() - datetime.timedelta(7)):]
        dftemp_fcst = dftemp_fcst.loc[temp_hist_end:]
           
        #Japan demand                 
        demand = DBTOPD('PRD-DB-SQL-211','LNG', 'ana','[JKC Demand]' )
        dfdemand = demand.sql_to_df()
        dfdemand_jp = dfdemand.loc[:,['index','Japan']]
        dfdemand_jp['index'] = pd.to_datetime(dfdemand_jp['index'])
        dfdemand_jp.set_index('index', inplace=True)
        #print(dfdemand_jp)

                    
        nuk = DBTOPD.get_JP_nuk()
        nuk = nuk[['CurveId','ValueDate','Value']]
        dfnuk = pd.pivot(nuk, index='ValueDate', columns='CurveId')
        dfnuk.fillna(method='ffill', inplace=True)
        #print(dfnuk.sum(axis=1))
        
        power = DBTOPD.get_JP_power()
        power = power[['Date','PowerGenerationFormat','AuthorizationOutput']]
        power = power.groupby(['Date','PowerGenerationFormat'], as_index=False).sum()
        #print(power.index)
        dfpower = pd.pivot(power, index='Date', columns='PowerGenerationFormat')
        dfpower.columns = dfpower.columns.droplevel(0)
        dfpower = dfpower/1000000
        
        #hydro = power.loc[power[power['PowerGenerationFormat']=='Hydropower'].index]
        #hydro = hydro[['Date','AuthorizationOutput']]
        
        #print(hydro.groupby(['Date'],as_index=True).sum().loc['2021-12-01':])
        
        return dfmetilast, demetifull, dftemp_hist,dftemp_fcst, dfnuk, dfpower, dfdemand_jp
    
    def get_temp_hist_gap():
        
         
        sqlConnScrape = pyodbc.connect('DRIVER={SQL SERVER};SERVER=PRD-DB-SQL-211;Trusted_Connection=yes')
        sqlens = '''
                        SELECT * FROM Meteorology.dbo.WeatherStationTimeSeriesHistory
                        WHERE ParameterName = 't_2m:C' AND ModelSourceName = 'ecmwf-ens' and WeightingName = 'temperature'
                        AND ForecastDate = {time}
            
                          '''.format(time='\''+str(datetime.date.today()-relativedelta(days=10))+' 00:00:00'+'\'')
            
        dfens = pd.read_sql(sqlens, sqlConnScrape)
        #print(df)
        dfens=dfens[['ValueDate','CountryName','Weighting','Value']]
        dfens['weighted'] = dfens['Weighting']*dfens['Value']
        dfens['ValueDate'] = pd.to_datetime(dfens['ValueDate'])
        #print(i, df)
        dfpivot = dfens.pivot_table(values='weighted',index='ValueDate',columns='CountryName',aggfunc=np.sum)
        dfpivot = dfpivot.resample('D').mean()
        
        sqlensjwa = '''
                        SELECT * FROM Meteorology.dbo.WeatherStationTimeSeriesHistory
                        WHERE ParameterName = 't_2m:C' AND ModelSourceName = 'jwa-14-d' and WeightingName = 'temperature'
                        AND ForecastDate = {time}
            
                          '''.format(time='\''+str(datetime.date.today()-relativedelta(days=10))+' 00:00:00'+'\'')
            
        dfensjwa = pd.read_sql(sqlensjwa, sqlConnScrape)
        #print(df)
        dfensjwa=dfensjwa[['ValueDate','CountryName','Weighting','Value']]
        dfensjwa['weighted'] = dfensjwa['Weighting']*dfensjwa['Value']
        dfensjwa['ValueDate'] = pd.to_datetime(dfensjwa['ValueDate'])
        #print(i, df)
        dfpivotjwa = dfensjwa.pivot_table(values='weighted',index='ValueDate',columns='CountryName',aggfunc=np.sum)
        dfpivotjwa = dfpivotjwa.resample('D').mean()
        
        dfpivot_weight = dfpivot*0.35 + dfpivotjwa*0.65
        #print(dfpivot)
        return dfpivot_weight
    
    
    def get_fx():
        
        today = datetime.date.today()
        
        fx = DBTOPD('PRD-DB-SQL-211','LNG', 'ana','[FXJPEURcurve]').sql_to_df()
        fx['index'] = pd.to_datetime(fx['index'])
        fx.set_index('index', inplace=True)
        
        dffx = fx.resample('D').fillna(method = 'ffill')
        #print(dffx)
        fxhist = DBTOPD('PRD-DB-SQL-211','LNG', 'ana','[FXJPYHist]').sql_to_df()
        fxhist['date'] = pd.to_datetime(fxhist['date'])
        fxhist.set_index('date', inplace=True)
        #print(fxhist)
        dffxall = pd.concat([fxhist['value'], dffx.loc[today:,'jpyusd']], names='jpyusd')
        
        #dffxall.rename(columns=['jpyusd'], inplace=True)
        #print(dffxall)
        
        return dffxall

    
    #Salman Model
    def model_data(dfmetilast, dftemp_hist, dftemp_fcst, dfnuk, dfpower):
        
        today = datetime.date.today()
        wed2 = today + datetime.timedelta(7+ (2-today.weekday()) % 7 )
        #print(dftemp_hist.Japan.loc[~dftemp_hist.Japan.isnull()].index[-1])
        temp_hist_end = dftemp_hist.Japan.loc[~dftemp_hist.Japan.isnull()].index[-1]
        df = pd.DataFrame(index = pd.date_range(start = '2021-12-01', end = wed2))
        #df.loc[:str(today - datetime.timedelta(7)),'temp'] = dftemp_hist['Japan']
        #df.loc[str(today - datetime.timedelta(7)):,'temp'] = dftemp_fcst['Japan ens']
        #hist end error use ens hist instead
        df.loc[:temp_hist_end,'temp'] = dftemp_hist['Japan']
        df.loc[temp_hist_end:,'temp'] = dftemp_fcst['Japan ens']
        df['Nuclear power'] = dfnuk.sum(axis=1)
        df['Thermal power (coal)'] = dfpower['Thermal power (coal)']
        df['Hydropower'] = dfpower['Hydropower']
        df.fillna(method='ffill', inplace=True)
        #df.to_csv('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/meti fcst.csv')

        #print(df)
        
        dfmodel = pd.DataFrame(index = pd.date_range(start = '2021-12-01', end = wed2))
        dfmodel['METI'] = dfmetilast
        dfmodel.fillna(method='bfill', inplace=True)
        dfmodel['Forecast'] = 4.93623903896347*df['temp'] + 0.0173846655760251*df['Nuclear power'] + 9.97809933781612*df['Thermal power (coal)'] -0.406509422929403*df['Hydropower'] - 63.9271947951422
        dfmodel['Delta'] = dfmodel['Forecast'] - dfmodel['METI'] 
        dfmodel['weekly fcst ave.'] = np.nan
        
        #print(dfmetilast.shape[0])
        for i in range(dfmetilast.shape[0]-1):
            dfmodel.loc[dfmetilast.index[i],'weekly fcst ave.'] = dfmodel.loc[dfmetilast.index[i]:dfmetilast.index[i+1],'Forecast'].mean()
        #print(dfmodel)
        dfmodel['weekly fcst ave.'].fillna(method='ffill', inplace=True)
        dfmodel = dfmodel.round(2)
        #print(dfmodel.head(20))
        
        
        return dfmodel
    
   

    
    #YW model
    #MS model
    def ms_model(dfmetilast, dftemp_hist, dftemp_fcst, dfdemand_jp):
        
        #JP demand hist 2021-12-01 : 2022-03-31
        dfdemandhist = pd.read_excel('C:\\Users\SVC-GASQuant2-Prod\YWwebsite\data\JP_METI_HIST.xlsx', sheet_name=0, header=0) #update every season
        dfdemandhist.set_index('date', inplace=True)
        #print(df)
        
        #date for model
        start = '2021-12-01'
        #start = '2022-04-01'
        end = dftemp_fcst.index[np.where(np.isnan(dftemp_fcst['Japan ens']))][0] - relativedelta(days = 1) #'2022-06-08'
        
        temp_hist_end = dftemp_hist.Japan.loc[~dftemp_hist.Japan.isnull()].index[-1]
        
        #print(dfdemand_jp.loc['2022-06-18':'2022-06-25'])
        
        dfms = pd.DataFrame(index = pd.date_range(start = start, end = end), columns = ['Y-RCP','Japan Temp'])
        dfms['METI'] = dfmetilast
        dfms.fillna(method='bfill', inplace=True)
        dfms['JP demand'] = dfdemandhist
        dfms.loc['2022-10-01':end,'JP demand'] = dfdemand_jp.loc['2022-10-01':end,'Japan']
        dfms.loc[:,'Y-RCP'] = dfms['METI'] - dfms['JP demand']
        dfms.loc[:,'Y-RCP'].fillna(method = 'ffill', inplace=True)
        #dfms['Japan Temp'] = dftemp_hist.loc[start:dftemp_fcst.index[0],'Japan']
        #dfms['Japan Temp'].loc[dftemp_fcst.index[0]:] = dftemp_fcst['Japan ens']
        
        dfms['Japan Temp'] = dftemp_hist.loc[start:temp_hist_end,'Japan']
        dfms['Japan Temp'].loc[temp_hist_end:] = dftemp_fcst['Japan ens']
        #print(dfms[['Japan Temp']].info())
        #print(dfms.loc[:,['JP demand']].info())
        
        #print(dfms[['Japan Temp']].info())
        test_start = dfmetilast.index[-1]
        
        mod_hamilton = statsmodels.tsa.regime_switching.markov_regression.MarkovRegression(
            endog=dfms.loc[:, ['Y-RCP']], 
            exog = dfms.loc[:, ['Japan Temp']] ,
            #switching_variance=True,
            k_regimes=2,
            #trend='ct',
            #order=2,
            #switching_trend=True, 
            #switching_exog=True,
            )
        
        
        
       
        res_hamilton = mod_hamilton.fit()
        
        #print(res_hamilton.summary())
        #res_hamilton.smoothed_marginal_probabilities[1].plot(title="Probability of being in the high regime", figsize=(12, 3))
        pred = res_hamilton.predict(start=start, end=end)
        #print(pred)
        result = pd.concat([dfms['Y-RCP'],pred], axis=1)
        #result['fcst'] = pred
        #print(result)
        #result.plot(legend=True)
        return result, dfms
    
    def knn(dfmetilast, dftemp_hist, dftemp_fcst, dfdemand_jp, dfms):
        
        #always use lastest temp hist for train, target use ens
        dfknn = dfms.copy()
        #print(dftemp_hist)
        for i in dfknn.index:
            #print(i)
            dfknn.loc[i, 'Japan normal'] = dftemp_hist.loc['2020-'+str(i.month)+'-'+str(i.day), 'Japan normal']
        #print(dfknn)
        
        columns = ['Japan normal','Japan Temp']
        
        test_start = '2021-12-01'
        test_end = dftemp_fcst.index[0]- relativedelta(days = 1)
        target_start = dftemp_fcst.index[0]
        target_end = dftemp_fcst.index[np.where(np.isnan(dftemp_fcst['Japan ens']))][0] - relativedelta(days = 1)
        #print(dfknn.loc['2022-09-01':test_end])
        train_X1, train_y1 = dfknn.loc[test_start:test_end,columns], dfknn.loc[test_start:test_end,'JP demand']
        
        
        Target_x = dfknn.loc[test_start:target_end, columns]
        #print(Target_x)
        test_y = dfknn.loc[test_start:target_end,'JP demand']
        
        #print(dftemp_fcst.index)
        #print(train_y1)
        #print('-------------------KNN---------------------')
        knn=KNeighborsRegressor(n_neighbors=3, #weights='distance')
                                weights='uniform')
        knn.fit(train_X1,train_y1)
        knn_y_predict = knn.predict(Target_x)
        
        ##########################################
        
        result=pd.DataFrame(index = pd.date_range(start=test_start, end = target_end))
        result.loc[test_start:target_end,'in sample'] = test_y
        result.loc[test_start:target_end,'out sample'] = knn_y_predict
        #result['Meti'] = df['Y']
        #result.to_csv('H:\Yuefeng\LNG Projects\JKC demand\Japan inventory fcst.csv')
        #result.plot(legend=True)
        #print(result)
        
        return result

    def threshold(dfmetilast, demetifull, dftemp_hist, dftempgap, dftemp_fcst, dfpower, dffx):
        
        
        #print(dfpower.loc['2021-12-01':])
        #print(dfpower)
        
        today = datetime.date.today()
        wed1 =  today + datetime.timedelta((2-today.weekday()) % 7 )
        wed2 = today + datetime.timedelta(21+ (2-today.weekday()) % 7 )
        #print(dftemp_hist.Japan.loc[~dftemp_hist.Japan.isnull()].index[-1])
        temp_hist_end = dftemp_hist.Japan.loc[~dftemp_hist.Japan.isnull()].index[-1]
        

        
        df = pd.DataFrame(index = pd.date_range(start = '2021-12-01', end = wed2))
        #df.loc[:str(today - datetime.timedelta(7)),'temp'] = dftemp_hist['Japan']
        #df.loc[str(today - datetime.timedelta(7)):,'temp'] = dftemp_fcst['Japan ens']
        #hist end error use ens hist instead
        df['Average Of Last 4 Years'] = demetifull['Average Of Last 4 Years']
        df['FX'] = dffx
        df.loc[:temp_hist_end,'temp'] = dftemp_hist['Japan']
        df.loc[temp_hist_end:today,'temp'] = dftempgap.loc[temp_hist_end:today,'Japan']
        df.loc[temp_hist_end:,'temp'] = dftemp_fcst['ecmwf-ens Japan']
        df['Nuclear power'] = dfpower['Nuclear power']
        df['Thermal power (coal)'] = dfpower['Thermal power (coal)']
        df.fillna(method='ffill', inplace=True)
        #print(df.loc['2022-01-10':])
        
        for i in df.index:
            
            if df.loc[i, 'Average Of Last 4 Years'] < 1840000:
                df.loc[i,'fcst'] = 0.000150851754805*df.loc[i,'Average Of Last 4 Years'] + 2.87111963729*df.loc[i,'FX'] - 13.4273523642*df.loc[i,'Nuclear power']
            if 1900000 >df.loc[i, 'Average Of Last 4 Years']>=1840000:
                df.loc[i,'fcst'] = 0.000118175807979*df.loc[i,'Average Of Last 4 Years'] + 1.47819383585*df.loc[i,'FX'] - 5.30105563448*df.loc[i,'Nuclear power']
            if 1900000 <=df.loc[i, 'Average Of Last 4 Years']<1980000:
                df.loc[i,'fcst'] = 0.000115301389051*df.loc[i,'Average Of Last 4 Years'] + 1.87739634029*df.loc[i,'FX'] - 7.69704109452*df.loc[i,'Nuclear power']
            if df.loc[i, 'Average Of Last 4 Years']>=1980000:
                df.loc[i,'fcst'] = -0.000070596303824033*df.loc[i,'Average Of Last 4 Years'] + 4.3989868775*df.loc[i,'FX'] - 6.6311296609*df.loc[i,'Nuclear power']
        '''
        test_date = ''
        test1 = 0.000150851754805*df.loc[test_date,'Average Of Last 4 Years'] + 2.87111963729*df.loc[test_date,'FX'] - 13.4273523642*df.loc[test_date,'Nuclear power']
        test2 = df.loc[i,'fcst'] = 0.000118175807979*df.loc[i,'Average Of Last 4 Years'] + 1.47819383585*df.loc[i,'FX'] - 5.30105563448*df.loc[i,'Nuclear power']
        test3 = df.loc[i,'fcst'] = 0.000115301389051*df.loc[i,'Average Of Last 4 Years'] + 1.87739634029*df.loc[i,'FX'] - 7.69704109452*df.loc[i,'Nuclear power']
        test4 = df.loc[i,'fcst'] = -0.000070596303824033*df.loc[i,'Average Of Last 4 Years'] + 4.3989868775*df.loc[i,'FX'] - 6.6311296609*df.loc[i,'Nuclear power']
        '''
        #print(wed1)
        dfmetilast['METI date'] = dfmetilast.index
        df1 = pd.concat([df['fcst'], dfmetilast['METI date']], axis=1)
        df1.fillna(method = 'bfill', inplace=True)
        df1.loc[pd.to_datetime(wed1),'METI date'] = wed1
        df1.loc[pd.to_datetime(wed2),'METI date'] = wed2
        #print(df1)
        #df1 = df1.loc[:wed2]
        dfg = df1.groupby(by='METI date').mean()
        #print(df1.loc[today:])
        #print(dfmetilast)
        
        df['weekly fcst'] = dfg
        df['METI'] = dfmetilast['Meti Latest']
        df.fillna(method='bfill', inplace=True)
        #df.to_csv('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/meti fcst.csv')
        #print(df.loc['2022-01-10':'2022-02-25'].info())
        #print(df.loc['2023-05-19':])
        
        
        #print(df)
        return df

    def chart_data(ms_result, knn_result, dfmetilast):
        
        #print(ms_result.columns)
        print(dfmetilast)
        
        result = pd.concat([ms_result[0], knn_result['out sample']], axis = 1)
        result['fcst'] = result.sum(axis=1)
        result['METI'] = dfmetilast
        result.fillna(method='bfill', inplace=True)
        result['Delta'] = result['fcst'] - result['METI'] 
        result['weekly fcst ave.'] = np.nan
        
        #print(dfmetilast.shape[0])
        for i in range(dfmetilast.shape[0]-1):
            result.loc[dfmetilast.index[i],'weekly fcst ave.'] = result.loc[dfmetilast.index[i]:dfmetilast.index[i+1],'fcst'].mean()
        #print(dfmodel)
        result.loc[str(dfmetilast.index[-1]+relativedelta(days=1)),'weekly fcst ave.'] = result.loc[str(dfmetilast.index[-1]+relativedelta(days=1)):str(dfmetilast.index[-1]+relativedelta(days=7)),'fcst'].mean()
        result.loc[str(dfmetilast.index[-1]+relativedelta(days=8)),'weekly fcst ave.'] = result.loc[str(dfmetilast.index[-1]+relativedelta(days=8)):str(dfmetilast.index[-1]+relativedelta(days=14)),'fcst'].mean()
        result['weekly fcst ave.'].fillna(method='ffill', inplace=True)
        result = result.round(2)
        #print(result.tail(20))
        return result
     
    def chart(dfmodel,dfmetilast, result_yw, demetifull):
        
        dftext = pd.DataFrame(index = dfmodel.index, columns = ['METI','FCST'])
        for i in dfmetilast.index:
            dftext.loc[i,'METI'] = dfmetilast.loc[i, 'Meti Latest']
            dftext.loc[i,'FCST'] = dfmodel.loc[i,'weekly fcst ave.']
            dftext.loc[i,'FCST_YW'] = result_yw.loc[i,'weekly fcst ave.']
        dftext.fillna('',inplace=True)
            
        fig = go.Figure() 
            
        fig.add_trace(
                go.Scatter(
                    x=dfmodel.index,
                    y=dfmodel['METI'],
                    mode="lines+text",
                    name='METI',
                    showlegend=True,
                    line=dict(color='grey', width=2, dash='solid'),
                    text = dftext['METI'],
                    textposition="top center",
                    textfont=dict(
                    family="sans serif",
                    size=12,
                    color="grey")
                    
                    )
                )
        fig.add_trace(
                go.Scatter(
                    x=dfmodel.index,
                    y=dfmodel['Forecast'],
                    mode="lines",
                    name='Forecast Salman',
                    showlegend=True,
                    line=dict(color='blue', width=2, dash='dash'),
                    visible='legendonly',
                    
                    )
                )
        fig.add_trace(
                go.Scatter(
                    x=dfmodel.index,
                    y=dfmodel['weekly fcst ave.'],
                    mode="lines+text",
                    name='Fcst Weely avg Salman',
                    showlegend=True,
                    line=dict(color='red', width=2, dash='dash'),
                    text = dftext['FCST'],
                    textposition="bottom center",
                    textfont=dict(
                    family="sans serif",
                    size=12,
                    color="red"),
                    visible='legendonly',
                    
                    )
                )
        #print(df_cumulative[p+' Cum Difference'].loc[date_dict['current_year_start']:date_dict['last_month']])
        fig.add_trace(
                go.Bar(
                    x=dfmodel.index,
                    y=dfmodel['Delta'],
                    name='Forecast Salman - METI Delta',
                    #width=1,
                
                    showlegend=True,
                    marker_color='Silver',
                    text=dfmodel['Delta'].loc[:dfmetilast.index[-1]],
                    textposition='outside',
                    textfont=dict(
                    #family="sans serif",
                    size=12,
                    ),
                    visible='legendonly',
                    )
                ) 

        fig.add_trace(
                go.Scatter(
                    x=result_yw.index,
                    y=result_yw['fcst'],
                    mode="lines",
                    name='Forecast YW',
                    showlegend=True,
                    line=dict(color='blue', width=2, dash='dash')
                    
                    )
                )
        fig.add_trace(
                go.Scatter(
                    x=result_yw.index,
                    y=result_yw['weekly fcst ave.'],
                    mode="lines+text",
                    name='Fcst Weely avg YW',
                    showlegend=True,
                    line=dict(color='red', width=2, dash='dash'),
                    text = dftext['FCST'],
                    textposition="bottom center",
                    textfont=dict(
                    family="sans serif",
                    size=12,
                    color="red")
                    
                    )
                )
        
        fig.add_trace(
            go.Scatter(
                x=result_yw.index,
                y=demetifull.loc[result_yw.index[0]:,'Average Of Last 4 Years']/10000,
                mode='lines',
                name='Average Of Last 4 Years',
                line=dict(color='black', dash='solid')
                    )
                )
        #print(df_cumulative[p+' Cum Difference'].loc[date_dict['current_year_start']:date_dict['last_month']])
        fig.add_trace(
                go.Bar(
                    x=result_yw.index,
                    y=result_yw['Delta'],
                    name='Forecast YW - METI Delta',
                    #width=1,
                
                    showlegend=True,
                    marker_color='Silver',
                    text=result_yw['Delta'].loc[:dfmetilast.index[-1]],
                    textposition='outside',
                    textfont=dict(
                    #family="sans serif",
                    size=12,
                    )
                    )
                ) 

        fig.update_layout(
            autosize=True,
            showlegend=True,
            legend=dict(x=0, y=-0.2),
            legend_orientation="h",
            title_text='Weekly METI Regression '+ str(datetime.date.today()),
            #yaxis_title="Exports (Mt)",
            #xaxis = dict(dtick="M1"),
            hovermode='x unified',
            plot_bgcolor = 'white',
            template='ggplot2'  ,
            bargap  = 0
        )
        fig.update_xaxes(
            showline=True, 
            linewidth=1, 
            linecolor='LightGrey', 
            showgrid=False, 
            gridwidth=1, 
            gridcolor='LightGrey'
        )
        fig.update_yaxes(
            showline=True, 
            linewidth=1, 
            linecolor='LightGrey', 
            showgrid=True, 
            gridwidth=1, 
            gridcolor='LightGrey'
        )
        py.plot(fig, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/JKstock/JPMETIFcst.html', auto_open=False) 
        fig.write_image('U:/Trading - Gas/LNG/LNG website/analysis/JKstock/JPMETIFcst.png')
   
    def chart_yw(result_yw, dfmetilast, demetifull):
        
        dftext = pd.DataFrame(index = result_yw.index, columns = ['METI','FCST'])
        for i in dfmetilast.index:
            dftext.loc[i,'METI'] = dfmetilast.loc[i, 'Meti Latest']
            dftext.loc[i,'FCST'] = result_yw.loc[i,'weekly fcst']
        dftext.fillna('',inplace=True)
            
        fig = go.Figure() 
            
        fig.add_trace(
                go.Scatter(
                    x=result_yw.index,
                    y=result_yw['METI'],
                    mode="lines+text",
                    name='METI',
                    showlegend=True,
                    line=dict(color='grey', width=2, dash='solid'),
                    #text = dftext['METI'],
                    textposition="top center",
                    textfont=dict(
                    family="sans serif",
                    size=12,
                    color="grey")
                    
                    )
                )
        fig.add_trace(
                go.Scatter(
                    x=result_yw.index,
                    y=result_yw['fcst'],
                    mode="lines",
                    name='Forecast',
                    showlegend=True,
                    line=dict(color='blue', width=2, dash='dash')
                    
                    )
                )
        fig.add_trace(
                go.Scatter(
                    x=result_yw.index,
                    y=result_yw['weekly fcst'],
                    mode="lines+text",
                    name='Fcst Weely avg',
                    showlegend=True,
                    line=dict(color='red', width=2, dash='solid'),
                    #text = dftext['FCST'],
                    textposition="bottom center",
                    textfont=dict(
                    family="sans serif",
                    size=12,
                    color="red")
                    
                    )
                )
        #print(df_cumulative[p+' Cum Difference'].loc[date_dict['current_year_start']:date_dict['last_month']])
        fig.add_trace(
                go.Bar(
                    x=result_yw.index,
                    y=result_yw['weekly fcst'] - result_yw['METI'],#result_yw['Delta'],
                    name='Forecast - METI Delta',
                    #width=1,
                
                    showlegend=True,
                    marker_color='Silver',
                    text=result_yw['METI'].loc[:dfmetilast.index[-1]],
                    textposition='outside',
                    textfont=dict(
                    #family="sans serif",
                    size=12,
                    )
                    )
                ) 
        fig.add_trace(
            go.Scatter(
                x=result_yw.index,
                y=demetifull.loc[result_yw.index[0]:,'Average Of Last 4 Years']/10000,
                mode='lines',
                name='Average Of Last 4 Years',
                line=dict(color='black', dash='solid')
                    )
                )

        '''
        bardate=[]
        
        for i in range (1,today.month):
            this_month_start = datetime.datetime(today.year, i , 1)
            bardate.append(this_month_start.strftime("%Y-%m-%d"))
        for j in range(0, today.month -1 ):
            fig.add_annotation(x=bardate[j], y=5,
                    text=df_cumulative[p+' Cum Difference'].loc[bardate[j]],
                    showarrow=False)
        #print(Basin)    
        '''

        fig.update_layout(
            autosize=True,
            showlegend=True,
            legend=dict(x=0, y=-0.2),
            legend_orientation="h",
            title_text='Weekly METI Regression '+ str(datetime.date.today()),
            #yaxis_title="Exports (Mt)",
            #xaxis = dict(dtick="M1"),
            hovermode='x unified',
            plot_bgcolor = 'white',
            template='ggplot2'  ,
            bargap  = 0
        )
        fig.update_xaxes(
            showline=True, 
            linewidth=1, 
            linecolor='LightGrey', 
            showgrid=False, 
            gridwidth=1, 
            gridcolor='LightGrey'
        )
        fig.update_yaxes(
            showline=True, 
            linewidth=1, 
            linecolor='LightGrey', 
            showgrid=True, 
            gridwidth=1, 
            gridcolor='LightGrey'
        )
        py.plot(fig, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/JKstock/JPMETIFcst_YW.html', auto_open=False)
        fig.write_image('U:/Trading - Gas/LNG/LNG website/analysis/JKstock/JPMETIFcst_YW.png')
    
    
    def fcst_table(dfmetilast):
        
        date_list=[]
        
        
        meti = DBTOPD('PRD-DB-SQL-211','LNG','ana','JPMETIFCST').sql_to_df()
        meti = meti[['index','weekly fcst','ts']]
        meti.set_index('index', inplace=True)
        #group = meti.groupby(by=['ts','index']).mean()
        #print(dfmetilast)
        pivot = pd.pivot_table(meti, values= 'weekly fcst', index='index',columns='ts')
        pivot = pivot.resample('D',axis=1).mean()
        
        pivot.dropna(axis=1,how='all', inplace=True)
        pivot = pivot.round(2)
        #print(pivot.loc[:,'2023-05-28':])
        
        w0 = dfmetilast.index[-1]
        weekindex = [dfmetilast.index[-5],dfmetilast.index[-4],dfmetilast.index[-3],dfmetilast.index[-2],
                     w0,
                     w0+relativedelta(weeks=1),w0+relativedelta(weeks=2),w0+relativedelta(weeks=3),w0+relativedelta(weeks=4),]
        #print(str(dfmetilast.index[-2]))
        #print(pivot.loc[weekindex,str(dfmetilast.index[-2])])
        
        
        neardate={  'Fcst0':min(pivot.columns, key=lambda x: abs(x - w0)),
                    'Fcst-1':min(pivot.columns, key=lambda x: abs(x - dfmetilast.index[-2])),
                    'Fcst-2':min(pivot.columns, key=lambda x: abs(x - dfmetilast.index[-3])),
                    'Fcst-3':min(pivot.columns, key=lambda x: abs(x - dfmetilast.index[-4])),
                    'Fcst-4':min(pivot.columns, key=lambda x: abs(x - dfmetilast.index[-5])),
            }
        #print(neardate.values())
        
        columns = ['METI Date','METI Act','Fcst0','Fcst-1','Fcst-2','Fcst-3','Fcst-4']
        dftable = pd.DataFrame(index = ['Week-4','Week-3','Week-2','Week-1','Week0','Week+1','Week+2','Week+3','Week+4'],columns=columns)
        dftable.loc[['Week-4','Week-3','Week-2','Week-1','Week0'],'METI Act'] = dfmetilast['Meti Latest'].iloc[-5:].values
        dftable['METI Date'] = weekindex
        dftable['METI Date'] = dftable['METI Date'].dt.date
        dftable['Fcst0'] = pivot.loc[weekindex,neardate['Fcst0']].values
        
        dftable['Fcst-1'] = pivot.loc[weekindex,neardate['Fcst-1']].values
        dftable['Fcst-2'] = pivot.loc[weekindex,neardate['Fcst-2']].values
        dftable['Fcst-3'] = pivot.loc[weekindex,neardate['Fcst-3']].values
        dftable['Fcst-4'] = pivot.loc[weekindex,neardate['Fcst-4']].values
        
        
        #print(dftable)
        dftabledelta = dftable.copy()
        for i in ['Fcst0','Fcst-1','Fcst-2','Fcst-3','Fcst-4']:
            dftabledelta[i] = dftabledelta[i] - dftabledelta['METI Act']
        dftabledelta[['Fcst0','Fcst-1','Fcst-2','Fcst-3','Fcst-4']] = dftabledelta[['Fcst0','Fcst-1','Fcst-2','Fcst-3','Fcst-4']].astype('float').round(2)
        #print(dftabledelta)
        dftable.fillna(' ', inplace=True)
        
        
        #table 2 color
        df2color=dftabledelta.copy()
        dftabledelta.fillna(' ', inplace=True)
        
        for i in df2color.index:
            for j in df2color.columns[2:]:
                if df2color.loc[i,j] >10:
                    df2color.loc[i,j] = 'LightGreen'
                elif df2color.loc[i,j] <-10:
                    df2color.loc[i,j] = 'pink'
                else:
                    df2color.loc[i,j] = 'white'
        
        df2color.insert(0,'index',['paleturquoise']*df2color.shape[0])
        df2color['METI Date'] = 'paleturquoise'
        #df2color['METI Date'] = 'paleturquoise'
        df2color['METI Act'] = 'paleturquoise'
        df2color.loc['Week0',['index','METI Date'   ,    'METI Act']] = 'yellow'
        #df2color.loc['Week0',[]]
        #print(df2color)
        df2color=df2color.T
        
        table_header = [[' ', 'Week'],
                              [' ', dftable.columns[0]],
                              [' ', dftable.columns[1]],
                              [neardate['Fcst0'].date(), dftable.columns[2]],
                              [neardate['Fcst-1'].date(), dftable.columns[3]],
                              [neardate['Fcst-2'].date(), dftable.columns[4]],
                              [neardate['Fcst-3'].date(), dftable.columns[5]],
                              [neardate['Fcst-4'].date(), dftable.columns[6]],]
        
        fig1 = go.Figure(data=[go.Table(
                        header=dict(values=table_header,
                                    fill_color='paleturquoise',
                                    align='center'),
                        cells=dict(values=[dftable.index, dftable['METI Date'],dftable['METI Act'], dftable['Fcst0'], dftable['Fcst-1'], dftable['Fcst-2'],dftable['Fcst-3'], dftable['Fcst-4']],
                                   #fill_color='lavender',
                                   fill=dict(color=df2color.values.tolist()),
                                   #font=dict(color="white"),
                                   align='center'))
                    ])
        
        fig1.update_layout(
             title_text='Japan METI Fcst, Mt '+ str(datetime.date.today()),
         )
        
        py.plot(fig1, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/JKstock/METItable.html', auto_open=False)
        #fig1.write_image('U:/Trading - Gas/LNG/LNG website/analysis/Japan power price/JapPowerTable1.png')
        
        fig2 = go.Figure(data=[go.Table(
                       header=dict(values=table_header,
                                   fill_color='paleturquoise',
                                   align='center'),
                       cells=dict(values=[dftable.index,dftabledelta['METI Date'],dftabledelta['METI Act'], dftabledelta['Fcst0'], dftabledelta['Fcst-1'], dftabledelta['Fcst-2'],dftabledelta['Fcst-3'], dftabledelta['Fcst-4']],
                                  #fill_color='lavender',
                                  fill=dict(color=df2color.values.tolist()),
                                  align='center'))
                   ])
        fig2.update_layout(
             title_text='Japan METI Delta Fcst - Act, Mt '+ str(datetime.date.today()),
         )
        py.plot(fig2, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/JKstock/METItableDelta.html', auto_open=False)
        #fig2.write_image('U:/Trading - Gas/LNG/LNG website/analysis/Japan power price/JapPowerTable2.png')
   
    
    def update():
        
        a=Japan_METI_FCST
        dfmetilast, demetifull, dftemp_hist,dftemp_fcst, dfnuk, dfpower, dfdemand_jp = a.get_data()
        dftempgap = a.get_temp_hist_gap()
        dffx = a.get_fx()
        #dfmodel = a.model_data(dfmetilast, dftemp_hist, dftemp_fcst, dfnuk, dfpower)
        
        #print(dfmetilast)
        #print(dftemp_hist)
        #print(dftemp_fcst)
        #print(dfdemand_jp)
        
        #ms_result, dfms = a.ms_model(dfmetilast, dftemp_hist, dftemp_fcst, dfdemand_jp)
        #knn_result = a.knn(dfmetilast, dftemp_hist, dftemp_fcst, dfdemand_jp, dfms)
        tresult = a.threshold(dfmetilast, demetifull, dftemp_hist, dftempgap, dftemp_fcst, dfpower,dffx)
        #result_yw = a.chart_data(tresult, knn_result, dfmetilast)
        a.chart_yw(tresult, dfmetilast, demetifull)
        #a.chart(dfmodel, dfmetilast,result_yw, demetifull)
        #print(result_yw.tail(30))
        tresult['ts'] = datetime.datetime.today()
        
        db_server_lng = "PRD-DB-SQL-211"
        db_name_lng = "LNG"
        sql_engine_lng = sqlalchemy.create_engine("mssql+pyodbc://" + db_server_lng + "/" + db_name_lng + "?trusted_connection=yes&driver=ODBC+Driver+13+for+SQL+Server")
        
        tresult.to_sql(name='JPMETIFCST', con=sql_engine_lng, index=True, if_exists='append', schema='ana')
        
        
        a.fcst_table(dfmetilast)
        
Japan_METI_FCST.update()