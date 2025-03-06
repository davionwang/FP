# -*- coding: utf-8 -*-
"""
Created on Mon Sep 11 13:54:40 2023

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


#import CEtools
from CEtools import *
ce=CEtools.CElink('swasti@freepoint.com','kfGa5uwL1+')
pd.set_option('display.max_columns',20)
from DBtoDF import DBTOPD


class Fundamental_research():
    
    def get_data():
        
        today = datetime.date.today()
        
        DateFrom='01_Jan_2018'        
        DateTo=today.strftime('%d_%b_%Y')
        
        #ce=CElink(user_name,password)
        #monthlystats = ce.eugasmonthly(id=51) #total monthly data
        
        cepipe = ce.eugasseries(id='50715,	50710,	50818,	50713,	50820,	50816', dateFrom=DateFrom,dateTo=DateTo,unit='mcm',nd='10')
        cepipe.set_index('Date', inplace=True)
        cepipe.drop(['DateExcel'],axis=1, inplace=True)
        #rename 
        rename=CErename.CEid(cepipe)
        cepipe=rename.replace()
        cepipe=cepipe.fillna(method='ffill')
        #print(cepipe)
        
        cehydro = ce.eugasseries(id='54743,	54755', dateFrom=DateFrom,dateTo=DateTo,unit='mcm',nd='10')
        cehydro.set_index('Date', inplace=True)
        cehydro.drop(['DateExcel'],axis=1, inplace=True)
        #rename 
        rename=CErename.CEid(cehydro)
        cehydro=rename.replace()
        cehydro=cehydro.fillna(method='ffill')
        #print(cehydro)
        
        cewind = ce.eugasseries(id='54767', dateFrom=DateFrom,dateTo=DateTo,unit='mcm',nd='10')
        cewind.set_index('Date', inplace=True)
        cewind.drop(['DateExcel'],axis=1, inplace=True)
        #rename 
        rename=CErename.CEid(cewind)
        cewind=rename.replace()
        cewind=cewind.fillna(method='ffill')
        #print(cewind)
        
        cecoal = ce.eugasseries(id='54797,	54761,	54749', dateFrom=DateFrom,dateTo=DateTo,unit='mcm',nd='10')
        cecoal.set_index('Date', inplace=True)
        cecoal.drop(['DateExcel'],axis=1, inplace=True)
        #rename 
        rename=CErename.CEid(cecoal)
        cecoal=rename.replace()
        cecoal=cecoal.fillna(method='ffill')
        #print(cecoal)
        ceoil = ce.eugasseries(id='54779', dateFrom=DateFrom,dateTo=DateTo,unit='mcm',nd='10')
        ceoil.set_index('Date', inplace=True)
        ceoil.drop(['DateExcel'],axis=1, inplace=True)
        #rename 
        rename=CErename.CEid(ceoil)
        ceoil=rename.replace()
        ceoil=ceoil.fillna(method='ffill')
        
        cesolar = ce.eugasseries(id='54773', dateFrom=DateFrom,dateTo=DateTo,unit='mcm',nd='10')
        cesolar.set_index('Date', inplace=True)
        cesolar.drop(['DateExcel'],axis=1, inplace=True)
        #rename 
        rename=CErename.CEid(cesolar)
        cesolar=rename.replace()
        cesolar=cesolar.fillna(method='ffill')
        #print(cesolar)
        
        ceother = ce.eugasseries(id='54779,54803', dateFrom=DateFrom,dateTo=DateTo,unit='mcm',nd='10')
        ceother.set_index('Date', inplace=True)
        ceother.drop(['DateExcel'],axis=1, inplace=True)
        #rename 
        rename=CErename.CEid(ceother)
        ceother=rename.replace()
        ceother=ceother.fillna(method='ffill')
        #print(ceother)
        
        
        cedemand = ce.eugasseries(id='54709,54707,	54708', dateFrom=DateFrom,dateTo=DateTo,unit='mcm',nd='10')
        cedemand.set_index('Date', inplace=True)
        cedemand.drop(['DateExcel'],axis=1, inplace=True)
        #rename 
        rename=CErename.CEid(cedemand)
        cedemand=rename.replace()
        cedemand=cedemand.fillna(method='ffill')
        #print(cedemand)
        
        cenetwith = ce.eugasseries(id='50685', dateFrom=DateFrom,dateTo=DateTo,unit='mcm',nd='10')
        cenetwith.set_index('Date', inplace=True)
        cenetwith.drop(['DateExcel'],axis=1, inplace=True)
        #rename 
        rename=CErename.CEid(cenetwith)
        cenetwith=rename.replace()
        cenetwith=cenetwith.fillna(method='ffill')
        #print(cenetwith)
        cecapa = ce.eugasseries(id='54910', dateFrom=DateFrom,dateTo=DateTo,unit='mcm',nd='10')
        cecapa.set_index('Date', inplace=True)
        cecapa.drop(['DateExcel'],axis=1, inplace=True)
        #rename 
        rename=CErename.CEid(cecapa)
        cecapa=rename.replace()
        cecapa=cecapa.fillna(method='ffill')
        #print(cecapa)
        
        dftotal = pd.DataFrame(index = cepipe.index)
        dftotal['pipe'] = cepipe['Flows, To TR']
        dftotal['pipeRU'] = cepipe['Flows, RU To TR']
        dftotal['pipeIR'] = cepipe['Flows, IR To TR']
        dftotal['pipeAZ'] = cepipe['Flows, GE To TR']
        dftotal['pipeBG'] = cepipe['Flows, BG To TR']
        dftotal['pipeGR'] = cepipe['Flows, GR To TR']
        dftotal['hydro'] = cehydro.sum(axis=1)
        dftotal['wind'] = cewind
        dftotal['coal'] = cecoal.sum(axis=1)
        dftotal['oil'] = ceoil
        dftotal['solar'] = cesolar
        dftotal['other'] = ceother.sum(axis=1)
        dftotal['LDC'] = cedemand['TR, Demand, LDC']
        dftotal['Industrial'] = cedemand['TR, Demand, Industrial']
        dftotal['Power'] = cedemand['TR, Demand, Power']
        dftotal['Netwith'] = -1*cenetwith
        dftotal['capa'] = cecapa
        dftotal.index = pd.to_datetime(dftotal.index)
        #dftotal = dftotal.resample('M').mean()
        
        
        gdpusd = pd.read_excel('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/Turkey.xlsx', sheet_name='gdp')
        gdpusd=gdpusd[['date','GDP in usd']]
        gdpusd.dropna(inplace=True)
        gdpusd.set_index('date', inplace=True)
        gdpusd.index = pd.to_datetime(gdpusd.index)
        #print(gdpusd)
        
        dftotal['gdp'] = gdpusd['GDP in usd']#np.nan
        
        
        dftotal['gdp'].fillna(method='bfill', inplace=True)
        #print(dftotal.loc['2022-01-01':,'gdp'])
        
        #print(dftotal)
        
        
        return dftotal
        
    def get_price():
        #get price
        
        
        price =  pd.read_excel('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/Turkey.xlsx', sheet_name='price')
        price.dropna(inplace=True)
        price.set_index('date', inplace=True)
        price = price.resample('MS').mean()
        #price.loc['2022-07-01':'2022-10-01'] = np.nan
        #print(price)
        
    
        curve_id = ['TTF FWD', 'JKM FWD','ICE BRENT FWD','API2 FWD'] 
        today=datetime.date.today()
        cob = today#'2021-12-15'
        time_period_id = 2
        start = '2022-01-01' #str((today+relativedelta(months=1)).year)+'-'+str((today+relativedelta(months=1)).month)+'-01'# 
        end = '2025-12-31'
        
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
        
        #end = str(today.year)+'-12-31'
        
        price = pd.concat([price,dfcurvefull.loc['2022-07-01':end]],axis=0)
        price.loc['2022-07-01':end,['TTF', 'JKM', 'Brent','API2']] = price.loc['2022-07-01':end,['TTF FWD', 'JKM FWD','ICE BRENT FWD','API2 FWD']].values
        #price = price.resample('M').mean()
        #print(price)
        #print(dftotal)
        return price
        
    
    def get_temp_data():
        
        #get weather temp
        temphist = DBTOPD('PRD-DB-SQL-211','[LNG]','[ana]','[TempHistNormal]').sql_to_df()
        temphisttr = temphist[['ValueDate','Turkey','Turkey normal']]
        temphisttr.set_index('ValueDate', inplace=True)
        temphisttr.sort_index(inplace=True)
        #print(temphisttr)
        tempfcst = DBTOPD('PRD-DB-SQL-211','[LNG]','[ana]','[TempFcst]').sql_to_df()
        tempfcsttr = tempfcst[['ValueDate','ecmwf-ens Turkey','ecmwf-vareps Turkey','ecmwf-mmsf Turkey']]
        tempfcsttr.set_index('ValueDate',inplace=True)
        tempfcsttr.sort_index(inplace=True)
        #get fcst gap
        sqlConnScrape = pyodbc.connect('DRIVER={SQL SERVER};SERVER=PRD-DB-SQL-211;Trusted_Connection=yes')
        sqlens = '''
                        SELECT * FROM Meteorology.dbo.WeatherStationTimeSeriesHistory
                        WHERE ParameterName = 't_2m:C' AND ModelSourceName = 'ecmwf-ens'
                        AND ForecastDate = {time} and CountryName='Turkey'
            
                          '''.format(time='\''+str(datetime.date.today()-relativedelta(days=10))+' 00:00:00'+'\'')
            
        dfens = pd.read_sql(sqlens, sqlConnScrape)
        #print(str(datetime.date.today()-relativedelta(days=10))+' 06:00:00'+'\'')
        dfens=dfens[['ValueDate','CountryName','Weighting','Value']]
        dfens['weighted'] = dfens['Weighting']*dfens['Value']
        dfens['ValueDate'] = pd.to_datetime(dfens['ValueDate'])
        #print(i, df)
        dfpivot = dfens.pivot_table(values='weighted',index='ValueDate',columns='CountryName',aggfunc=np.sum)
        #print(dfpivot)
        dfgap = dfpivot.resample('D').mean()
        
        #print(tempfcsttr.loc['2023-02-08':'2023-02-28'])
        hist_end = temphisttr['Turkey'].loc[~temphisttr['Turkey'].isnull()].index[-1]
        ens_end = tempfcsttr['ecmwf-ens Turkey'].loc[~tempfcsttr['ecmwf-ens Turkey'].isnull()].index[-1]
        ec_end = tempfcsttr['ecmwf-vareps Turkey'].loc[~tempfcsttr['ecmwf-vareps Turkey'].isnull()].index[-1]
        #sf_end = tempfcsttr['ecmwf-mmsf Turkey'].loc[~tempfcsttr['ecmwf-mmsf Turkey'].isnull()].index[-1]
        
        #print(tempfcsttr.loc[:ens_end,'ecmwf-ens Turkey'])
        today=datetime.date.today()
        dffcst = pd.DataFrame(index = pd.date_range(start = hist_end, end = ec_end), columns = ['Turkey'])
        #print(dffcst.loc[hist_end:today,'Turkey'])
        #print(dfgap)
        
        #suhaib temp hack: start
        ##dffcst.loc[hist_end:today,'Turkey'] = dfgap.loc[hist_end:today,'Turkey'].values
        dffcst.update(dfgap)
        dffcst.update(temphisttr)
        dffcst = dffcst.bfill()
        #suhaib temp hack: end
        
        dffcst.loc[today:ens_end,'Turkey'] = tempfcsttr.loc[today:ens_end,'ecmwf-ens Turkey'].values
        dffcst.loc[ens_end:ec_end,'Turkey'] = tempfcsttr.loc[ens_end:ec_end,'ecmwf-vareps Turkey'].values
        #dffcst.loc[ec_end:sf_end,'Turkey'] = tempfcsttr.loc[ec_end:sf_end,'ecmwf-mmsf Turkey'].values
        #print(dffcst.index)
        
        today=datetime.date.today()
        start = '2018-01-01'
        end = str(today.year+1)+'-12-31'
        dftemp = pd.DataFrame(index = pd.date_range(start = start, end = end), columns = ['Turkey'])
        
        #print( temphisttr)
        dftemp.loc[start:hist_end,'Turkey'] = temphisttr.loc[start:hist_end,'Turkey'].values
        
        #print(hist_end)
        #print(dftemp.loc[hist_end+relativedelta(days=1):sf_end,'Turkey'])
        #print(dffcst.loc[str(hist_end):str(sf_end),'Turkey'])
        if ec_end<=pd.to_datetime(end):
            
            dftemp.loc[hist_end+relativedelta(days=0):ec_end,'Turkey'] = dffcst.loc[str(hist_end):str(ec_end),'Turkey'].values
            
            for i in pd.date_range(start = ec_end, end = end):
                dftemp.loc[i] = temphisttr.loc['2020-'+str(i.month)+'-'+str(i.day),'Turkey normal']
                #dftemp.loc[i] = temphisttr.loc[i,'Turkey normal']
        
        else:
            dftemp.loc[hist_end:end] = dffcst.loc[str(hist_end):str(end)]
            
        #dftemp.plot()
        #print(dftemp.loc['2023-02-08':'2023-02-28'])
        
        
        return dftemp
        
        
    
    
    def production():
        
        today=datetime.date.today()
        
        df = pd.DataFrame(index = pd.date_range(start = str(today.year)+'-01-01', end = str(today.year+1)+'-12-31'), columns=['prod'])
        df.loc[:'2023-04-01'] = 1.05
        df.loc['2023-04-01'] = 1.05
        df.loc['2023-05-01'] = 2
        df.loc['2023-06-01'] = 2
        df.loc['2023-07-01'] = 2
        df.loc['2023-08-01'] = 2
        df.loc['2023-09-01'] = 2
        df.loc['2023-10-01'] = 3
        df.loc['2023-11-01'] = 4
        df.loc['2023-12-01'] = 5
        df.loc['2024-01-01'] = 7
        df.loc['2024-02-01'] = 9
        df.loc['2024-03-01':] = 10
        
        
        
        return df
    
    
    def fulldata(dfce, dftemp, dfprice, dfprod):
        
        today=datetime.date.today()
        
        columns = dfce.columns.to_list() + ['temp'] + ['TTF', 'JKM', 'Brent','API2']
        
        dfall = pd.DataFrame(index = pd.date_range(start = '2018-01-01', end = str(today.year+1)+'-12-31'), columns = columns)
        for i in dfall.index:
            try:
                dfall.loc[i, 'temp'] = dftemp.loc[i, 'Turkey']
                #dfall.loc[i,  ['TTF', 'JKM', 'Brent','API2']] = dfprice.loc[i, ['TTF', 'JKM', 'Brent','API2']]
                #dfall.loc[i,  dfce.columns.to_list()] = dfce.loc[i]
                
            except KeyError:
                #print(e)
                pass
        
        for i in dfall.index:
            try:
                #dfall.loc[i, 'temp'] = dftemp.loc[i, 'Turkey']
                dfall.loc[i,  ['TTF', 'JKM', 'Brent','API2']] = dfprice.loc[i, ['TTF', 'JKM', 'Brent','API2']]
                #dfall.loc[i,  dfce.columns.to_list()] = dfce.loc[i]
                
            except KeyError:
                #print(e)
                pass
            
        for i in dfall.index:
            try:
                #dfall.loc[i, 'temp'] = dftemp.loc[i, 'Turkey']
                #dfall.loc[i,  ['TTF', 'JKM', 'Brent','API2']] = dfprice.loc[i, ['TTF', 'JKM', 'Brent','API2']]
                dfall.loc[i,  dfce.columns.to_list()] = dfce.loc[i]
                
            except KeyError:
                #print(e)
                pass
       
        #print(dfall)
        
        dfpowersectornormal = dfall[['hydro','wind','oil','coal','solar','other']].copy()
        dfpowersectornormal.fillna(method = 'ffill', inplace=True)
        dfpowersectornormal = dfpowersectornormal.resample('MS').mean()
        
        dfpowersectornormal['year'] = dfpowersectornormal.index.year
        dfpowersectornormal['date'] = dfpowersectornormal.index.strftime('%m-%d')
        #print(dfpowersectornormal)
        
        
        dfnormal = pd.DataFrame(index = pd.date_range(start = str(today.year)+'-01-01', end = str(today.year+1)+'-12-31',freq = 'MS'))
        
        
        for i in ['hydro','wind','oil','coal','solar','other']:
            df = dfpowersectornormal[[i,'year','date']].set_index(['year','date']).unstack(0)
            df.columns = df.columns.droplevel(0)
            dfdiff = pd.DataFrame(index = [str(today-relativedelta(months=1))])
            #print(df.columns)
            for j in df.columns[:-2]:
                #print(abs(df[today.year].iloc[(today-relativedelta(months=1)).month-1] - df[j].iloc[(today-relativedelta(months=1)).month-1]))
                dfdiff.loc[str(today-relativedelta(months=1)), str(j)] = abs(df[today.year].iloc[(today-relativedelta(months=1)).month-1] - df[j].iloc[(today-relativedelta(months=1)).month-1])
            
            #dfnormal[i] = df.loc[int(dfdiff.idxmin(axis=1).loc[str(today-relativedelta(months=1))])]
            dfnormal.loc[str(today.year)+'-01-01':str(today.year)+'-12-31', i] = df[int(dfdiff.idxmin(axis=1).loc[str(today-relativedelta(months=1))])].values
            dfnormal.loc[str(today.year+1)+'-01-01':str(today.year+1)+'-12-31', i] = df[int(dfdiff.idxmin(axis=1).loc[str(today-relativedelta(months=1))])].values
            #print(dfdiff.idxmin(axis=1))
        #print(dfnormal)
        dfall.loc[str((today+relativedelta(months=1)).year)+'-'+str((today+relativedelta(months=1)).month)+'-01':, ['hydro','wind','oil','coal','solar','other']] = dfnormal.loc[str((today+relativedelta(months=1)).year)+'-'+str((today+relativedelta(months=1)).month)+'-01':, ['hydro','wind','oil','coal','solar','other']]
        #print(dfall.loc[str((today+relativedelta(months=1)).year)+'-'+str((today+relativedelta(months=1)).month)+'-01':, ['hydro','wind','oil','coal','solar','other']])
        '''
        dfpowersectornormal = dfall[['hydro','wind','oil','coal','solar','other']].copy()
        dfpowersectornormal.fillna(method = 'ffill', inplace=True)
        dfpowersectornormal = dfpowersectornormal.resample('MS').mean()
        
        dfpowersectornormal['year'] = dfpowersectornormal.index.year
        dfpowersectornormal['date'] = dfpowersectornormal.index.strftime('%m-%d')
        #print(dfpowersectornormal)
        
        
        dfnormal = pd.DataFrame(index = pd.date_range(start = str(today.year)+'-01-01', end = str(today.year+1)+'-12-31',freq = 'MS'))
        
        
        for i in ['hydro','wind','oil','coal','solar','other']:
            df = dfpowersectornormal[[i,'year','date']].set_index(['year','date']).unstack(0)
            #df = df.set_index[['year','date']].unstack(0)
            #print(df)
            df['ave'] = df.iloc[:,0:-1].mean(axis=1)
            dfnormal.loc[str(today.year)+'-01-01':str(today.year)+'-12-31', i] = df['ave'].values
            dfnormal.loc[str(today.year+1)+'-01-01':str(today.year+1)+'-12-31', i] = df['ave'].values
        #print(dfnormal)
        
        for i in dfnormal.index:
            
            dfall.loc[i, ['hydro','wind','oil','coal','solar','other']] = dfnormal.loc[i, ['hydro','wind','oil','coal','solar','other']]
        
        '''
        
        for i in dfprod.index:
            dfall.loc[i, 'prod'] = dfprod.loc[i, 'prod']
        #gdp
        #print(dfall.loc['2022-01-01':,'gdp'])
        #dfall['gdp'].fillna(method = 'ffill', inplace=True)    
        
        #dfall.loc['2022-09-30','gdp'] = dfall.loc['2021-09-30','gdp']*(1+0.05)
        dfall.loc['2022-09-30','gdp'] = dfall.loc['2021-09-30','gdp']*(1+0.05)
        dfall.loc['2022-12-31','gdp'] = dfall.loc['2021-12-31','gdp']*(1+0.05)
        dfall.loc['2023-03-31','gdp'] = dfall.loc['2022-03-31','gdp']*(1+0.028)        
        dfall.loc['2023-06-30','gdp'] = dfall.loc['2022-06-30','gdp']*(1+0.028)
        dfall.loc['2023-09-30','gdp'] = dfall.loc['2022-09-30','gdp']*(1+0.028)
        dfall.loc['2023-12-31','gdp'] = dfall.loc['2022-12-31','gdp']*(1+0.028)
        dfall.loc['2024-03-31','gdp'] = dfall.loc['2023-03-31','gdp']*(1+0.0381)        
        dfall.loc['2024-06-30','gdp'] = dfall.loc['2023-06-30','gdp']*(1+0.0381)
        dfall.loc['2024-09-30','gdp'] = dfall.loc['2023-09-30','gdp']*(1+0.0381)
        dfall.loc['2024-12-31','gdp'] = dfall.loc['2023-12-31','gdp']*(1+0.0381)
        dfall['gdp'].fillna(method = 'bfill', inplace=True)
        #dfall['gdp'].plot()
        #print(dfall)
        
        return dfall
        
    
    
    def ldz(dfall):
        
        
        #dfsig = pd.concat([dftotal['LDC'], dftemp], axis=1)
        dfsig = dfall[['LDC', 'temp']].copy()
        #dfsig['tempall'] = dftemp['Turkey']
        #dfsig = dfsig[['date','tempall']]
        #dfsig.set_index('date', inplace=True)
        #dfsig.rename(columns={'Turkey':'tempall'}, inplace=True)
        #print(dfsig)
        dfsig['month'] = dfsig.index.month
        dfsig['week'] = dfsig.index.dayofweek
        
        #print(dfsig)
        winweekday = [9.3,
                        3.9,
                        187,
                        20
                        ]
        
        winweekend = [8.35,
                        4.8,
                        204,
                        6
                        ]
        
        sumweekday = [8.3,
                        2.5,
                        188,
                        24
                        ]
        
        sumweekend = [8.3,
                        2.6,
                        170,
                        20
                        ]
        
        for i in dfsig.index:
            
            if i.month >=10 and i.month <=12:
                if dfsig.loc[i,'week'] >=0 and dfsig.loc[i,'week'] <=4:
                    
                    dfsig.loc[i, 'fcst'] = winweekday[2] - (winweekday[2] - winweekday[3])/(1 + math.exp(-(dfsig.loc[i,'temp']-winweekday[0])/winweekday[1]))
                if dfsig.loc[i,'week'] >=5 and dfsig.loc[i,'week'] <=6:
                    dfsig.loc[i, 'fcst'] = winweekend[2] - (winweekend[2] - winweekend[3])/(1 + math.exp(-(dfsig.loc[i,'temp']-winweekend[0])/winweekend[1]))
            if i.month >=1 and i.month <=3:
                if dfsig.loc[i,'week'] >=0 and dfsig.loc[i,'week'] <=4:
                    
                    dfsig.loc[i, 'fcst'] = winweekday[2] - (winweekday[2] - winweekday[3])/(1 + math.exp(-(dfsig.loc[i,'temp']-winweekday[0])/winweekday[1]))
                if dfsig.loc[i,'week'] >=5 and dfsig.loc[i,'week'] <=6:
                    dfsig.loc[i, 'fcst'] = winweekend[2] - (winweekend[2] - winweekend[3])/(1 + math.exp(-(dfsig.loc[i,'temp']-winweekend[0])/winweekend[1]))
            
            if i.month >=4 and i.month <=9:
                if dfsig.loc[i,'week'] >=0 and dfsig.loc[i,'week'] <=4:
                    
                    dfsig.loc[i, 'fcst'] = sumweekday[2] - (sumweekday[2] - sumweekday[3])/(1 + math.exp(-(dfsig.loc[i,'temp']-sumweekday[0])/sumweekday[1]))
                if dfsig.loc[i,'week'] >=5 and dfsig.loc[i,'week'] <=6:
                    dfsig.loc[i, 'fcst'] = sumweekend[2] - (sumweekend[2] - sumweekend[3])/(1 + math.exp(-(dfsig.loc[i,'temp']-sumweekend[0])/sumweekend[1]))
            
            
        #print(dfsig)
        #dfsig[['LDC','fcst']].plot()
        #dfsig[['temp']].plot()
        #dfsig.to_csv('H:/report/Fundamental research/Turkeyldcsigfcstshape .csv')
        return dfsig
    
    def pipefcst(dfall):
        
       
        
        today=datetime.date.today()
        #print(dftotal)
        dfpipe = dfall[['pipe','pipeRU','pipeIR','pipeAZ','pipeGR','pipeBG','Brent','TTF','temp']].copy()
        dfpipe.fillna(method='ffill', inplace=True)
        dfpipe = dfpipe.resample('MS').mean()
        dfpipe['TTF603'] = dfpipe['TTF'].rolling(6).mean()*0.3
        dfpipe['Brent603'] = dfpipe['Brent'].rolling(6).mean()*0.3
        dfpipe['Brent601'] = dfpipe['Brent'].rolling(6).mean()*0.1
        #print(dfpipe)
        #print(dfpipe)
        
        #print(price)
        #print(columns)
        test_start = '2018-01-01'
        test_end =  str((today+relativedelta(months=1)).year)+'-'+str((today+relativedelta(months=0)).month)+'-01'
        target_start = str((today+relativedelta(months=0)).year)+'-'+str((today+relativedelta(months=0)).month)+'-01'
        target_end = str((today+relativedelta(months=1)).year+1)+'-12-31'
        
        
        '''      
        2021-01-01:
        NET_RU = (TTF<11.399999)*(-15.3485458863*BRENT601 + 11.4737350894*TTF - 0.626099309381*TEMP + 31.8169222987) + 
        (TTF>=11.399999 AND TTF<82.664999)*(-8.42351754796*BRENT601 + 0.81060590998*TTF - 0.723769335729*TEMP + 101.761822772)
         + (TTF>=82.664999)*(4.45331996369*BRENT601 + 0.222195471155*TTF - 2.95967829709*TEMP + 27.1935657222)
        '''
        for i in dfpipe.index:
            if dfpipe.loc[i,'TTF'] < 11.399999:
                dfpipe.loc[i, 'pipeRUfcst'] = 11.4737350894*dfpipe.loc[i,'TTF']-15.3485458863*dfpipe.loc[i,'Brent601']- 0.626099309381*dfpipe.loc[i,'temp']+ 31.8169222987
            if 11.399999 <= dfpipe.loc[i,'TTF'] < 82.664999:
                dfpipe.loc[i, 'pipeRUfcst'] = 0.81060590998*dfpipe.loc[i,'TTF']-8.42351754796*dfpipe.loc[i,'Brent601']- 0.723769335729*dfpipe.loc[i,'temp']+101.761822772
            if dfpipe.loc[i,'TTF'] > 82.664999:
                dfpipe.loc[i, 'pipeRUfcst'] = 0.222195471155*dfpipe.loc[i,'TTF']+4.45331996369*dfpipe.loc[i,'Brent601']- 2.95967829709*dfpipe.loc[i,'temp']+ 27.1935657222
           


        '''
        2018 with 601
        for i in dfpipe.index:
            if dfpipe.loc[i,'Brent601'] < 7.642794:
                dfpipe.loc[i, 'pipeRUfcst'] = 16.1984728859*dfpipe.loc[i,'Brent601']- 1.04194049108*dfpipe.loc[i,'TTF']
            if 7.642794<=dfpipe.loc[i,'Brent601'] <9.219214:
                dfpipe.loc[i, 'pipeRUfcst'] = 12.3031779605*dfpipe.loc[i,'Brent601']- 0.220894048122*dfpipe.loc[i,'TTF']
            if 9.219214<=dfpipe.loc[i,'Brent601'] < 11.27468:
                dfpipe.loc[i, 'pipeRUfcst'] = 8.6856366873*dfpipe.loc[i,'Brent601']- 0.306015058741*dfpipe.loc[i,'TTF']
            if dfpipe.loc[i,'Brent601'] >=11.27468:
                dfpipe.loc[i, 'pipeRUfcst'] = 1.83412082903*dfpipe.loc[i,'Brent601']- 0.181699255636*dfpipe.loc[i,'TTF']

            
        for i in dfpipe.index:
            if dfpipe.loc[i,'TTF'] < 11.169999:
                dfpipe.loc[i, 'pipeRUfcst'] = 8.05141266995*dfpipe.loc[i,'TTF']-3.31941414097*dfpipe.loc[i,'Brent603']+0.0780968227946*dfpipe.loc[i,'temp']+15.9260511051
            if 11.169999 <= dfpipe.loc[i,'TTF'] < 20.462499:
                dfpipe.loc[i, 'pipeRUfcst'] = -3.09004022912*dfpipe.loc[i,'TTF']-5.68057337932*dfpipe.loc[i,'Brent603']-2.78048279658*dfpipe.loc[i,'temp']+231.604003831
            if dfpipe.loc[i,'TTF'] > 20.462499:
                dfpipe.loc[i, 'pipeRUfcst'] = 0.17717156347*dfpipe.loc[i,'TTF']-2.26036081434*dfpipe.loc[i,'Brent603']-0.76861832401*dfpipe.loc[i,'temp']+113.340567238
           
        for i in dfpipe.index:    
             if dfpipe.loc[i,'TTF603'] < 3.458547:
                 dfpipe.loc[i, 'pipeIRfcst'] = 74.898671918-2.4160166561*dfpipe.loc[i,'Brent603']-80.3289353587*dfpipe.loc[i,'TTF603']-7.21632975042*dfpipe.loc[i,'temp']
             if 3.458547 <= dfpipe.loc[i,'TTF603'] < 8.619728:
                 dfpipe.loc[i, 'pipeIRfcst'] = 38.830719488-0.988434178432*dfpipe.loc[i,'Brent603']+1.61789530809*dfpipe.loc[i,'TTF603']-0.462239644812*dfpipe.loc[i,'temp']
             if dfpipe.loc[i,'TTF603'] > 8.619728:
                 dfpipe.loc[i, 'pipeIRfcst']  = 38.8042454571-1.78438296865*dfpipe.loc[i,'Brent603']+0.804377462398*dfpipe.loc[i,'TTF603']+0.772876233273*dfpipe.loc[i,'temp']
        '''
        
        for i in dfpipe.index:    
             if dfpipe.loc[i,'temp'] < 7.8999999:
                 dfpipe.loc[i, 'pipeIRfcst'] = -1.12649532729*dfpipe.loc[i,'Brent603']+46.3289288007
             if dfpipe.loc[i,'temp'] >= 7.8999999:
                 dfpipe.loc[i, 'pipeIRfcst'] = 0.0975639407235*dfpipe.loc[i,'Brent603']+23.2828334566
             
        
        
        for i in dfpipe.index:
             if dfpipe.loc[i,'Brent603'] < 18.36377:
                  dfpipe.loc[i, 'pipeAZfcst'] = -1.74288062043*dfpipe.loc[i,'Brent603'] - 0.222108200602*dfpipe.loc[i,'TTF603'] - 0.612909092072*dfpipe.loc[i,'temp'] + 63.164658721
             if 18.36377 <= dfpipe.loc[i,'Brent603'] < 20.87812:
                 dfpipe.loc[i, 'pipeAZfcst'] = 3.56149082852*dfpipe.loc[i,'Brent603'] - 3.32870779245*dfpipe.loc[i,'TTF603'] - 0.494130174963*dfpipe.loc[i,'temp'] - 22.5345243853
             if dfpipe.loc[i,'Brent603'] >= 20.87812:             
                 dfpipe.loc[i, 'pipeAZfcst'] = 0.0808055863701*dfpipe.loc[i,'Brent603'] + 0.00804554095246*dfpipe.loc[i,'TTF603'] - 0.258952254446*dfpipe.loc[i,'temp'] + 23.0027076583
       
                
        dfpipe.loc['2023-01-01':,'pipeAZfcst'] = dfpipe.loc['2023-01-01':,'pipeAZfcst']+6 #AZ increased pipe to TR, around 6 vs 22. 28mcm/d for 23
        
        #print(dfpipe['pipeAZfcst'].loc['2022-01-01':,])
        dfpipe['TRfcst'] = dfpipe['pipeRUfcst'] + dfpipe['pipeIRfcst'] + dfpipe['pipeAZfcst']
        #print(dfpipe.tail(12))
        result2=pd.DataFrame(index = pd.date_range(start=target_start, end = target_end, freq='MS'))
        #result2.loc[:,'act'] = train_y1.values
        result2.loc[:,'fcst'] = dfpipe['TRfcst']
        #result2.loc[test_start:test_end,'linearfcst'] = reg_predit
        #result['Meti'] = df['Y']
        #result.to_csv('H:\Yuefeng\LNG Projects\JKC demand\Japan inventory fcst.csv')
        #result2=result2.resample('MS').mean()
        #result2.plot(legend=True)
        #print(result2)
        #print('mean error linear: ', round((result2['linearfcst']-result2['act']).mean()),2)
        #print('mean error knn: ', round((result2['fcst']-result2['act']).mean(),2))
        #print('r2 linear: ', round(r2_score(result2['linearfcst'], result2['act']),2))
        #print('r2 knn: ', round(r2_score(result2['fcst'], result2['act']),2))
        #print(result2.resample('MS').mean())
        return result2, dfpipe   
        '''
        #dfpipe['month'] = dfpipe.index.month
        dfpipe['quarter'] = dfpipe.index.quarter
        #print(dfpipe)
        columns = ['Brent','quarter']
        #print(dfpipe)
        train_X1, train_y1 = dfpipe.loc[test_start:test_end,columns], dfpipe.loc[test_start:test_end,'pipe']#(df.loc[:'2022-02-01','Y'] - df.loc[:'2022-02-01','JP demand'])
        train_X1.fillna(method='bfill', inplace=True)
        #print(train_X1)
        targetX = dfpipe.loc[target_start:target_end,columns]
        #Traget_y = rf_grid.predict(Target_x)
        #print(Target_x)
        test_y = dfpipe.loc[target_start:target_end,'pipe']
        

        #print('-------------------Pipe KNN---------------------')
        knn=KNeighborsRegressor(n_neighbors=3, #weights='distance')
                                weights='uniform')
        knn.fit(train_X1,train_y1)
        knn_y_predict = knn.predict(targetX)
        #print(knn_y_predict)
        ##########################################
        
        result2=pd.DataFrame(index = pd.date_range(start=target_start, end = target_end, freq='MS'))
        #result2.loc[:,'act'] = train_y1.values
        result2.loc[:,'fcst'] = knn_y_predict
        #result2.loc[test_start:test_end,'linearfcst'] = reg_predit
        #result['Meti'] = df['Y']
        #result.to_csv('H:\Yuefeng\LNG Projects\JKC demand\Japan inventory fcst.csv')
        result2=result2.resample('MS').mean()
        #result2.plot(legend=True)
        #print(result2)
        #print('mean error linear: ', round((result2['linearfcst']-result2['act']).mean()),2)
        #print('mean error knn: ', round((result2['fcst']-result2['act']).mean(),2))
        #print('r2 linear: ', round(r2_score(result2['linearfcst'], result2['act']),2))
        #print('r2 knn: ', round(r2_score(result2['fcst'], result2['act']),2))
        #print(result2.resample('MS').mean())
        return result2
        '''
        
        
    def indfcst(dfall, dfpipe):
    
        
        today=datetime.date.today()
        #pipe	TTF	JKM	Brent	gdp	temp	api2

        dfmodel = dfall[['Industrial','pipe','TTF', 'JKM', 'Brent','gdp','temp','API2']].copy()
        dfmodel.fillna(method = 'ffill', inplace=True)
        #print(dfmodel)
        #dfmodel['temp'] = dftemp['Turkey']
        #dfmodel.to_csv('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/turkey fcst.csv')
        dfmodel = dfmodel.resample('MS').mean()
        #dfmodel[]
        for i in dfpipe.index:
            dfmodel.loc[i, 'pipe'] = dfpipe.loc[i,'fcst']
        
        #dfmodel['quarter'] = dfmodel.index.quarter
        #print(dfmodel)
        
        #columns = ['pipe','TTF', 'JKM', 'Brent','gdp','temp','quarter'] #pipeline import, TTF, JKM, Brent, temperature and GDP
        
        test_start = '2018-01-01'
        test_end =  str((today+relativedelta(months=1)).year)+'-'+str((today+relativedelta(months=0)).month)+'-01'
        target_start = str((today+relativedelta(months=0)).year)+'-01-01'#+str((today+relativedelta(months=0)).month)+'-01'
        target_end = str((today+relativedelta(months=1)).year+1)+'-12-31'
        
        #print(dfmodel.loc['2022-03-01':'2023-05-01'])

        
        for i in  dfmodel.index:
            if dfmodel.loc[i,'gdp'] <170.82699:
                dfmodel.loc[i,'fcst'] = 0.00596065112962*dfmodel.loc[i,'TTF']-0.0544796882895*dfmodel.loc[i,'Brent']-0.503319609893*dfmodel.loc[i,'temp']-0.122233080765*dfmodel.loc[i,'API2']+0.757851159489*dfmodel.loc[i,'gdp']-73.7852634567
            if 170.82699 <= dfmodel.loc[i,'gdp'] < 177.73199:
                dfmodel.loc[i,'fcst'] = 0.203201066328*dfmodel.loc[i,'TTF']+0.2006236533*dfmodel.loc[i,'Brent']+0.0280005441087*dfmodel.loc[i,'temp']-0.058644694552*dfmodel.loc[i,'API2']+1.28881387727*dfmodel.loc[i,'gdp']-206.685867399
            if 177.73199 <= dfmodel.loc[i,'gdp'] < 193.37319:
                dfmodel.loc[i,'fcst'] = -0.014427243925*dfmodel.loc[i,'TTF']+0.112249892648*dfmodel.loc[i,'Brent']-0.51927895254*dfmodel.loc[i,'temp']-0.0167362138912*dfmodel.loc[i,'API2']+1.05491604717*dfmodel.loc[i,'gdp']-154.21469127
            if dfmodel.loc[i,'gdp'] > 193.37319:
                dfmodel.loc[i,'fcst'] = 0.187557892982*dfmodel.loc[i,'TTF']-0.150223466671*dfmodel.loc[i,'Brent']-0.243088152492*dfmodel.loc[i,'temp']-0.0281903088088*dfmodel.loc[i,'API2']+0.265482671994*dfmodel.loc[i,'gdp']-7.82353390574
        dfmodel[['fcst','Industrial']].plot(legend=True)

        '''
        train_X1, train_y1 = dfmodel.loc[test_start:test_end,columns], dfmodel.loc[test_start:test_end,'Industrial']#(df.loc[:'2022-02-01','Y'] - df.loc[:'2022-02-01','JP demand'])
        
        #print(train_X1)
        Target_x = dfmodel.loc[target_start:target_end,columns]
        #Traget_y = rf_grid.predict(Target_x)
        #print(Target_x)
        test_y = dfmodel.loc[target_start:target_end,'Industrial']
        
        
        
        
        #oldlinear = 0.10654864*train_X1['pipe'] - 0.04763632*train_X1['TTF'] + 0.45115691*train_X1['JKM'] -0.07094592* train_X1['Brent']-0.03748928*train_X1['temp']-0.0069607 *train_X1['gdp']+27.333715201142137
        oldlinear = 0.10654864*Target_x['pipe'] - 0.04763632*Target_x['TTF'] + 0.45115691*Target_x['JKM'] -0.07094592* Target_x['Brent']-0.03748928*Target_x['temp']-0.0069607 *Target_x['gdp']+27.333715201142137
        '''
        '''
        reg = LinearRegression().fit(train_X1, train_y1)
        print(reg.score(train_X1, train_y1))
        
        print(reg.coef_)
        
        print(reg.intercept_)
        
        reg_predit = reg.predict(train_X1)
        '''
        result=pd.DataFrame(index = pd.date_range(start=target_start, end = target_end, freq='MS'))
        #result.loc[target_start:target_end,'act'] = train_y1
        result.loc[:,'fcst'] = dfmodel.loc[target_start:target_end,'fcst']
        #result.loc[:,'old fcst'] = oldlinear
        
        #result['Meti'] = df['Y']
        #print('r2 linear: ', round(r2_score(result['fcst'], result['act']),2))   
        #print('r2 linear: ', round(r2_score(result['old fcst'], result['act']),2))  
        #result.plot(legend=True)
        #print(result)
        #result.to_csv('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/turkey fcst.csv')
        return result
    
    
    def indfcst1(dfall, dfpipeall):
        
        #:2021-08-01
            #INDUSTRIAL = 0.061661755594*LDC + 0.104731633908*POWER + 0.00759413944219*CAPA + 0.144159358083*PIPEIR

        #2021-08-01:
            #INDUSTRIAL = -0.1236322361*BRENT - 0.00504552306447*CAPA + 0.0725711371024*PIPE + 60.6281979308
            
            
        #INDUSTRIAL = -8.51920581749*OTHER + 64.8119113861 - 3.07003360614*SOLAR + 0.128697656815*HYDRO + 0.170384626553*JKM

        today=datetime.date.today()
        target_start = str((today+relativedelta(months=0)).year)+'-01-01'#+str((today+relativedelta(months=0)).month)+'-01'
        target_end = str((today+relativedelta(months=1)).year+1)+'-12-31'
        
        dfmodel = dfall[['Industrial','capa','Brent','pipe','LDC','Power','pipeIR','other','solar','hydro','JKM']].copy()
        dfmodel.fillna(method = 'ffill', inplace=True)
        dfmodel = dfmodel.resample('MS').mean()
        dfmodel['capa'].fillna(method = 'bfill', inplace=True)
        
        #print(dfmodel)
        for i in dfpipeall.loc[target_start:].index:
            dfmodel.loc[i, 'pipeIR'] = dfpipeall.loc[i,'pipeIRfcst']
            dfmodel.loc[i, 'pipe'] = dfpipeall.loc[i,'TRfcst']

        
        for i in dfmodel.loc[:'2021-08-01'].index:
            
            dfmodel.loc[i,'fcst'] = 0.061661755594*dfmodel.loc[i,'LDC'] + 0.104731633908*dfmodel.loc[i,'Power'] + 0.00759413944219*dfmodel.loc[i,'capa'] + 0.144159358083*dfmodel.loc[i,'pipeIR']
        
        for i in  dfmodel.loc['2021-09-01':].index:
            
            #dfmodel.loc[i,'fcst'] = -0.1236322361*dfmodel.loc[i,'Brent'] - 0.00504552306447*dfmodel.loc[i,'capa'] + 0.0725711371024*dfmodel.loc[i,'pipe'] + 60.6281979308
            dfmodel.loc[i,'fcst'] = -8.51920581749*dfmodel.loc[i,'other'] + 64.8119113861 - 3.07003360614*dfmodel.loc[i,'solar'] + 0.128697656815*dfmodel.loc[i,'hydro'] + 0.170384626553*dfmodel.loc[i,'JKM']
        
        #dfmodel[['fcst','Industrial']].plot(legend=True)
        #print('r2 linear: ', round(r2_score(dfmodel.loc[:'2023-09-01','Industrial'], dfmodel.loc[:'2023-09-01','fcst']),2))

        #print(dfmodel)
        result=pd.DataFrame(index = pd.date_range(start=target_start, end = target_end, freq='MS'))
        result.loc[:,'fcst'] = dfmodel.loc[target_start:target_end,'fcst']

        return result
    
    def powerfcst(dfall, dfpipe):
        
        #print(dfall)
        #POWER = 0.405713213887*PIPE + 0.230976967589*COAL - 0.46050052094*HYDRO + 1.32757548206*SOLAR - 0.626844525996*WIND + 1.3996245205*TEMP - 17.8412287599
        
        
        today=datetime.date.today()
        
        dfmodel = dfall[['Power', 'pipe', 'TTF', 'JKM', 'Brent', 'temp', 'gdp','hydro','wind','oil','coal','solar','other']].copy()
        #dfmodel = dfall[['Power', 'pipe', 'temp','hydro','wind','coal','solar']].copy()
        
        dfmodel.fillna(method = 'ffill', inplace=True)
        #print(dfmodel)
        #dfmodel['temp'] = dftemp['Turkey']
        dfmodel = dfmodel.resample('MS').mean()
        for i in dfpipe.index:
            dfmodel.loc[i, 'pipe'] = dfpipe.loc[i,'fcst']
            
        #dfmodel.to_csv('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/turkey fcst.csv')
        
        #columns = ['pipe', 'TTF', 'JKM', 'Brent', 'temp', 'gdp','hydro','wind','oil','coal','solar','other']
        columns = ['pipe', 'temp','hydro','wind','coal','oil','other','solar']

        #print(columns)
        test_start = '2018-01-01'
        test_end =  str((today-relativedelta(months=1)).year)+'-'+str((today-relativedelta(months=1)).month)+'-01'
        #test_end =  '2022-12-01'#str((today+relativedelta(months=1)).year)+'-'+str((today+relativedelta(months=0)).month)+'-01'
        target_start = str((today+relativedelta(months=0)).year)+'-'+str((today+relativedelta(months=0)).month)+'-01'
        target_end = str((today+relativedelta(months=1)).year+1)+'-12-31'
        
        
        train_X1, train_y1 = dfmodel.loc[test_start:test_end,columns], dfmodel.loc[test_start:test_end,'Power']
        
        #print(train_X1)
        Target_x = dfmodel.loc[target_start:target_end,columns]
        #Traget_y = rf_grid.predict(Target_x)
        #print(Target_x)
        #test_y = dfmodel.loc[target_start:target_end,'DemandPower']
        
        reg = LinearRegression().fit(train_X1, train_y1)
        #print(reg.score(train_X1, train_y1))
        
        #print(reg.coef_)
        
        #print(reg.intercept_)
        
        reg_predit = reg.predict(Target_x)
        #print(reg_predit)
        result1=pd.DataFrame(index = pd.date_range(start=target_start, end = target_end, freq = 'MS'))
        result1.loc[target_start:target_end,'act'] = train_y1
        result1.loc[target_start:target_end,'fcst'] = reg_predit
        #result1.plot(legend=True)
        #result1.to_csv('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/turkey fcst.csv')
        #print(result1)
        return result1
    
    def storagefcst(dfall, dfpipe):
        
        #print(dfall)
        today=datetime.date.today()
        
        dfmodel = dfall[['Industrial','capa','LDC', 'Netwith', 'pipe', 'TTF', 'JKM', 'Brent', 'temp', 'gdp','hydro','wind','oil','coal','solar','other']].copy()
        dfmodel.fillna(method = 'ffill', inplace=True)
        dfmodel['withrate'] = dfmodel['Netwith']/dfmodel['capa']
        #print(dfmodel)
        #dfmodel['temp'] = dftemp['Turkey']
        dfmodel = dfmodel.resample('MS').mean()
        for i in dfpipe.index:
            dfmodel.loc[i, 'pipe'] = dfpipe.loc[i,'fcst']
            
        #print(dfmodel)
        dfmodel['quarter'] = dfmodel.index.quarter
        #print(dfmodel)
        #dfmodel.to_csv('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/turkey fcst.csv')
        test_start = '2019-12-01'
        test_end =  str((today+relativedelta(months=1)).year)+'-'+str((today+relativedelta(months=0)).month)+'-01'
        target_start = str((today+relativedelta(months=0)).year)+'-'+str((today+relativedelta(months=0)).month)+'-01'
        target_end = str((today+relativedelta(months=1)).year+1)+'-12-31'
        
        
        dfmodeltest = dfmodel.loc['2019-12-01':test_end]
        dfmodelsumtest = dfmodeltest[(dfmodeltest['quarter']==2) | (dfmodeltest['quarter']==3)]
        dfmodelwintest = dfmodeltest[(dfmodeltest['quarter']==1) | (dfmodeltest['quarter']==4)]
        
        dfmodeltarget = dfmodel.loc[target_start:target_end]
        dfmodelsumtarget = dfmodeltarget[(dfmodeltarget['quarter']==2) | (dfmodeltarget['quarter']==3)]
        dfmodelwintarget = dfmodeltarget[(dfmodeltarget['quarter']==1) | (dfmodeltarget['quarter']==4)]
        
        #columns = ['pipe','TTF', 'JKM', 'Brent', 'temp', 'gdp','hydro','wind','oil','coal','solar','other']
        columns = ['temp','pipe']
        train_X1_sum, train_y1_sum = dfmodelsumtest.loc[:,columns], dfmodelsumtest.loc[:,['withrate']]#(df.loc[:'2022-02-01','Y'] - df.loc[:'2022-02-01','JP demand'])
        train_X1_win, train_y1_win = dfmodelwintest.loc[:,columns], dfmodelwintest.loc[:,['withrate']]#(df.loc[:'2022-02-01','Y'] - df.loc[:'2022-02-01','JP demand'])
        #print(train_y1_sum)
        Target_x_sum = dfmodelsumtarget.loc[:,columns]
        Target_x_win = dfmodelwintarget.loc[:,columns]
        #Traget_y = rf_grid.predict(Target_x)
        #print(Target_x)
        #test_y = dfmodel.loc[target_start:target_end,'Netwith']   
        '''
         #print('-------------------Pipe KNN---------------------')
        knnsum=KNeighborsRegressor(n_neighbors=3, #weights='distance')
                                weights='uniform')
        knnsum.fit(train_X1_sum,train_y1_sum)
        knn_y_predict = knnsum.predict(Target_x_sum)
        #print(knn_y_predict)
        knnwin=KNeighborsRegressor(n_neighbors=3, #weights='distance')
                                weights='uniform')
        knnwin.fit(train_X1_win,train_y1_win)
        knn_y_predict = knnwin.predict(Target_x_win)
        #print(knn_y_predict)
        
        Target_x_sum['fcst'] = knn_y_predict
        Target_x_win['fcst'] = knn_y_predict
        '''
        reg_sum = LinearRegression().fit(train_X1_sum, train_y1_sum)
        #print(reg_sum.coef_)
        #print( reg_sum.intercept_)
        reg_predit_sum = reg_sum.predict(Target_x_sum)
        #print(reg_predit_sum)
        reg_win = LinearRegression().fit(train_X1_win, train_y1_win)
        reg_predit_win = reg_win.predict(Target_x_win)
        
        Target_x_sum['fcst'] = reg_predit_sum
        Target_x_win['fcst'] = reg_predit_win
        
        '''
        train_y1_sum['fcst'] = reg_predit_sum
        train_y1_win['fcst'] = reg_predit_win
        #print(train_y1_sum)
        result_test = pd.concat([train_y1_sum['fcst'], train_y1_win['fcst']])
        result_test.sort_index(inplace=True)
        print(result_test)
        dfmodeltest['fcst'] = result_test
        print(dfmodeltest[['Netwith','fcst']])
        dfmodeltest[['Netwith','fcst']].plot(legend=True)
        '''
        result = pd.concat([Target_x_sum['fcst'], Target_x_win['fcst']])
        result.sort_index(inplace=True)
        result = result*dfmodel['capa']
        #print(Target_x_sum['fcst'])
        #print(Target_x_win['fcst'])
        #print(result)
        
        
        #result2.loc[test_start:test_end,'old'] =regold
        result.plot(legend=True)
        print(result)
        return result, dfmodel
        '''
        
        #print(dfmodelsum)        
        columns = ['pipe','TTF', 'JKM', 'Brent', 'temp', 'gdp','hydro','wind','oil','coal','solar','other']
        
        train_X1, train_y1 = dfmodel.loc[test_start:test_end,columns], dfmodel.loc[test_start:test_end,'Netwith']#(df.loc[:'2022-02-01','Y'] - df.loc[:'2022-02-01','JP demand'])
        
        #print(train_X1)
        Target_x = dfmodel.loc[target_start:target_end,columns]
        #Traget_y = rf_grid.predict(Target_x)
        #print(Target_x)
        test_y = dfmodel.loc[target_start:target_end,'Netwith']
        
        
        

        #regold = 4.59763731e-02*dfmodel['pipe']-4.96547983e-02*dfmodel['TTF']+1.18027361*dfmodel['JKM']+8.58289393e-02*dfmodel['Brent']-1.76682431*dfmodel['temp']-3.07948189e-01*dfmodel['gdp'] -3.69262793e-02*dfmodel['hydro']+  1.35306603e-01*dfmodel['wind']-1.16971984e+02*dfmodel['oil']+  6.06631173e-01*dfmodel['coal'] + 3.01669889e+01*dfmodel['solar'] -3.04152688e+01*dfmodel['other']+110.35080999274801
        
        reg = LinearRegression().fit(train_X1, train_y1)
        coff = reg.coef_
        inter = reg.intercept_
        #print(reg.score(train_X1, train_y1))
        
        #print(reg.coef_)
        
        #print(reg.intercept_)
        
        reg_predit = reg.predict(Target_x)
        #print(reg_predit)
        
        result2=pd.DataFrame(index = pd.date_range(start=target_start, end = target_end, freq = 'MS'))
        
        #result2.loc[test_start:test_end,'act'] = train_y1.values
        result2.loc[target_start:target_end,'linearfcst'] = reg_predit
        #result2.loc[test_start:test_end,'old'] =regold
        #result2.plot(legend=True)
        #print(result2)
        return result2
        '''
    
    
    def storagefcst1(dfall, dfldz, dfpipe):
        
        '''
        WITHRATE = 
        (LDC<23.86)*(-0.000497715968572*LDC + 0.000161888796435*PIPE + 0.000215766262209*TEMP - 0.0136233610602) + 
        (LDC>=23.86 AND LDC<32.65999)*(-0.000294880146596*LDC - 3.78527714832e-06*PIPE + 0.000110232272438*TEMP + 0.00245769538549) + 
        (LDC>=32.65999 AND LDC<77.84999)*(-0.000313868313404*LDC - 0.000124412186835*PIPE - 0.000373332560219*TEMP + 0.0298024201238) + 
        (LDC>=77.84999 AND LDC<115.28)*(9.80413330949e-05*LDC - 0.000117226241724*PIPE + 0.00066689537083*TEMP - 0.0036033481573) + 
        (LDC>=115.28)*(-8.69588782703e-05*LDC - 2.50996911894e-05*PIPE + 0.000921064243197*TEMP + 0.0107948805868)
        '''
        dfldz = dfldz[['fcst']].resample('MS').mean()
        #print(dfldz)
        today=datetime.date.today()
        
        #dfmodel = dfall[['capa', 'Netwith', 'pipe', 'temp']].copy()
        dfmodel = dfall[['capa','LDC', 'Netwith', 'pipe', 'TTF', 'JKM', 'Brent', 'temp', 'gdp','hydro','wind','oil','coal','solar','other']].copy()

        dfmodel['LDC'] = dfldz
        
        dfmodel.fillna(method = 'ffill', inplace=True)
        dfmodel['withrate'] = dfmodel['Netwith']/dfmodel['capa']
        #print(dfpipe)
        #dfmodel['temp'] = dftemp['Turkey']
        dfmodel = dfmodel.resample('MS').mean()
        #print(dfmodel.loc['2023-11-01'])
        for i in dfpipe.index:
            dfmodel.loc[i, 'pipe'] = dfpipe.loc[i,'fcst']
        #print(dfmodel.loc['2023-02-01':])  
        for i in dfmodel.index:
            
            if dfmodel.loc[i, 'LDC'] < 23.86:
                dfmodel.loc[i, 'storagefcstr'] = -0.000497715968572*dfmodel.loc[i, 'LDC'] + 0.000161888796435*dfmodel.loc[i, 'pipe'] + 0.000215766262209*dfmodel.loc[i, 'temp'] - 0.0136233610602
            if 32.65999 > dfmodel.loc[i, 'LDC'] >= 23.86:
                dfmodel.loc[i, 'storagefcstr'] = -0.000294880146596*dfmodel.loc[i, 'LDC'] - 3.78527714832e-06*dfmodel.loc[i, 'pipe'] + 0.000110232272438*dfmodel.loc[i, 'temp'] + 0.00245769538549
            if 77.84999 > dfmodel.loc[i, 'LDC'] >= 32.65999:
                dfmodel.loc[i, 'storagefcstr'] = -0.000313868313404*dfmodel.loc[i, 'LDC'] - 0.000124412186835*dfmodel.loc[i, 'pipe'] - 0.000373332560219*dfmodel.loc[i, 'temp'] + 0.0298024201238
            if 115.28 > dfmodel.loc[i, 'LDC'] >= 77.84999:
                dfmodel.loc[i, 'storagefcstr'] = 9.80413330949e-05*dfmodel.loc[i, 'LDC'] - 0.000117226241724*dfmodel.loc[i, 'pipe'] + 0.00066689537083*dfmodel.loc[i, 'temp'] - 0.0036033481573
            if dfmodel.loc[i, 'LDC'] >=115.28:
                dfmodel.loc[i, 'storagefcstr'] = -8.69588782703e-05*dfmodel.loc[i, 'LDC'] - 2.50996911894e-05*dfmodel.loc[i, 'pipe'] + 0.000921064243197*dfmodel.loc[i, 'temp'] + 0.0107948805868
        
        dfmodel['storagefcst'] = dfmodel['storagefcstr'] * dfmodel['capa']
        
        testdate = '2023-11-01'
        dftest1 = -0.000497715968572*dfmodel.loc[testdate, 'LDC'] + 0.000161888796435*dfmodel.loc[testdate, 'pipe'] + 0.000215766262209*dfmodel.loc[testdate, 'temp'] - 0.0136233610602
        dftest2 = -0.000294880146596*dfmodel.loc[testdate, 'LDC'] - 3.78527714832e-06*dfmodel.loc[testdate, 'pipe'] + 0.000110232272438*dfmodel.loc[testdate, 'temp'] + 0.00245769538549
        dftest3 = -0.000313868313404*dfmodel.loc[testdate, 'LDC'] - 0.000124412186835*dfmodel.loc[testdate, 'pipe'] - 0.000373332560219*dfmodel.loc[testdate, 'temp'] + 0.0298024201238
        dftest4 = 9.80413330949e-05*dfmodel.loc[testdate, 'LDC'] - 0.000117226241724*dfmodel.loc[testdate, 'pipe'] + 0.00066689537083*dfmodel.loc[testdate, 'temp'] - 0.0036033481573
        dftest5 = -8.69588782703e-05*dfmodel.loc[testdate, 'LDC'] - 2.50996911894e-05*dfmodel.loc[testdate, 'pipe'] + 0.000921064243197*dfmodel.loc[testdate, 'temp'] + 0.0107948805868

        #print(dftest1* dfmodel.loc[testdate, 'capa'], dftest2* dfmodel.loc[testdate, 'capa'], dftest3* dfmodel.loc[testdate, 'capa'], dftest4* dfmodel.loc[testdate, 'capa'], dftest5* dfmodel.loc[testdate, 'capa'])
        
        result = dfmodel['storagefcst'].copy()
        return result, dfmodel
        
    def storagefcst2(dfall, dfldz, dfpipe):
        
        dfldz = dfldz[['fcst']].resample('MS').mean()
        #print(dfldz)
        today=datetime.date.today()
        
        #dfmodel = dfall[['capa', 'Netwith', 'pipe', 'temp']].copy()
        dfmodel = dfall[[ 'Netwith', 'LDC','oil','pipe','Power','gdp','Brent','hydro','wind','coal','solar','other']].copy()

        dfmodel['LDC'] = dfldz
        
        dfmodel.fillna(method = 'ffill', inplace=True)
        #print(dfpipe)
        #dfmodel['temp'] = dftemp['Turkey']
        dfmodel = dfmodel.resample('MS').mean()
        #print(dfmodel.loc['2023-11-01'])
        for i in dfpipe.index:
            dfmodel.loc[i, 'pipe'] = dfpipe.loc[i,'fcst']
        
        #print(dfmodel)
        
        test_start = '2019-12-01'
        test_end =  str((today+relativedelta(months=1)).year)+'-'+str((today+relativedelta(months=0)).month)+'-01'
        target_start = str((today+relativedelta(months=0)).year)+'-'+str((today+relativedelta(months=0)).month)+'-01'
        target_end = str((today+relativedelta(months=1)).year+1)+'-12-31'
        
        columns = ['LDC','oil','pipe','Power']
        #train_X1, train_y1 = dfmodel.loc[test_start:test_end,columns], dfmodel.loc[test_start:test_end,['Netwith']]
        #print(dfmodel.columns)


        #for i in dfmodel.index:
            #print(i)
            #dfmodel.loc[i, 'storagefcst'] = 0.2938*dfmodel.loc[i, 'LDC'] -15.5178*dfmodel.loc[i, 'oil'] -0.3090*dfmodel.loc[i, 'pipe'] +0.4214 *dfmodel.loc[i, 'Power']
        for i in dfmodel.index:
            #print(i)
            dfmodel.loc[i, 'storagefcst'] = 0.29578242*dfmodel.loc[i, 'LDC'] -10.72249128*dfmodel.loc[i, 'oil'] -0.28742585*dfmodel.loc[i, 'pipe'] +0.43449918*dfmodel.loc[i, 'Power']-4.0189996870488915
        
        result = dfmodel['storagefcst'].copy()
        
        #dfmodel.loc['2019-12-01':,['storagefcst','Netwith']].plot(legend='True')
        #print('r2 linear: ', round(r2_score(dfmodel.loc['2019-12-01':'2023-09-01','Netwith'], dfmodel.loc['2019-12-01':'2023-09-01','storagefcst']),2))
        #print(result.loc['2023-08-01':])
        
        return result, dfmodel
    
    def lng_fcst(dfall, dfldz, dfpipe, dfindus,dfpower,dfstorage):
        #print(dfpower)
        demand=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'DemandCountry')
        dfdemand=demand.sql_to_df()
        dfdemand.set_index('Date', inplace=True)
        
        
        dffcst = dfall.copy()
        dffcst = dffcst.fillna(method='ffill').resample('MS').mean()
        #dfldz.loc['2023-01-01':,['fcst']].plot()
        dfldzfcst = dfldz[['fcst']].resample('MS').mean()
        #print(dfldzfcst.loc['2023-01-01':])
        #print(dffcst.loc['2023-01-01':])
        for i in dfpipe.index:
            dffcst.loc[i,'LDC'] = dfldzfcst.loc[i, 'fcst']
            dffcst.loc[i,'pipe'] = dfpipe.loc[i, 'fcst']
            dffcst.loc[i,'Industrial'] = dfindus.loc[i, 'fcst']
            dffcst.loc[i,'Power'] = dfpower.loc[i, 'fcst']
            dffcst.loc[i,'Netwith'] = dfstorage.loc[i]
        #print(dffcst.loc['2023-01-01':])
        
        dffcst['fcst'] = dffcst['LDC'] + dffcst['Industrial'] + dffcst['Power'] - dffcst['pipe'] - dffcst['prod'] - dffcst['Netwith']
        dffcst = dffcst.loc['2023-01-01':,['LDC','Industrial','Power','pipe','prod','Netwith','fcst']]
        
        today = datetime.date.today()
        dfdemandmin = dfdemand.loc[str(today.year-5)+'-01-01':str(today.year-1)+'-12-31',['Turkey']]
        dfdemandmin = dfdemandmin.resample('MS').mean().round(2)
        #print(dfdemandmin)
        dfdemandmin.index = pd.to_datetime(dfdemandmin.index)
        dfdemandmin['year'] = dfdemandmin.index.year
        dfdemandmin['date'] = dfdemandmin.index.strftime('%m-%d')
        dfdemandyoy = dfdemandmin.set_index(['year','date']).Turkey.unstack(0)
        dfdemandyoy['min'] = dfdemandyoy.min(axis=1)
        #print(dfdemandyoy)
        dffcst.loc['2023-01-01':'2023-12-01','5yrsmin'] = dfdemandyoy['min'].values
        dffcst.loc['2024-01-01':'2024-12-01','5yrsmin'] = dfdemandyoy['min'].values
        #print(dffcst)
        dffcst['fcstadj'] = dffcst['fcst'].round(2)
        for i in dffcst.index:
            if dffcst.loc[i, 'fcstadj'] < dffcst.loc[i,'5yrsmin']:
                dffcst.loc[i, 'fcstadj'] = dffcst.loc[i,'5yrsmin']
            
                
        dffcst['timestamp'] = datetime.datetime.now()
                
        
        #print(dffcst)
        #dffcst.loc['2023-01-01':,'fcst'].plot()
        #print(dffcst.loc['2023-01-01':,'Power'])
        #print(dffcst.loc['2022-07-01':,['LDC']].sum(axis=1))
        print(dffcst.loc['2022-07-01':,['LDC','Industrial','Power','pipe','prod','Netwith','fcst','5yrsmin','fcstadj']])
        print(dffcst.loc['2022-07-01':,['fcstadj']])
        return dffcst
    
   
    def chartpipe(dfall,dfpipeall):
        today = datetime.date.today()
        #print(dfpipeall)
        
        index = pd.date_range(start = str(today.year)+'-01-01', end=str(today.year)+'-12-01', freq='MS')
        
        #pipe ru
        figpipeRU=go.Figure()
        figpipeRU.add_trace(go.Scatter(x=index, y=dfpipeall.loc[str(today.year-5)+'-01-01':str(today.year-5)+'-12-01','pipeRU']+dfpipeall.loc[str(today.year-5)+'-01-01':str(today.year-5)+'-12-01','pipeGR'],
                            mode='lines',
                            name=str(today.year-5),
                            #line=dict(color='red', dash='solid')
                            ))
        figpipeRU.add_trace(go.Scatter(x=index, y=dfpipeall.loc[str(today.year-4)+'-01-01':str(today.year-4)+'-12-01','pipeRU']+dfpipeall.loc[str(today.year-4)+'-01-01':str(today.year-4)+'-12-01','pipeGR'],
                            mode='lines',
                            name=str(today.year-4),
                            #line=dict(color='red', dash='solid')
                            ))
        figpipeRU.add_trace(go.Scatter(x=index, y=dfpipeall.loc[str(today.year-3)+'-01-01':str(today.year-3)+'-12-01','pipeRU']+dfpipeall.loc[str(today.year-3)+'-01-01':str(today.year-3)+'-12-01','pipeGR'],
                            mode='lines',
                            name=str(today.year-3),
                            #line=dict(color='red', dash='solid')
                            ))
        figpipeRU.add_trace(go.Scatter(x=index, y=dfpipeall.loc[str(today.year-2)+'-01-01':str(today.year-2)+'-12-01','pipeRU']+dfpipeall.loc[str(today.year-2)+'-01-01':str(today.year-2)+'-12-01','pipeGR'],
                            mode='lines',
                            name=str(today.year-2),
                            #line=dict(color='red', dash='solid')
                            ))
        figpipeRU.add_trace(go.Scatter(x=index, y=dfpipeall.loc[str(today.year-1)+'-01-01':str(today.year-1)+'-12-01','pipeRU']+dfpipeall.loc[str(today.year-1)+'-01-01':str(today.year-1)+'-12-01','pipeGR'],
                            mode='lines',
                            name=str(today.year-1),
                            #line=dict(color='red', dash='solid')
                            ))
        figpipeRU.add_trace(go.Scatter(x=index, y=dfpipeall.loc[str(today.year-0)+'-01-01':str(today.year-0)+'-'+str(today.month-1)+'-01','pipeRU']+dfpipeall.loc[str(today.year-0)+'-01-01':str(today.year-0)+'-'+str(today.month-1)+'-01','pipeGR'],
                            mode='lines',
                            name=str(today.year-0),
                            #line=dict(color='red', dash='solid')
                            ))
        figpipeRU.add_trace(go.Scatter(x=index, y=dfpipeall.loc[str(today.year-0)+'-01-01':str(today.year-0)+'-12-01','pipeRUfcst'],
                            mode='lines',
                            name=str(today.year-0)+' fcst',
                            line=dict(color='red', dash='solid')
                            ))
        figpipeRU.add_trace(go.Scatter(x=index, y=dfpipeall.loc[str(today.year+1)+'-01-01':str(today.year+1)+'-12-01','pipeRUfcst'],
                            mode='lines',
                            name=str(today.year+1)+' fcst',
                            line=dict(color='red', dash='dot')
                            ))
        figpipeRU.update_layout(
             autosize=True,
             showlegend=True,
             #colorscale='RdBu',
             legend=dict(x=0, y=-0.2),
             legend_orientation="h",
             title_text='Net RU pipe import '+str(today)+' ,Mcm/d',
             xaxis = dict(dtick="M1"),
             hovermode='x unified',
             plot_bgcolor = 'white',
             template='ggplot2'  
         )
        py.plot(figpipeRU, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/TR chart/NetRUpipe chart.html', auto_open=False)
        
        #pipe AZ
        #print(dfpipeall)
        figpipeAZ=go.Figure()
        figpipeAZ.add_trace(go.Scatter(x=index, y=dfpipeall.loc[str(today.year-5)+'-01-01':str(today.year-5)+'-12-01','pipeAZ']+dfpipeall.loc[str(today.year-5)+'-01-01':str(today.year-5)+'-12-01','pipeBG'],
                            mode='lines',
                            name=str(today.year-5),
                            #line=dict(color='red', dash='solid')
                            ))
        figpipeAZ.add_trace(go.Scatter(x=index, y=dfpipeall.loc[str(today.year-4)+'-01-01':str(today.year-4)+'-12-01','pipeAZ']+dfpipeall.loc[str(today.year-4)+'-01-01':str(today.year-4)+'-12-01','pipeBG'],
                            mode='lines',
                            name=str(today.year-4),
                            #line=dict(color='red', dash='solid')
                            ))
        figpipeAZ.add_trace(go.Scatter(x=index, y=dfpipeall.loc[str(today.year-3)+'-01-01':str(today.year-3)+'-12-01','pipeAZ']+dfpipeall.loc[str(today.year-3)+'-01-01':str(today.year-3)+'-12-01','pipeBG'],
                            mode='lines',
                            name=str(today.year-3),
                            #line=dict(color='red', dash='solid')
                            ))
        figpipeAZ.add_trace(go.Scatter(x=index, y=dfpipeall.loc[str(today.year-2)+'-01-01':str(today.year-2)+'-12-01','pipeAZ']+dfpipeall.loc[str(today.year-2)+'-01-01':str(today.year-2)+'-12-01','pipeBG'],
                            mode='lines',
                            name=str(today.year-2),
                            #line=dict(color='red', dash='solid')
                            ))
        figpipeAZ.add_trace(go.Scatter(x=index, y=dfpipeall.loc[str(today.year-1)+'-01-01':str(today.year-1)+'-12-01','pipeAZ']+dfpipeall.loc[str(today.year-1)+'-01-01':str(today.year-1)+'-12-01','pipeBG'],
                            mode='lines',
                            name=str(today.year-1),
                            #line=dict(color='red', dash='solid')
                            ))
        figpipeAZ.add_trace(go.Scatter(x=index, y=dfpipeall.loc[str(today.year-0)+'-01-01':str(today.year-0)+'-'+str(today.month-1)+'-01','pipeAZ']+dfpipeall.loc[str(today.year-0)+'-01-01':str(today.year-0)+'-'+str(today.month-1)+'-01','pipeBG'],
                            mode='lines',
                            name=str(today.year-0),
                            #line=dict(color='red', dash='solid')
                            ))
        figpipeAZ.add_trace(go.Scatter(x=index, y=dfpipeall.loc[str(today.year-0)+'-01-01':str(today.year-0)+'-12-01','pipeAZfcst'],
                            mode='lines',
                            name=str(today.year-0)+' fcst',
                            line=dict(color='red', dash='solid')
                            ))
        figpipeAZ.add_trace(go.Scatter(x=index, y=dfpipeall.loc[str(today.year+1)+'-01-01':str(today.year+1)+'-12-01','pipeAZfcst'],
                            mode='lines',
                            name=str(today.year+1)+' fcst',
                            line=dict(color='red', dash='dot')
                            ))
        figpipeAZ.update_layout(
             autosize=True,
             showlegend=True,
             #colorscale='RdBu',
             legend=dict(x=0, y=-0.2),
             legend_orientation="h",
             title_text='Net AZ pipe import '+str(today)+' ,Mcm/d',
             xaxis = dict(dtick="M1"),
             hovermode='x unified',
             plot_bgcolor = 'white',
             template='ggplot2'  
         )
        py.plot(figpipeAZ, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/TR chart/NetAZpipe chart.html', auto_open=False)
        
        #pipe IR
        figpipeIR=go.Figure()
        figpipeIR.add_trace(go.Scatter(x=index, y=dfpipeall.loc[str(today.year-5)+'-01-01':str(today.year-5)+'-12-01','pipeIR'],
                            mode='lines',
                            name=str(today.year-5),
                            #line=dict(color='red', dash='solid')
                            ))
        figpipeIR.add_trace(go.Scatter(x=index, y=dfpipeall.loc[str(today.year-4)+'-01-01':str(today.year-4)+'-12-01','pipeIR'],
                            mode='lines',
                            name=str(today.year-4),
                            #line=dict(color='red', dash='solid')
                            ))
        figpipeIR.add_trace(go.Scatter(x=index, y=dfpipeall.loc[str(today.year-3)+'-01-01':str(today.year-3)+'-12-01','pipeIR'],
                            mode='lines',
                            name=str(today.year-3),
                            #line=dict(color='red', dash='solid')
                            ))
        figpipeIR.add_trace(go.Scatter(x=index, y=dfpipeall.loc[str(today.year-2)+'-01-01':str(today.year-2)+'-12-01','pipeIR'],
                            mode='lines',
                            name=str(today.year-2),
                            #line=dict(color='red', dash='solid')
                            ))
        figpipeIR.add_trace(go.Scatter(x=index, y=dfpipeall.loc[str(today.year-1)+'-01-01':str(today.year-1)+'-12-01','pipeIR'],
                            mode='lines',
                            name=str(today.year-1),
                            #line=dict(color='red', dash='solid')
                            ))
        figpipeIR.add_trace(go.Scatter(x=index, y=dfpipeall.loc[str(today.year-0)+'-01-01':str(today.year-0)+'-'+str(today.month-1)+'-01','pipeIR'],
                            mode='lines',
                            name=str(today.year-0),
                            #line=dict(color='red', dash='solid')
                            ))
        figpipeIR.add_trace(go.Scatter(x=index, y=dfpipeall.loc[str(today.year-0)+'-01-01':str(today.year-0)+'-12-01','pipeIRfcst'],
                            mode='lines',
                            name=str(today.year-0)+' fcst',
                            line=dict(color='red', dash='solid')
                            ))
        figpipeIR.add_trace(go.Scatter(x=index, y=dfpipeall.loc[str(today.year+1)+'-01-01':str(today.year+1)+'-12-01','pipeIRfcst'],
                            mode='lines',
                            name=str(today.year+1)+' fcst',
                            line=dict(color='red', dash='dot')
                            ))
        figpipeIR.update_layout(
             autosize=True,
             showlegend=True,
             #colorscale='RdBu',
             legend=dict(x=0, y=-0.2),
             legend_orientation="h",
             title_text='Net IR pipe import '+str(today)+' ,Mcm/d',
             xaxis = dict(dtick="M1"),
             hovermode='x unified',
             plot_bgcolor = 'white',
             template='ggplot2'  
         )
        py.plot(figpipeIR, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/TR chart/NetIRpipe chart.html', auto_open=False)
        
        #pipe total
        figpipetotal=go.Figure()
        figpipetotal.add_trace(go.Scatter(x=index, y=dfpipeall.loc[str(today.year-5)+'-01-01':str(today.year-5)+'-12-01','pipe'],
                            mode='lines',
                            name=str(today.year-5),
                            #line=dict(color='red', dash='solid')
                            ))
        figpipetotal.add_trace(go.Scatter(x=index, y=dfpipeall.loc[str(today.year-4)+'-01-01':str(today.year-4)+'-12-01','pipe'],
                            mode='lines',
                            name=str(today.year-4),
                            #line=dict(color='red', dash='solid')
                            ))
        figpipetotal.add_trace(go.Scatter(x=index, y=dfpipeall.loc[str(today.year-3)+'-01-01':str(today.year-3)+'-12-01','pipe'],
                            mode='lines',
                            name=str(today.year-3),
                            #line=dict(color='red', dash='solid')
                            ))
        figpipetotal.add_trace(go.Scatter(x=index, y=dfpipeall.loc[str(today.year-2)+'-01-01':str(today.year-2)+'-12-01','pipe'],
                            mode='lines',
                            name=str(today.year-2),
                            #line=dict(color='red', dash='solid')
                            ))
        figpipetotal.add_trace(go.Scatter(x=index, y=dfpipeall.loc[str(today.year-1)+'-01-01':str(today.year-1)+'-12-01','pipe'],
                            mode='lines',
                            name=str(today.year-1),
                            #line=dict(color='red', dash='solid')
                            ))
        figpipetotal.add_trace(go.Scatter(x=index, y=dfpipeall.loc[str(today.year-0)+'-01-01':str(today.year-0)+'-'+str(today.month-1)+'-01','pipe'],
                            mode='lines',
                            name=str(today.year-0),
                            #line=dict(color='red', dash='solid')
                            ))
        figpipetotal.add_trace(go.Scatter(x=index, y=dfpipeall.loc[str(today.year-0)+'-01-01':str(today.year-0)+'-12-01','TRfcst'],
                            mode='lines',
                            name=str(today.year-0)+' fcst',
                            line=dict(color='red', dash='solid')
                            ))
        figpipetotal.add_trace(go.Scatter(x=index, y=dfpipeall.loc[str(today.year+1)+'-01-01':str(today.year+1)+'-12-01','TRfcst'],
                            mode='lines',
                            name=str(today.year+1)+' fcst',
                            line=dict(color='red', dash='dot')
                            ))
        figpipetotal.update_layout(
             autosize=True,
             showlegend=True,
             #colorscale='RdBu',
             legend=dict(x=0, y=-0.2),
             legend_orientation="h",
             title_text='Net Total pipe import '+str(today)+' ,Mcm/d',
             xaxis = dict(dtick="M1"),
             hovermode='x unified',
             plot_bgcolor = 'white',
             template='ggplot2'  
         )
        py.plot(figpipetotal, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/TR chart/pipetotal chart.html', auto_open=False)
        
   
        
    def chartstorage(dfstorageall, dfstorage):
        
        #print(dfall)
        #print(dfstorage)
        
        today = datetime.date.today()
        #print(dfpipeall)
        
        index = pd.date_range(start = str(today.year)+'-01-01', end=str(today.year)+'-12-01', freq='MS')
        
        for i in ['hydro','wind','oil','coal','solar','other']:
        
            fig=go.Figure()
            fig.add_trace(go.Scatter(x=index, y=dfstorageall.loc[str(today.year-5)+'-01-01':str(today.year-5)+'-12-01',i],
                                mode='lines',
                                name=str(today.year-5),
                                #line=dict(color='red', dash='solid')
                                ))
            fig.add_trace(go.Scatter(x=index, y=dfstorageall.loc[str(today.year-4)+'-01-01':str(today.year-4)+'-12-01',i],
                                mode='lines',
                                name=str(today.year-4),
                                #line=dict(color='red', dash='solid')
                                ))
            fig.add_trace(go.Scatter(x=index, y=dfstorageall.loc[str(today.year-3)+'-01-01':str(today.year-3)+'-12-01',i],
                                mode='lines',
                                name=str(today.year-3),
                                #line=dict(color='red', dash='solid')
                                ))
            fig.add_trace(go.Scatter(x=index, y=dfstorageall.loc[str(today.year-2)+'-01-01':str(today.year-2)+'-12-01',i],
                                mode='lines',
                                name=str(today.year-2),
                                #line=dict(color='red', dash='solid')
                                ))
            fig.add_trace(go.Scatter(x=index, y=dfstorageall.loc[str(today.year-1)+'-01-01':str(today.year-1)+'-12-01',i],
                                mode='lines',
                                name=str(today.year-1),
                                #line=dict(color='red', dash='solid')
                                ))
            fig.add_trace(go.Scatter(x=index, y=dfstorageall.loc[str(today.year-0)+'-01-01':str(today.year-0)+'-'+str(today.month-1)+'-01',i],
                                mode='lines',
                                name=str(today.year-0),
                                #line=dict(color='red', dash='solid')
                                ))
            fig.add_trace(go.Scatter(x=index, y=dfstorageall.loc[str(today.year-0)+'-01-01':str(today.year-0)+'-12-01',i],
                                mode='lines',
                                name=str(today.year-0)+' fcst',
                                line=dict(color='red', dash='solid')
                                ))
            fig.add_trace(go.Scatter(x=index, y=dfstorageall.loc[str(today.year+1)+'-01-01':str(today.year+1)+'-12-01',i],
                                mode='lines',
                                name=str(today.year+1)+' fcst',
                                line=dict(color='red', dash='dot')
                                ))
            fig.update_layout(
                 autosize=True,
                 showlegend=True,
                 #colorscale='RdBu',
                 legend=dict(x=0, y=-0.2),
                 legend_orientation="h",
                 title_text=i+' '+str(today)+' ,Mcm/d',
                 xaxis = dict(dtick="M1"),
                 hovermode='x unified',
                 plot_bgcolor = 'white',
                 template='ggplot2'  
             )
            py.plot(fig, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/TR chart/'+i+' chart.html', auto_open=False)
         
            
         #storage
        fig1=go.Figure()
        fig1.add_trace(go.Scatter(x=index, y=dfstorageall.loc[str(today.year-5)+'-01-01':str(today.year-5)+'-12-01','Netwith'],
                            mode='lines',
                            name=str(today.year-5),
                            #line=dict(color='red', dash='solid')
                            ))
        fig1.add_trace(go.Scatter(x=index, y=dfstorageall.loc[str(today.year-4)+'-01-01':str(today.year-4)+'-12-01','Netwith'],
                            mode='lines',
                            name=str(today.year-4),
                            #line=dict(color='red', dash='solid')
                            ))
        fig1.add_trace(go.Scatter(x=index, y=dfstorageall.loc[str(today.year-3)+'-01-01':str(today.year-3)+'-12-01','Netwith'],
                            mode='lines',
                            name=str(today.year-3),
                            #line=dict(color='red', dash='solid')
                            ))
        fig1.add_trace(go.Scatter(x=index, y=dfstorageall.loc[str(today.year-2)+'-01-01':str(today.year-2)+'-12-01','Netwith'],
                            mode='lines',
                            name=str(today.year-2),
                            #line=dict(color='red', dash='solid')
                            ))
        fig1.add_trace(go.Scatter(x=index, y=dfstorageall.loc[str(today.year-1)+'-01-01':str(today.year-1)+'-12-01','Netwith'],
                            mode='lines',
                            name=str(today.year-1),
                            #line=dict(color='red', dash='solid')
                            ))
        fig1.add_trace(go.Scatter(x=index, y=dfstorageall.loc[str(today.year-0)+'-01-01':str(today.year-0)+'-'+str(today.month-1)+'-01','Netwith'],
                            mode='lines',
                            name=str(today.year-0),
                            #line=dict(color='red', dash='solid')
                            ))
        fig1.add_trace(go.Scatter(x=index, y=dfstorage.loc[str(today.year-0)+'-01-01':str(today.year-0)+'-12-01'],
                            mode='lines',
                            name=str(today.year-0)+' fcst',
                            line=dict(color='red', dash='solid')
                            ))
        fig1.add_trace(go.Scatter(x=index, y=dfstorage.loc[str(today.year+1)+'-01-01':str(today.year+1)+'-12-01'],
                            mode='lines',
                            name=str(today.year+1)+' fcst',
                            line=dict(color='red', dash='dot')
                            ))
        fig1.update_layout(
             autosize=True,
             showlegend=True,
             #colorscale='RdBu',
             legend=dict(x=0, y=-0.2),
             legend_orientation="h",
             title_text='Net Withdraw '+str(today)+' ,Mcm/d',
             xaxis = dict(dtick="M1"),
             hovermode='x unified',
             plot_bgcolor = 'white',
             template='ggplot2'  
         )
        py.plot(fig1, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/TR chart/storage chart.html', auto_open=False)
            
    
    def chartprod(dfall):
        
        dfchart = dfall[['prod']].copy()
        dfchart.fillna(method = 'ffill', inplace=True)
        dfchart = dfchart.resample('MS').mean()
        #print(dfchart)
        today = datetime.date.today()
        #print(dfpipeall)
        
        index = pd.date_range(start = str(today.year)+'-01-01', end=str(today.year)+'-12-01', freq='MS')
        
        fig1=go.Figure()
        fig1.add_trace(go.Scatter(x=index, y=dfchart.loc[str(today.year-5)+'-01-01':str(today.year-5)+'-12-01','prod'],
                            mode='lines',
                            name=str(today.year-5),
                            #line=dict(color='red', dash='solid')
                            ))
        fig1.add_trace(go.Scatter(x=index, y=dfchart.loc[str(today.year-4)+'-01-01':str(today.year-4)+'-12-01','prod'],
                            mode='lines',
                            name=str(today.year-4),
                            #line=dict(color='red', dash='solid')
                            ))
        fig1.add_trace(go.Scatter(x=index, y=dfchart.loc[str(today.year-3)+'-01-01':str(today.year-3)+'-12-01','prod'],
                            mode='lines',
                            name=str(today.year-3),
                            #line=dict(color='red', dash='solid')
                            ))
        fig1.add_trace(go.Scatter(x=index, y=dfchart.loc[str(today.year-2)+'-01-01':str(today.year-2)+'-12-01','prod'],
                            mode='lines',
                            name=str(today.year-2),
                            #line=dict(color='red', dash='solid')
                            ))
        fig1.add_trace(go.Scatter(x=index, y=dfchart.loc[str(today.year-1)+'-01-01':str(today.year-1)+'-12-01','prod'],
                            mode='lines',
                            name=str(today.year-1),
                            #line=dict(color='red', dash='solid')
                            ))
        fig1.add_trace(go.Scatter(x=index, y=dfchart.loc[str(today.year-0)+'-01-01':str(today.year-0)+'-'+str(today.month-1)+'-01','prod'],
                            mode='lines',
                            name=str(today.year-0),
                            #line=dict(color='red', dash='solid')
                            ))
        fig1.add_trace(go.Scatter(x=index, y=dfchart.loc[str(today.year-0)+'-01-01':str(today.year-0)+'-12-01','prod'],
                            mode='lines',
                            name=str(today.year-0)+' fcst',
                            line=dict(color='red', dash='solid')
                            ))
        fig1.add_trace(go.Scatter(x=index, y=dfchart.loc[str(today.year+1)+'-01-01':str(today.year+1)+'-12-01','prod'],
                            mode='lines',
                            name=str(today.year+1)+' fcst',
                            line=dict(color='red', dash='dot')
                            ))
        fig1.update_layout(
             autosize=True,
             showlegend=True,
             #colorscale='RdBu',
             legend=dict(x=0, y=-0.2),
             legend_orientation="h",
             title_text='Prod '+str(today)+' ,Mcm/d',
             xaxis = dict(dtick="M1"),
             hovermode='x unified',
             plot_bgcolor = 'white',
             template='ggplot2'  
         )
        py.plot(fig1, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/TR chart/prod chart.html', auto_open=False)
            
    
    def chartldz(dfall,dfldz):
        
        #print(dfldz)
        #print(dfall)
        #print(dfchart)
        dfchart = dfall[['LDC']].copy()
        dfchart.fillna(method = 'ffill', inplace=True)
        dfchart = dfchart.resample('MS').mean()
        
        dfchartfcst = dfldz[['fcst']].copy()
        dfchartfcst.fillna(method = 'ffill', inplace=True)
        dfchartfcst = dfchartfcst.resample('MS').mean()
        
        today = datetime.date.today()
        #print(dfpipeall)
        
        index = pd.date_range(start = str(today.year)+'-01-01', end=str(today.year)+'-12-01', freq='MS')
        
        fig1=go.Figure()
        fig1.add_trace(go.Scatter(x=index, y=dfchart.loc[str(today.year-5)+'-01-01':str(today.year-5)+'-12-01','LDC'],
                            mode='lines',
                            name=str(today.year-5),
                            #line=dict(color='red', dash='solid')
                            ))
        fig1.add_trace(go.Scatter(x=index, y=dfchart.loc[str(today.year-4)+'-01-01':str(today.year-4)+'-12-01','LDC'],
                            mode='lines',
                            name=str(today.year-4),
                            #line=dict(color='red', dash='solid')
                            ))
        fig1.add_trace(go.Scatter(x=index, y=dfchart.loc[str(today.year-3)+'-01-01':str(today.year-3)+'-12-01','LDC'],
                            mode='lines',
                            name=str(today.year-3),
                            #line=dict(color='red', dash='solid')
                            ))
        fig1.add_trace(go.Scatter(x=index, y=dfchart.loc[str(today.year-2)+'-01-01':str(today.year-2)+'-12-01','LDC'],
                            mode='lines',
                            name=str(today.year-2),
                            #line=dict(color='red', dash='solid')
                            ))
        fig1.add_trace(go.Scatter(x=index, y=dfchart.loc[str(today.year-1)+'-01-01':str(today.year-1)+'-12-01','LDC'],
                            mode='lines',
                            name=str(today.year-1),
                            #line=dict(color='red', dash='solid')
                            ))
        fig1.add_trace(go.Scatter(x=index, y=dfchart.loc[str(today.year-0)+'-01-01':str(today.year-0)+'-'+str(today.month-1)+'-01','LDC'],
                            mode='lines',
                            name=str(today.year-0),
                            #line=dict(color='red', dash='solid')
                            ))
        fig1.add_trace(go.Scatter(x=index, y=dfchartfcst.loc[str(today.year-0)+'-01-01':str(today.year-0)+'-12-01','fcst'],
                            mode='lines',
                            name=str(today.year-0)+' fcst',
                            line=dict(color='red', dash='solid')
                            ))
        fig1.add_trace(go.Scatter(x=index, y=dfchartfcst.loc[str(today.year+1)+'-01-01':str(today.year+1)+'-12-01','fcst'],
                            mode='lines',
                            name=str(today.year+1)+' fcst',
                            line=dict(color='red', dash='dot')
                            ))
        fig1.update_layout(
             autosize=True,
             showlegend=True,
             #colorscale='RdBu',
             legend=dict(x=0, y=-0.2),
             legend_orientation="h",
             title_text='LDC '+str(today)+' ,Mcm/d',
             xaxis = dict(dtick="M1"),
             hovermode='x unified',
             plot_bgcolor = 'white',
             template='ggplot2'  
         )
        py.plot(fig1, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/TR chart/ldc chart.html', auto_open=False)
            
    def chartindus(dfall, dfindus):
        
        
        dfchart = dfall[['Industrial']].copy()
        dfchart.fillna(method = 'ffill', inplace=True)
        dfchart = dfchart.resample('MS').mean()
        
        #print(dfindus)
        #print(dfchart)
        today = datetime.date.today()
        #print(dfpipeall)
        
        index = pd.date_range(start = str(today.year)+'-01-01', end=str(today.year)+'-12-01', freq='MS')
        
        fig1=go.Figure()
        fig1.add_trace(go.Scatter(x=index, y=dfchart.loc[str(today.year-5)+'-01-01':str(today.year-5)+'-12-01','Industrial'],
                            mode='lines',
                            name=str(today.year-5),
                            #line=dict(color='red', dash='solid')
                            ))
        fig1.add_trace(go.Scatter(x=index, y=dfchart.loc[str(today.year-4)+'-01-01':str(today.year-4)+'-12-01','Industrial'],
                            mode='lines',
                            name=str(today.year-4),
                            #line=dict(color='red', dash='solid')
                            ))
        fig1.add_trace(go.Scatter(x=index, y=dfchart.loc[str(today.year-3)+'-01-01':str(today.year-3)+'-12-01','Industrial'],
                            mode='lines',
                            name=str(today.year-3),
                            #line=dict(color='red', dash='solid')
                            ))
        fig1.add_trace(go.Scatter(x=index, y=dfchart.loc[str(today.year-2)+'-01-01':str(today.year-2)+'-12-01','Industrial'],
                            mode='lines',
                            name=str(today.year-2),
                            #line=dict(color='red', dash='solid')
                            ))
        fig1.add_trace(go.Scatter(x=index, y=dfchart.loc[str(today.year-1)+'-01-01':str(today.year-1)+'-12-01','Industrial'],
                            mode='lines',
                            name=str(today.year-1),
                            #line=dict(color='red', dash='solid')
                            ))
        fig1.add_trace(go.Scatter(x=index, y=dfchart.loc[str(today.year-0)+'-01-01':str(today.year-0)+'-'+str(today.month-1)+'-01','Industrial'],
                            mode='lines',
                            name=str(today.year-0),
                            #line=dict(color='red', dash='solid')
                            ))
        fig1.add_trace(go.Scatter(x=index, y=dfindus.loc[str(today.year-0)+'-01-01':str(today.year-0)+'-12-01','fcst'],
                            mode='lines',
                            name=str(today.year-0)+' fcst',
                            line=dict(color='red', dash='solid')
                            ))
        fig1.add_trace(go.Scatter(x=index, y=dfindus.loc[str(today.year+1)+'-01-01':str(today.year+1)+'-12-01','fcst'],
                            mode='lines',
                            name=str(today.year+1)+' fcst',
                            line=dict(color='red', dash='dot')
                            ))
        fig1.update_layout(
             autosize=True,
             showlegend=True,
             #colorscale='RdBu',
             legend=dict(x=0, y=-0.2),
             legend_orientation="h",
             title_text='Industrial '+str(today)+' ,Mcm/d',
             xaxis = dict(dtick="M1"),
             hovermode='x unified',
             plot_bgcolor = 'white',
             template='ggplot2'  
         )
        py.plot(fig1, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/TR chart/indus chart.html', auto_open=False)
            
    def chartpower(dfall, dfpower):
        
        dfchart = dfall[['Power']].copy()
        dfchart.fillna(method = 'ffill', inplace=True)
        dfchart = dfchart.resample('MS').mean()
        
        
        dfchart['fcst'] = dfpower['fcst']
        #print(dfpower)
        #print(dfchart)
        today = datetime.date.today()
        #print(dfpipeall)
        
        index = pd.date_range(start = str(today.year)+'-01-01', end=str(today.year)+'-12-01', freq='MS')
        
        fig1=go.Figure()
        fig1.add_trace(go.Scatter(x=index, y=dfchart.loc[str(today.year-5)+'-01-01':str(today.year-5)+'-12-01','Power'],
                            mode='lines',
                            name=str(today.year-5),
                            #line=dict(color='red', dash='solid')
                            ))
        fig1.add_trace(go.Scatter(x=index, y=dfchart.loc[str(today.year-4)+'-01-01':str(today.year-4)+'-12-01','Power'],
                            mode='lines',
                            name=str(today.year-4),
                            #line=dict(color='red', dash='solid')
                            ))
        fig1.add_trace(go.Scatter(x=index, y=dfchart.loc[str(today.year-3)+'-01-01':str(today.year-3)+'-12-01','Power'],
                            mode='lines',
                            name=str(today.year-3),
                            #line=dict(color='red', dash='solid')
                            ))
        fig1.add_trace(go.Scatter(x=index, y=dfchart.loc[str(today.year-2)+'-01-01':str(today.year-2)+'-12-01','Power'],
                            mode='lines',
                            name=str(today.year-2),
                            #line=dict(color='red', dash='solid')
                            ))
        fig1.add_trace(go.Scatter(x=index, y=dfchart.loc[str(today.year-1)+'-01-01':str(today.year-1)+'-12-01','Power'],
                            mode='lines',
                            name=str(today.year-1),
                            #line=dict(color='red', dash='solid')
                            ))
        fig1.add_trace(go.Scatter(x=index, y=dfchart.loc[str(today.year-0)+'-01-01':str(today.year-0)+'-'+str(today.month-1)+'-01','Power'],
                            mode='lines',
                            name=str(today.year-0),
                            #line=dict(color='red', dash='solid')
                            ))
        fig1.add_trace(go.Scatter(x=index, y=dfchart.loc[str(today.year-0)+'-01-01':str(today.year-0)+'-12-01','fcst'],
                            mode='lines',
                            name=str(today.year-0)+' fcst',
                            line=dict(color='red', dash='solid')
                            ))
        fig1.add_trace(go.Scatter(x=index, y=dfchart.loc[str(today.year+1)+'-01-01':str(today.year+1)+'-12-01','fcst'],
                            mode='lines',
                            name=str(today.year+1)+' fcst',
                            line=dict(color='red', dash='dot')
                            ))
        fig1.update_layout(
             autosize=True,
             showlegend=True,
             #colorscale='RdBu',
             legend=dict(x=0, y=-0.2),
             legend_orientation="h",
             title_text='power '+str(today)+' ,Mcm/d',
             xaxis = dict(dtick="M1"),
             hovermode='x unified',
             plot_bgcolor = 'white',
             template='ggplot2'  
         )
        py.plot(fig1, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/TR chart/power chart.html', auto_open=False)
            
    
    def update():
        
        dfce = Fundamental_research.get_data()
        dftemp = Fundamental_research.get_temp_data()
        
        dfprice = Fundamental_research.get_price()
        
        dfprod = Fundamental_research.production()
        dfall = Fundamental_research.fulldata(dfce, dftemp, dfprice, dfprod)
        
        dfldz = Fundamental_research.ldz(dfall)
        dfpipe, dfpipeall = Fundamental_research.pipefcst(dfall)
        #dfindus = Fundamental_research.indfcst(dfall, dfpipe)
        dfindus = Fundamental_research.indfcst1(dfall, dfpipeall)

        
        dfpower = Fundamental_research.powerfcst(dfall, dfpipe)
        #dfstorage, dfstorageall = Fundamental_research.storagefcst(dfall, dfpipe)
        #dfstorage, dfstorageall = Fundamental_research.storagefcst1(dfall,dfldz, dfpipe)
        dfstorage, dfstorageall = Fundamental_research.storagefcst2(dfall,dfldz, dfpipe)
        dffcst = Fundamental_research.lng_fcst(dfall, dfldz, dfpipe, dfindus, dfpower, dfstorage)
        
        Fundamental_research.chartpipe(dfall, dfpipeall)
        Fundamental_research.chartstorage(dfstorageall, dfstorage)
        Fundamental_research.chartprod(dfall)
        Fundamental_research.chartldz(dfall, dfldz)
        Fundamental_research.chartindus(dfall, dfindus)
        Fundamental_research.chartpower(dfall, dfpower)
        #print(dfpipe)
        
        db_server_lng = "PRD-DB-SQL-211"
        db_name_lng = "LNG"
        sql_engine_lng = sqlalchemy.create_engine("mssql+pyodbc://" + db_server_lng + "/" + db_name_lng + "?trusted_connection=yes&driver=ODBC+Driver+13+for+SQL+Server")
        
        dffcst.to_sql(name='TurkeyLNGfcst', con=sql_engine_lng, index=True, if_exists='append', schema='ana')
        
Fundamental_research.update()