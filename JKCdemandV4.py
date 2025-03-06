# -*- coding: utf-8 -*-
"""
Created on Tue Mar 22 14:09:25 2022

@author: SVC-GASQuant2-Prod
"""





#V1 table color ><+-20, hist range uses full 1980 - 2020
#v2 add +-1,2,3,4 degree scenarios, replace 25-5% to 1 std div, Table + D-SN cumsum, 1,2,4 chart + today line
#V3 chart3 all scenarios against normal
#v4 go to sum 22 

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
import sqlalchemy
from dateutil.relativedelta import relativedelta
import requests
sys.path.append('C:\\Users\SVC-GASQuant2-Prod\YWwebsite\Python Scripts\class') 
pd.set_option('display.max_columns',10)
from DBtoDF import DBTOPD
from temp3 import temp_hist

class JKC_Demand():
    
    def temp_hist_data():
        
        today=datetime.date.today()
        
        temp=temp_hist

        countrylist=['China','Japan','South Korea']
        
        dffull = pd.DataFrame(index=pd.date_range(start='1980-01-01', end=str(today.year)+'-12-31')) 
        
        for i in countrylist:
            try:
                df=temp.get_data(i, '1980-01-01',today)
                #print(df)
                df=df.resample('D').mean()
                dffull.loc['1980-01-01':df.index[-1],i]=df.loc['1980-01-01':,'2m Temp [C]']
                dffull.loc['1980-01-01':df.index[-1],i+' normal'] = df.loc['1980-01-01':,'2m Temp 1991-2020 [C]']
            except (FileNotFoundError, KeyError) as e:
                print(i, e)
        
        dffull=dffull.round(1)
        
        
        return dffull
    
    def date(temphist):
        
        today = datetime.date.today()
        
        '''
        #win date
        if today.month <10:
            start = str(today.year-1)+'-10-01'
        
            end = str(today.year)+'-03-31'#pd.to_datetime(str(next_month.year)+'-'+str(next_month.month)+'-'+str(calendar.monthrange(next_month.year, next_month.month)[1]))
        else:
            start = str(today.year)+'-10-01'
            end = str(today.year+1)+'-03-31'
        
        if today + relativedelta(months=(1)) <= pd.to_datetime(end):
            
            next_month = today + relativedelta(months=(1))
            
        else:
            next_month =  pd.to_datetime(end)
            
        if today + relativedelta(days=(15)) >  pd.to_datetime(end):
            ens_end = pd.to_datetime(end)
        else:
            ens_end = today + relativedelta(days=(15))
        '''
        
        #sum date
                                 
        start =  pd.to_datetime(str(today.year)+'-10-01')
        end =  pd.to_datetime(str(today.year+1)+'-03-31')
        #hist_start = today - relativedelta(days=(7))
        hist_start = temphist.Japan.loc[~temphist.Japan.isnull()].index[-1]
        if today + relativedelta(months=(1)) <= pd.to_datetime(end):
            
            next_month = today + relativedelta(months=(1))
            
        else:
            next_month =  pd.to_datetime(end)
            
        if today + relativedelta(days=(15)) >  pd.to_datetime(end):
            ens_end = pd.to_datetime(end)
        else:
            ens_end = today + relativedelta(days=(15))
            
        return start, end, next_month, ens_end,hist_start
    
    def temp_data():   
        
        weather=DBTOPD('PRD-DB-SQL-211','Meteorology', 'dbo', 'MeteomaticsPointDataTimeseries')
        #get temp data latest
        temp=weather.get_temp_data()
        dftemp = temp[['country','wmo_weighting','model_source','value_date','average_value']].copy()
        dftemp['weighted_value'] = dftemp['wmo_weighting']*dftemp['average_value'] #weighted
        dftemp.set_index('country', inplace=True)
        #print(dftemp)
        
        dffullcountry = pd.DataFrame()
        for country in ['China','Japan','South Korea']:
            
            dftempcountry = dftemp.loc[country].copy()
            dftempcountry = dftempcountry[['model_source','value_date','weighted_value']]
            dftempcountry.set_index('model_source', inplace=True)
            
            dftempens = dftempcountry.loc['ecmwf-ens',['value_date','weighted_value']]
            dftempens = dftempens.groupby(by=['value_date']).sum()
            dftempens.rename(columns={'weighted_value':'ecmwf-ens'}, inplace=True)
            
            dftempverps = dftempcountry.loc['ecmwf-vareps',['value_date','weighted_value']]
            dftempverps = dftempverps.groupby(by='value_date').sum()
            dftempverps.rename(columns={'weighted_value':'ecmwf-vareps'}, inplace=True)
            
            dftempmmsf = dftempcountry.loc['ecmwf-mmsf',['value_date','weighted_value']]
            dftempmmsf = dftempmmsf.groupby(by='value_date').sum()
            dftempmmsf.rename(columns={'weighted_value':'ecmwf-mmsf'}, inplace=True)
        
            dffull=pd.concat([dftempens,dftempverps,dftempmmsf], axis=1)
            #print(dffull)
            dffullcountry[[country + ' ens', country + ' EC46', country + ' Seasonal']] = dffull
        dffullcountry.index = pd.to_datetime(dffullcountry.index)
        
        #get temp data last
        templast=weather.get_temp_data_last()
        dftemplast = templast[['country','wmo_weighting','model_source','value_date','average_value']].copy()
        dftemplast['weighted_value'] = dftemplast['wmo_weighting']*dftemplast['average_value'] #weighted
        dftemplast.set_index('country', inplace=True)
        #print(dftemplast)
        
        dffullcountrylast = pd.DataFrame()
        for country in ['China','Japan','South Korea']:
            
            dftempcountrylast = dftemplast.loc[country].copy()
            dftempcountrylast = dftempcountrylast[['model_source','value_date','weighted_value']]
            dftempcountrylast.set_index('model_source', inplace=True)
            
            dftempenslast = dftempcountrylast.loc['ecmwf-ens',['value_date','weighted_value']]
            dftempenslast = dftempenslast.groupby(by=['value_date']).sum()
            dftempenslast.rename(columns={'weighted_value':'ecmwf-ens'}, inplace=True)
            
            dftempverpslast = dftempcountrylast.loc['ecmwf-vareps',['value_date','weighted_value']]
            dftempverpslast = dftempverpslast.groupby(by='value_date').sum()
            dftempverpslast.rename(columns={'weighted_value':'ecmwf-vareps'}, inplace=True)
            
            dftempmmsflast = dftempcountrylast.loc['ecmwf-mmsf',['value_date','weighted_value']]
            dftempmmsflast = dftempmmsflast.groupby(by='value_date').sum()
            dftempmmsflast.rename(columns={'weighted_value':'ecmwf-mmsf'}, inplace=True)
        
            dffulllast=pd.concat([dftempenslast,dftempverpslast,dftempmmsflast], axis=1)

            dffullcountrylast[[country + ' ens', country + ' EC46', country + ' Seasonal']] = dffulllast

        return dffullcountry, dffullcountrylast
    
    def demand_model(temphist, temp, dfmodelchina, dfmodeljapan, dfmodelsk, start, end, next_month, ens_end,hist_start):
        
        today = datetime.date.today()
        
        #create fcst temp df
        dfmodel = pd.DataFrame(index=pd.date_range(start=start, end=end, freq='D').date, columns=['China','Japan','South Korea'])
        if hist_start < pd.to_datetime(start):
            #dfmodel.loc[pd.to_datetime(start).date():today,['China','Japan','South Korea']] = temp.loc[pd.to_datetime(start).date():today,['China','Japan','South Korea']].values
            dfmodel.loc[start:ens_end,['China','Japan','South Korea']] = temp.loc[start:ens_end,['China ens','Japan ens','South Korea ens']].values
        else:
            dfmodel.loc[pd.to_datetime(start).date():hist_start,['China','Japan','South Korea']] = temphist.loc[pd.to_datetime(start).date():hist_start,['China','Japan','South Korea']].values
            dfmodel.loc[hist_start:ens_end,['China','Japan','South Korea']] = temp.loc[hist_start:ens_end,['China ens','Japan ens','South Korea ens']].values
        
        dfmodel.loc[today + relativedelta(days=(15)):next_month,['China','Japan','South Korea']] = temp.loc[today + relativedelta(days=(15)):next_month,['China EC46','Japan EC46','South Korea EC46']].values
        dfmodel.loc[next_month:pd.to_datetime(end).date(),['China','Japan','South Korea']] = temp.loc[next_month:pd.to_datetime(end).date(),['China Seasonal','Japan Seasonal','South Korea Seasonal']].values

        for i in dfmodel.index:
            dfmodel.loc[i, 'normal China'] = temphist.loc['2018-'+str(i.month)+'-'+str(i.day): '2018-'+str(i.month)+'-'+str(i.day),'China normal'].values
            dfmodel.loc[i, 'normal Japan'] = temphist.loc['2018-'+str(i.month)+'-'+str(i.day): '2018-'+str(i.month)+'-'+str(i.day),'Japan normal'].values
            dfmodel.loc[i, 'normal South Korea'] = temphist.loc['2018-'+str(i.month)+'-'+str(i.day): '2018-'+str(i.month)+'-'+str(i.day),'South Korea normal'].values
        dfmodel = dfmodel.round(2)
        #print(dfmodel.loc['2022-06-18':'2022-06-25'])
        #scenarios
        scenarios=[-4,-3,-2,-1,1,2,3,4]
        
        #model China
        demand_China = 0.6*dfmodel['China']**2 - 30.9*dfmodel['China'] + 781.6
        demand_ChinaS = 0.6*temphist.loc['2018-01-01':'2018-12-31',['China normal']]**2 - 30.9*temphist.loc['2018-01-01':'2018-12-31',['China normal']] + 781.6 
        
        dfChina = pd.DataFrame(index=pd.date_range(start=start, end=end, freq='D').date, columns=['China','Delta China'])
        dfChina['China'] = demand_China

        for i in dfChina.index:
            dfChina.loc[i, 'Delta China'] = dfChina.loc[i,'China'] - demand_ChinaS.loc['2018-'+str(i.month)+'-'+str(i.day): '2018-'+str(i.month)+'-'+str(i.day),'China normal'].values  
            
        dfChina['Delta China'] = dfChina['Delta China'].str[0].astype(float)
        #scenarios
        for i in scenarios:
            dfChina['China '+str(i)] =  0.6*(dfmodel['normal China']+i)**2 - 30.9*(dfmodel['normal China']+i) + 781.6
        #print(dfChina)
        dfChina['Max China'] = dfmodelchina['Max'] 
        dfChina['Min China'] = dfmodelchina['Min'] 
        dfChina['China normal-std'] = dfmodelchina['normal-std'] 
        dfChina['China normal+std'] = dfmodelchina['normal+std']
        #print(dfChina)
        #model Japan
        demand_Japan = 0.3*dfmodel['Japan']**2 - 11.1*dfmodel['Japan'] + 214.6
        demand_JapanS = 0.3*temphist.loc['2018-01-01':'2018-12-31',['Japan normal']]**2 - 11.1*temphist.loc['2018-01-01':'2018-12-31',['Japan normal']] + 214.6
        dfJapan = pd.DataFrame(index=pd.date_range(start=start, end=end, freq='D').date, columns=['Japan','Delta Japan'])
        dfJapan['Japan'] = demand_Japan
        
        for i in dfJapan.index:
            dfJapan.loc[i, 'Delta Japan'] = dfJapan.loc[i,'Japan'] - demand_JapanS.loc['2018-'+str(i.month)+'-'+str(i.day): '2018-'+str(i.month)+'-'+str(i.day),'Japan normal'].values  
        dfJapan['Delta Japan'] = dfJapan['Delta Japan'].str[0].astype(float)

        #scenarios
        for i in scenarios:
            dfJapan['Japan '+str(i)] =  0.3*(dfmodel['normal Japan']+i)**2 - 11.1*(dfmodel['normal Japan']+i) + 214.6
        dfJapan['Max Japan'] = dfmodeljapan['Max'] 
        dfJapan['Min Japan'] = dfmodeljapan['Min']
        dfJapan['Japan normal-std'] = dfmodeljapan['normal-std']
        dfJapan['Japan normal+std'] = dfmodeljapan['normal+std']
        #model SK
        demand_SK = 0.1*dfmodel['South Korea']**2 - 6.4*dfmodel['South Korea'] + 181.9
        demand_SKS = 0.1*temphist.loc['2018-01-01':'2018-12-31',['South Korea normal']]**2 - 6.4*temphist.loc['2018-01-01':'2018-12-31',['South Korea normal']] + 181.9
        dfSK = pd.DataFrame(index=pd.date_range(start=start, end=end, freq='D').date, columns=['South Korea','Delta South Korea'])
        dfSK['South Korea'] = demand_SK
        
        for i in dfSK.index:
            dfSK.loc[i, 'Delta South Korea'] = dfSK.loc[i,'South Korea'] - demand_SKS.loc['2018-'+str(i.month)+'-'+str(i.day): '2018-'+str(i.month)+'-'+str(i.day),'South Korea normal'].values  
        dfSK['Delta South Korea'] = dfSK['Delta South Korea'].str[0].astype(float)   
        
        #scenarios
        for i in scenarios:
            dfSK['South Korea '+str(i)] =  0.1*(dfmodel['normal South Korea']+1)**2 - 6.4*(dfmodel['normal South Korea']+i) + 181.9
        dfSK['Max South Korea'] = dfmodelsk['Max']
        dfSK['Min South Korea'] = dfmodelsk['Min']
        dfSK['South Korea normal-std'] = dfmodelsk['normal-std']
        dfSK['South Korea normal+std'] = dfmodelsk['normal+std']
        
        demand = pd.concat([dfChina, dfJapan, dfSK], axis=1)
        demand = demand.round(2)
        demand.fillna(0,inplace=True)
        demandnormal = pd.concat([demand_ChinaS, demand_JapanS, demand_SKS], axis=1)
        demandnormal = demandnormal.round(2)
        #print(demand)
        
        return dfmodel, demand, demandnormal

    
    def cum_demand(temphist, temp, start, end, next_month, ens_end,hist_start):
        
        
        #find win start and end
        today = datetime.date.today()
        
        #create fcst temp df
        dfmodel = pd.DataFrame(index=pd.date_range(start=start, end=end, freq='D').date, columns=['China','Japan','South Korea'])
        dfmodel.index = pd.to_datetime(dfmodel.index)
        #print(dfmodel.index)
        if hist_start < pd.to_datetime(start):
            #dfmodel.loc[pd.to_datetime(start).date():today,['China','Japan','South Korea']] = temp.loc[pd.to_datetime(start).date():today,['China','Japan','South Korea']].values
            dfmodel.loc[start:ens_end,['China','Japan','South Korea']] = temp.loc[start:ens_end,['China ens','Japan ens','South Korea ens']].values
        else:
            dfmodel.loc[pd.to_datetime(start).date():hist_start,['China','Japan','South Korea']] = temphist.loc[pd.to_datetime(start).date():hist_start,['China','Japan','South Korea']].values
            dfmodel.loc[hist_start:ens_end,['China','Japan','South Korea']] = temp.loc[hist_start:ens_end,['China ens','Japan ens','South Korea ens']].values
        
        dfmodel.loc[today + relativedelta(days=(15)):next_month,['China','Japan','South Korea']] = temp.loc[today + relativedelta(days=(15)):next_month,['China EC46','Japan EC46','South Korea EC46']].values

        dfmodel.loc[next_month:pd.to_datetime(end).date(),['China','Japan','South Korea']] = temp.loc[next_month:pd.to_datetime(end).date(),['China Seasonal','Japan Seasonal','South Korea Seasonal']].values

        for i in dfmodel.index:
            dfmodel.loc[i, 'normal China'] = temphist.loc['2018-'+str(i.month)+'-'+str(i.day): '2018-'+str(i.month)+'-'+str(i.day),'China normal'].values
            dfmodel.loc[i, 'normal Japan'] = temphist.loc['2018-'+str(i.month)+'-'+str(i.day): '2018-'+str(i.month)+'-'+str(i.day),'Japan normal'].values
            dfmodel.loc[i, 'normal South Korea'] = temphist.loc['2018-'+str(i.month)+'-'+str(i.day): '2018-'+str(i.month)+'-'+str(i.day),'South Korea normal'].values
        dfmodel = dfmodel.round(2)
        #print(dfmodel)
        
        temphist = temphist[~((temphist.index.month == 2) & (temphist.index.day == 29))] #delete all 02/29 leap year
        #create winter df
        dfchina = pd.DataFrame(index=pd.date_range(start=start, end=end))
        if pd.to_datetime(start).month !=  4: 
            for i in range(1980,2021):
                dfchina['winsum' +str(i)] = temphist.loc[str(i) + '-'+str(pd.to_datetime(start).month)+'-'+str(pd.to_datetime(start).day):str(i+1)+ '-'+str(pd.to_datetime(end).month)+'-'+str(pd.to_datetime(end).day),'China'].values
            
        else:
            for i in range(1980,2021):
                dfchina['winsum' +str(i)] = temphist.loc[str(i) + '-'+str(pd.to_datetime(start).month)+'-'+str(pd.to_datetime(start).day):str(i)+ '-'+str(pd.to_datetime(end).month)+'-'+str(pd.to_datetime(end).day),'China'].values
            
        dfchina['China max'] = dfchina.max(axis=1)
        dfchina['China min'] = dfchina.min(axis=1)
        dfchina['winsum'] = dfmodel['China']
        dfchina['normal'] = dfmodel['normal China']
        dfchina['winsum-std'] = np.nan
        dfchina['winsum+std'] =np.nan 
        for i in dfchina.index:
            dfchina.loc[i,'normal-std'] = dfchina.loc[i,'normal']-(dfchina.loc[i,'winsum1980':'winsum2020'].std())
            dfchina.loc[i,'normal+std'] = dfchina.loc[i,'normal']+(dfchina.loc[i,'winsum1980':'winsum2020'].std())
        
        #model china
        dfmodelchina = pd.DataFrame(index=pd.date_range(start=start, end=end))
        dfmodelchina['Max'] = 0.6*dfchina['China max']**2 - 30.9*dfchina['China max'] + 781.6
        dfmodelchina['Min'] = 0.6*dfchina['China min']**2 - 30.9*dfchina['China min'] + 781.6
        dfmodelchina['normal-std'] = 0.6*dfchina['normal-std']**2 - 30.9*dfchina['normal-std'] + 781.6
        dfmodelchina['normal+std'] = 0.6*dfchina['normal+std']**2 - 30.9*dfchina['normal+std'] + 781.6
        dfmodelchina['winsum'] = 0.6*dfchina['winsum']**2 - 30.9*dfchina['winsum'] + 781.6
        dfmodelchina['normal'] = 0.6*dfchina['normal']**2 - 30.9*dfchina['normal'] + 781.6
        #cumsum china
        dfcumchina = pd.DataFrame(index=pd.date_range(start=start, end=end))
        for i in dfmodelchina.columns:
            dfcumchina[i+' cum'] =  dfmodelchina[i].cumsum()/1000 #bcm

        #Japan
        dfjapan = pd.DataFrame(index=pd.date_range(start=start, end=end))
        if pd.to_datetime(start).month !=  4: 
            for i in range(1980,2021):
                dfjapan['winsum' +str(i)] = temphist.loc[str(i) + '-'+str(pd.to_datetime(start).month)+'-'+str(pd.to_datetime(start).day):str(i+1)+ '-'+str(pd.to_datetime(end).month)+'-'+str(pd.to_datetime(end).day),'Japan'].values
            
        else:
            for i in range(1980,2021):
                dfjapan['winsum' +str(i)] = temphist.loc[str(i) + '-'+str(pd.to_datetime(start).month)+'-'+str(pd.to_datetime(start).day):str(i)+ '-'+str(pd.to_datetime(end).month)+'-'+str(pd.to_datetime(end).day),'Japan'].values
            
        dfjapan['Japan max'] = dfjapan.max(axis=1)
        dfjapan['Japan min'] = dfjapan.min(axis=1)
        dfjapan['winsum'] = dfmodel['Japan']
        dfjapan['normal'] = dfmodel['normal Japan']
        dfjapan['normal-std'] = np.nan
        dfjapan['normal+std'] =np.nan 
        for i in dfjapan.index:
            dfjapan.loc[i,'normal-std'] = dfjapan.loc[i,'normal']-(dfjapan.loc[i,'winsum1980':'winsum2020'].std())
            dfjapan.loc[i,'normal+std'] = dfjapan.loc[i,'normal']+(dfjapan.loc[i,'winsum1980':'winsum2020'].std())
        #model japan
        dfmodeljapan = pd.DataFrame(index=pd.date_range(start=start, end=end))
        dfmodeljapan['Max'] = 0.3*dfjapan['Japan max']**2 - 11.1*dfjapan['Japan max'] + 214.6
        dfmodeljapan['Min'] = 0.3*dfjapan['Japan min']**2 - 11.1*dfjapan['Japan min'] + 214.6
        dfmodeljapan['normal-std'] = 0.3*dfjapan['normal-std']**2 - 11.1*dfjapan['normal-std'] + 214.6
        dfmodeljapan['normal+std'] = 0.3*dfjapan['normal+std']**2 - 11.1*dfjapan['normal+std'] + 214.6
        dfmodeljapan['winsum'] = 0.3*dfjapan['winsum']**2 - 11.1*dfjapan['winsum'] + 214.6
        dfmodeljapan['normal'] = 0.3*dfjapan['normal']**2 - 11.1*dfjapan['normal'] + 214.6
        #cumsum japan
        dfcumjapan = pd.DataFrame(index=pd.date_range(start=start, end=end))
        for i in dfmodeljapan.columns:
            dfcumjapan[i+' cum'] =  dfmodeljapan[i].cumsum()/1000 #bcm
        #print(dfcumjapan)
        #SK  
        dfsk = pd.DataFrame(index=pd.date_range(start=start, end=end))
        if pd.to_datetime(start).month !=  4: 
            for i in range(1980,2021):
                dfsk['winsum' +str(i)] = temphist.loc[str(i) + '-'+str(pd.to_datetime(start).month)+'-'+str(pd.to_datetime(start).day):str(i+1)+ '-'+str(pd.to_datetime(end).month)+'-'+str(pd.to_datetime(end).day),'South Korea'].values
            
        else:
            for i in range(1980,2021):
                dfsk['winsum' +str(i)] = temphist.loc[str(i) + '-'+str(pd.to_datetime(start).month)+'-'+str(pd.to_datetime(start).day):str(i)+ '-'+str(pd.to_datetime(end).month)+'-'+str(pd.to_datetime(end).day),'South Korea'].values
            
        dfsk['South Korea max'] = dfsk.max(axis=1)
        dfsk['South Korea min'] = dfsk.min(axis=1)
        dfsk['winsum'] = dfmodel['South Korea']
        dfsk['normal'] = dfmodel['normal South Korea']
        dfsk['normal-std'] = np.nan
        dfsk['normal+std'] =np.nan 
        for i in dfsk.index:
            dfsk.loc[i,'normal-std'] = dfsk.loc[i,'normal']-(dfsk.loc[i,'winsum1980':'winsum2020'].std())
            dfsk.loc[i,'normal+std'] = dfsk.loc[i,'normal']+(dfsk.loc[i,'winsum1980':'winsum2020'].std())
        #model South Korea
        dfmodelsk = pd.DataFrame(index=pd.date_range(start=start, end=end))
        dfmodelsk['Max'] = 0.1*dfsk['South Korea max']**2 - 6.4*dfsk['South Korea max'] + 181.9
        dfmodelsk['Min'] = 0.1*dfsk['South Korea min']**2 - 6.4*dfsk['South Korea min'] + 181.9
        dfmodelsk['normal-std'] = 0.1*dfsk['normal-std']**2 - 6.4*dfsk['normal-std'] + 181.9
        dfmodelsk['normal+std'] = 0.1*dfsk['normal+std']**2 - 6.4*dfsk['normal+std'] + 181.9
        dfmodelsk['winsum'] = 0.1*dfsk['winsum']**2 - 6.4*dfsk['winsum'] + 181.9
        dfmodelsk['normal'] = 0.1*dfsk['normal']**2 - 6.4*dfsk['normal'] + 181.9
        #cumsum South Korea
        dfcumsk = pd.DataFrame(index=pd.date_range(start=start, end=end))
        for i in dfmodelsk.columns:
            dfcumsk[i+' cum'] =  dfmodelsk[i].cumsum()/1000# bcm
        #print(dfcumsk)

        return dfcumchina, dfcumjapan, dfcumsk, dfmodelchina, dfmodeljapan, dfmodelsk
        
    
    def demand_model_last(temphist, templast, start, end, next_month, ens_end,hist_start):
        
        today = datetime.date.today()
        
        #create fcst temp df
        dfmodel = pd.DataFrame(index=pd.date_range(start=start, end=end, freq='D').date, columns=['China','Japan','South Korea'])
        for i in dfmodel.index:
           dfmodel.loc[i, 'normal China'] = temphist.loc['2018-'+str(i.month)+'-'+str(i.day): '2018-'+str(i.month)+'-'+str(i.day),'China normal'].values
           dfmodel.loc[i, 'normal Japan'] = temphist.loc['2018-'+str(i.month)+'-'+str(i.day): '2018-'+str(i.month)+'-'+str(i.day),'Japan normal'].values
           dfmodel.loc[i, 'normal South Korea'] = temphist.loc['2018-'+str(i.month)+'-'+str(i.day): '2018-'+str(i.month)+'-'+str(i.day),'South Korea normal'].values
            
        if hist_start < pd.to_datetime(start):
            #dfmodel.loc[pd.to_datetime(start).date():today,['China','Japan','South Korea']] = temp.loc[pd.to_datetime(start).date():today,['China','Japan','South Korea']].values
            dfmodel.loc[start:ens_end,['China','Japan','South Korea']] = templast.loc[start:ens_end,['China ens','Japan ens','South Korea ens']].values
        else:
            dfmodel.loc[pd.to_datetime(start).date():hist_start,['China','Japan','South Korea']] = temphist.loc[pd.to_datetime(start).date():hist_start,['China','Japan','South Korea']].values
            dfmodel.loc[hist_start:ens_end,['China','Japan','South Korea']] = templast.loc[hist_start:ens_end,['China ens','Japan ens','South Korea ens']].values
        
        dfmodel.loc[today + relativedelta(days=(15)):next_month,['China','Japan','South Korea']] = templast.loc[today + relativedelta(days=(15)):next_month,['China EC46','Japan EC46','South Korea EC46']].values
        if pd.to_datetime(end) > templast.index[-1]:
            dfmodel.loc[next_month:templast.index[-1],['China','Japan','South Korea']] = templast.loc[next_month:templast.index[-1],['China Seasonal','Japan Seasonal','South Korea Seasonal']]
            dfmodel.loc[templast.index[-1]:pd.to_datetime(end).date(),['China','Japan','South Korea']] = dfmodel.loc[templast.index[-1]:pd.to_datetime(end).date(),['normal China','normal Japan','normal South Korea']]
        else:
            dfmodel.loc[next_month:pd.to_datetime(end).date(),['China','Japan','South Korea']] = templast.loc[next_month:pd.to_datetime(end).date(),['China Seasonal','Japan Seasonal','South Korea Seasonal']]

       
        dfmodel = dfmodel.round(2)
        #model China
        demand_China = 0.6*dfmodel['China']**2 - 30.9*dfmodel['China'] + 781.6
        dfChina = pd.DataFrame(index=pd.date_range(start=start, end=end, freq='D').date, columns=['China','Delta China'])
        dfChina['China'] = demand_China

        
    
        #model Japan
        demand_Japan = 0.3*dfmodel['Japan']**2 - 11.1*dfmodel['Japan'] + 214.6
        dfJapan = pd.DataFrame(index=pd.date_range(start=start, end=end, freq='D').date, columns=['Japan','Delta Japan'])
        dfJapan['Japan'] = demand_Japan
        
        
        #model SK
        demand_SK = 0.1*dfmodel['South Korea']**2 - 6.4*dfmodel['South Korea'] + 181.9
        dfSK = pd.DataFrame(index=pd.date_range(start=start, end=end, freq='D').date, columns=['South Korea','Delta South Korea'])
        dfSK['South Korea'] = demand_SK
        
        
        demand = pd.concat([dfChina, dfJapan, dfSK], axis=1)
        demand = demand.round(2)
        demand.fillna(0,inplace=True)
        #print(demand)
        
        return dfmodel, demand
        
    
    def demand_model_table(temphist, temp, dfmodelchina, dfmodeljapan, dfmodelsk, start, end, next_month, ens_end,hist_start):
        
        #print(dfmodel)
        today = datetime.date.today()
        
        
        #create fcst temp df
        dfmodel = pd.DataFrame(index=pd.date_range(start=start, end=end, freq='D').date, columns=['China','Japan','South Korea'])
        if hist_start < pd.to_datetime(start):
            #dfmodel.loc[pd.to_datetime(start).date():today,['China','Japan','South Korea']] = temp.loc[pd.to_datetime(start).date():today,['China','Japan','South Korea']].values
            dfmodel.loc[start:ens_end,['China','Japan','South Korea']] = temp.loc[start:ens_end,['China ens','Japan ens','South Korea ens']].values
        else:
            dfmodel.loc[pd.to_datetime(start).date():hist_start,['China','Japan','South Korea']] = temphist.loc[pd.to_datetime(start).date():hist_start,['China','Japan','South Korea']].values
            dfmodel.loc[hist_start:ens_end,['China','Japan','South Korea']] = temp.loc[hist_start:ens_end,['China ens','Japan ens','South Korea ens']].values
        
        dfmodel.loc[today + relativedelta(days=(15)):next_month,['China','Japan','South Korea']] = temp.loc[today + relativedelta(days=(15)):next_month,['China EC46','Japan EC46','South Korea EC46']].values
        dfmodel.loc[next_month:pd.to_datetime(end).date(),['China','Japan','South Korea']] = temp.loc[next_month:pd.to_datetime(end).date(),['China Seasonal','Japan Seasonal','South Korea Seasonal']].values
        #print(dfmodel)
        for i in dfmodel.index:
            dfmodel.loc[i, 'normal China'] = temphist.loc['2018-'+str(i.month)+'-'+str(i.day): '2018-'+str(i.month)+'-'+str(i.day),'China normal'].values
            dfmodel.loc[i, 'normal Japan'] = temphist.loc['2018-'+str(i.month)+'-'+str(i.day): '2018-'+str(i.month)+'-'+str(i.day),'Japan normal'].values
            dfmodel.loc[i, 'normal South Korea'] = temphist.loc['2018-'+str(i.month)+'-'+str(i.day): '2018-'+str(i.month)+'-'+str(i.day),'South Korea normal'].values
        dfmodel = dfmodel.round(2)
        #scenarios
        scenarios=[-4,-3,-2,-1,1,2,3,4]
        
        #model China
        demand_China = 0.6*dfmodel['China']**2 - 30.9*dfmodel['China'] + 781.6
        demand_ChinaS = 0.6*temphist.loc['2018-01-01':'2018-12-31',['China normal']]**2 - 30.9*temphist.loc['2018-01-01':'2018-12-31',['China normal']] + 781.6 
        dfChina = pd.DataFrame(index=pd.date_range(start=start, end=end, freq='D').date, columns=['China','Delta China'])
        dfChina['China'] = demand_China

        for i in dfChina.index:
            dfChina.loc[i, 'Delta China'] = dfChina.loc[i,'China'] - demand_ChinaS.loc['2018-'+str(i.month)+'-'+str(i.day): '2018-'+str(i.month)+'-'+str(i.day),'China normal'].values  
            
        dfChina['Delta China'] = dfChina['Delta China'].str[0].astype(float)
        #scenarios
        for i in scenarios:
            dfChina['China '+str(i)] =  0.6*(dfmodel['China']+i)**2 - 30.9*(dfmodel['China']+i) + 781.6
        dfChina['Max China'] = dfmodelchina['Max'] 
        dfChina['Min China'] = dfmodelchina['Min'] 
        #print(dfChina)
        #model Japan
        demand_Japan = 0.3*dfmodel['Japan']**2 - 11.1*dfmodel['Japan'] + 214.6
        demand_JapanS = 0.3*temphist.loc['2018-01-01':'2018-12-31',['Japan normal']]**2 - 11.1*temphist.loc['2018-01-01':'2018-12-31',['Japan normal']] + 214.6
        dfJapan = pd.DataFrame(index=pd.date_range(start=start, end=end, freq='D').date, columns=['Japan','Delta Japan'])
        dfJapan['Japan'] = demand_Japan
        
        for i in dfJapan.index:
            dfJapan.loc[i, 'Delta Japan'] = dfJapan.loc[i,'Japan'] - demand_JapanS.loc['2018-'+str(i.month)+'-'+str(i.day): '2018-'+str(i.month)+'-'+str(i.day),'Japan normal'].values  
        dfJapan['Delta Japan'] = dfJapan['Delta Japan'].str[0].astype(float)

        #scenarios
        for i in scenarios:
            dfJapan['Japan '+str(i)] =  0.3*(dfmodel['Japan']+i)**2 - 11.1*(dfmodel['Japan']+i) + 214.6
        dfJapan['Max Japan'] = dfmodeljapan['Max'] 
        dfJapan['Min Japan'] = dfmodeljapan['Min']
        #model SK
        demand_SK = 0.1*dfmodel['South Korea']**2 - 6.4*dfmodel['South Korea'] + 181.9
        demand_SKS = 0.1*temphist.loc['2018-01-01':'2018-12-31',['South Korea normal']]**2 - 6.4*temphist.loc['2018-01-01':'2018-12-31',['South Korea normal']] + 181.9
        dfSK = pd.DataFrame(index=pd.date_range(start=start, end=end, freq='D').date, columns=['South Korea','Delta South Korea'])
        dfSK['South Korea'] = demand_SK
        
        for i in dfSK.index:
            dfSK.loc[i, 'Delta South Korea'] = dfSK.loc[i,'South Korea'] - demand_SKS.loc['2018-'+str(i.month)+'-'+str(i.day): '2018-'+str(i.month)+'-'+str(i.day),'South Korea normal'].values  
        dfSK['Delta South Korea'] = dfSK['Delta South Korea'].str[0].astype(float)   
        
        #scenarios
        for i in scenarios:
            dfSK['South Korea '+str(i)] =  0.1*(dfmodel['South Korea']+1)**2 - 6.4*(dfmodel['South Korea']+i) + 181.9
        dfSK['Max South Korea'] = dfmodelsk['Max']
        dfSK['Min South Korea'] = dfmodelsk['Min']
        
        demand = pd.concat([dfChina, dfJapan, dfSK], axis=1)
        demand = demand.round(2)
        demand.fillna(0,inplace=True)
        demandnormal = pd.concat([demand_ChinaS, demand_JapanS, demand_SKS], axis=1)
        demandnormal = demandnormal.round(2)
        #print(demand)
        
        return dfmodel, demand, demandnormal

        
    def demand_model_last_table(temphist, templast, start, end, next_month, ens_end,hist_start):
        
        today = datetime.date.today()
        
        '''
        start = str(today.year-1)+'-10-01'
        end =str(today.year)+'-'+str((today+relativedelta(months=3)).month)+'-01'
        next_month = today + relativedelta(months=(1))
        '''
        #sum
        
        #create fcst temp df
        dfmodel = pd.DataFrame(index=pd.date_range(start=start, end=end, freq='D').date, columns=['China','Japan','South Korea'])
        for i in dfmodel.index:
           dfmodel.loc[i, 'normal China'] = temphist.loc['2018-'+str(i.month)+'-'+str(i.day): '2018-'+str(i.month)+'-'+str(i.day),'China normal'].values
           dfmodel.loc[i, 'normal Japan'] = temphist.loc['2018-'+str(i.month)+'-'+str(i.day): '2018-'+str(i.month)+'-'+str(i.day),'Japan normal'].values
           dfmodel.loc[i, 'normal South Korea'] = temphist.loc['2018-'+str(i.month)+'-'+str(i.day): '2018-'+str(i.month)+'-'+str(i.day),'South Korea normal'].values
            
            
        if hist_start < pd.to_datetime(start):
            #dfmodel.loc[pd.to_datetime(start).date():today,['China','Japan','South Korea']] = temp.loc[pd.to_datetime(start).date():today,['China','Japan','South Korea']].values
            dfmodel.loc[start:ens_end,['China','Japan','South Korea']] = templast.loc[start:ens_end,['China ens','Japan ens','South Korea ens']].values
        else:
            dfmodel.loc[pd.to_datetime(start).date():hist_start,['China','Japan','South Korea']] = temphist.loc[pd.to_datetime(start).date():hist_start,['China','Japan','South Korea']].values
            dfmodel.loc[hist_start:ens_end,['China','Japan','South Korea']] = templast.loc[hist_start:ens_end,['China ens','Japan ens','South Korea ens']].values
        
        dfmodel.loc[today + relativedelta(days=(15)):next_month,['China','Japan','South Korea']] = templast.loc[today + relativedelta(days=(15)):next_month,['China EC46','Japan EC46','South Korea EC46']].values
        if pd.to_datetime(end) > templast.index[-1]:
            dfmodel.loc[next_month:templast.index[-1],['China','Japan','South Korea']] = templast.loc[next_month:templast.index[-1],['China Seasonal','Japan Seasonal','South Korea Seasonal']]
            dfmodel.loc[templast.index[-1]:pd.to_datetime(end).date(),['China','Japan','South Korea']] = dfmodel.loc[templast.index[-1]:pd.to_datetime(end).date(),['normal China','normal Japan','normal South Korea']]
        else:
            dfmodel.loc[next_month:pd.to_datetime(end).date(),['China','Japan','South Korea']] = templast.loc[next_month:pd.to_datetime(end).date(),['China Seasonal','Japan Seasonal','South Korea Seasonal']]

       
       
        dfmodel = dfmodel.round(2)
        #model China
        demand_China = 0.6*dfmodel['China']**2 - 30.9*dfmodel['China'] + 781.6
        dfChina = pd.DataFrame(index=pd.date_range(start=start, end=end, freq='D').date, columns=['China','Delta China'])
        dfChina['China'] = demand_China

        #model Japan
        demand_Japan = 0.3*dfmodel['Japan']**2 - 11.1*dfmodel['Japan'] + 214.6
        dfJapan = pd.DataFrame(index=pd.date_range(start=start, end=end, freq='D').date, columns=['Japan','Delta Japan'])
        dfJapan['Japan'] = demand_Japan
        
        #model SK
        demand_SK = 0.1*dfmodel['South Korea']**2 - 6.4*dfmodel['South Korea'] + 181.9
        dfSK = pd.DataFrame(index=pd.date_range(start=start, end=end, freq='D').date, columns=['South Korea','Delta South Korea'])
        dfSK['South Korea'] = demand_SK
        
        demand = pd.concat([dfChina, dfJapan, dfSK], axis=1)
        demand = demand.round(2)
        demand.fillna(0,inplace=True)
        #print(demand)
        
        return dfmodel, demand
        
    
    
    def plot_chart(dfmodel, demand, demandnormal, start, end, next_month, ens_end,hist_start):
        
        today = datetime.date.today()

        
        country_list = ['China','Japan','South Korea']
        for i in country_list:
            colors = [] 
            '''
            for j in dfmodel.index:
                if dfmodel.loc[j,i] - dfmodel.loc[j,'normal '+i] > 0:
                    colors.append('red')
                if dfmodel.loc[j,i] - dfmodel.loc[j,'normal '+i] < 0:
                    colors.append('green')
                else:
                    colors.append('white')
            '''
            for j in dfmodel.index:
                if demand.loc[j,'Delta '+i] > 0:
                    colors.append('green')
                elif demand.loc[j,'Delta '+i] < 0:
                    colors.append('red')
                else:
                    colors.append('white')
            #print(dfmodel.index)    
            #print(dfmodel[i])
            
            fig = make_subplots(specs=[[{"secondary_y": True}]])
            fig.add_trace(go.Bar(x=demand.index, y=demand['Delta '+i],
                                name='Gas Demand Delta',
                                marker_color=colors,
                                ),secondary_y=False)
            fig.add_trace(go.Scatter(x=dfmodel.index, y=dfmodel[i],
                                mode='lines',
                                name='temp',
                                line=dict(color='grey', dash='solid'),
                                ),secondary_y=True)
            fig.add_trace(go.Scatter(x=dfmodel.index, y=dfmodel['normal '+i],
                                mode='lines',
                                name='normal temp',
                                line=dict(color='#A5A5A5', dash='dot'),
                                ),secondary_y=True)
            fig.add_trace(go.Scatter(x=[datetime.datetime.today().date(),datetime.datetime.today().date()],y=[dfmodel[i].min(),dfmodel[i].max()],
                   mode="lines",
                   line=dict(color='red', width=2, dash='dot'),
                   showlegend=False,
                   name='Today'
                   ),secondary_y=True)
            
            
            fig.update_layout(
                 autosize=True,
                 showlegend=True,
                 #colorscale='RdBu',
                 legend=dict(x=0, y=-0.2),
                 legend_orientation="h",
                 title_text=i+ ' Gas Demand Delta vs. Monthly Forecast Temp, Mcm/d '+str(today),
                 #xaxis = dict(dtick="M2"),
                 #xaxis_tickformat = '%B',
                 hovermode='x unified',
                 plot_bgcolor = 'white',
                 template='ggplot2' ,
                 annotations=[dict(x=datetime.datetime.today().date(), y=1, yref='paper',font=dict(color="red",size=14),showarrow=False, xanchor='left', text='Today')],

             )
            fig.update_layout(dict(yaxis2={'anchor': 'x', 'overlaying': 'y', 'side': 'right'},
                      yaxis={'anchor': 'x', 'domain': [0.0, 1.0], 'side':'left'}))
            fig.update_yaxes(title_text="Temp Degreees Celcius", secondary_y=True)
            fig.update_yaxes(title_text="Gas Demand Mcm/d", secondary_y=False)
            #box for hist
            fig.add_vrect(x0=pd.to_datetime(start).date(), x1=today, row="all", col=1,
              annotation_text="History", annotation_position="top left", #annotation=dict(font_size=10),
              fillcolor="blue", opacity=0.05, line_width=0.1)
            
            fig.add_vrect(x0=today, x1=(today + relativedelta(days=(15))), row="all", col=1,
              annotation_text="Ens", annotation_position="top left",#annotation=dict(font_size=10),
              fillcolor="blue", opacity=0.1, line_width=0.1)
            
            fig.add_vrect(x0=(today + relativedelta(days=(15))), x1=next_month, row="all", col=1,
              annotation_text="EC46", annotation_position="top left",#annotation=dict(font_size=10),
              fillcolor="blue", opacity=0.15, line_width=0.1)
            
            fig.add_vrect(x0=next_month, x1=pd.to_datetime(end).date(), row="all", col=1,
              annotation_text='Seasonal', annotation_position="top left",#annotation=dict(font_size=10),
              fillcolor="blue", opacity=0.2, line_width=0.1)
            
            py.plot(fig, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/JKCdemand/'+i+' .html', auto_open=False)
            
            #plot scenarios
            #print(demandnormal)
            dfnormalchart = pd.DataFrame(index=pd.date_range(start = demand.index[0],end = demand.index[-1]), columns=demandnormal.columns)
            for index in dfnormalchart.index:
                dfnormalchart.loc[index] = demandnormal.loc['2018-'+str(index.month)+'-'+str(index.day)].values
            #print(dfnormalchart)

            
            figscenarios = go.Figure()
            figscenarios.add_trace(go.Scatter(x=demand.index, y=demand[i],
                                mode='lines',
                                name='FCST',
                                line=dict(color='red', dash='solid'),
                                ))
            figscenarios.add_trace(go.Scatter(x=demand.index, y=dfnormalchart[i+' normal'],
                                mode='lines',
                                name='normal',
                                line=dict(color='black', dash='dot'),
                                ))
            figscenarios.add_trace(go.Scatter(x=demand.index,y=demand['Max '+i],
                                              mode='lines',
                            #fill='tonexty',
                            #fillcolor='rgba(65,105,225,0)',
                             line=dict(color='grey', dash='dot'),
                            showlegend=True,
                            name='Max 1980-2020'
                            ))
                
            figscenarios.add_trace(go.Scatter(x=demand.index,y=demand['Min '+i],
                                 mode='lines',
                            #fill='tonexty',
                            #fillcolor='rgba(65,105,225,0)',
                             line=dict(color='grey', dash='dot'),
                                showlegend=True,
                                name='Min 1980-2020'
                                ))
            
            figscenarios.add_trace(go.Scatter(x=demand.index,y=demand[i+' normal-std'],
                                 mode='lines',
                            #fill='tonexty',
                            #fillcolor='rgba(65,105,225,0)',
                            line=dict(color='red', dash='dot'),
                                showlegend=True,
                                name='normal - std.div 1980-2020'
                                ))
                
            figscenarios.add_trace(go.Scatter(x=demand.index,y=demand[i+' normal+std'],
                                 mode='lines',
                            #fill='tonexty',
                            #fillcolor='rgba(65,105,225,0)',
                             line=dict(color='red', dash='dot'),
                                showlegend=True,
                                name='normal + std.div 1980-2020'
                                ))
            for j in [-4,-3,-2,-1]:
                figscenarios.add_trace(go.Scatter(x=demand.index, y=demand[i+' '+str(j)],
                                fill='tonexty',
                                fillcolor='rgba(255,192,203,{})'.format(abs(j)/10),
                                line_color='rgba(255,192,203,0)',
                                showlegend=True,
                                name=str(j)+' degree scenario'
                                ))
            for k in [1,2,3,4]:
                figscenarios.add_trace(go.Scatter(x=demand.index, y=demand[i+' '+str(k)],
                                fill='tonexty',
                                fillcolor='rgba(60,179,113,{})'.format(abs(k)/10),
                                line_color='rgba(255,192,203,0)',
                                showlegend=True,
                                name=str(k)+' degree scenario'
                                ))
            figscenarios.add_trace(go.Scatter(x=[datetime.datetime.today().date(),datetime.datetime.today().date()],y=[demand[i+' 4'].min(),demand[i+' -4'].max()],
                   mode="lines",
                   line=dict(color='red', width=2, dash='dot'),
                   showlegend=False,
                   name='Today'
                   ))
            
            figscenarios.update_layout(
                 autosize=True,
                 showlegend=True,
                 #colorscale='RdBu',
                 legend=dict(x=0, y=-0.2),
                 legend_orientation="h",
                 title_text=i+ ' Gas Demand vs. Min/Max Temp Scenarios, Mcm/d '+str(today),
                 #xaxis = dict(dtick="M2"),
                 #xaxis_tickformat = '%B',
                 hovermode='x unified',
                 plot_bgcolor = 'white',
                 template='ggplot2'  ,
                 annotations=[dict(x=datetime.datetime.today().date(), y=1, yref='paper',font=dict(color="red",size=14),showarrow=False, xanchor='left', text='Today')],

             )
            figscenarios.update_yaxes(title_text="Gas Demand Mcm/d")
            #box for hist
            figscenarios.add_vrect(x0=pd.to_datetime(start).date(), x1=today, row="all", col=1,
              annotation_text="History", annotation_position="top left", #annotation=dict(font_size=10),
              fillcolor="blue", opacity=0.05, line_width=0.1)
            
            figscenarios.add_vrect(x0=today, x1=(today + relativedelta(days=(15))), row="all", col=1,
              annotation_text="Ens", annotation_position="top left",#annotation=dict(font_size=10),
              fillcolor="blue", opacity=0.1, line_width=0.1)
            
            figscenarios.add_vrect(x0=(today + relativedelta(days=(15))), x1=next_month, row="all", col=1,
              annotation_text="EC46", annotation_position="top left",#annotation=dict(font_size=10),
              fillcolor="blue", opacity=0.15, line_width=0.1)
            
            figscenarios.add_vrect(x0=next_month, x1=pd.to_datetime(end).date(), row="all", col=1,
              annotation_text='Seasonal', annotation_position="top left",#annotation=dict(font_size=10),
              fillcolor="blue", opacity=0.2, line_width=0.1)
            
            py.plot(figscenarios, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/JKCdemand/'+i+' scenarios.html', auto_open=False)
        
    def plot_chart_cum(dfcumchina, dfcumjapan, dfcumsk):
        
        dfcumchina, dfcumjapan, dfcumsk = dfcumchina.astype(float).round(2), dfcumjapan.astype(float).round(2), dfcumsk.astype(float).round(2)
        
        today = datetime.datetime.today()
        season = 'Sum22'
        #china
        figchina = make_subplots(specs=[[{"secondary_y": True}]])
        figchina.add_trace(go.Bar(x=dfcumchina.index,y=(dfcumchina['winsum cum'] - dfcumchina['normal cum']),
                            name='Model - normal delta',marker_opacity=0.5
                            ),secondary_y=True,)
        figchina.add_trace(go.Scatter(x=dfcumchina.index, y=dfcumchina['winsum cum'],
                            mode='lines',
                            name=season,
                            line=dict(color='red', dash='solid'),
                            ),secondary_y=False,)
        figchina.add_trace(go.Scatter(x=dfcumchina.index, y=dfcumchina['normal cum'],
                            mode='lines',
                            name='normal',
                            line=dict(color='black', dash='dot'),
                            ),secondary_y=False,)
        figchina.add_trace(go.Scatter(x=dfcumchina.index,y=dfcumchina['Max cum'],
                            fill='tonexty',
                            fillcolor='rgba(65,105,225,0)',
                            line_color='rgba(65,105,225,0)',
                            showlegend=False,
                            name='Min-Max 1980-2020'
                            ),secondary_y=False,)
            
        figchina.add_trace(go.Scatter(x=dfcumchina.index,y=dfcumchina['Min cum'],
                            fill='tonexty',
                            fillcolor='rgba(65,105,225,0.1)',
                            line_color='rgba(65,105,225,0)',
                            showlegend=True,
                            name='Min-Max 1980-2020'
                            ),secondary_y=False,)
        
        figchina.add_trace(go.Scatter(x=dfcumchina.index,y=dfcumchina['normal-std cum'],
                            fill='tonexty',
                            fillcolor='rgba(65,105,225,0)',
                            line_color='rgba(65,105,225,0)',
                            showlegend=False,
                            name=season+' ± std.div 1980-2020'
                            ),secondary_y=False,)
            
        figchina.add_trace(go.Scatter(x=dfcumchina.index,y=dfcumchina['normal+std cum'],
                            fill='tonexty',
                            fillcolor='rgba(65,105,225,0.3)',
                            line_color='rgba(65,105,225,0)',
                            showlegend=True,
                            name=season+' ± std.div 1980-2020'
                            ),secondary_y=False,)
        #print(dfcumchina)
        figchina.add_trace(go.Scatter(x=[datetime.datetime.today().date(),datetime.datetime.today().date()],y=[dfcumchina['Max cum'].min(),dfcumchina['Min cum'].max()],
                   mode="lines",
                   line=dict(color='red', width=2, dash='dot'),
                   showlegend=False,
                   name='Today'
                   ),secondary_y=False,)
 
        figchina.update_layout(
             autosize=True,
             showlegend=True,
             #colorscale='RdBu',
             legend=dict(x=0, y=-0.2),
             legend_orientation="h",
             title_text= 'China Gas Demand cumulative sum Fcst vs. Min/Max Temp Scenarios, Bcm '+'<br>'+season+'  model - normal = '+ str((dfcumchina['winsum cum'].iloc[-1] - dfcumchina['normal cum'].iloc[-1]).round(2)) +' Bcm '+str(today.date()),
             #xaxis = dict(dtick="M2"),
             #xaxis_tickformat = '%B',
             hovermode='x unified',
             plot_bgcolor = 'white',
             template='ggplot2'  ,
             annotations=[dict(x=datetime.datetime.today().date(), y=1, yref='paper',font=dict(color="red",size=14),showarrow=False, xanchor='left', text='Today')],

         )
        
        figchina.update_yaxes(title_text="Gas Demand Bcm", secondary_y=False)
        figchina.update_yaxes(title_text="Delta Gas Demand Bcm", secondary_y=True, range=[(dfcumchina['winsum cum'] - dfcumchina['normal cum']).min(), (dfcumchina['winsum cum'] - dfcumchina['normal cum']).max()*5])
        
        py.plot(figchina, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/JKCdemand/China cum.html', auto_open=False)
        
        #Japan
        figjapan = make_subplots(specs=[[{"secondary_y": True}]])
        figjapan.add_trace(go.Bar(x=dfcumjapan.index,y=(dfcumjapan['winsum cum'] - dfcumjapan['normal cum']),
                            name='Model - normal delta'
                            ),secondary_y=True,)
        figjapan.add_trace(go.Scatter(x=dfcumjapan.index, y=dfcumjapan['winsum cum'],
                            mode='lines',
                            name=season,
                            line=dict(color='red', dash='solid'),
                            ),secondary_y=False,)
        figjapan.add_trace(go.Scatter(x=dfcumjapan.index, y=dfcumjapan['normal cum'],
                            mode='lines',
                            name='normal',
                            line=dict(color='black', dash='dot'),
                            ),secondary_y=False,)
        figjapan.add_trace(go.Scatter(x=dfcumjapan.index,y=dfcumjapan['Max cum'],
                            fill='tonexty',
                            fillcolor='rgba(65,105,225,0)',
                            line_color='rgba(65,105,225,0)',
                            showlegend=False,
                            name='Min-Max 1980-2020'
                            ),secondary_y=False,)
            
        figjapan.add_trace(go.Scatter(x=dfcumjapan.index,y=dfcumjapan['Min cum'],
                            fill='tonexty',
                            fillcolor='rgba(65,105,225,0.1)',
                            line_color='rgba(65,105,225,0)',
                            showlegend=True,
                            name='Min-Max 1980-2020'
                            ),secondary_y=False,)
        
        figjapan.add_trace(go.Scatter(x=dfcumjapan.index,y=dfcumjapan['normal-std cum'],
                            fill='tonexty',
                            fillcolor='rgba(65,105,225,0)',
                            line_color='rgba(65,105,225,0)',
                            showlegend=False,
                            name=season+' ± std.div 1980-2020'
                            ),secondary_y=False,)
            
        figjapan.add_trace(go.Scatter(x=dfcumjapan.index,y=dfcumjapan['normal+std cum'],
                            fill='tonexty',
                            fillcolor='rgba(65,105,225,0.3)',
                            line_color='rgba(65,105,225,0)',
                            showlegend=True,
                            name=season+' ± std.div 1980-2020'
                            ),secondary_y=False,)
        figjapan.add_trace(go.Scatter(x=[datetime.datetime.today().date(),datetime.datetime.today().date()],y=[dfcumjapan['Min cum'].min(),dfcumjapan['Min cum'].max()],
                   mode="lines",
                   line=dict(color='red', width=2, dash='dot'),
                   showlegend=False,
                   name='Today'
                   ))
        
 
        figjapan.update_layout(
             autosize=True,
             showlegend=True,
             #colorscale='RdBu',
             legend=dict(x=0, y=-0.2),
             legend_orientation="h",
             title_text= 'Japan Gas Demand cumulative sum Fcst vs. Min/Max Temp Scenarios, Bcm '+'<br>'+season+'  model - normal = '+ str((dfcumjapan['winsum cum'].iloc[-1] - dfcumjapan['normal cum'].iloc[-1]).round(2)) +' Bcm '+str(today.date()),
             #xaxis = dict(dtick="M2"),
             #xaxis_tickformat = '%B',
             hovermode='x unified',
             plot_bgcolor = 'white',
             template='ggplot2'  ,
             annotations=[dict(x=datetime.datetime.today().date(), y=1, yref='paper',font=dict(color="red",size=14),showarrow=False, xanchor='left', text='Today')],

         )
        
        figjapan.update_yaxes(title_text="Gas Demand Bcm", secondary_y=False)
        figjapan.update_yaxes(title_text="Delta Gas Demand Bcm", secondary_y=True,range=[(dfcumjapan['winsum cum'] - dfcumjapan['normal cum']).min(), (dfcumjapan['winsum cum'] - dfcumjapan['normal cum']).max()*5])
        
        py.plot(figjapan, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/JKCdemand/Japan cum.html', auto_open=False)
        
        #SK
        figsk = make_subplots(specs=[[{"secondary_y": True}]])
        figsk.add_trace(go.Bar(x=dfcumsk.index,y=(dfcumsk['winsum cum'] - dfcumsk['normal cum']),
                            name='Model - normal delta'
                            ),secondary_y=True,)
        figsk.add_trace(go.Scatter(x=dfcumsk.index, y=dfcumsk['winsum cum'],
                            mode='lines',
                            name=season,
                            line=dict(color='red', dash='solid'),
                            ),secondary_y=False,)
        figsk.add_trace(go.Scatter(x=dfcumsk.index, y=dfcumsk['normal cum'],
                            mode='lines',
                            name='normal',
                            line=dict(color='black', dash='dot'),
                            ),secondary_y=False,)
        figsk.add_trace(go.Scatter(x=dfcumsk.index,y=dfcumsk['Max cum'],
                            fill='tonexty',
                            fillcolor='rgba(65,105,225,0)',
                            line_color='rgba(65,105,225,0)',
                            showlegend=False,
                            name='Min-Max 1980-2020'
                            ),secondary_y=False,)
            
        figsk.add_trace(go.Scatter(x=dfcumsk.index,y=dfcumsk['Min cum'],
                            fill='tonexty',
                            fillcolor='rgba(65,105,225,0.1)',
                            line_color='rgba(65,105,225,0)',
                            showlegend=True,
                            name='Min-Max 1980-2020'
                            ),secondary_y=False,)
        
        figsk.add_trace(go.Scatter(x=dfcumsk.index,y=dfcumsk['normal-std cum'],
                            fill='tonexty',
                            fillcolor='rgba(65,105,225,0)',
                            line_color='rgba(65,105,225,0)',
                            showlegend=False,
                            name=season+' ± std.div 1980-2020'
                            ),secondary_y=False,)
            
        figsk.add_trace(go.Scatter(x=dfcumsk.index,y=dfcumsk['normal+std cum'],
                            fill='tonexty',
                            fillcolor='rgba(65,105,225,0.3)',
                            line_color='rgba(65,105,225,0)',
                            showlegend=True,
                            name=season+' ± std.div 1980-2020'
                            ),secondary_y=False,)
        figsk.add_trace(go.Scatter(x=[datetime.datetime.today().date(),datetime.datetime.today().date()],y=[dfcumsk['Min cum'].min(),dfcumsk['Min cum'].max()],
                   mode="lines",
                   line=dict(color='red', width=2, dash='dot'),
                   showlegend=False,
                   name='Today'
                   ))
 
        figsk.update_layout(
             autosize=True,
             showlegend=True,
             #colorscale='RdBu',
             legend=dict(x=0, y=-0.2),
             legend_orientation="h",
             title_text= 'South Korea Gas Demand cumulative sum Fcst vs. Min/Max Temp Scenarios, Bcm '+'<br>'+season+'  model - normal = '+ str((dfcumsk['winsum cum'].iloc[-1] - dfcumsk['normal cum'].iloc[-1]).round(2)) +' Bcm '+str(today.date()),
             #xaxis = dict(dtick="M2"),
             #xaxis_tickformat = '%B',
             hovermode='x unified',
             plot_bgcolor = 'white',
             template='ggplot2'  ,
             annotations=[dict(x=datetime.datetime.today().date(), y=1, yref='paper',font=dict(color="red",size=14),showarrow=False, xanchor='left', text='Today')],

         )
        
        figsk.update_yaxes(title_text="Gas Demand Bcm", secondary_y=False)
        figsk.update_yaxes(title_text="Delta Gas Demand Bcm", secondary_y=True,range=[(dfcumsk['winsum cum'] - dfcumsk['normal cum']).min(), (dfcumsk['winsum cum'] - dfcumsk['normal cum']).max()*5])
        
        py.plot(figsk, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/JKCdemand/South Korea cum.html', auto_open=False)

    def plot_table(dfmodel, demand, demandlast, demandnormal):
        
        today = datetime.date.today()
        
        #create table df for cn, jp and sk
        dftablechina =pd.DataFrame(index = pd.date_range(start=demand.index[0], end=demand.index[-1]))
        dftablechina['China D'] = demand['China']
        for i in dftablechina.index:
            dftablechina.loc[i,'China SN'] = demandnormal.loc['2018-'+str(i.month)+'-'+str(i.day),'China normal']
        dftablechina['China D-1'] = demandlast['China']
        dftablechina['DoD'] = (dftablechina['China D'] - dftablechina['China D-1']) 
        dftablechina['D-SN'] = (dftablechina['China D'] - dftablechina['China SN'])
        dftablechina['D-SN Cumsum'] = (dftablechina['China D'] - dftablechina['China SN'])*7 #correct on sum of the act of week
        dftablechina = dftablechina.resample('W').sum()/7
        
        
        dftablechina['Week'] = 'Week '+ dftablechina.index.strftime('%W') + '/' + dftablechina.index.strftime('%b%d')
        dftablechina.set_index('Week', inplace=True)
        #print(dftablechina)
        dftablechina = dftablechina.loc['Week '+today.strftime('%W')+ '/' + (today+relativedelta(days=6-today.weekday())).strftime('%b%d'):]
        dftablechina = dftablechina.round(1)
        
        dftablejapan =pd.DataFrame(index = pd.date_range(start=demand.index[0], end=demand.index[-1]))
        dftablejapan['Japan D'] = demand['Japan']
        for i in dftablejapan.index:
            dftablejapan.loc[i,'Japan SN'] = demandnormal.loc['2018-'+str(i.month)+'-'+str(i.day),'Japan normal']
        dftablejapan['Japan D-1'] = demandlast['Japan']
        dftablejapan['DoD'] = dftablejapan['Japan D'] - dftablejapan['Japan D-1']
        dftablejapan['D-SN'] = (dftablejapan['Japan D'] - dftablejapan['Japan SN'])
        dftablejapan['D-SN Cumsum'] = (dftablejapan['Japan D'] - dftablejapan['Japan SN'])*7
        dftablejapan = dftablejapan.resample('W').sum()/7
        dftablejapan['Week'] = 'Week '+ dftablejapan.index.strftime('%W')+ '/' + dftablejapan.index.strftime('%b%d')
        dftablejapan.set_index('Week', inplace=True)
        dftablejapan = dftablejapan.loc['Week '+today.strftime('%W')+ '/' + (today+relativedelta(days=6-today.weekday())).strftime('%b%d'):]
        dftablejapan = dftablejapan.round(1)
        
        dftablesk =pd.DataFrame(index = pd.date_range(start=demand.index[0], end=demand.index[-1]))
        dftablesk['South Korea D'] = demand['South Korea']
        for i in dftablesk.index:
            dftablesk.loc[i,'South Korea SN'] = demandnormal.loc['2018-'+str(i.month)+'-'+str(i.day),'South Korea normal']
        dftablesk['South Korea D-1'] = demandlast['South Korea']
        dftablesk['DoD'] = dftablesk['South Korea D'] - dftablesk['South Korea D-1']
        dftablesk['D-SN'] = (dftablesk['South Korea D'] - dftablesk['South Korea SN'])
        dftablesk['D-SN Cumsum'] = (dftablesk['South Korea D'] - dftablesk['South Korea SN'])*7
        dftablesk = dftablesk.resample('W').sum()/7
        dftablesk['Week'] = 'Week '+ dftablesk.index.strftime('%W')+ '/' + dftablesk.index.strftime('%b%d')
        #print(dftablesk)
        dftablesk.set_index('Week', inplace=True)
        dftablesk = dftablesk.loc['Week '+today.strftime('%W')+ '/' + (today+relativedelta(days=6-today.weekday())).strftime('%b%d'):]
        dftablesk = dftablesk.round(1)
        
        #color code
        dfcolor=pd.DataFrame()
        dfcolor.loc[:,'China DoD'] = dftablechina.loc[:,'DoD'].values
        dfcolor.loc[:,'China D-SN'] = dftablechina.loc[:,'D-SN'].values
        dfcolor.loc[:,'China D-SN Cumsum'] = dftablechina.loc[:,'D-SN Cumsum'].values
        dfcolor.loc[:,'Japan DoD'] = dftablejapan.loc[:,'DoD'].values
        dfcolor.loc[:,'Japan D-SN'] = dftablejapan.loc[:,'D-SN'].values
        dfcolor.loc[:,'Japan D-SN Cumsum'] = dftablejapan.loc[:,'D-SN Cumsum'].values
        dfcolor.loc[:,'South Korea DoD'] = dftablesk.loc[:,'DoD'].values
        dfcolor.loc[:,'South Korea D-SN'] = dftablesk.loc[:,'D-SN'].values
        dfcolor.loc[:,'South Korea D-SN Cumsum'] = dftablesk.loc[:,'D-SN Cumsum'].values
        #print(dfcolor)
        for ism in dfcolor.index:
            for jsm in dfcolor.columns:
                if dfcolor.loc[ism,jsm] > 20:
                    dfcolor.loc[ism,jsm] = 'green'
                elif dfcolor.loc[ism,jsm] <-20:
                    dfcolor.loc[ism,jsm] = 'red'
                else:
                    dfcolor.loc[ism,jsm] = 'white'
        #dfcolor.insert(0,'normal',['paleturquoise']*dfcolor.shape[0])
        #dfcolor.insert(0,'Week',['paleturquoise']*dfcolor.shape[0])
        #dfcolor=dfcolor.T
        dfcolorChina = pd.DataFrame('white', index = dfcolor.index, columns=dftablechina.columns)
        dfcolorChina[['DoD','D-SN','D-SN Cumsum']] = dfcolor[['China DoD','China D-SN','China D-SN Cumsum']]
        dfcolorChina.insert(0,'Week',['paleturquoise']*dfcolor.shape[0])
        dfcolorChina=dfcolorChina.T
        
        dfcolorJapan = pd.DataFrame('white', index = dfcolor.index, columns=dftablechina.columns)
        dfcolorJapan[['DoD','D-SN','D-SN Cumsum']] = dfcolor[['Japan DoD','Japan D-SN','Japan D-SN Cumsum']]
        dfcolorJapan.insert(0,'Week',['paleturquoise']*dfcolor.shape[0])
        dfcolorJapan=dfcolorJapan.T
        
        dfcolorSK = pd.DataFrame('white', index = dfcolor.index, columns=dftablechina.columns)
        dfcolorSK[['DoD','D-SN','D-SN Cumsum']] = dfcolor[['South Korea DoD','South Korea D-SN','South Korea D-SN Cumsum']]
        dfcolorSK.insert(0,'Week',['paleturquoise']*dfcolor.shape[0])
        dfcolorSK=dfcolorSK.T
        #print(dfcolorChina)
        #plot table
        fig_china = go.Figure(
            data=[go.Table(
            header=dict(values=['Week']+list(dftablechina.columns),
                        line_color='darkslategray',
                        fill_color='royalblue',
                        align=['center'],
                        font=dict(color='white'),
                        ),
            cells=dict(values=[dftablechina.index] + [dftablechina[pm] for pm in dftablechina.columns],
                       #values=[dfwind_wave.]
                       line_color='darkslategray',
                        fill=dict(color = dfcolorChina.values.tolist()),
                        align=[ 'center'],
                        #font_size=12,
                        #height=30,
                        
                        ))
        ])
        fig_china.update_layout(title_text='China weekly demand table Mcm/d, Green > 20, Red < -20')
        
        py.plot(fig_china, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/JKCdemand/China table.html', auto_open=False)
        
        fig_japan = go.Figure(
            data=[go.Table(
            header=dict(values=['Week']+list(dftablejapan.columns),
                        line_color='darkslategray',
                        fill_color='royalblue',
                        align=['center'],
                        font=dict(color='white'),
                        ),
            cells=dict(values=[dftablejapan.index] + [dftablejapan[pm] for pm in dftablejapan.columns],
                       #values=[dfwind_wave.]
                       line_color='darkslategray',
                        fill=dict(color=dfcolorJapan.values.tolist()),
                        align=[ 'center'],
                        #font_size=12,
                        #height=30,
                        
                        ))
        ])
        fig_japan.update_layout(title_text='Japan weekly demand table Mcm/d, Green > 20, Red < -20')
        
        py.plot(fig_japan, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/JKCdemand/Japan table.html', auto_open=False)
        
        fig_sk = go.Figure(
            data=[go.Table(
            header=dict(values=['Week']+list(dftablesk.columns),
                        line_color='darkslategray',
                        fill_color='royalblue',
                        align=['center'],
                        font=dict(color='white'),
                        ),
            cells=dict(values=[dftablesk.index] + [dftablesk[pm] for pm in dftablesk.columns],
                       #values=[dfwind_wave.]
                       line_color='darkslategray',
                        fill=dict(color=dfcolorSK.values.tolist()),
                        align=[ 'center'],
                        #font_size=12,
                        #height=30,
                        
                        ))
        ])
        fig_sk.update_layout(title_text='South Korea weekly demand table Mcm/d, Green > 20, Red < -20')
        
        py.plot(fig_sk, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/JKCdemand/South Korea table.html', auto_open=False)
        
        
    def update():        
        a=JKC_Demand
        
        temphist=a.temp_hist_data()
        start, end, next_month, ens_end,hist_start = a.date(temphist)
        temp, templast = a.temp_data()
        #latest
        dfcumchina, dfcumjapan, dfcumsk, dfmodelchina, dfmodeljapan, dfmodelsk = a.cum_demand(temphist, temp, start, end, next_month, ens_end,hist_start)
        dfmodel, demand, demandnormal = a.demand_model(temphist, temp, dfmodelchina, dfmodeljapan, dfmodelsk, start, end, next_month, ens_end,hist_start)
        dfmodel_table, demand_table, demandnormal_table = a.demand_model_table(temphist, temp, dfmodelchina, dfmodeljapan, dfmodelsk, start, end, next_month, ens_end,hist_start)
        #last
        dfmodellast, demandlast = a.demand_model_last(temphist, templast, start, end, next_month, ens_end,hist_start)
        dfmodellast_table, demandlast_table = a.demand_model_last_table(temphist, templast, start, end, next_month, ens_end, hist_start)
        #charts
        a.plot_chart(dfmodel, demand, demandnormal, start, end, next_month, ens_end, hist_start)
        a.plot_chart_cum(dfcumchina, dfcumjapan, dfcumsk)
        a.plot_table(dfmodel_table, demand_table, demandlast_table, demandnormal_table)
        
        db_server_lng = "PRD-DB-SQL-211"
        db_name_lng = "LNG"
        sql_engine_lng = sqlalchemy.create_engine("mssql+pyodbc://" + db_server_lng + "/" + db_name_lng + "?trusted_connection=yes&driver=ODBC+Driver+13+for+SQL+Server")
        
        temp.to_sql(name='JKC Temp', con=sql_engine_lng, index=True, if_exists='replace', schema='ana')
        temphist.to_sql(name='JKC Temp Hist', con=sql_engine_lng, index=True, if_exists='replace', schema='ana')
        demand.to_sql(name='JKC Demand', con=sql_engine_lng, index=True, if_exists='replace', schema='ana')

#JKC_Demand.update()