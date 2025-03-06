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
#from datetime import datetime, timedelta
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

        countrylist=['China', 'Japan','South Korea']
        #df=temp.get_data(countrylist)
        #df=df.resample('D').mean()
        #print(df)
        dffull = pd.DataFrame(index=pd.date_range(start='1990-01-01', end=str(today.year)+'-12-31')) 
        
        for i in countrylist:
            try:
                df=temp.get_data(i, '1990-01-01',today)
                #print(df)
                df=df.resample('D').mean()
                dffull.loc['1990-01-01':df.index[-1],i]=df.loc['1990-01-01':,'2m Temp [C]']
                dffull.loc['1990-01-01':df.index[-1],i+' normal'] = df.loc['1990-01-01':,'2m Temp 1991-2020 [C]']
            except (FileNotFoundError, KeyError) as e:
                print(i, e)
        
        #print(dffull.loc['2000-12-22':])
        dffull=dffull.round(1)
        
        return dffull
    
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
        #print(dffullcountry)    
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
            #print(dffulllast)
            dffullcountrylast[[country + ' ens', country + ' EC46', country + ' Seasonal']] = dffulllast
            
        #print(dffullcountrylast)
        return dffullcountry, dffullcountrylast
    
    def demand_model(temphist, temp, scenario):
        
        #print(dfmodel)
        today = datetime.date.today()
        if today.month <10:
            start = '1990-01-01'#str(today.year-1)+'-10-01'
        
            end = str(today.year)+'-03-31'#pd.to_datetime(str(next_month.year)+'-'+str(next_month.month)+'-'+str(calendar.monthrange(next_month.year, next_month.month)[1]))
        else:
            start = str(today.year)+'-10-01'
            end = str(today.year+1)+'-03-31'
        
        next_month = today + relativedelta(months=(1))
        #print(temphist.loc['2000-01-01':])
        #print(temp)
        #print(temp.index[-1])
        #create fcst temp df
        dfmodel = pd.DataFrame(index=pd.date_range(start=start, end=temp.index[-1], freq='D').date, columns=['China','Japan','South Korea'])
        dfmodel.loc[pd.to_datetime(start).date():today,['China','Japan','South Korea']] = temphist.loc[pd.to_datetime(start).date():today,['China','Japan','South Korea']]
        dfmodel.index = pd.to_datetime(dfmodel.index)
        #dfmodel.loc[today- relativedelta(days=(5)):today + relativedelta(days=(15)),['China','Japan','South Korea']] = temp.loc[today- relativedelta(days=(5)):today + relativedelta(days=(15)),['China ens','Japan ens','South Korea ens']].values
        #dfmodel.loc[today + relativedelta(days=(15)):next_month,['China','Japan','South Korea']] = temp.loc[today + relativedelta(days=(15)):next_month,['China EC46','Japan EC46','South Korea EC46']].values
        #dfmodel.loc[next_month:temp.index[-1],['China','Japan','South Korea']] = temp.loc[next_month:temp.index[-1],['China Seasonal','Japan Seasonal','South Korea Seasonal']].values
        #print(temp.index)
        #dfmodel = temp.resample('MS').mean()
        #temphist = temphist.resample('MS').mean()
        #dfmodel = temp
        #for i in dfmodel.index:
            #dfmodel.loc[i, 'normal China'] = temphist.loc['2018-'+str(i.month)+'-'+str(i.day): '2018-'+str(i.month)+'-'+str(i.day),'China normal'].values
            #dfmodel.loc[i, 'normal Japan'] = temphist.loc['2018-'+str(i.month)+'-'+str(i.day): '2018-'+str(i.month)+'-'+str(i.day),'Japan normal'].values
            #dfmodel.loc[i, 'normal South Korea'] = temphist.loc['2018-'+str(i.month)+'-'+str(i.day): '2018-'+str(i.month)+'-'+str(i.day),'South Korea normal'].values
        dfmodel = dfmodel.round(2)
        
        #dfmodel = dfmodel + scenario
        #temphist = temphist + scenario
        #print(dfmodel)
        #print(temphist)
        
        tempall = pd.concat([dfmodel, temphist[['China normal','Japan normal','South Korea normal']]], axis=1)
        print(tempall.info())
        #tempall['delta China'] = tempall['China']-tempall['China normal']
        #tempall['delta Japan'] = tempall['Japan']-tempall['Japan normal'],
        #tempall['delta SK'] = tempall['South Korea']-tempall['South Korea normal']
        tempall=tempall.astype(float)
        tempall = tempall.resample('MS').mean()
        print(tempall)
        tempall.to_csv('C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/JKCdemand/tempall.csv')
        #model China
        demand_China = 0.6*dfmodel['China']**2 - 30.9*dfmodel['China'] + 781.6
        demand_ChinaS = 0.6*temphist['China normal']**2 - 30.9*temphist['China normal'] + 781.6 
        dfChina = pd.DataFrame(index=pd.date_range(start=start, end=end, freq='D').date, columns=['China','Delta China'])
        dfChina['China'] = demand_China
        dfChina['Delta China'] = dfChina['China'] - demand_ChinaS
        #for i in dfChina.index:
        #    dfChina.loc[i, 'Delta China'] = dfChina.loc[i,'China'] - demand_ChinaS.loc['2020-'+str(i.month)+'-'+str(i.day): '2020-'+str(i.month)+'-'+str(i.day),'China normal'].values  
            
        #dfChina['Delta China'] = dfChina['Delta China'].str[0].astype(float)
    
        #model Japan
        demand_Japan = 0.3*dfmodel['Japan']**2 - 11.1*dfmodel['Japan'] + 214.6
        demand_JapanS = 0.3*temphist['Japan normal']**2 - 11.1*temphist['Japan normal'] + 214.6
        dfJapan = pd.DataFrame(index=pd.date_range(start=start, end=end, freq='D').date, columns=['Japan','Delta Japan'])
        dfJapan['Japan'] = demand_Japan
        dfJapan['Delta Japan'] = dfJapan['Japan'] - demand_JapanS
        #for i in dfJapan.index:
         #   dfJapan.loc[i, 'Delta Japan'] = dfJapan.loc[i,'Japan'] - demand_JapanS.loc['2020-'+str(i.month)+'-'+str(i.day): '2020-'+str(i.month)+'-'+str(i.day),'Japan normal'].values  
        #dfJapan['Delta Japan'] = dfJapan['Delta Japan'].str[0].astype(float)
        #model SK
        demand_SK = 0.1*dfmodel['South Korea']**2 - 6.4*dfmodel['South Korea'] + 181.9
        demand_SKS = 0.1*temphist['South Korea normal']**2 - 6.4*temphist['South Korea normal'] + 181.9
        dfSK = pd.DataFrame(index=pd.date_range(start=start, end=end, freq='D').date, columns=['South Korea','Delta South Korea'])
        dfSK['South Korea'] = demand_SK
        dfSK['Delta South Korea'] = dfSK['South Korea'] - demand_SKS
        #for i in dfSK.index:
        #    dfSK.loc[i, 'Delta South Korea'] = dfSK.loc[i,'South Korea'] - demand_SKS.loc['2020-'+str(i.month)+'-'+str(i.day): '2020-'+str(i.month)+'-'+str(i.day),'South Korea normal'].values  
        #dfSK['Delta South Korea'] = dfSK['Delta South Korea'].str[0].astype(float)  
        
        demand = pd.concat([dfChina, dfJapan, dfSK], axis=1)
        demand = demand.round(2)
        demand.fillna(0,inplace=True)
        demand.index = pd.to_datetime(demand.index)
        demand = demand.resample('MS').mean()
        #print(demand)
        demand.to_csv('C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/JKCdemand/scenarios.csv')
        
        return dfmodel


    def demand_model_last(temphist, templast):
        
        today = datetime.date.today()
        if today.month <10:
            start = str(today.year-1)+'-10-01'
        
            end = str(today.year)+'-03-31'#pd.to_datetime(str(next_month.year)+'-'+str(next_month.month)+'-'+str(calendar.monthrange(next_month.year, next_month.month)[1]))
        else:
            start = str(today.year)+'-10-01'
            end = str(today.year+1)+'-03-31'
        
        next_month = today + relativedelta(months=(1))
        #print(temphist.loc[pd.to_datetime(start).date():today])
        #print(templast)
        #create fcst temp df
        dfmodel = pd.DataFrame(index=pd.date_range(start=start, end=end, freq='D').date, columns=['China','Japan','South Korea'])
        dfmodel.loc[pd.to_datetime(start).date():today,['China','Japan','South Korea']] = temphist.loc[pd.to_datetime(start).date():today,['China','Japan','South Korea']]
        dfmodel.loc[today- relativedelta(days=(5)):today + relativedelta(days=(15)),['China','Japan','South Korea']] = templast.loc[today- relativedelta(days=(5)):today + relativedelta(days=(15)),['China ens','Japan ens','South Korea ens']].values
        dfmodel.loc[today + relativedelta(days=(15)):next_month,['China','Japan','South Korea']] = templast.loc[today + relativedelta(days=(15)):next_month,['China EC46','Japan EC46','South Korea EC46']].values
        dfmodel.loc[next_month:pd.to_datetime(end).date(),['China','Japan','South Korea']] = templast.loc[next_month:pd.to_datetime(end).date(),['China Seasonal','Japan Seasonal','South Korea Seasonal']].values

        for i in dfmodel.index:
            dfmodel.loc[i, 'normal China'] = temphist.loc['2018-'+str(i.month)+'-'+str(i.day): '2018-'+str(i.month)+'-'+str(i.day),'China normal'].values
            dfmodel.loc[i, 'normal Japan'] = temphist.loc['2018-'+str(i.month)+'-'+str(i.day): '2018-'+str(i.month)+'-'+str(i.day),'Japan normal'].values
            dfmodel.loc[i, 'normal South Korea'] = temphist.loc['2018-'+str(i.month)+'-'+str(i.day): '2018-'+str(i.month)+'-'+str(i.day),'South Korea normal'].values
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
    
    
    def update():        
        a=JKC_Demand
        temphist=a.temp_hist_data()
        temp, templast = a.temp_data()
        #latest
       
        
        scenario = -1
        dfmodel = a.demand_model(temphist, temp, scenario)
        #last
        #dfmodellast, demandlast = a.demand_model_last(temphist, templast)
        #charts
        #a.plot_chart(dfmodel, demand, demandnormal)
        #a.plot_chart_cum(dfcumchina, dfcumjapan, dfcumsk)
        #a.plat_table(dfmodel, demand, demandlast, demandnormal)
a=JKC_Demand
a.update()