# -*- coding: utf-8 -*-
"""
Created on Thu Dec  1 14:45:50 2022

@author: SVC-GASQuant2-Prod
"""

# -*- coding: utf-8 -*-
"""
Created on Wed Oct 12 10:11:44 2022

@author: SVC-GASQuant2-Prod
"""

import numpy as np
import pandas as pd
import datetime
import plotly.offline as py
import plotly.graph_objs as go
from dateutil.relativedelta import relativedelta
import sys
sys.path.append('C:\\Users\SVC-GASQuant2-Prod\YWwebsite\Python Scripts\class') 
pd.set_option('display.max_columns',20)
from DBtoDF import DBTOPD
import pyodbc
import sqlalchemy
import calendar


#v1 add us feed T,E,i1,i2,i3 from today to d-30 table. Top table m to m-12 back rolling, add new chart total us 
    
class US_Feed():
    
    def get_data():
        
        today = datetime.date.today()
        
        df = DBTOPD.get_us_feed()
        #df.set_index('GasDate', inplace=True)
        #print(df)
        df = df[['MEASUREMENT_DATE','FACILITY','NOMINATION_CYCLE','LOCATION_TYPE','SCHEDULED_VOLUME']]
        #df = df[(df['NOMINATION_CYCLE'] == 'T') | (df['NOMINATION_CYCLE'] =='I3' )]
        #print(df)
        dfelba =  df[df['LOCATION_TYPE'] =='D']
        dfelba = dfelba[dfelba['FACILITY'] =='Elba']
        dfelbahist = dfelba[dfelba['NOMINATION_CYCLE'] =='I3']
        dfelbahist_povit = pd.pivot_table(dfelbahist, values = 'SCHEDULED_VOLUME', index = 'MEASUREMENT_DATE', columns = 'FACILITY', aggfunc='sum')
        #print(dfelbahist_povit)
        dfelbatoday = dfelba[dfelba['NOMINATION_CYCLE'] =='T']
        dfelbatoday_povit = pd.pivot_table(dfelbatoday, values = 'SCHEDULED_VOLUME', index = 'MEASUREMENT_DATE', columns = 'FACILITY', aggfunc='sum')
        dfelbatoday_povit = dfelbatoday_povit.loc[[str(today)]]
        dfelbafull =  pd.concat([dfelbahist_povit, dfelbatoday_povit])
        #print(dfelbafull)
        
        #df for all others
        df = df[df['LOCATION_TYPE']=='Sum']
        
        dfhist = df[df['NOMINATION_CYCLE'] =='I3']
        #print(dfhist)
        dfhist_povit = pd.pivot_table(dfhist, values = 'SCHEDULED_VOLUME', index = 'MEASUREMENT_DATE', columns = 'FACILITY', aggfunc='sum')
        dfhist_povit = dfhist_povit.loc[:today-relativedelta(days=1)]
        #print(dfhist_povit)
        
        dftoday = df[df['NOMINATION_CYCLE'] =='T']
        dftoday_povit = pd.pivot_table(dftoday, values = 'SCHEDULED_VOLUME', index = 'MEASUREMENT_DATE', columns = 'FACILITY', aggfunc='sum')
        dftoday_povit = dftoday_povit.loc[[str(today)]]
        #print(dftoday_povit)
        
        
        
        dffull = pd.concat([dfhist_povit, dftoday_povit])
        dffull['Elba'] = dfelbafull
        dffull.fillna(0, inplace=True)
        dffull['LNG Exports Feedgas'] = dffull.sum(axis=1)
        
        db_server_lng = "PRD-DB-SQL-211"
        db_name_lng = "LNG"
        sql_engine_lng = sqlalchemy.create_engine("mssql+pyodbc://" + db_server_lng + "/" + db_name_lng + "?trusted_connection=yes&driver=ODBC+Driver+13+for+SQL+Server")
        dffull.sort_index(inplace=True)
        dffull.to_sql(name='USBentekFeedgas', con=sql_engine_lng, index=True, if_exists='replace', schema='ana')
        
        #print(dffull)
        
        desk = DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'DeskSupplyCountry').sql_to_df()
        desk.set_index('Date', inplace=True)
        #print(desk)
        dffull['US desk'] = desk['United States']
        for i in dffull.index:
            days=calendar.monthrange(i.year,i.month)
            dffull['US desk'].loc[i]=dffull['US desk'].loc[i]*days[1]/1397/0.88/0.00739778419697034/days[1]*365 #overall desk loss 12%
        
        #print(dffull.loc['2019-01-01':,'Elba'].head(20))
        
        
        #read data from Kpler
        Kpler=DBTOPD('PRD-DB-SQL-211','LNG', 'dbo', 'KplerLNGTrades')
        dfkpler=Kpler.sql_to_df()
        
        #get supply and demand df
        supply_plant_columns=['EndOrigin','InstallationOrigin','VolumeOriginM3']
        df_supply_plant = dfkpler[supply_plant_columns]
        
        #create supply list
        #SupplyCurveId = pd.read_excel('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/DeskViewID.xlsx',header=(0),sheet_name='supply')
        #SupplyCurveId=SupplyCurveId.loc[:,['CurveID','plant']]
        '''
        curveid = DBTOPD.get_curve_id()
        dfcurveid = curveid.loc[:,['CurveId','Type','Location','Country']]
        
        SupplyCurveId = dfcurveid.loc[dfcurveid[dfcurveid['Type']=='Supply'].index]
        SupplyCurveId = SupplyCurveId[['CurveId','Country','Location']]
        SupplyCurveId.rename(columns={'CurveId':'CurveID','Country':'Country','Location':'plant'}, inplace=True)
        #SupplyCurveId=SupplyCurveId.loc[:,['CurveID','plant']]
        #SupplyCurveId = SupplyCurveId
        print(SupplyCurveId)
        '''
        #rolling start date
        start_date='2016-01-01'
        today=datetime.date.today()
        dfMA=pd.DataFrame(index=pd.date_range(start=start_date,end=today))
        dfMA.index.name='Date'
        for i in ['Calcasieu Pass',  'Cameron (Liqu.)' , 'Corpus Christi',  'Cove Point' ,'Elba Island Liq.', 'Freeport','Sabine Pass'] :
            
            dffulldate=pd.DataFrame(index=pd.date_range(start=start_date,end=today)) 
            dffulldate.index.name='Date'
            dfplant = df_supply_plant[df_supply_plant['InstallationOrigin']==i]
            dfplant.loc[:,'EndOrigin']=pd.to_datetime(dfplant.loc[:,'EndOrigin']).dt.strftime('%Y-%m-%d') 
            
            dfplant=dfplant.groupby(['EndOrigin']).sum()*0.0000216135248180093*1000
            dfplant.index.name='Date'
            dfplant.index=pd.DatetimeIndex(dfplant.index)
            dfplant.sort_index(inplace=True)
            
            merged = pd.merge(dffulldate, dfplant.loc[start_date: str(today)], on='Date', how='outer')
            merged.fillna(0, inplace=True)
            
            npMA=merged.loc[start_date: str(today)]
            dfMA[i]=npMA.loc[start_date:today].values
        dfMA.columns = ['Calcasieu Pass',  'Cameron' , 'Corpus',  'Cove' , 'Elba', 'Freeport','Sabine Pass'] 
        #print(dfMA)
        
        dfsabine = dfkpler[['Vessel','StartOrigin','EndOrigin','InstallationOrigin','VolumeOriginM3' ]]
        dfsabine = dfsabine[dfsabine['InstallationOrigin']=='Sabine Pass']
        dfsabine.loc[:,'StartOrigin']=pd.to_datetime(dfsabine.loc[:,'StartOrigin'])
        dfsabine.sort_values(by='StartOrigin', inplace=True)
        dfsabine = dfsabine.loc[dfsabine[dfsabine['StartOrigin'] <= (datetime.datetime.today()+relativedelta(days = 1))].index]
        #dfsabine.drop(index = dfsabine[dfsabine['StartOrigin'] == 'NaT'].index, inplace=True)
        dfsabine = dfsabine.iloc[-14:]
        dfsabine.reset_index(inplace=True)
                
        
        #print(dfsabine)
        
        
        
        return df, dffull, dfMA, dfsabine
    
    def table_us_tei(dftei): 
        
        #print(dftei)
        today = datetime.date.today()
        dfsum =  dftei[dftei['LOCATION_TYPE'] =='Sum']
        
        dfcalcasieu = dfsum[dfsum['FACILITY'] =='Calcasieu Pass']
        dfcalcasieupovit = pd.pivot_table(dfcalcasieu, values = 'SCHEDULED_VOLUME', index = 'MEASUREMENT_DATE', columns = 'NOMINATION_CYCLE', aggfunc='sum')
        dfcalcasieufull = pd.DataFrame(index = pd.date_range(start = today-relativedelta(days=30), end = today), columns = ['T','E','I1','I2','I3'])
        for i in pd.date_range(start = today-relativedelta(days=30), end = dfcalcasieupovit.index[-1]):
            dfcalcasieufull.loc[i] = dfcalcasieupovit.loc[i]
        dfcalcasieufull.fillna(0, inplace=True)
        #print(dfcalcasieufull)
        
        dfcameron = dfsum[dfsum['FACILITY'] =='Cameron']
        dfcameronpovit = pd.pivot_table(dfcameron, values = 'SCHEDULED_VOLUME', index = 'MEASUREMENT_DATE', columns = 'NOMINATION_CYCLE', aggfunc='sum')
        dfcameronfull = pd.DataFrame(index = pd.date_range(start = today-relativedelta(days=30), end = today), columns = ['T','E','I1','I2','I3'])
        for i in pd.date_range(start = today-relativedelta(days=30), end = dfcameronpovit.index[-1]):
            dfcameronfull.loc[i] = dfcameronpovit.loc[i]
        dfcameronfull.fillna(0, inplace=True)
        #print(dfcameronfull)
        
        dfcorpus = dfsum[dfsum['FACILITY'] =='Corpus']
        dfcorpuspovit = pd.pivot_table(dfcorpus, values = 'SCHEDULED_VOLUME', index = 'MEASUREMENT_DATE', columns = 'NOMINATION_CYCLE', aggfunc='sum')
        dfcorpusfull = pd.DataFrame(index = pd.date_range(start = today-relativedelta(days=30), end = today), columns = ['T','E','I1','I2','I3'])
        for i in pd.date_range(start = today-relativedelta(days=30), end = dfcorpuspovit.index[-1]):
            dfcorpusfull.loc[i] = dfcorpuspovit.loc[i]
        dfcorpusfull.fillna(0, inplace=True)
        #print(dfcorpusfull)
        
        dfcove = dfsum[dfsum['FACILITY'] =='Cove']
        dfcovepovit = pd.pivot_table(dfcove, values = 'SCHEDULED_VOLUME', index = 'MEASUREMENT_DATE', columns = 'NOMINATION_CYCLE', aggfunc='sum')
        dfcovefull = pd.DataFrame(index = pd.date_range(start = today-relativedelta(days=30), end = today), columns = ['T','E','I1','I2','I3'])
        for i in pd.date_range(start = today-relativedelta(days=30), end = dfcovepovit.index[-1]):
            dfcovefull.loc[i] = dfcovepovit.loc[i]
        dfcovefull.fillna(0, inplace=True)
        #print(dfcovefull)
        
        #dfelbasum =  dftei[dftei['LOCATION_TYPE'] =='D']
        #dfelba =  dftei[dftei['LOCATION_TYPE'] =='D']
        df = DBTOPD.get_us_feed()
        df = df[['MEASUREMENT_DATE','FACILITY','NOMINATION_CYCLE','LOCATION_TYPE','SCHEDULED_VOLUME']]
        dfelba =  df[df['LOCATION_TYPE'] =='D']
        dfelba = dfelba[dfelba['FACILITY'] =='Elba']
        #dfelba = dfelbasum[dfelbasum['FACILITY'] =='Elba']
        dfelbapovit = pd.pivot_table(dfelba, values = 'SCHEDULED_VOLUME', index = 'MEASUREMENT_DATE', columns = 'NOMINATION_CYCLE', aggfunc='sum')
        dfelbafull = pd.DataFrame(index = pd.date_range(start = today-relativedelta(days=30), end = today), columns = ['T','E','I1','I2','I3'])
        for i in pd.date_range(start = today-relativedelta(days=30), end = dfelbapovit.index[-1]):
            dfelbafull.loc[i] = dfelbapovit.loc[i]
        dfelbafull.fillna(0, inplace=True)
        #print(dfelbafull)
        
        dffreeport = dfsum[dfsum['FACILITY'] =='Freeport']
        dffreeportpovit = pd.pivot_table(dffreeport, values = 'SCHEDULED_VOLUME', index = 'MEASUREMENT_DATE', columns = 'NOMINATION_CYCLE', aggfunc='sum')
        dffreeportfull = pd.DataFrame(index = pd.date_range(start = today-relativedelta(days=30), end = today), columns = ['T','E','I1','I2','I3'])
        for i in pd.date_range(start = today-relativedelta(days=30), end = today):
            dffreeportfull.loc[i] = dffreeportpovit.loc[i]
        dffreeportfull.fillna(0, inplace=True)
        #print(dffreeportfull)
        
        dfsabine = dfsum[dfsum['FACILITY'] =='Sabine Pass']
        dfsabinepovit = pd.pivot_table(dfsabine, values = 'SCHEDULED_VOLUME', index = 'MEASUREMENT_DATE', columns = 'NOMINATION_CYCLE', aggfunc='sum')
        dfsabinefull = pd.DataFrame(index = pd.date_range(start = today-relativedelta(days=30), end = today), columns = ['T','E','I1','I2','I3'])
        for i in pd.date_range(start = today-relativedelta(days=30), end = dfsabinepovit.index[-1]):
            dfsabinefull.loc[i] = dfsabinepovit.loc[i]
        dfsabinefull.fillna(0, inplace=True)
        #print(dfsabinefull)
        #print(dfelbapovit)
        start = today-relativedelta(days=30)
        #dfus = pd.concat([dfcalcasieufull, dfcameronfull,dfcorpusfull,dfcovefull,dfelbafull,dffreeportfull,dfsabinefull]).sum(axis=1,skipna=True)
        dfus = dfcalcasieufull.loc[start:] + dfcameronfull.loc[start:] + dfcorpusfull.loc[start:] + dfcovefull.loc[start:] + dfelbafull.loc[start:] + dffreeportfull.loc[start:] + dfsabinefull.loc[start:]
        dfus = dfus.loc[:today]
        #print(dfus)
        '''
        dfus = pd.DataFrame(index = pd.date_range(start = today-relativedelta(days=30), end = today), columns = ['T','E','I1','I2','I3'])
        #print(dfcalcasieupovit.index[-1].date() == dfus.index[-3].date())
        
        
        
        if dfcalcasieupovit.index[-1].date() == dfus.index[-1].date():
            #print('YEs')
            for i in dfus.index:
                dfus.loc[i] = dfcalcasieupovit.loc[i] + dfcameronpovit.loc[i] + dfcorpuspovit.loc[i] + dfcovepovit.loc[i] + dfelbapovit.loc[i] + dffreeportpovit.loc[i] + dfsabinepovit.loc[i]
            #print(dfus)
        else:
            #print('No')
            for i in dfus.index[:-3]:
                dfus.loc[i] = dfcalcasieupovit.loc[i] + dfcameronpovit.loc[i] + dfcorpuspovit.loc[i] + dfcovepovit.loc[i] + dfelbapovit.loc[i] + dffreeportpovit.loc[i] + dfsabinepovit.loc[i]
            for i in dfus.index[-3:]:
                dfus.loc[i] =  dfcameronpovit.loc[i] + dfcorpuspovit.loc[i] + dfcovepovit.loc[i] + dfelbapovit.loc[i] + dffreeportpovit.loc[i] + dfsabinepovit.loc[i]
        '''   
            #print(dfus)
        dfus.sort_index(ascending=False, inplace=True)
        dfus.rename(columns={'T':'Nom made D-1 by 11:00','E':'Nom made D-1 by 16:00','I1':'Nom made D by 08:00','I2':'Nom made D by 12:30','I3':'Nom made D by 17:00'}, inplace=True)
        fig = go.Figure(data=[go.Table(
                                        header=dict(values=['date'] + list(dfus.columns),
                                                    fill_color='paleturquoise',
                                                    align='center'),
                                        cells=dict(values=[[pd.to_datetime(m).strftime('%Y-%m-%d') for m in dfus.index]] + [dfus[pm] for pm in dfus.columns],
                                                   fill_color='lavender',
                                                   #font=dict(color = ['black','black']+dfcolor.values.tolist()),
                                                   align='center'))
                                    ])
        py.plot(fig, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/US feedgas/USfeedteitable.html', auto_open=False)
        fig.write_image('U:/Trading - Gas/LNG/LNG website/analysis/US feedgas/USfeedteitable.png')
        
        dfcalcasieupovit =  dfcalcasieupovit.loc[today-relativedelta(days=30):today,['T','E','I1','I2','I3']]  
        dfcalcasieupovit.sort_index(ascending=False, inplace=True)
        dfcalcasieupovit.rename(columns={'T':'Nom made D-1 by 11:00','E':'Nom made D-1 by 16:00','I1':'Nom made D by 08:00','I2':'Nom made D by 12:30','I3':'Nom made D by 17:00'}, inplace=True)

        #print(dfcalcasieupovit)
        figcalcasieu = go.Figure(data=[go.Table(
                                        header=dict(values=['date'] + list(dfcalcasieupovit.columns),
                                                    fill_color='paleturquoise',
                                                    align='center'),
                                        cells=dict(values=[[pd.to_datetime(m).strftime('%Y-%m-%d') for m in dfcalcasieupovit.index]] + [dfcalcasieupovit[pm] for pm in dfcalcasieupovit.columns],
                                                   fill_color='lavender',
                                                   #font=dict(color = ['black','black']+dfcolor.values.tolist()),
                                                   align='center'))
                                    ])
        py.plot(figcalcasieu, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/US feedgas/calcasieuteitable.html', auto_open=False)
        figcalcasieu.write_image('U:/Trading - Gas/LNG/LNG website/analysis/US feedgas/calcasieuteitable.png')
        
        dfcameronpovit =  dfcameronpovit.loc[today-relativedelta(days=30):today,['T','E','I1','I2','I3']]  
        dfcameronpovit.sort_index(ascending=False, inplace=True)
        dfcameronpovit.rename(columns={'T':'Nom made D-1 by 11:00','E':'Nom made D-1 by 16:00','I1':'Nom made D by 08:00','I2':'Nom made D by 12:30','I3':'Nom made D by 17:00'}, inplace=True)

        #print(dfcalcasieupovit)
        figcameron = go.Figure(data=[go.Table(
                                        header=dict(values=['date'] + list(dfcameronpovit.columns),
                                                    fill_color='paleturquoise',
                                                    align='center'),
                                        cells=dict(values=[[pd.to_datetime(m).strftime('%Y-%m-%d') for m in dfcameronpovit.index]] + [dfcameronpovit[pm] for pm in dfcameronpovit.columns],
                                                   fill_color='lavender',
                                                   #font=dict(color = ['black','black']+dfcolor.values.tolist()),
                                                   align='center'))
                                    ])
        py.plot(figcameron, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/US feedgas/cameronteitable.html', auto_open=False)
        figcameron.write_image('U:/Trading - Gas/LNG/LNG website/analysis/US feedgas/cameronteitable.png')
        
        dfcorpuspovit =  dfcorpuspovit.loc[today-relativedelta(days=30):today,['T','E','I1','I2','I3']]  
        dfcorpuspovit.sort_index(ascending=False, inplace=True)
        dfcorpuspovit.rename(columns={'T':'Nom made D-1 by 11:00','E':'Nom made D-1 by 16:00','I1':'Nom made D by 08:00','I2':'Nom made D by 12:30','I3':'Nom made D by 17:00'}, inplace=True)
        #print(dfcalcasieupovit)
        figcorpus = go.Figure(data=[go.Table(
                                        header=dict(values=['date'] + list(dfcorpuspovit.columns),
                                                    fill_color='paleturquoise',
                                                    align='center'),
                                        cells=dict(values=[[pd.to_datetime(m).strftime('%Y-%m-%d') for m in dfcorpuspovit.index]] + [dfcorpuspovit[pm] for pm in dfcorpuspovit.columns],
                                                   fill_color='lavender',
                                                   #font=dict(color = ['black','black']+dfcolor.values.tolist()),
                                                   align='center'))
                                    ])
        py.plot(figcorpus, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/US feedgas/corpusteitable.html', auto_open=False)
        figcorpus.write_image('U:/Trading - Gas/LNG/LNG website/analysis/US feedgas/corpusteitable.png')
        
        dfcovepovit =  dfcovepovit.loc[today-relativedelta(days=30):today,['T','E','I1','I2','I3']]  
        dfcovepovit.sort_index(ascending=False, inplace=True)
        dfcovepovit.rename(columns={'T':'Nom made D-1 by 11:00','E':'Nom made D-1 by 16:00','I1':'Nom made D by 08:00','I2':'Nom made D by 12:30','I3':'Nom made D by 17:00'}, inplace=True)
        #print(dfcalcasieupovit)
        figcove = go.Figure(data=[go.Table(
                                        header=dict(values=['date'] + list(dfcovepovit.columns),
                                                    fill_color='paleturquoise',
                                                    align='center'),
                                        cells=dict(values=[[pd.to_datetime(m).strftime('%Y-%m-%d') for m in dfcovepovit.index]] + [dfcovepovit[pm] for pm in dfcovepovit.columns],
                                                   fill_color='lavender',
                                                   #font=dict(color = ['black','black']+dfcolor.values.tolist()),
                                                   align='center'))
                                    ])
        py.plot(figcove, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/US feedgas/coveteitable.html', auto_open=False)
        figcove.write_image('U:/Trading - Gas/LNG/LNG website/analysis/US feedgas/coveteitable.png')
        
        dfelbapovit =  dfelbapovit.loc[today-relativedelta(days=30):today,['T','E','I1','I2','I3']]  
        dfelbapovit.sort_index(ascending=False, inplace=True)
        dfelbapovit.rename(columns={'T':'Nom made D-1 by 11:00','E':'Nom made D-1 by 16:00','I1':'Nom made D by 08:00','I2':'Nom made D by 12:30','I3':'Nom made D by 17:00'}, inplace=True)
        #print(dfcalcasieupovit)
        figelba = go.Figure(data=[go.Table(
                                        header=dict(values=['date'] + list(dfelbapovit.columns),
                                                    fill_color='paleturquoise',
                                                    align='center'),
                                        cells=dict(values=[[pd.to_datetime(m).strftime('%Y-%m-%d') for m in dfelbapovit.index]] + [dfelbapovit[pm] for pm in dfelbapovit.columns],
                                                   fill_color='lavender',
                                                   #font=dict(color = ['black','black']+dfcolor.values.tolist()),
                                                   align='center'))
                                    ])
        py.plot(figelba, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/US feedgas/elbateitable.html', auto_open=False)
        figelba.write_image('U:/Trading - Gas/LNG/LNG website/analysis/US feedgas/elbateitable.png')
        
        dffreeportpovit =  dffreeportpovit.loc[today-relativedelta(days=30):today,['T','E','I1','I2','I3']]  
        dffreeportpovit.sort_index(ascending=False, inplace=True)
        dffreeportpovit.rename(columns={'T':'Nom made D-1 by 11:00','E':'Nom made D-1 by 16:00','I1':'Nom made D by 08:00','I2':'Nom made D by 12:30','I3':'Nom made D by 17:00'}, inplace=True)
        #print(dfcalcasieupovit)
        figfreeport = go.Figure(data=[go.Table(
                                        header=dict(values=['date'] + list(dffreeportpovit.columns),
                                                    fill_color='paleturquoise',
                                                    align='center'),
                                        cells=dict(values=[[pd.to_datetime(m).strftime('%Y-%m-%d') for m in dffreeportpovit.index]] + [dffreeportpovit[pm] for pm in dffreeportpovit.columns],
                                                   fill_color='lavender',
                                                   #font=dict(color = ['black','black']+dfcolor.values.tolist()),
                                                   align='center'))
                                    ])
        py.plot(figfreeport, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/US feedgas/freeportteitable.html', auto_open=False)
        figfreeport.write_image('U:/Trading - Gas/LNG/LNG website/analysis/US feedgas/freeportteitable.png')
        
        dfsabinepovit =  dfsabinepovit.loc[today-relativedelta(days=30):today,['T','E','I1','I2','I3']]  
        dfsabinepovit.sort_index(ascending=False, inplace=True)
        dfsabinepovit.rename(columns={'T':'Nom made D-1 by 11:00','E':'Nom made D-1 by 16:00','I1':'Nom made D by 08:00','I2':'Nom made D by 12:30','I3':'Nom made D by 17:00'}, inplace=True)
        #print(dfcalcasieupovit)
        figsabine = go.Figure(data=[go.Table(
                                        header=dict(values=['date'] + list(dfsabinepovit.columns),
                                                    fill_color='paleturquoise',
                                                    align='center'),
                                        cells=dict(values=[[pd.to_datetime(m).strftime('%Y-%m-%d') for m in dfsabinepovit.index]] + [dfsabinepovit[pm] for pm in dfsabinepovit.columns],
                                                   fill_color='lavender',
                                                   #font=dict(color = ['black','black']+dfcolor.values.tolist()),
                                                   align='center'))
                                    ])
        py.plot(figsabine, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/US feedgas/sabineteitable.html', auto_open=False)
        figsabine.write_image('U:/Trading - Gas/LNG/LNG website/analysis/US feedgas/sabineteitable.png')
        
        
        
    
    def chart_us(df):
        
        today = datetime.date.today()
        
        dfus = df.loc['2016-01-01': today, ['LNG Exports Feedgas']]
        dfus['10 day MA'] = dfus['LNG Exports Feedgas'].rolling(10).mean()
        dfus['30 day MA'] = dfus['LNG Exports Feedgas'].rolling(30).mean()
        dfus['US desk'] = df['US desk']
        #print(dfus)
        
        fig=go.Figure()
        fig.add_trace(go.Scatter(x=dfus.index, y=dfus['LNG Exports Feedgas'],
                            mode='lines',
                            name='Actual',
                            line=dict(color='red', dash='solid')
                            ))
        fig.add_trace(go.Scatter(x=dfus.index, y=dfus['10 day MA'],
                            mode='lines',
                            name='10 day MA',
                            line=dict(color='black', dash='dash')
                            ))
        fig.add_trace(go.Scatter(x=dfus.index, y=dfus['30 day MA'],
                            mode='lines',
                            name='30 day MA',
                            line=dict(color='blue', dash='dash')
                            ))
        fig.add_trace(go.Scatter(x=dfus.index, y=dfus['US desk'],
                            mode='lines',
                            name='US desk',
                            line=dict(color='green', dash='solid')
                            ))
        
        fig.update_layout(
             autosize=True,
             showlegend=True,
             #colorscale='RdBu',
             legend=dict(x=0, y=-0.2),
             legend_orientation="h",
             title_text='US Total Feedgas Bcf/d '+str(today),
             #xaxis = dict(dtick="M1"),
             #xaxis_tickformat = '%B',
             hovermode='x unified',
             plot_bgcolor = 'white',
             template='ggplot2'  
         )
        fig.update_yaxes(title_text="Bcf/d")
        
        py.plot(fig, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/US feedgas/chartUS.html', auto_open=False)
        fig.write_image('U:/Trading - Gas/LNG/LNG website/analysis/US feedgas/chartUS.png')
        
    def chart_plant(df):
        
        today = datetime.date.today()
        #print(df.columns)
        columns = ['Calcasieu Pass',  'Cameron' , 'Corpus',  'Cove' ,'Elba', 'Freeport','Sabine Pass'] 
        
        
        dfplant = df.loc['2016-01-01': today]
        #for i in dfplant.columns:
            #dfplant[i+' cumsum'] = dfplant[i].cumsum()
        
        
        for i in columns:
            
            #print(i)
            start = dfplant[dfplant[i].gt(201)].index[0] # get first > 0, means start to operate
            
            fig1 = go.Figure()
            fig1.add_trace(go.Scatter(x=dfplant.loc[start:].index, y=dfplant.loc[start:,i],
                                mode='lines',
                                name=i,
                                line=dict(color='blue', dash='solid')
                                ))
            
            fig1.update_layout(
                 autosize=True,
                 #showlegend=True,
                 #colorscale='RdBu',
                 legend=dict(x=0, y=-0.2),
                 legend_orientation="h",
                 title_text=i+' Total Feedgas Mcf/d '+str(today),
                 xaxis = dict(dtick="M3"),
                 #xaxis_tickformat = '%B',
                 hovermode='x unified',
                 plot_bgcolor = 'white',
                 template='ggplot2'  
             )
            fig1.update_yaxes(title_text="Mcf/d")

            
            py.plot(fig1, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/US feedgas/'+i+'chart.html', auto_open=False)
            fig1.write_image('U:/Trading - Gas/LNG/LNG website/analysis/US feedgas/'+i+'chart.png')
        
    def chart_plant_cum(df, dfcargo):
        
        today = datetime.date.today()

        column = ['Calcasieu Pass',  'Cameron' , 'Corpus',  'Cove' , 'Elba', 'Freeport','Sabine Pass'] 
        cmtobcf = 0.0000216135248180093
        vesselcm = 150000
        vessel = vesselcm*cmtobcf*1000
        loss = 0.1
        
        dfcum = pd.DataFrame(index = ['storage max','storage'], columns = column)
        dfcum.loc['storage','Sabine Pass'] = 1262768
        dfcum.loc['storage','Cove'] = 700000
        dfcum.loc['storage','Corpus'] = 480000
        dfcum.loc['storage','Cameron'] = 640000
        dfcum.loc['storage','Freeport'] = 320000
        dfcum.loc['storage','Elba'] = 200000
        dfcum.loc['storage','Calcasieu Pass'] = 400000 
        
        dfcumfull = df.loc['2016-01-01':,column].copy()
        dfcumfull['1 vessel'] = vessel
        
        for i in column:
            dfcum.loc['storage max',i] = dfcum.loc['storage',i]*cmtobcf*1000
            
            dfcumfull[i+' Storage max'] = dfcum.loc['storage max',i]
            
        
            
        dfcumfull.loc['2016-01-01',['Calcasieu Pass cum',  'Cameron cum' , 'Corpus cum',  'Cove cum' , 'Elba cum', 'Freeport cum','Sabine Pass cum']] = dfcumfull.loc['2016-01-01', column].values
        #dfcumfull.loc[dfcumfull.index[0], ['Calcasieu Pass cum',  'Cameron cum' , 'Corpus cum',  'Cove cum' , 'Elba cum', 'Freeport cum','Sabine Pass cum']] = dfcumfull.loc[dfcumfull.index[0], column]
        #print(dfcumfull)
        for j in dfcumfull.index[1:]:
            for i in column:
                if dfcumfull.loc[j-relativedelta(days=1), i+' cum'] >=  dfcumfull.loc[j-relativedelta(days=1),i+' Storage max']:
                    dfcumfull.loc[j, i+' cum'] = dfcumfull.loc[j-relativedelta(days=1),i+' Storage max'] + dfcumfull.loc[j, i]*(1-loss)-dfcargo.loc[j,i]
                elif dfcumfull.loc[j-relativedelta(days=1), i+' cum'] <=0 :
                    dfcumfull.loc[j, i+' cum'] = dfcumfull.loc[j, i]*(1-loss)-dfcargo.loc[j,i]
                else:
                    dfcumfull.loc[j, i+' cum'] = dfcumfull.loc[j-relativedelta(days=1), i+' cum'] + dfcumfull.loc[j, i]*(1-loss)-dfcargo.loc[j,i]
        #print(dfcumfull.info())
        #print(dfcumfull)
        dfcumfull = dfcumfull.astype('int')
        
        #dfcargo.to_csv('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/usfeed.csv')
        
        dfplant = df.loc['2016-01-01': today]
        #print(dfcargo)
            
        for i in column:
            
            start = dfplant[dfplant[i].gt(dfplant[i].max()*0.5)].index[0]# dfcumfull[dfcumfull[i].gt(0)].index.to_list()[0] # get first > 0, means start to operate
            fig1 = go.Figure()
            fig1.add_trace(go.Scatter(x=dfcumfull.loc[start:,].index, y=dfcumfull.loc[start:,i+' cum'],
                                mode='lines',
                                name='Cumulative',
                                line=dict(color='blue', dash='solid')
                                ))
            fig1.add_trace(go.Scatter(x=dfcumfull.loc[start:,].index, y=dfcumfull.loc[start:,i+' Storage max'],
                                mode='lines',
                                name='storage max',
                                line=dict(color='grey', dash='dash')
                                ))
            fig1.add_trace(go.Scatter(x=dfcumfull.loc[start:,].index, y=dfcumfull.loc[start:,'1 vessel'],
                                mode='lines',
                                name='1 vessel',
                                line=dict(color='orange', dash='solid')
                                ))
            fig1.add_trace(go.Bar(x=dfcargo.loc[start:,].index, y=dfcargo.loc[start:,i],
                                name='Cargoes',
                                marker_color='grey',
                                ))
            
            fig1.update_layout(
                 autosize=True,
                 showlegend=True,
                 #colorscale='RdBu',
                 legend=dict(x=0, y=-0.2),
                 legend_orientation="h",
                 title_text=i+' meter at plant Mcf '+str(today),
                 xaxis = dict(dtick="M3"),
                 #xaxis_tickformat = '%B',
                 hovermode='x unified',
                 plot_bgcolor = 'white',
                 template='ggplot2'  
             )
            fig1.update_yaxes(title_text="Mcf/d")
            
            py.plot(fig1, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/US feedgas/'+i+'cumchart.html', auto_open=False)
            fig1.write_image('U:/Trading - Gas/LNG/LNG website/analysis/US feedgas/'+i+'cumchart.png')
    
    def table_plant(df):
        
        today = datetime.date.today()
        d1 = today - relativedelta(days = 1)
        d7 = today - relativedelta(days = 7)
        d30 = today - relativedelta(days = 30)
        d365 = today - relativedelta(days = 365)
        
        column = ['Calcasieu Pass',  'Cameron' , 'Corpus',  'Cove' , 'Elba', 'Freeport','Sabine Pass'] 

        dftable = pd.DataFrame(index = [today, d1, d7, d30, d365], columns = column)
        for i in dftable.index:
            dftable.loc[i] = df.loc[str(i)]
        
        dftable['Total'] = dftable.sum(axis=1)
        #print(dftable)
        dftable = dftable.astype('int')
        #print(df)
        dfm = df.resample('MS').mean()
        dfmonth = pd.DataFrame(index = pd.date_range(start = str((today-relativedelta(months=12)).year)+'-'+str((today-relativedelta(months=12)).month)+'-01', end = str(today.year)+'-'+str(today.month)+'-01', freq = 'MS'), columns = column)
        for i in dfmonth.index:
            try:
                dfmonth.loc[str(i)] = dfm.loc[str(i), column].astype('int')
            except KeyError:
                pass
        dfmonth['Total'] = dfmonth.sum(axis = 1)    
        dfmonth.index = dfmonth.index.strftime('%Y-%m-%d')
        dfmonth.sort_index(ascending=False, inplace=True)
        #dfmonth = dfmonth.astype('int')
        
        dftable = pd.concat([dftable, dfmonth])
        #dftable.index = dftable.index.strftime('%Y-%m-%d')
        
        #print(dfmonth.index)
        
        for i in dftable.index:
            if dftable.loc[i, 'Total'] > 0:
                dftable.loc[i, 'Today vs. Hist'] =   dftable.loc[today, 'Total'] - dftable.loc[i, 'Total']
        #print(dftable)
        #dftable.fillna(0, inplace=True)
        
        dfcolor = dftable.copy()
        dfcolor.loc[:,dfcolor.columns[:-1]] = 'black'
        #print(dfcolor.info())
        for i in dfcolor.index:
            #print(i)
            if dfcolor.loc[i, 'Today vs. Hist'] > 0:
                dfcolor.loc[i, 'Today vs. Hist'] = 'red'
            elif dfcolor.loc[i, 'Today vs. Hist'] < 0:
               dfcolor.loc[i, 'Today vs. Hist'] = 'blue' 
            else:
               dfcolor.loc[i, 'Today vs. Hist'] = 'black'
        dfcolor = dfcolor.T
        #print(dfcolor)
        
        fig = go.Figure(data=[go.Table(
                                        header=dict(values=[' ','date'] + list(dftable.columns),
                                                    fill_color='paleturquoise',
                                                    align='center'),
                                        cells=dict(values=[['today', 'D-1','D-7','D-30','D-365']+[pd.to_datetime(m).strftime('%b-%y') for m in dfmonth.index]]+[dftable.index] + [dftable[pm] for pm in dftable.columns],
                                                   fill_color='lavender',
                                                   font=dict(color = ['black','black']+dfcolor.values.tolist()),
                                                   align='center'))
                                    ])
        py.plot(fig, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/US feedgas/USfeedtable.html', auto_open=False)
        fig.write_image('U:/Trading - Gas/LNG/LNG website/analysis/US feedgas/USfeedtable.png')
    
    
    def chart_sabine(dfsabine):
        
        maxdays = 6
        days = 1.2
        
        #sabine table data
        dfsabine_table = dfsabine[['Vessel', 'StartOrigin','EndOrigin','VolumeOriginM3']]
        dfsabine_table['Days'] = round((dfsabine_table['EndOrigin'] - dfsabine_table['StartOrigin']).dt.total_seconds() / 3600.0/24, 2)
        dfsabine_table['Days'].fillna(days, inplace=True)
        dfsabine_table['Hrs'] = round(dfsabine_table['Days']*24, 2)
        dfsabine_table['Arms/disconnect'] = maxdays
        dfsabine_table['Loading time'] = dfsabine_table['Hrs'] - dfsabine_table['Arms/disconnect']
        dfsabine_table['LoadRate'] = round(dfsabine_table['VolumeOriginM3'] / dfsabine_table['Loading time'],0)
        #dfsabine_table['EndOrigin'].fillna(dfsabine_table['StartOrigin']+relativedelta(hours=days*24), inplace=True)
        
        #print(dfsabine_table)    
        vessel_count = dfsabine_table['Vessel'].value_counts()
        #print(vessel_count.index)
        for i in vessel_count.index:
            if vessel_count.loc[i] > 1:
                #for j in range(1,vessel_count.loc[i]):
                dfduplicate = dfsabine_table.loc[dfsabine_table[dfsabine_table['Vessel']==i].index]
                #dfduplicate.reset_index(inplace=True)
                
                #print(dfduplicate)
                dfduplicate.loc['sum','Vessel'] = i
                dfduplicate.loc['sum','StartOrigin'] = dfduplicate['StartOrigin'].iloc[0]
                dfduplicate.loc['sum','EndOrigin'] = dfduplicate['EndOrigin'].iloc[-1]
                dfduplicate.loc['sum','VolumeOriginM3'] = dfduplicate['VolumeOriginM3'].sum()
                dfduplicate.loc['sum','Days'] = dfduplicate['Days'].sum()
                dfduplicate.loc['sum','Hrs'] = dfduplicate['Hrs'].sum()
                dfduplicate.loc['sum','Arms/disconnect'] = maxdays
                dfduplicate.loc['sum','Loading time'] = dfduplicate.loc['sum','Hrs'] - dfduplicate.loc['sum', 'Arms/disconnect']
                dfduplicate.loc['sum','LoadRate'] = round(dfduplicate.loc['sum','VolumeOriginM3'] / dfduplicate.loc['sum','Loading time'],0)
        
                dfsabine_table.drop(dfsabine_table[dfsabine_table['Vessel']==i].index, inplace=True)
                dfsabine_table = dfsabine_table.append(dfduplicate.loc['sum'])
                dfsabine_table.sort_values(by='StartOrigin', inplace=True)
        #print(dfsabine_table)    
                
        #dfsabine_table['@ 12,000 cm/hr'] = dfsabine_table['VolumeOriginM3'] / 12000
        #dfsabine_table['@ 6,000 cm/hr'] = dfsabine_table['VolumeOriginM3'] / 6000
        dfsabine_table=dfsabine_table.groupby(by=['Vessel','StartOrigin','EndOrigin'],sort=False,dropna=False, as_index=False).sum()
        
        #print(dfsabine_table)
        fig_table = go.Figure(data=[go.Table(
                                        header=dict(values=['Vessel', 'StartOrigin','EndOrigin','Loading','Days','Hrs','Load Rate'],
                                                    fill_color='paleturquoise',
                                                    align='left'),
                                        cells=dict(values=[dfsabine_table.Vessel, dfsabine_table.StartOrigin, dfsabine_table.EndOrigin, dfsabine_table.VolumeOriginM3, dfsabine_table.Days, dfsabine_table.Hrs, dfsabine_table.LoadRate],
                                                   fill_color='lavender',
                                                   align='left'))
                                    ])
        py.plot(fig_table, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/US feedgas/sabinetable.html', auto_open=False)
        fig_table.write_image('U:/Trading - Gas/LNG/LNG website/analysis/US feedgas/sabinetable.png')
        
        #sabine chart data
        dfsabinechart = dfsabine_table.copy()
        for i in dfsabinechart.index:
            #if dfsabinechart.loc[i, 'EndOrigin'] == np.datetime64('NAT'):
            #print(dfsabinechart.loc[i, 'EndOrigin'].types)
            if pd.isnull([dfsabinechart.loc[i, 'EndOrigin']]) == True:
                dfsabinechart.loc[i, 'EndOrigin'] = dfsabinechart.loc[i, 'StartOrigin']+relativedelta(hours=days*24)
                #print(i)
        #print(dfsabinechart)
        start_datetime = dfsabine_table['StartOrigin'].iloc[-1]
        dfsabinechart.set_index('Vessel', inplace=True)
        daysback = 7
        #print(dfsabinechart)
        dfchart = pd.DataFrame(index = pd.date_range(start = start_datetime - relativedelta(hours = daysback*24), end = start_datetime, freq = 'H'), columns = dfsabinechart.index)
        for i in dfchart.index:
            for j in dfchart.columns:
                #print(i, j)
                #print(dfsabinechart.loc[j, 'StartOrigin'])
                #print(dfsabinechart.loc[j, 'EndOrigin'])
                if dfsabinechart.loc[j, 'StartOrigin']<=i<=dfsabinechart.loc[j, 'EndOrigin']:
                    #print(i,j)
                    dfchart.loc[i, j] = dfsabinechart.loc[j, 'LoadRate']
                else:
                    #print(i,j)
                    dfchart.loc[i, j] = np.nan
        dfchart['high'] = 12000
        dfchart['low'] = 6000
        #print(dfchart.columns[:-2])
        
        fig_chart = go.Figure()
        for i in dfchart.columns[:-2]:
            fig_chart.add_trace(go.Scatter(
                x=dfchart.index, y=dfchart[i],
                mode='lines',
                name = i,
                stackgroup='one' # define stack group
            ))
        fig_chart.add_trace(go.Scatter(
            x=dfchart.index, y=dfchart['high'],
            line=dict(color='red', dash='dash'),
            name='Load rate high'
        ))
        fig_chart.add_trace(go.Scatter(
            x=dfchart.index, y=dfchart['low'],
            line=dict(color='red', dash='solid'),
            name='Load rate low'
        ))
        fig_chart.update_layout(
                 autosize=True,
                 showlegend=True,
                 #colorscale='RdBu',
                 legend=dict(x=0, y=-0.2),
                 legend_orientation="h",
                 title_text='Loading rate Sabine'+str(datetime.date.today()),
                 #xaxis = dict(dtick="M3"),
                 #xaxis_tickformat = '%B',
                 hovermode='x unified',
                 plot_bgcolor = 'white',
                 template='ggplot2'  
             )
        fig_chart.update_yaxes(title_text="Mcf/d")
        py.plot(fig_chart, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/US feedgas/sabineloadchart.html', auto_open=False)
        fig_chart.write_image('U:/Trading - Gas/LNG/LNG website/analysis/US feedgas/sabineloadchart.png')

    def chart_grossus(df):
        
        today = datetime.date.today()
        
        start = str(today.year)+'-'+str(today.month)+'-01'
        end = str(today.year)+'-'+str(today.month)+'-'+str(calendar.monthrange(today.year, today.month)[1])
        start1 = str((today-relativedelta(months=1)).year)+'-'+str((today-relativedelta(months=1)).month)+'-01'
        end1 = str(pd.to_datetime(start) - relativedelta(days=1))
        
        #print(df.loc['2021-10-01':])
        
        dfgross = pd.DataFrame(index = pd.date_range(start = start, end = end), columns = ['Acutal','Month-1','Desk','Average'])
        dfgross['Acutal'] = df.loc[dfgross.index[0]:dfgross.index[-1],'LNG Exports Feedgas']
        #dfgross['Month-1'] = df.loc[start1:end1, 'LNG Exports Feedgas'].values
        dfgross['Desk'] = int(df.loc[dfgross.index[0],'US desk'])
        dfgross['Average'] = int(dfgross['Acutal'].mean())
        
        #print(dfgross)
        fig_chart = go.Figure()
        
        fig_chart.add_trace(go.Scatter(
            x=dfgross.index, y=dfgross['Acutal'],
            line=dict(color='red', dash='solid'),
            name='Acutal'
        ))
        fig_chart.add_trace(go.Scatter(
            x=dfgross.index, y=df.loc[start1:end1, 'LNG Exports Feedgas'].values,
            line=dict(color='black', dash='dot'),
            name='Month - 1'
        ))
        fig_chart.add_trace(go.Scatter(
            x=dfgross.index, y=dfgross['Desk'],
            line=dict(color='blue', dash='solid'),
            name='desk'
        ))
        fig_chart.add_trace(go.Scatter(
            x=dfgross.index, y=dfgross['Average'],
            line=dict(color='black', dash='dash'),
            name='Average'
        ))
        fig_chart.add_annotation(x=dfgross.index[-1], y=dfgross['Desk'].iloc[-1],
                    text = str(dfgross['Desk'].iloc[-1]),
                    showarrow=True,
                    arrowhead=5)
        fig_chart.add_annotation(x=dfgross.index[-1], y=dfgross['Average'].iloc[-1],
                    text = str(dfgross['Average'].iloc[-1]),
                    showarrow=True,
                    ax=10,
                    ay=30,
                    arrowhead=5) 
        fig_chart.update_layout(
                 autosize=True,
                 showlegend=True,
                 #colorscale='RdBu',
                 legend=dict(x=0, y=-0.2),
                 legend_orientation="h",
                 title_text='US Gross feedgas Mcf/d '+str(datetime.date.today()),
                 #xaxis = dict(dtick="M3"),
                 #xaxis_tickformat = '%B',
                 hovermode='x unified',
                 plot_bgcolor = 'white',
                 template='ggplot2'  
             )
        fig_chart.update_yaxes(title_text="Mcf/d")
        py.plot(fig_chart, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/US feedgas/USGrosschart.html', auto_open=False)
        fig_chart.write_image('U:/Trading - Gas/LNG/LNG website/analysis/US feedgas/USGrosschart.png')
        
        
    def update():        
        dftei, df, dfcargo, dfsabine = US_Feed.get_data()
        US_Feed.table_us_tei(dftei)
        US_Feed.chart_us(df)
        US_Feed.chart_plant(df)
        US_Feed.chart_plant_cum(df, dfcargo)
        US_Feed.table_plant(df)
        US_Feed.chart_sabine(dfsabine)
        US_Feed.chart_grossus(df)
        
#US_Feed.update()