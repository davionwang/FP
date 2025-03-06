# -*- coding: utf-8 -*-
"""
Created on Thu Nov 18 08:58:47 2021

@author: SVC-GASQuant2-Prod
"""

#v1 add two charts on dash
# V2 fix data issues

    
        
import time
import sys
import numpy as np
import pandas as pd
import plotly.offline as py
import plotly.graph_objs as go
from plotly.subplots import make_subplots
import datetime
import calendar

#from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import requests
sys.path.append('C:\\Users\SVC-GASQuant2-Prod\YWwebsite\Python Scripts\class') 
pd.set_option('display.max_columns',10)
from DBtoDF import DBTOPD

class SuezRegression():
    
    def getsuez():
        
        '''cargo analysis'''
        Kpler = DBTOPD('PRD-DB-SQL-211','LNG', 'dbo', 'OutputsCargoTrackingBreakdownCounts')     
        dfkpler=Kpler.sql_to_df()
        dfkpler.set_index('InstallationOrigin', inplace=True)
        #print(dfkpler)
        dfkpler=dfkpler.loc[['Snohvit','Atlantic LNG','Cameroon FLNG Terminal','Soyo','Bonny LNG','Bioko','Ras Laffan','Das Island','Balhaf','Yamal','Sabine Pass','Cameron (Liqu.)','Cove Point','Corpus Christi','Freeport',
                            'Elba Island Liq.','Tango FLNG','Damietta','Idku','Skikda','Bethioua']]
        #print(dfkpler)
        
        dfkpler.set_index('SuezDestination', inplace=True)
        EoS = dfkpler.loc['EoS']
        EoS.reset_index(inplace=True)
        EoS=EoS[['Date','CountsWater','CountsDelivered']] #new DB columns count=CountsWater+CountsDelivered
        EoS['Count']=EoS['CountsWater']+EoS['CountsDelivered']
        EoS=EoS[['Date','Count']]
        EoS=EoS.groupby(by='Date').sum()
        EoS = -EoS
        #print(EoS)
        WoS = dfkpler.loc['WoS']
        WoS.reset_index(inplace=True)
        WoS=WoS[['Date','CountsWater','CountsDelivered']]
        WoS['Count']=WoS['CountsWater']+WoS['CountsDelivered']
        WoS=WoS[['Date','Count']]
        WoS=WoS.groupby(by='Date').sum()
        
        return EoS, WoS
        
    def getprice():
        
        '''price data'''
        #JKM
        platts=DBTOPD('PRD-DB-SQL-208','Scrape', 'dbo', 'ReutersActualPrices')
        dbplatts=platts.get_platts_index()
        dbplatts=dbplatts[['Timestamp','Close']]
        dbplatts['Timestamp']=pd.to_datetime(dbplatts['Timestamp'])
        dbplatts.set_index('Timestamp', inplace=True)
        #print(dbplatts)
        #TTF
        dbprice=DBTOPD('PRD-DB-SQL-208','Scrape', 'dbo', 'ReutersActualPrices')
        dbTTF=dbprice.get_price()
        dbTTF.set_index('RIC', inplace=True)
        #print(dbTTF)
        dfmc1=dbTTF.loc['TRNLTTFMc1']
        dfmc1.set_index('Timestamp',inplace=True)
        dfmc1=dfmc1['Close']
        #print(dfmc1)
        dfmc2=dbTTF.loc['TRNLTTFMc2']
        dfmc2.set_index('Timestamp', inplace=True)
        dfmc2=dfmc2['Close']
        #print(dfmc2)
        #FX
        from_ccy = 'EUR'
        to_ccy = 'USD'
        start_date = datetime.datetime(2017,10, 1)
        end_date = datetime.date.today()
        
        url = 'http://prd-alp-app-13:18080/frmService/rest/getFxRateHistoricStrip'
        
       
        load = {    'startDate': start_date.strftime('%d-%b-%Y'),
                    'endDate': end_date.strftime('%d-%b-%Y'), 
                    'fromCcy': from_ccy,
                    'toCcy': to_ccy,
                    'justSpot':'true'
                }

        r = requests.get(url, params=load)
        
        if r.status_code == 200:
            df = pd.read_json(r.text)
        else:
            raise Exception('Error: request failed with status code ' + str(r.status_code) + '. Reason: ' + r.reason)
        
        dffx=df[['quoteDate','fxValue']].copy()
        dffx['quoteDate']=pd.to_datetime(dffx['quoteDate']).copy()
        dffx.set_index('quoteDate', inplace=True)
        #print(dffx.index)
        dfprice=pd.DataFrame(dfmc1.values, index=dfmc1.index, columns=(['Mc1']))
        dfprice['Mc2']=dfmc2
        dfprice['FX']=dffx
        dfprice['Mc1mmbtu']=dfprice['Mc1']/dfprice['FX']/3.412
        dfprice['Mc2mmbtu']=dfprice['Mc2']/dfprice['FX']/3.412
        dfprice['JKM']=dbplatts['Close']
        dfprice['TTFM+2-JKM']=dfprice['Mc2mmbtu'] - dfprice['JKM']
        #print(dfprice)
        #df spead analysis
        dfspread=pd.DataFrame(index=pd.date_range(start='2018-05-01', end=datetime.date.today()-datetime.timedelta(days=1)))
        dfspread['M+2 TTF-JKM'] = dfprice['TTFM+2-JKM']
        dfspread['FWD Spread'] = dfspread['M+2 TTF-JKM']
        dfspread.fillna(0, inplace=True)
        dfspread['M+2 15 Day AV'] = dfspread['M+2 TTF-JKM'].rolling(15).mean()
        dfspread['FWD 15 Day Av'] = dfspread['FWD Spread'].rolling(15).mean()
        #dfspread['Fixed Spread/Forecast Line Cut-Off'] = dfspread['M+2 15 Day AV']
        
        
        #print(dfspread)
        return dfspread
        
    
    def createdf(dfspread, EoS, WoS):     
        #print(dfspread.loc[:datetime.datetime.today().date()].tail(30))
        #print(dfspread.loc[:'2020-03-05'].tail(30))
        '''chart data'''
        dfchart=pd.DataFrame(index=pd.date_range(start='2017-10-01',end='2023-12-31'), columns=['Net WoS','Net EoS'])  
        dfchart[['Net Cargo WoS Y-1','Net Cargo EoS Y-1']]=np.nan
        for i in dfchart.index:
            try:
                if EoS.loc[i,'Count']+WoS.loc[i,'Count']>=0:
                    dfchart['Net WoS'].loc[i]=EoS.loc[i,'Count']+WoS.loc[i,'Count']
                    #dfchart['Net Cargo WoS Y-1'].loc[i]=EoS.loc[str(i.year-1)+'-'+str(i.month)+'-'+str(i.day),'Count']+WoS.loc[str(i.year-1)+'-'+str(i.month)+'-'+str(i.day),'Count']
                else:
                    dfchart['Net EoS'].loc[i]=EoS.loc[i,'Count']+WoS.loc[i,'Count']
                    #dfchart['Net Cargo EoS Y-1'].loc[i]=EoS.loc[str(i.year-1)+'-'+str(i.month)+'-'+str(i.day),'Count']+WoS.loc[str(i.year-1)+'-'+str(i.month)+'-'+str(i.day),'Count']
            except KeyError:
                #print('{} is not in the index'.format(i))
                continue
        
        for j in dfchart.index:
            try:
                if EoS.loc[str(j.year-1)+'-'+str(j.month)+'-'+str(j.day),'Count']+WoS.loc[str(j.year-1)+'-'+str(j.month)+'-'+str(j.day),'Count']>=0:
                    
                    dfchart['Net Cargo WoS Y-1'].loc[j]=EoS.loc[str(j.year-1)+'-'+str(j.month)+'-'+str(j.day),'Count']+WoS.loc[str(j.year-1)+'-'+str(j.month)+'-'+str(j.day),'Count']
                else:
                   
                    dfchart['Net Cargo EoS Y-1'].loc[j]=EoS.loc[str(j.year-1)+'-'+str(j.month)+'-'+str(j.day),'Count']+WoS.loc[str(j.year-1)+'-'+str(j.month)+'-'+str(j.day),'Count']
            except KeyError:
                #print('{} js not jn the jndex'.format(j))
                continue
        
        #dfchart[['Net Cargo WoS Y-1','Net Cargo EoS Y-1']].loc['2018-01-01':]=dfchart[['Net WoS','Net EoS']].loc[str(dfchart.index.year -1)+'-'+str(dfchart.index.month)+'-'+str(dfchart.index.day)]
        dfchart['Fixed Spread/Forecast Line Cut-Off'] = dfspread['M+2 15 Day AV']
        dfchart['TTF-JKM 15 Day fwd avg.'] = dfspread['FWD 15 Day Av']
        dfchart['Today'] = np.nan
        dfchart['Today'].loc[datetime.datetime.today()]=-10
        dfchart['Net Cargo Forecast'] = (dfspread['FWD 15 Day Av']*6.76-4.9).shift(periods=30)
        dfchart['M+2 TTF-JKM 15 Day fwd avg.'] = dfspread['FWD 15 Day Av'].shift(periods=30)
        dfchart['Previous view'] =0#EoS.loc[datetime.datetime.today().date()- datetime.timedelta(days = 1),'Count']+WoS.loc[datetime.datetime.today().date()- datetime.timedelta(days = 1),'Count']
        #print(dfchart.loc[:datetime.datetime.today().date()])
        dfprevious=pd.read_excel('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/SuezPrevious.xlsx',header=(0),sheet_name=0)
        dfprevious.set_index('date', inplace=True)
        dfchart['Previous view'] = dfprevious['Previous view']
        #dfchart['Previous view']=dfchart['Previous view'].shift(periods=17)
        return dfchart
    
    def Suez_chart(dfchart):
        #print(dfchart.loc[:'2021-03-05'].tail(30))
        dfchart = dfchart.loc['2021-01-01':'2021-12-31']
        #fig = go.Figure() 
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        fig.add_trace(
               go.Scatter(
                   x=dfchart.index,
                   y=dfchart['Net WoS'],
                   fill='tozeroy',
                   fillcolor='rgba(255,0,0,0.5)',
                   line_color='rgba(255,0,0,0.5)',
                   showlegend=True,
                   name='Net WoS'
                   )
               )
        fig.add_trace(
               go.Scatter(
                   x=dfchart.index,
                   y=dfchart['Net EoS'],
                   fill='tozeroy',
                   fillcolor='rgba(237,125,49,0.5)',
                   line_color='rgba(237,125,49,0.5)',
                   showlegend=True,
                   name='Net EoS'
                   )
               )
        fig.add_trace(
               go.Scatter(
                   x=dfchart.index,
                   y=dfchart['Net Cargo WoS Y-1'],
                   fill='tozeroy',
                   fillcolor='rgba(165,165,165,0.5)',
                   line_color='rgba(165,165,165,0.5)',
                   showlegend=True,
                   name='Net Cargo WoS Y-1'
                   )
               )
        fig.add_trace(
               go.Scatter(
                   x=dfchart.index,
                   y=dfchart['Net Cargo EoS Y-1'],
                   fill='tozeroy',
                   fillcolor='rgba(255,192,0,0.5)',
                   line_color='rgba(255,192,0,0.5)',
                   showlegend=True,
                   name='Net Cargo EoS Y-1'
                   )
               )
        fig.add_trace(
               go.Scatter(
                   x=[datetime.datetime.today().date()+datetime.timedelta(days = 45),datetime.datetime.today().date()+datetime.timedelta(days = 45)],
                   y=[30,-100],
                   #fill='tozeroy',
                   #fillcolor='rgba(99,99,99,1)',
                   #line_color='rgba(99,99,99,1)',
                   line=dict(color='#636363', width=2, dash='solid'),
                   mode="lines",
                   #line=dict(color='black', width=2, dash='dash'),
                   showlegend=True,
                   name='Fixed Spread/Forecast Line Cut-Off'
                   ),#secondary_y=True,
               )
        fig.add_trace(
               go.Scatter(
                   x=[datetime.datetime.today().date(),datetime.datetime.today().date()],
                   y=[30,-100],
                   mode="lines",
                   line=dict(color='red', width=2, dash='solid'),
                   showlegend=True,
                   name='Today'
                   )
               )
        fig.add_trace(
               go.Scatter(
                   x=dfchart.index,
                   y=dfchart['Net Cargo Forecast'],
                   mode="lines",
                   line=dict(color='black', width=2, dash='dash'),
                   showlegend=True,
                   name='Net Cargo Forecast'
                   )#,secondary_y=True,
               )
        fig.add_trace(
               go.Scatter(
                   x=dfchart.index,
                   y=dfchart['Previous view'],
                   mode="lines",
                   line=dict(color='red', width=2, dash='dot'),
                   showlegend=True,
                   name='Previous view'
                   )#,secondary_y=True,
               )
        fig.add_trace(
               go.Scatter(
                   x=dfchart.index,
                   y=dfchart['M+2 TTF-JKM 15 Day fwd avg.'],
                   mode="lines",
                   line=dict(color='#00B0F0', width=2, dash='dash'),
                   showlegend=True,
                   name='M+2 TTF-JKM 15 Day fwd avg.'
                   ),secondary_y=True,
               )
        fig.update_layout(
            autosize=True,
            showlegend=True,
            legend=dict(x=0, y=-0.2),
            legend_orientation="h",
            title_text='Net Cargo Direction from Key Liquefaction Plants',
            yaxis_title="                                     EOS Headed Cargoes                                            WOS Headed Cargoes",
            xaxis = dict(dtick="M1"),
            hovermode='x unified',
            plot_bgcolor = 'white',
            template='ggplot2'  
        )
        fig.update_xaxes(
            showline=True, 
            linewidth=1, 
            linecolor='LightGrey', 
            showgrid=True, 
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
        fig.update_yaxes(title_text="M+2 TTF-JKM ($/MMBtu)", secondary_y=True)

           
        py.plot(fig, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/Suez regression/SuezRegression.html', auto_open=False)
        fig.write_image('U:/Trading - Gas/LNG/LNG website/analysis/Suez regression/SuezRegression.png')
        
        
    def get_kpler_data():
        
        Kpler=DBTOPD('PRD-DB-SQL-211','LNG', 'dbo', 'KplerLNGTrades')
        dfkpler=Kpler.sql_to_df()
        
        #Kplarsuez=DBTOPD('PRD-DB-SQL-208','Scrape', 'dbo', 'KplerLNGTrades')
        Kplarsuez=DBTOPD('PRD-DB-SQL-211','LNG', 'dbo', 'KplerLNGTrades')
        dfsuez=Kplarsuez.get_suez()
        #print(dfsuez)
        
        deskplant=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'BasinSupply')
        dfdeskplant=deskplant.sql_to_df()
        
        deskbasin=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'BasinDemand')
        dfdeskbasin=deskbasin.sql_to_df()
        
        return dfkpler,dfsuez,dfdeskplant,dfdeskbasin
        

    def get_supply_data(dfkpler,dfdeskplant):
        
        start_date='2018-01-01' 
        today=datetime.date.today()
        
        
        curveid = DBTOPD.get_curve_id()
        dfcurveid = curveid.loc[:,['CurveId','Type','Location','Country']]
        
        SupplyCurveId = dfcurveid.loc[dfcurveid[dfcurveid['Type']=='Supply'].index]
        SupplyCurveId = SupplyCurveId[['CurveId','Country','Location']]
        columns = SupplyCurveId['Location'].to_list()
        #print(columns)
        #columns=['APLNG',	'Atlantic LNG','Balhaf','Bethioua','Bintulu','Bioko','Bonny LNG','Bontang',	'Cameron (Liqu.)',	'Cameroon FLNG Terminal','Corpus Christi','Cove Point','Damietta','Darwin','Das Island','DSLNG','Elba Island Liq.','Freeport','GLNG','Gorgon','Ichthys','Idku','Kollsnes','Lumut I','Marsa El Brega',
#'Coral South FLNG',	'NWS',	'Peru LNG',	'PFLNG 1 Sabah',	'PFLNG 2',	'Pluto',	'PNG LNG',	'Prelude',	'Qalhat',	'QCLNG',	'Ras Laffan',	'Sabine Pass',	'Sakhalin 2'	,'Senkang',	'Skikda','Snohvit',	'Soyo'	,'Stavanger'	,'Tangguh','Tango FLNG',	'Vysotsk',	'Wheatstone',	'Yamal','Portovaya']
        columns.remove('Kollsnes')
        columns.remove('Greater Tortue FLNG')
        columns.remove('PFLNG 2')
        columns.remove('Senkang')
        columns.remove('Stavanger')
        columns.remove('Vysotsk')
        
        dfdeskplant.set_index('Date', inplace=True)
        dfdeskplant=dfdeskplant.resample('MS').mean()
        dfdeskplant=dfdeskplant.loc[start_date:]
        
        dfsupplyfull=pd.DataFrame(index=pd.date_range(start=start_date, end=str(today.year+1)+'-12-31', freq='MS'), columns=columns)
        for i in dfsupplyfull.columns:
            dfsupplyfull[i] = dfdeskplant[i+' Desk']
        
        return dfsupplyfull#dfsupplycargo
    
    def get_demand_data(dfkpler,dfdeskbasin):
       
        start_date='2018-01-01' 
        today=datetime.date.today()
        columns=['Australia','Bahrain','Bangladesh','Belgium','Brazil','Canada','Chile','China','Colombia','Croatia','Cyprus','Dominican Republic',	'Egypt','Finland','France',	'Ghana','Greece','India','Indonesia','Israel','Italy','Jamaica','Japan','Jordan','Kuwait','Lithuania','Malaysia','Malta','Mexico','Myanmar','Netherlands','Norway','Oman','Pakistan','Panama','Philippines','Poland','Portugal','Puerto Rico','Singapore Republic',	'South Korea','Spain','Sweden','Taiwan','Thailand',	'Turkey','United Arab Emirates','United Kingdom','United States','Uruguay','Vietnam']
        
        dfdeskbasin.set_index('Date', inplace=True)
        #dfdeskbasin=dfdeskbasin.resample('MS').mean()
        dfdeskbasin=dfdeskbasin.resample('MS').sum()
        #dfdemandmcm=dfdemand.copy()
        for j in dfdeskbasin.index:
            days=calendar.monthrange(j.year,j.month)
            dfdeskbasin.loc[j]=dfdeskbasin.loc[j]/days[1]
        
        dfdeskbasin=dfdeskbasin.loc[start_date:]
        
        dfdemandfull=pd.DataFrame(index=pd.date_range(start=start_date, end=str(today.year+1)+'-12-31', freq='MS'), columns=columns)
        for i in dfdemandfull.columns:
            dfdemandfull[i] = dfdeskbasin[i+' Desk']
            
        #print(dfdemandfull.loc['2021-07-01',['Australia','China','Japan','Indonesia','Malaysia','Myanmar','Mexico','Singapore Republic','South Korea','Taiwan','Thailand']])
        return dfdemandfull
    
    def kpler_flex_data(dfkpler,dfsuez):
        
        
        SupplyCategories=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'CategoriesSupply').sql_to_df()
        drop_list_supply = ['Senkang', 'Greater Tortue FLNG']
        for i in drop_list_supply:
            SupplyCategories.drop(SupplyCategories[SupplyCategories['Plant'] == i].index, inplace=True)
            
        DemandCategories=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'CategoriesDemand').sql_to_df()
        droplist = ['Algeria','Angola','Bahamas','Brunei','Cameroon','Equatorial Guinea','Libya','Nigeria','Papua New Guinea','Peru','Philipines','Qatar','Russian Federation','Trinidad and Tobago','Yemen']
        for i in droplist:
            DemandCategories.drop(DemandCategories[DemandCategories['Country']==i].index, inplace=True)
        DemandCategories.set_index('Country', inplace=True)
        DemandCategories=DemandCategories['Suez']
        suezdict=DemandCategories.to_dict()
        #print(dfkpler) 
        
        
        #get supply and demand df
        supply_plant_columns=['StartDestination','InstallationOrigin','CountryDestination','VolumeOriginM3']
        df_supply_plant = dfkpler[supply_plant_columns]
        plant_list=SupplyCategories['Plant'].tolist()
        #plant_list.remove('Mozambique Area 1')
        #plant_list.remove('Calcasieu Pass')
        #print(plant_list)
        df_supply_plant.set_index('InstallationOrigin', inplace=True)
        df_supply_plant = df_supply_plant.loc[plant_list]
        df_supply_plant['MonthDelivered'] = df_supply_plant['StartDestination'].dt.month 
        df_supply_plant['Suez'] = df_supply_plant['CountryDestination'].replace(suezdict)
        #print(df_supply_plant)
        df_supply_plant.reset_index(inplace=True)
        
        dfsuez.fillna(0, inplace=True)
        EOS = dfsuez.loc[dfsuez[dfsuez['Suez']=='EoS'].index]
        EOS = EOS[['InstallationOrigin','MonthDelivered','SumVolDelivered']]
        EOSgroup=EOS.groupby(['InstallationOrigin','MonthDelivered'], as_index=False).sum()
        EOSpivot=pd.pivot(EOSgroup, index='InstallationOrigin', columns='MonthDelivered')
        EOSpivot.fillna(0, inplace=True)
        #print(EOSpivot)
        WOS = dfsuez.loc[dfsuez[dfsuez['Suez']=='WoS'].index]
        WOS = WOS[['InstallationOrigin','MonthDelivered','SumVolDelivered']]
        WOSgroup=WOS.groupby(['InstallationOrigin','MonthDelivered'], as_index=False).sum()
        WOSpivot=pd.pivot(WOSgroup, index='InstallationOrigin', columns='MonthDelivered')
        WOSpivot.fillna(0, inplace=True)
        #print(WOSpivot)
        df_flex=EOSpivot/(EOSpivot+WOSpivot)
        
        df_flex.columns=df_flex.columns.droplevel(0)
        #print(df_flex)
        
        return df_flex
        
    def on_water():
        
        Kplar=DBTOPD('PRD-DB-SQL-211','LNG', 'dbo', 'OutputsCargoTrackingBreakdownCounts')
        onwater=Kplar.sql_to_df()
        onwater.set_index('InstallationOrigin', inplace=True)
        try:
            onwater=onwater.loc[['Atlantic LNG','Bioko','Bonny LNG','Cameron (Liqu.)','Cameroon FLNG Terminal','Corpus Christi','Cove Point','Elba Island Liq.','Freeport','Sabine Pass','Snohvit','Soyo','Yamal','Idku','Damietta','Bethioua','Skikda']]
        except KeyError as e:
            print('On water error: ',e)
        onwater=onwater[['Date','SuezDestination','CountsWater']]
        onwater.set_index('SuezDestination', inplace=True)
        onwater=onwater.loc['EoS']
        #onwater=onwater.loc[onwater[onwater['SuezDestination']=='EoS'].index]
        onwater.set_index('Date', inplace=True)
        
        onwatercount=onwater.loc[str(datetime.date.today()-datetime.timedelta(days=1))].sum()
        #print(onwater)
        return onwatercount
     
    def hist():
        today = datetime.date.today()
        
        hist_list=['Idku','Damietta','Bethioua','Skikda','Atlantic LNG','Bioko','Bonny LNG','Cameron (Liqu.)','Cameroon FLNG Terminal','Corpus Christi','Cove Point','Elba Island Liq.','Freeport','Sabine Pass','Snohvit','Soyo','Yamal']

        #DemandCategories = pd.read_excel('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/Categories.xlsx',header=(0),sheet_name='Index_Demand')
        #DemandCategories = DemandCategories.iloc[:64,0:6]
        DemandCategories=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'CategoriesDemand').sql_to_df()
        droplist = ['Algeria','Angola','Bahamas','Brunei','Cameroon','Equatorial Guinea','Libya','Nigeria','Papua New Guinea','Peru','Philipines','Qatar','Russian Federation','Trinidad and Tobago','Yemen']
        for i in droplist:
            DemandCategories.drop(DemandCategories[DemandCategories['Country']==i].index, inplace=True)
        DemandCategories.set_index('Country', inplace=True)
        DemandCategories=DemandCategories['Suez']
        suezdict=DemandCategories.to_dict()
        
        
        Kplar=DBTOPD('PRD-DB-SQL-211','LNG', 'dbo', 'KplerLNGTrades')
        hist=Kplar.get_dash_hist()
        hist.set_index('InstallationOrigin', inplace=True) 
        #get hist table plant hist
        hist=hist.loc[hist_list]
        #get EoS
        hist['Suez'] = hist['CountryDestination'].replace(suezdict)
        hist.reset_index(inplace=True)
        hist=hist.loc[hist[hist['Suez']=='EoS'].index]
        #print(hist['YearDelivered'])
        #set date
        hist['Date']=hist['YearDelivered'].astype(str).str.cat(hist['MonthDelivered'].astype(str), sep='-')
        
        #print(hist)
        #get df hist
        dfhist=pd.DataFrame(index=['History'], columns=pd.date_range(start='2020-11-01', end=today, freq='MS'))
        for i in dfhist.columns:
            dfhist[i]=hist.loc[hist[hist['Date']==str(i.year)+'-'+str(i.month)].index, 'NumShips'].sum()
        #print(dfhist)
        return dfhist
    
    def jkmttf():
        
        today = datetime.date.today()
        
        dfall=pd.DataFrame(index=pd.date_range(start='2020-11-01',end='2024-03-01', freq='MS'))
        
        Kplar=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'BBGJKMSPOTTTFMA1')
        jkmttf=Kplar.sql_to_df()
        jkmttf.set_index('date', inplace=True)
        dfjkmttf=jkmttf.loc['2020-11-01':, 'value']
        dfjkmttf=dfjkmttf.resample('MS').mean()
        #print(dfjkmttf)
        
        dfall['Regression JKM-TTF M+2'] = dfjkmttf*5.895823357+51.58886525
        #print(dfall)
        
        curve =DBTOPD('PRD-DB-SQL-208','Scrape', 'dbo', 'CurveForwardPrice')
        dfcurve =curve.get_curve()
        dfcurve = dfcurve[['CURVE_ID','CURVE_DT','PRICE']]
        
        dfjkmcurve = dfcurve.loc[dfcurve[dfcurve['CURVE_ID']=='JKM FWD'].index]
        dfjkmcurve = dfjkmcurve[['CURVE_DT','PRICE']]
        dfjkmcurve.set_index('CURVE_DT', inplace=True)
        dfttfcurve = dfcurve.loc[dfcurve[dfcurve['CURVE_ID']=='TTF FWD'].index]
        dfttfcurve = dfttfcurve[['CURVE_DT','PRICE']]
        dfttfcurve.set_index('CURVE_DT', inplace=True)
        
        dfjkmttfcurve = dfjkmcurve - dfttfcurve/3.412
        #print(dfjkmttfcurve.columns)
        dfjkmttfcurve = dfjkmttfcurve.rename(columns={'PRICE':'value'})
        #print(dfjkmttfcurve.loc[str(today.year)+'-'+str(today.month+1)+'-01':,'PRICE'])
        
        dfall['JKMTTF (Platts)'] = jkmttf.loc['2020-11-01':, 'value'].resample('MS').mean()
        dfall['JKMTTF (Mark)'] = dfjkmttfcurve.loc[str((today+relativedelta(months=1)).year)+'-'+str((today+relativedelta(months=1)).month)+'-01':,'value']
        dfall['JKMTTF (Mark)'].loc[str(today.year)+'-'+str(today.month)+'-01'] = dfall['JKMTTF (Platts)'].loc[str(today.year)+'-'+str(today.month)+'-01']
        dfall['Regression JKM-TTF M+2'].loc[str((today+relativedelta(months=1)).year)+'-'+str((today+relativedelta(months=1)).month)+'-01':] = dfall['JKMTTF (Mark)'].loc[str((today+relativedelta(months=1)).year)+'-'+str((today+relativedelta(months=1)).month)+'-01':]*5.895823357+51.58886525
        #print(dfall)
        
        return dfall
    
    def regress_model():
        
        df=pd.DataFrame(index=range(1,13), columns=range(2017, 2025))
        df.loc[1]=[0,89.79718666,91.15659185,94.29790386,116.2763958,184.1557049,185.2585921,186.035954]
        df.loc[2]=[0,	74.40775402,	70.04908056,	78.70022566,	101.7938027,	161.6189656,	163.2231612,	168.2870799]
        df.loc[3]=[77.09902695,	90.10441216,	83.78358338,	87.86848347,	109.2433895,	146.7546457,	162.0761099,	101.5545201]
        df.loc[4]=[61.03755198,	75.15883915,	76.22217645,	78.32346585,	96.80491297,	139.9571867,	156.2273956,	0]
        df.loc[5]=[66.36390307,	70.382935,	74.81550013,	71.37232291,	84.95262814,	148.8588515,	161.1659789,	0]
        df.loc[6]=[68.27763658,	74.32330618,	76.14222089,	93.36246733,	111.2527246,	148.2865894,	152.4061718,	0]
        df.loc[7]=[64.85484821,	79.62004584,	89.9018532,	87.80545049,	102.1989463,	158.4856215,	157.1500821,	0]
        df.loc[8]=[73.61527307,	80.83433747,	78.17454794,	83.60387806,	105.6275969,	162.0481429,	159.1339815,	0]
        df.loc[9]=[65.83187867,	82.1422001,	85.15936881,	94.5965646,	16.41177701,	168.9510058,	158.7839076,	0]
        df.loc[10]=[78.68099488,	94.20754028,	93.35724944,	102.7947789,	154.2849884,	177.9778811,	169.70094,	0]
        df.loc[11]=[85.5737321,	82.5834628,	87.26783243,	99.71624685,	175.0419572,	176.6776215,	170.4740367,	0]
        df.loc[12]=[80.55689843,	72.13712734,	83.79752928,	114.7055367,	185.7343644,	183.799081,	180.6410438,	0]
        #print(df)
        return df


    def desh_data(dfsupplycargo, dfdemandcargo, df_flex, dfhist, onwatercount,dfjkmttf, dfregression):
        
        today = datetime.date.today()
        '''get dash hist supply table'''
        df_flex = df_flex.T
        ABtoEOSlist=['Atlantic LNG','Bioko','Bonny LNG','Cameron (Liqu.)','Cameroon FLNG Terminal','Corpus Christi','Cove Point','Elba Island Liq.','Freeport','Sabine Pass','Snohvit','Soyo','Yamal']
        dfABtoEOScargo = pd.DataFrame(index=ABtoEOSlist, columns=dfsupplycargo.index)
        #print(dfABtoEOScargo['2021-08-01'])
        for i in dfABtoEOScargo.columns:

            dfABtoEOScargo.loc[ABtoEOSlist,i] = dfsupplycargo[ABtoEOSlist].loc[i]*df_flex.loc[i.month,ABtoEOSlist]

        dfABtoEOScargo.loc['AB to EoS']=dfABtoEOScargo.loc[['Atlantic LNG','Bioko','Bonny LNG','Cameroon FLNG Terminal','Snohvit','Yamal']].sum()
        dfABtoEOScargo.loc['US']=dfABtoEOScargo.loc[['Cameron (Liqu.)','Corpus Christi','Cove Point','Elba Island Liq.','Freeport','Sabine Pass']].sum()
        dfABtoEOScargo.loc['US Contracts Asia'] = dfABtoEOScargo.loc['US']*0.2
        #print(dfABtoEOScargo.loc[['Cameron (Liqu.)','Corpus Christi','Cove Point','Elba Island Liq.','Freeport','Sabine Pass'],'2021-07-01'])
        MENAtoEOSlist=['Ras Laffan','Idku','Damietta','Bethioua','Skikda','Marsa El Brega','Qalhat','Das Island']
        dfMENAtoEOScargo = pd.DataFrame(index=MENAtoEOSlist, columns=dfsupplycargo.index)
        for j in dfMENAtoEOScargo.columns:

            dfMENAtoEOScargo.loc[MENAtoEOSlist,j] = dfsupplycargo[MENAtoEOSlist].loc[j]*df_flex.loc[j.month,MENAtoEOSlist]
        
        dfMENAtoEOScargo.loc['MENA to EoS']=dfMENAtoEOScargo.loc[['Ras Laffan','Marsa El Brega','Qalhat','Das Island']].sum()
        #print(dfMENAtoEOScargo['2021-07-01'])
        
        '''dash supply basin'''
        #SupplyCategories = pd.read_excel('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/Categories.xlsx',header=(0),sheet_name='Index_Supply')
        #SupplyCategories = SupplyCategories.iloc[:44,0:5]
        SupplyCategories=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'CategoriesSupply').sql_to_df()
        SupplyCategories.set_index('Plant', inplace=True)
        SupplyCategories=SupplyCategories['Basin']
        supplybasindict=SupplyCategories.to_dict()
        Malaysia=['Bintulu','PFLNG 1 Sabah']
        Australia=['APLNG','Darwin','GLNG','Gorgon','NWS','Pluto','QCLNG','Wheatstone','Ichthys','Prelude']
        dfsupplybasincargo=dfsupplycargo.copy().rename(columns=supplybasindict)
        dfdashsupply=pd.DataFrame(index=['PB Other','Malaysia','Australia'], columns=pd.date_range(start='2020-11-01', end=str(today.year+1)+'-12-01', freq='MS'))
        #print(dfsupplycargo)
        dfdashsupply.loc['Malaysia']=dfsupplycargo[Malaysia].sum(axis=1)
        dfdashsupply.loc['Australia']=dfsupplycargo[Australia].sum(axis=1)
        dfdashsupply.loc['PB Other']=dfsupplybasincargo['PB'].sum(axis=1)-dfsupplycargo[Malaysia].sum(axis=1)-dfsupplycargo[Australia].sum(axis=1)
        #print(dfdashsupply)
        
        '''get dash basin'''
        #DemandCategories = pd.read_excel('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/Categories.xlsx',header=(0),sheet_name='Index_Demand')
        #DemandCategories = DemandCategories.iloc[:64,0:6]
        DemandCategories=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'CategoriesDemand').sql_to_df()
        droplist = ['Algeria','Angola','Bahamas','Brunei','Cameroon','Equatorial Guinea','Libya','Nigeria','Papua New Guinea','Peru','Philipines','Qatar','Russian Federation','Trinidad and Tobago','Yemen']
        for i in droplist:
            DemandCategories.drop(DemandCategories[DemandCategories['Country']==i].index, inplace=True)
        DemandCategories.set_index('Country', inplace=True)
        DemandCategories=DemandCategories['Basin']
        demandbasindict=DemandCategories.to_dict()
        demandbasindict['Chile']='AB'
        #print(demandbasindict)
        dfdemandbasincargo=dfdemandcargo.copy().rename(columns=demandbasindict)
        #print(dfdemandbasincargo)
        dfdashbasin=pd.DataFrame(index=['PB Demand','MENA Demand','PB Supply','Firm (EoS + WoS)'], columns=pd.date_range(start='2020-11-01', end=str(today.year+1)+'-12-01', freq='MS'))
        dfdashbasin.loc['PB Demand'] = dfdemandbasincargo.loc['2020-11-01':,'PB'].sum(axis=1)
        dfdashbasin.loc['MENA Demand'] = dfdemandbasincargo.loc['2020-11-01':,'MENA'].sum(axis=1)
        dfdashbasin.loc['PB Supply'] = dfdashsupply.sum()
        dfdashbasin.loc['Firm (EoS + WoS)']=dfMENAtoEOScargo.loc['MENA to EoS'] + dfABtoEOScargo.loc['AB to EoS'] + dfABtoEOScargo.loc['US Contracts Asia']
        #print(dfMENAtoEOScargo.loc['MENA to EoS'], dfABtoEOScargo.loc['AB to EoS'], dfABtoEOScargo.loc['US Contracts Asia'])
        '''get dash cargos table for chart 1'''
        dfdashcargos = pd.DataFrame(index=['US Contracts Asia','AB non US','N Africa Flex','WoS Price','Desk','History'], columns=pd.date_range(start='2020-11-01', end=str(today.year+1)+'-12-01', freq='MS'))
        dfdashcargos.loc['US Contracts Asia'] = dfABtoEOScargo.loc['US Contracts Asia']/3
        dfdashcargos.loc['AB non US'] = dfABtoEOScargo.loc['AB to EoS']/3
        dfdashcargos.loc['N Africa Flex'] = dfMENAtoEOScargo.loc[['Idku','Damietta','Bethioua','Skikda']].sum()/3
        for i in dfdashcargos.columns:
            dfdashcargos.loc['Regression Model', i] =  dfregression.loc[i.month, i.year]

            if dfdashbasin.loc['PB Demand',i] + dfdashbasin.loc['MENA Demand',i] -dfdashbasin.loc['PB Supply',i]-dfdashbasin.loc['Firm (EoS + WoS)',i] < 0:
                dfdashcargos.loc['WoS Price',i] = np.nan
            else:
                dfdashcargos.loc['WoS Price',i] = (dfdashbasin.loc['PB Demand',i] + dfdashbasin.loc['MENA Demand',i] -dfdashbasin.loc['PB Supply',i]-dfdashbasin.loc['Firm (EoS + WoS)',i])/3
        #print(dfdashcargos.loc['WoS Price'])
        dfdashcargos.loc['Desk'] = dfdashcargos.loc[['US Contracts Asia','AB non US','N Africa Flex','WoS Price']].sum()
        dfdashcargos.loc['OnWater'] = 0
        dfdashcargos.loc['OnWater', str(today.year)+'-'+str(today.month)+'-01']=onwatercount.values
        dfdashcargos.loc['History'] = dfhist.loc['History'] + dfdashcargos.loc['OnWater']
        dfdashcargos.loc['History',str((today+relativedelta(months=1)).year)+'-'+str((today+relativedelta(months=1)).month)+'-01':]=np.nan
        dfdashcargos.loc['Regression JKM-TTF M+2'] = dfjkmttf['Regression JKM-TTF M+2']
        dfdashcargos.loc['Delta Desk vs Kpler'] = dfdashcargos.loc['History'] - dfdashcargos.loc['Desk']
        dfdashcargos.loc['Delta Desk vs Kpler',str((today+relativedelta(months=1)).year)+'-'+str((today+relativedelta(months=1)).month)+'-01':]=np.nan
        dfdashcargos.loc['JKM-TTF (Platts)'] = dfjkmttf['JKMTTF (Platts)']
        dfdashcargos.loc['JKM-TTF (Mark)'] = dfjkmttf['JKMTTF (Mark)']
         
        
        dfdashcargos = dfdashcargos.astype(float).round()
        
        #print(dfdashcargos['2021-08-01'])
        
        return dfdashcargos
    
    def dash_chart(dfdashcargos):
        
        #chart 1
        #a=datetime.datetime.strptime('2021-11-30', '%Y-%m-%d')
        #b=datetime.datetime.strptime('2021-12-01', '%Y-%m-%d')
        #linedate = a + (b - a)/2
        #linedate='2021-11-30'#str(datetime.date.today().year)+'-'+str(datetime.date.today().month)+'-01'
        today = datetime.date.today()
        
        figchart1 = go.Figure()
        figchart1.add_trace(
            go.Bar(
                x=dfdashcargos.columns,
                y=dfdashcargos.loc['US Contracts Asia'],
                name='US Contracts Asia',
                text=dfdashcargos.loc['US Contracts Asia'],
                marker_color=['#44546A',] *dfdashcargos.shape[1] ,
                textposition='auto' 
                ))
        figchart1.add_trace(
            go.Bar(
                x=dfdashcargos.columns,
                y=dfdashcargos.loc['WoS Price'],
                name='WoS Price',
                text=dfdashcargos.loc['WoS Price'],
                marker_color=['#FFC000',] *dfdashcargos.shape[1], 
                textposition='auto'
                ))
        figchart1.add_trace(
            go.Bar(
                x=dfdashcargos.columns,
                y=dfdashcargos.loc['AB non US'],
                name='AB non US',
                text=dfdashcargos.loc['AB non US'],
                marker_color=['#7030A0',] *dfdashcargos.shape[1],
                textposition='auto'
                ))
        figchart1.add_trace(
            go.Bar(
                x=dfdashcargos.columns,
                y=dfdashcargos.loc['N Africa Flex'],
                name='N Africa Flex',
                text=dfdashcargos.loc['N Africa Flex'],
                marker_color=['#00B0F0',] *dfdashcargos.shape[1],
                textposition='auto'
                ))
        
        figchart1.add_trace(
            go.Scatter(
                x=dfdashcargos.columns,
                y=dfdashcargos.loc['History'],
                name='History',
                text=dfdashcargos.loc['History'],
                textposition='top center',
                #yaxis="y3",
                mode='lines+markers',
                line = dict(color='red', dash='solid')
            ))
        figchart1.add_trace(
            go.Scatter(
                x=dfdashcargos.columns,
                y=dfdashcargos.loc['Regression JKM-TTF M+2'],
                name='Regression JKM-TTF M+2',
                #yaxis="y3",
                mode='lines+markers',
                line = dict(color='green', dash='dash')
            ))
        figchart1.add_trace(
            go.Scatter(
                x=dfdashcargos.columns,
                y=[30]*dfdashcargos.shape[1],
                name='dot 30',
                #yaxis="y3",
                mode='lines',
                line = dict(color='blue', dash='dot'),
                showlegend=False
            ))
        figchart1.add_trace(go.Scatter(x=[str(today.year)+'-'+str(today.month)+'-01',str(today.year)+'-'+str(today.month)+'-01'],y=[0,120],#[dfdashcargos.min(),dfdashcargos.max()],
                   mode="lines",
                   line=dict(color='red', width=2, dash='dot'),
                   showlegend=False,
                   name='Today'
                   )
               )
        #figchart1.update_layout(uniformtext_minsize=8, uniformtext_mode='show')

        figchart1.update_xaxes(
                                dtick="M1",
                                tickformat="%b\n%Y")
        figchart1.update_yaxes(title_text='Cargos',
                               dtick=10,)
        figchart1.update_layout(barmode='relative',
                                 title="AB to PB cargo forecast",
                                 #shapes = [dict( x0=0.5, x1=0.5, y0=0, y1=1, xref='paper', yref='paper',line_width=2,line=dict(color="MediumPurple",dash="dot"))],
                                 annotations=[dict(x=datetime.datetime.today().date(), y=1, yref='paper',font=dict(color="red",size=14),showarrow=False, xanchor='left', text='Forecasst'),
                                            dict(x=0.95, y=0.5, xref='paper', yref='paper',font=dict(color="red",size=14),showarrow=False, xanchor='left', text='panama congestion')],
                                 #annotations=[dict(x=1, y=0.35, xref='paper', yref='paper',font=dict(color="red",size=14),showarrow=False, xanchor='left', text='panama congestion')],
                                 legend=dict(orientation="h",)
                                 )
        
        py.plot(figchart1, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/Suez regression/AB to PB cargo forecast.html', auto_open=False)
        figchart1.write_image('U:/Trading - Gas/LNG/LNG website/analysis/Suez regression/AB to PB cargo forecast.png')
        #chart 2
        figchart2 = go.Figure()
        figchart2.add_trace(
            go.Bar(
                x=dfdashcargos.columns,
                y=dfdashcargos.loc['Delta Desk vs Kpler'],
                name='Delta Desk vs Kpler',
                text=dfdashcargos.loc['Delta Desk vs Kpler'],
                textposition='auto' 
                ))
        
        figchart2.add_trace(
            go.Scatter(
                x=dfdashcargos.columns,
                y=dfdashcargos.loc['Desk'],
                name='Desk',
                #yaxis="y3",
                mode='lines',
                line = dict(color='black', dash='solid')
            ))
        figchart2.add_trace(
            go.Scatter(
                x=dfdashcargos.columns,
                y=dfdashcargos.loc['History'],
                name='History',
                #yaxis="y3",
                mode='lines+markers',
                line = dict(color='red', dash='solid')
            ))
        figchart2.add_trace(
            go.Scatter(
               x=dfdashcargos.columns,
                y=dfdashcargos.loc['Regression JKM-TTF M+2'],
                name='Regression JKM-TTF M+2',
                #yaxis="y3",
                mode='lines+markers',
                line = dict(color='green', dash='solid')
            ))
        '''
        figchart2.add_trace(
            go.Scatter(
               x=dfdashcargos.columns,
                y=dfdashcargos.loc['Regression Model'],
                name='Regression Model',
                #yaxis="y3",
                mode='lines+markers',
                line = dict(color='gray', dash='solid')
            ))
        '''
        figchart2.add_trace(
            go.Scatter(
               x=dfdashcargos.columns,
                y=dfdashcargos.loc['JKM-TTF (Platts)'],
                name='JKM-TTF (Platts)',
                yaxis="y3",
                mode='lines',
                line = dict(color='blue', dash='solid')
            ))
        figchart2.add_trace(
            go.Scatter(
               x=dfdashcargos.columns,
                y=dfdashcargos.loc['JKM-TTF (Mark)'],
                name='JKM-TTF (Mark)',
                yaxis="y3",
                mode='lines',
                line = dict(color='blue', dash='dash')
            ))
        figchart2.update_xaxes(
                                dtick="M1",
                                tickformat="%b\n%Y")
        figchart2.update_yaxes(title_text='Cargos')
        figchart2.update_layout(barmode='relative',
                                 title="AB to PB cargo forecast",
                                 yaxis3=dict(
                                     title="$/mmbtu",
                                     titlefont=dict(color="#d62728"),
                                     tickfont=dict(color="#d62728"),
                                     anchor="x",
                                     overlaying="y",
                                     side="right",
                                     ),
                                 legend=dict(orientation="h",)
                                 )
        
        py.plot(figchart2, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/Suez regression/AB to PB cargo forecast1.html', auto_open=False)
        figchart2.write_image('U:/Trading - Gas/LNG/LNG website/analysis/Suez regression/AB to PB cargo forecast1.png')
    
    def update():
        a=SuezRegression
        dfspread = a.getprice()
        EoS, WoS  = a.getsuez()
        dfchart = a.createdf(dfspread, EoS, WoS)
        #a.getsuez()
        a.Suez_chart(dfchart)
        
        #print(dfspread)
        #print(EoS, WoS)
        #print(dfchart)
     
        #a=SuezRegression
        dfkpler,dfsuez,dfdeskplant,dfdeskbasin = a.get_kpler_data()
        dfsupplycargo = a.get_supply_data(dfkpler,dfdeskplant)
        dfdemandcargo = a.get_demand_data(dfkpler,dfdeskbasin)
        onwatercount = a.on_water()
        dfjkmttf=a.jkmttf()
        dfhist=a.hist()
        dfregression=a.regress_model()
        df_flex = a.kpler_flex_data(dfkpler,dfsuez)
        dfdashcargos=a.desh_data(dfsupplycargo, dfdemandcargo, df_flex, dfhist, onwatercount, dfjkmttf, dfregression)
        
        #print(dfkpler,dfsuez,dfdeskplant,dfdeskbasin)
        
        a.dash_chart(dfdashcargos)
        
#SuezRegression.update()
