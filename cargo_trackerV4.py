# -*- coding: utf-8 -*-
"""
Created on Mon Oct 18 13:44:48 2021

@author: SVC-GASQuant2-Prod
"""





#v1 fix soyo and weekly fcst to desk view
# V2 change to trade database
# V3 use utilisation instead of average rate
#V4 re use weekly, fix pie chart not all on water, utilisation rate use 3yrs rolling

import pandas as pd
import sqlalchemy
import datetime
import numpy as np
import plotly.graph_objects as go
import plotly.offline as py

import calendar
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.combining import OrTrigger
from apscheduler.triggers.cron import CronTrigger
import sys
sys.path.append('C:\\Users\SVC-GASQuant2-Prod\YWwebsite\Python Scripts\class') 
from DBtoDF import DBTOPD

pd.set_option('display.max_columns',20)

class Cargo_traker():
    
    def Kpler_data():
        
        
        #get categories
        #SupplyCategories=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'CategoriesSupply').sql_to_df()
        DemandCategories=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'CategoriesDemand').sql_to_df()
        
        droplist = ['Algeria','Angola','Bahamas','Brunei','Cameroon','Equatorial Guinea','Libya','Nigeria','Papua New Guinea','Peru','Qatar','Russian Federation','Trinidad and Tobago','Yemen']
        for i in droplist:
            DemandCategories.drop(DemandCategories[DemandCategories['Country']==i].index, inplace=True)
        DemandCategories.set_index('Country', inplace=True)
        DemandCategories=DemandCategories['Region']
        regiondict=DemandCategories.to_dict()
        #print(regiondict)
        
        #read data from Kpler
        #Kpler=DBTOPD('PRD-DB-SQL-211','LNG', 'dbo', 'OutputsCargoTrackingBreakdownCounts')
        #dfkpler=Kpler.sql_to_df()
        
        Kplertrade=DBTOPD('PRD-DB-SQL-211','LNG', 'dbo', 'KplerLNGTrades')
        dfkplerall = Kplertrade.sql_to_df()
        
        dfkplerall = dfkplerall[['DateOrigin','CountryOrigin','EndOrigin','InstallationOrigin','CountryDestination','TradeStatus','VolumeDestinationM3']]
        #print(dfkplerall)
        dfkpleronwater = dfkplerall.loc[dfkplerall[dfkplerall['TradeStatus']=='In Transit'].index].copy()
        dfkpleronwater = dfkpleronwater.loc[dfkpleronwater[dfkpleronwater['EndOrigin']>='2020-01-01'].index]
        dfkpleronwater['cargo'] = dfkpleronwater['VolumeDestinationM3']/150000 #m3 to cargo
        dfkpleronwater = dfkpleronwater[['EndOrigin','InstallationOrigin','CountryDestination','cargo']]
        #print(dfkpleronwater.loc[dfkpleronwater[dfkpleronwater['CountryDestination']==None].index])
        dfkpleronwater['RegionDestination']=dfkpleronwater['CountryDestination'].replace(regiondict)
        #print(dfkpleronwater['CountryDestination'].head(20))
        dfKplerNULL=dfkpleronwater.loc[dfkpleronwater[dfkpleronwater['RegionDestination'].isna()].index].copy() #select NULL for unknown
        #print(dfKplerNULL)
        #dfonwater=
        
        dfdelivered= dfkplerall.loc[dfkplerall[dfkplerall['TradeStatus']=='Delivered'].index].copy()
        dfdelivered = dfdelivered.loc[dfdelivered[dfdelivered['EndOrigin']>='2020-01-01'].index]
        dfdelivered['cargo'] = dfdelivered['VolumeDestinationM3']/150000 #m3 to cargo
        dfdelivered = dfdelivered[['EndOrigin','InstallationOrigin','CountryDestination','cargo']]
        dfdelivered['RegionDestination']=dfdelivered['CountryDestination'].replace(regiondict)
        #print(dfdelivered)
        
        dfkplertrade = dfkplerall[['DateOrigin','CountryOrigin','InstallationOrigin','CountryDestination','TradeStatus','VolumeDestinationM3']]
        dfkplerloading = dfkplertrade.loc[dfkplertrade[dfkplertrade['TradeStatus']=='Loading'].index]
        dfkplerloading['cargo'] = dfkplerloading['VolumeDestinationM3']/150000 #m3 to cargo
        dfkplerScheduled = dfkplertrade.loc[dfkplertrade[dfkplertrade['TradeStatus']=='Scheduled' ].index]
        dfkplerScheduled['cargo'] = dfkplerScheduled['VolumeDestinationM3']/150000 #m3 to cargo

        dfkpleractual = dfkplerall[['EndOrigin', 'InstallationOrigin','VolumeDestinationM3']]
        #print(dfkplerScheduled)
        
        #print(dfkpler)
        return dfdelivered,dfkpleronwater,dfKplerNULL,dfkplerloading,dfkplerScheduled,dfkpleractual
    
    def desk_data():
        
        #read curveID and to dict, use for change id to country name
        '''
        SupplyCurveId = pd.read_excel('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/DeskViewID.xlsx',header=(0),sheet_name='supply')
        SupplyCurveId=SupplyCurveId[['CurveID','plant']]
        SupplyCurveId=SupplyCurveId.set_index('CurveID').T.to_dict('list')
        print(SupplyCurveId)
        '''
        
        curveid = DBTOPD.get_curve_id()
        dfcurveid = curveid.loc[:,['CurveId','Type','Location','Country']]
        #print(dfcurveid)
        SupplyCurveId = dfcurveid.loc[dfcurveid[dfcurveid['Type']=='Supply'].index]
        SupplyCurveId = SupplyCurveId[['CurveId','Country','Location']]
        SupplyCurveId.rename(columns={'CurveId':'CurveID','Country':'Country','Location':'plant'}, inplace=True)
        SupplyCurveId.loc[SupplyCurveId[SupplyCurveId['Country']=='Mozambique'].index, 'plant'] = 'Coral South FLNG' #rename Mozambique
        #SupplyCurveId.loc[SupplyCurveId[SupplyCurveId['plant']=='Portovaya LNG'].index, 'plant'] = 'Portovaya'
        SupplyCurveId=SupplyCurveId[['CurveID','plant']]
        SupplyCurveId=SupplyCurveId.set_index('CurveID').T.to_dict('list')
        #print(SupplyCurveId)
        
        #read desk view data
        desksupply=DBTOPD('PRD-DB-SQL-209','AnalyticsModel', 'ts', 'AnalyticsLatest')
        dfdesksupply=desksupply.desksupply_to_df()
        #replace curve id to country name
        dfdesksupply['CurveId'].replace(SupplyCurveId, inplace=True)
        #change data format
        dfsupplynew=dfdesksupply.groupby(['ValueDate','CurveId'], as_index=False).sum()
        dfsupplynew['ValueDate'] = pd.to_datetime(dfsupplynew['ValueDate'])
        dfsupplynew=pd.pivot(dfsupplynew, index='ValueDate', columns='CurveId')
        dfsupplynew.index.name='Date'
        dfsupplynew.columns=dfsupplynew.columns.droplevel(0)
        dfsupplynew.rename_axis(None, axis=1, inplace=True) #mt
        #print(dfsupplynew)
        
    
        return dfsupplynew
    
    def capa_data():
        
        today=datetime.date.today()
        '''
        #pull IHS name to kpler name
        IHSname=pd.read_excel('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/IHS project names.xlsx',header=(0),sheet_name=1) 
        IHSname.set_index('IHS Markit', inplace=True)
        IHSnamedict = IHSname['Plant'].to_dict()
        #print(IHSnamedict)
        #capa data  
        
        capa=pd.read_excel('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/IHS_Liquefaction Project Details.xlsx',header=(2),sheet_name=0) #mt
        capa=capa[['Liquefaction Project','Status','Initial_Capacity','IHS Markit Start Date']]
        #capa=capa[capa['Status']=='Existing'] 
        capaexist=capa[capa['Status']=='Existing'] 
        capacons=capa[capa['Status']=='Under Construction']
        capa=pd.concat([capaexist,capacons])
        
        capa['Liquefaction Project'].replace(IHSnamedict, inplace=True)
        capa['monthly capacity'] = capa['Initial_Capacity']/12 #mt/month
        capa.reset_index(inplace=True)
        capa.sort_values(by='IHS Markit Start Date', inplace=True)
        #print(capa.loc[capa['Liquefaction Project']=='Sabine Pass'])
        dbcapagroup=capa[['Liquefaction Project','IHS Markit Start Date','Initial_Capacity']]
        #dbcapagroup.set_index('IHS Markit Start Date', inplace=True)
        
        dfcapa = pd.DataFrame(index=pd.date_range(start='1964-01-01', end='2027-12-31'), columns=IHSname['Plant'].to_list())
        #dfcapa.fillna(0, inplace=True)
        #print(IHSname['Plant'])
        #print(dfcapa.loc['2021-02-01'])
        for i in IHSname['Plant']:
            df=capa.loc[capa[capa['Liquefaction Project']==i].index]
            df=df[['IHS Markit Start Date','monthly capacity']]
            df['cum capa']=df['monthly capacity'].cumsum()
            df=df.groupby('IHS Markit Start Date').sum()
            for j in df.index:
                dfcapa.loc[j, i]=df.loc[j,'cum capa']#dfcapa.loc[j,i]+df.loc[df[df['Liquefaction Project']==i].index]
            #print(i,df)
        dfcapa.fillna(0, inplace=True)
        #print(dfcapa['Sabine Pass'])
        
        dfcapa=dfcapa.resample('MS').sum()
        dfcapa.replace(to_replace=0, method='ffill', inplace=True)
        dfcapa=dfcapa/0.063 #mt/month to 
        dfcapa=dfcapa.loc['2018-01-01':,:'Calcasieu Pass']
        #get capa day cargo rage
        for i in dfcapa.index:
            days=calendar.monthrange(i.year,i.month)[1]
            dfcapa.loc[i]=dfcapa.loc[i]/days
        
       
        '''
        dfcapa = DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'IHScapa').sql_to_df()
        dfcapa.set_index('index', inplace=True)
        dfcapa.sort_index(inplace=True)
        #print(dfcapa)
        dfcapa['Portovaya'] = dfcapa['Portovaya LNG']
        #print(dfcapa['Portovaya LNG'])        
        #print(capa.index[0]<dfcapa.index[0])
        
        
        return dfcapa
    
    def plant_data(dfdelivered,dfkpleronwater, dfKplerNULL, dfkplerloading, dfkplerScheduled, dfkpleractual, dfdesk, dfcapa, plant):
       
        today=datetime.date.today()#.strftime('%Y-%m-%d')
        #hist
        dfplantdelivered=dfdelivered[dfdelivered['InstallationOrigin'] == plant].copy()
        dfplantdelivered.set_index('EndOrigin', inplace=True)
        #dfplantdelivered=dfplantdelivered.loc['2020-01-01':str(today)]
        
        dfplantonwater=dfkpleronwater[dfkpleronwater['InstallationOrigin'] == plant].copy()
        dfplantonwater.set_index('EndOrigin', inplace=True)
        
        dfplantNULL=dfKplerNULL[dfKplerNULL['InstallationOrigin'] == plant].copy()
        dfplantNULL.set_index('EndOrigin', inplace=True)
       
        #loading
        dfplantloading=dfkplerloading[dfkplerloading['InstallationOrigin'] == plant].copy()
        dfplantloading.set_index('DateOrigin', inplace=True)
        dfplantScheduled=dfkplerScheduled[dfkplerScheduled['InstallationOrigin'] == plant].copy()
        dfplantScheduled.set_index('DateOrigin', inplace=True)
        
        
        dfplantactual=dfkpleractual[dfkpleractual['InstallationOrigin'] == plant].copy()
        
        #dfplantactual['mcm']=dfplantactual['VolumeDestinationM3']*0.000612027 #Mcm
        dfplantactual['cargo']=dfplantactual['VolumeDestinationM3']/150000 #cargo
        dfplantactual.set_index('EndOrigin', inplace=True)
        dfplantactual=dfplantactual.resample('MS').sum()
        dfplantactualfull=pd.DataFrame(index=pd.date_range(start='2020-01-01', end=today, freq='MS'))
        dfplantactualfull.index.name='EndOrigin'
        dfplantactualfull=pd.merge(dfplantactualfull, dfplantactual, on='EndOrigin', how='outer')
        dfplantactualfull.fillna(0,inplace=True)
        #print(dfplantactualfull)
        #desk cargo
        dfplantdesk=dfdesk.loc['2020-01-01':today,plant].copy()
        dfplantdesk=dfplantdesk/0.062 #mt to cargo
        #print(dfplantdesk)
        dfplantdesk=dfplantdesk.resample('MS').sum()
        #print(dfplantdesk)
        region=['NW Eur','Other RoW','JKTC','Lat Am','MEIP','Med Eur','Other Eur']
        
        dfsummary=pd.DataFrame(index=pd.date_range(start='2020-01-01', end=today, freq='MS'))
        for i in region:
            dfsummary[i]=dfplantdelivered[dfplantdelivered['RegionDestination']==i].loc[:,'cargo'].resample('MS').sum()
            dfsummary[i+' on Water']=dfplantonwater[dfplantonwater['RegionDestination']==i].loc[:,'cargo'].resample('MS').sum()
            
        dfsummary.fillna(0, inplace=True)
        dfsummary['Asia'] = dfsummary['Other RoW'] + dfsummary['JKTC'] + dfsummary['MEIP']
        dfsummary['Europe'] = dfsummary['NW Eur'] + dfsummary['Med Eur'] + dfsummary['Other Eur']
        dfsummary['Asia on Water'] = dfsummary['Other RoW on Water'] + dfsummary['JKTC on Water'] + dfsummary['MEIP on Water']
        dfsummary['Europe on Water'] = dfsummary['NW Eur on Water'] + dfsummary['Med Eur on Water'] + dfsummary['Other Eur on Water']
        #dfsummary['scheduled']=dfplantScheduled.loc[:,'cargo'].resample('MS').sum()
        #dfsummary['NULL']=dfplantNULL.loc[:,'cargo'].resample('MS').sum()
        dfsummary['unknown'] = dfplantNULL.loc[:,'cargo'].resample('MS').sum()#dfplantNULL.loc[:,'cargo'].resample('MS').sum()
        
        dfsummary['loading'] = dfplantloading.loc[:,'cargo'].resample('MS').sum() #include last month loading if it has
        #dfsummary['unknown'].iloc[-1] = #dfsummary['unknown'].iloc[-1] + dfplantScheduled.shape[0]
        #dfsummary['To load @ 0.8 x day rate'] =
        dfsummary['sum']=dfsummary.loc[:,['NW Eur','Other RoW','JKTC','Lat Am','MEIP','Med Eur','Other Eur']].sum(axis=1)
        dfsummary['sum on Water']=dfsummary.loc[:,['Asia on Water','Europe on Water','MEIP on Water','unknown','loading']].sum(axis=1)
        dfsummary=dfsummary.round()
        dfsummary['Weekly Actual Load rate'] = np.nan
        dfsummary.loc[dfsummary.index.isin(dfplantactualfull.index), 'Weekly Actual Load rate'] = round(dfplantactualfull['cargo']/4,2)
        dfsummary['Weekly Actual Load rate'].iloc[-1]=round(dfplantactualfull['cargo'].loc[dfsummary.index[-1]]/today.day*7, 2)
        dfsummary.loc[dfsummary.index.isin(dfplantdesk.index),'Weekly Forecast. Load rate']=round(dfplantdesk/4,2) 
        #dfsummary['Weekly Forecast. Load rate'].iloc[-1]=round(dfplantdesk.iloc[-1]/today.day*7, 2)
        #dfsummary['Daily Actual Load rate'] =  np.nan
        #dfsummary['Daily Forecast. Load rate'] =  np.nan
        #for j in dfsummary.index:
             #days=calendar.monthrange(j.year,j.month)
             #dfsummary.loc[j,'Daily Actual Load rate']=dfplantactualfull.loc[j,'cargo']/days[1] #daily cargo loading
             #dfsummary.loc[j,'Daily Forecast. Load rate']=dfplantdesk.loc[j]/days[1] 
        
        #aveloadingrate=round(dfsummary['Daily Actual Load rate'].loc[str(today.year-2)+'-01-01':].mean(),2)
        #print('plant rolling mean 36: ', plant, dfcapa[plant].rolling(36).mean().loc['2022-02-01'])
        #dailyloadingrate=round(dfcapa[plant].rolling(36).mean().iloc[-1],2)
        dailyloadingrate=round(dfcapa[plant].rolling(36).mean().loc[str(today.year)+'-'+str(today.month)+'-01'],2)

        remain_days=calendar.monthrange(today.year,today.month)[1]-today.day
        #print(aveloadingrate,remain_days)
        dfsummary['To load '+str(dailyloadingrate)+' x day rate'] = np.nan
        dfsummary['To load '+str(dailyloadingrate)+' x day rate'].iloc[-1]=round((remain_days*dailyloadingrate),2)
        
        #print(dfsummary.tail())
        return dfsummary
       
    def plot(dfsummary, plant):
        
        #first bar chart
        figbar = go.Figure()
        figbar.add_trace(
                go.Bar(name='Asia', 
                       x=dfsummary.index, 
                       y=dfsummary.loc[:,'Asia'],
                       text=dfsummary.loc[:,'Asia'],
                       textposition='auto',
                       marker_color='#17375E'
                       )
                
            )
        figbar.add_trace(
                go.Bar(name='Asia on Water', 
                       x=dfsummary.index, 
                       y=dfsummary.loc[:,'Asia on Water'],
                       text=dfsummary.loc[:,'Asia on Water'],
                       textposition='auto',
                       marker_color='#1F497D') 
            )
        figbar.add_trace(
                go.Bar(name='Europe', 
                       x=dfsummary.index, 
                       y=dfsummary.loc[:,'Europe'],
                       text=dfsummary.loc[:,'Europe'],
                       textposition='auto',
                       marker_color='#FF0000') 
            )
        figbar.add_trace(
                go.Bar(name='Europe on Water', 
                       x=dfsummary.index, 
                       y=dfsummary.loc[:,'Europe on Water'],
                       text=dfsummary.loc[:,'Europe on Water'],
                       textposition='auto',
                       marker_color='#FF0000')
            )
        figbar.add_trace(
                go.Bar(name='MEIP on Water', 
                       x=dfsummary.index, 
                       y=dfsummary.loc[:,'MEIP on Water'],
                       text=dfsummary.loc[:,'MEIP on Water'],
                       textposition='auto',
                       marker_color='#F79646')
            )
        figbar.add_trace(
                go.Bar(name='Lat Am', 
                       x=dfsummary.index, 
                       y=dfsummary.loc[:,'Lat Am'],
                       text=dfsummary.loc[:,'Lat Am'],
                       textposition='auto',
                       marker_color='#00B0F0') 
            )
        figbar.add_trace(
                go.Bar(name='loading', 
                       x=dfsummary.index, 
                       y=dfsummary.loc[:,'loading'],
                       text=dfsummary.loc[:,'loading'],
                       textposition='auto',
                       marker_color='#A6A6A6') 
            )
        figbar.add_trace(
                go.Bar(name='unknown', 
                       x=dfsummary.index, 
                       y=dfsummary.loc[:,'unknown'],
                       text=dfsummary.loc[:,'unknown'],
                       textposition='auto',
                       marker_color='#FFFF00') 
            )
        figbar.add_trace(
                go.Bar(name=dfsummary.columns[-1], 
                       x=dfsummary.index, 
                       y=dfsummary.loc[:,dfsummary.columns[-1]],
                       text=dfsummary.loc[:,dfsummary.columns[-1]],
                       textposition='auto',
                       marker_color='#F2F2F2') 
            )
        figbar.update_xaxes(
                            dtick="M1",
                            tickformat="%b\n%Y")
        # Change the bar mode
        figbar.update_layout(barmode='relative',
                             title=plant+" cargoes by Destination region, Export Date",
                             legend=dict(orientation="h", ))
        #figbar.show()
        py.plot(figbar, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/cargos tracker/'+plant+' bar.html', auto_open=False)
        figbar.write_image('U:/Trading - Gas/LNG/LNG website/analysis/cargos tracker/'+plant+' bar.png')
        #second bar+line
        figbarline = go.Figure()
        figbarline.add_trace(
            go.Bar(
                x=dfsummary.index,
                y=dfsummary.loc[:,'sum'],
                name='Delivered',
                text=dfsummary.loc[:,'sum'],
                textposition='auto',
                ))
        figbarline.add_trace(
            go.Bar(
                x=dfsummary.index,
                y=dfsummary.loc[:,'sum on Water'],
                name='On Water',
                text=dfsummary.loc[:,'sum on Water'],
                textposition='auto'
                ))
        figbarline.add_trace(
            go.Bar(
                x=dfsummary.index,
                y=dfsummary.loc[:,dfsummary.columns[-1]],
                name=dfsummary.columns[-1],
                text=dfsummary.loc[:,dfsummary.columns[-1]],
                textposition='auto'
                ))
        figbarline.add_trace(
            go.Scatter(
                x=dfsummary.index,
                y=dfsummary.loc[:,'Weekly Actual Load rate'],
                name='Weekly Actual Load rate',
                yaxis="y3",
                mode='lines+markers',
                line = dict(color='red', dash='solid')
            ))
        figbarline.add_trace(
            go.Scatter(
                x=dfsummary.index,
                y=dfsummary.loc[:,'Weekly Forecast. Load rate'],
                name='Weekly Forecast. Load rate',
                yaxis="y3",
                mode='lines+markers',
                line = dict(color='red', dash='dash')
            ))
        figbarline.update_xaxes(
                                dtick="M1",
                                tickformat="%b\n%Y")
        figbarline.update_yaxes(title_text='Cargos')
        figbarline.update_layout(barmode='relative',
                                 title=plant+" cargoes by Destination region, Export Date",
                                 yaxis3=dict(
                                     title="Weekly cargo load rate",
                                     #titlefont=dict(color="#d62728"),
                                     #tickfont=dict(color="#d62728"),
                                     anchor="x",
                                     overlaying="y",
                                     side="right",
                                     ),
                                 legend=dict(orientation="h",)
                                 )
        
        py.plot(figbarline, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/cargos tracker/'+plant+' barline.html', auto_open=False)
        figbarline.write_image('U:/Trading - Gas/LNG/LNG website/analysis/cargos tracker/'+plant+' barline.png')
        #third pie chart
        labels = ['Asia on Water','Europe on Water','MEIP on Water','Lat Am on Water']
        colors = ['#1F497D','#FF0000','#F79646','#00B0F0']
        values = dfsummary[labels].sum()
        #print(dfsummary[labels].sum())
        #print(dfsummary[labels].sum().sum())
        figpie = go.Figure(
            data=[go.Pie(labels=labels, values=values)]
            )
        figpie.update_traces(hoverinfo='label+percent', textinfo='value', textfont_size=20, marker=dict(colors=colors))
        figpie.update_layout(title_text=plant+' Cargos on Water '+str(datetime.date.today())+'<br>'+'(Total: '+str(dfsummary[labels].sum().sum())+')',
                             legend=dict(
                                        orientation="h",
                           
                                    ))
        py.plot(figpie, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/cargos tracker/'+plant+' pie.html', auto_open=False)
        figpie.write_image('U:/Trading - Gas/LNG/LNG website/analysis/cargos tracker/'+plant+' pie.png')
        
    def USplot(dfsummary, plant): 
  
        #first bar chart
        figbar = go.Figure()
        figbar.add_trace(
               go.Bar(name='Asia', 
                      x=dfsummary.index, 
                      y=dfsummary.loc[:,'Asia'],
                      text=dfsummary.loc[:,'Asia'],
                      textposition='auto',
                      marker_color='#17375E'
                      )
               
               )
        figbar.add_trace(
               go.Bar(name='Asia on Water', 
                      x=dfsummary.index, 
                      y=dfsummary.loc[:,'Asia on Water'],
                      text=dfsummary.loc[:,'Asia on Water'],
                      textposition='auto',
                      marker_color='#1F497D') 
           )
        figbar.add_trace(
               go.Bar(name='Europe', 
                      x=dfsummary.index, 
                      y=dfsummary.loc[:,'Europe'],
                      text=dfsummary.loc[:,'Europe'],
                      textposition='auto',
                      marker_color='#FF0000') 
           )
        figbar.add_trace(
               go.Bar(name='Europe on Water', 
                      x=dfsummary.index, 
                      y=dfsummary.loc[:,'Europe on Water'],
                      text=dfsummary.loc[:,'Europe on Water'],
                      textposition='auto',
                      marker_color='#FF0000')
           )
        figbar.add_trace(
               go.Bar(name='MEIP on Water', 
                      x=dfsummary.index, 
                      y=dfsummary.loc[:,'MEIP on Water'],
                      text=dfsummary.loc[:,'MEIP on Water'],
                      textposition='auto',
                      marker_color='#F79646')
           )
        figbar.add_trace(
               go.Bar(name='Lat Am', 
                      x=dfsummary.index, 
                      y=dfsummary.loc[:,'Lat Am'],
                      text=dfsummary.loc[:,'Lat Am'],
                      textposition='auto',
                      marker_color='#00B0F0') 
           )
        figbar.add_trace(
               go.Bar(name='loading', 
                      x=dfsummary.index, 
                      y=dfsummary.loc[:,'loading'],
                      text=dfsummary.loc[:,'loading'],
                      textposition='auto',
                      marker_color='#A6A6A6') 
           )
        figbar.add_trace(
               go.Bar(name='unknown', 
                      x=dfsummary.index, 
                      y=dfsummary.loc[:,'unknown'],
                      text=dfsummary.loc[:,'unknown'],
                      textposition='auto',
                      marker_color='#FFFF00') 
           )
        figbar.add_trace(
               go.Bar(name=dfsummary.columns[-1], 
                      x=dfsummary.index, 
                      y=dfsummary.loc[:,dfsummary.columns[-1]],
                      text=dfsummary.loc[:,dfsummary.columns[-1]],
                      textposition='auto',
                      marker_color='#F2F2F2') 
           )
        figbar.update_xaxes(
                           dtick="M1",
                           tickformat="%b\n%Y")
        # Change the bar mode
        figbar.update_layout(barmode='relative',
                             title=plant+" cargoes by Destination region, Export Date",
                             legend=dict(orientation="h", ),
                             )
        #figbar.show()
        py.plot(figbar, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/cargos tracker/'+plant+' bar.html', auto_open=False)
        figbar.write_image('U:/Trading - Gas/LNG/LNG website/analysis/cargos tracker/'+plant+' bar.png')
        #second bar+line
        figbarline = go.Figure()
        
        figbarline.add_trace(
            go.Bar(
                x=dfsummary.index,
                y=dfsummary.loc[:,'sum'],
                name='Delivered',
                text=dfsummary.loc[:,'sum'],
                textposition='auto' 
                ))
        figbarline.add_trace(
            go.Bar(
                x=dfsummary.index,
                y=dfsummary.loc[:,'sum on Water'],
                name='On Water',
                text=dfsummary.loc[:,'sum on Water'],
                textposition='auto'
                ))
        figbarline.add_trace(
            go.Bar(
                x=dfsummary.index,
                y=dfsummary.loc[:,dfsummary.columns[-1]],
                name=dfsummary.columns[-1],
                text=dfsummary.loc[:,dfsummary.columns[-1]],
                textposition='auto'
                ))
        figbarline.add_trace(
            go.Scatter(
                x=dfsummary.index,
                y=dfsummary.loc[:,'Weekly Actual Load rate'],
                name='Weekly Actual Load rate',
                yaxis="y3",
                mode='lines+markers',
                line = dict(color='red', dash='solid')
            ))
        figbarline.add_trace(
            go.Scatter(
                x=dfsummary.index,
                y=dfsummary.loc[:,'Weekly Forecast. Load rate'],
                name='Weekly Forecast. Load rate',
                yaxis="y3",
                mode='lines+markers',
                line = dict(color='red', dash='dash')
            ))
        figbarline.update_xaxes(
                                dtick="M1",
                                tickformat="%b\n%Y")
        figbarline.update_yaxes(title_text='Cargos')
        figbarline.update_layout(barmode='relative',
                                 title=plant+" cargoes by Destination region, Export Date",
                                 yaxis3=dict(
                                     title="Weekly cargo load rate",
                                     #titlefont=dict(color="#d62728"),
                                     #tickfont=dict(color="#d62728"),
                                     anchor="x",
                                     overlaying="y",
                                     side="right",
                                     ),
                                 legend=dict(orientation="h",)
                                 )
        
        py.plot(figbarline, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/cargos tracker/'+plant+' barline.html', auto_open=False)
        figbarline.write_image('U:/Trading - Gas/LNG/LNG website/analysis/cargos tracker/'+plant+' barline.png')
        
        #third pie chart
        labels = ['Asia on Water','Europe on Water','MEIP on Water','Lat Am on Water'] #no unknown and loading
        colors = ['#1F497D','#FF0000','#F79646','#00B0F0']
        values = dfsummary[labels].sum()
        #print(values)
        figpie = go.Figure(
            data=[go.Pie(labels=labels, values=values)]
            )
        figpie.update_traces(hoverinfo='label+percent', textinfo='value', textfont_size=20,marker=dict(colors=colors))
        figpie.update_layout(title_text=plant+' Cargos on Water '+str(datetime.date.today())+'<br>'+'(Total: '+str(dfsummary[labels].sum().sum())+')',
                             legend=dict(
                                        orientation="h",
                             
                            
                                    ))
        py.plot(figpie, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/cargos tracker/'+plant+' pie.html', auto_open=False)
        figpie.write_image('U:/Trading - Gas/LNG/LNG website/analysis/cargos tracker/'+plant+' pie.png')
        
        
    def plant_list():
        #get categories
        #SupplyCategories = pd.read_excel('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/Categories.xlsx',header=(0),sheet_name='Index_Supply')
        #SupplyCategories = SupplyCategories.iloc[:44,0:5]
        
        SupplyCategories=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'CategoriesSupply').sql_to_df()
        #SupplyCategories.drop(SupplyCategories[SupplyCategories['Plant']=='Calcasieu Pass'].index, inplace=True)
        #SupplyCategories.drop(SupplyCategories[SupplyCategories['Plant']=='Mozambique Area 1'].index, inplace=True)
        #droplist = ['Algeria','Angola','Bahamas','Brunei','Cameroon','Equatorial Guinea','Libya','Nigeria','Papua New Guinea','Peru','Philipines','Qatar','Russian Federation','Trinidad and Tobago','Yemen']
        plant_list=SupplyCategories.loc[:,'Plant'].tolist()
        
        return plant_list
        
    def update():     
        a=Cargo_traker
        dfdelivered,dfkpleronwater,dfKplerNULL,dfkplerloading,dfkplerScheduled,dfkpleractual=a.Kpler_data()
        dfdesk=a.desk_data()
        dfcapa=a.capa_data()
        #dfkplerall=dfkpler.copy()
        plantlist=a.plant_list()
        #dfsummary=a.plant_data(dfdelivered,dfkpleronwater, dfkplerloading, dfkplerScheduled,dfkpleractual, dfdesk,'Cove Point')
        #a.plot(dfsummary, 'Soyo')
        #print(plant_list)
        
        #plant=plantlist[0]
        
        for plant in plantlist:
            #print(plant)
            #print(dfkpler)
            #dfsummary=pd.DataFrame(index=pd.date_range(start='2020-01-01', end=datetime.date.today(), freq='MS'))
            #dfkplerall=dfkpler.copy()
            #print(dfkplerall.loc[:,'InstallationOrigin'])
            #if plant in dfkplerall.loc[:,'InstallationOrigin']:
            try:    
                dfsummary=a.plant_data(dfdelivered,dfkpleronwater, dfKplerNULL,dfkplerloading, dfkplerScheduled,dfkpleractual,dfdesk, dfcapa, plant)
                
        #a.plant_data(a.Kpler_data(),'Bonny LNG')
                #print(dfsummary1)
                a.plot(dfsummary, plant)
            except Exception as e:
            #except KeyError:
            #else:
                print('Error: ' + plant+ str( e))
                #print(plant+' is null')
        
        #USall=['Cameron (Liqu.)','Cove Point','Sabine Pass','Corpus Christi','Elba Island Liq.','Freeport']
        dfsummary1=a.plant_data(dfdelivered,dfkpleronwater, dfKplerNULL,dfkplerloading, dfkplerScheduled,dfkpleractual,dfdesk,dfcapa, 'Cameron (Liqu.)')
        dfsummary2=a.plant_data(dfdelivered,dfkpleronwater, dfKplerNULL,dfkplerloading, dfkplerScheduled,dfkpleractual,dfdesk,dfcapa, 'Cove Point')
        dfsummary3=a.plant_data(dfdelivered,dfkpleronwater, dfKplerNULL,dfkplerloading, dfkplerScheduled,dfkpleractual,dfdesk,dfcapa, 'Sabine Pass')
        dfsummary4=a.plant_data(dfdelivered,dfkpleronwater, dfKplerNULL,dfkplerloading, dfkplerScheduled,dfkpleractual,dfdesk,dfcapa, 'Corpus Christi')
        dfsummary5=a.plant_data(dfdelivered,dfkpleronwater, dfKplerNULL,dfkplerloading, dfkplerScheduled,dfkpleractual,dfdesk,dfcapa, 'Elba Island Liq.')
        dfsummary6=a.plant_data(dfdelivered,dfkpleronwater, dfKplerNULL,dfkplerloading, dfkplerScheduled,dfkpleractual,dfdesk,dfcapa, 'Freeport')
        dfsummary7=a.plant_data(dfdelivered,dfkpleronwater, dfKplerNULL,dfkplerloading, dfkplerScheduled,dfkpleractual,dfdesk,dfcapa, 'Calcasieu Pass')
        #print(dfsummary6)
        dfsummaryUS=pd.DataFrame(index=dfsummary6.index, columns=dfsummary6.columns[:-1])
        for i in dfsummaryUS.columns:
            dfsummaryUS[i]= dfsummary1[i]+dfsummary2[i]+dfsummary3[i]+dfsummary4[i]+dfsummary5[i]+dfsummary6[i]+dfsummary7[i]
        
        dfUSloading=pd.concat([dfsummary1.loc[:,'loading'],dfsummary2.loc[:,'loading'],dfsummary3.loc[:,'loading'],dfsummary4.loc[:,'loading'],dfsummary5.loc[:,'loading'],dfsummary6.loc[:,'loading'],dfsummary7.loc[:,'loading']],axis=1)
        #print(dfUSloading)
        dfsummaryUS.loc[:,'loading']=np.nansum(np.array(dfUSloading),axis=1)
        dfUSunknown=pd.concat([dfsummary1.loc[:,'unknown'],dfsummary2.loc[:,'unknown'],dfsummary3.loc[:,'unknown'],dfsummary4.loc[:,'unknown'],dfsummary5.loc[:,'unknown'],dfsummary6.loc[:,'unknown'],dfsummary7.loc[:,'unknown']],axis=1)
        #print(dfUSunknown)
        dfsummaryUS.loc[:,'unknown']=np.nansum(np.array(dfUSunknown),axis=1)
        
        #aveloadingrateUS=round(dfsummaryUS['Daily Actual Load rate'].loc[str(datetime.date.today().year-2)+'-01-01':].mean(),2)
        dailyloadingrateUS = round(dfcapa[['Cameron (Liqu.)','Cove Point','Sabine Pass','Corpus Christi','Elba Island Liq.','Freeport','Calcasieu Pass']].rolling(36).mean().loc[str(datetime.date.today().year)+'-'+str(datetime.date.today().month)+'-01'].sum(),2)
        #round(dfcapa[plant].rolling(36).mean().loc[str(today.year)+'-'+str(today.month)+'-01'],2)
        remain_daysUS=calendar.monthrange(datetime.date.today().year,datetime.date.today().month)[1]-datetime.date.today().day
        #print(dfcapa[['Cameron (Liqu.)','Cove Point','Sabine Pass','Corpus Christi','Elba Island Liq.','Freeport','Calcasieu Pass']])
        dfsummaryUS['To load '+str(dailyloadingrateUS)+' x day rate'] = np.nan
        dfsummaryUS['To load '+str(dailyloadingrateUS)+' x day rate'].iloc[-1]=round((remain_daysUS*dailyloadingrateUS),2)
        a.USplot(dfsummaryUS, 'US')
        #print(dfsummaryUS)


#Cargo_traker.update()
