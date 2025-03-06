# -*- coding: utf-8 -*-
"""
Created on Tue Nov  1 10:38:56 2022

@author: SVC-GASQuant2-Prod
"""

#Model YoY supply desk view 

from DBtoDF import DBTOPD
import pandas as pd
import numpy as np
import datetime
from dateutil.relativedelta import relativedelta
import plotly.offline as py
import plotly.graph_objs as go
import calendar

class modelYoY():
    
    def get_data():
        
        dfbasin = DBTOPD('PRD-DB-SQL-211', 'LNG','ana','SupplyBasinHist').sql_to_df()
        dfbasin.set_index('Date', inplace=True)
        #print(dfbasin)
        dfcountry = DBTOPD('PRD-DB-SQL-211', 'LNG','ana','SupplyCountryHist').sql_to_df()
        dfcountry.set_index('Date', inplace=True)
        
        dfplant = DBTOPD('PRD-DB-SQL-211', 'LNG','ana','SupplyPlantHist').sql_to_df()
        dfplant.set_index('Date', inplace=True)
        #print(dfcountry)
        dfdeskbasin = DBTOPD('PRD-DB-SQL-211', 'LNG','ana','BasinSupply').sql_to_df()
        dfdeskbasin.set_index('Date', inplace=True)
        #print(dfdesk)
        dfdeskcountry = DBTOPD('PRD-DB-SQL-211', 'LNG','ana','DeskSupplyCountry').sql_to_df()
        dfdeskcountry.set_index('Date', inplace=True)
        
        dfdeskplant = DBTOPD('PRD-DB-SQL-211', 'LNG','ana','DeskSupplyPlant').sql_to_df()
        dfdeskplant.set_index('Date', inplace=True)
        
        #get desk previous run
        update_date = DBTOPD.get_model_run_date()
        #print(update_date)
        rundate = update_date.iloc[1,0]
        #hist data
        curveid = DBTOPD.get_curve_id()
        dfcurveid = curveid.loc[:,['CurveId','Type','Location','Country']]
        #print(dfcurveid)
        SupplyCurveId = dfcurveid.loc[dfcurveid[dfcurveid['Type']=='Supply'].index]
        SupplyCurveId = SupplyCurveId[['CurveId','Country','Location']]
        SupplyCurveId.rename(columns={'CurveId':'CurveID','Country':'Country','Location':'plant'}, inplace=True)
        SupplyCurveId.drop(SupplyCurveId[SupplyCurveId['plant']=='Arctic LNG 2'].index, inplace=True)
        
        desk_supply_hist = DBTOPD.get_desk_supply_hist()
        SupplyCurveId_plant=SupplyCurveId.loc[:,['CurveID','plant']]
        SupplyCurveId_plant=SupplyCurveId_plant.set_index('CurveID').T.to_dict('list')
        #replace curve id to country name
        desk_supply_hist_plant = desk_supply_hist.copy()
        desk_supply_hist_plant.loc[:,'CurveId'].replace(SupplyCurveId_plant, inplace=True)
        
        
        desk_supply_hist = desk_supply_hist_plant.loc[desk_supply_hist_plant[desk_supply_hist_plant['ForecastDate'] == str(rundate)].index,['CurveId','ValueDate','Value']]
        #print(desk_supply_hist)
        #change data format
        dfsupplynew=desk_supply_hist.groupby(['ValueDate','CurveId'], as_index=False).sum()
        dfsupplynew.loc[:,'ValueDate'] = pd.to_datetime(dfsupplynew.loc[:,'ValueDate'])
        dfsupplynew=pd.pivot(dfsupplynew, index='ValueDate', columns='CurveId')
        dfsupplynew.index.name='Date'
        dfsupplynew.columns=dfsupplynew.columns.droplevel(0)
        dfsupplynew.rename_axis(None, axis=1, inplace=True)
        for i in dfsupplynew.index:
            days=calendar.monthrange(i.year,i.month)
            dfsupplynew.loc[i]=dfsupplynew.loc[i]*1397/days[1]
        #print(dfsupplynew)
        #change to full date data
        today = datetime.date.today()
        dfalldesksupply=pd.DataFrame(index=pd.date_range(start=str(today.year-2)+'-01-01',end=str(today.year+1)+'-12-31'))
        dfalldesksupply.index.name='Date'
        dfdeskplanthistfull = pd.merge(dfalldesksupply, dfsupplynew, on='Date', how='outer')
        dfdeskplanthistfull.fillna(method='ffill', inplace=True) 
        dfdeskplanthistfull=dfdeskplanthistfull.loc[str(today.year-2)+'-01-01':str(today.year+1)+'-12-31']
        #print(dfdeskplanthistfull.columns)
        SupplyCategories=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'CategoriesSupply').sql_to_df()
        drop_list_supply = ['Vysotsk','Kollsnes', 'Stavanger']
        for i in drop_list_supply:
            SupplyCategories.drop(SupplyCategories[SupplyCategories['Plant'] == i].index, inplace=True)
        
        #Basin plants list, and remove 
        SupplyCategories.set_index('Basin', inplace=True)
        PB = SupplyCategories['Plant'].loc['PB'].values.tolist()
        #PB.remove('Mozambique Area 1')
        AB = SupplyCategories['Plant'].loc['AB'].values.tolist()
        #AB.remove('Calcasieu Pass')
        MENA = SupplyCategories['Plant'].loc['MENA_Bas'].values.tolist()

        
        dfdeskplanthistfull['PB Desk'] = dfdeskplanthistfull[PB].sum(axis=1)
        dfdeskplanthistfull['AB Desk'] = dfdeskplanthistfull[AB].sum(axis=1)
        dfdeskplanthistfull['MENA Desk'] = dfdeskplanthistfull[MENA].sum(axis=1)
        dfdeskplanthistfull['Global Desk'] = dfdeskplanthistfull.loc[:,['AB Desk', 'PB Desk','MENA Desk']].sum(axis=1)
        #print(dfdeskplanthistfull)
        
        return dfbasin, dfcountry, dfplant, dfdeskbasin, dfdeskcountry, dfdeskplant, dfdeskplanthistfull
    
    def chart_data(dfbasin, dfcountry, dfplant, dfdeskbasin, dfdeskcountry, dfdeskplant, dfdeskplanthistfull):
        
        today = datetime.date.today()
        todayy1 = today - relativedelta(years=1)
        last_year_start = str(today.year-1)+'-01-01'
        last_year_end = str(today.year-1)+'-12-31'
        current_year_start = str(today.year)+'-01-01'
        current_year_end = str(today.year)+'-12-31'
        next_year_end = str(today.year+1)+'-12-31'

        start = today - relativedelta(months=12)
        end = today + relativedelta(months=6)
        start_year1 = start - relativedelta(months=12)
        end_year1 = end - relativedelta(months=12)
        #print(start_year1)
        #print(dfdeskbasin)
        #print(dfdeskplanthistfull.loc[current_year_start:next_year_end,'Global Desk'])
        #print(dfdeskplanthistfull.loc[last_year_start:current_year_end,'Global Desk'])
        
    
        if (today.year+1)%4 == 0 or (today.year)%4 == 0:                       
            
            dfdeskcountry = dfdeskcountry[~((dfdeskcountry.index.month==2)&(dfdeskcountry.index.day==29))]
            dfdeskplant = dfdeskplant[~((dfdeskplant.index.month==2)&(dfdeskplant.index.day==29))]
            dfdeskbasin = dfdeskbasin[~((dfdeskbasin.index.month==2)&(dfdeskbasin.index.day==29))]
            dfdeskplanthistfull = dfdeskplanthistfull[~((dfdeskplanthistfull.index.month==2)&(dfdeskplanthistfull.index.day==29))]
            #dfcountry = dfcountry[~((dfcountry.index.month==2)&(dfcountry.index.day==29))]
            
            #print(dfdeskcountry.loc['2024-02-28':])
            dfchart = pd.DataFrame(index=pd.date_range(start = start, end = next_year_end))
            dfchart = dfchart[~((dfchart.index.month==2)&(dfchart.index.day==29))]
            dfchart.loc[:today,['Australia', 'United States', 'Russia']] = dfcountry.loc[start:today, ['Australia', 'United States', 'Russian Federation']].values - dfcountry.loc[start_year1:todayy1,['Australia', 'United States', 'Russian Federation']].values
            dfchart.loc[:today,['Yamal', 'Sakhalin 2']] = dfplant.loc[start:today, ['Yamal', 'Sakhalin 2']].values - dfplant.loc[start_year1:todayy1,['Yamal', 'Sakhalin 2']].values
            dfchart.loc[:today,[ 'AB','PB','MENA']] = dfbasin.loc[start:today,[ 'AB','PB','MENA']].values - dfbasin.loc[start_year1:todayy1,[ 'AB','PB','MENA']].values
            #print(dfdeskcountry)
            dfchart.loc[today:,['Australia', 'United States', 'Russia']] = dfdeskcountry.loc[today:next_year_end,['Australia', 'United States', 'Russian Federation']].values - dfdeskcountry.loc[todayy1:current_year_end,['Australia', 'United States', 'Russian Federation']].values
            dfchart.loc[today:,['Yamal', 'Sakhalin 2']] = dfdeskplant.loc[today:next_year_end,['Yamal', 'Sakhalin 2']].values - dfdeskplant.loc[todayy1:current_year_end,['Yamal', 'Sakhalin 2']].values
            dfchart.loc[today:,['AB','PB','MENA']] = dfdeskbasin.loc[today:next_year_end,['AB Desk','PB Desk','MENA Desk']].values - dfdeskbasin.loc[todayy1:current_year_end,['AB Desk','PB Desk','MENA Desk']].values
            
            dfchart['AB (less US & Yamal)'] = dfchart['AB'] - dfchart['United States'] - dfchart['Yamal']
            dfchart['PB (less Australia, Sakhalin 2)'] = dfchart['PB'] - dfchart['Australia'] - dfchart['Sakhalin 2']
            #print(dfdeskbasin.loc[start_year1:current_year_end,['AB Desk','PB Desk','MENA Desk']])
            dfchart['Desk view'] = dfdeskbasin.loc[start:next_year_end,['AB Desk','PB Desk','MENA Desk']].sum(axis=1).values - dfdeskbasin.loc[start_year1:current_year_end,['AB Desk','PB Desk','MENA Desk']].sum(axis=1).values
            dfchart.loc[:,'Previous view'] = dfdeskplanthistfull.loc[start:next_year_end,'Global Desk'].values - dfdeskplanthistfull.loc[start_year1:current_year_end,'Global Desk'].values
            #print(dfchart.loc['2022-11-01':])
            dfchart = dfchart.resample('MS').sum()
            #print(dfchart)
        else: 
            dfchart = pd.DataFrame(index=pd.date_range(start = start, end = next_year_end))
            dfchart.loc[:today,['Australia', 'United States', 'Russia']] = dfcountry.loc[start:today, ['Australia', 'United States', 'Russian Federation']].values - dfcountry.loc[start_year1:todayy1,['Australia', 'United States', 'Russian Federation']].values
            dfchart.loc[:today,['Yamal', 'Sakhalin 2']] = dfplant.loc[start:today, ['Yamal', 'Sakhalin 2']].values - dfplant.loc[start_year1:todayy1,['Yamal', 'Sakhalin 2']].values
            dfchart.loc[:today,[ 'AB','PB','MENA']] = dfbasin.loc[start:today,[ 'AB','PB','MENA']].values - dfbasin.loc[start_year1:todayy1,[ 'AB','PB','MENA']].values
            #print(dfdeskcountry)
            dfchart.loc[today:,['Australia', 'United States', 'Russia']] = dfdeskcountry.loc[today:next_year_end,['Australia', 'United States', 'Russian Federation']].values - dfdeskcountry.loc[todayy1:current_year_end,['Australia', 'United States', 'Russian Federation']].values
            dfchart.loc[today:,['Yamal', 'Sakhalin 2']] = dfdeskplant.loc[today:next_year_end,['Yamal', 'Sakhalin 2']].values - dfdeskplant.loc[todayy1:current_year_end,['Yamal', 'Sakhalin 2']].values
            dfchart.loc[today:,['AB','PB','MENA']] = dfdeskbasin.loc[today:next_year_end,['AB Desk','PB Desk','MENA Desk']].values - dfdeskbasin.loc[todayy1:current_year_end,['AB Desk','PB Desk','MENA Desk']].values
            
            dfchart['AB (less US & Yamal)'] = dfchart['AB'] - dfchart['United States'] - dfchart['Yamal']
            dfchart['PB (less Australia, Sakhalin 2)'] = dfchart['PB'] - dfchart['Australia'] - dfchart['Sakhalin 2']
            #print(dfdeskbasin.loc[start_year1:current_year_end,['AB Desk','PB Desk','MENA Desk']])
            dfchart['Desk view'] = dfdeskbasin.loc[start:next_year_end,['AB Desk','PB Desk','MENA Desk']].sum(axis=1).values - dfdeskbasin.loc[start_year1:current_year_end,['AB Desk','PB Desk','MENA Desk']].sum(axis=1).values
            dfchart.loc[:,'Previous view'] = dfdeskplanthistfull.loc[start:next_year_end,'Global Desk'].values - dfdeskplanthistfull.loc[start_year1:current_year_end,'Global Desk'].values
            #print(dfchart.loc['2022-11-01':])
            dfchart = dfchart.resample('MS').sum()
        #print(dfchart)
        for i in dfchart.index:
            dfchart.loc[i] = dfchart.loc[i]*0.0004381
            
        dfchart = dfchart.round(2)
        #print(dfchart)
        
        return dfchart
    
    def chart(dfchart):
        
        today = datetime.date.today()
        
        fig = go.Figure()
        fig.add_trace(go.Bar(x=dfchart.index, y=dfchart['Australia'],
                            name='Australia',
                            marker_color='blue',
                            ))
        fig.add_trace(go.Bar(x=dfchart.index, y=dfchart['United States'],
                            name='United States',
                            marker_color='grey',
                            ))
        fig.add_trace(go.Bar(x=dfchart.index, y=dfchart['Russia'],
                            name='Russia',
                            marker_color='gold',
                            ))
        fig.add_trace(go.Bar(x=dfchart.index, y=dfchart['AB (less US & Yamal)'],
                            name='AB (less US & Yamal)',
                            marker_color='lightgrey',
                            ))
        fig.add_trace(go.Bar(x=dfchart.index, y=dfchart['PB (less Australia, Sakhalin 2)'],
                            name='PB (less Australia, Sakhalin 2)',
                            marker_color='lightblue',
                            ))
        fig.add_trace(go.Bar(x=dfchart.index, y=dfchart['MENA'],
                            name='MENA',
                            marker_color='orange',
                            ))
        
        fig.add_trace(go.Scatter(x=dfchart.index, y=dfchart['Desk view'],
                            mode='lines',
                            name='Desk view',
                            line=dict(color='red', dash='solid'),
                            ))#,secondary_y=False)
        fig.add_trace(go.Scatter(x=dfchart.index, y=dfchart['Previous view'],
                            mode='lines',
                            name='Previous view',
                            line=dict(color='black', dash='dot'),
                            ))#,secondary_y=False)
        fig.add_trace(go.Scatter(x=[today,today],y=[dfchart.values.min(),dfchart.values.max()],
                   mode="lines",
                   line=dict(color='red', width=2, dash='dot'),
                   showlegend=False,
                   name='Today'
                   ))
        fig.update_layout(
             autosize=True,
             showlegend=True,
             #colorscale='RdBu',
             legend=dict(x=0, y=-0.2),
             legend_orientation="h",
             title_text='Y-o-Y Supply Desk View (mt) '+str(today),
             xaxis = dict(dtick="M1"),
             #xaxis_tickformat = '%B',
             hovermode='x unified',
             plot_bgcolor = 'white',
             template='ggplot2'  
         )
        fig.update_layout(barmode='relative', annotations=[dict(x=today, y=0.9, yref='paper',font=dict(color="red",size=14),showarrow=False, xanchor='left', text='Today'),
                                           ],)

        fig.update_yaxes(showgrid=True,gridcolor='lightgrey')
        #fig.update_yaxes(title_text="Mcm/d")
        
        py.plot(fig, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/modelyoy/modelyoy.html', auto_open=False)
        fig.write_image("U:/Trading - Gas/LNG/LNG website/analysis//modelyoy/modelyoy.png")
        
    def update():
        
        dfbasin, dfcountry, dfplant, dfdeskbasin, dfdeskcountry, dfdeskplant, dfdeskplanthistfull = modelYoY.get_data()
        dfchart = modelYoY.chart_data(dfbasin, dfcountry, dfplant, dfdeskbasin, dfdeskcountry, dfdeskplant, dfdeskplanthistfull)
        modelYoY.chart(dfchart)
        
#modelYoY.update()