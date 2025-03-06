# -*- coding: utf-8 -*-
"""
Created on Mon Oct 31 10:07:14 2022

@author: SVC-GASQuant2-Prod
"""

#JKCI 
from DBtoDF import DBTOPD
import pandas as pd
import numpy as np
import datetime
from dateutil.relativedelta import relativedelta
import plotly.offline as py
import plotly.graph_objs as go
import os
import kaleido


class JKCI():
    
    def get_data():
        
        dfhist = DBTOPD('PRD-DB-SQL-211', 'LNG','ana','DemandCountryHist').sql_to_df()
        dfhist.set_index('Date', inplace=True)
        dfhist = dfhist[['China','Japan','South Korea','India']]
        #print(dfhist)
        
        dfdesk = DBTOPD('PRD-DB-SQL-211', 'LNG','ana','DeskDemand').sql_to_df()
        dfdesk.set_index('Date', inplace=True)
        dfdesk = dfdesk[['China','Japan','South Korea','India']]
        dfdesk.sort_index(inplace=True)
        #print(dfdesk)

        dfdesk = dfdesk[~((dfdesk.index.month==2)&(dfdesk.index.day==29))]
        #print(dfdesk)
        
        return dfhist, dfdesk
    
    def chart_data(dfhist, dfdesk):
        
        today = datetime.date.today()
        todayy1 = today - relativedelta(years=1)
        #last_year_start = str(today.year-1)+'-01-01'
        #last_year_end = str(today.year-1)+'-12-31'
        #current_year_start = str(today.year)+'-01-01'
        #current_year_end = str(today.year)+'-12-31'
        
        start = today - relativedelta(months=12)
        end = today + relativedelta(months=6)
        start_year1 = start - relativedelta(months=12)
        end_year1 = end - relativedelta(months=12)
        
        #before today use hist act - desk, after today, use desk - MA30
        dfhist = dfhist.rolling(30).mean()
        
        dfchart = pd.DataFrame(index=pd.date_range(start = start, end = end))
        dfchart = dfchart[~((dfchart.index.month==2)&(dfchart.index.day==29))]
        dfchart.loc[:today,['China', 'Japan', 'South Korea', 'India']] = dfhist.loc[start:today].values - dfhist.loc[start_year1:todayy1].values
        dfchart.loc[today:,['China', 'Japan', 'South Korea', 'India']] = dfdesk.loc[today:end].values - dfhist.loc[todayy1:end_year1].values

        dfchart['Net'] = dfchart.sum(axis=1)
        dfchart['Desk History'] = dfdesk.loc[start:end, ['China', 'Japan', 'South Korea', 'India']].sum(axis=1).values - dfhist.loc[start_year1:end_year1,['China', 'Japan', 'South Korea', 'India']].sum(axis=1).values

        #dfchart = dfchart.resample('MS').mean()
        dfchart = dfchart.round(2)
        #print(dfchart)
        return dfchart
    
    def chart(dfchart):
        
        #print(dfchart.loc[:,:].min(),dfchart.loc[:,:].max())
        today = datetime.date.today()
        
        fig = go.Figure()
        fig.add_trace(go.Bar(x=dfchart.index, y=dfchart['China'],
                            name='China',
                            #marker_color='#FFC000',
                            ))
        fig.add_trace(go.Bar(x=dfchart.index, y=dfchart['Japan'],
                            name='Japan',
                            #marker_color='#FFC000',
                            ))
        fig.add_trace(go.Bar(x=dfchart.index, y=dfchart['South Korea'],
                            name='South Korea',
                            #marker_color='#FFC000',
                            ))
        fig.add_trace(go.Bar(x=dfchart.index, y=dfchart['India'],
                            name='India',
                            #marker_color='#FFC000',
                            ))
        
        fig.add_trace(go.Scatter(x=dfchart.index, y=dfchart['Net'],
                            mode='lines',
                            name='Net',
                            line=dict(color='black', dash='solid'),
                            ))#,secondary_y=False)
        fig.add_trace(go.Scatter(x=dfchart.index, y=dfchart['Desk History'],
                            mode='lines',
                            name='Desk History',
                            line=dict(color='black', dash='dot'),
                            ))
        #print(dfchart.values.min(),dfchart.values.max())
        fig.add_trace(go.Scatter(x=[today,today],y=[dfchart.loc['2022-02-01':].values.min(),dfchart.loc['2022-02-01':].values.max()],
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
             title_text='Y-o-Y Import Delta JKCI '+str(today),
             xaxis = dict(dtick="M1"),
             #xaxis_tickformat = '%B',
             hovermode='x unified',
             plot_bgcolor = 'white',
             template='ggplot2'  
         )
        fig.update_layout(barmode='relative', annotations=[dict(x=today, y=0.9, yref='paper',font=dict(color="red",size=14),showarrow=False, xanchor='left', text='Today'),
                                           ],)

        fig.update_yaxes(title_text="Mcm/d", showgrid=True,gridcolor='lightgrey')
        
        py.plot(fig, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/JKCdemand/JKCIMA30.html', auto_open=False)
        fig.write_image("U:/Trading - Gas/LNG/LNG website/analysis/JKCdemand/JKCIMA30.png")
      
    def update():

        dfhist, dfdesk = JKCI.get_data()
        dfchart = JKCI.chart_data(dfhist, dfdesk)
        JKCI.chart(dfchart)

#JKCI.update()