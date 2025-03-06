# -*- coding: utf-8 -*-
"""
Created on Mon Oct 24 10:37:12 2022

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
import calendar
import dash_html_components as html
import colorlover

class Aus_Feed():
    
    
    def get_data():
        
        df = DBTOPD.get_aus_feedgas()
        df = df[['GasDate',	'FacilityName',	'Demand',	'NomsAndForecasted',	'LastUpdated']]
        #print(df)
        dfpivot = pd.pivot_table(data= df, values = 'Demand', index = 'GasDate', columns = 'FacilityName',aggfunc='sum')
        dfpivot.rename(columns={'APLNG Pipeline':'APLNG','GLNG Gas Transmission Pipeline':'GLNG','Wallumbilla to Gladstone Pipeline':'QCLNG'}, inplace=True)
        #print(dfpivot)
        
        dfpivot_mcmd = round(dfpivot*365/1000*0.0693601525923357, 2)  #pj/y to mcm/d
        
        
        #read data from Kpler
        Kpler=DBTOPD('PRD-DB-SQL-211','LNG', 'dbo', 'KplerLNGTrades')
        dfkpler=Kpler.sql_to_df()
        
        #get supply and demand df
        supply_plant_columns=['StartOrigin','InstallationOrigin','VolumeOriginM3']
        df_supply_plant = dfkpler[supply_plant_columns]
      
        #create supply list
        SupplyCurveId = pd.read_excel('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/DeskViewID.xlsx',header=(0),sheet_name='supply')
        SupplyCurveId=SupplyCurveId.loc[:,['CurveID','plant']]
        #rolling start date
        start_date='2019-01-01'
        today=datetime.date.today()
        dfMA=pd.DataFrame(index=pd.date_range(start=start_date,end=today))
        dfMA.index.name='Date'
        for i in ['QCLNG',	'APLNG','GLNG'] :
            
            dffulldate=pd.DataFrame(index=pd.date_range(start=start_date,end=today)) 
            dffulldate.index.name='Date'
            dfplant = df_supply_plant[df_supply_plant['InstallationOrigin']==i]
            dfplant.loc[:,'StartOrigin']=pd.to_datetime(dfplant.loc[:,'StartOrigin']).dt.strftime('%Y-%m-%d') 
            
            dfplant=dfplant.groupby(['StartOrigin']).sum()*0.000612
            dfplant.index.name='Date'
            dfplant.index=pd.DatetimeIndex(dfplant.index)
            
            merged = pd.merge(dffulldate, dfplant.loc[start_date: str(today)], on='Date', how='outer')
            merged.fillna(0, inplace=True)
            
            npMA=merged.loc[start_date: str(today)]
            dfMA[i]=npMA.loc[start_date:today].values
        #print(dfMA)
        
        
        return dfpivot_mcmd, dfMA
    
    
    def mcmchart(dfpivot_mcmd):
        
        today = datetime.date.today()
        
        fig=go.Figure()
        fig.add_trace(go.Scatter(x=dfpivot_mcmd.index, y=dfpivot_mcmd['APLNG'],
                            mode='lines',
                            name='APLNG',
                            #line=dict(color='red', dash='solid')
                            ))
        fig.add_trace(go.Scatter(x=dfpivot_mcmd.index, y=dfpivot_mcmd['GLNG'],
                            mode='lines',
                            name='GLNG',
                            #line=dict(color='red', dash='solid')
                            ))
        fig.add_trace(go.Scatter(x=dfpivot_mcmd.index, y=dfpivot_mcmd['QCLNG'],
                            mode='lines',
                            name='QCLNG',
                            #line=dict(color='red', dash='solid')
                            ))
        fig.add_trace(go.Scatter(x=dfpivot_mcmd.index, y=dfpivot_mcmd[['APLNG','QCLNG','GLNG']].sum(axis=1),
                            mode='lines',
                            name='AGGREG.',
                            #line=dict(color='red', dash='solid')
                            ))
        
        fig.update_layout(
             autosize=True,
             showlegend=True,
             #colorscale='RdBu',
             legend=dict(x=0, y=-0.2),
             legend_orientation="h",
             title_text='AUS Feedgas Mcm/d '+str(today),
             #xaxis = dict(dtick="M1"),
             #xaxis_tickformat = '%B',
             hovermode='x unified',
             plot_bgcolor = 'white',
             template='ggplot2'  
         )
        fig.update_yaxes(title_text="Mcm/d")
        
        py.plot(fig, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/AUS feedgas/mcmchart.html', auto_open=False)
        fig.write_image("U:/Trading - Gas/LNG/LNG website/analysis/AUS feedgas/mcmchart.png")
        
        
    def untilasation(dfpivot_mcmd, dfkpler):
        today = datetime.date.today()
        
        dfuti = dfkpler.copy()
        dfuti = dfuti.rolling(10).mean()
        #print(dfuti.head(10))
        
        dfutifeed = pd.DataFrame()
        dfutifeed['QCLNG daily uti'] = dfpivot_mcmd['QCLNG']/(8.34*1.397*1000/365*(1+0.09))
        dfutifeed['APLNG daily uti'] = dfpivot_mcmd['APLNG']/(8.83*1.397*1000/365*(1+0.09))
        dfutifeed['GLNG daily uti'] = dfpivot_mcmd['GLNG']/(7.66*1.397*1000/365*(1+0.09))
        dfutifeed['Aust LNG CBM'] = (dfpivot_mcmd['QCLNG'] + dfpivot_mcmd['APLNG'] + dfpivot_mcmd['GLNG']) / (36.85	+ 31.94	+ 34.80)
        #print(dfutifeed)
        #dfuti['QCLNG daily uti'] = dfpivot_mcmd['QCLNG']/(8.34*1.397*1000/365*(1+0.09))
        #dfuti['APLNG daily uti'] = dfpivot_mcmd['APLNG']/(8.83*1.397*1000/365*(1+0.09))
        #dfuti['GLNG daily uti'] = dfpivot_mcmd['GLNG']/(7.66*1.397*1000/365*(1+0.09))
        #dfuti['Aust LNG CBM'] = (dfpivot_mcmd['QCLNG'] + dfpivot_mcmd['APLNG'] + dfpivot_mcmd['GLNG']) / (36.85	+ 31.94	+ 34.80)
        dfuti['QCLNG_Kpler'] = dfuti['QCLNG']/(8.5*3.82774704398606)
        dfuti['APLNG_Kpler'] = dfuti['APLNG']/(9*3.82774704398606)
        dfuti['GLNG_Kpler'] = dfuti['GLNG']/(7.8*3.82774704398606)
        dfuti['Aust LNG CBM_Kpler']  = (dfuti['QCLNG'] + dfuti['APLNG'] + dfuti['GLNG'])/(32.54	+ 34.45	+ 29.86)
        dfuti = dfuti.round(2)
        #print(dfutifeed)
        index = pd.date_range(start = dfuti.index[0], end = dfutifeed.index[-1])
        #print(index)
        fig1=go.Figure()
        fig1.add_trace(go.Scatter(x=index, y=dfuti['QCLNG_Kpler'],
                            mode='lines',
                            name='QCLNG_Kpler',
                            line=dict(color='blue', dash='solid')
                            ))
        fig1.add_trace(go.Scatter(x=index, y=dfutifeed['QCLNG daily uti'],
                            mode='lines',
                            name='QCLNG daily uti',
                            line=dict(color='red', dash='solid')
                            ))
        fig1.add_trace(go.Scatter(x=[today,today],y=[0,1.5],
                   mode="lines",
                   line=dict(color='red', width=2, dash='dot'),
                   showlegend=False,
                   name='Today'
                   ))
        fig1.update_layout(barmode='relative', annotations=[dict(x=today, y=0.9, yref='paper',font=dict(color="red",size=14),showarrow=False, xanchor='left', text='Today'),
                                           ],)
        fig1.update_layout(
             autosize=True,
             showlegend=True,
             #colorscale='RdBu',
             legend=dict(x=0, y=-0.2),
             legend_orientation="h",
             #title_text='AUS Feedgas Mcm/d '+str(today),
             #xaxis = dict(dtick="M1"),
             #xaxis_tickformat = '%B',
             hovermode='x unified',
             plot_bgcolor = 'white',
             template='ggplot2'  
         )
        
        fig1.update_yaxes(dict(tickformat=".0%"))
        
        py.plot(fig1, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/AUS feedgas/QCLNG%.html', auto_open=False)
        fig1.write_image("U:/Trading - Gas/LNG/LNG website/analysis/AUS feedgas/QCLNG%.png")
        
        fig2=go.Figure()
        fig2.add_trace(go.Scatter(x=index, y=dfuti['APLNG_Kpler'],
                            mode='lines',
                            name='APLNG_Kpler',
                            line=dict(color='blue', dash='solid')
                            ))
        fig2.add_trace(go.Scatter(x=index, y=dfutifeed['APLNG daily uti'],
                            mode='lines',
                            name='APLNG daily uti',
                            line=dict(color='red', dash='solid')
                            ))
        fig2.add_trace(go.Scatter(x=[today,today],y=[0,1.5],
                   mode="lines",
                   line=dict(color='red', width=2, dash='dot'),
                   showlegend=False,
                   name='Today'
                   ))
        fig2.update_layout(barmode='relative', annotations=[dict(x=today, y=0.9, yref='paper',font=dict(color="red",size=14),showarrow=False, xanchor='left', text='Today'),
                                           ],)
        fig2.update_layout(
             autosize=True,
             showlegend=True,
             #colorscale='RdBu',
             legend=dict(x=0, y=-0.2),
             legend_orientation="h",
             #title_text='AUS Feedgas Mcm/d '+str(today),
             #xaxis = dict(dtick="M1"),
             #xaxis_tickformat = '%B',
             hovermode='x unified',
             plot_bgcolor = 'white',
             template='ggplot2'  
         )
        fig2.update_yaxes(dict(tickformat=".0%"))
        
        py.plot(fig2, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/AUS feedgas/APLNG%.html', auto_open=False)
        fig2.write_image("U:/Trading - Gas/LNG/LNG website/analysis/AUS feedgas/APLNG%.png")
        
       
        fig3=go.Figure()
        fig3.add_trace(go.Scatter(x=index, y=dfuti['GLNG_Kpler'],
                            mode='lines',
                            name='GLNG_Kpler',
                            line=dict(color='blue', dash='solid')
                            ))
        fig3.add_trace(go.Scatter(x=index, y=dfutifeed['GLNG daily uti'],
                            mode='lines',
                            name='GLNG daily uti',
                            line=dict(color='red', dash='solid')
                            ))
        fig3.add_trace(go.Scatter(x=[today,today],y=[0,1.5],
                   mode="lines",
                   line=dict(color='red', width=2, dash='dot'),
                   showlegend=False,
                   name='Today'
                   ))
        fig3.update_layout(barmode='relative', annotations=[dict(x=today, y=0.9, yref='paper',font=dict(color="red",size=14),showarrow=False, xanchor='left', text='Today'),
                                           ],)
        fig3.update_layout(
             autosize=True,
             showlegend=True,
             #colorscale='RdBu',
             legend=dict(x=0, y=-0.2),
             legend_orientation="h",
             #title_text='AUS Feedgas Mcm/d '+str(today),
             #xaxis = dict(dtick="M1"),
             #xaxis_tickformat = '%B',
             hovermode='x unified',
             plot_bgcolor = 'white',
             template='ggplot2'  
         )
        fig3.update_yaxes(dict(tickformat=".0%"))
        
        py.plot(fig3, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/AUS feedgas/GLNG%.html', auto_open=False)
        fig3.write_image("U:/Trading - Gas/LNG/LNG website/analysis/AUS feedgas/GLNG%.png")
        
        fig4=go.Figure()
        fig4.add_trace(go.Scatter(x=index, y=dfuti['Aust LNG CBM_Kpler'],
                            mode='lines',
                            name='Aust LNG CBM_Kpler',
                            line=dict(color='blue', dash='solid')
                            ))
        fig4.add_trace(go.Scatter(x=index, y=dfutifeed['Aust LNG CBM'],
                            mode='lines',
                            name='Aust LNG CBM',
                            line=dict(color='red', dash='solid')
                            ))
        fig4.add_trace(go.Scatter(x=[today,today],y=[0,1.5],
                   mode="lines",
                   line=dict(color='red', width=2, dash='dot'),
                   showlegend=False,
                   name='Today'
                   ))
        fig4.update_layout(barmode='relative', annotations=[dict(x=today, y=0.9, yref='paper',font=dict(color="red",size=14),showarrow=False, xanchor='left', text='Today'),
                                           ],)
        fig4.update_layout(
             autosize=True,
             showlegend=True,
             #colorscale='RdBu',
             legend=dict(x=0, y=-0.2),
             legend_orientation="h",
             #title_text='AUS Feedgas Mcm/d '+str(today),
             #xaxis = dict(dtick="M1"),
             #xaxis_tickformat = '%B',
             hovermode='x unified',
             plot_bgcolor = 'white',
             template='ggplot2'  
         )
        fig4.update_yaxes(dict(tickformat=".0%"))
        
        py.plot(fig4, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/AUS feedgas/Aust%.html', auto_open=False)
        fig4.write_image("U:/Trading - Gas/LNG/LNG website/analysis/AUS feedgas/Aust%.png")
        
        #print(dfuti.head(10))
        return dfutifeed, dfuti
    
    def utiyoy(dfutifeed):
        
        
        today = datetime.date.today()
        index = pd.date_range(start = str(today.year)+'-01-01', end = str(today.year)+'-12-31')
        #print(dfutifeed.values.nanmin(), dfutifeed.values.nanmax())
        #print(np.nanmin(dfutifeed.values), np.nanmax(dfutifeed.values))
        
        for i in ['QCLNG daily uti','APLNG daily uti','GLNG daily uti','Aust LNG CBM']:
            
            fig=go.Figure()
            fig.add_trace(go.Scatter(x=index, y=dfutifeed.loc[str(today.year-3)+'-01-01':str(today.year-3)+'-12-31', i],
                                mode='lines',
                                name=str(today.year-3),
                                #line=dict(color='blue', dash='solid')
                                ))
            fig.add_trace(go.Scatter(x=index, y=dfutifeed.loc[str(today.year-2)+'-01-01':str(today.year-2)+'-12-31', i],
                                mode='lines',
                                name=str(today.year-2),
                                #line=dict(color='blue', dash='solid')
                                ))
            fig.add_trace(go.Scatter(x=index, y=dfutifeed.loc[str(today.year-1)+'-01-01':str(today.year-1)+'-12-31', i],
                                mode='lines',
                                name=str(today.year-1),
                                #line=dict(color='blue', dash='solid')
                                ))
            fig.add_trace(go.Scatter(x=index, y=dfutifeed.loc[str(today.year-0)+'-01-01':dfutifeed.index[-1], i],
                                mode='lines',
                                name=str(today.year-0),
                                #line=dict(color='blue', dash='solid')
                                ))
            fig.add_trace(go.Scatter(
                   x=[today,today],y=[np.nanmin(dfutifeed.values), np.nanmax(dfutifeed.values)],
                   #x=[today,today],y=[0,dfutifeed.values.max()],
                   mode="lines",
                   line=dict(color='red', width=2, dash='dot'),
                   showlegend=False,
                   name='Today'
                   ))
            fig.update_layout(barmode='relative', annotations=[dict(x=today, y=1, yref='paper',font=dict(color="red",size=14),showarrow=False, xanchor='left', text='Today'),
                                           ],)
            fig.update_layout(
                 autosize=True,
                 showlegend=True,
                 #colorscale='RdBu',
                 legend=dict(x=0, y=-0.2),
                 legend_orientation="h",
                 title_text=i + ' %'+str(today),
                 xaxis = dict(dtick="M1"),
                 xaxis_tickformat = '%d/%B',
                 hovermode='x unified',
                 plot_bgcolor = 'white',
                 template='ggplot2'  
             )
            fig.update_yaxes(dict(tickformat=".0%"))
            
            py.plot(fig, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/AUS feedgas/'+i+'.html', auto_open=False)
            fig.write_image('U:/Trading - Gas/LNG/LNG website/analysis/AUS feedgas/'+i+'.png')
        
       
    def tables(dfpivot_mcmd, dfuti,dfutifeed, dfkpler):
        
        #print(dfpivot_mcmd)
        today = datetime.date.today()
        d6 = today + relativedelta(days = 6)
        ytd = today - relativedelta(days = 1)
        m = today.strftime('%b-%Y') 
        m1 = (today - relativedelta(months = 1)).strftime('%b-%Y') 
        y1 = (today - relativedelta(years = 1)).strftime('%b-%Y')
        
        index = ['D+6','Today','Today-1',m, m1, y1]
        column = ['APLNG', 'GLNG',	'QCLNG', 'AGGREG.',	'QCLNG Kpler', 'APLNG Kpler','GLNG Kpler','Aust LNG CBM.']
     
        #table 1
        dftable1 = pd.DataFrame(index = index, columns = column)
        for i in column[0:3]:
            dftable1.loc['D+6',i] = dfpivot_mcmd.loc[str(d6), i]
            dftable1.loc['Today',i] = dfpivot_mcmd.loc[str(today), i]
            dftable1.loc['Today-1',i] = dfpivot_mcmd.loc[str(ytd), i]
            dftable1.loc[m,i] = dfpivot_mcmd.loc[str(today.year)+'-'+str(today.month)+'-01':str(today.year)+'-'+str(today.month)+'-'+str(calendar.monthrange(today.year, today.month)[1]), i].mean()
            dftable1.loc[m1,i] = dfpivot_mcmd.loc[str((today - relativedelta(months = 1)).year)+'-'+str((today - relativedelta(months = 1)).month)+'-01':str((today - relativedelta(months = 1)).year)+'-'+str((today - relativedelta(months = 1)).month)+'-'+str(calendar.monthrange((today - relativedelta(months = 1)).year, (today - relativedelta(months = 1)).month)[1]), i].mean()
            dftable1.loc[y1,i] = dfpivot_mcmd.loc[str((today - relativedelta(years = 1)).year)+'-'+str((today - relativedelta(years = 1)).month)+'-01':str((today - relativedelta(years = 1)).year)+'-'+str((today - relativedelta(years = 1)).month)+'-'+str(calendar.monthrange((today - relativedelta(years = 1)).year, (today - relativedelta(years = 1)).month)[1]), i].mean()
        dftable1['AGGREG.'] = dftable1[['APLNG', 'GLNG',	'QCLNG']].sum(axis = 1)
        
        for i in column[0:3]:
            #dftable1.loc['D+6',i] = dfkpler.loc[str(d6), i]
            dftable1.loc['Today',i+' Kpler'] = dfkpler.loc[str(today), i]
            dftable1.loc['Today-1',i+' Kpler'] = dfkpler.loc[str(ytd), i]
            dftable1.loc[m,i+' Kpler'] = dfkpler.loc[str(today.year)+'-'+str(today.month)+'-01':str(today.year)+'-'+str(today.month)+'-'+str(calendar.monthrange(today.year, today.month)[1]), i].mean()
            dftable1.loc[m1,i+' Kpler'] = dfkpler.loc[str((today - relativedelta(months = 1)).year)+'-'+str((today - relativedelta(months = 1)).month)+'-01':str((today - relativedelta(months = 1)).year)+'-'+str((today - relativedelta(months = 1)).month)+'-'+str(calendar.monthrange((today - relativedelta(months = 1)).year, (today - relativedelta(months = 1)).month)[1]), i].mean()
            dftable1.loc[y1,i+' Kpler'] = dfkpler.loc[str((today - relativedelta(years = 1)).year)+'-'+str((today - relativedelta(years = 1)).month)+'-01':str((today - relativedelta(years = 1)).year)+'-'+str((today - relativedelta(years = 1)).month)+'-'+str(calendar.monthrange((today - relativedelta(years = 1)).year, (today - relativedelta(years = 1)).month)[1]), i].mean()
        dftable1['Aust LNG CBM.'] = dftable1[['QCLNG Kpler', 'APLNG Kpler','GLNG Kpler']].sum(axis = 1)
        dftable1.fillna(0, inplace=True)
        dftable1 = dftable1.round(2)
        #table 2
        index2 = ['D+6','Today','Today-1','Monthly Avg.','M-o-M','Y-o-Y']

        dftable2 = pd.DataFrame(index = index2, columns = column)
        dftable2.loc['D+6'] = dftable1.loc['Today-1'] - dftable1.loc['D+6'] 
        dftable2.loc['Today'] = dftable1.loc['Today-1'] - dftable1.loc['Today'] 
        dftable2.loc['Today-1'] = dftable1.loc['Today-1']
        dftable2.loc['Monthly Avg.'] = dftable1.loc['Today-1'] - dftable1.loc[m]
        dftable2.loc['M-o-M'] = dftable1.loc['Today-1'] - dftable1.loc[m1]
        dftable2.loc['Y-o-Y'] = dftable1.loc['Today-1'] - dftable1.loc[y1]
        dftable2 = dftable2.astype('float').round(2)
        
        #table 3
        dftable3 = pd.DataFrame(index = index, columns = column)
        for i in column[0:3]:
            dftable3.loc['D+6',i] = dfutifeed.loc[str(d6), i+' daily uti']
            dftable3.loc['Today',i] = dfutifeed.loc[str(today), i+' daily uti']
            dftable3.loc['Today-1',i] = dfutifeed.loc[str(ytd), i+' daily uti']
            dftable3.loc[m,i] = dfutifeed.loc[str(today.year)+'-'+str(today.month)+'-01':str(today.year)+'-'+str(today.month)+'-'+str(calendar.monthrange(today.year, today.month)[1]), i+' daily uti'].mean()
            dftable3.loc[m1,i] = dfutifeed.loc[str((today - relativedelta(months = 1)).year)+'-'+str((today - relativedelta(months = 1)).month)+'-01':str((today - relativedelta(months = 1)).year)+'-'+str((today - relativedelta(months = 1)).month)+'-'+str(calendar.monthrange((today - relativedelta(months = 1)).year, (today - relativedelta(months = 1)).month)[1]), i+' daily uti'].mean()
            dftable3.loc[y1,i] = dfutifeed.loc[str((today - relativedelta(years = 1)).year)+'-'+str((today - relativedelta(years = 1)).month)+'-01':str((today - relativedelta(years = 1)).year)+'-'+str((today - relativedelta(years = 1)).month)+'-'+str(calendar.monthrange((today - relativedelta(years = 1)).year, (today - relativedelta(years = 1)).month)[1]), i+' daily uti'].mean()
        dftable3.loc['Today','AGGREG.'] = dfutifeed.loc[str(today), 'Aust LNG CBM']
        dftable3.loc['Today-1','AGGREG.'] = dfutifeed.loc[str(ytd), 'Aust LNG CBM']
        dftable3.loc[m,'AGGREG.'] = dfutifeed.loc[str(today.year)+'-'+str(today.month)+'-01':str(today.year)+'-'+str(today.month)+'-'+str(calendar.monthrange(today.year, today.month)[1]), 'Aust LNG CBM'].mean()
        dftable3.loc[m1,'AGGREG.'] = dfutifeed.loc[str((today - relativedelta(months = 1)).year)+'-'+str((today - relativedelta(months = 1)).month)+'-01':str((today - relativedelta(months = 1)).year)+'-'+str((today - relativedelta(months = 1)).month)+'-'+str(calendar.monthrange((today - relativedelta(months = 1)).year, (today - relativedelta(months = 1)).month)[1]), 'Aust LNG CBM'].mean()
        dftable3.loc[y1,'AGGREG.'] = dfutifeed.loc[str((today - relativedelta(years = 1)).year)+'-'+str((today - relativedelta(years = 1)).month)+'-01':str((today - relativedelta(years = 1)).year)+'-'+str((today - relativedelta(years = 1)).month)+'-'+str(calendar.monthrange((today - relativedelta(years = 1)).year, (today - relativedelta(years = 1)).month)[1]), 'Aust LNG CBM'].mean()
        
        
        
        for i in column[0:3]:
            dftable3.loc['D+6',i+' Kpler'] = np.nan#dfuti.loc[str(d6), i+'_Kpler']
            dftable3.loc['Today',i+' Kpler'] = dfuti.loc[str(today), i+'_Kpler']
            dftable3.loc['Today-1',i+' Kpler'] = dfuti.loc[str(ytd), i+'_Kpler']
            dftable3.loc[m,i+' Kpler'] = dfuti.loc[str(today.year)+'-'+str(today.month)+'-01':str(today.year)+'-'+str(today.month)+'-'+str(calendar.monthrange(today.year, today.month)[1]), i+'_Kpler'].mean()
            dftable3.loc[m1,i+' Kpler'] = dfuti.loc[str((today - relativedelta(months = 1)).year)+'-'+str((today - relativedelta(months = 1)).month)+'-01':str((today - relativedelta(months = 1)).year)+'-'+str((today - relativedelta(months = 1)).month)+'-'+str(calendar.monthrange((today - relativedelta(months = 1)).year, (today - relativedelta(months = 1)).month)[1]), i+'_Kpler'].mean()
            dftable3.loc[y1,i+' Kpler'] = dfuti.loc[str((today - relativedelta(years = 1)).year)+'-'+str((today - relativedelta(years = 1)).month)+'-01':str((today - relativedelta(years = 1)).year)+'-'+str((today - relativedelta(years = 1)).month)+'-'+str(calendar.monthrange((today - relativedelta(years = 1)).year, (today - relativedelta(years = 1)).month)[1]), i+'_Kpler'].mean()
        dftable3.loc['Today','Aust LNG CBM.'] = dfuti.loc[str(today), 'Aust LNG CBM_Kpler']
        dftable3.loc['Today-1','Aust LNG CBM.'] = dfuti.loc[str(ytd), 'Aust LNG CBM_Kpler']
        dftable3.loc[m,'Aust LNG CBM.'] = dfuti.loc[str(today.year)+'-'+str(today.month)+'-01':str(today.year)+'-'+str(today.month)+'-'+str(calendar.monthrange(today.year, today.month)[1]), 'Aust LNG CBM_Kpler'].mean()
        dftable3.loc[m1,'Aust LNG CBM.'] = dfuti.loc[str((today - relativedelta(months = 1)).year)+'-'+str((today - relativedelta(months = 1)).month)+'-01':str((today - relativedelta(months = 1)).year)+'-'+str((today - relativedelta(months = 1)).month)+'-'+str(calendar.monthrange((today - relativedelta(months = 1)).year, (today - relativedelta(months = 1)).month)[1]), 'Aust LNG CBM_Kpler'].mean()
        dftable3.loc[y1,'Aust LNG CBM.'] = dfuti.loc[str((today - relativedelta(years = 1)).year)+'-'+str((today - relativedelta(years = 1)).month)+'-01':str((today - relativedelta(years = 1)).year)+'-'+str((today - relativedelta(years = 1)).month)+'-'+str(calendar.monthrange((today - relativedelta(years = 1)).year, (today - relativedelta(years = 1)).month)[1]), 'Aust LNG CBM_Kpler'].mean()
        dftable3.fillna(0, inplace=True)
        dftable3 = dftable3.round(2)
        #table 4
        dftable4 = pd.DataFrame(index = index2, columns = column)
        dftable4.loc['D+6', column[0:4]] = dftable3.loc['Today-1',column[0:4]] - dftable3.loc['D+6',column[0:4]] 
        dftable4.loc['Today'] = dftable3.loc['Today-1'] - dftable3.loc['Today'] 
        dftable4.loc['Today-1'] = dftable3.loc['Today-1']
        dftable4.loc['Monthly Avg.'] = dftable3.loc['Today-1'] - dftable3.loc[m]
        dftable4.loc['M-o-M'] = dftable3.loc['Today-1'] - dftable3.loc[m1]
        dftable4.loc['Y-o-Y'] = dftable3.loc['Today-1'] - dftable3.loc[y1]
        
        dftable4 = dftable4.astype('float').round(2)
        #print(dftable1)
        #print(dftable2)
        #print(dftable3)
        #print(dftable4.round(2))
        return dftable1, dftable2, dftable3, dftable4 
      
    
    def plot_tables(dftable1, dftable2, dftable3, dftable4):
        
        #bgcolor = colorlover.scales['10']['div']['RdYlGn']
        #print(dftable2.info())
        
        fig1 = go.Figure(data=[go.Table(
                                        header=dict(values=['Mcm/d'] + list(dftable1.columns),
                                                    fill_color='paleturquoise',
                                                    align='left'),
                                        cells=dict(values=[dftable1.index] + [dftable1[pm] for pm in dftable1.columns],
                                                   fill_color='lavender',
                                                   #font=dict(color = ['black','black']+dfcolor.values.tolist()),
                                                   align='left'))
                                    ])
    
        py.plot(fig1, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/AUS feedgas/table1.html', auto_open=False)
        fig1.write_image('U:/Trading - Gas/LNG/LNG website/analysis/AUS feedgas/table1.png')
        
        fig2 = go.Figure(data=[go.Table(
                                        header=dict(values=['Mcm/d'] + list(dftable2.columns),
                                                    fill_color='paleturquoise',
                                                    align='left'),
                                        cells=dict(values=[dftable2.index] + [dftable2[pm] for pm in dftable2.columns],
                                                   fill_color='lavender',
                                                   #font=dict(color = ['black','black']+dfcolor.values.tolist()),
                                                   align='left'))
                                    ])
    
        py.plot(fig2, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/AUS feedgas/table2.html', auto_open=False)
        fig2.write_image('U:/Trading - Gas/LNG/LNG website/analysis/AUS feedgas/table2.png')
        #print(dftable3)
        fig3 = go.Figure(data=[go.Table(
                                        header=dict(values=['Mcm/d'] + list(dftable3.columns),
                                                    fill_color='paleturquoise',
                                                    align='left'),
                                        cells=dict(values=[dftable3.index] + [dftable3[pm] for pm in dftable3.columns],
                                                   fill_color='lavender',
                                                   #font=dict(color = ['black','black']+dfcolor.values.tolist()),
                                                   align='left'))
                                    ])
    
        py.plot(fig3, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/AUS feedgas/table3.html', auto_open=False)
        fig3.write_image('U:/Trading - Gas/LNG/LNG website/analysis/AUS feedgas/table3.png')
        
        fig4 = go.Figure(data=[go.Table(
                                        header=dict(values=['Mcm/d'] + list(dftable4.columns),
                                                    fill_color='paleturquoise',
                                                    align='left'),
                                        cells=dict(values=[dftable4.index] + [dftable4[pm] for pm in dftable4.columns],
                                                   fill_color='lavender',
                                                   #font=dict(color = ['black','black']+dfcolor.values.tolist()),
                                                   align='left'))
                                    ])
    
        py.plot(fig4, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/AUS feedgas/table4.html', auto_open=False)
        fig4.write_image('U:/Trading - Gas/LNG/LNG website/analysis/AUS feedgas/table4.png')
   
    def update():
        dfpivot_mcmd, dfkpler = Aus_Feed.get_data()
        Aus_Feed.mcmchart(dfpivot_mcmd)
        dfutifeed, dfuti = Aus_Feed.untilasation(dfpivot_mcmd, dfkpler)
        Aus_Feed.utiyoy(dfutifeed)
        dftable1, dftable2, dftable3, dftable4 = Aus_Feed.tables(dfpivot_mcmd, dfuti,dfutifeed, dfkpler)
        Aus_Feed.plot_tables(dftable1, dftable2, dftable3, dftable4)

#Aus_Feed.update()