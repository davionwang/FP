# -*- coding: utf-8 -*-
"""
Created on Thu Dec  1 09:11:15 2022

@author: SVC-GASQuant2-Prod
"""


#v1 add %table


import sys
sys.path.append('C:\\Users\SVC-GASQuant2-Prod\YWwebsite\Python Scripts\class') 
from DBtoDF import DBTOPD
import pandas as pd
import plotly.graph_objs as go
import plotly.offline as py
import datetime
from dateutil.relativedelta import relativedelta


class Brazil_hydro():
    
    
    def get_data():
        df = DBTOPD.get_brizal_hydro()
        df.set_index('DAT1', inplace=True)
        df['value'] = df['value'].astype('float')
        #print(df)
        
        
        dfhist = pd.read_excel('C:\\Users\SVC-GASQuant2-Prod\YWwebsite\data\Brazil hist.xlsx', sheet_name=0)
        dfhist.set_index('2019 Date', inplace=True)
        dfhist = dfhist.loc[:'2019-12-31'].round(2)
        dfhist.fillna(method='ffill', inplace=True)
        
        dfhist['ave. 2011-2021'] = dfhist.mean(axis=1)
        dfhist['max'] = dfhist.max(axis=1)
        dfhist['min'] = dfhist.min(axis=1)
        #print(dfhist)
        
        today = datetime.date.today()
        #
        df['year'] = df.index.year
        df['date'] = df.index.strftime('%m-%d')
        dfyoy = df.set_index(['year','date']).value.unstack(0)
        dfyoy.index = pd.to_datetime(dfyoy.index, format='%m-%d')
        #print(dfyoy.index)
        #print(dfyoy)
        dffull = pd.DataFrame(index = pd.date_range(start = '1900-01-01', end='1900-12-31'), columns=dfyoy.columns)
        for i in dfyoy.index:
            dffull.loc[i] = dfyoy.loc[i]
        
        #print(dffull)
        #print(dfhist.columns.to_list())
        #dffull = pd.DataFrame(index = pd.date_range(start = str(today.year)+'-01-01', end=str(today.year)+'-12-31'), columns=[str(today.year)])
        #for i in dffull.index:
        #    dffull.loc[i,str(today.year)] = df.loc[i,'value']
        #print(dffull)
        #dffull[dfhist.columns] = dfhist.values            
        #dffull = dffull.interpolate()
        
        dffull[dfhist.columns.to_list()] = dfhist.values
        #dffull = pd.concat([dffull, dfhist], axis=1, ignore_index=True)
        #print(dffull)
            
        ma10 = DBTOPD('PRD-DB-SQL-211','[LNG]','[ana]','[DemandCountry]').sql_to_df()
        ma10 = ma10[['Date','Brazil']]
        ma10.set_index('Date', inplace=True)
        ma10.sort_index(inplace=True)
        
        ma30 = DBTOPD('PRD-DB-SQL-211','[LNG]','[ana]','[DemandCountry30]').sql_to_df()
        ma30 = ma30[['Date','Brazil']]
        ma30.set_index('Date', inplace=True)
        ma30.sort_index(inplace=True)
        #print(ma10)
        
        return df, dffull, ma10, ma30
    
    
    def chart(dffull):
        
        fig1 = go.Figure()


        for i in dffull.columns[:-3]:
            fig1.add_trace(go.Scatter(
            x=dffull.index,
            y=dffull[i].values,
            name = i, # Style name/legend entry with html tags
            connectgaps=True # override default to connect the gaps
        ))
        fig1.add_trace(go.Scatter(
            x=dffull.index,
            y=dffull['ave. 2011-2021'].values,
            name = 'ave. 2011-2021', 
            line=dict(color='black', dash='solid'),
            connectgaps=True # override default to connect the gaps
        ))
        
        fig1.update_layout(
                    autosize=True,
                    #showlegend=True,
                    #legend=dict(x=0, y=-0.2),
                    #legend_orientation="h",
                    #legend_tracegroupgap = 288,
                    title_text='Brazil-SE Hydro reservoir fill level, ONS',
                    yaxis_title="%fill",
                    xaxis = dict(dtick="M1"),
                    hovermode='x unified',
                    xaxis_tickformat = '%d/%B',
                    #height=888,
                    yaxis = dict(showgrid=True, gridcolor='lightgrey'),
                    plot_bgcolor = 'white',
                    #template='ggplot2'  ,
                    showlegend = True
                    
                    
                
                )
        
          
        py.plot(fig1, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/Brazil hydro/chart1.html', auto_open=False)
        fig1.write_image('U:/Trading - Gas/LNG/LNG website/analysis/Brazil hydro/chart1.png')
        
        #print(dffull.columns.tolist())
        show_list = [datetime.date.today().year,'2012','2013']
        hidden_list = dffull.columns.tolist()
        hidden_list.remove('max')
        hidden_list.remove('min')
        for i in show_list:
            hidden_list.remove(i)
        
        fig2 = go.Figure()
        
        
        for i in show_list:
            fig2.add_trace(go.Scatter(
            x=dffull.index,
            y=dffull[i].values,
            name = i, 
            connectgaps=True # override default to connect the gaps
        ))
            
        for i in hidden_list:
            fig2.add_trace(go.Scatter(
            x=dffull.index,
            y=dffull[i].values,
            name = i, 
            visible='legendonly',
            connectgaps=True
        ))
            
        fig2.add_trace(go.Scatter(x=dffull.index,y=dffull['max'],
                            fill='tonexty',
                            fillcolor='rgba(65,105,225,0)',
                            line_color='rgba(65,105,225,0)',
                            showlegend=False,
                            name='Min/Max 2011-2021'
                            ))
                
        fig2.add_trace(go.Scatter(x=dffull.index,y=dffull['min'],
                            fill='tonexty',
                            fillcolor='rgba(65,105,225,0.1)',
                            line_color='rgba(65,105,225,0)',
                            showlegend=True,
                            name='Min/Max 2011-2021'
                            ))
        
        fig2.update_layout(
                    autosize=True,
                    #showlegend=True,
                    #legend=dict(x=0, y=-0.2),
                    #legend_orientation="h",
                    #legend_tracegroupgap = 288,
                    title_text='Brazil-SE Hydro reservoir fill level, ONS '+str(datetime.date.today()),
                    yaxis_title="%fill",
                    xaxis = dict(dtick="M1"),
                    hovermode='x unified',
                    xaxis_tickformat = '%d/%B',
                    #height=888,
                    yaxis = dict(showgrid=True, gridcolor='lightgrey'),
                    plot_bgcolor = 'white',
                    #template='ggplot2'  ,
                    showlegend = True
                    
                    
                
                )
        
          
        py.plot(fig2, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/Brazil hydro/chart2.html', auto_open=False)
        fig2.write_image('U:/Trading - Gas/LNG/LNG website/analysis/Brazil hydro/chart2.png')
        
    
    def table(df, dffull, ma10, ma30):
        
        #print(df.index[-1])
        
        today = datetime.date.today()
        #d0 = dffull[today.year].loc[~dffull[today.year].isnull()].index[-1].date()
        d0 = df.index[-1]
        d1 = d0 - relativedelta(days=1)
        d7 = d0 - relativedelta(days=7)
        m1 = today - relativedelta(months=1)
        y1 = today - relativedelta(years=1)
       
        df = pd.DataFrame(index = [d0, today, d1, d7, m1, y1], columns = ['fill level','Kpler MA10', 'Kpler MA30'])
        for i in df.index:
            if i.year == today.year:
                df.loc[i, 'fill level'] = format(dffull.loc['1900-'+str(i.month)+'-'+str(i.day),today.year]/100, '.2%')
                df.loc[i, 'Kpler MA10'] = ma10.loc[str(i),'Brazil'].round(2)
                df.loc[i, 'Kpler MA30'] = ma30.loc[str(i),'Brazil'].round(2)
            else:
                df.loc[i, 'fill level'] =format( dffull.loc['1900-'+str(i.month)+'-'+str(i.day), today.year-1]/100, '.2%')
                df.loc[i, 'Kpler MA10'] = ma10.loc[str(i),'Brazil'].round(2)
                df.loc[i, 'Kpler MA30'] = ma30.loc[str(i),'Brazil'].round(2)
        #print(df)
        
        figtable = go.Figure(data=[go.Table(
                    header=dict(values = [' ','Date']+df.columns.to_list(),
                                #fill_color=['paleturquoise']+['paleturquoise']*14+['SkyBlue']*len(week_index),
                                align='center'),
                    cells=dict(values = [['Latest date','today','today-1','today-7','month-1','year-1']]+[df.index.to_list()]+[df.loc[:,pm].to_list() for pm in df.columns],#delta_table_all.columns],
                               #fill_color='lavender',
                               #fill=dict(color=[['white', 'white', 'white', 'white', 'LightGrey', 'white', 'white', 'white', 'LightGrey', 'white', 'white', 'LightGrey', 'white', 'white', 'white', 'white', 'white', 'LightGrey', 'white', 'white', 'white', 'white', 'white', 'white', 'LightGrey', 'white', 'white', 'white', 'white', 'white', 'LightGrey', 'white']]+dfdelta_color_cell.values.tolist()),
                               #font=dict(color=['black']+dfdelta_color.values.tolist()),
                               align='center'))
                ])
        figtable.update_layout(title_text='Brazil-SE Hydro reservoir fill level and Kpler '+str(today))
    
        py.plot(figtable, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/Brazil hydro/table.html', auto_open=False)
        figtable.write_image('U:/Trading - Gas/LNG/LNG website/analysis/Brazil hydro/table.png')
    
    
    def machart(ma10):
        
        today = datetime.date.today()
        #print(ma10)
        df = ma10.copy()
        df['year'] = ma10.index.year
        df['date'] = ma10.index.strftime('%m-%d')
        dfyoy = df.set_index(['year','date']).Brazil.unstack(0)
        #dfyoy.index = pd.to_datetime(dfyoy.index)
        #print(dfyoy.index)
        dfyoy = dfyoy.round(2)
        index = pd.date_range(start = str(today.year)+'-01-01', end = str(today.year)+'-12-31')
        
        
        fig =  go.Figure()

        for i in dfyoy.columns[0:-3]:
            fig.add_trace(go.Scatter(
            x=index,
            y=dfyoy[i],
            name = i, 
            visible = 'legendonly'
            #connectgaps=True # override default to connect the gaps
        ))
            
        for i in dfyoy.columns[-3:]:
            fig.add_trace(go.Scatter(
            x=index,
            y=dfyoy[i],
            name = i, 
            #connectgaps=True # override default to connect the gaps
        ))
       
        
        fig.add_trace(go.Scatter( x=index,y=dfyoy.iloc[:,-6:-1].min(axis=1),
                   fill='tonexty',
                   fillcolor='rgba(65,105,225,0)',
                   line_color='rgba(65,105,225,0)',
                   showlegend=False,
                   name='5yrs range Min'
                   ))
           
        fig.add_trace(go.Scatter(x=index,y=dfyoy.iloc[:,-6:-1].max(axis=1),
                fill='tonexty',
                fillcolor='rgba(191,191,191,0.7)',
                line_color='rgba(65,105,225,0)',
                showlegend=True,
                name='5 yrs range'
                ))
        
        fig.update_layout(
                    autosize=True,
                    #showlegend=True,
                    #legend=dict(x=0, y=-0.2),
                    #legend_orientation="h",
                    #legend_tracegroupgap = 288,
                    title_text='Brazil LNG Import MA10 (Mcm)',
                    #yaxis_title="%fill",
                    xaxis = dict(dtick="M1"),
                    hovermode='x unified',
                    xaxis_tickformat = '%d/%B',
                    #height=888,
                    yaxis = dict(showgrid=True, gridcolor='lightgrey'),
                    plot_bgcolor = 'white',
                    #template='ggplot2'  ,
                    showlegend = True
                    
                    
                
                )
        
        py.plot(fig, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/Brazil hydro/chartma10.html', auto_open=False)
        fig.write_image('U:/Trading - Gas/LNG/LNG website/analysis/Brazil hydro/chartma10.png')
        
        
        
    
    def update():
        
        df, dffull, ma10, ma30 = Brazil_hydro.get_data()
        Brazil_hydro.chart(dffull)
        Brazil_hydro.table(df, dffull, ma10, ma30)
        Brazil_hydro.machart(ma10)
        
        
#Brazil_hydro.update()