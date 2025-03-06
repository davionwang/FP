# -*- coding: utf-8 -*-
"""
Created on Thu Feb 23 09:29:14 2023

@author: SVC-GASQuant2-Prod
"""




import CErename
import pandas as pd
from CEtools import CEtools
import sys
sys.path.append('C:\\Users\SVC-GASQuant2-Prod\YWwebsite\Python Scripts\class') 
import numpy as np
import datetime
from dateutil.relativedelta import relativedelta
import plotly.graph_objs as go
import plotly.offline as py


#import CEtools
from CEtools import *
ce=CEtools.CElink('swasti@freepoint.com','kfGa5uwL1+')
pd.set_option('display.max_columns',20)
from DBtoDF import DBTOPD


class Algeria_Flow():
    
    def get_data():
        
        today = datetime.date.today()
        
        DateFrom='01_Jan_'+str(today.year-3)        
        DateTo=today.strftime('%d_%b_%Y')
        
        #ce=CElink(user_name,password)
        #monthlystats = ce.eugasmonthly(id=51) #total monthly data
        
        cepipe = ce.eugasseries(id='26403,	26393,	25189', dateFrom=DateFrom,dateTo=DateTo,unit='mcm',nd='10')
        cepipe['Date'] = pd.to_datetime(cepipe['Date'])
        cepipe.set_index('Date', inplace=True)
        cepipe.drop(['DateExcel'],axis=1, inplace=True)
        #rename 
        rename=CErename.CEid(cepipe)
        cepipe=rename.replace()
        cepipe=cepipe.fillna(method='ffill')
    
        #print(cepipe)
        
        
        dfkpler10 =  DBTOPD('PRD-DB-SQL-211','[LNG]','[ana]','[SupplyPlant]').sql_to_df()
        dfkpler10.set_index('Date', inplace=True)
        dfkpler10 = dfkpler10.loc[str(today.year-3)+'-01-01':,['Bethioua','Skikda']]
        #print(dfkpler10)
        
        dfal = pd.concat([dfkpler10, cepipe], axis=1)
        dfal['export'] = dfal.sum(axis=1)
        dfal = dfal.round(2)
        #print(dfal)
        
        return dfal
    
    def chart(dfal):
        
        today = datetime.date.today()
        
        figexport1 = go.Figure()
        figexport1.add_trace(go.Scatter(x=dfal.index, y=dfal.loc[:,'Bethioua'],
                            mode='lines',
                            name='Bethioua',
                            stackgroup='one',
                            #line=dict(color='red', dash='solid')
                            ))
        figexport1.add_trace(go.Scatter(x=dfal.index, y=dfal.loc[:,'Skikda'],
                            mode='lines',
                            name='Skikda',
                            stackgroup='one',
                            #line=dict(color='red', dash='solid')
                            ))
        figexport1.add_trace(go.Scatter(x=dfal.index, y=dfal.loc[:,'Almeria_netFlow_Enagas_blend'],
                            mode='lines',
                            name='SP_Almeria',
                            stackgroup='one',
                            #line=dict(color='red', dash='solid')
                            ))
        figexport1.add_trace(go.Scatter(x=dfal.index, y=dfal.loc[:,'Tarifa_netFlow_Enagas_blend'],
                            mode='lines',
                            name='SP_Tarifa',
                            stackgroup='one',
                            #line=dict(color='red', dash='solid')
                            ))
        figexport1.add_trace(go.Scatter(x=dfal.index, y=dfal.loc[:,'Mazara del Vallo_netFlow_Snam_blend'],
                            mode='lines',
                            name='Italy_pipe',
                            stackgroup='one',
                            #line=dict(color='red', dash='solid')
                            ))
        figexport1.update_layout(
             autosize=True,
             showlegend=True,
             #colorscale='RdBu',
             legend=dict(x=0, y=-0.2),
             legend_orientation="h",
             title_text='Exports '+str(today)+' ,Mcm/d',
             #xaxis = dict(dtick="M1"),
             hovermode='x unified',
             plot_bgcolor = 'white',
             template='ggplot2'  
         )
        py.plot(figexport1, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/Algeria flow/export1.html', auto_open=False)
        
        
        
        figpipelng = go.Figure()
        figpipelng.add_trace(go.Scatter(x=dfal.index, y=dfal.loc[:,['Bethioua','Skikda']].sum(axis=1),
                            mode='lines',
                            name='LNG',
                            #stackgroup='one',
                            #line=dict(color='red', dash='solid')
                            ))
        figpipelng.add_trace(go.Scatter(x=dfal.index, y=dfal.loc[:,['Almeria_netFlow_Enagas_blend','Tarifa_netFlow_Enagas_blend','Mazara del Vallo_netFlow_Snam_blend']].sum(axis=1),
                            mode='lines',
                            name='Pipe',
                            #stackgroup='one',
                            #line=dict(color='red', dash='solid')
                            ))
        
        figpipelng.update_layout(
             autosize=True,
             showlegend=True,
             #colorscale='RdBu',
             legend=dict(x=0, y=-0.2),
             legend_orientation="h",
             title_text='Pipe vs. LNG '+str(today)+' ,Mcm/d',
             #xaxis = dict(dtick="M1"),
             hovermode='x unified',
             plot_bgcolor = 'white',
             template='ggplot2'  
         )
        py.plot(figpipelng, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/Algeria flow/pipelng.html', auto_open=False)
        
        
        figpipe = go.Figure()
        figpipe.add_trace(go.Scatter(x=dfal.index, y=dfal.loc[:,'Almeria_netFlow_Enagas_blend'],
                            mode='lines',
                            name='Almeria_netFlow_Enagas_blend',
                            #stackgroup='one',
                            #line=dict(color='red', dash='solid')
                            ))
        figpipe.add_trace(go.Scatter(x=dfal.index, y=dfal.loc[:,'Tarifa_netFlow_Enagas_blend'],
                            mode='lines',
                            name='Tarifa_netFlow_Enagas_blend',
                            #stackgroup='one',
                            #line=dict(color='red', dash='solid')
                            ))
        figpipe.add_trace(go.Scatter(x=dfal.index, y=dfal.loc[:,'Mazara del Vallo_netFlow_Snam_blend'],
                            mode='lines',
                            name='Mazara del Vallo_netFlow_Snam_blend',
                            #stackgroup='one',
                            #line=dict(color='red', dash='solid')
                            ))
        
        figpipe.update_layout(
             autosize=True,
             showlegend=True,
             #colorscale='RdBu',
             legend=dict(x=0, y=-0.2),
             legend_orientation="h",
             title_text='Pipe export '+str(today)+' ,Mcm/d',
             #xaxis = dict(dtick="M1"),
             hovermode='x unified',
             plot_bgcolor = 'white',
             template='ggplot2'  
         )
        py.plot(figpipe, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/Algeria flow/figpipeall.html', auto_open=False)
        
        
        
        
    def chartyoy(dfal):
        
        today = datetime.date.today()
        
        column = dfal.columns
        dfal['year'] = dfal.index.year
        dfal['date'] = dfal.index.strftime('%m-%d')
        #print(dfal)
        
        for i in column:
            dfyoy = dfal[[i,'year','date']].set_index(['year','date']).unstack(0)
            dfyoy.columns = dfyoy.columns.droplevel(0)
            index =list(map(lambda x:str(today.year)+'-'+x,dfyoy.index.to_list()))
            dfyoy.fillna(method='bfill', inplace=True)
            #print(dfyoy.loc['02-25':])
            fig = go.Figure()
            for j in dfyoy.columns:
                fig.add_trace(go.Scatter(x=index, y=dfyoy.loc[:,j],
                                mode='lines',
                                name=j,
                                #line=dict(color='red', dash='solid')
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
            py.plot(fig, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/Algeria flow/'+i+'.html', auto_open=False)
        
        
        dfsppipe = dfal[['year','date','Almeria_netFlow_Enagas_blend','Tarifa_netFlow_Enagas_blend']].copy()
        dfsppipe['SP Pipe'] = dfsppipe[['Almeria_netFlow_Enagas_blend','Tarifa_netFlow_Enagas_blend']].sum(axis=1)
        dfsppipeyoy = dfsppipe[['SP Pipe','year','date']].set_index(['year','date']).unstack(0)
        dfsppipeyoy.columns = dfsppipeyoy.columns.droplevel(0)
        dfsppipeyoy.fillna(method='bfill', inplace=True)
        figsppipe = go.Figure()
        for j in dfsppipeyoy.columns:
            figsppipe.add_trace(go.Scatter(x=index, y=dfsppipeyoy.loc[:,j],
                    mode='lines',
                    name=j,
                    #line=dict(color='red', dash='solid')
                    ))
        
        figsppipe.update_layout(
                            autosize=True,
                            showlegend=True,
                            #colorscale='RdBu',
                            legend=dict(x=0, y=-0.2),
                            legend_orientation="h",
                            title_text='SP Pipe '+str(today)+' ,Mcm/d',
                            xaxis = dict(dtick="M1"),
                            hovermode='x unified',
                            plot_bgcolor = 'white',
                            template='ggplot2'  
                            )
        py.plot(figsppipe, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/Algeria flow/SP Pipe.html', auto_open=False)
   
        
   
        dflng = dfal[['year','date','Bethioua','Skikda']].copy()
        dflng['LNG'] = dflng[['Bethioua','Skikda']].sum(axis=1)
        dflngyoy = dflng[['LNG','year','date']].set_index(['year','date']).unstack(0)
        dflngyoy.columns = dflngyoy.columns.droplevel(0)
        dflngyoy.fillna(method='bfill', inplace=True)
        figlng = go.Figure()
        for j in dflngyoy.columns:
            figlng.add_trace(go.Scatter(x=index, y=dflngyoy.loc[:,j],
                    mode='lines',
                    name=j,
                    #line=dict(color='red', dash='solid')
                    ))
        
        figlng.update_layout(
                            autosize=True,
                            showlegend=True,
                            #colorscale='RdBu',
                            legend=dict(x=0, y=-0.2),
                            legend_orientation="h",
                            title_text='LNG '+str(today)+' ,Mcm/d',
                            xaxis = dict(dtick="M1"),
                            hovermode='x unified',
                            plot_bgcolor = 'white',
                            template='ggplot2'  
                            )
        py.plot(figlng, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/Algeria flow/LNG.html', auto_open=False)
   
    
        dfpipe = dfal[['year','date','Almeria_netFlow_Enagas_blend','Tarifa_netFlow_Enagas_blend','Mazara del Vallo_netFlow_Snam_blend']].copy()
        dfpipe['Pipe'] = dfpipe[['Almeria_netFlow_Enagas_blend','Tarifa_netFlow_Enagas_blend','Mazara del Vallo_netFlow_Snam_blend']].sum(axis=1)
        dfpipeyoy = dfpipe[['Pipe','year','date']].set_index(['year','date']).unstack(0)
        dfpipeyoy.columns = dfpipeyoy.columns.droplevel(0)
        dfpipeyoy.fillna(method='bfill', inplace=True)
        figpipe = go.Figure()
        for j in dfpipeyoy.columns:
            figpipe.add_trace(go.Scatter(x=index, y=dfpipeyoy.loc[:,j],
                    mode='lines',
                    name=j,
                    #line=dict(color='red', dash='solid')
                    ))
        
        figpipe.update_layout(
                            autosize=True,
                            showlegend=True,
                            #colorscale='RdBu',
                            legend=dict(x=0, y=-0.2),
                            legend_orientation="h",
                            title_text='Pipe '+str(today)+' ,Mcm/d',
                            xaxis = dict(dtick="M1"),
                            hovermode='x unified',
                            plot_bgcolor = 'white',
                            template='ggplot2'  
                            )
        py.plot(figpipe, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/Algeria flow/Pipe.html', auto_open=False)
   
        
    
    def table(dfal):
        
        today = datetime.date.today()
        
        dftable = dfal.copy()
        
        dftable['Pipe'] = dftable[['Almeria_netFlow_Enagas_blend','Tarifa_netFlow_Enagas_blend','Mazara del Vallo_netFlow_Snam_blend']].sum(axis=1)
        dftable['Sp'] =  dftable[['Almeria_netFlow_Enagas_blend','Tarifa_netFlow_Enagas_blend']].sum(axis=1)
        dftable['SP_Almeria'] = dftable['Almeria_netFlow_Enagas_blend']
        dftable['SP_Tarifa'] = dftable['Tarifa_netFlow_Enagas_blend']
        dftable['Italy'] = dftable['Mazara del Vallo_netFlow_Snam_blend']
        dftable['LNG'] = dftable[['Bethioua','Skikda']].sum(axis=1)
        dftable['Total'] = dftable['LNG'] + dftable['Pipe']
        
        #dftable.index = pd.to_datetime(dftable.index)
        #print(dftable)
        
        dftablechart = pd.DataFrame(index=['SP_Almeria','SP_Tarifa','Italy','Skikda','Bethioua','Sp','Pipe','LNG','Total'], columns=[str(today), str(today-relativedelta(days=1)),str(today-relativedelta(months=1)), str(today-relativedelta(years=1)), 'DoD', 'MoM','YoY'])
        for i in dftablechart.index:
            #print(dftable.loc[str(today), i])
            dftablechart.loc[i,str(today)] = round(dftable.loc[str(today), i],2)
            dftablechart.loc[i,str(today-relativedelta(days=1))] = round(dftable.loc[str(today-relativedelta(days=1)), i],2)
            dftablechart.loc[i,str(today-relativedelta(months=1))] = round(dftable.loc[str(today-relativedelta(months=1)), i],2)
            dftablechart.loc[i,str(today-relativedelta(years=1))] = round(dftable.loc[str(today-relativedelta(years=1)), i],2)
            dftablechart.loc[i,'DoD'] = round(dftable.loc[str(today), i] - dftable.loc[str(today-relativedelta(days=1)), i],2)
            dftablechart.loc[i,'MoM'] = round(dftable.loc[str(today), i] - dftable.loc[str(today-relativedelta(months=1)), i],2)
            dftablechart.loc[i,'YoY'] = round(dftable.loc[str(today), i] - dftable.loc[str(today-relativedelta(years=1)), i],2)
            
        dftablechart = round(dftablechart,2)
        dfdelta_color = dftablechart.copy()
        dfdelta_color_cell = dftablechart.copy()
        #print(dfdelta_color)
        dfdelta_color.loc[:,0:4] = 'paleturquoise'
        dfdelta_color_cell.loc[:,0:4] = 'black'
        #print(dfdelta_color)
        
        for ism in dfdelta_color.index:
            for jsm in dfdelta_color.columns[4:]:
                #print(dfdelta_color.loc[ism,jsm])
                if dfdelta_color.loc[ism,jsm] > 0:
                    dfdelta_color.loc[ism,jsm] = 'red'
                    dfdelta_color_cell.loc[ism,jsm] = 'white'
                elif dfdelta_color.loc[ism,jsm] < 0:
                    dfdelta_color.loc[ism,jsm] = 'blue'
                    dfdelta_color_cell.loc[ism,jsm] = 'white'
                elif dfdelta_color.loc[ism,jsm] == 0:
                    dfdelta_color.loc[ism,jsm] = 'black'
                    dfdelta_color_cell.loc[ism,jsm] = 'white'
                else:
                    dfdelta_color.loc[ism,jsm] = 'LightGrey'
                    dfdelta_color_cell.loc[ism,jsm] = 'black'
        #print(dfdelta_color)
        #print(dfdelta_color_cell)

        figtable = go.Figure(data=[go.Table(
                    header=dict(values = [[' ']]+dftablechart.columns.to_list(),
                                fill_color=['SkyBlue']*dftablechart.shape[1],
                                align='center'),
                    cells=dict(values = [['SP_Almeria','SP_Tarifa','Italy','Skikda','Bethioua','Sp','Pipe','LNG','Total']] + [dftablechart.loc[:,pm].to_list() for pm in dftablechart.columns],
                               #fill_color='lavender',
                               fill=dict(color=[['LightGrey','LightGrey', 'LightGrey', 'LightGrey', 'LightGrey']]+dfdelta_color.T.values.tolist()),
                               font=dict(color=[['black','black','black','black','black']]+dfdelta_color_cell.T.values.tolist()),
                               align='center'))
                ])
        figtable.update_layout(title_text='Algeria flows '+str(today))
        py.plot(figtable, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/Algeria flow/table.html', auto_open=False)

        #print(dftablechart)

    def update():    
        dfal = Algeria_Flow.get_data()
        Algeria_Flow.chart(dfal)
        Algeria_Flow.chartyoy(dfal)
        Algeria_Flow.table(dfal)