# -*- coding: utf-8 -*-
"""
Created on Sun Jun 22 11:05:10 2025

@author: 岳峰
"""

import pandas as pd
import numpy as np
from blp import blp
import datetime
from dateutil.relativedelta import relativedelta
import plotly.offline as py
import plotly.graph_objs as go 
from plotly.subplots import make_subplots

import dash
from dash import html
from dash import dcc
from dash.dependencies import Input, Output, State, MATCH, ALL

import sys
sys.path.append('xxx')
from common_function import Common_Data
from grid_search_V2 import Grid_Search_signal

pd.set_option('display.max_columns',20)


class tech_dash():
    
    def get_dropdown_list(values):
        
        today = datetime.date.today()
        columns_list = []
        
        #month
        for i in range(0, 12):
            columns_list.append((today + relativedelta(months = i+1)).strftime('%b') + '-' + str((today + relativedelta(months = i+1)).year)[2:])
         
            
        #quarter
        qa_ticker_list = []
        if 4 <= datetime.date.today().month <=6:
            
            qa1_ticker = 'Q3-' + str(datetime.date.today().year)[-2:] 
            qa2_ticker = 'Q4-' + str(datetime.date.today().year)[-2:] 
            qa3_ticker = 'Q1-' + str(datetime.date.today().year+1)[-2:] 
            qa4_ticker = 'Q2-' + str(datetime.date.today().year+1)[-2:] 
            qa5_ticker = 'Q3-' + str(datetime.date.today().year+1)[-2:] 
            qa6_ticker = 'Q4-' + str(datetime.date.today().year+1)[-2:] 
            qa_ticker_list.append(qa1_ticker)
            qa_ticker_list.append(qa2_ticker)
            qa_ticker_list.append(qa3_ticker)
            qa_ticker_list.append(qa4_ticker)
            qa_ticker_list.append(qa5_ticker)
            qa_ticker_list.append(qa6_ticker)
            
        if 7 <= datetime.date.today().month <=9:
            
            qa1_ticker = 'Q4-' + str(datetime.date.today().year)[-2:] 
            qa2_ticker = 'Q1-' + str(datetime.date.today().year+1)[-2:] 
            qa3_ticker = 'Q2-' + str(datetime.date.today().year+1)[-2:] 
            qa4_ticker = 'Q3-' + str(datetime.date.today().year+1)[-2:] 
            qa5_ticker = 'Q4-' + str(datetime.date.today().year+1)[-2:] 
            qa6_ticker = 'Q1-' + str(datetime.date.today().year+2)[-2:] 
            qa_ticker_list.append(qa1_ticker)
            qa_ticker_list.append(qa2_ticker)
            qa_ticker_list.append(qa3_ticker)
            qa_ticker_list.append(qa4_ticker)
            qa_ticker_list.append(qa5_ticker)
            qa_ticker_list.append(qa6_ticker)
            
        if 10 <= datetime.date.today().month <=12:
            
            qa1_ticker = 'Q1-' + str(datetime.date.today().year+1)[-2:] 
            qa2_ticker = 'Q2-' + str(datetime.date.today().year+1)[-2:] 
            qa3_ticker = 'Q3-' + str(datetime.date.today().year+1)[-2:] 
            qa4_ticker = 'Q4-' + str(datetime.date.today().year+1)[-2:] 
            qa5_ticker = 'Q1-' + str(datetime.date.today().year+2)[-2:] 
            qa6_ticker = 'Q2-' + str(datetime.date.today().year+2)[-2:] 
            qa_ticker_list.append(qa1_ticker)
            qa_ticker_list.append(qa2_ticker)
            qa_ticker_list.append(qa3_ticker)
            qa_ticker_list.append(qa4_ticker)
            qa_ticker_list.append(qa5_ticker)
            qa_ticker_list.append(qa6_ticker)
            
        if 1 <= datetime.date.today().month <=3:
            
            qa1_ticker = 'Q2-' + str(datetime.date.today().year)[-2:] 
            qa2_ticker = 'Q3-' + str(datetime.date.today().year)[-2:] 
            qa3_ticker = 'Q4-' + str(datetime.date.today().year)[-2:] 
            qa4_ticker = 'Q1-' + str(datetime.date.today().year+1)[-2:] 
            qa5_ticker = 'Q2-' + str(datetime.date.today().year+1)[-2:] 
            qa6_ticker = 'Q3-' + str(datetime.date.today().year+1)[-2:] 
            qa_ticker_list.append(qa1_ticker)
            qa_ticker_list.append(qa2_ticker)
            qa_ticker_list.append(qa3_ticker)
            qa_ticker_list.append(qa4_ticker)
            qa_ticker_list.append(qa5_ticker)
            qa_ticker_list.append(qa6_ticker)
            
        #season
        if 4 <= datetime.date.today().month <=9:
            sa1_ticker = 'Win' + str(datetime.date.today().year)[-2:] 
            sa2_ticker = 'Sum' + str(datetime.date.today().year+1)[-2:] 
            sa3_ticker = 'Win' + str(datetime.date.today().year+1)[-2:]
            
        if 10 <= datetime.date.today().month <=12:
            sa1_ticker = 'Sum' + str(datetime.date.today().year+1)[-2:] 
            sa2_ticker = 'Win' + str(datetime.date.today().year+1)[-2:] 
            sa3_ticker = 'Sum' + str(datetime.date.today().year+2)[-2:]
            
        if 1 <= datetime.date.today().month <=3:
            sa1_ticker = 'Sum' + str(datetime.date.today().year)[-2:] 
            sa2_ticker = 'Win' + str(datetime.date.today().year)[-2:] 
            sa3_ticker = 'Sum' + str(datetime.date.today().year+1)[-2:]
            
        sa_ticker_list = [sa1_ticker, sa2_ticker, sa3_ticker]
        
        ca1_ticker = 'Cal' + str(datetime.date.today().year+1)[-2:]
        ca2_ticker = 'Cal' + str(datetime.date.today().year+2)[-2:]
        ca_ticker_list = [ca1_ticker, ca2_ticker]
        
        columns_list = columns_list +  qa_ticker_list + sa_ticker_list + ca_ticker_list
        
        return columns_list
    
    
    def contract_to_ticker(values):
        
        month_dict = {
                'Jan' : 'F',
                'Feb' : 'G',
                'Mar' : 'H',
                'Apr' : 'J',
                'May' : 'K',
                'Jun' : 'M',
                'Jul' : 'N',
                'Aug' : 'Q',
                'Sep' : 'U',
                'Oct' : 'V',
                'Nov' : 'X',
                'Dec' : 'Z',
                
                
                }
            
        #flat
        #month
        if len(values) == 6:
            ticker = 'TZT' + month_dict[values[0:3]] + values[-1] + 'Comdty'

        #quarter
        if values[0:2] == 'Q1':
            ticker = 'QZTF' + values[-1] + ' Comdty'
        if values[0:2] == 'Q2':
            ticker = 'QZTJ' + values[-1] + ' Comdty'
        if values[0:2] == 'Q3':
            ticker = 'QZTN' + values[-1] + ' Comdty'
        if values[0:2] == 'Q4':
            ticker = 'QZTV' + values[-1] + ' Comdty'
            
        #season
        if values[0:3] == 'Sum':
            ticker = 'QQTJ' + values[-1] + ' Comdty'
        if values[0:3] == 'Win':
            ticker = 'QQTV' + values[-1] + ' Comdty'
            
        #cal
        if values[0:3] == 'Cal':
            ticker = 'QTTF' + values[-1] + ' Comdty'
            
        return ticker
    
    def get_bbg_intraday_prices(ticker, para_dict):
        
        today = datetime.date.today()
        end = datetime.date.today() + relativedelta(days=1)
        
        ttf = bquery.bdib(
                ticker,
                event_type = "TRADE",
                interval = 5,
                start_datetime = today,
                end_datetime = end
                )
        
        ttf.set_index('time', implace=True)
        ttf('ma') = ttf['close'].rolling(para_dict['move ave.']).mean()
        ttf['std'] = ttf['close'].rolling(30).std()
        if ttf.loc[ttf.index[0], 'high'] <= ttf.loc[ttf.index[0], 'low']:
            ttf.loc[ttf.index[0], 'low'] = 2*ttf.loc[ttf.index[0], 'close'] - ttf.loc[ttf.index[0], 'high']
            
        ttf.rename(columns={
                'high':'px_high',
                'low':'px_low',
                'open':'px_open',
                'close':'px_last'
                }, inplace=True)
            
        return ttf
    
    def get_bbg_daily_prices(ticker, para_dict):
        
        today = (datetime.date.today() - relativedelta(days=0)).strftime('%Y%m%d')
        start_date = str((datetime.date.today() - relativedelta(months=6)).year)+'-'+str((datetime.date.today() - relativedelta(months=6)).month)+'-01'
        
        ttf = bquery.bdh(
                ticker,
                ['PX_OPEN','PX_HIGH','PX_LOW','PX_LAST','Px_Settle'],
                start_date = '20230101',
                end_date = today
                )
        
        ttf.set_index('date', inplace=True)
        if pd.isna(ttf['Px_Settle'].iloc[-1]) == False:
            ttf.loc[ttf.index[-1], 'PX_LAST'] - ttf.loc[ttf.index[-1], 'Px_Settle']
            
        ttf['ma'] = ttf['PX_LAST'].rolling(para_dict['move ave.']).mean()
        ttf['std'] = ttf['PX_LAST'].rolling(30).std()
        
        ttf = ttf.loc[start_date:]
        
        return ttf
    
    def intraday_signal(dfintraday, ticker, para_dict):
        
        result = Common_Data.tech_signal_intraday(dfintraday, ticker, para_dict)
        
        return result
    
    def daily_signal(dfdaily, ticker, para_dict):
        
        result = Common_Data.tech_signal(dfdaily, ticker, para_dict)
        
        dfstatis, column_name, dfreport = Common_Data.Statistic(result, ticker, para_dict)
        
        return result, dfstatis, column_name
    
    def daily_signal_ts(dfdaily, ticker, para_dict):
        
        result = Common_Data.tech_signal(dfdaily, ticker, para_dict)
        
        dfstatis, column_name, dfreport = Common_Data.Statistic(result, ticker, para_dict)
        
        return result, dfstatis, column_name
    
    def chart(dfsignal_intraday, dfsignal_daily, contract_name, dfstatis, para_dict):
        
        fig = make_subplots(row=5, cols=3,
                            shared_xaxes = True,
                            vertical_spacing = 0.02,
                            subplot_titles = (
                                    'Intraday Signal', 'Daily Signal', 'Statistics','KDJ','KDJ', 'MACD','MACD','Trend Vector Degree'+str(para_dict['slope_high_trend'])+
                                    '/'+str(para_dict['slope_low_trend']), 'Trend Vector Degree'+str(para_dict['slope_high_trend'])+
                                    '/'+str(para_dict['slope_low_trend']), 'MA Trend Vector Degree '+str(para_dict['MAslope_high'])+'/'+str(para_dict['MAslope_low']),
                                    'MA Trend Vector Degree '+str(para_dict['MAslope_high'])+'/'+str(para_dict['MAslope_low'])
                                    ),
                            column_widths = [0.4, 0.4, 0.2],
                            row_heights = [0.4, 0.2, 0.2, 0.2, 0.2],
                            specs=[[{"type": "scatter"},{"type": "scatter"},{"type": "table"}],
                                   [{"type": "scatter"},{"type": "scatter"}, None],
                                   [{"type": "scatter"},{"type": "scatter"}, None],
                                   [{"type": "scatter"},{"type": "scatter"}, None],
                                   [{"type": "scatter"},{"type": "scatter"}, None]],
                            )
                            
        #intraday chart
        fig.add_trace(
                go.Candlestick(
                        x=dfsignal_intraday.index,
                        open = dfsignal_intraday.loc[:,'open'], high = dfsignal_intraday.loc[:, 'high'],
                        low = dfsignal_intraday.loc[:,'low'], close = dfsignal_intraday.loc[:,'chart close'], showlegend=False
                        ),
                        row = 1, col = 1
                )
                
        for i in dfsignal_intraday.index:
            if dfsignal_intraday.loc[i, 'signal'] == 'Sell':
                fig.add_annotation(x=i, y=dfsignal_intraday['close'].loc[i], xref='x1',yref='y1',
                                   text = 'Sell @'+str(dfsignal_intraday['close'].loc[i].round(2)),
                                   showarrow=True,
                                   arrowhead=5
                                   )
            if dfsignal_intraday.loc[i, 'signal'] == 'Buy':
                fig.add_annotation(x=i, y=dfsignal_intraday['close'].loc[i], xref='x1',yref='y1',
                                   text = 'Buy @'+str(dfsignal_intraday['close'].loc[i].round(2)),
                                   showarrow=True,
                                   arrowhead=5
                                   )
                
            if dfsignal_intraday.loc[i, 'signal'] == 'Close':
                fig.add_annotation(x=i, y=dfsignal_intraday['close'].loc[i], xref='x1',yref='y1',
                                   text = 'Close @'+str(dfsignal_intraday['close'].loc[i].round(2)),
                                   showarrow=True,
                                   arrowhead=5
                                   )
                
            if dfsignal_intraday.loc[i, 'signal'] == 'Close Buy and Sell':
                fig.add_annotation(x=i, y=dfsignal_intraday['close'].loc[i], xref='x1',yref='y1',
                                   text = 'Close Buy and Sell @'+str(dfsignal_intraday['close'].loc[i].round(2)),
                                   showarrow=True,
                                   arrowhead=5
                                   )
                
            if dfsignal_intraday.loc[i, 'signal'] == 'Close Sell and Buy':
                fig.add_annotation(x=i, y=dfsignal_intraday['close'].loc[i], xref='x1',yref='y1',
                                   text = 'Close Sell and Buy @'+str(dfsignal_intraday['close'].loc[i].round(2)),
                                   showarrow=True,
                                   arrowhead=5
                                   )
                
            if dfsignal_intraday.loc[i, 'signal'] == 'Sell Close':
                fig.add_annotation(x=i, y=dfsignal_intraday['close'].loc[i], xref='x1',yref='y1',
                                   text = 'Sell Close @'+str(dfsignal_intraday['close'].loc[i].round(2)),
                                   showarrow=True,
                                   arrowhead=5
                                   )
        
            if dfsignal_intraday.loc[i, 'signal'] == 'Buy Close':
                fig.add_annotation(x=i, y=dfsignal_intraday['close'].loc[i], xref='x1',yref='y1',
                                   text = 'Buy Close @'+str(dfsignal_intraday['close'].loc[i].round(2)),
                                   showarrow=True,
                                   arrowhead=5
                                   )
        fig.add_trace(go.Scatter(x=dfsignal_intraday.index, y=dfsignal_intraday.loc[:,'ma'], name='MA'+str(para_dict['move ave.']), xaxis='x1', line=dict(color='blue',
                                 dash='dot'), showlegend=False),
                    row=1, col=1)
        fig.add_trace(go.Scatter(x=dfsignal_intraday.index, y=dfsignal_intraday.loc[:,'J'], name='J', xaxis='x1', line=dict(color='red',
                                 dash='solid'), showlegend=False),
                    row=2, col=1)
        fig.add_trace(go.Scatter(x=dfsignal_intraday.index, y=dfsignal_intraday.loc[:,'D'], name='D', xaxis='x1', line=dict(color='blue',
                                 dash='dot'), showlegend=False),
                    row=2, col=1)
        fig.add_trace(go.Scatter(x=dfsignal_intraday.index, y=dfsignal_intraday.loc[:,'K'], name='K', xaxis='x1', line=dict(color='green',
                                 dash='dot'), showlegend=False),
                    row=2, col=1)
        fig.add_trace(go.Scatter(x=dfsignal_intraday.index, y=dfsignal_intraday.loc[:,'MACD'], name='MACD', xaxis='x1', line=dict(color='red',
                                 dash='solid'), showlegend=False),
                    row=3, col=1)
        fig.add_trace(go.Scatter(x=dfsignal_intraday.index, y=dfsignal_intraday.loc[:,'macds'], name='J', xaxis='x1', line=dict(color='blue',
                                 dash='solid'), showlegend=False),
                    row=3, col=1)
        colors_trend = []
        for i in dfsignal_intraday.index:
            if dfsignal_intraday.loc[i, 'slope'] > para_dict['slope_high_trend']:
                colors_trend.append('green')
            elif dfsignal_intraday.loc[i, 'slope'] < para_dict['slope_low_trend']:
                colors_trend.append('red')
            else:
                colors_trend.append('grey')
        fig.add_trace(go.Bar(x=dfsignal_intraday.index, y=dfsignal_intraday.loc[:,'slope'], name='trend', xaxis = 'x1', market_color = colors_trend, showlegend=True),
                      row=4, col=1)
        
        colors_ma = []
        for i in dfsignal_intraday.index:
            if dfsignal_intraday.loc[i, 'MAslope'] > para_dict['MAslope_high']:
                colors_ma.append('green')
            elif dfsignal_intraday.loc[i, 'MAslope'] < para_dict['MAslope_low']:
                colors_ma.append('red')
            else:
                colors_ma.append('grey')
        fig.add_trace(go.Bar(x=dfsignal_intraday.index, y=dfsignal_intraday.loc[:,'MAslope'], name='MAtrend', xaxis = 'x1', market_color = colors_ma, showlegend=True),
                      row=5, col=1)
        
        colors= []
        for i in dfsignal_intraday.index:
            if dfsignal_intraday.loc[i, 'macdh']>0:
                colors.append('green')
            else:
                colors.append('red')
        fig.add_trace(go.Bar(x=dfsignal_intraday.index, y=dfsignal_intraday.loc[:,'macdh'], name='macdh', xaxis = 'x1', market_color = colors, showlegend=True),
                      row=3, col=1)  
        
        #daily chart
        today = datetime.date.today()
        fig.add_trace(
                go.Candlestick(
                        x=dfsignal_daily.index,
                        open = dfsignal_daily.loc[:,'open'], high = dfsignal_daily.loc[:, 'high'],
                        low = dfsignal_daily.loc[:,'low'], close = dfsignal_daily.loc[:,'chart close'], showlegend=False
                        ),
                        row = 1, col = 1
                )
                
        for i in dfsignal_daily.index:
            if dfsignal_daily.loc[i, 'signal'] == 'Sell':
                fig.add_annotation(x=i, y=dfsignal_daily['close'].loc[i], xref='x1',yref='y1',
                                   text = 'Sell @'+str(dfsignal_daily['close'].loc[i].round(2)),
                                   showarrow=True,
                                   arrowhead=5
                                   )
            if dfsignal_daily.loc[i, 'signal'] == 'Buy':
                fig.add_annotation(x=i, y=dfsignal_daily['close'].loc[i], xref='x1',yref='y1',
                                   text = 'Buy @'+str(dfsignal_daily['close'].loc[i].round(2)),
                                   showarrow=True,
                                   arrowhead=5
                                   )
                
            if dfsignal_daily.loc[i, 'signal'] == 'Close':
                fig.add_annotation(x=i, y=dfsignal_daily['close'].loc[i], xref='x1',yref='y1',
                                   text = 'Close @'+str(dfsignal_daily['close'].loc[i].round(2)),
                                   showarrow=True,
                                   arrowhead=5
                                   )
                
            if dfsignal_daily.loc[i, 'signal'] == 'Close Buy and Sell':
                fig.add_annotation(x=i, y=dfsignal_daily['close'].loc[i], xref='x1',yref='y1',
                                   text = 'Close Buy and Sell @'+str(dfsignal_daily['close'].loc[i].round(2)),
                                   showarrow=True,
                                   arrowhead=5
                                   )
                
            if dfsignal_daily.loc[i, 'signal'] == 'Close Sell and Buy':
                fig.add_annotation(x=i, y=dfsignal_daily['close'].loc[i], xref='x1',yref='y1',
                                   text = 'Close Sell and Buy @'+str(dfsignal_daily['close'].loc[i].round(2)),
                                   showarrow=True,
                                   arrowhead=5
                                   )
                
            if dfsignal_daily.loc[i, 'signal'] == 'Sell Close':
                fig.add_annotation(x=i, y=dfsignal_daily['close'].loc[i], xref='x1',yref='y1',
                                   text = 'Sell Close @'+str(dfsignal_daily['close'].loc[i].round(2)),
                                   showarrow=True,
                                   arrowhead=5
                                   )
        
            if dfsignal_daily.loc[i, 'signal'] == 'Buy Close':
                fig.add_annotation(x=i, y=dfsignal_daily['close'].loc[i], xref='x1',yref='y1',
                                   text = 'Buy Close @'+str(dfsignal_daily['close'].loc[i].round(2)),
                                   showarrow=True,
                                   arrowhead=5
                                   )
        fig.add_trace(go.Scatter(x=dfsignal_daily.index, y=dfsignal_daily.loc[:,'ma'], name='MA'+str(para_dict['move ave.']), xaxis='x1', line=dict(color='blue',
                                 dash='dot'), showlegend=False),
                    row=1, col=2)
        fig.add_trace(go.Scatter(x=dfsignal_daily.index, y=dfsignal_daily.loc[:,'J'], name='J', xaxis='x1', line=dict(color='red',
                                 dash='solid'), showlegend=False),
                    row=2, col=2)
        fig.add_trace(go.Scatter(x=dfsignal_daily.index, y=dfsignal_daily.loc[:,'D'], name='D', xaxis='x1', line=dict(color='blue',
                                 dash='dot'), showlegend=False),
                    row=2, col=2)
        fig.add_trace(go.Scatter(x=dfsignal_daily.index, y=dfsignal_daily.loc[:,'K'], name='K', xaxis='x1', line=dict(color='green',
                                 dash='dot'), showlegend=False),
                    row=2, col=2)
        fig.add_trace(go.Scatter(x=dfsignal_daily.index, y=dfsignal_daily.loc[:,'MACD'], name='MACD', xaxis='x1', line=dict(color='red',
                                 dash='solid'), showlegend=False),
                    row=3, col=2)
        fig.add_trace(go.Scatter(x=dfsignal_daily.index, y=dfsignal_daily.loc[:,'macds'], name='J', xaxis='x1', line=dict(color='blue',
                                 dash='solid'), showlegend=False),
                    row=3, col=2)
        colors_trend = []
        for i in dfsignal_daily.index:
            if dfsignal_daily.loc[i, 'slope'] > para_dict['slope_high_trend']:
                colors_trend.append('green')
            elif dfsignal_daily.loc[i, 'slope'] < para_dict['slope_low_trend']:
                colors_trend.append('red')
            else:
                colors_trend.append('grey')
        fig.add_trace(go.Bar(x=dfsignal_daily.index, y=dfsignal_daily.loc[:,'slope'], name='trend', xaxis = 'x1', market_color = colors_trend, showlegend=True),
                      row=4, col=2)
        
        colors_ma = []
        for i in dfsignal_daily.index:
            if dfsignal_daily.loc[i, 'MAslope'] > para_dict['MAslope_high']:
                colors_ma.append('green')
            elif dfsignal_daily.loc[i, 'MAslope'] < para_dict['MAslope_low']:
                colors_ma.append('red')
            else:
                colors_ma.append('grey')
        fig.add_trace(go.Bar(x=dfsignal_daily.index, y=dfsignal_daily.loc[:,'MAslope'], name='MAtrend', xaxis = 'x1', market_color = colors_ma, showlegend=True),
                      row=5, col=2)
        
        colors= []
        for i in dfsignal_daily.index:
            if dfsignal_daily.loc[i, 'macdh']>0:
                colors.append('green')
            else:
                colors.append('red')
        fig.add_trace(go.Bar(x=dfsignal_daily.index, y=dfsignal_daily.loc[:,'macdh'], name='macdh', xaxis = 'x1', market_color = colors, showlegend=True),
                      row=3, col=2)  
        
        
        #statis table
        dfstatis.reset_index(inplace=True)
        fig.add_trace(go.Table(
                header = dict(
                        values=[['Figure'],['Jsignal'],['MACDsignal'],['MAsignal']],
                        font=dict(size=10),
                        align='left'
                        ),
                cells=dict(
                        values=[dfstatis[k].tolist() for k in dfstatis.columns],
                        align = 'left')
                        ),
                        row=1, col=3
                )
                
        fig.update_layout(
                autosize=True,
                title_text=para_dict['product']+' '+contract_name+' Live Trading Signal '++str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+'<br>',
                yaxis_title='EUR',
                hovermode='x unified',
                height=888,
                plot_bgcolor = 'white',
                template = 'ggplot2',
                bargap = 0
                )
        fig.update_xaxes(
                showline=True,
                linewidth=1,
                linecolor='LightGrey',
                showgrid=False,
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
        fig.update_xaxes(
                anchor = 'x2',
                rangebreaks=[
                        dict(counds=['sat','mon']),#hide weekend
                        #dict(values=['2015-12-25','2016-01-01']) #hide XMas and new year
                        ], row=1, col=2
                )
        fig.update_layout(legend=dict(
                                        orientation='h',
                                        yanchor='bottom',
                                        y=1.02,
                                        xanchor='left',
                                        x=0
                ))
        
        fig['layour']['xaxis2'].update(rangeselector=dict(
                buttons=list([
                dict(count=1, label='1m',step='month',stepmodel='backward'),
                dict(count=6, label='6m',step='month',stepmodel='backward'),
                dict(count=1, label='YTD',step='year',stepmodel='todate'),
                dict(step='all')
                ]))
            )
        
        fig.update_xaxes(rangeslider = {'visible':False}, row=1, col=2)
        fig.update_xaxes(rangeslider = {'visible':False}, row=1, col=1)
        fig.update_xaxes(range=[today - relativedelta(days=60), today+relativedelta(days=1)], row=1, col=2)
        fig.update_xaxes(range=[today - relativedelta(days=60), today+relativedelta(days=1)], row=2, col=2)
        fig.update_xaxes(range=[today - relativedelta(days=60), today+relativedelta(days=1)], row=3, col=2)
        fig.update_xaxes(range=[today - relativedelta(days=60), today+relativedelta(days=1)], row=4, col=2)
        fig.update_xaxes(range=[today - relativedelta(days=60), today+relativedelta(days=1)], row=5, col=2)

        
        py.plot(fig, filename='xxxx'+contract_name+'.html', auto_open=False)
        
        return fig
    
    
bquery = blp.BlpQuery().start()
#dashboard
app = dash.Dash(__name__)

app.layout = html.Div([
        dcc.Location(id='url', refresh = False),
        html.Div(id = 'page-content')
        ])
        
index_page = html.Div([
        dcc.Link('Tech',href='/Tech'),
        html.Br(),
        dcc.Link('TTFfund',href='/TTFfund'),
        ])
        
#update the index
@app.callback(Output('page-content','children'),
              [Input('url','pathname')]
              )
def display_page(pathname):
    
    if pathname == '/Tech':
        
        all_options = {
                        'TTF':['Flat','Time Spread','Box'],
                        'JKM':['Flat','Time Spread','Box'],
                        'EUA':['Flat','Time Spread','Box'],
                        
                }
        
        TTFtech_layout = html.Div([
                #select type of prices
                dcc.RadioItems(
                        id = 'signal-type',
                        options = list(all_options, keys()),
                        value = 'Intraday',
                        labelStyle = {'display':'inline-block','narginTop':'Spx'}
                        ),
                html.Hr(),
                
                dcc.RadioItems(id='contract-radio', labelStyle={'display': 'inline-block','marginTop':'5px'}),
                
                html.Hr(),
                #dropdown
                html.Div(id = 'dropdown-container', children = []),
                
                html.Div(dcc.Loading(id = 'dropdown-container-output')),
                #refrash page
                dcc.Interval(
                        id = 'interval-component-intraday',
                        interval = 1*60*1000, #in milliseconds
                        n_intervals = 0
                        )
                ])
            
            return TTFtech_layout
        
        if pathname == '/Tech_table':
            
            TTFtech_table_layout = html.Div([
                    
                    html.Div(dcc.Loading(id = 'table-container-output')),
                    html.Div(dcc.Loading(id = 'tables-container-output')),
                    #refrash page
                    dcc.Interval(
                            id = 'interval-component-intraday-table',
                            interval = 15*60*1000, #in milliseconds,
                            n_intervals = 0
                            )
                    
                    ])
            return TTFtech_table_layout
        
    else:
        return index_page
    
#chain option for product
@app.callback(
        Output('contract-radio', 'options'),
        Input('signal-type','value')
        )
def set_contract_options(selected_product):
    all_options = {
            'TTF':['Flat','Time Spread','Box'],
            'JKM':['Flat','Time Spread','Box'],
            'EUA':['Flat','Time Spread','Box'],
            }
    return [{'label':i, 'value':i} for i in all_options[selected_product]]

#chain option for contract
@app.callback(
        Output('contract-radio', 'value'),
        Input('contract-radio','options')
        )
def set_cities_value(available_options):
    return available_options[0]['value']

#dropdown for selected product and contract
@app.callback(
        Output('dropdown-container', 'children'),
        Input('signal-type','value'),
        Input('contract-radio','value')
        )
def display_dropdowns(selected_product, values):
    
    if selected_productn == 'TTF':
        
        columns_list = tech_dash.get_dropdown_list(values)
        
        option = []
        for i in columns_list:
            option.append({'label':str(i), 'value':str(i)})
            
        if values == 'flat':
            new_dropdown = html.Div([
                    dcc.Dropdown(
                            options = option,
                            placeholder = 'Select a Contract',
                            id = {
                                    'type':'filter-dropdown',
                                    'index':values
                                    },
                            sytle={'width':'%48'}
                            
                            ),
                    html.Div(
                            id = {
                                    'type':'dynamic-output',
                                    'index':values
                                    }
                            )
                    ])
            return new_dropdown
        
        if values == 'Time Spread':
            new_dropdown1 = html.Div([
                    dcc.Dropdown(
                            options = option,
                            placeholder = 'Select Contract 1',
                            id = {
                                    'type':'filter-dropdown',
                                    'index':values
                                    },
                            sytle={'width':'%48'}
                            
                            ),
                    html.Div(
                            id = {
                                    'type':'dynamic-output',
                                    'index':values
                                    }
                            )
                    ])
            new_dropdown2 = html.Div([
                    dcc.Dropdown(
                            options = option,
                            placeholder = 'Select Contract 2',
                            id = {
                                    'type':'filter-dropdown',
                                    'index':values
                                    },
                            sytle={'width':'%48'}
                            
                            ),
                    html.Div(
                            id = {
                                    'type':'dynamic-output',
                                    'index':values
                                    }
                            )
                    ])
            return new_dropdown1, new_dropdown2
        
        if values == 'Box':
            new_dropdown3= dcc.Dropdown(
                            options = option,
                            placeholder = 'Select Timespread 1 Contract 1',
                            id = {
                                    'type':'filter-dropdown',
                                    'index':values + '1'
                                    },
                            sytle={'width':'%48', 'display':'inline-block'}
                            
                            ),
            new_dropdown4= dcc.Dropdown(
                            options = option,
                            placeholder = 'Select Timespread 2 Contract 1',
                            id = {
                                    'type':'filter-dropdown',
                                    'index':values + '2'
                                    },
                            sytle={'width':'%48', 'display':'inline-block'}
                            
                            ),
            new_dropdown5= dcc.Dropdown(
                            options = option,
                            placeholder = 'Select Timespread 1 Contract 2',
                            id = {
                                    'type':'filter-dropdown',
                                    'index':values + '3'
                                    },
                            sytle={'width':'%48', 'display':'inline-block'}
                            
                            ),
            new_dropdown6= dcc.Dropdown(
                            options = option,
                            placeholder = 'Select Timespread 2 Contract 2',
                            id = {
                                    'type':'filter-dropdown',
                                    'index':values + '4'
                                    },
                            sytle={'width':'%48', 'display':'inline-block'}
                            
                            )
            return new_dropdown3,new_dropdown4,new_dropdown5,new_dropdown6
        
@app.callback(
        Output('dropdown-container-output','children'),
        Input({'type':'filter-dropdown','index':ALL}, 'value'),
        Input('interval-component-intraday','n_intervals'),
        )
def display_output(values, n):
    
    contract_list = []
    
    for value in values:
        contract_list.append(value)
        
    #plot chart
    if len(contract_list) == 1:
        
        ttf_para_dict_flat = {
                'product':'TTF',
                'kdj_j':'kdjj_3',
                'j_high':70,
                'j_low':20,
                'slope_high':1.55,
                'slope_low':-1.55,
                'back_days':-60,
                'move ave.':30,
                'sl_abs':-2.5,
                'MAslope_high':20,
                'MAslope_low':-20,
                'slope_high_trend':15,
                'slope_low_trend':-15
                }
                    
        contract1 = contract_list[0]
        contract2 = contract_list[0]
        contract_name = contract2
        
        #set up ticker
        ticker = tech_dash.contract_to_ticker(contract_name)
        print(ticker)
        dfintraday = tech_dash.get_bbg_intraday_prices(ticker, ttf_para_dict_flat)
        dfdaily = tech_dash.get_bbg_daily_prices(ticker, ttf_para_dict_flat)
        dfsignal_intraday = tech_dash.intraday_signal(dfintraday, ticker, ttf_para_dict_flat)
        dfsignal_daily, dfstatis, column_name = tech_dash.daily_signal(dfdaily, ticker, ttf_para_dict_flat)
        fig = tech_dash.chart(dfsignal_intraday, dfsignal_daily, column_name, dfstatis, ttf_para_dict_flat)
        
        return html.Div(dcc.Graph(figure=fig))
    
    if len(contract_list) == 2:
        
        ttf_para_dict_ts = {
                'product':'TTF TS',
                'kdj_j':'kdjj_3',
                'j_high':70,
                'j_low':20,
                'slope_high':1.55,
                'slope_low':-1.55,
                'back_days':-60,
                'move ave.':30,
                'sl_abs':-2.5,
                'MAslope_high':20,
                'MAslope_low':-20,
                'slope_high_trend':15,
                'slope_low_trend':-15
                }
        
        contract1 = contract_list[0]
        contract2 = contract_list[1]
        contract_name = contract1 + 'x' + contract2
        ticker1 = tech_dash.contract_to_ticker(contract1)
        ticker2 = tech_dash.contract_to_ticker(contract2)
        ticker = ticker1[:-7] + ticker2[-9:-7] + ' Comdty'
        print(ticker)
        
        dfdaily = tech_dash.get_bbg_daily_prices(ticker, ttf_para_dict_ts)
        dfsignal_daily, dfstatis, column_name = tech_dash.daily_signal_ts(dfdaily, ticker, ttf_para_dict_ts)
        
        #intraday
        dfintraday = tech_dash.get_bbg_intraday_prices(ticker, ttf_para_dict_ts)
        dfsignal_intraday = tech_dash.intraday_signal(dfintraday, ticker, ttf_para_dict_ts)
        
        fig = tech_dash.chart(dfsignal_intraday, dfsignal_daily, column_name, dfstatis, ttf_para_dict_ts)
        
        return html.Div(dcc.Graph(figure=fig))
        
    return html.Div([
            html.Div('Dropdown {} = {}'.format(i+1, value))
            for (i, value) in enumerate(values)
            ])
            
@app.callback(
        Output('table-container-output','children'),
        Output('tables-container-output','children'),
        Input('interval-component-intraday-table','n_intervals')
        )
def display_output_table(n):
    
    ttf_para_dict_flat = {
                'product':'TTF',
                'kdj_j':'kdjj_3',
                'j_high':70,
                'j_low':20,
                'slope_high':1.55,
                'slope_low':-1.55,
                'back_days':-60,
                'move ave.':30,
                'sl_abs':-2.5,
                'MAslope_high':20,
                'MAslope_low':-20,
                'slope_high_trend':15,
                'slope_low_trend':-15
                }
    
    tickers = Grid_search_signal.create_TTF_bbg_flat_ticker()
    ts_tickers = Grid_search_signal.create_TTF_bbg_ts_ticker()
    
    dfreport_all = pd.DataFrame(column=['Signal','Start date','Signal Price','Market EoD','PnL','Correct rate'])
    #TTF flat signal
    for i in tickers[:]:
        try:
            dfbbgttf_pivot = Grid_Search_signal.get_bbg_curve(i)
            dfdaily = Grid_Search_signal.curve_start_end(dfbbgttf_pivot, 1, 6, ttf_para_dict_flat)
            result, dfreport = Grid_Search_signal.signal_daily(dfdaily, i, ttf_para_dict_flat)
            dfreport_all.loc[dfreport.index[0]] = dfreport.loc[dfreport.index[0]]
            
        except IndexError:
            pass
        
    print('grid')
    fig = Grid_Search_signal.grid_table(dfreport_all)
        
    ttf_para_dict_ts = {
                'product':'TTF TS',
                'kdj_j':'kdjj_3',
                'j_high':70,
                'j_low':20,
                'slope_high':1.55,
                'slope_low':-1.55,
                'back_days':-60,
                'move ave.':30,
                'sl_abs':-2.5,
                'MAslope_high':20,
                'MAslope_low':-20,
                'slope_high_trend':15,
                'slope_low_trend':-15
                }
    
    dfreport_all_ts = pd.DataFrame(column=['Signal','Start date','Signal Price','Market EoD','PnL','Correct rate'])
    for i in ts_tickers[:]:
        try:
            dfbbgttf_pivot_ts = Grid_Search_signal.get_bbg_curve(i)
            dfdaily_ts = Grid_Search_signal.curve_start_end(dfbbgttf_pivot_ts, 1, 6, ttf_para_dict_ts)
            result_ts, dfreport_ts = Grid_Search_signal.signal_daily(dfdaily_ts, i, ttf_para_dict_ts)
            dfreport_all_ts.loc[dfreport_ts.index[0]] = dfreport_ts.loc[dfreport_ts.index[0]]
            
        except IndexError:
            pass
            
    print('grid')
    fig_ts = Grid_Search_signal.grid_table(dfreport_all_ts)   

    return html.Div(dcc.Graph(figure=fig)), html.Div(dcc.Graph(figure=fig_ts))

if __name__ == '__main__':
    app.run_server(debug=False, host = 'localhost', port = 8888)     

        