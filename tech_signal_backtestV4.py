# -*- coding: utf-8 -*-
"""
Created on Wed Jun 18 18:36:19 2025

@author: 岳峰
"""


import pandas as pd
import numpy as np
import pdblp
import datetime
from dateutil.relativedelta import relativedelta
import stockstats
import plotly.offline as py
import plotly.graph_objs as go 
from plotly.subplots import make_subplots
import math
import sys
sys.path.append('C:/Users/yuewang/Desktop/python scripts/')
from common_function import Common_Data

pd.set_option('display.max_columns',20)

#ma 10 stop loss
#6m back rolling MA1 test
#chart on cumpnl and risk return rate, MA10 line on stick
#table:'ave.VAR','Risk Return ratio','Sharpe ratio','Signal correct rate','Ave. total PnL','Ave. Pnl per trade','Max Profit per trade','Max Loss per trade','Max holding days per trade','Min holding days per trade','Ave.holding days per trade','stop level','total sl count'

#V1: add if macd <> +-1, use trend more than macdh
#v2 test md vs ma
#v3 add ts signal fanction
#v4 rebuild signal system, 3 signals on the right table, chart add option show which signal

'''
Test:
    MA1 rolling forward
    start: 6 months before 3 days prior to delivery date
    end: 3 days prior to delivery date
    
Strategy:
    Signal: KDJ, MACD and Trend vector
    Logic:
        Close and Stop Loss:
        Prices:
Statistics:
    VaR: 2*30 days std
    Risk Return ratio:  total return / Sum. VaR
    sharpe ratio: mean return / return.std
    Cum. PnL: cum sum return
    Correct rate: correct signal num. / total signal num.
    Average PnL: Ave. return
    Max Profit: Max return
    Max Loss: SL or Min return
    Max holding days: max signal last days
    min holding days: min signal last days
    Ave. holding days: ave. signal last days
'''


class Signal_backtest():
    
    def ttf_curve(ticker):
        today = (datetime.date.today() - relativedelta(days=0)).strftime('%Y%m%d')
        start_date = '20170101'#(datetime.date.today() + relativedelta(days=-90)).strftime('%Y%m%d')
        #end_date = today.strftime('%Y%m%d')
        
        dfbbgttf = con.bdh(tickers = ticker, flds=['PX_OPEN','PX_HIGH','PX_LOW','PX_LAST'], start_date=start_date, end_date=today, longdata=True)
        dfbbgttf = dfbbgttf[['date','field','value']]
        dfbbgttf_group = dfbbgttf.groupby(['date', 'field'], as_index = False).mean()
        dfbbgttf_pivot = pd.pivot(dfbbggroup, index = 'date', columns = 'filed')
        dfbbgttf_pivot.columns =  dfbbgttf_povit.columns.droplevel(0)
        
        return dfbbgttf_pivot
        

    def curve_start_end(dfbbgttf_pivot, end_nbday, start_mm, para_dict):
        end_date = dfbbgttf_pivot.index[-end_nbday]
        start_date = str((end_date-relativedelta(months=start_nm)).year) + '-' + str((end_date-relativedelta(months=start_nm)).month) + '-01'
        
        dfbbgttf_pivot['ma'] = dfbbgttf_pivot['PX_LAST'].rolling(para_dict['move ave.']).mean()
        dfbbgttf_pivot['std'] = dfbbgttf_pivot['PX_LAST'].rolling(30).std()
        dfdaily = dfbbgttf_pivot.loc[start_date:end_date]
        
        #avoid kdj nan, as same close high low <=, change to < at v4
        
        # end_date if start_date <= '2020-12-31':
        #   end_date = pd.to_datetime('2020-12-31')
        if dfdaily.loc[dfdaily.index[0], 'PX_HIGH'] < dfdaily.loc[dfdaily.index[0], 'PX_LOW']:
            dfdaily.loc[dfdaily.index[0], 'PX_LOW'] = 2 * dfdaily.loc[dfdaily.index[0], 'PX_LAST'] - dfdaily.loc[dfdaily.index[0], 'PX_HIGH']
        
        return dfdaily

    def ticker_to_cont(ticker):
        month_dict = {
            'Jan': 'F', 
            'Feb': 'G', 
            'Mar': 'H', 
            'Apr': 'J', 
            'May': 'K', 
            'Jun': 'M',
            'Jul': 'N', 
            'Aug': 'Q', 
            'Sep': 'U', 
            'Oct': 'V', 
            'Nov': 'X', 
            'Dec': 'Z'
        }
        new_month_dict = {v: k for k, v in month_dict.items()}
        #month
        if ticker[0:3] == 'TZT':
            if len(ticker) == 12:
                contract = 'FM ' + new_month_dict[ticker[3]] + '2' + ticker[4]
            if len(ticker) == 14:
                contract = 'FM TS' + new_month_dict[ticker[3]] + '2' + ticker[4] + new_month_dict[ticker[5]] + '2' + ticker[6]
            if len(ticker) == 13:
                contract = 'FM ' + new_month_dict[ticker[3]] + ticker[4:6]
         
        #quarter name
        if ticker[0:3] == 'QTZ':
            if len(ticker) == 12:
                contract = 'FQ ' + new_month_dict[ticker[3]] + '2' + ticker[4]
            if len(ticker) == 14:
                contract = 'FQ TS' + new_month_dict[ticker[3]] + '2' + ticker[4] + new_month_dict[ticker[5]] + '2' + ticker[6]
            
        #season name qqtv24 Comdty
        if ticker[0:4] == 'QQTV' or ticker[0:4] == 'QQTJ':
            if len(ticker) == 12:
                contract = 'FS ' + new_month_dict[ticker[3]] + '2' + ticker[4]
            if len(ticker) == 14:
                contract = 'FS TS' + new_month_dict[ticker[3]] + '2' + ticker[4] + new_month_dict[ticker[5]] + '2' + ticker[6]
            if len(ticker) == 13:
                contract = 'FS ' + new_month_dict[ticker[3]] + ticker[4:6]
         
        #year
        if ticker[0:4] == 'QTTF':
            if len(ticker) == 12:
                contract = 'FY ' + new_month_dict[ticker[3]] + '2' + ticker[4]
            if len(ticker) == 14:
                contract = 'FY TS' + new_month_dict[ticker[3]] + '2' + ticker[4] + new_month_dict[ticker[5]] + '2' + ticker[6]
                
                
        #MA1
        if ticker == 'TZT1 Comdty':
            contract = 'MA1'
            
        return contract

    def signal(dfdaily, ticker, para_dict):
        today = datetime.date.today()

        stock = stockstats.StockDataFrame.retype(dfdaily)
        '''
        for i in stock.index:
            if stock.loc[i, 'px_high'] > 0:
                pass
            else:
                stock.loc[i, 'px_high'] = stock.loc[i, 'px_last']

            if stock.loc[i, 'px_low'] > 0:
                pass
            else:
                stock.loc[i, 'px_low'] = stock.loc[i, 'px_last']

            if stock.loc[i, 'px_open'] > 0:
                pass
            else:
                stock.loc[i, 'px_open'] = stock.loc[i, 'px_last']
        '''
        
        # delete na value and fill na with close
        stock.dropna(subset = ['px_last'], inplace=True)
        for i in stock.columns:
            stock[i].fillna(stock['px_last'], inplace=True)

        
        stock['open'] = stock['px_open']
        stock['close'] = stock['px_last']
        stock['high'] = stock['px_high']
        stock['low'] = stock['px_low']
        stock['volume'] = 0
        stock['amount'] = 0

        result = pd.DataFrame(stock['close'].values, index = stock.index[:], columns=['MA'])
        result['open'] = stock['open']
        result['close'] = stock['close']
        result['high'] = stock['high']
        result['low'] = stock['low']
        result['ma'] = dfdaily['ma']
        result['std'] = dfdaily['std']
        result['chart_close'] = stock['close']  # close for chart
        result['close'] = (result['high'] + result['low']) / 2
        result['MACD'] = stock['macd']*1
        result['macds'] = stock['macds']
        result['macdh'] = (stock['macdh']*2).round(2)
        result['K'] = stock['kdjk']
        result['D'] = stock['kdjd']
        result['J'] = stock[para_dict['kdj_j']]
        '''
        # KDJ, avoid stockstats KDJ all nan
        ndays = 6
        ln = result['low'].rolling(window=ndays, min_periods=1).min()
        hn = result['high'].rolling(window=ndays, min_periods=1).max()
        result['ln'] = ln.values
        result['hn'] = hn.values
        result['rsv'] = 100 * (result['close'] - result['ln']) / (result['hn'] - result['ln'])
        result['K'] = 50
        result['D'] = 50
        result['K'] = result['K'].shift(1, fill_value=50) * 2/3 + result['rsv'] * 1/3
        result['D'] = result['D'].shift(1, fill_value=50) * 2/3 + result['K'] * 1/3
        result['J'] = 3 * result['K'] - 2 * result['D']
        '''
        '''
        MD = MD1 + (Price -MD1) / (N * (Price/MD1)^4)
        where :
            MD1 = the previous value of the  McGinley dynamic indicator
            
            price = the most recent price in the data series.
            
            N = a smoothing factor chosen by the trader or analyst, It's often set to a default value of 10, but can be adjusted based on individual trading strategies aand preferences.
            
            (Price/MD1)^4 = This ratio, raised to fourth power, acts as an adaptive factor that adjusts based on the speed of price changes.
        '''
        result['MD'] = np.nan
        result['MD'].iloc[0] = result['chart close'].iloc[0]
        N = 30
        for i in range(1, result.shape[0]):
            result.loc[result.index[i], 'MD'] = result.loc[result.index[i-1], 'MD'] + (result.loc[result.index[i], 'chart close'] - result.loc[result.index[i-1], 'MD'])/(N*(result.loc[result.index[i],'chart close']/
                      result.loc[result.index[i-1], 'MD'])**4)
        
        for i in result.index:
            if result.loc[i, 'J'] > 150:
                result.loc[i, 'J'] = 110
                
            if result.loc[i, 'J'] < -50:
                result.loc[i, 'J'] = -10
                
        result['slope'] = np.nan
        result['MAslope'] = np.nan
        
        trend_variable = 'ma'
        for i in range(1, result.shape[0]):
            
            dx1 = 3 #smooth avoid flip dod
            dy1 = result(result['MACD'].iloc[i] - result['MACD'].iloc[i-dx1])*1
            
            angle1 = math.atan2(dy1, dx1)
            angle1 = float(angle1 * 180/math.pi)
            
            result['slope'].iloc[i] = round(angle1, 2)
            
            dx2 = 1
            dy2 = (result[trend_variable].iloc[i] - result[trend_variable].iloc[i - dx2])*1
            
            angle2 =  math.atan(dy2, dx2)
            angle2 = float(angle2 * 180/math.pi)
            
            result['MAslope'].iloc[i] * round(angle2, 2)
            
        result['slope'] = result['slope'].rolling(5).mean()
        #para_dict['slope_high'] = result.loc[result[result['slope']>0].index, 'slope'].mean() + 1*result.loc[result[result['slope']>0].index, 'slope'].std()
        #para_dict['slope_low'] = result.loc[result[result['slope']<0].index, 'slope'].mean() - 1*result.loc[result[result['slope']<0].index, 'slope'].std()
        #para_dict['slope_high'] = result['slope'].mean() + 2*result['slope'].std()
        #para_dict['slope_low'] = -para_dict['slope_high'] 
        result['MAslope'] = result['MAslope'].rolling(7).mean()
        #signal logic
        result.loc[:,'Jsignal'] = np.nan
        result.loc[:,'MAsignal'] = np.nan
        result.loc[:,'MACDsignal'] = np.nan
        result.loc[:,'signal'] = np.nan
        
        for i in range(1, result.shape[0]):
            try:
                #MA signal
                '''
                if result.loc[result.index[i], 'MAslope'] <= para_dict['MAslope_low']:
                    result.loc[result.index[i], 'MAsignal'] = 'Sell'
                if result.loc[result.index[i], 'MAslope'] > para_dict['MAslope_high']:
                    result.loc[result.index[i], 'MAsignal'] = 'Buy'
                '''
                if result.loc[result.index[i-1], 'MAslope'] <0 and result.loc[result.index[i], 'MAslope'] >0:
                    result.loc[result.index[i], 'MAsignal'] = 'Buy'
                if result.loc[result.index[i-1], 'MAslope'] >0 and result.loc[result.index[i], 'MAslope'] <0:
                    result.loc[result.index[i], 'MAsignal'] = 'Sell'
                    
                #J signal
                if result.loc[result.index[i-1],'J'] >= para_dict['j_high'] and result.loc[result.index[i], 'J'] < result.loc[result.index[i-1],'J'] and result.loc[result.index[i],'J'] > para_dict['j_low']:
                    result.loc[result.index[i], 'Jsignal'] = 'Sell'
                    
                if result.loc[result.index[i-1],'J'] <= para_dict['j_low'] and result.loc[result.index[i], 'J'] > result.loc[result.index[i-1],'J'] and result.loc[result.index[i],'J'] < para_dict['j_high']:
                    result.loc[result.index[i], 'Jsignal'] = 'Buy'
                    
                #macd signal
                if result.loc[result.index[i], 'slope'] >= para_dict['slope_high']:
                    result.loc[result.index[i], 'MACDsignal'] = 'Buy'
                if result.loc[result.index[i], 'slope'] <= para_dict['slope_low']:
                    result.loc[result.index[i], 'MACDsignal'] = 'Sell'
                    
                if para_dict['slope_low'] < result.loc[result.index[i], 'slope'] < para_dict['slope_high']:
                    if result.loc[result.index[i-2], 'slope'] > result.loc[result.index[i], 'slope']+0.01:
                        result.loc[result.index[i],'MACDsignal'] = 'Sell'
                    if result.loc[result.index[i-2], 'slope'] < result.loc[result.index[i], 'slope']-0.01:
                        result.loc[result.index[i],'MACDsignal'] = 'Buy'
                        
                '''
                if result.loc[result.index[i-2], 'slope'] > result.loc[result.index[i],'slope']+0.01:
                    result.loc[result.index[i],'MACDsignal'] = 'Sell'
                if result.loc[result.index[i-2], 'slope'] < result.loc[result.index[i],'slope']-0.01:
                    result.loc[result.index[i],'MACDsignal'] = 'Buy'
                '''
                
            except KeyError as e:
                print(ticker)
                pass
            
            
        #final signal
        for i in range(0, result.shape[0]):
            if result.loc[result.index[i], 'Jsignal'] == 'Sell':
                for j in range(i+1, result.shape[0]):
                    
                    if result.loc[result.index[j], 'Jsignal'] == 'Sell':
                        result.loc[result.index[j], 'Jsignal'] = np.nan
                    if result.loc[result.index[j], 'Jsignal'] == 'Buy':
                        break
                    
            if result.loc[result.index[i], 'Jsignal'] == 'Buy':
                for j in range(i+1, result.shape[0]):
                    
                    if result.loc[result.index[j], 'Jsignal'] == 'Buy':
                        result.loc[result.index[j], 'Jsignal'] = np.nan
                    if result.loc[result.index[j], 'Jsignal'] == 'Sell':
                        break
            
        for i in range(0, result.shape[0]):
            if result.loc[result.index[i], 'MACDsignal'] == 'Sell':
                for j in range(i+1, result.shape[0]):
                    
                    if result.loc[result.index[j], 'MACDsignal'] == 'Sell':
                        result.loc[result.index[j], 'MACDsignal'] = np.nan
                    if result.loc[result.index[j], 'MACDsignal'] == 'Buy':
                        break
                    
            if result.loc[result.index[i], 'MACDsignal'] == 'Buy':
                for j in range(i+1, result.shape[0]):
                    
                    if result.loc[result.index[j], 'MACDsignal'] == 'Buy':
                        result.loc[result.index[j], 'MACDsignal'] = np.nan
                    if result.loc[result.index[j], 'MACDsignal'] == 'Sell':
                        break
                
        for i in range(0, result.shape[0]):
            if result.loc[result.index[i], 'MAsignal'] == 'Sell':
                for j in range(i+1, result.shape[0]):
                    
                    if result.loc[result.index[j], 'MAsignal'] == 'Sell':
                        result.loc[result.index[j], 'MAsignal'] = np.nan
                    if result.loc[result.index[j], 'MAsignal'] == 'Buy':
                        break
                    
            if result.loc[result.index[i], 'MAsignal'] == 'Buy':
                for j in range(i+1, result.shape[0]):
                    
                    if result.loc[result.index[j], 'MAsignal'] == 'Buy':
                        result.loc[result.index[j], 'MAsignal'] = np.nan
                    if result.loc[result.index[j], 'MAsignal'] == 'Sell':
                        break
                    
        return result
    
    def statistic(result, ticker, para_dict):
        
        d = {}
        
        for sig_column in ['Jsignal','MACDsignal','MAsignal']:
            #calc cum.pnl
            cumpnl = pd.DataFrame(columns=['Sell','Sell Close','Buy','Buy Close'])
            
            for i in result.index:
                if result.loc[i, sig_column] == 'Sell':
                    cumpnl.loc[i, 'Sell'] = result.loc[i, 'chart close']
                    for j in result.loc[j, sig_column] == 'Buy':
                        cumpnl.loc[j, 'Sell Close'] = result.loc[j, 'chart close']
                        break
                    
        cumpnl.sort_index(inplace=True)
        
        if pd.notna(cumpnl.loc[cumpnl.index[-1], 'Sell']):
            cumpnl.loc[result.index[-1], 'Sell Close'] = result.loc[result.index[-1], 'chart close']
        if pd.notna(cumpnl.loc[cumpnl.index[-1], 'Buy']):
            cumpnl.loc[result.index[-1], 'Buy Close'] = result.loc[result.index[-1], 'chart close'] 
        cumpnl.fillna(0, inplace=True)
        
        #pnl
        for i in range(1, cumpnl.shape[0]):
            cumpnl.loc[cumpnl.index[i], 'Buy pnl'] = cumpnl.loc[cumpnl.index[i], 'Buy Close'] - cumpnl.loc[cumpnl.index[i-1], 'Buy']
            cumpnl.loc[cumpnl.index[i], 'Sell pnl'] = cumpnl.loc[cumpnl.index[i-1], 'Sell'] - cumpnl.loc[cumpnl.index[i], 'Sell Close']
        sl_count = []
        for i in range(1, cumpnl.shape[0]):
            if cumpnl.loc[cumpnl.index[i], 'Buy pnl'] <= para_dict['sl_abs']:
                cumpnl.loc[cumpnl.index[i], 'Buy pnl'] = para_dict['sl_abs']
                sl_count.append(1)
            if cumpnl.loc[cumpnl.index[i], 'Sell pnl'] <= para_dict['sl_abs']:
                cumpnl.loc[cumpnl.index[i], 'Sell pnl'] = para_dict['sl_abs']
                sl_count.append(1)
                
        cumpnl['pnl'] = cumpnl['Buy pnl'] + cumpnl['Sell pnl']
        
        #correct rate
        correct = []
        incorrect = []
        for i in cumpnl.index:
            if cumpnl.loc[i, 'pnl'] > 0:
                correct.append(1)
                
            if cumpnl.loc[i, 'pnl'] < 0:
                incorrect.append(1)
        correct_rate = round(len(correct)/(len(correct) + len(incorrect)), 2)
        
        #max loss, max profit, ave.return
        max_loss = cumpnl['pnl'].min()
        max_win = cumpnl['pnl'].max()
        average_return = cumpnl['pnl'].mean()
        
        #total pnl
        cumpnl.loc['live pnl'] = cumpnl.sum()
        livepnl = round(cumpnl.loc['live pnl', 'Sell pnl'] + cumpnl.loc['live pnl', 'Buy pnl'], 2)
        
        #risk return pnl/2xstd(30 days) each trade
        riskreturn = result[['std']].copy()
        riskreturn['ave.VAR'] = 2*riskreturn['std']
        
        for i in range(1, cumpnl.shape[0]-1):
            cumpnl.loc[cumpnl.index[i], 'risk'] = riskreturn.loc[cumpnl.index[i-1]:cumpnl.index[i], 'ave.VAR'].mean()
            
        risk_return = livepnl/cumpnl['risk'].sum()
        
        
        #sharpe
        for i in range(1, cumpnl.shape[0]-1):
            cumpnl.loc[cumpnl.index[i], 'num of days'] = cumpnl.index[i] - cumpnl.index[i-1]
            
        mean_return = livepnl/cumpnl['num of days'].sum().days*100
        risk = cumpnl['pnl'].std()
        sharpe = mean_return / (risk)
        
        #max holding days, min holding days
        holding_days_max = cumpnl['num of days'].max().days
        holding_days_min = cumpnl['num of days'].min().days
        ave_days = cumpnl['num of days'].mean()
        
        #contract = Signal_backtest.ticker_to_cont(ticker)
        index_list = ['ave.VAR','Risk Return ratio','Sharpe ratio','Signal correct rate','Ave. total PnL','Ave. Pnl per trade','Max Profit per trade','Max Loss per trade','Max holding days per trade','Min holding days per trade','Ave.holding days per trade','stop level','total sl count']
        column_name = Signal_backtest.ticker_to_cont(ticker)
        d[sig_column] = pd.DataFrame(index = index_list, columns = [sig_column])
        d[sig_column].loc['ave.VAR'] = riskreturn['ave.VAR'].iloc[-1].round(1)
        d[sig_column].loc['Signal correct rate'] = round(correct_rate, 2)
        d[sig_column].loc['Risk Return ratio'] = round(risk_return, 2)
        d[sig_column].loc['Sharpe ratio'] = round(sharpe,2)
        d[sig_column].loc['Ave. total PnL'] = livepnl
        d[sig_column].loc['Ave. PnL per trade'] = round(average_return, 2)
        d[sig_column].loc['Max Profit per trade'] = round(max_win, 2)
        d[sig_column].loc['Max Loss per trade'] = round(max_loss, 2)
        d[sig_column].loc['Max holding days per trade'] = holding_days_max
        d[sig_column].loc['Min holding days per trade'] = holding_days_min
        d[sig_column].loc['Ave. holding days per trade'] = ave_days.days
        d[sig_column].loc['stop level'] = para_dict['sl_abs']
        d[sig_column].loc['total SL count'] = len(sl_cound)
        
        
        dfstatis_J = d['Jsignal']
        dfstatis_MACD = d['MACDsignal']
        dfstatis_MA = d['MAsignal']
        dfstatis = pd.concat([dfstatis_J, dfstatis_MACD, dfstatis_MA], axis=1)
        
        return dfstatis, column_name
    
    
    def chart(dfsignal_daily, contract_name, dfstatis, para_dict, sig_column):
        
        fig = make_subplots(rows=5, cols=2,
                            shared_xaxes = True,
                            vertical_spacing = 0.02,
                            subplot_titles = ('Daily Signal','Statistics','KDJ','MACD','Trend Vector Degree '+str(para_dict['slope_high_trend'])+'/'+str(para_dict['slope_low_trend']), 'MA Trend Vector Degree'+str(para_dict['MAslope_high'])+'/'+str(para_dict['MAslope_low'])),
                            column_widths = [0.7, 0.3],
                            row_hights=[0.4, 0.2, 0.2, 0.2, 0.2],
                            specs=[[{"type": "scatter"},{"type": "table", "rowspan":3}],
                                   [{"type": "scatter"}, None],
                                   [{"type": "scatter"}, None],
                                   [{"type": "scatter"}, None],
                                   [{"type": "scatter"}, None]],
                                    )
        
        #daily charts
        today = datetime.date.today()
        start = str((today - relativedelta(month = 1)).year) + '-'+str((today - relativedelta(month = 1)).month)+'-01'
        fig.add_trace(
                go.Candlestick(x=dfsignal_daily.index,
                               open = dfsingal_daily.loc[:, 'open'], high = dfsignal_daily.loc[:,'high'],
                               low = dfsignal_daily.loc[:,'low'], close = dfsignal_daily.loc[:,'chart close'], xaxis = 'x1', showlegend=False,),
                row=1, col=1
                )
        for signal in sig_column:
            if signal == 'Jsignal':
                color = 'red'
            if signal == 'MACDsignal':
                color = 'orange'
            if signal == 'MAsignal':
                color = 'grey'
            for i in dfsignal_daily.index:
                if dfsignal_daily.loc[i, signal] == 'Sell':
                    fig.add_annotation(x=i, y=dfsignal_daily['MA'].loc[i], xref='x1', yred='y1',
                                       text = 'Sell @'+str(dfsignal_daily['chart close'].loc[i].round(2)),
                                       showarrow=True,
                                       arrowhead=5,
                                       font=dict(
                                               color=color
                                               ),)
                if dfsignal_daily.loc[i, signal] == 'Buy':
                    fig.add_annotation(x=i, y=dfsignal_daily['MA'].loc[i], xref='x1', yred='y1',
                                       text = 'Buy @'+str(dfsignal_daily['chart close'].loc[i].round(2)),
                                       showarrow=True,
                                       arrowhead=5,
                                       font=dict(
                                               color=color
                                               ),) 
        fig.add_trace(go.Scatter(x=dfsignal_daily.index, y=dfsignal_daily.loc[:,'ma'], name='MA'+str(para_dict['move ave.']), xaxis='x1', line=dict(color='blue',
                                 dash='dot'), showlegend=True),
                    row=1, col=1)
        fig.add_trace(go.Scatter(x=dfsignal_daily.index, y=dfsignal_daily.loc[:,'J'], name='J', xaxis='x1', line=dict(color='red',
                                 dash='solid'), showlegend=True),
                    row=2, col=1)
        fig.add_trace(go.Scatter(x=dfsignal_daily.index, y=dfsignal_daily.loc[:,'D'], name='D', xaxis='x1', line=dict(color='blue',
                                 dash='dot'), showlegend=True),
                    row=2, col=1)
        fig.add_trace(go.Scatter(x=dfsignal_daily.index, y=dfsignal_daily.loc[:,'K'], name='K', xaxis='x1', line=dict(color='green',
                                 dash='dot'), showlegend=True),
                    row=2, col=1)
        fig.add_trace(go.Scatter(x=dfsignal_daily.index, y=dfsignal_daily.loc[:,'MACD'], name='MACD', xaxis='x1', line=dict(color='red',
                                 dash='solid'), showlegend=True),
                    row=3, col=1)
        fig.add_trace(go.Scatter(x=dfsignal_daily.index, y=dfsignal_daily.loc[:,'macds'], name='J', xaxis='x1', line=dict(color='blue',
                                 dash='solid'), showlegend=True),
                    row=3, col=1)
                    
        colors_trend = []
        for i in dfsignal_daily.index:
            if dfsignal_daily.loc[i, 'slope'] > para_dict['slope_high_trend']:
                colors_trend.append('green')
            elif dfsignal_daily.loc[i, 'slope'] < para_dict['slope_low_trend']:
                colors_trend.append('red')
            else:
                colors_trend.append('grey')
        fig.add_trace(go.Bar(x=dfsignal_daily.index, y=dfsignal_daily.loc[:,'slope'], name='trend', xaxis = 'x1', market_color = colors_trend, showlegend=True),
                      row=4, col=1)
        
        colors_ma = []
        for i in dfsignal_daily.index:
            if dfsignal_daily.loc[i, 'MAslope'] > para_dict['MAslope_high']:
                colors_ma.append('green')
            elif dfsignal_daily.loc[i, 'MAslope'] < para_dict['MAslope_low']:
                colors_ma.append('red')
            else:
                colors_ma.append('grey')
        fig.add_trace(go.Bar(x=dfsignal_daily.index, y=dfsignal_daily.loc[:,'MAslope'], name='MAtrend', xaxis = 'x1', market_color = colors_ma, showlegend=True),
                      row=5, col=1)
        
        colors= []
        for i in dfsignal_daily.index:
            if dfsignal_daily.loc[i, 'macdh']>0:
                colors.append('green')
            else:
                colors.append('red')
        fig.add_trace(go.Bar(x=dfsignal_daily.index, y=dfsignal_daily.loc[:,'macdh'], name='macdh', xaxis = 'x1', market_color = colors, showlegend=True),
                      row=3, col=1)
        
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
                        row=1, col=2
                )
                
        fig.update_layout(
                autosize=True,
                title_text='TTF '+contract_name+' Backtest Trading Singal '+str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')),
                yaxis_title='EUR',
                hovermode='x unified',
                height=999,
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
                rangebreaks=[
                        dict(counds=['sat','mon']),#hide weekend
                        #dict(values=['2015-12-25','2016-01-01']) #hide XMas and new year
                        ]
                )
        fig.update_layout(legend=dict(
                                        orientation='h',
                                        yanchor='bottom',
                                        y=1.02,
                                        xanchor='left',
                                        x=0
                ))
        '''
        fig['layour']['xaxis2'].update(rangeselector=dict(
                buttons=list([
                dict(count=1, label='1m',step='month',stepmodel='backward'),
                dict(count=6, label='6m',step='month',stepmodel='backward'),
                dict(count=1, label='YTD',step='year',stepmodel='todate'),
                dict(step='all')
                ]))
            )
        '''
        fig.update_xaxes(rangeslider = {'visible':False}, row=1, col=1)
        py.plot(fig, filename='xxxx'+contract_name+'.html', auto_open=True)
        
        return fig
    
    
    def create_test_tickers(start_date, end_date):
        
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
        
        test_date  = pd.date_range(start = start_date, end = end_date, freq = "MS')
        
        bbg_ticker_list = []
        for i in test_date:
            ticker = 'TZT' + month_dict[i.strftime('%b')] + str(i.year)[-2:]+' Comdty'
            bbg_ticker_list.append(ticker)
            
        '''
        season_ticker = ['QQTJ21 Comdty','QQTV21 Comdty']
        bbg_ticker_list = season_ticker
        '''
        return bbg_ticker_list
    
    
    def back_test_static(bbg_ticker_list, para_dict):
        
        dfstatis=pd.DataFrame()
        index_list = ['ave.VAR','Risk Return ratio','Sharpe ratio','Signal correct rate','Ave. total PnL','Ave. Pnl per trade','Max Profit per trade','Max Loss per trade','Max holding days per trade','Min holding days per trade','Ave.holding days per trade','stop level','total sl count']
        
        column_name = []
        for i in bbg_ticker_list:
            column_name.append(Signal_backtest.ticker_to_cont(i))
        
        dfbacktest_statis = pd.DataFrame(index = index_list, columns = column_name)
        for i in bbg_ticker_list:
            dfbbgttf_pivot = Signal_backtest.ttf_curve(i)
            dfdaily = Signal_backtest.curve_start_end(dfbbgttf_pivot,2,2,para_dict) #change back months for season
            #para_dict_flat['s]
            result = Signal_backtest.signal(dfdaily, i, para_dict)
            
            dfstatis, contract_name = Signal_backtest.statistic(result, i, para_dict)
            
            dfbacktest_statis[contract_name] = dfstatis
        dfbacktest_statis = dfbacktest.astype('float')
        dfbacktest_statis_all = pd.DataFrame(index = dfbacktest_statis.index, columns=['Ave.','Sum,'])
        dfbacktest_statis_all =['Ave.'] = dfbacktest_statis.mean(axis=1).round(2)
        
        for i in dfbacktest_statis.columns:
            dfbacktest_statis_all[i] = dfbacktest_statis[i] * (1/dfbacktest_statis.shape[1])
        
        dfbacktest_statis_all['Sum.'] = dfbacktest_statis_all[dfbacktest_statis.columns].sum(axis=1).values
        
        return dfbacktest_statis
    
    def back_test_statis_chart(dfbacktest_statis):
        
        for i in dfbacktest_statis.index[0:5]:
            fig = go.Figure(data=[go.Bar(
                    x=dfbacktest_statis.columns, y=dfbacktest_statis.loc[i],
                    text = dfbnacktest_statis.loc[i],
                    textposition='auto',
                    )])
            fig.update_layour(
                    autosize=True,
                    title_text='TTF MA1 '+i+' Statistics ',+'<br>'+'Ave. '+str(dfbacktest_statis.loc[i].mean().round(2)),
                    #yaxis_title='EUR',
                    hovermode = 'x unified',
                    height = 888,
                    plot_bgcolor = 'white',
                    template = 'ggplot2',
                    )
            py.plot(fig, filename='xxx'+i+'.html', auto_open=True)
            
con = pdblp.Bcon(debug=False, port=8194, timeour=5000)
con.start()


        
                    

            
            
            
                

        
        
        
            
        
