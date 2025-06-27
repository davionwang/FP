# -*- coding: utf-8 -*-
"""
Created on Fri Jun 27 20:00:00 2025

@author: 岳峰
"""

import pandas as pd
import numpy as np
from blp import blp
import datetime
from dateutil.relativedelta import relativedelta
import stockstats

import plotly.offline as py
import plotly.graph_objs as go 
from plotly.subplots import make_subplots
import sys
sys.path.append('xxx')
from common_function import Common_Data

class Grid_Search_signal():
    
    def create_TTF_bbg_flat_ticker():
        
        today = (datetime.date.today() - relativedelta(days=1)).strftime('%Y%m%d')
        start_date = (datetime.date.today() - relativedelta(months=6)).strftime('%Y%m%d')
        
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
        
        ma_ticker_list = []
        
        for i in range(0,12):
            ma = 'TZT' + month_dict[(datetime.date.today() + relativedelta(months=i+1)).strftime('%b')] + str((datetime.date.today() + relativedelta(months=i+1)).year)[-1]+' Comdty'
            ma_ticker_list.append(ma)
            
        qa_ticker_list = []
        if 4 <= datetime.date.today().month <=6:
            
            qa1_ticker = 'QZTN' + str(datetime.date.today().year)[-1] + ' Comdty'
            qa2_ticker = 'QZTV' + str(datetime.date.today().year)[-1] + ' Comdty'
            qa3_ticker = 'QZTF' + str(datetime.date.today().year+1)[-1] + ' Comdty'
            qa4_ticker = 'QZTJ' + str(datetime.date.today().year+1)[-1] + ' Comdty'
            qa5_ticker = 'QZTN' + str(datetime.date.today().year+1)[-1] + ' Comdty'
            qa6_ticker = 'QZTV' + str(datetime.date.today().year+1)[-1] + ' Comdty'
            qa_ticker_list.append(qa1_ticker)
            qa_ticker_list.append(qa2_ticker)
            qa_ticker_list.append(qa3_ticker)
            qa_ticker_list.append(qa4_ticker)
            qa_ticker_list.append(qa5_ticker)
            qa_ticker_list.append(qa6_ticker)
            
        if 7 <= datetime.date.today().month <=9:
            
            qa1_ticker = 'QZTV' + str(datetime.date.today().year)[-1] + ' Comdty'
            qa2_ticker = 'QZTF' + str(datetime.date.today().year+1)[-1] + ' Comdty'
            qa3_ticker = 'QZTJ' + str(datetime.date.today().year+1)[-1] + ' Comdty'
            qa4_ticker = 'QZTN' + str(datetime.date.today().year+1)[-1] + ' Comdty'
            qa5_ticker = 'QZTV' + str(datetime.date.today().year+1)[-1] + ' Comdty'
            qa6_ticker = 'QZTF' + str(datetime.date.today().year+2)[-1] + ' Comdty'
            qa_ticker_list.append(qa1_ticker)
            qa_ticker_list.append(qa2_ticker)
            qa_ticker_list.append(qa3_ticker)
            qa_ticker_list.append(qa4_ticker)
            qa_ticker_list.append(qa5_ticker)
            qa_ticker_list.append(qa6_ticker)
            
        if 10 <= datetime.date.today().month <=12:
            
            qa1_ticker = 'QZTF' + str(datetime.date.today().year+1)[-1] + ' Comdty'
            qa2_ticker = 'QZTJ' + str(datetime.date.today().year+1)[-1] + ' Comdty'
            qa3_ticker = 'QZTN' + str(datetime.date.today().year+1)[-1] + ' Comdty'
            qa4_ticker = 'QZTV' + str(datetime.date.today().year+1)[-1] + ' Comdty'
            qa5_ticker = 'QZTF' + str(datetime.date.today().year+2)[-1] + ' Comdty'
            qa6_ticker = 'QZTJ' + str(datetime.date.today().year+2)[-1] + ' Comdty'
            qa_ticker_list.append(qa1_ticker)
            qa_ticker_list.append(qa2_ticker)
            qa_ticker_list.append(qa3_ticker)
            qa_ticker_list.append(qa4_ticker)
            qa_ticker_list.append(qa5_ticker)
            qa_ticker_list.append(qa6_ticker)
            
        if 1 <= datetime.date.today().month <=3:
            
            qa1_ticker = 'QZTJ' + str(datetime.date.today().year)[-1] + ' Comdty'
            qa2_ticker = 'QZTN' + str(datetime.date.today().year)[-1] + ' Comdty'
            qa3_ticker = 'QZTV' + str(datetime.date.today().year)[-1] + ' Comdty'
            qa4_ticker = 'QZTF' + str(datetime.date.today().year+1)[-1] + ' Comdty'
            qa5_ticker = 'QZTJ' + str(datetime.date.today().year+1)[-1] + ' Comdty'
            qa6_ticker = 'QZTN' + str(datetime.date.today().year+1)[-1] + ' Comdty'
            qa_ticker_list.append(qa1_ticker)
            qa_ticker_list.append(qa2_ticker)
            
            qa_ticker_list.append(qa3_ticker)
            qa_ticker_list.append(qa4_ticker)
            qa_ticker_list.append(qa5_ticker)
            qa_ticker_list.append(qa6_ticker)
            
        #season
        if 4 <= datetime.date.today().month <=9:
            sa1_ticker = 'QQTV' + str(datetime.date.today().year)[-1] + ' Comdty'
            sa2_ticker = 'QQTJ' + str(datetime.date.today().year+1)[-1] + ' Comdty'
            sa3_ticker = 'QQTV' + str(datetime.date.today().year+1)[-1] + ' Comdty'
            
        if 10 <= datetime.date.today().month <=12:
            sa1_ticker = 'QQTJ' + str(datetime.date.today().year+1)[-1] + ' Comdty'
            sa2_ticker = 'QQTV' + str(datetime.date.today().year+1)[-1] + ' Comdty'
            sa3_ticker = 'QQTJ' + str(datetime.date.today().year+2)[-1] + ' Comdty'
            
        if 1 <= datetime.date.today().month <=3:
            sa1_ticker = 'QQTJ' + str(datetime.date.today().year)[-1] + ' Comdty'
            sa2_ticker = 'QQTV' + str(datetime.date.today().year)[-1] + ' Comdty'
            sa3_ticker = 'QQTJ' + str(datetime.date.today().year+1)[-1] + ' Comdty'
            
        ca1_ticker = 'QTTF' + str(datetime.date.today().year+1)[-1] + ' Comdty'
        ca2_ticker = 'QTTF' + str(datetime.date.today().year+2)[-1] + ' Comdty'  
        
        ma_ticker_list = ma_ticker_list + qa_ticker_list
        ma_ticker_list.append(sa1_ticker)
        ma_ticker_list.append(sa2_ticker)
        ma_ticker_list.append(sa3_ticker)
        ma_ticker_list.append(ca1_ticker)
        ma_ticker_list.append(ca2_ticker)
        
        return ma_ticker_list
    
    def create_TTF_bbg_ts_ticker():
        
        today = (datetime.date.today() - relativedelta(days=1)).strftime('%Y%m%d')
        start_date = (datetime.date.today() - relativedelta(months=6)).strftime('%Y%m%d')
        
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
        
        ma_ticker_list = []
        
        for i in range(0,12):
            ma = 'TZT' + month_dict[(datetime.date.today() + relativedelta(months=i+1)).strftime('%b')] + str((datetime.date.today() + relativedelta(months=i+1)).year)[-1]+' Comdty'
            ma_ticker_list.append(ma)
            
        qa_ticker_list = []
        if 4 <= datetime.date.today().month <=6:
            
            qa1_ticker = 'QZTN' + str(datetime.date.today().year)[-1] + ' Comdty'
            qa2_ticker = 'QZTV' + str(datetime.date.today().year)[-1] + ' Comdty'
            qa3_ticker = 'QZTF' + str(datetime.date.today().year+1)[-1] + ' Comdty'
            qa4_ticker = 'QZTJ' + str(datetime.date.today().year+1)[-1] + ' Comdty'
            qa5_ticker = 'QZTN' + str(datetime.date.today().year+1)[-1] + ' Comdty'
            qa6_ticker = 'QZTV' + str(datetime.date.today().year+1)[-1] + ' Comdty'
            qa_ticker_list.append(qa1_ticker)
            qa_ticker_list.append(qa2_ticker)
            qa_ticker_list.append(qa3_ticker)
            qa_ticker_list.append(qa4_ticker)
            qa_ticker_list.append(qa5_ticker)
            qa_ticker_list.append(qa6_ticker)
            
        if 7 <= datetime.date.today().month <=9:
            
            qa1_ticker = 'QZTV' + str(datetime.date.today().year)[-1] + ' Comdty'
            qa2_ticker = 'QZTF' + str(datetime.date.today().year+1)[-1] + ' Comdty'
            qa3_ticker = 'QZTJ' + str(datetime.date.today().year+1)[-1] + ' Comdty'
            qa4_ticker = 'QZTN' + str(datetime.date.today().year+1)[-1] + ' Comdty'
            qa5_ticker = 'QZTV' + str(datetime.date.today().year+1)[-1] + ' Comdty'
            qa6_ticker = 'QZTF' + str(datetime.date.today().year+2)[-1] + ' Comdty'
            qa_ticker_list.append(qa1_ticker)
            qa_ticker_list.append(qa2_ticker)
            qa_ticker_list.append(qa3_ticker)
            qa_ticker_list.append(qa4_ticker)
            qa_ticker_list.append(qa5_ticker)
            qa_ticker_list.append(qa6_ticker)
            
        if 10 <= datetime.date.today().month <=12:
            
            qa1_ticker = 'QZTF' + str(datetime.date.today().year+1)[-1] + ' Comdty'
            qa2_ticker = 'QZTJ' + str(datetime.date.today().year+1)[-1] + ' Comdty'
            qa3_ticker = 'QZTN' + str(datetime.date.today().year+1)[-1] + ' Comdty'
            qa4_ticker = 'QZTV' + str(datetime.date.today().year+1)[-1] + ' Comdty'
            qa5_ticker = 'QZTF' + str(datetime.date.today().year+2)[-1] + ' Comdty'
            qa6_ticker = 'QZTJ' + str(datetime.date.today().year+2)[-1] + ' Comdty'
            qa_ticker_list.append(qa1_ticker)
            qa_ticker_list.append(qa2_ticker)
            qa_ticker_list.append(qa3_ticker)
            qa_ticker_list.append(qa4_ticker)
            qa_ticker_list.append(qa5_ticker)
            qa_ticker_list.append(qa6_ticker)
            
        if 1 <= datetime.date.today().month <=3:
            
            qa1_ticker = 'QZTJ' + str(datetime.date.today().year)[-1] + ' Comdty'
            qa2_ticker = 'QZTN' + str(datetime.date.today().year)[-1] + ' Comdty'
            qa3_ticker = 'QZTV' + str(datetime.date.today().year)[-1] + ' Comdty'
            qa4_ticker = 'QZTF' + str(datetime.date.today().year+1)[-1] + ' Comdty'
            qa5_ticker = 'QZTJ' + str(datetime.date.today().year+1)[-1] + ' Comdty'
            qa6_ticker = 'QZTN' + str(datetime.date.today().year+1)[-1] + ' Comdty'
            qa_ticker_list.append(qa1_ticker)
            qa_ticker_list.append(qa2_ticker)
            
            qa_ticker_list.append(qa3_ticker)
            qa_ticker_list.append(qa4_ticker)
            qa_ticker_list.append(qa5_ticker)
            qa_ticker_list.append(qa6_ticker)
            
        #season
        if 4 <= datetime.date.today().month <=9:
            sa1_ticker = 'QQTV' + str(datetime.date.today().year)[-1] + ' Comdty'
            sa2_ticker = 'QQTJ' + str(datetime.date.today().year+1)[-1] + ' Comdty'
            sa3_ticker = 'QQTV' + str(datetime.date.today().year+1)[-1] + ' Comdty'
            
        if 10 <= datetime.date.today().month <=12:
            sa1_ticker = 'QQTJ' + str(datetime.date.today().year+1)[-1] + ' Comdty'
            sa2_ticker = 'QQTV' + str(datetime.date.today().year+1)[-1] + ' Comdty'
            sa3_ticker = 'QQTJ' + str(datetime.date.today().year+2)[-1] + ' Comdty'
            
        if 1 <= datetime.date.today().month <=3:
            sa1_ticker = 'QQTJ' + str(datetime.date.today().year)[-1] + ' Comdty'
            sa2_ticker = 'QQTV' + str(datetime.date.today().year)[-1] + ' Comdty'
            sa3_ticker = 'QQTJ' + str(datetime.date.today().year+1)[-1] + ' Comdty'
            
        ca1_ticker = 'QTTF' + str(datetime.date.today().year+1)[-1] + ' Comdty'
        ca2_ticker = 'QTTF' + str(datetime.date.today().year+2)[-1] + ' Comdty'  
        
        month_ts = []
        for i in range(0, len(ma_ticker_list)-1):
            month_ts.append(ma_ticker_list[i] + ma_ticker_list[i+1][-2:] + ' Comdty')
            i = i+1
            
        quarter_ts = []
        for i in range(0, len(qa_ticker_list)-1):
            quarter_ts.append(qa_ticker_list[i][0:5] + qa_ticker_list[i+1][3:])
            i=i+1
        
        season_ts = [sa1_ticker[0:5] + sa2_ticker[3:5] + ' Comdty', sa1_ticker[0:5] + sa3_ticker[3:5] + ' Comdty', sa2_ticker[0:5] + sa3_ticker[3:5] + ' Comdty']
        cal_ts = [cal_ticker[0:5] + ca2_ticker[3:5] + ' Comdty']
        
        tstickers = month_ts + quarter_ts + season_ts + cal_ts
        
        return tstickers
    
    def get_bbg_curve(ticker):
        
        today = (datetime.date.today() - relativedelta(days=0)).strftime('%Y%m%d')
        start_date = pd.to_datetime(str((datetime.date.today() - relativedelta(months=6)).year)+'-'+str((datetime.date.today() - relativedelta(months=6)).month+'-01').strftime('%Y%m%d')
        
        dfbbgttf = con.bdh(tickers = ticker, flds=['PX_OPEN','PX_HIGH','PX_LOW','PX_LAST','Px_Settle'],start_date = start_date,end_date = today, longdata=True)
        dfbbgttf = dfbbgttf[['date','field','value']]
        dfbbgttf_group = dfbbgttf.groupby(['date','field'], as_index = False).mean()
        dfbbg_ttf_pivot = pd.pivot(dfbbgttf_group, index = 'date', columns = 'field')
        dfbbgttf_pivot.columns = dfbbgttf_pivot.columns.droplevel(0)
        
        if pd.isna(dfbbgttf_pivot['Px_Settle'].iloc[-1])==False:
            dfbbgttf_pivot.loc[dfbbgttf_pivot.index[-1], 'PX_LAST'] = dfbbgttf_pivot.loc[dfbbgttf_pivot.index[-1], 'Px_Settle']
            
        return dfbbgttf
    
    def curve_start_end(dfbbgttf_pivot, end_nbday, start_nm, para_dict):
        
        end_date = dfbbgttf_pivot.index[end_nbday]
        start_date = str((end_date - relativedelta(months=start_nm)).year)+'-'+str((end_date - relativedelta(months=start_nm)).month)+'-01'
        
        dfbbgttf_pivot['ma'] = dfbbgttf_pivot['PX_LAST'].rolling(para_dict['move ave.']).mean()
        dfbbgttf_pivot['std'] = dfbbgttf_pivot['PX_LAST'].rolling(30).std()
        dfdaily = dfbbgttf_pivot.loc[start_date:end_date]
        if dfdaily.loc[dfdaily.index[0], 'PX_HIGH'] <= dfdaily.loc[dfdaily.index[0], 'PX_LOW']:
            dfdaily.loc[dfdaily.index[0], 'PX_LOW'] = 2*dfdaily.loc[dfdaily.index[0], 'PX_LAST'] - dfdaily.loc[dfdaily.index[0], 'PX_HIGH']
            
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
    
    def signal_daily(dfdaily, ticker, para_dict):
        
        today = datetime.date.today()
        
        stock = stockstats.StockDataFrame.retype(dfdaily)
        
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
        
        MACD line (macd): (12-day EMA - 26-day EMA)
        Signal Line (macds): 9-day EMA of MACD Line
        MACD Histogram (macdh): MACD Line - Signal Line
        
        MD = MD1 + (Price - MD1) / (N * (Price/MD1)^4)
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
            
        #signal logic
        result.loc[:,'Jsignal'] = np.nan
        result.loc[:,'MAsignal'] = np.nan
        result.loc[:,'MACDsignal'] = np.nan
        result.loc[:,'signal'] = np.nan
        
       for i in range(1, result.shape[0]):
           try:
               if result.loc[result.index[i], 'MAslope'] <= para_dict['MAslope_low'] or result.loc[result.index[i], 'MAslope'] >= para_dict['MAslope_high'] :
                    
                    if result.loc[result.index[i], 'MAslope'] <= para_dict['MAslope_low'] and (result.loc[result.index[i], trend_variable]) > result.loc[result.index[i], 'close']:
                        
                        result.loc[result.index[i], 'MACDsignal'] = 'Sell'
                        result.loc[result.index[i], 'Jsignal'] = 'Sell'
                        
                    if result.loc[result.index[i], 'MAslope'] >= para_dict['MAslope_high'] and (result.loc[result.index[i], trend_variable]) < result.loc[result.index[i], 'close']:
                            if result.loc[result.index[i], 'MAslope'] <= para_dict['MAslope_low'] and (result.loc[result.index[i], trend_variable]) > result.loc[result.index[i], 'close'] and 
                                result.loc[result.index[i], 'slope'] > para_dict['slope_high_trend']:
                                    result.loc[result.index[i], 'MACDsignal'] = 'Buy'
                                    result.loc[result.index[i], 'Jsignal'] = 'Buy'
                                    
                            if result.loc[result.index[i], 'MAslope'] >= para_dict['MAslope_high'] and (result.loc[result.index[i], trend_variable]) < result.loc[result.index[i], 'close'] and 
                                result.loc[result.index[i], 'slope'] < para_dict['slope_low_trend']:
                                    result.loc[result.index[i], 'MACDsignal'] = 'Sell'
                                    result.loc[result.index[i], 'Jsignal'] = 'Sell'
                                    
                            if result.loc[result.index[i], 'MAslope'] <= para_dict['MAslope_low'] and (result.loc[result.index[i], trend_variable]) <= result.loc[result.index[i], 'close']:
                                    result.loc[result.index[i], 'MACDsignal'] = 'Buy'
                                    result.loc[result.index[i], 'Jsignal'] = 'Buy'
                            
                            if result.loc[result.index[i], 'MAslope'] >= para_dict['MAslope_high'] and (result.loc[result.index[i], trend_variable]) >= result.loc[result.index[i], 'close']:
                                    result.loc[result.index[i], 'MACDsignal'] = 'Sell'
                                    result.loc[result.index[i], 'Jsignal'] = 'Sell'
                    else:
                        if result.loc[result.index[i-1], 'J'] >= para_dict['j_high'] and result.loc[result.index[i], 'J'] < result.loc[result.index[i-1], 'J'] and result.loc[result.index[i], 'J'] > para_dict['j_low']:
                            result.loc[result.index[i], 'Jsignal'] = 'Sell'
                            
                        if result.loc[result.index[i-1], 'J'] <= para_dict['j_low'] and result.loc[result.index[i], 'J'] > result.loc[result.index[i-1], 'J'] and result.loc[result.index[i], 'J'] < para_dict['j_high']:
                            result.loc[result.index[i], 'Jsignal'] = 'Buy'
                            
                        if result.loc[result.index[i-1], 'macdh'] > result.loc[result.index[i], 'macdh'] +0.01:
                            result.loc[result.index[i], 'MACDsignal'] = 'Sell'
                            
                        if result.loc[result.index[i-1], 'macdh'] < result.loc[result.index[i], 'macdh'] +0.01:
                            result.loc[result.index[i], 'MACDsignal'] = 'Buy'
                            
            except KeyError as e:
                        print(ticker)
                        pass
                    
        result.MACDsignal.fillna(method='ffill', inplace=True)
        result.Jsignal.fillna(method='ffill', inplace=True)
        
        #create buy and sell signal
        for i in result.index:
            if result.loc[i, 'Jsignal'] == 'Sell' and  result.loc[i, 'MACDsignal'] == 'Sell':
                result.loc[i, 'signal'] = 'Sell'
                result.loc[i, 'signal time'] = datetime.datetime.now()
                
            if result.loc[i, 'Jsignal'] == 'Buy' and  result.loc[i, 'MACDsignal'] == 'Buy':
                result.loc[i, 'signal'] = 'Buy'
                result.loc[i, 'signal time'] = datetime.datetime.now()
        
        #final signal
        for i in range(0, result.shape[0]):
            if result.loc[result.index[i], 'signal'] == 'Sell' or result.loc[result.index[i], 'signal'] == 'Close Buy and Sell':
                for j in range(i+1, result.shape[0]):
                    if result.loc[result.index[i], 'close'] - result.loc[result.index[j], 'close'] <= para_dict['sl_abs']:
                        if result.loc[result.index[i], 'signal'] == 'Sell Close':
                            result.loc[result.index[j], 'signal'] = np.nan
                        else:
                            result.loc[result.index[j], 'signal'] = 'Sell Close'
                        break
                    elif result.loc[result.index[j], 'signal'] = 'Sell':
                        result.loc[result.index[j], 'signal'] = np.nan
                    elif result.loc[result.index[j], 'signal'] = 'Buy':
                        if result.loc[result.index[i], 'signal'] == 'Sell Close':
                            result.loc[result.index[j], 'signal'] = 'Buy'
                            break
                        else:
                            result.loc[result.index[j], 'signal'] = 'Close Sell and Buy'
                            result.loc[result.index[j], 'signal time'] = datetime.datetime.now()
                            break
                    else:
                        continue
                    
                    
            if result.loc[result.index[i], 'signal'] == 'Buy' or result.loc[result.index[i], 'signal'] == 'Close Sell and Buy':
                for j in range(i+1, result.shape[0]):
                    if result.loc[result.index[i], 'close'] - result.loc[result.index[j], 'close'] <= para_dict['sl_abs']:
                        if result.loc[result.index[i], 'signal'] == 'Buy Close':
                            result.loc[result.index[j], 'signal'] = np.nan
                        else:
                            result.loc[result.index[j], 'signal'] = 'Buy Close'
                        break
                    elif result.loc[result.index[j], 'signal'] = 'Buy':
                        result.loc[result.index[j], 'signal'] = np.nan
                    elif result.loc[result.index[j], 'signal'] = 'Sell':
                        if result.loc[result.index[i], 'signal'] == 'Buy Close':
                            result.loc[result.index[j], 'signal'] = 'Sell'
                            break
                        else:
                            result.loc[result.index[j], 'signal'] = 'Close Buy and Sell'
                            result.loc[result.index[j], 'signal time'] = datetime.datetime.now()
                            break
                    else:
                        continue
                    
        dfstatis, column_name, dfreport = Common_Data.statistic(result, ticker, para_dict)
        #days back if exist signal to report
        chart_contract = []
        for i in result.index:
            if result.loc[i, 'signal'] == 'Buy' or result.loc[i, 'signal'] == 'Sell' or result.loc[i, 'signal'] == 'Close Buy and Sell' or result.loc[i, 'signal'] == 'Close Sell and Buy' or result.loc[i, 'signal'] == 'Buy Close' or result.loc[i, 'signal'] == 'Sell Close':
                #plot chart
                contract = Grid_Search_signal.ticker_to_cont(ticker)
                chart_contract.append(contract)
                
        return result, dfreport
    
    def statistic(result, ticker, para_dict):
        
        #calc cum, pnl
        cumpnl = pd.DataFrame(columns=['Sell','Sell Close','Buy','Buy Close'])
        
        for i in result.index:
            if result.loc[i, 'signal'] == 'Sell' or result.loc[i, 'signal'] == 'Close Buy and Sell':
                cumpnl.loc[i, 'Sell'] = result.loc[i, 'chart close']
                for j in result.loc[i:].index:
                    if result.loc[j, 'signal'] == 'Sell Close' or result.loc[j, 'signal'] == 'Close Sell and Buy':
                        cumpnl.loc[j, 'Sell Close'] = result.loc[j, 'chart close']
                        break
                    
            if result.loc[i, 'signal'] == 'Buy' or result.loc[i, 'signal'] == 'Close Sell and Buy':
                cumpnl.loc[i, 'Buy'] = result.loc[i, 'chart close']
                for j in result.loc[i:].index:
                    if result.loc[j, 'signal'] == 'Buy Close' or result.loc[j, 'signal'] == 'Close Buy and Sell':
                        cumpnl.loc[j, 'Buy Close'] = result.loc[j, 'chart close']
                        break
                    
        cumpnl.sort_index(inplace=True)
        if pd.notna(cumpnl.loc[cumpnl.index[-1], 'Sell']):
            cumpnl.loc[result.index[-1], 'Sell Close'] =  cumpnl.loc[result.index[-1], 'chart close']
        if pd.notna(cumpnl.loc[cumpnl.index[-1], 'Buy']):
            cumpnl.loc[result.index[-1], 'Buy Close'] =  cumpnl.loc[result.index[-1], 'chart close']
        cumpnl.fillna(0, inplace=True)
        
        #pnl
        for i in range(1, cumpnl.shape[0]):
            cumpnl.loc[cumpnl.index[i], 'Buy pnl'] = cumpnl.loc[cumpnl.index[i], 'Buy Close'] - cumpnl.loc[cumpnl.index[i-1], 'Buy']
            cumpnl.loc[cumpnl.index[i], 'Sell pnl'] = -1*(cumpnl.loc[cumpnl.index[i], 'Sell Close'] - cumpnl.loc[cumpnl.index[i-1], 'Sell'])
        #pnl for tiday got signal
        if cumpnl.loc[cumpnl.index[-1], 'Sell'] == cumpnl.loc[cumpnl.index[-1], 'Sell Close']:
            cumpnl.loc[cumpnl.index[-1], 'Sell pnl'] = 0
        if cumpnl.loc[cumpnl.index[-1], 'Buy'] == cumpnl.loc[cumpnl.index[-1], 'Buy Close']:
            cumpnl.loc[cumpnl.index[-1], 'Buy pnl'] = 0
            
        for i in range(1, cumpnl.shape[0]):
            if cumpnl.loc[cumpnl.index[i], 'Buy pnl'] <= para_dict['sl_abs']:
                cumpnl.loc[cumpnl.index[i], 'Buy pnl'] = para_dict['sl_abs']
            if cumpnl.loc[cumpnl.index[i], 'Sell pnl'] <= para_dict['sl_abs']:
                cumpnl.loc[cumpnl.index[i], 'Sell pnl'] = para_dict['sl_abs']
        
        cumpnl['pnl'] = cumpnl['Buy pnl'] + cumpnl['Sell pnl']
        
        #DAILY REPORT
        index_name = [Common_data.ticker_to_cont(ticker)]
        dfreport = pd.DataFrame(index = index_name, columns = ['Signal','Start Date','Signal Price','Market EoD','PnL'])
        if cumpnl['Buy Close'].iloc[-1]!= 0 and cumpnl['Buy'].iloc[-1] == 0 and cumpnl['Sell'].iloc[-1] == 0 and cumpnl['Sell Close'].iloc[-1] == 0:
            dfreport.loc[index_name, 'Signal'] = 'Buy'
            dfreport.loc[index_name, 'Start Date'] = cumpnl.index[-2].strftime('%Y-%m-%d')
            dfreport.loc[index_name, 'Signal Price'] = cumpnl['Buy'].iloc[-2].round(2)
            dfreport.loc[index_name, 'Market EoD'] = result['chart close'].iloc[-1].round(2) #use mid prices
            dfreport.loc[index_name, 'PnL'] = cumpnl['Buy pnl'].iloc[-1].round(2)
        if cumpnl['Buy Close'].iloc[-1]!= 0 and cumpnl['Buy'].iloc[-1] != 0 and cumpnl['Sell'].iloc[-1] == 0 and cumpnl['Sell Close'].iloc[-1] != 0:
            dfreport.loc[index_name, 'Signal'] = 'Buy'
            dfreport.loc[index_name, 'Start Date'] = cumpnl.index[-1].strftime('%Y-%m-%d')
            dfreport.loc[index_name, 'Signal Price'] = cumpnl['Buy'].iloc[-1].round(2)
            dfreport.loc[index_name, 'Market EoD'] = result['chart close'].iloc[-1].round(2) #use mid prices
            dfreport.loc[index_name, 'PnL'] = cumpnl['Buy pnl'].iloc[-1].round(2)
        if cumpnl['Buy Close'].iloc[-1]!= 0 and cumpnl['Buy'].iloc[-1] != 0 and cumpnl['Sell'].iloc[-1] == 0 and cumpnl['Sell Close'].iloc[-1] == 0:
            dfreport.loc[index_name, 'Signal'] = 'Buy'
            dfreport.loc[index_name, 'Start Date'] = cumpnl.index[-1].strftime('%Y-%m-%d')
            dfreport.loc[index_name, 'Signal Price'] = cumpnl['Buy'].iloc[-1].round(2)
            dfreport.loc[index_name, 'Market EoD'] = result['chart close'].iloc[-1].round(2) #use mid prices
            dfreport.loc[index_name, 'PnL'] = cumpnl['Buy pnl'].iloc[-1].round(2)
            
        if cumpnl['Sell Close'].iloc[-1]!= 0 and cumpnl['Sell'].iloc[-1] == 0 and cumpnl['Buy'].iloc[-1] == 0 and cumpnl['Buy Close'].iloc[-1] == 0:
            dfreport.loc[index_name, 'Signal'] = 'Sell'
            dfreport.loc[index_name, 'Start Date'] = cumpnl.index[-2].strftime('%Y-%m-%d')
            dfreport.loc[index_name, 'Signal Price'] = cumpnl['Sell'].iloc[-2].round(2)
            dfreport.loc[index_name, 'Market EoD'] = result['chart close'].iloc[-1].round(2) #use mid prices
            dfreport.loc[index_name, 'PnL'] = cumpnl['Sell pnl'].iloc[-1].round(2)
        if cumpnl['Sell Close'].iloc[-1]!= 0 and cumpnl['Sell'].iloc[-1] != 0 and cumpnl['Buy'].iloc[-1] == 0 and cumpnl['Buy Close'].iloc[-1] != 0:
            dfreport.loc[index_name, 'Signal'] = 'Sell'
            dfreport.loc[index_name, 'Start Date'] = cumpnl.index[-1].strftime('%Y-%m-%d')
            dfreport.loc[index_name, 'Signal Price'] = cumpnl['Sell'].iloc[-1].round(2)
            dfreport.loc[index_name, 'Market EoD'] = result['chart close'].iloc[-1].round(2) #use mid prices
            dfreport.loc[index_name, 'PnL'] = cumpnl['Sell pnl'].iloc[-1].round(2)
        if cumpnl['Sell Close'].iloc[-1]!= 0 and cumpnl['Sell'].iloc[-1] != 0 and cumpnl['Buy'].iloc[-1] == 0 and cumpnl['Buy Close'].iloc[-1] == 0:
            dfreport.loc[index_name, 'Signal'] = 'Sell'
            dfreport.loc[index_name, 'Start Date'] = cumpnl.index[-1].strftime('%Y-%m-%d')
            dfreport.loc[index_name, 'Signal Price'] = cumpnl['Sell'].iloc[-1].round(2)
            dfreport.loc[index_name, 'Market EoD'] = result['chart close'].iloc[-1].round(2) #use mid prices
            dfreport.loc[index_name, 'PnL'] = cumpnl['Sell pnl'].iloc[-1].round(2)
        
                        
        #correct rate
        correct = []
        incorrect = []
        for i in cumpnl.index:
            if cumpnl.loc[i, 'pnl'] > 0:
                correct.append(1)
                
            if cumpnl.loc[i, 'pnl'] < 0:
                incorrect.append(1)  
        if len(correct) + len(incorrect) != 0:
            correct_rate = round(len(correct)/(len(correct) + len(incorrect)), 2)
        else:
            correct_rate = 0
            
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
            
        if livepnl != 0:
            risk_return = livepnl/cumpnl['risk'].sum()
        else:
            risk_return = 0
            
        #sharpe
        for i in range(1, cumpnl.shape[0]-1):
            cumpnl.loc[cumpnl.index[i], 'num of days'] = cumpnl.index[i] - cumpnl.index[i-1]
            
        if livepnl != 0:
            mean_return = livepnl/cumpnl['num of days'].sum().days*100
            risk = cumpnl['pnl'].std()
            sharpe = mean_return / (risk)
        else:
            mean_return = 0
            sharpe = 0
            
        #max holding days, min holding days
        if livepnl != 0:
            holding_days_max = cumpnl['num of days'].max().days
            holding_days_min = cumpnl['num of days'].min().days
            ave_days = cumpnl['num of days'].mean()
        else:
            holding_days_max = 0
            holding_days_min = 0
            ave_days = pd.NaT
            
        #contract = Signal_backtest.ticker_to_cont(ticker)
        index_list = ['Risk Return ratio','Sharpe ratio','Signal correct rate','Total PnL','Ave. Pnl per trade','Max Profit per trade','Max Loss per trade','Max holding days per trade','Min holding days per trade','Ave.holding days per trade']
        column_name = Common_Data.ticker_to_cont(ticker)
        dfstatis = pd.DataFrame(index = index_list, columns = [column_name])
        dfstatis.loc['Signal correct rate'] = round(correct_rate, 2)
        dfstatis.loc['Risk Return ratio'] = round(risk_return, 2)
        dfstatis.loc['Sharpe ratio'] = round(sharpe,2)
        dfstatis.loc['Total PnL'] = livepnl
        dfstatis.loc['Ave. PnL per trade'] = round(average_return, 2)
        dfstatis.loc['Max Profit per trade'] = round(max_win, 2)
        dfstatis.loc['Max Loss per trade'] = round(max_loss, 2)
        dfstatis.loc['Max holding days per trade'] = holding_days_max
        dfstatis.loc['Min holding days per trade'] = holding_days_min
        dfstatis.loc['Ave. holding days per trade'] = ave_days.days
        
        dfreport.loc[index_name, 'Correct_rate'] = correct_rate
        
        return dfstatis, column_name, dfreport
    
    def chart(dfsignal_daily, contract_name, dfstatis, para_dict):
        
        fig = make_subplots(rows=4, cols=2,
                            shared_xaxes = True,
                            vertical_spacing = 0.02,
                            subplot_titles = ('Daily Signal','Statistics','KDJ','MACD','Trend Vector Degree '+str(para_dict['slope_high_trend'])+'/'+str(para_dict['slope_low_trend']),),
                            column_widths = [0.7, 0.3],
                            row_hights=[0.4, 0.2, 0.2, 0.2],
                            specs=[[{"type": "scatter"},{"type": "table", "rowspan":3}],
                                   [{"type": "scatter"}, None],
                                   [{"type": "scatter"}, None],
                                   [{"type": "scatter"}, None]],
                                    )
        #daily charts
        today = datetime.date.today()
        start = str((today - relativedelta(month = 1)).year) + '-'+str((today - relativedelta(month = 1)).month)+'-01'
        
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
                    row=1, col=1)
        fig.add_trace(go.Scatter(x=dfsignal_daily.index, y=dfsignal_daily.loc[:,'J'], name='J', xaxis='x1', line=dict(color='red',
                                 dash='solid'), showlegend=False),
                    row=2, col=1)
        fig.add_trace(go.Scatter(x=dfsignal_daily.index, y=dfsignal_daily.loc[:,'D'], name='D', xaxis='x1', line=dict(color='blue',
                                 dash='dot'), showlegend=False),
                    row=2, col=1)
        fig.add_trace(go.Scatter(x=dfsignal_daily.index, y=dfsignal_daily.loc[:,'K'], name='K', xaxis='x1', line=dict(color='green',
                                 dash='dot'), showlegend=False),
                    row=2, col=1)
        fig.add_trace(go.Scatter(x=dfsignal_daily.index, y=dfsignal_daily.loc[:,'MACD'], name='MACD', xaxis='x1', line=dict(color='red',
                                 dash='solid'), showlegend=False),
                    row=3, col=1)
        fig.add_trace(go.Scatter(x=dfsignal_daily.index, y=dfsignal_daily.loc[:,'macds'], name='J', xaxis='x1', line=dict(color='blue',
                                 dash='solid'), showlegend=False),
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
    
    def daily_report_chart(dfreport_all, title):
        
        fig = go.Figure()
        
        fig.add_trace(go.Scatter(
                                    x = dfreport_all.index,
                                    y = dfreport_all['Signal Price'],
                                    name = 'Signal Price',
                                    mode = 'lines+makers',
                                    line = dict(color='red',
                                                dash='solid'),
                                                showlegend=True
                                    ))
        fig.add_trace(go.Scatter(
                                    x = dfreport_all.index,
                                    y = dfreport_all['Market EoD'],
                                    name = 'Market EoD',
                                    mode = 'lines+makers',
                                    line = dict(color='black',
                                                dash='solid'),
                                                showlegend=True
                                    ))
        for i in dfreport_all.index:
            if dfreport_all.loc[i, 'Signal'] == 'Sell':
                fig.add_annotation(
                            x=i, y=dfreport_all['Signal Price'].loc[i], xref='x1',yref='y1',
                            text = 'Sell @'+str(dfreport_all['Signal Price'].loc[i])+'<br>'+'on '+dfreport_all['Start date'].loc[i]+'<br>'+'PnL '+str(dfreport_all['PnL'].loc[i]),
                            font = dict(color='red'),
                        )
                if dfreport_all.loc[i, 'Signal'] == 'Buy':
                fig.add_annotation(
                            x=i, y=dfreport_all['Signal Price'].loc[i], xref='x1',yref='y1',
                            text = 'Sell @'+str(dfreport_all['Signal Price'].loc[i])+'<br>'+'on '+dfreport_all['Start date'].loc[i]+'<br>'+'PnL '+str(dfreport_all['PnL'].loc[i]),
                            font = dict(color='green'),
                        )
                
        fig.update_layout(
                autosize=True,
                title_text=title+' EoD Daily Trading Signal '++str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')),
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
        
        py.plot(fig, filename='xxx'+'.html',auto_open=True)
        
        
    def grid_table(dfreport_all):
        
        #table color
        dfcolor = dfreport_all.copy()
        for ism in dfcolor.index:
            if dfcolor.loc[ism, 'Singal'] == 'Sell':
                dfcolor.loc.loc[ism, 'Signal'] = 'red'
            if dfcolor.loc[ism, 'Signal'] == 'Buy':
                dfcolor.loc[ism, 'Signal'] == 'Green'
            for jsm in ['Start date']:
                if dfcolor.loc[ism, jsm] == datetime.date.today().strftime('%Y-%m-%d'):
                    dfcolor.loc[ism, jsm] = 'red'
                else:
                    dfcolor.loc[ism, jsm] = 'black'
            for jsm in ['PnL']:
                    if dfcolor.loc[ism, jsm]>0:
                        dfcolor.loc[ism, jsm] = 'Green'
                    elif dfcolor.loc[ism, jsm]<0:
                        dfcolor.loc[ism, jsm] = 'red'
                    else:
                        dfcolor.loc[ism, jsm] = 'black'
        dfcolor[['Signal Price','Market EoD','Correct rate']] = 'black'
        
        dfcolor.insert(0, 'date', ['black']*dfcolor.shape[0])
        
        dfcolor = dfcolor.T
        
        fig = go.Figure()
        fig.add_trace(go.Table(
                                header = dict(
                                        values = ['Contract','Signal','Start date','Signal Price','Market Live','Pnl','Correct rate'],
                                        font=dict(size=10),
                                        align='left'
                                        ),
                                cells = dict(
                                        values=[dfreport_all.index, dfreport_all['Signal'].tolist(), dfreport_all['Start date'].tolist(),dfreport_all['Signal Price'].tolist(),dfreport_all['Market Eod'].tolist(), dfreport_all['PnL'].tolist(),dfreport_all['Correct rate'].tolist()],
                                        align = 'lift',
                                        font = dict(color=dfcolor, values.tolist())
                                        ),
                ),
                )
                                
        fig.update_layout(title_text = 'TTF Grid Search Signal '+ str(datetime.datetime.now()))
        fig.update_layout(
                autosize=True,
                title_text = 'TTF Grid Search Signal '+str(datetime.datetime.now()),
                height=998,
                )
        py.plot(fig, filename = 'xxx', auto_open=False)
        return fig
    
con = pdble.BCon(debug=False, port = 8194, timeout=5000)
con.start()

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
'''
tickers = Grid_Search_signal.create_TTF_bbg_flat_ticker()
ts_tickers = Grid_Search_signal.create_TTF_bbg_ts_ticker()

dfreport_all = pd.DataFrame(columns=['Signal','Start date','Signal Price','Market Live','Pnl','Correct rate'])
#TTF flat signal
for i in tickers[:]:
    try:
        dfbbgttf_pivot = Grid_Search_signal.get_bbg_curve(i)
        dfdaily = Grid_Search_signal.curve_start_end(dfbbgttf_pivot, 1, 6, ttf_para_dict_flat)
        result, dfreport = Grid_Search_signal.signal_daily(dfdaily, i, ttf_para_dict_flat)
        dfreport_all.loc[dfreport.index[0]] = dfreport.loc[dfreport.index[0]]
        
        
        