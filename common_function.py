# -*- coding: utf-8 -*-
"""
Created on Sat Jun 21 11:17:00 2025

@author: 岳峰
"""

import datetime
from dateutil.relativedelta import relativedelta
import stockstats
import pandas as pd
import numpy as np
import math

import sys
sys.path.append('xxx')

from CEtools import CEtools
from CEtools import *
ce = CEtools.CElink('name','pw')
import CErename

class Common_Data():
    
    def create_TTF_bbg_flat_ticker():
        
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
    
    def tech_signal(dfdaily, ticker, para_dict):
        
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
                '''
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
                    '''
                    '''
                    if result.loc[result.index[i-1], 'J'] >= para_dict['j_high'] and result.loc[result.index[i], 'J'] < result.loc[result.index[i-1], 'J'] and result.loc[result.index[i], 'J'] > para_dict['j_low']:
                        result.loc[result.index[i], 'Jsignal'] = 'Sell'
                            
                    if result.loc[result.index[i-1], 'J'] <= para_dict['j_low'] and result.loc[result.index[i], 'J'] > result.loc[result.index[i-1], 'J'] and result.loc[result.index[i], 'J'] < para_dict['j_high']:
                        result.loc[result.index[i], 'Jsignal'] = 'Buy'
                    
                    if result.loc[result.index[i-1], 'slope'] <= para_dict['slope_low']:
                            result.loc[result.index[i], 'MACDsignal'] = 'Sell'
                            
                    if result.loc[result.index[i-1], 'slope'] >= para_dict['slope_high']:
                            result.loc[result.index[i], 'MACDsignal'] = 'Buy'
                    '''
                    if result.loc[result.index[i], 'MAslope'] <= para_dict['MAslope_low'] or result.loc[result.index[i], 'MAslope'] >= para_dict['MAslope_high']:
                        
                        if result.loc[result.index[i], 'MAslope'] <= para_dict['MAslope_low'] and (result.loc[result.index[i], 'ma']) > result.loc[result.index[i], 'close']:
                            result.loc[result.index[i], 'MACDsignal'] = 'Sell'
                            result.loc[result.index[i], 'Jsignal'] = 'Sell'
                        
                        if result.loc[result.index[i], 'MAslope'] >= para_dict['MAslope_high'] and (result.loc[result.index[i], 'ma']) < result.loc[result.index[i], 'close']:
                            result.loc[result.index[i], 'MACDsignal'] = 'Buy'
                            result.loc[result.index[i], 'Jsignal'] = 'Buy'
                        
                        if result.loc[result.index[i], 'MAslope'] <= para_dict['MAslope_low'] and (result.loc[result.index[i], 'ma']) < result.loc[result.index[i], 'close'] and 
                                result.loc[result.index[i], 'slope'] > para_dict['slope_high_trend']:
                                    result.loc[result.index[i], 'MACDsignal'] = 'Buy'
                                    result.loc[result.index[i], 'Jsignal'] = 'Buy'
                                    
                        if result.loc[result.index[i], 'MAslope'] <= para_dict['MAslope_low'] and (result.loc[result.index[i], 'ma']) > result.loc[result.index[i], 'close'] and 
                                result.loc[result.index[i], 'slope'] > para_dict['slope_high_trend']:
                                    result.loc[result.index[i], 'MACDsignal'] = 'Buy'
                                    result.loc[result.index[i], 'Jsignal'] = 'Buy'
                        
                        if result.loc[result.index[i], 'MAslope'] >= para_dict['MAslope_high'] and (result.loc[result.index[i], 'ma']) < result.loc[result.index[i], 'close'] and 
                                result.loc[result.index[i], 'slope'] < para_dict['slope_low_trend']:
                                    result.loc[result.index[i], 'MACDsignal'] = 'Sell'
                                    result.loc[result.index[i], 'Jsignal'] = 'Sell'
                        
                        if result.loc[result.index[i], 'MAslope'] <= para_dict['MAslope_low'] and (result.loc[result.index[i], 'ma']) <= result.loc[result.index[i], 'close']:
                                    result.loc[result.index[i], 'MACDsignal'] = 'Buy'
                                    result.loc[result.index[i], 'Jsignal'] = 'Buy'
                            
                        if result.loc[result.index[i], 'MAslope'] >= para_dict['MAslope_high'] and (result.loc[result.index[i], 'ma']) >= result.loc[result.index[i], 'close']:
                                result.loc[result.index[i], 'MACDsignal'] = 'Sell'
                                result.loc[result.index[i], 'Jsignal'] = 'Sell'
                                
                    else:
                        
                        if result.loc[result.index[i-1], 'J'] >= para_dict['j_high'] and result.loc[result.index[i], 'J'] < result.loc[result.index[i-1], 'J'] and result.loc[result.index[i], 'J'] > para_dict['j_low']:
                            result.loc[result.index[i], 'Jsignal'] = 'Sell'
                            
                        if result.loc[result.index[i-1], 'J'] <= para_dict['j_low'] and result.loc[result.index[i], 'J'] > result.loc[result.index[i-1], 'J'] and result.loc[result.index[i], 'J'] < para_dict['j_high']:
                            result.loc[result.index[i], 'Jsignal'] = 'Buy'
                            
                        #i slop > 0 sell
                        if result.loc[result.index[i-1], 'slope'] > result.loc[result.index[i], 'slope']+0.01 and result.loc[result.index[i], 'slope'] >= 0:
                            if result.loc[result.index[i-1], 'slope'] > para_dict['slope_high'] and result.loc[result.index[i], 'slope'] <= para_dict['slope_high']:
                                result.loc[result.index[i], 'MACDsignal'] = 'Sell'
                            if result.loc[result.index[i-1], 'slope'] > para_dict['slope_high'] and result.loc[result.index[i], 'slope'] > para_dict['slope_high'] and result.loc[result.index[i-2], 'slope'] - 
                            result.loc[result.index[i], 'slope'] >= 0:
                                result.loc[result.index[i], 'MACDsignal'] = 'Sell'
                            if result.loc[result.index[i-1], 'slope'] > para_dict['slope_high'] and result.loc[result.index[i], 'slope'] > para_dict['slope_high'] and result.loc[result.index[i-2], 'slope'] - 
                            result.loc[result.index[i], 'slope'] < 0:
                                result.loc[result.index[i], 'MACDsignal'] = 'Buy'
                        #i slop <0 sell
                        if result.loc[result.index[i-1], 'slope'] > result.loc[result.index[i], 'slope']+0.01 and result.loc[result.index[i], 'slope'] < 0:
                            if result.loc[result.index[i-1], 'slope'] > para_dict['slope_low'] and result.loc[result.index[i], 'slope'] <= para_dict['slope_low']:
                                result.loc[result.index[i], 'MACDsignal'] = 'Sell'
                            if result.loc[result.index[i-1], 'slope'] < para_dict['slope_low'] and result.loc[result.index[i], 'slope'] < para_dict['slope_low'] and result.loc[result.index[i-2], 'slope'] - 
                            result.loc[result.index[i], 'slope'] >= 0:
                                result.loc[result.index[i], 'MACDsignal'] = 'Sell'
                            if result.loc[result.index[i-1], 'slope'] < para_dict['slope_low'] and result.loc[result.index[i], 'slope'] < para_dict['slope_low'] and result.loc[result.index[i-2], 'slope'] - 
                            result.loc[result.index[i], 'slope'] < 0:
                                result.loc[result.index[i], 'MACDsignal'] = 'Buy'
                                
                        #i slop <0 buy
                        if result.loc[result.index[i-1], 'slope'] < result.loc[result.index[i], 'slope']-0.01 and result.loc[result.index[i], 'slope'] < 0:
                            if result.loc[result.index[i-1], 'slope'] < para_dict['slope_low'] and result.loc[result.index[i], 'slope'] > para_dict['slope_low']:
                                result.loc[result.index[i], 'MACDsignal'] = 'Buy'
                            if result.loc[result.index[i-1], 'slope'] < para_dict['slope_low'] and result.loc[result.index[i], 'slope'] < para_dict['slope_low'] and result.loc[result.index[i-2], 'slope'] - 
                            result.loc[result.index[i], 'slope'] <= 0:
                                result.loc[result.index[i], 'MACDsignal'] = 'Buy'
                            if result.loc[result.index[i-1], 'slope'] < para_dict['slope_low'] and result.loc[result.index[i], 'slope'] < para_dict['slope_low'] and result.loc[result.index[i-2], 'slope'] - 
                            result.loc[result.index[i], 'slope'] > 0:
                                result.loc[result.index[i], 'MACDsignal'] = 'Sell'
                        
                        #i slop >0 buy wrong not check
                        if result.loc[result.index[i-1], 'slope'] < result.loc[result.index[i], 'slope']-0.01 and result.loc[result.index[i], 'slope'] > 0:
                            if result.loc[result.index[i-1], 'slope'] < para_dict['slope_low'] and result.loc[result.index[i], 'slope'] > para_dict['slope_low']:
                                result.loc[result.index[i], 'MACDsignal'] = 'Buy'
                            if result.loc[result.index[i-1], 'slope'] < para_dict['slope_low'] and result.loc[result.index[i], 'slope'] < para_dict['slope_low'] and result.loc[result.index[i-2], 'slope'] - 
                            result.loc[result.index[i], 'slope'] <= 0:
                                result.loc[result.index[i], 'MACDsignal'] = 'Buy'
                            if result.loc[result.index[i-1], 'slope'] < para_dict['slope_low'] and result.loc[result.index[i], 'slope'] < para_dict['slope_low'] and result.loc[result.index[i-2], 'slope'] - 
                            result.loc[result.index[i], 'slope'] > 0:
                                result.loc[result.index[i], 'MACDsignal'] = 'Sell'
                        
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
                                
        return result
    
    def statistic(result, ticker, para_dict):
        
        #calc cum. pnl
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
    
    df ce_inventory():
        
        
                            
        
    
    
            