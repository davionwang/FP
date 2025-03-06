# -*- coding: utf-8 -*-
"""
Created on Wed Nov 10 11:10:53 2021

@author: SVC-GASQuant2-Prod
"""

#JK stocks

import time
import sys
import numpy as np
import pandas as pd
import plotly.offline as py
import plotly.graph_objs as go
from plotly.subplots import make_subplots
import datetime
import calendar
import plotly.express as px

#from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import requests
sys.path.append('C:\\Users\SVC-GASQuant2-Prod\YWwebsite\Python Scripts\class') 
pd.set_option('display.max_columns',20)
from DBtoDF import DBTOPD

class JKstocks():
    
    def get_data():
        
        jodi =DBTOPD('ENTFUND-DB','fpEntFundamental', 'gas', 'JODI_WORLD')
        dfjodi = jodi.get_jodi_data()
        #[fpEntFundamental].gas.JODI_WORLDENTFUND-DB
        
        dfjodi_jp = dfjodi.loc[dfjodi[dfjodi['REF_AREA']=='JP'].index, ['TIME_PERIOD','OBS_VALUE']]
        dfjodi_jp['TIME_PERIOD']=pd.to_datetime(dfjodi_jp['TIME_PERIOD'])
        dfjodi_jp.sort_values(by='TIME_PERIOD',inplace=True)
        dfjodi_jp.set_index('TIME_PERIOD', inplace=True)
        #print(dfjodi_jp)
        #print(dfjodi_jp.loc['2015-01-01':'2015-12-01'])
    
        dfjodi_kr = dfjodi.loc[dfjodi[dfjodi['REF_AREA']=='KR'].index, ['TIME_PERIOD','OBS_VALUE']]
        dfjodi_kr['TIME_PERIOD']=pd.to_datetime(dfjodi_kr['TIME_PERIOD'])
        dfjodi_kr.sort_values(by='TIME_PERIOD',inplace=True)
        dfjodi_kr.set_index('TIME_PERIOD', inplace=True)

        #print(dfjodi_kr)
        #Japan Meti data
        meti = DBTOPD('PRD-DB-SQL-209','AnalyticsModel','ts','AnalyticsLatest' )
        dfmeti = meti.get_meti_data()
        
        dfmeti = dfmeti[['CurveId', 'ValueDate', 'Value']] #mt
        #print(dfmeti)
        
        dfmetilast = dfmeti.loc[dfmeti[dfmeti['CurveId']==59328].index,['ValueDate','Value']]
        dfmetilast.set_index('ValueDate', inplace=True)
        dfmetiave = dfmeti.loc[dfmeti[dfmeti['CurveId']==59329].index, ['ValueDate','Value']]
        dfmetiave.set_index('ValueDate', inplace=True)
        #print(dfmetilast)
        dfmetiave.replace(to_replace=0, method='bfill', inplace=True)
        #print(dfmetiave)
        dfmetiave_full = pd.DataFrame(index = pd.date_range(start = dfmetiave.index[0], end = dfmetiave.index[-1]))
        for i in dfmetiave.index:
            dfmetiave_full.loc[i,'Value'] = dfmetiave.loc[i,'Value']
        dfmetiave_full.fillna(method = 'bfill', inplace=True)
        #print(dfmetiave_full)
        demetifull = pd.DataFrame(index = pd.date_range(start = dfmetiave.index[0], end = datetime.date.today()), columns=['Meti Latest','Average Of Last 4 Years'])
        demetifull['Meti Latest'] = dfmetilast
        demetifull['Average Of Last 4 Years'] = dfmetiave_full
        demetifull.fillna(method = 'ffill', inplace=True)
        #demetifull=pd.concat([dfmetilast,dfmetiave], axis=1)
        #demetifull.columns=['Meti Latest','Average Of Last 4 Years']
        demetifull=round(demetifull,2) #Mt 
        demetifull.to_csv('C:/Users/SVC-GASQuant2-Prod/YWwebsite/data/meti fcst.csv')
        
        
        print(demetifull)
        
        return dfjodi_jp, dfjodi_kr, demetifull
    
    def copy_data(dfjodi_jp, dfjodi_kr, demetifull):
        today=datetime.date.today()
        
        dfjp=pd.DataFrame(np.nan,index=pd.date_range(start=str(today.year)+'-01-01', freq='MS',periods=12), columns=['Japan METI 2021'])
        #dfjp['Max']=[6.243193,6.211062,6.609207,6.620383,7.119112,6.846697,6.951472,6.185916,6.156579,6.891401,7.187565,6.90118]
        #dfjp['Min']=[4.073652,4.07924,4.108577,5.18287,5.567045,4.878324,4.67995,4.734433,4.80568,5.092065,5.456682,4.646422]
        dfjp['2020 Act'] = [6.243193,6.211062,6.609207,6.620383,7.119112,6.846697,6.951472,5.446903,5.773801,6.051804,5.456682,4.663186]
        dfjp['2020F'] = [5.64,5.56,6.02,6.24,6.54,6.28,6.12,6.14,6.34,6.71,6.95,6.44]
        dfjp['2021F'] = [6.17,5.88,6.33,6.51,6.70,6.59,6.35,6.58,6.89,7.15,7.30,7.06]
        dfjp['2022F'] = [5.56,4.56,5.01,5.19,5.69,6.29,6.79,7.29,7.29,7.64,7.72,7.15]
        dfjp['2023F'] = [7.04,6.97,7.42,7.60,7.94,7.68,7.57,7.29,7.51,7.87,7.94,7.37]#check 
       

        dfjp['2024F'] = [6.45, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan]#check 

        dfjp['METI 2021'] = [4.41,	5.70,	6.19,	5.94,	5.87,	6.35,	6.34, 7.01, 7.70, 6.98, 7.12, 	6.98] 	
        dfjp['METI 2022'] = [5.46,	4.95,	5.25,	5.97,	7.01,	7.36,	7.38,	8.24,	7.96,	8.01,	8.49,	7.74]
        dfjp['METI 2023'] = [7.34, 7.51, 7.64, 7.58, 7.75, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan] #input 12
        dfjp['City Gas Stocks 2015'] = [1773,	1536,	1853,	1936,	2060,	1959,	1737,	1754,	1648,	1773,	1928,	2178]
        dfjp['City Gas Stocks 2016'] = [1864,	1636,	1858,	1922,	1941,	1663,	1398,	1487,	1771,	1904,	2031,	1790]
        dfjp['City Gas Stocks 2017'] = [1826,	1584,	1361,	1560,	1706,	1707,	1995,	1932,	1821,	2049,	1728,	1555]
        dfjp['City Gas Stocks 2018'] = [1167,	1137,	1386,	1557,	1955,	2252,	1806,	1806,	2100,	2347,	2441,	2274]
        dfjp['City Gas Stocks 2019'] = [2100,	2067,	2180,	2062,	2254,	2056,	2104,	2003,	2411,	2319,	2598,	2297]
        dfjp['City Gas Stocks 2020'] = [2046,	1890,	2300,	2227,	2313,	2175,	2120,	1922,	1966,	1870,	1714,	1489]
        dfjp['City Gas Stocks 2021'] = [1255,	1519,	1424,	1635,	1705,	1884,	1609.281,	1971.14,	2504.531, 2453, 2420, 2236]
        dfjp['City Gas Stocks 2022'] = [1779,	1485,	1568,   1954,   2356, 2479, 2511, 2538, 2563, 2684, 2815, 2701] 
        dfjp['City Gas Stocks 2023'] = [2335,    2491, 2649, 2584, 2707, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan]    #input 21
        dfjp['City Gas Stocks min'] = dfjp[['City Gas Stocks 2015','City Gas Stocks 2016','City Gas Stocks 2017','City Gas Stocks 2018','City Gas Stocks 2019','City Gas Stocks 2020', 'City Gas Stocks 2021']].min(axis=1)
        dfjp['City Gas Stocks max'] = dfjp[['City Gas Stocks 2015','City Gas Stocks 2016','City Gas Stocks 2017','City Gas Stocks 2018','City Gas Stocks 2019','City Gas Stocks 2020', 'City Gas Stocks 2021']].max(axis=1)
        dfjp['Power Gas Stocks 2015'] = [2137,	2062,	2304,	2462,	2127,	2289,	2088,	2051,	2687,	2427,	2136,	2497]
        dfjp['Power Gas Stocks 2016'] = [1870,	1881,	2151,	2404,	2044,	1829,	1952,	1902,	1669,	1741,	1902,	1967]
        dfjp['Power Gas Stocks 2017'] = [1613,	1664,	1580,	2150,	2409,	2618,	1966,	2496,	2348,	2040,	2221,	1771]
        dfjp['Power Gas Stocks 2018'] = [1749,	1783,	2326,	2214,	2314,	1919,	1777,	1958,	2307,	2586,	2556,	2666]
        dfjp['Power Gas Stocks 2019'] = [2069,	2271,	2501,	2107,	2231,	2049,	2638,	2117,	1847,	2353,	2547,	2238]
        dfjp['Power Gas Stocks 2020'] = [2423,	2556,	2431,	2512,	2783,	2726,	2856,	1977,	2167,	2462,	2192,	1849]
        dfjp['Power Gas Stocks 2021'] = [1901,	2562,	3005,	2616,	2494,	2661,	2929.105533,	3045.44018, 3007, 2544,	2675, 2575]	
        dfjp['Power Gas Stocks 2022'] = [2130,	2058,	2186,   2320,	2660,	2791,   2770,	3361,	3135,	3048,	3262,	2842]
        dfjp['Power Gas Stocks 2023'] = [2919,	2882,   2818,   np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan]#input 22
        dfjp['Power Gas Stocks min'] = dfjp[['Power Gas Stocks 2015','Power Gas Stocks 2016','Power Gas Stocks 2017','Power Gas Stocks 2018','Power Gas Stocks 2019','Power Gas Stocks 2020','Power Gas Stocks 2021','Power Gas Stocks 2022']].min(axis=1)
        dfjp['Power Gas Stocks max'] = dfjp[['Power Gas Stocks 2015','Power Gas Stocks 2016','Power Gas Stocks 2017','Power Gas Stocks 2018','Power Gas Stocks 2019','Power Gas Stocks 2020','Power Gas Stocks 2021','Power Gas Stocks 2022']].max(axis=1)
        #print(dfjp)
        dfkr=pd.DataFrame(np.nan,index=pd.date_range(start=str(today.year)+'-01-01', freq='MS',periods=12), columns=['Korea METI 2021'])
        dfkr['2020F']=[5.18,4.16,5.08,4.68,4.68,5.28,5.88,6.10,5.10,5.66,5.55,4.86]
        dfkr['2021F']=[3.22, 2.56, 2.86, 2.68, 3.04, 3.54, 3.68, 4.28, 5.03, 5.62, 5.52, 4.77]#
        dfkr['2022F']=[3.99,2.49,2.09,2.02,2.52,2.10,3.10,4.10,5.50,7.0,6.5,5.6] 
        dfkr['2023F']=[6.10,6.20,6.6,6.55,6.45,6.45,5.85,6.50,7.17,7.87,7.80,6.90]#check update 

        dfkr['2015'] = [4.06,	3.72,	3.14,	3.24,	3.67,	3.29,	3.19,	3.23,	3.94,	4.78,	4.93,	4.24]
        dfkr['2016'] = [3.08,	2.06,	2.54,	2.34,	2.75,	3.01,	2.39,	1.9,	2.02,	2.98,	2.78,	2.37]
        dfkr['2017'] = [2.44,	2.06,	2.17,	1.83,	2.41,	3.54,	4.09,	4.39,	4.9,	5.49,	4.87,	3.22]
        dfkr['2018'] = [1.61,	1.48,	2.48,	2.3,	2.43,	3.43,	3.72,	3.32,	5.37,	5.88,	6.03,	5.98]
        dfkr['2019'] = [4.7,	3.6,	2.88,	2.88,	3.21,	4.4,	4.74,	5.57,	5.9,	6.17,	6.44,	5.55]
        dfkr['2020 Act']=[4.8	,4.69,	4.77,	5.12,	6.22,	6.16,	5.87,	4.66,	4.63,	5.6,	5.44,	3.91]
        dfkr['2021 Act']=[2,	3.34	,3.06,	2.63,	2.84,	2.87,	3.83,   4.44,   5.43,  6.21,    5.79,   4.47]		
        dfkr['2022 Act']=[3.83, 2.06, 2.44, 2.73, 2.97, 2.59, 2.60, 3.94, 5.55, 7.28, 7.78, 6.1]     
        dfkr['2023 Act']=[5.52, 6.27, 7.25, 7.17, 7.36, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan]  #input kesis bcm 6     
        dfkr['Max']=dfkr[['2015','2016','2017','2018','2019','2020 Act','2021 Act','2022 Act']].max(axis=1)
        dfkr['Min']=dfkr[['2015','2016','2017','2018','2019','2020 Act','2021 Act','2022 Act']].min(axis=1)
        #dfkr['City Gas Stocks 2020'] = [1810,	1773,	1536,	1853,	1936,	2060,	1959,	1737,	1754,	1648,	1773,	1928]
        #dfkr['City Gas Stocks 2021'] = [2178,	1864,	1636,	1858,	1922,	1941,	1663,	1398,	1487,	1771, np.nan, np.nan]
        #dfkr['Power Gas Stocks 2020'] = [2280,	2137,	2062,	2304,	2462,	2127,	2289,	2088,	2051,	2687,	2427,	2136]
        #dfkr['Power Gas Stocks 2021'] = [2497,	1870,	1881,	2151,	2404,	2044,	1829,	1952,	1902,	1669, np.nan, np.nan]

        dfjapanstocks=pd.DataFrame(np.nan,index=pd.date_range(start='2018-01-01', end='2023-12-01', freq='MS'), columns=['METI'])
        #change iloc num
        '''
        METI_list = [6.181,	6.195,	5.935,	4.265,	4.843,	4.229,	5.149,	5.7,	4.76,	4.954,	4.806,	5.445, 5.64,	5.483,	5.441,	4.253,	4.222,	3.957,	5.079,	4.617,	4.87,	4.773,	4.762,	5.161,
                     6.243193,6.211062,6.609207,6.620383,7.119112,6.846697,6.951472,5.446903,5.773801,6.051804,5.456682,4.663186,
                     4.41,	5.70,	6.19,	5.94,	5.87,	6.35,	6.34, 7.01, 7.70, 6.98, 7.12, 6.51,
                     4.82,	4.59,	4.51,	5.48,	6.07, 6.36,6.69, 7.18, 7.32] #input dfjp['METI 2022'] 
        '''
        METI_list = [4.07,	4.08,	5.19,	5.27,	5.96,	5.83,	5.01,	5.26,	6.16,	6.89,	6.98,	6.90,
                     5.82,	6.06,	6.54,	5.82,	6.27,	5.73,	6.62,	5.76,	5.95,	6.53,	7.19,	6.34,
                     6.243193,6.211062,6.609207,6.620383,7.119112,6.846697,6.951472,5.446903,5.773801,6.051804,5.456682,4.663186,
                     4.41,	5.70,	6.19,	5.94,	5.87,	6.35,	6.34, 7.01, 7.70, 6.98, 7.12, 6.98,
                     5.46,	4.95,	5.25,	5.97,	7.01,	7.36,	7.38,	8.24,	7.96,	8.01,	8.49,	7.74,
                     7.34, 7.51, 7.64, 7.58,7.75] #input dfjp['METI 2023'] 99
        #print(len(METI_list))
        dfjapanstocks['METI'].iloc[0:len(METI_list)] = METI_list
        dfjapanstocks['JODI'] = dfjodi_jp.loc['2018-01-01': '2023-12-01']/1000
        dfjapanstocks['JODI vs METI'] = (dfjapanstocks['JODI'] - dfjapanstocks['METI'])
        #print(dfjapanstocks)
        
        dfkoreastocks=pd.DataFrame(np.nan,index=pd.date_range(start='2018-01-01', end='2023-12-01', freq='MS'), columns=['KEEI Monthly energy statistics'])
        dfkoreastocks['KEEI Monthly energy statistics'].iloc[0:27] = [1.60655,1.482217,	2.418207,2.268728,	2.352548,3.358388,	3.643376,3.24104,5.290439,5.850636,5.944235,5.952617,4.700905,3.601466,2.882011,2.876423,3.214497,4.401947,4.744212,5.569839,	5.899531,6.17474,6.444361,5.516753,4.86156,	4.923028,4.769358]#check

        dfkoreastocks['KESIS']=np.nan
        KESIS_list = [1.61,	1.48,	2.48,	2.3,	2.43,	3.43,	3.72,	3.32,	5.37,	5.88,	6.03,	5.98,4.7,	3.6,	2.88,	2.88,	3.21,	4.4,	4.74,	5.57,	5.9,	6.17,	6.44,	5.55,4.8	,4.69,	4.77,	5.12,	6.22,	6.16,	5.87,	4.66,	4.63,	5.6,	5.44,	3.91,2,	3.34	,3.06,	2.63,	2.84,	2.87,	3.83,	4.44,5.43,6.21,5.79,4.47, 
                      3.83, 2.06, 2.44, 2.73, 2.97,2.59, 2.60, 3.94,5.55, 7.28, 7.78, 6.1, 5.52, 6.27,7.25,7.17,7.36] #input  dfkr['2022 Act'] 137
        dfkoreastocks['KESIS'].iloc[0:len(KESIS_list)] = KESIS_list
        dfkoreastocks['JODI'] = dfjodi_kr.loc['2018-01-01': '2023-12-01']/1000
        dfkoreastocks['JODI vs KESIS'] = (dfkoreastocks['JODI']-dfkoreastocks['KESIS'])
        #print(dfkoreastocks)
        dfjpc = dfjp.copy()
        dfkrc = dfkr.copy()
        return dfjpc, dfkrc, dfjapanstocks, dfkoreastocks

    
    def chart_data(dfjodi_jp, dfjodi_kr):
        
        today=datetime.date.today()
        '''
        dfjp=pd.DataFrame(np.nan,index=pd.date_range(start=str(today.year)+'-01-01', freq='MS',periods=12), columns=['Japan JODI 2021'])
        dfjp['Japan JODI 2020'] =  dfjodi_jp.loc['2020-01-01':'2020-12-01'].values
        dfjp['Japan JODI 2019'] =  dfjodi_jp.loc['2019-01-01':'2019-12-01'].values
        dfjp['Japan JODI 2018'] =  dfjodi_jp.loc['2018-01-01':'2018-12-01'].values
        dfjp['Japan JODI 2017'] =  dfjodi_jp.loc['2017-01-01':'2017-12-01'].values
        dfjp['Japan JODI 2016'] =  dfjodi_jp.loc['2016-01-01':'2016-12-01'].values 
        dfjp['Japan JODI 2015'] =  dfjodi_jp.loc['2015-01-01':'2015-12-01'].values
        dfjp.loc['2021-01-01':dfjodi_jp.index[-1],['Japan JODI 2021']] = dfjodi_jp.loc['2021-01-01':dfjodi_jp.index[-1],['OBS_VALUE']].values
        #print(dfjp)
    
        dfkr=pd.DataFrame(np.nan,index=pd.date_range(start=str(today.year)+'-01-01', freq='MS',periods=12), columns=['Korea JODI 2021'])
        dfkr['Japan JODI 2020'] =  dfjodi_kr.loc['2020-01-01':'2020-12-01'].values
        dfkr['Japan JODI 2019'] =  dfjodi_kr.loc['2019-01-01':'2019-12-01'].values
        dfkr['Japan JODI 2018'] =  dfjodi_kr.loc['2018-01-01':'2018-12-01'].values
        dfkr['Japan JODI 2017'] =  dfjodi_kr.loc['2017-01-01':'2017-12-01'].values
        dfkr['Japan JODI 2016'] =  dfjodi_kr.loc['2016-01-01':'2016-12-01'].values 
        dfkr['Japan JODI 2015'] =  dfjodi_kr.loc['2015-01-01':'2015-12-01'].values
        dfkr.loc['2021-01-01':dfjodi_kr.index[-1],['Japan JODI 2021']] = dfjodi_kr.loc['2021-01-01':dfjodi_kr.index[-1],['OBS_VALUE']].values
        '''
        dfjp=pd.DataFrame(np.nan,index=pd.date_range(start=str(today.year)+'-01-01', freq='MS',periods=12), columns=['Japan METI 2021'])
        dfjp['2020'] = [6.243193,6.211062,6.609207,6.620383,7.119112,6.846697,6.951472,5.446903,5.773801,6.051804,5.456682,4.663186]
        dfjp['2015'] = [np.nan, np.nan,5.81,6.14,5.85,	5.93,	5.34,	5.32,	6.06,	5.87,	5.68,	6.53]
        dfjp['2016'] = [5.22,	4.91,	5.60,	6.04,	5.57,	4.88,	4.68,	4.73,	4.81,	5.09,	5.49,	5.25]
        dfjp['2017'] = [4.80,	4.54,	4.11,	5.18,	5.75,	6.04,	5.53,	6.19,	5.82,	5.71,	5.52,	4.65]
        dfjp['2018'] = [4.07,	4.08,	5.19,	5.27,	5.96,	5.83,	5.01,	5.26,	6.16,	6.89,	6.98,	6.90]
        dfjp['2019'] = [5.82,	6.06,	6.54,	5.82,	6.27,	5.73,	6.62,	5.76,	5.95,	6.53,	7.19,	6.34]
        dfjp['2021'] = [4.41,	5.70,	6.19,	5.94,	5.87,	6.35,	6.34, 7.01, 7.70, 6.98,  7.12,  6.98] 
        dfjp['2022'] = [5.46,	4.95,	5.25,	5.97,	7.01,	7.36,	7.38,	8.24,	7.96,	8.01,	8.49,	7.74]
        dfjp['2023 Act'] = [7.34, 7.51, 7.64, 7.58, 7.75, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan] #input#input 99

        #print(dfjp)
        dfkr=pd.DataFrame(np.nan,index=pd.date_range(start=str(today.year)+'-01-01', freq='MS',periods=12), columns=['Korea METI 2021'])
        dfkr['2015'] = [4.06,	3.72,	3.14,	3.24,	3.67,	3.29,	3.19,	3.23,	3.94,	4.78,	4.93,	4.24]
        dfkr['2016'] = [3.08,	2.06,	2.54,	2.34,	2.75,	3.01,	2.39,	1.9,	2.02,	2.98,	2.78,	2.37]
        dfkr['2017'] = [2.44,	2.06,	2.17,	1.83,	2.41,	3.54,	4.09,	4.39,	4.9,	5.49,	4.87,	3.22]
        dfkr['2018'] = [1.61,	1.48,	2.48,	2.3,	2.43,	3.43,	3.72,	3.32,	5.37,	5.88,	6.03,	5.98]
        dfkr['2019'] = [4.7,	3.6,	2.88,	2.88,	3.21,	4.4,	4.74,	5.57,	5.9,	6.17,	6.44,	5.55]
        dfkr['2020']=[4.8	,4.69,	4.77,	5.12,	6.22,	6.16,	5.87,	4.66,	4.63,	5.6,	5.44,	3.91] 
        dfkr['2021']=[2,	3.34	,3.06,	2.63,	2.84,	2.87,	3.83,	4.44, 5.43, 6.21, 5.79,4.47]		
        dfkr['2022']=[3.83,  2.06, 2.44, 2.73, 2.97, 2.59, 2.60, 3.94, 5.55, 7.28, 7.78, 6.1]        
        dfkr['2023 Act']=[5.52,  6.27, 7.25, 7.17, 7.36, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan] #input  137    
        #dfkr['City Gas Stocks 2020'] = [1810,	1773,	1536,	1853,	1936,	2060,	1959,	1737,	1754,	1648,	1773,	1928]
        #dfkr['City Gas Stocks 2021'] = [2178,	1864,	1636,	1858,	1922,	1941,	1663,	1398,	1487,	1771, np.nan, np.nan]
        #dfkr['Power Gas Stocks 2020'] = [2280,	2137,	2062,	2304,	2462,	2127,	2289,	2088,	2051,	2687,	2427,	2136]
        #dfkr['Power Gas Stocks 2021'] = [2497,	1870,	1881,	2151,	2404,	2044,	1829,	1952,	1902,	1669, np.nan, np.nan]
        
        return dfjp,dfkr
    
    
    def plot_chart( dfjp,dfkr,dfjpc, dfkrc, dfjapanstocks, dfkoreastocks, demetifull):
        
        today=datetime.date.today()
        
        fig1=go.Figure()
        fig1.add_trace(go.Scatter(x=dfjp.index, y=dfjp['2023 Act'],
                            mode='lines',
                            name='2023 Act',
                            line=dict(color='red', dash='solid')
                            ))
        fig1.add_trace(go.Scatter(x=dfjp.index, y=dfjp['2022'],
                            mode='lines',
                            name='2022',
                            line=dict(color='#D9D9D9', dash='solid')
                            ))
        fig1.add_trace(go.Scatter(x=dfjp.index, y=dfjp['2021'],
                            mode='lines',
                            name='2021',
                            line=dict(color='#00B0F0', dash='dash')
                            ))
        fig1.add_trace(go.Scatter(x=dfjp.index, y=dfjp['2020'],
                            mode='lines',
                            name='2020',
                            line=dict(color='#2E75B6', dash='solid')
                            ))
        fig1.add_trace(go.Scatter(x=dfjp.index, y=dfjp['2019'],
                            mode='lines',
                            name='2019',
                            line=dict(color='#BDD7EE', dash='solid')
                            ))
        fig1.add_trace(go.Scatter(x=dfjp.index, y=dfjp['2018'],
                            mode='lines',
                            name='2018',
                            line=dict(color='#7F7F7F', dash='solid')
                            ))
        fig1.add_trace(go.Scatter(x=dfjp.index, y=dfjp['2017'],
                            mode='lines',
                            name='2017',
                            line=dict(color='#BFBFBF', dash='solid')
                            ))
        #fig1.add_trace(go.Scatter(x=dfjp.index, y=dfjp['2015'],
         #                   mode='lines',
          #                  name='2015',
           #                 line=dict(color='#D9D9D9', dash='solid')
            #                ))
        fig1.update_layout(
             autosize=True,
             showlegend=True,
             #colorscale='RdBu',
             legend=dict(x=0, y=-0.2),
             legend_orientation="h",
             title_text='Japan Storage Inventory '+str(today)+' ,METI ,Bcm',
             xaxis = dict(dtick="M1"),
             xaxis_tickformat = '%B',
             hovermode='x unified',
             plot_bgcolor = 'white',
             template='ggplot2'  
         )
        fig1.update_yaxes(rangemode="tozero")
        
        fig2=go.Figure()
        fig2.add_trace(go.Scatter(x=dfkr.index, y=dfkr['2023 Act'],
                            mode='lines',
                            name='2023 Act',
                            line=dict(color='red', dash='solid')
                            ))
        fig2.add_trace(go.Scatter(x=dfkr.index, y=dfkr['2022'],
                            mode='lines',
                            name='2022',
                            line=dict(color='#755d19', dash='solid')
                            ))
        fig2.add_trace(go.Scatter(x=dfkr.index, y=dfkr['2021'],
                            mode='lines',
                            name='2021',
                            line=dict(color='#D9D9D9', dash='dash')
                            ))
        fig2.add_trace(go.Scatter(x=dfkr.index, y=dfkr['2020'],
                            mode='lines',
                            name='2020',
                            line=dict(color='#00B0F0', dash='dash')
                            ))
        fig2.add_trace(go.Scatter(x=dfkr.index, y=dfkr['2019'],
                            mode='lines',
                            name='2019',
                            line=dict(color='#2E75B6', dash='solid')
                            ))
        fig2.add_trace(go.Scatter(x=dfkr.index, y=dfkr['2018'],
                            mode='lines',
                            name='2018',
                            line=dict(color='#BDD7EE', dash='solid')
                            ))
        fig2.add_trace(go.Scatter(x=dfkr.index, y=dfkr['2017'],
                            mode='lines',
                            name='2017',
                            line=dict(color='#7F7F7F', dash='solid')
                            ))
        fig2.add_trace(go.Scatter(x=dfkr.index, y=dfkr['2016'],
                            mode='lines',
                            name='2016',
                            line=dict(color='#BFBFBF', dash='solid')
                            ))
        #fig2.add_trace(go.Scatter(x=dfkr.index, y=dfkr['2015'],
         #                   mode='lines',
          #                  name='2015',
           #                 line=dict(color='#D9D9D9', dash='solid')
            #                ))
        fig2.update_layout(
             autosize=True,
             showlegend=True,
             #colorscale='RdBu',
             legend=dict(x=0, y=-0.2),
             legend_orientation="h",
             title_text='Korea Storage Inventory '+str(today)+' ,KESIS ,Bcm',
             xaxis = dict(dtick="M1"),
             xaxis_tickformat = '%B',
             hovermode='x unified',
             plot_bgcolor = 'white',
             template='ggplot2'  
         )
        fig2.update_yaxes(rangemode="tozero")
        
        #print(dfjpc)
        fig3=go.Figure()
        fig3.add_trace(go.Scatter( x=dfjpc.index,y=dfjp[['2015','2016','2017','2018','2019','2020','2021','2022']].min(axis=1),
                   fill='tonexty',
                   fillcolor='rgba(65,105,225,0)',
                   line_color='rgba(65,105,225,0)',
                   showlegend=False,
                   name='2015-2022 range Min'
                   ))
           
        fig3.add_trace(go.Scatter(x=dfjpc.index,y=dfjp[['2015','2016','2017','2018','2019','2020','2021','2022']].max(axis=1),
                fill='tonexty',
                fillcolor='rgba(191,191,191,0.7)',
                line_color='rgba(65,105,225,0)',
                showlegend=True,
                name='2015-2022 range'
                ))
        fig3.add_trace(go.Scatter(x=dfjpc.index, y=dfjpc['2021F'],
                            mode='lines',
                            name='2021F',
                            visible='legendonly',
                            line=dict(color='blue', dash='dot')
                            ))
        fig3.add_trace(go.Scatter(x=dfjpc.index, y=dfjpc['METI 2021'],
                            mode='lines',
                            name='2021 Act',
                            visible='legendonly',
                            line=dict(color='red', dash='solid')
                            ))
        fig3.add_trace(go.Scatter(x=dfjpc.index, y=dfjpc['2022F'],
                            mode='lines',
                            name='2022F',
                            visible='legendonly',
                            line=dict(color='#7030A0', dash='dash')
                            ))
        fig3.add_trace(go.Scatter(x=dfjpc.index, y=dfjpc['METI 2022'],
                            mode='lines+markers',
                            name='METI 2022',
                            line=dict(color='#636363', dash='solid')
                            ))
        '''
        fig3.add_trace(go.Scatter(x=dfjpc.index, y=dfjpc['2022F'],
                            mode='lines',
                            name='2022F',
                            line=dict(color='#997300', dash='dot')
                            ))
        fig3.add_trace(go.Scatter(x=dfjpc.index, y=dfjpc['METI 2022'],
                            mode='lines+markers',
                            name='METI 2022',
                            line=dict(color='#264478', dash='solid')
                            ))
        '''
        fig3.add_trace(go.Scatter(x=dfjpc.index, y=dfjpc['2023F'],
                            mode='lines',
                            name='2023F',
                            line=dict(color='#078cb8', dash='dot')
                            ))
        fig3.add_trace(go.Scatter(x=dfjpc.index, y=dfjpc['METI 2023'],
                            mode='lines+markers',
                            name='METI 2023',
                            line=dict(color='#264478', dash='solid')
                            ))
        
       
        fig3.update_layout(
             autosize=True,
             showlegend=True,
             #colorscale='RdBu',
             legend=dict(x=0, y=-0.2),
             legend_orientation="h",
             title_text='Japan Storage Inventory '+str(today)+' ,METI, Bcm',
             xaxis = dict(dtick="M1"),
             xaxis_tickformat = '%B',
             hovermode='x unified',
             plot_bgcolor = 'white',
             template='ggplot2'  
         )
        fig3.update_yaxes(rangemode="tozero")
        
        fig4=go.Figure()
        fig4.add_trace(
               go.Scatter(
                   x=dfkrc.index,
                   y=dfkrc['Min'],
                   fill='tonexty',
                   fillcolor='rgba(65,105,225,0)',
                   line_color='rgba(65,105,225,0)',
                   showlegend=False,
                   name='2015-2020 range Min'
                   )
               )           
        fig4.add_trace(
            go.Scatter(
                x=dfkrc.index,
                y=dfkrc['Max'],
                fill='tonexty',
                fillcolor='rgba(191,191,191,0.7)',
                line_color='rgba(65,105,225,0)',
                showlegend=True,
                name='2015-2020 range'
                )
            )
        fig4.add_trace(go.Scatter(x=dfkrc.index, y=dfkrc['2020F'],
                            mode='lines',
                            name='2020F',
                            visible='legendonly',
                            line=dict(color='blue', dash='dot')
                            ))
        fig4.add_trace(go.Scatter(x=dfkrc.index, y=dfkrc['2020 Act'],
                            mode='lines',
                            name='2020 Act',
                            visible='legendonly',
                            line=dict(color='red', dash='solid')
                            ))
        fig4.add_trace(go.Scatter(x=dfkrc.index, y=dfkrc['2021F'],
                            mode='lines',
                            name='2021F',
                            visible='legendonly',
                            line=dict(color='#7030A0', dash='dash')
                            ))
        fig4.add_trace(go.Scatter(x=dfkrc.index, y=dfkrc['2021 Act'],
                            mode='lines+markers',
                            name='2021 Act',
                            line=dict(color='#636363', dash='solid')
                            ))
        fig4.add_trace(go.Scatter(x=dfkrc.index, y=dfkrc['2022F'],
                            mode='lines',
                            name='2022F',
                            line=dict(color='#997300', dash='dot')
                            ))
        fig4.add_trace(go.Scatter(x=dfkrc.index, y=dfkrc['2022 Act'],
                            mode='lines+markers',
                            name='2022 Act',
                            line=dict(color='#264478', dash='solid')
                            ))
        fig4.add_trace(go.Scatter(x=dfkrc.index, y=dfkrc['2023F'],
                            mode='lines',
                            name='2023F',
                            line=dict(color='#078cb8', dash='dot')
                            ))
        fig4.add_trace(go.Scatter(x=dfkrc.index, y=dfkrc['2023 Act'],
                            mode='lines+markers',
                            name='2023 Act',
                            line=dict(color='#264478', dash='solid')
                            ))
        
        fig4.update_layout(
             autosize=True,
             showlegend=True,
             #colorscale='RdBu',
             legend=dict(x=0, y=-0.2),
             legend_orientation="h",
             title_text='Korea Storage Inventory '+str(today)+' ,KESIS, Bcm',
             xaxis = dict(dtick="M1"),
             xaxis_tickformat = '%B',
             hovermode='x unified',
             plot_bgcolor = 'white',
             template='ggplot2'  
         )
        fig4.update_yaxes(rangemode="tozero")
        
        fig5 = go.Figure()#make_subplots(specs=[[{"secondary_y": True}]])
        fig5.add_trace(go.Bar(x=dfjapanstocks.index, y=dfjapanstocks['JODI vs METI'],
                            name='JODI vs METI',
                            marker_color='#FFC000',
                            ))#,secondary_y=False)
        fig5.add_trace(go.Scatter(x=dfjapanstocks.index, y=dfjapanstocks['METI'],
                            mode='lines',
                            name='METI',
                            line=dict(color='#5B9BD5', dash='solid'),
                            ))#,secondary_y=False)
        fig5.add_trace(go.Scatter(x=dfjapanstocks.index, y=dfjapanstocks['JODI'],
                            mode='lines',
                            name='JODI',
                            line=dict(color='#A5A5A5', dash='solid'),
                            ))#,secondary_y=False)
        
        fig5.update_layout(
             autosize=True,
             showlegend=True,
             #colorscale='RdBu',
             legend=dict(x=0, y=-0.2),
             legend_orientation="h",
             title_text='Japan Stocks '+str(today),
             xaxis = dict(dtick="M2"),
             #xaxis_tickformat = '%B',
             hovermode='x unified',
             plot_bgcolor = 'white',
             template='ggplot2'  
         )
        #fig5.update_layout(dict(yaxis2={'anchor': 'x', 'overlaying': 'y', 'side': 'left'},
         #         yaxis={'anchor': 'x', 'domain': [0.0, 1.0], 'side':'right'}))
        fig5.update_yaxes(title_text="Stocks Bcm")#, secondary_y=True)
        #fig5.update_yaxes(title_text="Delta Mt", secondary_y=False)
        #fig5.update_yaxes(rangemode="tozero", secondary_y=False)
        
        fig6 = go.Figure()#make_subplots(specs=[[{"secondary_y": True}]])
        
        fig6.add_trace(go.Scatter(x=dfkoreastocks.index, y=dfkoreastocks['KEEI Monthly energy statistics'],
                            mode='lines',
                            name='KEEI Monthly energy statistics',
                            line=dict(color='#ED7D31', dash='solid'),
                            ))#,secondary_y=False)
        fig6.add_trace(go.Scatter(x=dfkoreastocks.index, y=dfkoreastocks['JODI'],
                            mode='lines',
                            name='JODI',
                            line=dict(color='#A5A5A5', dash='solid'),
                            ))#,secondary_y=False)
        
        fig6.add_trace(go.Scatter(x=dfkoreastocks.index, y=dfkoreastocks['KESIS'],
                            mode='lines',
                            name='KESIS',
                            line=dict(color='#5B9BD5', dash='solid'),
                            ))#,secondary_y=False)
        fig6.add_trace(go.Bar(x=dfkoreastocks.index, y=dfkoreastocks['JODI vs KESIS'],
                            name='JODI vs KESIS',
                            marker_color='#FFC000',
                            ))#,secondary_y=False)
        fig6.update_layout(
             autosize=True,
             showlegend=True,
             #colorscale='RdBu',
             legend=dict(x=0, y=-0.2),
             legend_orientation="h",
             title_text='Korea Stocks '+str(today),
             xaxis = dict(dtick="M2"),
             #xaxis_tickformat = '%B',
             hovermode='x unified',
             plot_bgcolor = 'white',
             template='ggplot2'  
         )
        #fig6.update_layout(dict(yaxis2={'anchor': 'x', 'overlaying': 'y', 'side': 'left'},yaxis={'anchor': 'x', 'domain': [0.0, 1.0], 'side':'right'}))
        fig6.update_yaxes(title_text="Stocks Bcm")
        #fig6.update_yaxes(title_text="Delta Mt", secondary_y=False)
        #fig6.update_yaxes(rangemode="tozero",secondary_y=False)
        #print(dfjpc)
        fig7=go.Figure()
        fig7.add_trace(go.Scatter( x=dfjpc.index,y=dfjpc['City Gas Stocks min'],
                   fill='tonexty',
                   fillcolor='rgba(65,105,225,0)',
                   line_color='rgba(65,105,225,0)',
                   showlegend=False,
                   name='2015-2022 range Min'
                   ))
           
        fig7.add_trace(go.Scatter(x=dfjpc.index,y=dfjpc['City Gas Stocks max'],
                fill='tonexty',
                fillcolor='rgba(191,191,191,0.7)',
                line_color='rgba(65,105,225,0)',
                showlegend=True,
                name='2015-2022 range'
                ))
        fig7.add_trace(
               go.Scatter(
                   x=dfjpc.index,
                   y=dfjpc['City Gas Stocks 2022'],
                   mode='lines',
                   name='City Gas Stocks 2022',
                   line=dict(color='blue', dash='dot')                   
                   )
               )           
        fig7.add_trace(
            go.Scatter(
                x=dfjpc.index,
                y=dfjpc['City Gas Stocks 2023'],
                mode='lines',
                name='City Gas Stocks 2023',
                    )
                )
       
        fig7.update_layout(
             autosize=True,
             showlegend=True,
             #colorscale='RdBu',
             legend=dict(x=0, y=-0.2),
             legend_orientation="h",
             title_text='Japan City Gas Stock '+str(today)+', kt',
             xaxis = dict(dtick="M1"),
             xaxis_tickformat = '%B',
             hovermode='x unified',
             plot_bgcolor = 'white',
             template='ggplot2'  
         )
        fig7.update_yaxes(rangemode="tozero")
        '''
        fig8=go.Figure()
        fig8.add_trace(go.Scatter( x=dfjpc.index,y=dfjpc['Power Gas Stocks min'] ,
                   fill='tonexty',
                   fillcolor='rgba(65,105,225,0)',
                   line_color='rgba(65,105,225,0)',
                   showlegend=False,
                   name='2015-2022 range Min'
                   ))
           
        fig8.add_trace(go.Scatter(x=dfjpc.index,y=dfjpc['Power Gas Stocks max'],
                fill='tonexty',
                fillcolor='rgba(191,191,191,0.7)',
                line_color='rgba(65,105,225,0)',
                showlegend=True,
                name='2015-2022 range'
                ))
              
        fig8.add_trace(
            go.Scatter(
                x=dfjpc.index,
                y=dfjpc['Power Gas Stocks 2022'],
                mode='lines',
                name='Power Gas Stocks 2022',
                line=dict(color='blue', dash='dot')  
                    )
                )
        fig8.add_trace(
            go.Scatter(
                x=dfjpc.index,
                y=dfjpc['Power Gas Stocks 2023'],
                mode='lines',
                name='Power Gas Stocks 2023',
                    )
                )
        #print(demetifull.resample('MS').mean().loc[dfjpc.index.to_list()[0]:, 'Meti Latest']/1.397*1000/1000000)
        fig8.add_trace(go.Scatter(x=dfjp.index, y=demetifull.resample('MS').mean().loc[dfjpc.index.to_list()[0]:, 'Meti Latest']/1000,  #mt to kt

                                  mode='lines',
                            name='METI 2023 Act',
                            line=dict(color='red', dash='solid')
                            ))
       
        fig8.update_layout(
             autosize=True,
             showlegend=True,
             #colorscale='RdBu',
             legend=dict(x=0, y=-0.2),
             legend_orientation="h",
             title_text='Japan Power Gas Stocks '+str(today)+', kt',
             xaxis = dict(dtick="M1"),
             xaxis_tickformat = '%B',
             hovermode='x unified',
             plot_bgcolor = 'white',
             template='ggplot2'  
         )
        fig8.update_yaxes(rangemode="tozero")
        '''
        
        #create new df same with meti index
        #print(dfjpc)
        monthend = dfjpc['Power Gas Stocks 2023'].loc[~dfjpc['Power Gas Stocks 2023'].isnull()].index[-1]
        days= calendar.monthrange(monthend.year, monthend.month)[1]
        end =str(monthend.year)+'-'+str(monthend.month)+'-'+str(days)
        dffig8 = pd.DataFrame(index = pd.date_range(start = dfjpc.index[0], end = '2023-12-31'))
        #print(demetifull.loc['2023-01-01':])
        dffig8['Power Gas Stocks min'] = dfjpc['Power Gas Stocks min']
        dffig8['Power Gas Stocks max'] = dfjpc['Power Gas Stocks max']
        dffig8['Power Gas Stocks 2022'] = dfjpc['Power Gas Stocks 2022'] 
        dffig8.fillna(method = 'ffill', inplace=True)
        dffig8['Power Gas Stocks 2023'] = dfjpc['Power Gas Stocks 2023']
        dffig8.loc[:end, 'Power Gas Stocks 2023'].fillna(method='ffill', inplace=True)
        dffig8['Meti Latest'] = demetifull['Meti Latest']/1000
        monthend = pd.date_range(start=dfjpc.index[0], end = demetifull.index[-1],freq='M')
        dffig8.loc[monthend, 'Meti Latest monthly'] = demetifull.loc[monthend,'Meti Latest']/1000
        dffig8.loc[:monthend[-1],'Meti Latest monthly'].fillna(method='bfill', inplace=True)
        #print(dffig8)
        fig8=go.Figure()
        fig8.add_trace(go.Scatter( x=dffig8.index,y=dffig8['Power Gas Stocks min'] ,
                   fill='tonexty',
                   fillcolor='rgba(65,105,225,0)',
                   line_color='rgba(65,105,225,0)',
                   showlegend=False,
                   name='2015-2022 range Min'
                   ))
           
        fig8.add_trace(go.Scatter(x=dffig8.index,y=dffig8['Power Gas Stocks max'],
                fill='tonexty',
                fillcolor='rgba(191,191,191,0.7)',
                line_color='rgba(65,105,225,0)',
                showlegend=True,
                name='2015-2022 range'
                ))
              
        fig8.add_trace(
            go.Scatter(
                x=dffig8.index,
                y=dffig8['Power Gas Stocks 2022'],
                mode='lines',
                name='Power Gas Stocks 2022',
                line=dict(color='blue', dash='dot')  
                    )
                )
        fig8.add_trace(
            go.Scatter(
                x=dffig8.index,
                y=dffig8['Power Gas Stocks 2023'],
                mode='lines',
                name='Power Gas Stocks 2023',
                    )
                )
        #print(demetifull.resample('MS').mean().loc[dfjpc.index.to_list()[0]:, 'Meti Latest']/1.397*1000/1000000)
        fig8.add_trace(go.Scatter(x=dffig8.index, y=dffig8['Meti Latest'] ,

                                  mode='lines',
                            name='METI 2023 Act',
                            line=dict(color='red', dash='solid')
                            ))
        
        fig8.add_trace(go.Scatter(x=dffig8.index, y=dffig8['Meti Latest monthly'] ,

                                  mode='lines',
                            name='METI 2023 Act monthly',
                            line=dict(color='red', dash='dot')
                            ))
       
        fig8.update_layout(
             autosize=True,
             showlegend=True,
             #colorscale='RdBu',
             legend=dict(x=0, y=-0.2),
             legend_orientation="h",
             title_text='Japan Power Gas Stocks '+str(today)+', kt',
             #xaxis = dict(dtick="M1"),
             #xaxis_tickformat = '%B',
             hovermode='x unified',
             plot_bgcolor = 'white',
             template='ggplot2'  
         )
        fig8.update_yaxes(rangemode="tozero")
        
        
        fig9=go.Figure()
        fig9.add_trace(
               go.Scatter(
                   x=demetifull.index,
                   y=demetifull['Meti Latest'],
                   mode='lines',
                   name='METI',
                   line=dict(color='red', dash='solid')                   
                   )
               )           
        fig9.add_trace(
            go.Scatter(
                x=demetifull.index,
                y=demetifull['Average Of Last 4 Years'],
                mode='lines',
                name='Average Of Last 4 Years',
                line=dict(color='black', dash='solid')
                    )
                )
       
        fig9.update_layout(
             autosize=True,
             showlegend=True,
             #colorscale='RdBu',
             legend=dict(x=0, y=-0.2),
             legend_orientation="h",
             title_text='Japan METI Weekly survey power stock inventory level',
             #xaxis = dict(dtick="M1"),
             #xaxis_tickformat = '%B',
             hovermode='x unified',
             plot_bgcolor = 'white',
             template='ggplot2'  
         )
        fig9.update_yaxes(rangemode="tozero")
        
        
        fig10=go.Figure()
        fig10.add_trace(go.Scatter(x=dfjp.index, y=dfjp['2023 Act'],
                            mode='lines',
                            name='2023 Act',
                            line=dict(color='red', dash='solid')
                            ))
        fig10.add_trace(go.Scatter(x=dfjpc.index, y=dfjpc['METI 2023'],
                            mode='lines',
                            name='METI 2023',
                            line=dict(color='#264478', dash='solid')
                            ))
        fig10.add_trace(
             go.Scatter(
                 x=dfjpc.index,
                 y=dfjpc['City Gas Stocks 2023'],
                 mode='lines',
                 name='City Gas Stocks 2023',
                     )
                 )
        fig10.add_trace(
            go.Scatter(
                x=dfjpc.index,
                y=dfjpc['Power Gas Stocks 2023'],
                mode='lines',
                name='Power Gas Stocks 2023',
                    )
                )
        fig10.update_layout(
             autosize=True,
             showlegend=True,
             #colorscale='RdBu',
             legend=dict(x=0, y=-0.2),
             legend_orientation="h",
             title_text='Japan METI Weekly survey power stock inventory level',
             #xaxis = dict(dtick="M1"),
             #xaxis_tickformat = '%B',
             hovermode='x unified',
             plot_bgcolor = 'white',
             template='ggplot2'  
         )
        fig10.update_yaxes(rangemode="tozero")
        
        py.plot(fig1, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/JKstock/JPJODI.html', auto_open=False)
        py.plot(fig2, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/JKstock/KRJODI.html', auto_open=False)
        py.plot(fig3, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/JKstock/JPrange.html', auto_open=False)
        py.plot(fig4, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/JKstock/KRrange.html', auto_open=False)
        py.plot(fig5, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/JKstock/JPdiff.html', auto_open=False)
        py.plot(fig6, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/JKstock/KRdiff.html', auto_open=False)
        py.plot(fig7, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/JKstock/JPcitygas.html', auto_open=False)
        py.plot(fig8, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/JKstock/JPpowergas.html', auto_open=False)
        py.plot(fig9, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/analysis/JKstock/JPMeti.html', auto_open=False)

        
        
        fig1.write_image("U:/Trading - Gas/LNG/LNG website/analysis/JKstock/JPJODI.png")
        fig2.write_image("U:/Trading - Gas/LNG/LNG website/analysis/JKstock/KRJODI.png")
        fig3.write_image("U:/Trading - Gas/LNG/LNG website/analysis/JKstock/JPrange.png")
        fig4.write_image("U:/Trading - Gas/LNG/LNG website/analysis/JKstock/KRrange.png")
        fig5.write_image("U:/Trading - Gas/LNG/LNG website/analysis/JKstock/JPdiff.png")
        fig6.write_image("U:/Trading - Gas/LNG/LNG website/analysis/JKstock/KRdiff.png")
        fig7.write_image("U:/Trading - Gas/LNG/LNG website/analysis/JKstock/JPcitygas.png")
        fig8.write_image("U:/Trading - Gas/LNG/LNG website/analysis/JKstock/JPpowergas.png")
        fig9.write_image("U:/Trading - Gas/LNG/LNG website/analysis/JKstock/JPMeti.png")
        
        
        
a=JKstocks
dfjodi_jp, dfjodi_kr, demetifull=a.get_data()
dfjpc, dfkrc, dfjapanstocks, dfkoreastocks=a.copy_data(dfjodi_jp, dfjodi_kr, demetifull)
dfjp,dfkr=a.chart_data(dfjodi_jp, dfjodi_kr)
a.plot_chart(dfjp,dfkr,dfjpc, dfkrc, dfjapanstocks, dfkoreastocks, demetifull)