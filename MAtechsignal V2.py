# -*- coding: utf-8 -*-
"""
Created on Mon Apr 11 15:43:16 2022

@author: SVC-GASQuant2-Prod
"""


#V1 add std sl and close
#V2 not use V1 sl and close, only need buy and sell signal, also no need pnl calc for the signal

import numpy as np
import pandas as pd
import sqlalchemy as sa
import urllib
import datetime
import stockstats
from dateutil.relativedelta import relativedelta
import plotly.offline as py
import plotly.graph_objs as go
from pandas.tseries.offsets import BDay

import dash
import dash_html_components as html
import dash_core_components as dcc
from dash.dependencies import Input, Output
import dash_table
from dash.exceptions import PreventUpdate

pd.set_option('display.max_columns',20)

def get_trade_price():
#%% Read in data and merge
    #Inputs
    startp = str(datetime.date.today()-BDay(0))#'2022-01-01'
    endp = str(datetime.date.today()+BDay(1))#'2022-02-25'
    contract = "'"+(datetime.date.today()+relativedelta(months=1)).strftime('%b-%y')+"'"
    #print(contract)
    #Read in Trayport Data
    params = urllib.parse.quote_plus(r'DRIVER={SQL Server};SERVER=PRD-DB-SQL-214;DATABASE=Trayport;Trusted_Connection=yes')
    conn_str = 'mssql+pyodbc:///?odbc_connect={}'.format(params)
    engine = sa.create_engine(conn_str)
    
    
    trayportdb = pd.read_sql("""SELECT [Id]
                                      ,[DateTime]
                                      ,[LastUpdate]
                                      ,[TradeID]
                                      ,[OrderID]
                                      ,[Action]
                                      ,[InstrumentID]
                                      ,[InstrumentName]
                                      ,[FirstSequenceItemName]
                                      ,[ExecutionVenueID]
                                      ,[Price]
                                      ,[Volume]
                                      ,[Timestamp]
                                     FROM [Trayport].[ts].[Trade]
                                    WHERE [DateTime] >= {} AND [DateTime] <= {} and [SeqSpan] = 'Single' 
                                    
                                    
                                    AND ([InstrumentName] = 'TTF Hi Cal 51.6' or [InstrumentName] = 'TTF Hi Cal 51.6 1MW'                                                                                                                           
                                    or [InstrumentName] = 'TTF Hi Cal 51.6 EEX' or [InstrumentName] = 'TTF Hi Cal 51.6 EEX OTF'
                                    or [InstrumentName] = 'TTF Hi Cal 51.6 1MW EEX Non-MTF' or [InstrumentName] = 'TTF Hi Cal 51.6 PEGAS' 
                                    or [InstrumentName] = 'TTF Hi Cal 51.6 PEGAS OTF' or [InstrumentName] = 'TTF Hi Cal 51.6 1MW PEGAS Non-MTF' 
                                    or [InstrumentName] = 'TTF Hi Cal 51.6 ICE ENDEX' or [InstrumentName] = 'TTF Hi Cal 51.6 ICE')
                                    
                                    AND [Volume] >= 5
                                    
                                    AND (CHARINDEX('Jan', [FirstSequenceItemName]) > 0
                                     or CHARINDEX('Feb', [FirstSequenceItemName]) > 0
                                     or CHARINDEX('Mar', [FirstSequenceItemName]) > 0
                                     or CHARINDEX('Apr', [FirstSequenceItemName]) > 0
                                     or CHARINDEX('May', [FirstSequenceItemName]) > 0
                                     or CHARINDEX('Jun', [FirstSequenceItemName]) > 0
                                     or CHARINDEX('Jul', [FirstSequenceItemName]) > 0
                                     or CHARINDEX('Aug', [FirstSequenceItemName]) > 0
                                     or CHARINDEX('Sep', [FirstSequenceItemName]) > 0
                                     or CHARINDEX('Oct', [FirstSequenceItemName]) > 0
                                     or CHARINDEX('Nov', [FirstSequenceItemName]) > 0
                                     or CHARINDEX('Dec', [FirstSequenceItemName]) > 0)
                                    
                                    AND CHARINDEX('BOM', [FirstSequenceItemName]) = 0
                                    
                                    AND FirstSequenceItemName = {}
    								order by DateTime
                                  
                                        """.format('\''+startp+'\'','\''+endp+'\'',contract), engine)
                                        
    #print(trayportdb)
    return trayportdb


def regroup_trade(trade, freq):
    
    dftrade = trade.copy()
    #regroup and get trade price
    dftrade = dftrade[['DateTime','Price']]
    dftrade['DateTime'] = pd.to_datetime(dftrade['DateTime'])
    dftrade = dftrade.groupby('DateTime').mean()
    dftrade = dftrade.resample('5S').mean() #every 5s traded price mean
    dftrade.fillna(method='bfill', inplace=True)
    #print(dftrade)
    dftrade = dftrade.loc[str(datetime.date.today())+' 06:01:00':]
    #print(dftrade)
    #dftradefull = pd.DataFrame(index=pd.date_range(start=2022-04-07 06:00:10, end = dftrade.index[-1], freq=freq))
    #trade df
    df = pd.DataFrame(index = pd.date_range(start=dftrade.index[0], end = dftrade.index[-1], freq=freq), columns=['open','close','high','low'])
    for i in df.index:
        #print(i)
        try:
            df.loc[i,'open'] = dftrade.loc[i,'Price']
            df.loc[i,'close'] = dftrade.loc[i+relativedelta(minutes=5),'Price']
            df.loc[i,'high'] = dftrade.loc[i:i+relativedelta(minutes=5),'Price'].max()
            df.loc[i,'low'] = dftrade.loc[i:i+relativedelta(minutes=5),'Price'].min()
        except KeyError:
            df.loc[i,'open'] = dftrade.loc[i,'Price']
            df.loc[i,'close'] = dftrade.iloc[-1,0]
            df.loc[i,'high'] = dftrade.loc[i:i+relativedelta(minutes=5),'Price'].max()
            df.loc[i,'low'] = dftrade.loc[i:i+relativedelta(minutes=5),'Price'].min()
            
    df['volume'] = 0
    
    
    
    #fig = go.Figure(data=[go.Candlestick(x=df.index,open=df['open'], high=df['high'],low=df['low'], close=df['close'])])
    #fig.update_layout(xaxis_rangeslider_visible=False)
    #py.plot(fig, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/trade view/TTF/MAtech.html', auto_open=True)
    #print(df)
    
    return df

def signal(df, freq, stoplevel):
    
    freqnum = int(freq[0])
    #tech format load    
    stock = stockstats.StockDataFrame.retype(df)
    #print(stock)
    stock['open']=stock['open']
    stock['close']=stock['close']
    stock['high']=stock['high']
    stock['low']=stock['low']
    stock['volume']=stock['volume']
    stock['amount']=stock['volume']
    #df for create signal
    result=pd.DataFrame(stock['close'].values, index=stock.index[:], columns=['MA'])
    result['open']=stock['open']
    result['close']=stock['close']
    result['high']=stock['high']
    result['low']=stock['low']
    
    
    #print(result.shape, ln.shape)
    result['K']=stock['kdjk']
    result['D']=stock['kdjd']
    result['J']=stock['kdjj_2'] 
    #result['macds'] = stock[['MACD_EMA_SHORT_3']]
    #result['macdh'] =stock['macd_{}_ema'.format(cls.MACD_EMA_SIGNAL)]
    result['MACD']=stock['macd']   
    #result['WR']=stock['wr_9']  
       
    #plot signal chart                                                  
    #result.MA.plot(figsize=(12,6),grid=True,fontsize=15, legend=True)
    #result.J.plot(figsize=(12,6),grid=True,fontsize=15,secondary_y=True, legend=True)
    #result.D.plot(figsize=(12,6),grid=True,fontsize=15,secondary_y=True, legend=True)
    #result.K.plot(figsize=(12,6),grid=True,fontsize=15,secondary_y=True, legend=True)
    #result.MACD.plot(figsize=(12,6),grid=True,fontsize=15,secondary_y=True, legend=True)
    #result.WR.plot(figsize=(12,6),grid=True,fontsize=15,secondary_y=True, legend=True)
    
    
    #signal logic    
    result['Jsignal'] = np.nan
    result['MACDsignal'] = np.nan
    result['signal'] = np.nan
    for i in result.index[1:]:
        try:
            
            if result.loc[i-relativedelta(minutes=freqnum),'J'] >= 80 and result.loc[i,'J'] < result.loc[i-relativedelta(minutes=freqnum),'J']:
                result.loc[i,'Jsignal'] = 'Sell'
                
                            
            if result.loc[i-relativedelta(minutes=freqnum),'J'] <= 20 and result.loc[i,'J'] > result.loc[i-relativedelta(minutes=freqnum),'J'] :
                result.loc[i,'Jsignal'] ='Buy'
                
                            
            if result.loc[i-relativedelta(minutes=freqnum),'MACD'] <= result.loc[i,'MACD'] > result.loc[i+relativedelta(minutes=freqnum),'MACD']:
                result.loc[i,'MACDsignal'] = 'Sell'
                
            if result.loc[i-relativedelta(minutes=freqnum),'MACD'] >= result.loc[i,'MACD'] < result.loc[i+relativedelta(minutes=freqnum),'MACD']:
                result.loc[i,'MACDsignal'] = 'Buy'
             
            
        except KeyError:
            pass
    #print(result['signal'].head(20))    
    result.fillna(method='ffill', inplace=True)
    
    #print(std)
    #create buy/sell signal
    for i in result.index:
        if result.loc[i,'Jsignal'] == 'Sell' and result.loc[i,'MACDsignal'] == 'Sell':
            result.loc[i,'signal'] = 'Sell'
        if result.loc[i,'Jsignal'] == 'Buy' and result.loc[i,'MACDsignal'] == 'Buy':
            result.loc[i,'signal'] = 'Buy'
        #if  result.loc[i,'Jsignal'] != result.loc[i,'MACDsignal']:
            #result.loc[i,'signal'] = 'Close'
    
    
        
        
    #print(result.loc['2022-04-08 09:45:00':])
    result['shift signal'] = result['signal'].shift(1)
    #print(result.loc[:, 'signal'].head(20))
    #print(result)
    
    '''
    for i in result.index[:]:
        try:
            if result.loc[i,'signal'] =='Buy' and result.loc[i+relativedelta(minutes=freqnum),'signal'] == 'Sell':
                result.loc[i,'signal'] = 'Close Buy and Sell'
                result.loc[i+relativedelta(minutes=freqnum),'signal'] = np.nan
            if result.loc[i,'signal'] =='Sell' and result.loc[i+relativedelta(minutes=freqnum),'signal'] == 'Buy':
                result.loc[i,'signal'] = 'Close Sell and Buy'
                result.loc[i+relativedelta(minutes=freqnum),'signal'] = np.nan
            if result.loc[i,'signal'] == result.loc[i,'shift signal']:
                result.loc[i,'signal'] = np.nan
        except KeyError:
            if result.loc[i,'signal'] == result.loc[i,'shift signal']:
                result.loc[i,'signal'] = np.nan
    
   '''     
    
    #result = result.loc[str(datetime.date.today())+' 06:15:00':]
    
    #print(result.loc[:,'signal'].to_list())
    
    #print(result.loc['2022-04-07 07:01:00','signal'] == 'Close Sell and Buy')
    #print(result.loc[str(datetime.date.today())+' 10:51:00':str(datetime.date.today())+' 13:31:00','signal'])
    
    #close filter
    #std = round(result['close'].std(), 2)
    #print(std)
    
    for i in range(0, result.shape[0]-1):
        if result.loc[result.index[i], 'signal'] == 'Sell' or result.loc[result.index[i],'signal'] == 'Close Buy and Sell':
            for j in range(i+1,result.shape[0]-1):
                if result.loc[result.index[j],'signal'] == 'Sell':
                    result.loc[result.index[j],'signal'] = np.nan
                elif result.loc[result.index[j],'signal'] == 'Buy':
                    result.loc[result.index[j],'signal'] = 'Close Sell and Buy'
                    break
                              
                else:
                    continue
                
        if result.loc[result.index[i], 'signal'] == 'Buy' or result.loc[result.index[i],'signal'] == 'Close Sell and Buy':
            for j in range(i+1,result.shape[0]-1):
                if result.loc[result.index[j],'signal'] == 'Buy':
                    result.loc[result.index[j],'signal'] = np.nan
                elif result.loc[result.index[j],'signal'] == 'Sell':
                    result.loc[result.index[j],'signal'] = 'Close Buy and Sell'
                    break
                
                else:
                    continue 
    #print(result.loc[str(datetime.date.today())+' 10:51:00':str(datetime.date.today())+' 13:31:00','signal'])                
            
    '''
    for i in range(0, result.shape[0]-1):
        
        if result.loc[result.index[i], 'signal'] == 'Sell' or result.loc[result.index[i],'signal'] == 'Close Buy and Sell':
            for j in range(i+1,result.shape[0]-1):
                if result.loc[result.index[j],'signal'] == 'Sell' and result.loc[result.index[i],'close'] - result.loc[result.index[j],'close'] < std:
                    #print('yes2',result.index[j])
                    result.loc[result.index[j],'signal'] = np.nan
                    
                if result.loc[result.index[j],'signal'] == 'Buy':
                    result.loc[result.index[j],'signal'] = 'Close Sell and Buy'
                    break #end J loop
                else:
                    continue  
        
                 
                    
        if result.loc[result.index[i], 'signal'] == 'Buy' or result.loc[result.index[i],'signal'] == 'Close Sell and Buy':
            #print('yes1', result.index[i])
            #print(result.loc['2022-04-11 07:51:15'],'signal')
            for j in range(i+1,result.shape[0]-1):
                #print(result.loc[result.index[j],'signal'] == 'Sell',result.index[j])
               # print(j)
                #print(result.loc[str(datetime.date.today())+' 10:51:00':str(datetime.date.today())+' 13:31:00','signal'])
                if result.loc[result.index[j],'signal'] == 'Buy' and result.loc[result.index[j],'close'] - result.loc[result.index[i],'close'] < std:
                    result.loc[result.index[j],'signal'] = np.nan
                    
                if result.loc[result.index[j],'signal'] == 'Sell':
                    #print('yes3', result.index[j])
                    result.loc[result.index[j],'signal'] = 'Close Buy and Sell'
                    break #end j
                else:
                    continue  
        
        
                 #end j, continue i
    
    print(result.loc[str(datetime.date.today())+' 10:51:00':str(datetime.date.today())+' 13:31:00','signal'])
    
    '''
    
    #calc pnl
    cumpnl = pd.DataFrame(columns=['Sell','Sell Close','Buy','Buy Close'])
    for i in result.index:
        if result.loc[i, 'signal'] == 'Sell' or result.loc[i,'signal'] == 'Close Buy and Sell':
            cumpnl.loc[i,'Sell'] = result.loc[i, 'high']
            for j in pd.date_range(start=i, end=result.index[-1], freq=freq):
                if result.loc[j, 'signal'] == 'Close' or result.loc[j,'signal'] == 'Close Sell and Buy':
                    cumpnl.loc[j,'Sell Close'] = result.loc[j,'low']
                    break
        if result.loc[i, 'signal'] == 'Buy' or result.loc[i,'signal'] == 'Close Sell and Buy':
            cumpnl.loc[i,'Buy'] = result.loc[i, 'low']
            for j in pd.date_range(start=i, end=result.index[-1], freq=freq):
                if result.loc[j, 'signal'] == 'Close' or result.loc[j,'signal'] == 'Close Buy and Sell':
                    cumpnl.loc[j,'Buy Close'] = result.loc[j,'high']
                    break
    cumpnl.sort_index(inplace=True)
    #print(cumpnl)
    if pd.notna(cumpnl.loc[cumpnl.index[-1],'Sell']):
         cumpnl.loc[result.index[-1],'Sell Close'] = result.loc[result.index[-1], 'low']
    if pd.notna(cumpnl.loc[cumpnl.index[-1],'Buy']):
         cumpnl.loc[result.index[-1],'Buy Close'] = result.loc[result.index[-1], 'high']
    cumpnl.fillna(0, inplace=True)
    
    '''
    std = result['close'].std()
    for i in range(0,cumpnl.shape[0]-1):#cumpnl.index:
        if pd.notna(cumpnl.loc[cumpnl.index[i],'Sell']):
            if cumpnl.loc[cumpnl.index[i+1],'Sell Close'] - cumpnl.loc[cumpnl.index[i],'Sell'] < std:
                cumpnl.loc[cumpnl.index[i+1],'Sell Close'] = 0
                result.loc[cumpnl.index[i+1], 'signal'] = np.nan
                for j in range(i, cumpnl.shape[0]-1):
                    
                    if pd.notna(cumpnl.loc[cumpnl.index[j],'Sell'])
    '''
    
    
    #live pnl
    cumpnl.loc['live pnl'] = cumpnl.sum()
    livepnl = round(cumpnl.loc['live pnl','Sell'] - cumpnl.loc['live pnl','Sell Close'] + cumpnl.loc['live pnl','Buy Close'] - cumpnl.loc['live pnl','Buy'], 2) 
    print('Live PnL ',livepnl)
    
    
    
    dfsignal = result.copy()
    
    return dfsignal

def chart(dfsignal):
    
    #print(str(dfsignal))
    fig = go.Figure() 
            
    fig.add_trace(
            go.Candlestick(x=dfsignal.index,
                           open=dfsignal['open'], high=dfsignal['high'],
                           low=dfsignal['low'], close=dfsignal['close']))
            
    for i in dfsignal.index:
        if dfsignal.loc[i,'signal'] == 'Sell':
            fig.add_annotation(x=i, y=dfsignal['high'].loc[i],
                    text = 'Sell @'+str(dfsignal['high'].loc[i].round(2)),
                    showarrow=True,
                    arrowhead=5)
        if dfsignal.loc[i,'signal'] == 'Buy':
            fig.add_annotation(x=i, y=dfsignal['low'].loc[i],
                    text = 'Buy @'+str(dfsignal['low'].loc[i].round(2)),
                    showarrow=True,
                    arrowhead=5)
        if dfsignal.loc[i,'signal'] == 'Close':
            fig.add_annotation(x=i, y=dfsignal['MA'].loc[i],
                    text = 'Close @'+str(dfsignal['MA'].loc[i].round(2)),
                    showarrow=True,
                    arrowhead=5)
        if dfsignal.loc[i,'signal'] == 'Close Buy and Sell':
            fig.add_annotation(x=i, y=dfsignal['high'].loc[i],
                    text = 'Close Buy and Sell @'+str(dfsignal['high'].loc[i].round(2)),
                    showarrow=True,
                    arrowhead=5)
        if dfsignal.loc[i,'signal'] == 'Close Sell and Buy':
            fig.add_annotation(x=i, y=dfsignal['low'].loc[i],
                    text = 'Close Sell and Buy @'+str(dfsignal['low'].loc[i].round(2)),
                    showarrow=True,
                    arrowhead=5)
        if dfsignal.loc[i,'signal'] == 'SL':
            fig.add_annotation(x=i, y=dfsignal['MA'].loc[i],
                    text = 'SL @'+str(dfsignal['MA'].loc[i].round(2)),
                    showarrow=True,
                    arrowhead=5)    
        
        
    #print(Basin)    
    fig.update_layout(
        autosize=True,
        showlegend=False,
        legend=dict(x=0, y=-0.2),
        legend_orientation="h",
        title_text='TTF '+(datetime.date.today()+relativedelta(months=1)).strftime('%b-%y')+' Intraday Live Trading Signal '+ str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')),#+'<br>'+'Live Cumulative PnL '+str(livepnl),
        yaxis_title="Eur",
        #xaxis = dict(dtick="M1"),
        hovermode='x unified',
        height=888,
        plot_bgcolor = 'white',
        template='ggplot2'  ,
        bargap  = 0
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
    fig.update_layout(xaxis_rangeslider_visible=False)
    #py.plot(fig, filename='C:/Users/SVC-GASQuant2-Prod/YWwebsite/LNG website/trade view/TTF/MAtech.html', auto_open=False)    
    return fig

#trade = get_trade_price()
#freq = '5T'
#df = regroup_trade(trade,freq)
#dfsignal = signal(df,freq, stoplevel=1)
#chart(dfsignal)

app = dash.Dash(__name__)

app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-content')
])


index_page = html.Div([
    dcc.Link('TTFMA1', href='/TTFMA1'),
    html.Br(),
    #dcc.Link('Live Position Total', href='/Live_Position_total'),
    #html.Br(),
    #dcc.Link('Live Position New', href='/Live_Position_New'),
    #html.Br(),
    #dcc.Link('Calc', href='/Calc'),
    
    
])

# Update the index
@app.callback(Output('page-content', 'children'),
              [Input('url', 'pathname')])
def display_page(pathname):
    if pathname == '/TTFMA1':
        
        TTFMA1_layout = html.Div([
            dcc.Graph(id='TTFMA1'),
            dcc.Interval(
            id='interval-component',
            interval=30*1000, # in milliseconds, every 10 second
            n_intervals=0
        )
            ])
      
        
        return TTFMA1_layout
    
    else:
        return index_page
    
 
@app.callback(Output('TTFMA1', 'figure'),
              Input('interval-component', 'n_intervals'))
def update_graph_live(n):
    
    trade = get_trade_price()
    freq = '5T'
    df = regroup_trade(trade,freq)
    dfsignal = signal(df,freq, stoplevel=1)
    fig = chart(dfsignal)
    
    

    return fig

if __name__ == '__main__':
    app.run_server(debug=False, host='0.0.0.0', port=8100)
    
