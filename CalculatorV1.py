# -*- coding: utf-8 -*-
"""
Created on Fri Aug  6 09:57:15 2021

@author: SVC-PowerUK-Test
"""

#calculator V1 fixed overnight bug and use BBG price
import time
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

import dash
import dash_html_components as html
import dash_core_components as dcc
import numpy as np
from dash.dependencies import Input, Output
import sys

import datetime
import numpy as np
import pandas as pd

from dateutil.relativedelta import relativedelta
sys.path.append('C:\\Users\SVC-GASQuant2-Prod\YWwebsite\Python Scripts\class') 

import dash_table
from DBtoDF import DBTOPD


#print(df)
def get_new_data():
    global df, month_start, month_end, total_days, ndays, remain_days, today, column, cum_platts, dfprint
    #get platts price data
    platts=DBTOPD('PRD-DB-SQL-211','LNG', 'ana', 'PlattsPrint')
    dbplatts=platts.sql_to_df()

    df=pd.DataFrame(dbplatts['value'].iloc[-30:].values, index=pd.to_datetime(dbplatts['date'].iloc[-30:],format='%Y-%m-%d') , columns=['Close'])
    #print(df)
    #Singapore holiday
    holidays=pd.to_datetime(['2021-07-20','2021-08-09','2021-11-04','2021-12-25','2021-12-27','2022-01-01','2022-02-01','2022-02-02','2022-04-15','2022-05-01',

                             '2022-05-02','2022-05-16','2022-07-09','2022-08-09','2022-10-24','2022-12-24','2022-12-26','2023-01-02',
                             '2023-01-23','2023-01-24','2023-04-07','2023-05-01','2023-06-02','2023-06-29','2023-08-09','2023-09-01','2023-11-13','2023-12-25','2024-01-01'])
    
    #time window
    today = datetime.date.today()
    if today.day <= 15:
        month_start = str((today-relativedelta(months=1)).year) +'-'+ str((today-relativedelta(months=1)).month) +'-16'
        month_end = str(today.year) +'-'+ str(today.month) +'-15'
        
    else:
        month_start = str(today.year) +'-'+ str(today.month) +'-16'
        month_end = str((today+relativedelta(months=1)).year) +'-'+ str((today+relativedelta(months=1)).month) +'-15'

    month = pd.bdate_range(month_start, month_end,freq='B').to_list()
    
    for i in holidays:
        if i in month:
            #nholidays+=1
            month.remove(i)
    total_days = len(month)
    
   #return last working day when today is bank holiday 
    month_days=pd.bdate_range(month_start, month_end,freq='B').to_list()
    if today in month:
        ndays = month.index(today)+1
    else:
        last_idx = np.searchsorted([datetime.datetime.timestamp(i) for i in month], 
                    datetime.datetime.timestamp(pd.Timestamp('now').normalize())) - 1 
        last_day = month_days[last_idx]
        ndays = month.index(last_day)+1
    
    remain_days = total_days - ndays
    #print('remain_days',remain_days)
    #return df, month_start, month_end, total_days, ndays, remain_days, month
    cum_platts = df['Close'].loc[month_start:month_end].mean()
    #print('cum_platts',cum_platts)
    #print(month)
    dfprint = pd.DataFrame(index=['print'], columns=month)
    #print('1',month)
    #print('2',df['Close'].loc[month_start:])
    #yesterdayidx=np.searchsorted([datetime.datetime.timestamp(i) for i in month], datetime.datetime.timestamp(pd.Timestamp('now').normalize())) - 1 
    #yesterday = month_days[yesterdayidx]
    #print(dfprint.loc['print',month_start:df.index[-1]])
    #print(df['Close'].loc[month_start:])
    dfprint.loc['print',month_start:df.index[-1]] = df['Close'].loc[month_start:].values.round(3) #zheli
    #print('3',dfprint) 
    dfprint.columns = dfprint.columns.strftime('%Y-%m-%d')
    #dfprint = dfprint.round(3)
    #print('4',dfprint)
    column = []
    
    for i in dfprint.columns:
        hd={"name":i,"id": i}
        column.append(hd)
    #print('5',column)  
    print('get_new_data at ', datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

UPDADE_INTERVAL = 3600
def get_new_data_every(period=UPDADE_INTERVAL):
    """Update the data every 'period' seconds"""
    global cal_layout
    while True:
        get_new_data()
        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print("data updated at: ", now)
        #BOM
        cal_layout= html.Div([
        
        html.Div([html.I("BOM CALCULATOR"),
        html.Hr(),
        
        dcc.Input(id="swap", type="number", placeholder="Input SWAP Here"),
        dcc.Input(id="print_today", type="number", placeholder="Input 0 if no today's print"),
        
        ],title='print price'),
        
        html.Div([
            html.Table([
            html.Tr([html.Td(['Today']), html.Td(id='today')]),
            html.Tr([html.Td(['nth of the month']), html.Td(id='ndays')]),
            html.Tr([html.Td(['Remaining days of month']), html.Td(id='remain_days')]),
            html.Tr([html.Td(['Total days']), html.Td(id='total_days')]),
            html.Tr([html.Td(['Cumulative monthly average']), html.Td(id='cum_price')]),
            html.Tr([html.Td(['BOM']), html.Td(id='bom')]),
                    ]),
        ]),
        
        html.Div(
            [html.Hr(),
             html.Br(),     
             html.I('Daily Print of Month'),
             html.Hr(),
        dash_table.DataTable(
            
            id='daily print',
            columns = column,
            #columns=[{"name": i, "id": i} for i in dfprint.columns.strftime('%Y-%m-%d')],
            data=dfprint.to_dict('records'),
            style_cell={
                    'whiteSpace': 'normal',
                    'height': 'auto',
                    'textAlign': 'center',
                },
            style_cell_conditional=[
               {
                    'if': {
                        'row_index': 5,
                        },
                    'backgroundColor': '#DDEBF7'
                },   
            ],
            style_header={
                    'backgroundColor': '#D9D9D9',
                    #'fontWeight': 'bold',
                    'textAlign': 'center',
                    #'border': '3px solid black'
                    #'color': 'white'
                },
            )
        ])

        ])
        time.sleep(period)
        
app = dash.Dash(__name__)
# get initial data                                                                                                                                                            
get_new_data()

app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-content')
])
index_page = html.Div([
    dcc.Link('cal', href='/cal'),
])

# Update the index
@app.callback(dash.dependencies.Output('page-content', 'children'),
              [dash.dependencies.Input('url', 'pathname')])
def display_page(pathname):
    if pathname == '/cal':
        return cal_layout
    else:
        return index_page



@app.callback(
    Output('today', 'children'),
    Output('ndays', 'children'),
    Output('remain_days', 'children'),
    Output('total_days', 'children'),
    Output('cum_price', 'children'),
    Output('bom', 'children'),
    Input('swap', 'value'),
    Input('print_today', 'value'),
    )
def callback_a(swap, print_today):
    
    if print_today == 0:
        bom = (swap*total_days - cum_platts*(ndays-1))/(remain_days+1)
    else:
        bom = (swap - ndays/total_days*(cum_platts*(ndays - 1)+print_today)/ndays)*total_days/remain_days
        
    return datetime.date.today(), ndays, remain_days, total_days,  round(cum_platts,3), round(bom,3)

# Run the function in another thread

executor = ThreadPoolExecutor(max_workers=2)
executor.submit(get_new_data_every)

if __name__ == '__main__':
    app.run_server(debug=False, host='0.0.0.0', port=8070)
