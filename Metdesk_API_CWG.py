# -*- coding: utf-8 -*-
"""

Functions that get CWG data via Metdesk API

model - ecop, eceps, ec46, gfsop, gfsens, gem, cmcens, cfs, cfsd, cfsw, magma, ukgl, ukgr
issue - A valid model issue, represented as an ISO 8601 format date/time eg. 2022-08-03T00:00:00Z
element - Available values : tt, rad, rrr6, ff100
location - see MetDesk_API_get_locations() eg. "JP" for Japan
start_dtg - in the format eg. 2022-08-03T00:00:00Z
end_dtg - in the format eg. 2022-08-18T00:00:00Z

"""

import requests
import pandas as pd

global headers

#Authorization is key provided by Jeremy from Metdesk
headers = {'Content-Type': 'application/json',
           'Authorization': 'jwt eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ0b2tlbklkIjoiRnJlZXBvaW50QVZGbHA4Q1hvRmZMUlNTS3hGd3g1N2oiLCJleHAiOjE2ODg3NzQzOTl9.KWv1X_9mYUCQCX5YAv36gc_IE1ZHjcCJAvmL7hHWB2E'}

def MetDesk_API_get_issues(model):
    
    url = 'https://api.metdesk.com/get/metdesk/cwg/v1/issues'
    params = {'model': model,
              }
    r = requests.get(url, headers=headers, params=params)
    
    return r.json()['data']

def MetDesk_API_get_latest(model, issue, element, location, mean=1, median=0, percentiles=0, members=0):
    
    url = 'https://api.metdesk.com/get/metdesk/cwg/v1/latest'
    
    params = {'model': model,
              'issue': issue,
              'element': element,
              'location': location,
              'mean': mean,
              'median': median,
              'percentiles': percentiles,
              'members': members
              }
    
    r = requests.get(url, headers=headers, params=params)
    
    df = pd.DataFrame(r.json()["data"]).set_index("dtg")
    #print(df)
    df.index = pd.to_datetime(df.index)
    
    return df

def MetDesk_API_get_dtgs(model, issue):
    
    url = 'https://api.metdesk.com/get/metdesk/cwg/v1/dtgs'
    
    params = {'model': model,
              'issue': issue,
              }
    
    r = requests.get(url, headers=headers, params=params)
    
    return r.json()["data"]

def MetDesk_API_get_climate(start_dtg, end_dtg, element, location):
    
    url = 'https://api.metdesk.com/get/metdesk/cwg/v1/climate'
    
    params = {'start_dtg': start_dtg,
              'end_dtg': end_dtg,
              'element': element,
              'location': location,
              }
    
    r = requests.get(url, headers=headers, params=params)
    
    df = pd.DataFrame(r.json()["data"]).set_index("dtg")
    df.index = pd.to_datetime(df.index)
    
    return df

def MetDesk_API_get_forecasts(model, issue, start_dtg, end_dtg, element, location, mean=1, median=0, percentiles=0, members=0):
    
    url = 'https://api.metdesk.com/get/metdesk/cwg/v1/forecasts'
    
    params = {'model': model,
              'issue': issue,
              'start_dtg': start_dtg,
              'end_dtg': end_dtg,
              'element': element,
              'location': location,
              'mean': mean,
              'median': median,
              'percentiles': percentiles,
              'members': members
              }
    
    r = requests.get(url, headers=headers, params=params)
    
    df = pd.DataFrame(r.json()["data"]).set_index("dtg")
    df.index = pd.to_datetime(df.index)
    
    return df

def MetDesk_API_get_locations():
    
    url = 'https://api.metdesk.com/get/metdesk/cwg/v1/locations'
    
    r = requests.get(url, headers=headers)
    
    df = pd.DataFrame(r.json()["data"])
    
    return df

def MetDesk_API_get_observations(start_dtg, end_dtg, element, location):
    url = 'https://api.metdesk.com/get/metdesk/cwg/v1//observations'
    params = {'start_dtg': start_dtg,
              'end_dtg': end_dtg,
              'element': element,
              'location': location,
              }
    r = requests.get(url, headers=headers, params=params)
    df = pd.DataFrame(r.json()["data"]).set_index("dtg")
    df.index = pd.to_datetime(df.index)
    return df
#print(MetDesk_API_get_locations())

#dflocation = MetDesk_API_get_locations()
#print(dflocation.loc[dflocation[dflocation['name'] == 'Taiwan'].index, 'location'])

def get_data():
    
    location_list = ['CN', 'JP', 'KR','TW']
    
    dftemp_latest = pd.DataFrame()
    dftemp_last = pd.DataFrame()
    #print( MetDesk_API_get_issues('ec46'))
    
    
    for i in location_list:
        
        #lastet
        latest_ens_dt = MetDesk_API_get_issues('eceps')[-1]
        latest_ec46_dt = MetDesk_API_get_issues('ec46')[-1]
        #print()
        ens_fcstdt = MetDesk_API_get_dtgs('eceps', latest_ens_dt)
        ec46_fcstdt = MetDesk_API_get_dtgs('ec46', latest_ec46_dt)
        dfens = MetDesk_API_get_forecasts('eceps', latest_ens_dt, ens_fcstdt[0], ens_fcstdt[-1], 'tt', i, mean=1, median=0, percentiles=0, members=0)
        dfens_pivot = dfens.pivot_table('value', index=dfens.index, columns='member')
        
        dfec46 = MetDesk_API_get_forecasts('ec46', latest_ec46_dt, ec46_fcstdt[0], ec46_fcstdt[-1], 'tt', i, mean=1, median=0, percentiles=0, members=0)
        dfec46_pivot = dfec46.pivot_table('value', index=dfec46.index, columns='member')
        
        dfnormal = MetDesk_API_get_climate(ec46_fcstdt[0], ec46_fcstdt[-1], 'tt', i)
        dftemp_latest[i+' ec46'] = dfec46_pivot['mean']
        dftemp_latest[i+' ens'] = dfens_pivot['mean']
        
        dftemp_latest[i+' normal'] = dfnormal['value']
        
        #last
        last_ens_dt = MetDesk_API_get_issues('eceps')[-2]
        last_ec46_dt = MetDesk_API_get_issues('ec46')[-2]
        
        ens_fcstdt = MetDesk_API_get_dtgs('eceps', last_ens_dt)
        ec46_fcstdt = MetDesk_API_get_dtgs('ec46', last_ec46_dt)
        dfens = MetDesk_API_get_forecasts('eceps', last_ens_dt, ens_fcstdt[0], ens_fcstdt[-1], 'tt', i, mean=1, median=0, percentiles=0, members=0)
        dfens_pivot = dfens.pivot_table('value', index=dfens.index, columns='member')
        
        dfec46 = MetDesk_API_get_forecasts('ec46', last_ec46_dt, ec46_fcstdt[0], ec46_fcstdt[-1], 'tt', i, mean=1, median=0, percentiles=0, members=0)
        dfec46_pivot = dfec46.pivot_table('value', index=dfec46.index, columns='member')
        
        
        #print(ec46_fcstdt[0], ec46_fcstdt[-1])
        dfnormal = MetDesk_API_get_climate(ec46_fcstdt[0], ec46_fcstdt[-1], 'tt', i)
        #print(dfnormal)
        
        dfhist = MetDesk_API_get_observations('2010-01-01T00:00:00Z', '2022-01-01T00:00:00Z', 'tt', i)
        print(dfhist)
        
        dftemp_last[i+' ec46'] = dfec46_pivot['mean']
        dftemp_last[i+' ens'] = dfens_pivot['mean']
        
        dftemp_last[i+' normal'] = dfnormal['value']
        
    dftemp_latest = dftemp_latest.resample('D').mean()  
    dftemp_latest.index = dftemp_latest.index.date
    dftemp_last = dftemp_last.resample('D').mean() 
    dftemp_last.index = dftemp_last.index.date
    #print(dftemp_latest)
    #print(dftemp_last)
    
    return dftemp_latest, dftemp_last

get_data()
