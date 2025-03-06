# -*- coding: utf-8 -*-
"""
Script to read ERA-5 reanalysis data from NetCDF files on network share

get_data() function takes country_name and outputs pandas dataframe

get_countries() returns list of countries stored in database

@author: Henry Bright hbright@freepoint.com
"""

import xarray as xr
import sqlalchemy
import pandas as pd
import numpy as np

global engine

engine = sqlalchemy.create_engine(
        'mssql+pyodbc://' + "PRD-DB-SQL-211" + '/' + "Meteorology" + '?driver=ODBC+Driver+13+for+SQL+Server')


def read_coordinate_data(latitude, longitude):
    
    latitude_rounded = round(latitude*4)/4
    longitude_rounded = round(longitude*4)/4
    
    path = r"\\prd-wth-app-01\WEATHER\Time_series"
    # path = r"Z:\dev_test\Time_series"
    
    try:
    
        with xr.open_dataset(r"{}\{}_{}.nc".format(path, latitude_rounded, longitude_rounded)) as ds_:
            ds = ds_.load()
        
    except FileNotFoundError:
        
        try:
        
            with xr.open_dataset(r"{}\{}_{}.nc".format(path, latitude_rounded, int(longitude_rounded))) as ds_:
                ds = ds_.load()
            
        except FileNotFoundError:
            
            try:
            
                with xr.open_dataset(r"{}\{}_{}.nc".format(path, int(latitude_rounded), longitude_rounded)) as ds_:
                    ds = ds_.load()                
                
            except FileNotFoundError:
                
                try:
                
                    with xr.open_dataset(r"{}\{}_{}.nc".format(path, int(latitude_rounded), int(longitude_rounded))) as ds_:
                        ds = ds_.load() 
                    
                except FileNotFoundError:
                    
                    try:
    
                        with xr.open_dataset(r"{}\{}_{}.nc".format(path, latitude_rounded, longitude_rounded)) as ds_:
                            ds = ds_.load() 
                        
                    except FileNotFoundError:
                        
                        try:
                        
                            with xr.open_dataset(r"{}\{}_{}.nc".format(path, latitude_rounded, int(longitude_rounded))) as ds_:
                                ds = ds_.load() 
                            
                        except FileNotFoundError:
                            
                            try:
                            
                                with xr.open_dataset(r"{}\{}_{}.nc".format(path, int(latitude_rounded), longitude_rounded)) as ds_:
                                    ds = ds_.load() 
                                
                            except FileNotFoundError:
                                
                                with xr.open_dataset(r"{}\{}_{}.nc".format(path, int(latitude_rounded), int(longitude_rounded))) as ds_:
                                    ds = ds_.load() 

    return ds

def read_coordinate_data_precip(latitude, longitude):
    
    latitude_rounded = round(latitude*4)/4
    longitude_rounded = round(longitude*4)/4
    
    path = r"\\prd-wth-app-01\WEATHER\Precipitation_time_series"
    # path = r"Z:\dev_test\Time_series"
    
    try:
    
        with xr.open_dataset(r"{}\{}_{}.nc".format(path, latitude_rounded, longitude_rounded)) as ds_:
            ds = ds_.load()
        
    except FileNotFoundError:
        
        try:
        
            with xr.open_dataset(r"{}\{}_{}.nc".format(path, latitude_rounded, int(longitude_rounded))) as ds_:
                ds = ds_.load()
            
        except FileNotFoundError:
            
            try:
            
                with xr.open_dataset(r"{}\{}_{}.nc".format(path, int(latitude_rounded), longitude_rounded)) as ds_:
                    ds = ds_.load()                
                
            except FileNotFoundError:
                
                try:
                
                    with xr.open_dataset(r"{}\{}_{}.nc".format(path, int(latitude_rounded), int(longitude_rounded))) as ds_:
                        ds = ds_.load() 
                    
                except FileNotFoundError:
                    
                    try:
    
                        with xr.open_dataset(r"{}\{}_{}.nc".format(path, latitude_rounded, longitude_rounded)) as ds_:
                            ds = ds_.load() 
                        
                    except FileNotFoundError:
                        
                        try:
                        
                            with xr.open_dataset(r"{}\{}_{}.nc".format(path, latitude_rounded, int(longitude_rounded))) as ds_:
                                ds = ds_.load() 
                            
                        except FileNotFoundError:
                            
                            try:
                            
                                with xr.open_dataset(r"{}\{}_{}.nc".format(path, int(latitude_rounded), longitude_rounded)) as ds_:
                                    ds = ds_.load() 
                                
                            except FileNotFoundError:
                                
                                with xr.open_dataset(r"{}\{}_{}.nc".format(path, int(latitude_rounded), int(longitude_rounded))) as ds_:
                                    ds = ds_.load() 

    return ds

def get_station_ids():    
    
    query = f"""SELECT * FROM [Meteorology].[dbo].[WeatherStationsCurveDefinition]
                WHERE ParameterName = 't_2m:C' AND ModelSourceName = 'ecmwf-era5'"""
    
    station_ids = pd.read_sql_query(query, engine)    
    #print(station_ids)
    return station_ids

# def get_all_station_ids():
    
#     engine = sqlalchemy.create_engine(
#             'mssql+pyodbc://' + "PRD-DB-SQL-211" + '/' + "Meteorology" + '?driver=ODBC+Driver+13+for+SQL+Server')
    
    
#     station_ids = pd.read_sql_table("RefMetDeskStationCatalogue", engine)    
    
#     return station_ids

def process_dates(start_date, end_date):
    
    start_date, end_date = pd.to_datetime(start_date), pd.to_datetime(end_date)
    
    try:
    
        return start_date.strftime("%Y-%m%d %H:00:00"), end_date.strftime("%Y-%m%d %H:00:00")
    
    except AttributeError:
        
        return None, None

def get_data(country, parameters, start_date=None, end_date=None):
    
    start_date_str, end_date_str = process_dates(start_date, end_date)
    
    parameter_mapping = {'t_2m:C':"2m Temp [C]", "wind_speed_10m:kn":"10m winds [m/s]",
                         'global_rad:W': "Solar Radiation [w/m2]"}
    
    parameter_tuple_str = str(tuple(parameters))[:-2]+')'
    
    station_ids = get_station_ids()
    
    curve_list = []
    
    if country not in station_ids["CountryName"].drop_duplicates().values:
        
        raise Exception(country+" not in list of countries stored in database")
    
    if (start_date_str==None) & (end_date_str==None):
        
        query = f"""Select a.ValueDate, SUM(a.WeightedValue) as WeightedAverage, ParameterName
                            from
                            (
                            	SELECT *,
                            	ts.Weighting * ts.Value as WeightedValue
                            	FROM [Meteorology].[dbo].[WeatherStationTimeSeriesLatest] ts
                            	WHERE ModelSourceName = 'ecmwf-era5' and CountryName = '{country}' and ParameterName in {parameter_tuple_str}
                            ) a
                            group by ValueDate, ParameterName
                            order by ValueDate"""    
    
    elif start_date_str==None:
    
        query = f"""Select a.ValueDate, SUM(a.WeightedValue) as WeightedAverage, ParameterName
                    from
                    (
                    	SELECT *,
                    	ts.Weighting * ts.Value as WeightedValue
                    	FROM [Meteorology].[dbo].[WeatherStationTimeSeriesLatest] ts
                    	WHERE ModelSourceName = 'ecmwf-era5' and CountryName = '{country}' and ParameterName in {parameter_tuple_str}
                        AND ValueDate < {end_date_str}
                    ) a
                    group by ValueDate, ParameterName
                    order by ValueDate""" 
                    
    elif end_date_str==None:
        
        query = f"""Select a.ValueDate, SUM(a.WeightedValue) as WeightedAverage, ParameterName
                    from
                    (
                    	SELECT *,
                    	ts.Weighting * ts.Value as WeightedValue
                    	FROM [Meteorology].[dbo].[WeatherStationTimeSeriesLatest] ts
                    	WHERE ModelSourceName = 'ecmwf-era5' and CountryName = '{country}' and ParameterName in {parameter_tuple_str}
                        AND ValueDate > {start_date_str}
                    ) a
                    group by ValueDate, ParameterName
                    order by ValueDate"""
                            
    else:
        
        query = f"""Select a.ValueDate, SUM(a.WeightedValue) as WeightedAverage, ParameterName
                            from
                            (
                            	SELECT *,
                            	ts.Weighting * ts.Value as WeightedValue
                            	FROM [Meteorology].[dbo].[WeatherStationTimeSeriesLatest] ts
                            	WHERE ModelSourceName = 'ecmwf-era5' and CountryName = '{country}' and ParameterName in {parameter_tuple_str}
                                AND ValueDate > {start_date_str} AND ValueDate < {end_date_str}
                            ) a
                            group by ValueDate, ParameterName
                            order by ValueDate"""    
        
        
    
    df = pd.read_sql_query(query, engine)
    
    df_final = pd.DataFrame()
    
    for ParameterName in df.ParameterName.drop_duplicates():
        
        df_final[parameter_mapping[ParameterName]] = df[df.ParameterName==ParameterName].set_index("ValueDate")["WeightedAverage"]
    
    try:
    
        df_final["10m winds [m/s]"] = df_final["10m winds [m/s]"] * 0.514444
        
    except KeyError:
        
        pass
    
    df_final_1991_2020 = df_final[((df_final.index.year>1990) & (df_final.index.year<2021))]
    
    seasonal_list = []
    
    for day in list(df_final_1991_2020.groupby(df_final_1991_2020.index.dayofyear)):
    
        seasonal_list.append(day[1].groupby(day[1].index.hour).mean())

    df_seasonal = pd.concat(seasonal_list).reset_index()
    df_seasonal["dayofyear"] = np.repeat(np.arange(1,367),24)
    
    df_seasonal = df_seasonal.rename(columns={"ValueDate":"hour","2m Temp [C]":"2m Temp 1991-2020 [C]","10m winds [m/s]":"10m winds 1991-2020 [m/s]", "Solar Radiation [w/m2]":"Solar Radiation 1991-2020 [w/m2]"})
    
    # add 5 years of blank data for use with forecast data
    df_final = df_final.reindex(pd.date_range(df_final.index[0], end=df_final.index[-1]+pd.Timedelta(weeks=52*10), freq="1H"))
    df_final["hour"] = df_final.index.hour
    df_final["dayofyear"] = df_final.index.dayofyear
    
    df_final_merge = pd.merge(df_final.reset_index(), df_seasonal, on=["hour","dayofyear"])
    df_final_merge.index.name = "time"
    
    if (start_date_str==None) or (end_date_str==None):
        
        return df_final_merge.set_index("index").drop(columns=["hour","dayofyear"]).sort_index()
        
    # else:
    
    #     return df_final_merge.set_index("index").drop(columns=["hour","dayofyear"]).sort_index()[pd.to_datetime(start_date_str):pd.to_datetime(end_date_str)]

# def get_precip_data(country, start_date_str=None, end_date_str=None):
    
#     station_ids = get_station_ids()
    
#     curve_list = []
    
#     if country not in station_ids["country"].drop_duplicates().values:
        
#         raise Exception(country+" not in list of countries stored in database")
    
#     for index, row in station_ids[station_ids["country"]==country].iterrows():
        
#         weighting = row[6]
#         latitude = row[7]
#         longitude = row[8]
        
#         print(weighting, latitude, longitude)
        
#         ds = read_coordinate_data_precip(latitude, longitude)
        
#         # print(ds, latitude, longitude)
        
#         curve_list.append(weighting * ds)
        
#     ds = sum(curve_list).load()
    
#     try:
    
#         df = ds.to_dataframe().dropna().droplevel("realization")
        
#     except KeyError:
        
#         try:
        
#             df = ds.to_dataframe().dropna().drop(columns=["lat","lon","realization"])
            
#         except KeyError:
            
#             try:
            
#                 df = ds.to_dataframe().dropna().drop(columns=["lat","realization"])
                
#             except KeyError:
                
#                 try:
                
#                     df = ds.to_dataframe().dropna().drop(columns=["lon","realization"])  
                    
#                 except KeyError:
                    
#                    df = ds.to_dataframe().dropna().drop(columns=["realization"])   
    
#     ds.close()        
    
#     df_final = pd.DataFrame()
    
#     try:
    
#         df_final["10m winds [m/s]"] = (df["uas"]**2 + df["vas"]**2)**0.5
        
#     except KeyError:
        
#         pass
    
#     try:
        
#         df_final["Solar Radiation [w/m2]"] = df["rss_accumulated"]/((df_final.index.shift(1, freq="H")-df_final.index).seconds)
        
#     except KeyError:
        
#         pass
    
#     df_final_1991_2020 = df_final[((df_final.index.year>1990) & (df_final.index.year<2021))]
    
#     seasonal_list = []
    
#     for day in list(df_final_1991_2020.groupby(df_final_1991_2020.index.dayofyear)):
    
#         seasonal_list.append(day[1].groupby(day[1].index.hour).mean())

#     df_seasonal = pd.concat(seasonal_list).reset_index()
#     df_seasonal["dayofyear"] = np.repeat(np.arange(1,367),24)
    
#     df_seasonal = df_seasonal.rename(columns={"time":"hour","2m Temp [C]":"2m Temp 1991-2020 [C]","10m winds [m/s]":"10m winds 1991-2020 [m/s]", "Solar Radiation [w/m2]":"Solar Radiation 1991-2020 [w/m2]"})
    
#     # add 5 years of blank data for use with forecast data
#     df_final = df_final.reindex(pd.date_range(df_final.index[0], end=df_final.index[-1]+pd.Timedelta(weeks=52*10), freq="1H"))
#     df_final["hour"] = df_final.index.hour
#     df_final["dayofyear"] = df_final.index.dayofyear
    
#     df_final_merge = pd.merge(df_final.reset_index(), df_seasonal, on=["hour","dayofyear"])
    
#     if (start_date_str==None) or (end_date_str==None):
        
#         return df_final_merge.set_index("index").drop(columns=["hour","dayofyear"]).sort_index()
        
#     else:
    
#         return df_final_merge.set_index("index").drop(columns=["hour","dayofyear"]).sort_index()[pd.to_datetime(start_date_str):pd.to_datetime(end_date_str)]


def get_station_data(station_id, parameters, start_date=None, end_date=None):
    
    start_date_str, end_date_str = process_dates(start_date, end_date)
    
    parameter_mapping = {'t_2m:C':"2m Temp [C]", "wind_speed_10m:kn":"10m winds [m/s]",
                         'global_rad:W': "Solar Radiation [w/m2]"}
    
    parameter_tuple_str = str(tuple(parameters))[:-2]+')'
    
    station_id = str(station_id).upper()
    
    station_ids = get_station_ids()["WMOId"].drop_duplicates().astype(str)
    
    if station_id in list(station_ids):
        
        pass
                                      
    else:
        
        raise Exception(station_id+" not listed in database")
    
    if (start_date_str==None) & (end_date_str==None):
    
        query = f"""
        
        SELECT *,
    	1 * ts.Value as WeightedValue
    	FROM [Meteorology].[dbo].[WeatherStationTimeSeriesLatest] ts
    	WHERE ModelSourceName = 'ecmwf-era5' and WMOId = '{station_id}' and ParameterName in {parameter_tuple_str}
    	"""
    elif start_date_str==None:
        
        query = f"""
        
        SELECT *,
    	1 * ts.Value as WeightedValue
    	FROM [Meteorology].[dbo].[WeatherStationTimeSeriesLatest] ts
    	WHERE ModelSourceName = 'ecmwf-era5' and WMOId = '{station_id}' and ParameterName in {parameter_tuple_str}
        AND ValueDate < {end_date_str}
    	"""
    
    elif end_date_str==None:
        
        query = f"""
        
        SELECT *,
    	1 * ts.Value as WeightedValue
    	FROM [Meteorology].[dbo].[WeatherStationTimeSeriesLatest] ts
    	WHERE ModelSourceName = 'ecmwf-era5' and WMOId = '{station_id}' and ParameterName in {parameter_tuple_str}
        AND ValueDate > {start_date_str}
    	"""
        
    else:
        
        query = f"""
        
        SELECT *,
    	1 * ts.Value as WeightedValue
    	FROM [Meteorology].[dbo].[WeatherStationTimeSeriesLatest] ts
    	WHERE ModelSourceName = 'ecmwf-era5' and WMOId = '{station_id}' and ParameterName in {parameter_tuple_str}
        AND ValueDate > {start_date_str} AND ValueDate <{end_date_str}
        """
    
    df = pd.read_sql_query(query, engine)
    
    df_final = pd.DataFrame()
    
    for ParameterName in df.ParameterName.drop_duplicates():
        
        df_final[parameter_mapping[ParameterName]] = df[df.ParameterName==ParameterName].set_index("ValueDate")["WeightedValue"]
        
        
    try:
    
        df_final["10m winds [m/s]"] = df_final["10m winds [m/s]"] * 0.514444
        
    except KeyError:
        
        pass
    
    #print(df_final)
    df_final_1991_2020 = df_final[((df_final.index.year>1990) & (df_final.index.year<2021))]
    
    seasonal_list = []
    
    for day in list(df_final_1991_2020.groupby(df_final_1991_2020.index.dayofyear)):
    
        seasonal_list.append(day[1].groupby(day[1].index.hour).mean())
    #print(seasonal_list)    
    df_seasonal = pd.concat(seasonal_list).reset_index()
    df_seasonal["dayofyear"] = np.repeat(np.arange(1,367),24)
    
    df_seasonal = df_seasonal.rename(columns={"ValueDate":"hour","2m Temp [C]":"2m Temp 1991-2020 [C]","10m winds [m/s]":"10m winds 1991-2020 [m/s]", "Solar Radiation [w/m2]":"Solar Radiation 1991-2020 [w/m2]"})
    #print('1111111111111')
    #print(df_seasonal)
    # add 5 years of blank data for use with forecast data
    df_final = df_final.reindex(pd.date_range(df_final.index[0], end=df_final.index[-1]+pd.Timedelta(weeks=52*10), freq="1H"))
    df_final["hour"] = df_final.index.hour
    df_final["dayofyear"] = df_final.index.dayofyear
    
    df_final_merge = pd.merge(df_final.reset_index(), df_seasonal, on=["hour","dayofyear"])
    df_final_merge.index.name = "time"
    #print(df_final_merge)
    if (start_date_str==None) or (end_date_str==None):
        
        return df_final_merge.set_index("index").drop(columns=["hour","dayofyear"]).sort_index()
    
    
    
def get_country_list():
    
    station_ids = get_station_ids()
    #print(station_ids)
    return station_ids["CountryName"].drop_duplicates()

station_id = get_station_ids()
#print(station_id)
station_id = station_id[['WMOId','Weighting','CountryName']]
station_id.dropna(axis=0, inplace=True)
station_id_list = station_id['WMOId'].to_list()
dfrangefull = pd.DataFrame()
#station_id_list=['6240']

dfrange = pd.DataFrame()


#print(df)
for i in station_id_list:
    print(i)
    try:
        df = get_station_data(i,['t_2m:C'], start_date=None, end_date=None)
        df = df.resample('D').mean()
        
        #print(df)
        dfrange[i] = df.loc[:,'2m Temp [C]']
    except Exception as e:
        print(i, e, station_id.loc[station_id[station_id['WMOId']==i].index,'CountryName'] )
        pass

station_id.dropna(axis=0, inplace=True)
#print(station_id)

for i in dfrange.columns:
    print(i)
    #print(station_id.loc[station_id[station_id['WMOId']==i].index,'Weighting'])
    try:
        dfrange[i] = dfrange[i]*station_id.loc[station_id[station_id['WMOId']==i].index,'Weighting'].values
        #print(station_id.loc[station_id[station_id['WMOId']==i].index,'CountryName'].values)
        dfrange.rename(columns={str(i):station_id.loc[station_id[station_id['WMOId']==i].index,'CountryName'].values[0]}, inplace=True)
        #print(dfnormal)
    except Exception as e:
        print(i, e, station_id.loc[station_id[station_id['WMOId']==i].index,'CountryName'] )
        pass

#print(dfnormal)
dfrange = dfrange.groupby(dfrange.columns, axis=1).sum()
dfrange = dfrange.loc[:'2022-12-31']
dfrangefull = pd.DataFrame(index = pd.date_range(start = '2020-01-01', end='2020-12-31'))

for i in dfrange.columns:
    dfrangeyoy = dfrange[[i]].copy()
    dfrangeyoy['year'] = dfrangeyoy.index.year
    dfrangeyoy['date'] = dfrangeyoy.index.strftime('%m-%d')
    dfrangeyoy = dfrangeyoy.set_index(['year','date'])[i].unstack(0)
    dfrangeyoy['max'] = dfrangeyoy.max(axis=1)
    dfrangeyoy['min'] = dfrangeyoy.min(axis=1)
    dfrangefull[i+' min'] = dfrangeyoy['min'].values
    dfrangefull[i+' max'] = dfrangeyoy['max'].values
print(dfrangefull)



       
dfrangefull.reset_index(inplace=True)    
db_server_lng = "PRD-DB-SQL-211"
db_name_lng = "LNG"
sql_engine_lng = sqlalchemy.create_engine("mssql+pyodbc://" + db_server_lng + "/" + db_name_lng + "?trusted_connection=yes&driver=ODBC+Driver+13+for+SQL+Server")

dfrangefull.to_sql(name='TempHistMinMax', con=sql_engine_lng, index=True, if_exists='replace', schema='ana')   

'''
dfnormal = pd.DataFrame(index = pd.date_range(start = '2020-01-01', end = '2020-12-31'))
#print(station_id.loc[station_id[station_id['WMOId']=='13615'].index,'Weighting'])
#station_id_list=['6240']

#print(df)
for i in station_id_list:
    print(i)
    try:
        df = get_station_data(i,['t_2m:C'], start_date=None, end_date=None)
        df = df.resample('D').mean()
        
        #print(df)
        dfnormal[i] = df.loc['2020-01-01':'2020-12-31','2m Temp 1991-2020 [C]']
    except Exception as e:
        print(i, e, station_id.loc[station_id[station_id['WMOId']==i].index,'CountryName'] )
        pass
#print(dfnormal)
station_id.dropna(axis=0, inplace=True)
#print(station_id)

for i in dfnormal.columns:
    print(i)
    #print(station_id.loc[station_id[station_id['WMOId']==i].index,'Weighting'])
    try:
        dfnormal[i] = dfnormal[i]*station_id.loc[station_id[station_id['WMOId']==i].index,'Weighting'].values
        #print(station_id.loc[station_id[station_id['WMOId']==i].index,'CountryName'].values)
        dfnormal.rename(columns={str(i):station_id.loc[station_id[station_id['WMOId']==i].index,'CountryName'].values[0]}, inplace=True)
        #print(dfnormal)
    except Exception as e:
        print(i, e, station_id.loc[station_id[station_id['WMOId']==i].index,'CountryName'] )
        pass

#print(dfnormal)
dfnormal = dfnormal.groupby(dfnormal.columns, axis=1).sum()


dfnormalfull = pd.DataFrame(index = pd.date_range(start = '2018-01-01', end='2028-12-31'), columns = dfnormal.columns)
for i in dfnormalfull.index:
    dfnormalfull.loc[i] = dfnormal.loc['2020'+'-'+str(i.month)+'-'+str(i.day)].values
print(dfnormalfull)
db_server_lng = "PRD-DB-SQL-211"
db_name_lng = "LNG"
sql_engine_lng = sqlalchemy.create_engine("mssql+pyodbc://" + db_server_lng + "/" + db_name_lng + "?trusted_connection=yes&driver=ODBC+Driver+13+for+SQL+Server")

dfnormalfull.to_sql(name='TempNormal', con=sql_engine_lng, index=True, if_exists='replace', schema='ana')
'''
'''  
SELECT * FROM WeatherStationTimeSeriesHistory
WHERE ParameterName = 't_2m:C' AND ModelSourceName = 'ecmwf-ens'
AND ForecastDate = '2023-02-03 00:00:00'
New
2:53
set the forecast date as todays date with time set as 00:00:00 or 12:00:00 and the same for yesterdays date
2:53
make sure you query the history WeatherStationTimeSeriesHistory not the latest table WeatherStationTimeSeriesLatest for these queries
'''