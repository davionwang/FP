
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

class temp_hist():
    def read_coordinate_data(latitude, longitude):
        
        latitude_rounded = round(latitude*4)/4
        longitude_rounded = round(longitude*4)/4
        
        try:
        
            ds = xr.open_dataset(r"\\prd-wth-app-01\WEATHER\Time_series\{}_{}.nc".format(latitude_rounded, longitude_rounded))
            
        except FileNotFoundError:
            
            try:
            
                ds = xr.open_dataset(r"\\prd-wth-app-01\WEATHER\Time_series\{}_{}.nc".format(latitude_rounded, int(longitude_rounded)))
                
            except FileNotFoundError:
                
                try:
                
                    ds = xr.open_dataset(r"\\prd-wth-app-01\WEATHER\Time_series\{}_{}.nc".format(int(latitude_rounded), longitude_rounded))
                    
                except FileNotFoundError:
                    
                    try:
                    
                        ds = xr.open_dataset(r"\\prd-wth-app-01\WEATHER\Time_series\{}_{}.nc".format(int(latitude_rounded), int(longitude_rounded)))
                        
                    except FileNotFoundError:
                        
                        try:
        
                            ds = xr.open_dataset(r"\\prd-wth-app-01\WEATHER\Time_series\{}_{}.nc".format(latitude_rounded, longitude_rounded))
                            
                        except FileNotFoundError:
                            
                            try:
                            
                                ds = xr.open_dataset(r"\\prd-wth-app-01\WEATHER\Time_series\{}_{}.nc".format(latitude_rounded, int(longitude_rounded)))
                                
                            except FileNotFoundError:
                                
                                try:
                                
                                    ds = xr.open_dataset(r"\\prd-wth-app-01\WEATHER\Time_series\{}_{}.nc".format(int(latitude_rounded), longitude_rounded))
                                    
                                except FileNotFoundError:
                                    
                                    ds = xr.open_dataset(r"\\prd-wth-app-01\WEATHER\Time_series\{}_{}.nc".format(int(latitude_rounded), int(longitude_rounded))) 
    
        return ds
    
    def get_station_ids():
        
        engine = sqlalchemy.create_engine(
                'mssql+pyodbc://' + "PRD-DB-SQL-211" + '/' + "Meteorology" + '?driver=ODBC+Driver+13+for+SQL+Server')
        
        
        station_ids = pd.read_sql_table("RefInternalMeteomaticCoordinatesLNGMarketsCurves", engine)    
        
        return station_ids
    
    def get_all_station_ids():
        
        engine = sqlalchemy.create_engine(
                'mssql+pyodbc://' + "PRD-DB-SQL-211" + '/' + "Meteorology" + '?driver=ODBC+Driver+13+for+SQL+Server')
        
        
        station_ids = pd.read_sql_table("RefMetDeskStationCatalogue", engine)    
        
        return station_ids
    
    def get_data(country, start_date_str=None, end_date_str=None):
        
        station_ids = temp_hist.get_station_ids()
        
        curve_list = []
        
        if country not in station_ids["country"].drop_duplicates().values:
            
            raise Exception(country+" not in list of countries stored in database")
        
        for index, row in station_ids[station_ids["country"]==country].iterrows():
            
            weighting = row[6]
            latitude = row[7]
            longitude = row[8]
            
            ds = temp_hist.read_coordinate_data(latitude, longitude)
            
            # print(ds, latitude, longitude)
            
            curve_list.append(weighting * ds)
            
        ds = sum(curve_list).load()
        
        try:
        
            df = ds.to_dataframe().dropna().droplevel("realization")
            
        except KeyError:
            
            try:
            
                df = ds.to_dataframe().dropna().drop(columns=["lat","lon","realization"])
                
            except KeyError:
                
                try:
                
                    df = ds.to_dataframe().dropna().drop(columns=["lat","realization"])
                    
                except KeyError:
                    
                    try:
                    
                        df = ds.to_dataframe().dropna().drop(columns=["lon","realization"])  
                        
                    except KeyError:
                        
                       df = ds.to_dataframe().dropna().drop(columns=["realization"])   
        
        ds.close()        
        
        df_final = pd.DataFrame()
        df_final["2m Temp [C]"] = df["tas"] - 273.15
        
        try:
        
            df_final["10m winds [m/s]"] = (df["uas"]**2 + df["vas"]**2)**0.5
            
        except KeyError:
            
            pass
        
        try:
            
            df_final["Solar Radiation [w/m2]"] = df["rss_accumulated"]
            
        except KeyError:
            
            pass
        
        df_final_1991_2020 = df_final[((df_final.index.year>1990) & (df_final.index.year<2021))]
        
        seasonal_list = []
        
        for day in list(df_final_1991_2020.groupby(df_final_1991_2020.index.dayofyear)):
        
            seasonal_list.append(day[1].groupby(day[1].index.hour).mean())
    
        df_seasonal = pd.concat(seasonal_list).reset_index()
        df_seasonal["dayofyear"] = np.repeat(np.arange(1,367),24)
        
        df_seasonal = df_seasonal.rename(columns={"time":"hour","2m Temp [C]":"2m Temp 1991-2020 [C]","10m winds [m/s]":"10m winds 1991-2020 [m/s]", "Solar Radiation [w/m2]":"Solar Radiation [w/m2] 1991-2020"})
        
        df_final["hour"] = df_final.index.hour
        df_final["dayofyear"] = df_final.index.dayofyear
        
        df_final_merge = pd.merge(df_final.reset_index(), df_seasonal, on=["hour","dayofyear"])
        
        if (start_date_str==None) or (end_date_str==None):
            
            return df_final_merge.set_index("time").drop(columns=["hour","dayofyear"]).sort_index()
            
        else:
        
            return df_final_merge.set_index("time").drop(columns=["hour","dayofyear"]).sort_index()[pd.to_datetime(start_date_str):pd.to_datetime(end_date_str)]
    
    def get_station_data(station_id):
        
        station_id = str(station_id).upper()
        
        station_ids = temp_hist.get_all_station_ids()
        
        if station_id in station_ids["wmo"].drop_duplicates().values:
            
            latitude = float(station_ids[station_ids["wmo"]==station_id]["latitude"])
            longitude = float(station_ids[station_ids["wmo"]==station_id]["longitude"])   
            
        elif station_id in station_ids["name"].drop_duplicates().values:
        
            latitude = float(station_ids[station_ids["name"]==station_id]["latitude"])
            longitude = float(station_ids[station_ids["name"]==station_id]["longitude"]) 
                                          
        else:
            
            raise Exception(station_id+" not listed in database")
    
        ds = temp_hist.read_coordinate_data(latitude, longitude)
        
        try:
        
            df = ds.to_dataframe().dropna().droplevel("realization")
            
        except KeyError:
            
            try:
            
                df = ds.to_dataframe().dropna().drop(columns=["lat","lon","realization"])
                
            except KeyError:
                
                try:
                
                    df = ds.to_dataframe().dropna().drop(columns=["lat","realization"])
                    
                except KeyError:
                    
                    try:
                    
                        df = ds.to_dataframe().dropna().drop(columns=["lon","realization"])  
                        
                    except KeyError:
                        
                       df = ds.to_dataframe().dropna().drop(columns=["realization"])   
                
        df_final = pd.DataFrame()
        df_final["2m Temp [C]"] = df["tas"] - 273.15
        
        try:
        
            df_final["10m winds [m/s]"] = (df["uas"]**2 + df["vas"]**2)**0.5
            
        except KeyError:
            
            pass
        
        df_final_1991_2020 = df_final[((df_final.index.year>1990) & (df_final.index.year<2021))]
        
        seasonal_list = []
        
        for day in list(df_final_1991_2020.groupby(df_final_1991_2020.index.dayofyear)):
        
            seasonal_list.append(day[1].groupby(day[1].index.hour).mean())
    
        df_seasonal = pd.concat(seasonal_list).reset_index()
        df_seasonal["dayofyear"] = np.repeat(np.arange(1,367),24)
        
        df_seasonal = df_seasonal.rename(columns={"time":"hour","2m Temp [C]":"2m Temp 1991-2020 [C]","10m winds [m/s]":"10m winds 1991-2020 [m/s]"})
        
        df_final["hour"] = df_final.index.hour
        df_final["dayofyear"] = df_final.index.dayofyear
        
        df_final_merge = pd.merge(df_final.reset_index(), df_seasonal, on=["hour","dayofyear"])
        
        return df_final_merge.set_index("time").drop(columns=["hour","dayofyear"]).sort_index()
        
    def get_country_list():
        
        station_ids = temp_hist.get_station_ids()
        
        return station_ids["country"].drop_duplicates()
    
#print(temp_hist.get_country_list())