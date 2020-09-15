# -*- coding: utf-8 -*-
"""
Created on Thu Aug 13 15:29:48 2020

@author: Yasaswi Pisupati
"""

import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas_gbq
from geopy.geocoders import Nominatim
import numpy as np
import plotly.express as px
from plotly.offline import plot

credentials = service_account.Credentials.from_service_account_file('C:/Users/Yasaswi Pisupati/Desktop/Yasaswi/NY Strat Mrkt Sizing/GCP_M24_ServiceAccount.json')
project_id = "mfor24"
query = """
SELECT * FROM `mfor24.nys_foil.nys_foil_original` LIMIT 1000
"""
sm_df = pandas_gbq.read_gbq(query, project_id= project_id, credentials= credentials)

def CleanData(DF, COL, LIMIT):
    empthy_list = []
    for each_item in DF[COL]: 
        if len(each_item) > LIMIT:
            empthy_list.append(each_item)
    
    return empthy_list

sm_dff = sm_df[sm_df['RADDNUMBER'].isin(CleanData(sm_df, 'RADDNUMBER', 0))]
sm_dff = sm_dff[sm_dff['RSTREETNAME'].isin(CleanData(sm_df, 'RSTREETNAME', 1))]
sm_dff = sm_dff[sm_dff['RCITY'].isin(CleanData(sm_df,'RCITY', 5))]
sm_dff = sm_dff[sm_dff['RZIP5'].isin(CleanData(sm_df,'RZIP5',4))]


sm_dff['ADDY'] = sm_dff['RADDNUMBER'].astype(str) + ' ' + sm_dff['RSTREETNAME'].astype(str) + ' ' + sm_dff['RCITY'].astype(str) + ' ' + sm_dff['RZIP5'].astype(str)

print(sm_df.head())
geolocator = Nominatim(user_agent="editable.py")

def getLocation(addy):
    return geolocator.geocode(addy)

def getLong(location):
    return location.longitude
    
def getLat(location):
    return location.latitude

def getLongLatAddy(df, address_column):
    index = 0
    lat = []
    long = []
    lat_long_index = []
    
    for each_address in df[address_column]:
        location = getLocation(each_address)    
        
        try:
            lat.append(getLat(location))
            long.append(getLong(location))
            lat_long_index.append(index)
        except AttributeError:
            lat.append('')
            long.append('')
            lat_long_index.append(index)
        
        index += 1
    
    return lat, long, lat_long_index

lat_column, long_column, indexes_list = getLongLatAddy(sm_dff, 'ADDY')
        
sm_dff['LAT'] = lat_column
sm_dff['LONG'] = long_column


sm_dff = sm_dff.drop(columns = ['lat', 'long'])
sm_dff[sm_dff['LAT'].fillna(0) != 0]
 

fig = px.scatter_geo(data_frame = sm_dff, lat = 'LAT', lon = 'LONG', scope = 'usa')
plot(fig)