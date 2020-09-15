# -*- coding: utf-8 -*-
"""
Created on Wed Aug 26 11:12:11 2020
"""

import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from geopy.geocoders import Nominatim
import json
import pandas_gbq
import time
from google.colab import auth
auth.authenticate_user()
print('Authenticated')

startTime = time.time()

# 1. Function to Import Data

## This function returns a dataframe that is imported from Google BQ
def GetDatafromGBQ(credentials_file, project_id, folder, limit):
    credentials = service_account.Credentials.from_service_account_file(credentials_file)

    project_id = project_id

    if limit > 0:
        query = "SELECT * FROM" + ' ' + str(project_id) + '.' +  str(folder) + ' ' +  "LIMIT " + str(limit)
    else:
        query = "SELECT * FROM" + ' ' + str(project_id) + '.' + str(folder)

    return pandas_gbq.read_gbq(query, project_id= project_id, credentials= credentials)


# 2. Import Data as Pandas DataFrames

## Import Campaign Finance Data
#merged_dff = GetDatafromGBQ('/content/GCP_M24_ServiceAccount.json', 'mfor24', 'central_voter_data.central_contributions', 10)
merged_dff = pandas_gbq.read_gbq("SELECT * FROM mfor24.central_voter_data.central_contributions LIMIT 10", project_id = "mfor24")

# Edited version of "address" (see Yash/Peter's previous code from app.py)
clean_address = merged_dff['RADDNUMBER'].astype(str) + ' ' + merged_dff['RSTREETNAME'].astype(str) + ' ' + merged_dff['RCITY'].astype(str) + ' ' + merged_dff['RZIP5'].astype(str)
merged_dff['clean_address'] = clean_address

# API-generated LAT/LONG tuple based on clean_address
geolocator = Nominatim(user_agent="editable.py")
def getLocation(addy):
    return geolocator.geocode(addy)
def getLong(location):
    return location.longitude

def getLat(location):
    return location.latitude
def getLongLatAddy(df, address_column):
  lat = []
  long = []
  i = 0
  for each_address in df[address_column]:
    i+=1
    print(i , each_address)


    location = getLocation(each_address)

    try:
      lat.append(getLat(location))
      long.append(getLong(location))
    except AttributeError:
      lat.append(0)
      long.append(0)
  return lat, long

latitude, longitude = getLongLatAddy(merged_dff, 'clean_address')

merged_dff['latitude'] = latitude
merged_dff['longitude'] = longitude

print("Dtypes former conversion: {}".format(merged_dff.info()))
print(merged_dff.info())

merged_dff['latitude'] = merged_dff['latitude'].astype(float)
merged_dff['longitude'] = merged_dff['longitude'].astype(float)

print("Dtypes after conversion: {}".format(merged_dff.info()))
#merged_dff = merged_dff.set_index('SBOEID')


# Upload to GBQ
data = merged_dff
projectName ='mfor24'

# Set client to set up configuration needed for API request / pass in project ID for which the client acts on behalf of
client = bigquery.Client(project=projectName)
client._credentials
dataset_id = projectName+'.central_voter_data'
table_id = dataset_id+'.central_contributions_lat_long'


# Configuration option to load job
job_config = bigquery.LoadJobConfig()
job_config.source_format = bigquery.SourceFormat.CSV
job_config.autodetect = False #auto-schema
job_config.write_disposition = "WRITE_TRUNCATE"


job_config.schema = [
bigquery.SchemaField("CONTRID", "STRING"),
bigquery.SchemaField("ELECTION", "STRING"),
bigquery.SchemaField("OFFICECD", "STRING"),
bigquery.SchemaField("RECIPID", "STRING"),
bigquery.SchemaField("CANCLASS", "STRING"),

bigquery.SchemaField("RECIPNAME", "STRING"),
bigquery.SchemaField("COMMITTEE", "STRING"),
bigquery.SchemaField("FILING", "STRING"),
bigquery.SchemaField("SCHEDULE", "STRING"),
bigquery.SchemaField("PAGENO", "STRING"),

bigquery.SchemaField("SEQUENCENO", "STRING"),
bigquery.SchemaField("REFNO", "STRING"),
bigquery.SchemaField("DATE", "STRING"),
bigquery.SchemaField("REFUNDDATE", "STRING"),
bigquery.SchemaField("NAME", "STRING"),

bigquery.SchemaField("C_CODE", "STRING"),
bigquery.SchemaField("STRNO", "STRING"),
bigquery.SchemaField("STRNAME", "STRING"),
bigquery.SchemaField("APARTMENT", "STRING"),
bigquery.SchemaField("BOROUGHCD", "STRING"),

bigquery.SchemaField("CITY", "STRING"),
bigquery.SchemaField("ZIP", "STRING"),
bigquery.SchemaField("OCCUPATION", "STRING"),
bigquery.SchemaField("EMPNAME", "STRING"),
bigquery.SchemaField("EMPSTRNO", "STRING"),

bigquery.SchemaField("EMPSTRNAME", "STRING"),
bigquery.SchemaField("EMPCITY", "STRING"),
bigquery.SchemaField("EMPSTATE", "STRING"),
bigquery.SchemaField("AMNT", "STRING"),
bigquery.SchemaField("MATCHAMNT", "STRING"), #

bigquery.SchemaField("INT_C_CODE", "STRING"), #
bigquery.SchemaField("SEG_IND", "STRING"),
bigquery.SchemaField("RR_IND", "STRING"),
bigquery.SchemaField("ADJTYPECD", "STRING"), #
bigquery.SchemaField("EXEMPTCD", "STRING"), #

bigquery.SchemaField("PURPOSECD", "STRING"), #
bigquery.SchemaField("INTOCCUPA", "STRING"), #
bigquery.SchemaField("INTEMPST", "STRING"), #
bigquery.SchemaField("INTEMPCITY", "STRING"),
bigquery.SchemaField("INTEMPSTNM", "STRING"),

bigquery.SchemaField("INTEMPSTNO", "STRING"),
bigquery.SchemaField("INTEMPNAME", "STRING"), #
bigquery.SchemaField("INTZIP", "STRING"), #
bigquery.SchemaField("INTST", "STRING"), #
bigquery.SchemaField("INTCITY", "STRING"),
bigquery.SchemaField("INTAPTNO", "STRING"),

bigquery.SchemaField("INTSTRNM", "STRING"),
bigquery.SchemaField("INTSTRNO", "STRING"), #
bigquery.SchemaField("INTERMNAME", "STRING"), #
bigquery.SchemaField("INTERMNO", "STRING"), #
bigquery.SchemaField("PAY_METHOD", "STRING"),
bigquery.SchemaField("PREVAMNT", "STRING"),

bigquery.SchemaField("Election_District", "STRING"),
bigquery.SchemaField("ELECTION_YEAR", "STRING"), #
bigquery.SchemaField("ELECTION_CODE", "STRING"), #
bigquery.SchemaField("Winner", "STRING"), #
bigquery.SchemaField("sboeid", "STRING"),
bigquery.SchemaField("CONTRIBUTION_ID", "STRING"),

bigquery.SchemaField("RADDNUMBER", "STRING"),
bigquery.SchemaField("RAPARTMENT", "STRING"), #
bigquery.SchemaField("RSTREETNAME", "STRING"), #
bigquery.SchemaField("RCITY", "STRING"), #
bigquery.SchemaField("RZIP5", "STRING"),
bigquery.SchemaField("DOB", "STRING"),

bigquery.SchemaField("GENDER", "STRING"),
bigquery.SchemaField("ENROLLMENT", "STRING"), #
bigquery.SchemaField("OTHERPARTY", "STRING"), #
bigquery.SchemaField("COUNTYCODE", "STRING"), #
bigquery.SchemaField("ED", "STRING"),
bigquery.SchemaField("LD", "STRING"),

bigquery.SchemaField("CD", "STRING"),
bigquery.SchemaField("TOWNCITY", "STRING"), #
bigquery.SchemaField("SD", "STRING"), #
bigquery.SchemaField("AD", "STRING"), #
bigquery.SchemaField("STATUS", "STRING"),
bigquery.SchemaField("latitude", "FLOAT64"),
bigquery.SchemaField("longitude", "FLOAT64"),
]



# Create and upload database
client.create_dataset(dataset_id,exists_ok=True)
table = client.create_table(table_id,exists_ok=True)

job = client.load_table_from_dataframe(data, table, job_config=job_config)
job.result()

endTime = time.time()

rowCountReport = "Loaded {} rows into {}:{}.".format(job.output_rows, dataset_id, table_id)
timeUsed=round(endTime-startTime,2)
timeReport = "It take {} s to complete".format(timeUsed)
print(timeReport+rowCountReport)
