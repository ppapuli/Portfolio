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
dfinance = GetDatafromGBQ('GCP_M24_ServiceAccount.json',
                          'mfor24', 'campaign_finance.contributions', 100) # for limits, add before ): ", 1000"

## Import FOIL Data
dfoil = GetDatafromGBQ('GCP_M24_ServiceAccount.json',
                       'mfor24', 'nys_foil.foil_voters', 100)

## Import Campaign Finance + FOIL Merged View Data
merged = GetDatafromGBQ('GCP_M24_ServiceAccount.json',
                        'mfor24', 'campaign_finance.contributor_SBOEID', 100) # contributor & foil datasets


# Pulled Straight from FOIL:
SBOEID = dfoil['SBOEID']
LAST = dfoil['LASTNAME']
FIRST = dfoil['FIRSTNAME']
MIDDLE = dfoil['MIDDLENAME']
SUFFIX = dfoil['NAMESUFFIX']

add_number = dfoil['RADDNUMBER']
add_apt_no = dfoil['RAPARTMENT']
add_street = dfoil['RSTREETNAME']
add_city = dfoil['RCITY']
add_zip = dfoil['RZIP5']

dob = dfoil['DOB']
gender = dfoil['GENDER']
party = dfoil['ENROLLMENT']
other_party = dfoil['OTHERPARTY']
COUNTYCODE = dfoil['COUNTYCODE']

ED = dfoil['ED']
LD = dfoil['LD']
TOWNCITY = dfoil['TOWNCITY']
WARD = dfoil['WARD']
CD = dfoil['CD']

SD = dfoil['SD']
AD = dfoil['AD']
LASTVOTEDDATE = dfoil['LASTVOTEDDATE']
PREVYEARVOTED = dfoil['PREVYEARVOTED']
REGDATE = dfoil['REGDATE']

VRSOURCE = dfoil['VRSOURCE']
STATUS = dfoil['STATUS']
REASONCODE = dfoil['REASONCODE']
PURGE_DATE = dfoil['PURGE_DATE']

## Compile lists into new DF called "df"
data = [SBOEID, LAST, FIRST, MIDDLE, SUFFIX, add_number, add_apt_no, add_street, add_city, add_zip, dob, gender, party, other_party, COUNTYCODE,
        ED, LD, TOWNCITY, WARD, CD, SD, AD, LASTVOTEDDATE, PREVYEARVOTED, REGDATE, VRSOURCE, STATUS, REASONCODE, PURGE_DATE]
columns = ['SBOEID', 'LAST', 'FIRST', 'MIDDLE', 'SUFFIX', 'add_number', 'add_apt_no', 'add_street', 'add_city', 'add_zip', 'dob', 'gender', 'party', 'other_party', 'COUNTYCODE', 'ED', 'LD', 'TOWNCITY', 'WARD', 'CD', 'SD', 'AD', 'LASTVOTEDDATE', 'PREVYEARVOTED', 'REGDATE', 'VRSOURCE', 'STATUS', 'REASONCODE', 'PURGE_DATE']


# Collect first set of data into one data table // Add columns moving forward
df = pd.concat(data, axis=1)

cols = {k:v for (k,v) in zip(df.columns, columns)}
df = df.rename(columns=cols)

merged_df = df.merge(merged[['CONTRID', 'TOTALAMNT', 'OCCUPATION', 'EMPNAME', 'SBOEID']], on = 'SBOEID', how = 'left')

total_contribution_amt = dfinance.groupby(['CONTRID','NAME'], as_index = False).agg({'AMNT':'sum'})

total_number_of_contributions = dfinance.groupby(['CONTRID', 'NAME'], as_index = False).agg({'AMNT':'count'})

avg_contribution_amt = total_contribution_amt['AMNT']/total_number_of_contributions['AMNT'] #pandas series

df_individual_contributions = dfinance.groupby(['CONTRID','NAME', 'DATE'], as_index = False).agg({'AMNT':'sum'})

max_contribution_amt = [] #this
min_contribution_amt = [] #this
contr_id = []

for each_contributor in df_individual_contributions['CONTRID'].unique():
    filtered_df = df_individual_contributions[df_individual_contributions['CONTRID'] == each_contributor]
    max_contribution_amt.append(max(filtered_df['AMNT']))
    min_contribution_amt.append(min(filtered_df['AMNT']))
    contr_id.append(each_contributor)

fake_df = pd.DataFrame({'max':max_contribution_amt, 'min':min_contribution_amt, 'CONTRID':contr_id})

total_contribution_amt.to_csv('total_contribution_amt.csv')

merged_df['NAME'] = merged_df['LAST'].astype(str) + ' ' + merged_df['FIRST'] + ' ' + merged_df['MIDDLE']
total_contribution_amt.rename(columns = {'AMNT':'total_amt_per_contributor'})
total_number_of_contributions = total_number_of_contributions.rename(columns = {'AMNT':'total_num_contrubutions'})

total_contribution_amt['num_contributions'] = total_number_of_contributions['total_num_contrubutions']
total_contribution_amt['Avg_contributions'] = avg_contribution_amt
total_contribution_amt = total_contribution_amt.merge(fake_df, on = 'CONTRID',how = 'left')

# DataFrame.merge(right, how='inner', on=None, left_on=None, right_on=None, left_index=False, right_index=False, sort=False, suffixes='_x', '_y', copy=True, indicator=False, validate=None)[source]
merged_dff = merged_df.merge(total_contribution_amt, on = ['CONTRID', 'NAME'], how = 'left')
merged_dff = merged_dff.drop(columns='NAME')

# Edited version of "address" (see Yash/Peter's previous code from app.py)
clean_address = merged_dff['add_number'].astype(str) + ' ' + merged_dff['add_apt_no'].astype(str) + ' ' + merged_dff['add_street'].astype(str) + ' ' + merged_dff['add_city'].astype(str) + ' ' + merged_dff['add_zip'].astype(str)
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
    
    for each_address in df[address_column]:
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
table_id = dataset_id+'.central_voters_'


# Configuration option to load job
job_config = bigquery.LoadJobConfig()
job_config.source_format = bigquery.SourceFormat.CSV
job_config.autodetect = False #auto-schema
job_config.write_disposition = "WRITE_TRUNCATE"


job_config.schema = [
bigquery.SchemaField("SBOEID", "STRING"),
bigquery.SchemaField("LAST", "STRING"),
bigquery.SchemaField("FIRST", "STRING"),
bigquery.SchemaField("MIDDLE", "STRING"),
bigquery.SchemaField("SUFFIX", "STRING"),

bigquery.SchemaField("add_number", "STRING"),
bigquery.SchemaField("add_apt_no", "STRING"),
bigquery.SchemaField("add_street", "STRING"),
bigquery.SchemaField("add_city", "STRING"),
bigquery.SchemaField("add_zip", "STRING"),

bigquery.SchemaField("dob", "STRING"),
bigquery.SchemaField("gender", "STRING"),
bigquery.SchemaField("party", "STRING"),
bigquery.SchemaField("other_party", "STRING"),
bigquery.SchemaField("COUNTYCODE", "STRING"),

bigquery.SchemaField("ED", "STRING"),
bigquery.SchemaField("LD", "STRING"),
bigquery.SchemaField("TOWNCITY", "STRING"),
bigquery.SchemaField("WARD", "STRING"),
bigquery.SchemaField("CD", "STRING"),

bigquery.SchemaField("SD", "STRING"),
bigquery.SchemaField("AD", "STRING"),
bigquery.SchemaField("LASTVOTEDDATE", "STRING"),
bigquery.SchemaField("PREVYEARVOTED", "STRING"),
bigquery.SchemaField("REGDATE", "STRING"),

bigquery.SchemaField("VRSOURCE", "STRING"),
bigquery.SchemaField("STATUS", "STRING"),
bigquery.SchemaField("REASONCODE", "STRING"),
bigquery.SchemaField("PURGE_DATE", "STRING"),
bigquery.SchemaField("CONTRID", "STRING"), #

bigquery.SchemaField("TOTALAMNT", "STRING"), #
bigquery.SchemaField("OCCUPATION", "STRING"),
bigquery.SchemaField("EMPNAME", "STRING"),
bigquery.SchemaField("AMNT", "STRING"), #
bigquery.SchemaField("num_contributions", "STRING"), #

bigquery.SchemaField("Avg_contributions", "STRING"), #
bigquery.SchemaField("max", "STRING"), #
bigquery.SchemaField("min", "STRING"), #
bigquery.SchemaField("clean_address", "STRING"),
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

