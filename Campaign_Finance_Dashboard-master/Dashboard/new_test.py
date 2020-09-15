import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from geopy.geocoders import Nominatim
import json
import pandas_gbq

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
dfinance = GetDatafromGBQ('GCP_M24_ServiceAccount.json', 'mfor24', 'campaign_finance.contributions', 100)

## Import FOIL Data
dfoil = GetDatafromGBQ('GCP_M24_ServiceAccount.json', 'mfor24', 'nys_foil.nys_foil_original', 100)

## Import Campaign Finance + FOIL Merged View Data
merged = GetDatafromGBQ('GCP_M24_ServiceAccount.json', 'mfor24', 'campaign_finance.contributor_SBOEID', 100)

# 3. Prepare Columns One by One
"""
Columns:
0. Voter_id (SBOEID) - FOIL / Voters (nys_foil_original)
1. Name - FOIL / Voters (nys_foil_original)
2. Party Affiliation - FOIL / Voters (nys_foil_original)
3. Occupation - Campaign Finance Data / Contributors
4. Address - FOIL
5. Clean Address - Properly formatted for Lat / Long
6. Lat / Long - API based on Address
7. Phone Number
8. Total Contribution Amount - Campaign Finance Data
9. Total Number of Contributions - Campaign Finance Data
10. Average Amount of Contributions - Campaign Finance Data
11. Max Contribution - Campaign Finance Data
"""

## Import Initial Datapoints from FOIL

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
data = [sboeid, last, first, middle, suffix, add_number, add_apt_no, add_street, add_city, add_zip, dob, gender, party, other_party, COUNTYCODE,
                    ED, LD, TOWNCITY, WARD, CD, SD, AD, LASTVOTEDDATE, PREVYEARVOTED, REGDATE, VRSOURCE, STATUS, REASONCODE, PURGE_DATE]
columns = ['sboeid', 'last', 'first', 'middle', 'suffix', 'add_number', 'add_apt_no', 'add_street', 'add_city', 'add_zip', 'dob', 'gender', 'party', 'other_party', 'COUNTYCODE', 'ED', 'LD', 'TOWNCITY', 'WARD', 'CD', 'SD', 'AD', 'LASTVOTEDDATE', 'PREVYEARVOTED', 'REGDATE', 'VRSOURCE', 'STATUS', 'REASONCODE', 'PURGE_DATE']


# Collect first set of data into one data table // Add columns moving forward
df = pd.concat(data, axis=1)

cols = {k:v for (k,v) in zip(df.columns, columns)}
df = df.rename(columns=cols)


# Edited version of "address" (see Yash/Peter's previous code from app.py)
clean_address = df['add_number'].astype(str) + ' ' + df['add_apt_no'].astype(str) + ' ' + df['add_street'].astype(str) + ' ' + df['add_city'].astype(str) + ' ' + df['add_zip'].astype(str)
df['clean_address'] = clean_address

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
            lat.append('')
            long.append('')
    return lat, long

latitude, longitude = getLongLatAddy(df, 'clean_address')

df['latitude'] = latitude
df['longitude'] = longitude


######
# Pulled from Campaign Finance with a join on voter_id:
contributor_id = dfinance['CONTRID']
occupation = ['OCCUPATION']
employer = ['EMPLOYER']

# Where do we have this?
"""
phone = 

# Calculated Fields from Campaign Finance data joined by voter_id
total_contribution_amt = 
total_number_of_contributions = 
avg_contribution_amt = 
max_contribution_amt = 


# 4. Stitch the columns together into one dataset (to upload to GCB as a single table)

# 5. Upload to GBQ as a new Dataset
# """


preview = df.sample(100)
df.to_csv('test.csv')