#connect to Google Cloud Storage
import gcsfs
import pandas as pd

from google.cloud import storage
from google.oauth2 import service_account

#authenticate_user
def Authenticate(credentials_file)
    credentials = service_account.Credentials.from_service_account_file(credentials_file)
    print('Authenticated')
    return credentials

##This function returns a dataframe that is imported from Google BQ
def GetDatafromGBQ(credentials_file, project_id, folder, limit):
    """
    This function imports any data from GBQ services as a pandas dataframe

    ===================================
    INPUTS:
    credentials_file: a json file or any such string that has the credentials key for GBQ access
    project_id: the project name in GBQ where the data is located
    folder: the location (data_set and table_name)
    limit: the total number of rows that user wants to import
    """
    credentials = Authenticate(credentials_file)

    project_id = project_id

    if limit > 0:
        query = "SELECT * FROM" + ' ' + str(project_id) + '.' +  str(folder) + ' ' +  "LIMIT " + str(limit)
    else:
        query = "SELECT * FROM" + ' ' + str(project_id) + '.' + str(folder)

    return pandas_gbq.read_gbq(query, project_id= project_id, credentials= credentials)

##Imports data from Google Cloud Storage Buckets into a pandas DataFrame
def getBucket(bucket_name, file_name_and_type):
  """
  This function downloads data from Google Cloud Storage Buckets as a data frame

  *****PLEASE NOTE, MUST AUTHENTICATE ACCOUNT FIRST*****

  =========================================================
  INPUTS:

  bucket_name: name of bucket user wants to pull data from
  file_name_and_type: The name of the file and the type (exl, csv, etc.) that needs to be imported
    --> EX: "test_data.csv"

  OUTPUTS:

  a Pandas DataFrame that is populated by google storage data
  """
  start = 'gs://'
  full_loc = start + str(bucket_name) + '/' + str(file_name_and_type)
  return pd.read_csv(full_loc)


##Uploas desired DataFrame into a Google Cloud Storage Bucket
def loadBucket(data_frame, project_name, bucket, file_name):
  """
  Takes a data frame that was created in python and uploads it into a google cloud storage bucket.

  ===========================================================
  INPUTS:

  data_frame: the data frame you would like to save into GCP
  project_name: the project name where data should be stored
  bucket: bucket name where data should be stored
  file_name: the name of the data_frame the user wants to set

  OUTPUTS:

  the Output is a new file in google cloud storage in the specified project/bucket/file
  """
  csv_file_name = str(file_name) + '.csv'
  data_frame.to_csv(csv_file_name)
  client = storage.Client(project = str(project_name))
  bucket = client.get_bucket(str(bucket))
  blob = bucket.blob(csv_file_name)
  blob.upload_from_filename(csv_file_name)
