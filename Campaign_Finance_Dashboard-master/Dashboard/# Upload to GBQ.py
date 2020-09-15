# Upload to GBQ
data = merged_dff
projectName ='mfor24'

# Set client to set up configuration needed for API request / pass in project ID for which the client acts on behalf of
client = bigquery.Client(project=projectName)
client._credentials
dataset_id = projectName+'.central_voter_data'
table_id = dataset_id+'.voters_test'


# Configuration option to load job
job_config = bigquery.LoadJobConfig()
job_config.source_format = bigquery.SourceFormat.CSV
job_config.autodetect = True #auto-schema
job_config.write_disposition = "WRITE_TRUNCATE"

"""
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
bigquery.SchemaField("latitude", "STRING"),

bigquery.SchemaField("longitude", "STRING"),
]"""



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

for x in merged_dff['latitude']:
    print(x, type(x))