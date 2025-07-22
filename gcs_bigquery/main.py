from google.cloud import bigquery


def trigger_bigquery_load(event, context):
    # Name and path of the uploaded file
    bucket_name = event['bucket']
    file_name = event['name']
    
    print(f"New file: gs://{bucket_name}/{file_name}")

    # Set the target tables
    if file_name.startswith("summary_users/"):
        table_id = "glamira-data-project.glamira_elt.summary_users"
    elif file_name.startswith("ip2location/"):
        table_id = "glamira-data-project.glamira_elt.ip2location"
    elif file_name.startswith("product_names/"):
        table_id = "glamira-data-project.glamira_elt.product_name"
    else:
        print("No matching table for this file path.")
        return

    uri = f"gs://{bucket_name}/{file_name}"
    
    # Bigquery client
    client = bigquery.Client()
    
    # JobConfig: append Parquet files from the source to Bigquery
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )

    # Make an API request
    load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)
    load_job.result()  # wait for the job to complete

    destination_table = client.get_table(table_id)
    print("Loaded {} rows.".format(destination_table.num_rows))
