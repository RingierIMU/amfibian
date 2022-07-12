import json
import boto3
from botocore.vendored import requests
import awswrangler as wr
from datetime import datetime

def main():

    #TODO : Make tables etc dynamic

    database = 'database'
    output_table = 'output_table'
    output_s3_location = "'s3://output_s3_location_staging'"
    output_s3_storage_format = "'Parquet'" # "'TEXTFILE'"  # "'Parquet'"

    create_query = """ CREATE TABLE table
    (   column_name datatype
		) LOCATION 's3://s3location for table'"""
    
    insert_query = """INSERT INTO table """
    
    select_query = """SELECT table_name """ 

    wr.athena.read_sql_query(sql="DROP TABLE IF EXISTS " + database +"." + output_table + ";", database=database, ctas_approach=False)
    wr.athena.read_sql_query(sql=create_query, database=database, ctas_approach=False)
    wr.athena.read_sql_query(sql=insert_query + select_query, database=database, ctas_approach=False)
    
def lambda_handler(event, context):
    main()

    return {
        'statusCode': 200,
        'body': json.dumps('Successful')
    }
