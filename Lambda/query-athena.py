import json
import boto3
from botocore.vendored import requests
import awswrangler as wr
from datetime import datetime

def main():

    #TODO : Make tables etc dynamic

    database = 'playweekdemo'
    output_table = 'dbchange_staging'
    output_s3_location = "'s3://playweek/dbchange_staging'"
    output_s3_storage_format = "'Parquet'" # "'TEXTFILE'"  # "'Parquet'"

    create_query = """ CREATE TABLE playweekdemo.dbchange_staging
    (    table_name VARCHAR(255),
	    column_name VARCHAR(45),
	    column_datatype VARCHAR(255),
	    column_nullable CHAR(5),
	    created_at TIMESTAMP,
	    updated_at TIMESTAMP
		) LOCATION 's3://playweek/dbchange_staging'"""
    
    insert_query = """INSERT INTO dbchange_staging(table_name, column_name, column_datatype, column_nullable)
    SELECT table_name,
    column_name,
    data_type,
    is_nullable
    FROM information_schema.columns
    WHERE table_schema = 'playweekdemo'
    AND table_name IN ( 'countries','people','third_table');
    
      INSERT INTO playweekdemo.db_changelog (
        table_name,
        column_name,
        change_type,
        datatype)
    SELECT staging.table_name,
        staging.column_name,
        'New Column',
        staging.column_datatype
    FROM dbchange_staging staging
    LEFT JOIN dbchange live ON staging.table_name = live.table_name
        AND staging.column_name = live.column_name
        AND staging.column_datatype = live.column_datatype
        AND staging.column_nullable = live.column_nullable
    WHERE live.column_name IS NULL;
    """ 

    #wr.athena.read_sql_query(sql="DROP TABLE IF EXISTS " + database +"." + output_table + ";", database=database, ctas_approach=False)
    #wr.athena.read_sql_query(sql=create_query, database=database, ctas_approach=False)
    wr.athena.read_sql_query(sql=insert_query, database=database, ctas_approach=False)
    
def lambda_handler(event, context):
    main()

    return {
        'statusCode': 200,
        'body': json.dumps('Successful')
    }
