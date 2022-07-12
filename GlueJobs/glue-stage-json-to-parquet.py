import sys

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, TimestampType
from pyspark.sql import *
from pyspark.sql import functions as f, types as t

args = getResolvedOptions(sys.argv, ["TempDir", "JOB_NAME"])
sc = SparkContext()
sqlContext = SQLContext(sc)

glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
job_name = args["JOB_NAME"]

try:
    _meta_verticals = {
            # Example :
            # "brk": "s3://location/...json",
        }

    for _vertical, _paths in _meta_verticals.items():
     
        _vertical = _vertical.replace("-", "_")

        df = spark.read.option("multiline", "true").json(_paths)

        _stage_bucket = "destination_bucket"
        _stage_prefix = "destination_folder"
        print(df.printSchema())
        print(df.show(5, False))

        # write data to s3
        sources_write_path = (
            f"s3://{_stage_bucket}/{_stage_prefix}_sources_created_date/"
        )
        print(f"Writing data_frame to: {sources_write_path}.")

        max_records_per_file = 200000
        partitions = ["year", "month", "day"]

        df.write.option(
            "maxRecordsPerFile", max_records_per_file
        ).parquet(
            f"s3://{_stage_bucket}/{_stage_prefix}/{_vertical}/",
            mode="overwrite",
        )
        print("Done writing")
except Exception as err:
    print(err)
