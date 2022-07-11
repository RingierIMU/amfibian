CREATE EXTERNAL TABLE `db_changelog`(
  `vertical` string, 
  `table_name` string, 
  `column_name` string, 
  `change_type` string, 
  `datatype` string, 
  `created_at` timestamp, 
  `updated_at` timestamp)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://playweek/tables/dbchangelog'