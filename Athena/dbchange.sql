--dbchange and dbchange staging
CREATE EXTERNAL TABLE `dbchange`(
  `table_name` string, 
  `column_name` string, 
  `column_datatype` string, 
  `column_nullable` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://playweek/tables/dbchange'