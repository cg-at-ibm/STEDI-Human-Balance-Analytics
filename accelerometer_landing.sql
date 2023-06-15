CREATE EXTERNAL TABLE `stedi`.`accelerometer_landing`(
  `user` string COMMENT '', 
  `timestamp` bigint COMMENT '', 
  `x` float COMMENT '', 
  `y` float COMMENT '', 
  `z` float COMMENT '')
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe' 
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://stedi-analytics-project/accelerometer/landing/'
TBLPROPERTIES (
  'classification'='json', 
  'created_by'='arn:aws:iam::461897430395:root'
);