CREATE EXTERNAL TABLE `stedi`.`customer_landing`(
  `customername` string COMMENT '', 
  `email` string COMMENT '', 
  `phone` string COMMENT '', 
  `birthday` string COMMENT 'Expected format: YYYY-MM-DD', 
  `serialnumber` string COMMENT 'Expected format: UUID', 
  `registrationdate` bigint COMMENT 'Timestamp in milliseconds', 
  `lastupdatedate` bigint COMMENT 'Timestamp in milliseconds', 
  `sharewithresearchasofdate` bigint COMMENT 'Timestamp in milliseconds', 
  `sharewithpublicasofdate` bigint COMMENT 'Timestamp in milliseconds',
  `ssharewithfriendsasofdate` bigint COMMENT 'Timestamp in milliseconds')
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe' 
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://stedi-analytics-project/customer/landing/'
TBLPROPERTIES (
  'classification'='json', 
  'created_by'='arn:aws:iam::461897430395:root'
);