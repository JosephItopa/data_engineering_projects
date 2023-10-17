CREATE EXTERNAL TABLE IF NOT EXISTS
weather_db.weather_tbl (
  City string,
  AverageHumidity float,
  MaxWindSpeed float,
  Temperature float,
) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'separatorChar' = ',',
  'quoteChar' = '\"',
  'escapeChar' = '\\'
)
STORED AS TEXTFILE
LOCATION 's3://weather-data/input/';