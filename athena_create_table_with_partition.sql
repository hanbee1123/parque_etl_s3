CREATE EXTERNAL TABLE `tb_temp_todomath_etl`(
  `country` string, 
  `platform` string, 
  `install_cnt` int
)
PARTITIONED BY ( 
  `server_yyyymmdd` string)

STORED AS parquet

LOCATION
  's3://kiboko-study/python/pandas/todomath_etl/'

tblproperties ("parquet.compression"="SNAPPY");


MSCK REPAIR TABLE tb_temp_todomath_etl
