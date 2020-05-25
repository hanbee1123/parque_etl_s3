The following code works as follows

1. Parque data is extracted from S3. 
(data is partitions in dates. Therefore, it is extracted using a for loop)

2. Data is transformed using pandas in local server.

3. Data is transformed to parque file then uplaoded into s3 with partition.

4. Partitioned data is then read by athena
