The ETL process is as follows:

1. Extract parquet file data from S3 using athena.
2. Load extracted parquet data in local directory as data frame.
3. Transform data using pandas.
4. Load data back into boto3. (During this process, data was partitioned)
5. Create table in athena.

Libraries included:

1. import boto3
2. import pandas as pd
3. import os
4. import sys
5. import logging
6. from pyathena import connect
7. from datetime import datetime, timedelta
8. from io import BytesIO
