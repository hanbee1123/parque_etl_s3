The ETL process is as follows:

1. Extract parquet file data from S3 using athena.
2. Load extracted parquet data in local directory as data frame.
3. Transform data using pandas.
4. Load data back into boto3. (During this process, data was partitioned)
5. Create table in athena.

Libraries included:

import boto3
import pandas as pd
import os
import sys
import logging
from pyathena import connect
from datetime import datetime, timedelta
from io import BytesIO
