import boto3
import pandas as pd
import os
import sys
import logging
from pyathena import connect
from datetime import datetime, timedelta
from io import BytesIO

logging.basicConfig(
    level = logging.INFO
    )

def etl_install_count(*args):
    # All arguments will be made into a list
    args = args[0]
    # If there is no argument given, it'll perform ETL on today's date
    # We are using sys.argv thus, the first argument will be the name of the file
    if len(args) == 1:
        start_date = datetime.today().date()
        end_date = datetime.today().date()

    # If there is one argument given, the start date and end date will be set to the date of the argument.
    elif len(args) == 2:
        start_date = datetime.strptime(args[1], '%Y-%m-%d').date()
        end_date = datetime.strptime(args[1], '%Y-%m-%d').date()

    # If there are two arguments, the start_date will be set to the first argument and the end_date will be set to the second argument.
    elif len(args) == 3:
        start_date = datetime.strptime(args[1], '%Y-%m-%d').date()
        end_date = datetime.strptime(args[2], '%Y-%m-%d').date()

    # If there are more arguments than 2, raise a value error.
    else:
        raise ValueError('Invalid date range')

    # Perform data extraction using athena connect
    athena = connect(
        aws_access_key_id=os.environ['KIBOKO_KEY'],
        aws_secret_access_key=os.environ['KIBOKO_SECRET'],
        s3_staging_dir='s3://kiboko-study' 
    )

    # Create the list of days that will be used for ETL
    # list comprehension is used in this case
    day_list = []
    diff = end_date - start_date
    logging.info('you will perform etl for the following days: \r') 
    
    for i in range(diff.days+1):
        day_list.append(start_date+timedelta(i))
        logging.info(day_list[i])

    # Perform SQL to bring data through athena
    for day in day_list: 
        query = f"""
        SELECT 
            DISTINCT installid,
            country,
            platform,
            device_regdate
        FROM
            dw_temp.tb_temp_todomath
        WHERE
            DATE(device_regdate) = DATE(server_yyyymmdd)
            AND
            DATE(server_yyyymmdd) = DATE('{day}')
        LIMIT 
            10
        """

        #Put data into a dataframe in local memory
        df = pd.read_sql(query, athena)


        # Perform Data Transformation
        # 1. Any country that is not US,JP,CN,KR will be set to 'etc'
        df['country'].apply(lambda x: 'ETC' if x not in ['US','JP','CN','KR'] else x)

        # 2. Any OS that is not iOS or android will be set to 'etc'
        df['platform'].apply(lambda x: 'ETC' if x not in ['iOS','android'] else x)

        # Group the data by country and platform and count the number of install id
        df = df.groupby(['country', 'platform']).agg({'installid':'count'}).reset_index()
        df.rename(columns={"installid": "install_cnt"}, inplace = True)

    # Perform data loading 
    # Connect to s3 using boto3
        s3 = boto3.client(
            's3',
            aws_access_key_id=os.environ['KIBOKO_KEY'],
            aws_secret_access_key=os.environ['KIBOKO_SECRET']
        )

    # Save the dataframe into parquet buffer in parquet format
        parquet_buffer = BytesIO()
        df.to_parquet(parquet_buffer)
        
        Bucket = 'kiboko-study'
        Key = f'python/pandas/todomath_etl/server_yyyymmdd={day}/{day}.parquet'

        # Delete any object incase there is a duplicate
        s3.delete_object(
            Bucket = Bucket,
            Key = Key
            )
        
        # Upload the object
        s3.put_object(
            Bucket = Bucket,
            Key = Key, 
            Body=parquet_buffer.getvalue()
            )

        logging.info(f'ETL successfully done for: {day}')

if __name__ == "__main__":
    etl_install_count(sys.argv)