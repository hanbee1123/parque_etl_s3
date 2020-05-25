import boto3
import pandas as pd
import os
import pyarrow
from pyathena import connect
from datetime import datetime, timedelta
from io import BytesIO


def etl_install_count(*args):
    # 입력된 argument 값을 list로 정리
    args = list(args)
    # argument 가 없는 경우, 오늘의 날짜에 대한 etl을 실행
    if len(args) == 0:
        start_date = datetime.today().date()
        end_date = datetime.today().date()
        if start_date < datetime.strptime('2020-03-01', '%Y-%m-%d').date() or end_date > datetime.strptime('2020-04-30', '%Y-%m-%d').date():
            raise ValueError('Date range is between 2020-03-01 and 2020-04-30')

    # argument 가 하나인 경우, 그날에 대해서만 etl을 실행
    elif len(args) == 1:
        start_date = datetime.strptime(args[0], '%Y-%m-%d').date()
        end_date = datetime.strptime(args[0], '%Y-%m-%d').date()
        #argument 가 3~4월 사이를 벗어나는 경우 raise value error
        if start_date < datetime.strptime('2020-03-01', '%Y-%m-%d').date() or end_date > datetime.strptime('2020-04-30', '%Y-%m-%d').date():
            raise ValueError('Date range is between 2020-03-01 and 2020-04-30')
        else:
            pass

    # argument 가 2개인 경우, 첫번째 argument를 시작일로 설정, 두번째 argument를 끝나는일로 설정
    elif len(args) == 2:
        start_date = datetime.strptime(args[0], '%Y-%m-%d').date()
        end_date = datetime.strptime(args[1], '%Y-%m-%d').date()
        #arguments 가 3~4월 사이를 벗어나는 경우 raise value error
        if start_date < datetime.strptime('2020-03-01', '%Y-%m-%d').date() or end_date > datetime.strptime('2020-04-30', '%Y-%m-%d').date():
            raise ValueError('Date range is between 2020-03-01 and 2020-04-30')
        else:
            pass

    # arguments 가 3개 이상 들어가는 경우, raise value error
    else:
        raise ValueError('Invalid date range')

    # Perform Data Extraction
    athena = connect(
        aws_access_key_id=os.environ['KIBOKO_KEY'],
        aws_secret_access_key=os.environ['KIBOKO_SECRET'],
        s3_staging_dir='s3://kiboko-study/python/pandas/todomath'
    )
    #Day List 를 설정
    daylist = []
    diff = end_date - start_date
    
    print('you will perform etl for the following days: \r')
    for i in range(diff.days+1):
        daylist.append(start_date+timedelta(i))
        print(daylist[i])

    for day in daylist: 
        query = '''
        SELECT 
            DISTINCT installid,
            country,
            platform,
            device_regdate,
            server_yyyymmdd
        FROM
            dw_temp.tb_temp_todomath
        WHERE
            DATE(device_regdate) = DATE(server_yyyymmdd)
            AND
            DATE(server_yyyymmdd) = DATE('{day}')
        '''.format(day=day)

        df = pd.read_sql(query, athena)

        # Perform Data Transformation

        # 1. US,JP,KR,CN 가 아닌  국가들을 etc로 변경
        def country_to_etc(x):
            if x not in ['US', 'JP', 'CN', 'KR']:
                x = 'etc'
            return x
        df['country'] = df['country'].apply(country_to_etc)

        # 2. OS 가 ios나 android가 아닌경우, etc로 변경
        def platform_to_etc(x):
            if x not in ['iOS', 'android']:
                x = 'etc'
            return x
        df['platform'] = df['platform'].apply(platform_to_etc)

        # country와 platform으로 group by 실행 후 installid를 count
        df = df.groupby(['country', 'platform']).agg('installid').count().reset_index()
        df['server_yyyymmdd'] = day
        df.rename(columns={"installid": "install_cnt"}, inplace = True)

    # Perform data loading 
        s3 = boto3.client(
            's3',
            aws_access_key_id=os.environ['KIBOKO_KEY'],
            aws_secret_access_key=os.environ['KIBOKO_SECRET']
        )

        parquet_buffer = BytesIO()
        df.to_parquet(parquet_buffer)
        s3.delete_object(
            Bucket='kiboko-study',
            Key='python/pandas/todomath_etl/server_yyyymmdd={day}/{day}.parquet'.format(day=day)
            )
        s3.put_object(
            Bucket='kiboko-study',
            Key='python/pandas/todomath_etl/server_yyyymmdd={day}/{day}.parquet'.format(day=day), 
            Body=parquet_buffer.getvalue()
            )
        print('ETL successfully done for: {day}'.format(day = day))

if __name__ == "__main__":
    etl_install_count('2020-03-01','2020-04-30')