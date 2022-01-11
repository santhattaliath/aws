import pyodbc, pandas, sys, os
import logging
from datetime import datetime
import boto3

logFormat = '%(asctime)s %(message)s'
logging.basicConfig(filename='process.log', format=logFormat, level=logging.INFO)
# for folder name
runDay = datetime.now().strftime('%d_%B_%Y')
# for subfolder name with timestamp
# folder and object names will be in lowercase
runTime = datetime.now().strftime('%d_%B_%Y_%H_%M_%S').lower()

try:
    conn = pyodbc.connect(
        'DRIVER={ODBC Driver 13 for SQL Server};SERVER=172.31.33.147;DATABASE=INT77DB2014;UID=sisensedb_user;PWD=Sisense12#$')
    if conn is not None:
        logging.info('Connection established....')
except:
    print('error {}'.format(str(sys.exc_info()[0])))
    sys.exit()

tables = ['chdrpf', 'riskpf','descpf','cmovpf','ltrnpf','cheqpf']

df = pandas.DataFrame()

region = 'ap-southeast-1'
# create s3 client
s3Client = boto3.client('s3', aws_access_key_id='AKIASZY4VHT7YZU3E26G',
                        aws_secret_access_key='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')
# create new folder for the day
# check if bucketName exists for the day
bucketName = 'integralba'
folderName = 'integralaws_daily_feed_{}'.format(runDay).lower()
logging.info('Creating folder {} under bucket {}'.format(bucketName, folderName))
#s3Client.put_object(Bucket=bucketName, Key='{}'.format(folderName))

for table in tables:
    logging.info('Processing table {}'.format(table))
    df = None
    sql = 'select * from VM1DTA.{}'.format(table)
    df = pandas.read_sql(sql, conn)
    df.fillna(0, inplace=True)
    # data to csv after cleansing
    fileName = '{}_{}.csv'.format(table, runTime)
    df.to_csv(fileName, index=False)
    s3Client.upload_file(fileName, bucketName, '{}/{}'.format(folderName, fileName))

# create new folder for every run
