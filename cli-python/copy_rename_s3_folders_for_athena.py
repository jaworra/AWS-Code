#python code that uses cli and boto3 enviroment to move contents of S3 bucket
#to a new bucket - for the current case this is required for athena partitions
#needing lower case. e.g Year=2020 to year=2020 etc

#using the boto3 library
import boto3
import re
import datetime
import pathlib

#switching accounts with the cli
#aws_profile=('jaworra_sys' 'aws_mpi_account') #my accounts
boto3.setup_default_session(profile_name='tmr_mpi_account') #change default session in code


'''
#Check with account with below list of buckets
s3 = boto3.resource('s3')
for bucket in s3.buckets.all():
  print(bucket.name)
'''

def main ():
    bucket_source = 'streams-gateway-raw-extract' 
    bucket_destination = bucket_source#'streams-gateway-raw-extract-partition'

    paths = ['traffic/v1/ds/csv/Year=2020/Month=02',
             'traffic/v1/ds/csv/Year=2020/Month=01'
 
             #'traffic/v1/ds/csv/Year=2020/Month=02',
             #'traffic/v1/ds/csv/Year=2020/Month=01'
                    
            # 'traffic/v1/ds/csv/Year=2020/Month=03/Day=22',
            # 'traffic/v1/ds/csv/Year=2020/Month=03/Day=23',
            # 'traffic/v1/ds/csv/Year=2020/Month=03/Day=24',
            # 'traffic/v1/ds/csv/Year=2020/Month=03/Day=25',
            # 'traffic/v1/ds/csv/Year=2020/Month=03/Day=26',
            # 'traffic/v1/ds/csv/Year=2020/Month=03/Day=27',
            # 'traffic/v1/ds/csv/Year=2020/Month=03/Day=28',
            # 'traffic/v1/ds/csv/Year=2020/Month=03/Day=29',
            # 'traffic/v1/ds/csv/Year=2020/Month=03/Day=30',
            # 'traffic/v1/ds/csv/Year=2020/Month=03/Day=31'


            # 'traffic/v1/ds/csv/Year=2020/Month=04/Day=01',
            # 'traffic/v1/ds/csv/Year=2020/Month=04/Day=02',
            # 'traffic/v1/ds/csv/Year=2020/Month=04/Day=03',
            # 'traffic/v1/ds/csv/Year=2020/Month=04/Day=04',
            # 'traffic/v1/ds/csv/Year=2020/Month=04/Day=05',
            # 'traffic/v1/ds/csv/Year=2020/Month=04/Day=06' #this is half done
            # month 4 done!

            # ToDo: 3,2,1 for year 2020

            #'traffic/v1/ds/csv/Year=2019'
            #completed
            #  'traffic/v1/ds/csv/Year=2020/Month=02',
            #  'traffic/v1/ds/csv/Year=2020/Month=01',
            #  'traffic/v1/ds/agg/csv/Year=2019',
            #  'traffic/v1/ds/agg/csv/Year=2020'

            #  below traffic not needed.
            #  'traffic/v1/link/csv/Year=2020',
            #  'traffic/v1/link/csv/Year=2019',
            #  'traffic/v1/movement/csv/Year=2020',
            #  'traffic/v1/movement/csv/Year=2019',
            #  'traffic/v1/npi/agg/csv/Year=2020',
            #  'traffic/v1/npi/agg/csv/Year=2019',s
            #  'traffic/v1/npi/csv/Year=2020',
            #  'traffic/v1/npi/csv/Year=2019',  
            #  'traffic/v1/vd/agg/csv/Year=2020',
            #  'traffic/v1/vd/agg/csv/Year=2019',
            #  'traffic/v1/vd/csv/Year=2020',
            #  'traffic/v1/vd/csv/Year=2019'                                    
             ]

    for i in range (len(paths)):
        startAfter = paths[i]
        get_list_after_1000_and_etl(bucket_source,bucket_destination,startAfter)
        print("Completed transfer of: "+startAfter)

    return


def group_minutes(modified_date_mm):
    if modified_date_mm < 15:
        minute_group = 00
    elif modified_date_mm < 30:
        minute_group = 15
    elif modified_date_mm < 45:
        minute_group = 30 
    elif modified_date_mm < 60:
        minute_group = 45 
    return minute_group

def get_list_after_1000_and_etl(bucket_source,bucket_destination,prefix):
    s3 = boto3.client('s3')
    s3_resource = boto3.resource('s3')
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_source, Prefix=prefix)
    k,i = 0,0
    for page in pages:
        k += 1
        for obj in page['Contents']: 
            i+=1
            old_key= obj['Key']
            new_key = old_key.replace("Year=", "year=").replace("Month=", "month=").replace("Day=", "day=")
            
            #comment below lines if hhmm partition not included.
            #copy_source = {'Bucket': bucket_source,'Key': old_key}
            #s3_resource.meta.client.copy(copy_source, bucket_destination, new_key)
            #return

            #modify new key to include the hhmm partion. only works on orginal
            modified_date= obj['LastModified']
            modified_date_mm = group_minutes(modified_date.minute)
            hhmm_key_str = '{:02}{:02}'.format(int(modified_date.hour), int(modified_date_mm))

            filename = new_key.split('/').pop()
            new_key_hhmm = new_key[:-len(filename)] + 'hhmm=' + hhmm_key_str + '/' + filename
            copy_source = {'Bucket': bucket_source,'Key': old_key}
            s3_resource.meta.client.copy(copy_source, bucket_destination, new_key_hhmm)

    return

main()
