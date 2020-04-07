#python code that uses cli and boto3 enviroment to move contents of S3 bucket
#to a new bucket - for the current case this is required for athena partitions
#needing lower case. e.g Year=2020 to year=2020 etc

#using the boto3 library
import boto3
import re

#switching accounts with the cli
#aws_profile=('jaworra_sys' 'aws_mpi_account') #my accounts
boto3.setup_default_session(profile_name='aws_mpi_account') #change default session in code
'''
#Check with account with below list of buckets
s3 = boto3.resource('s3')
for bucket in s3.buckets.all():
  print(bucket.name)
'''

def main ():
    bucket_source = 'streams-gateway-raw-extract' 
    bucket_destination = bucket_source#'streams-gateway-raw-extract-partition'

    # #2019 Nov
    # for i in range(1,2,1):
    #     startAfter = 'traffic/v1/ds/csv/Year=2019/Month=11/Day=0'+str(i)
    #     get_list_after_1000_and_etl(bucket_source,bucket_destination,startAfter)
    #     print(startAfter)
    #     print(i)

    #startAfter = 'traffic/v1/ds/csv/Year=2020/Month=03'
    #get_list_after_1000_and_etl(bucket_source,bucket_destination,startAfter)
    #write as a list of array to process

    paths = ['traffic/v1/ds/csv/Year=2020/Month=02',
             'traffic/v1/ds/csv/Year=2020/Month=01',
             'traffic/v1/ds/agg/csv/Year=2019',
             'traffic/v1/ds/agg/csv/Year=2020'
            #  'traffic/v1/link/csv/Year=2020',
            #  'traffic/v1/link/csv/Year=2019',
            #  'traffic/v1/movement/csv/Year=2020',
            #  'traffic/v1/movement/csv/Year=2019',
            #  'traffic/v1/npi/agg/csv/Year=2020',
            #  'traffic/v1/npi/agg/csv/Year=2019',
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

    # startAfter = 'traffic/v1/ds/csv/Year=2020/Month=02'
    # get_list_after_1000_and_etl(bucket_source,bucket_destination,startAfter)
    # startAfter = 'traffic/v1/ds/csv/Year=2020/Month=01'
    # get_list_after_1000_and_etl(bucket_source,bucket_destination,startAfter)
    return


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
            copy_source = {'Bucket': bucket_source,'Key': old_key}
            s3_resource.meta.client.copy(copy_source, bucket_destination, new_key)
main()





