#python code that uses cli and boto3 enviroment to move contents of S3 bucket
#to a new bucket - for the current case this is required for athena partitions
#needing lower case. e.g Year=2020 to year=2020 etc


#using the boto3 library
import boto3
import re

#switching accounts with the cli
#export AWS_DEFAULT_PROFILE=[account_name]
#aws s3 ls #check

s3 = boto3.resource('s3')
bucket_source = 'streams-gateway-raw-extract' 
bucket_destination ='streams-gateway-raw-extract-partition'


def get_list_after_1000_and_etl(bucket_source,bucket_destination,prefix):
    s3 = boto3.client('s3')
    s3_resource = boto3.resource('s3')
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_source, Prefix=prefix)
    k,i = 0,0
    for page in pages:
        k += 1
        if k > 1: #only copy after 1000 records
            for obj in page['Contents']: 
                old_key= obj['Key']
                new_key = old_key.replace("Year=", "year=").replace("Month=", "month=").replace("Day=", "day=")
                copy_source = {'Bucket': bucket_source,'Key': old_key}
                s3_resource.meta.client.copy(copy_source, bucket_destination, new_key)


#Feb
for i in range(1,10,1):
    startAfter = 'traffic/v1/ds/csv/Year=2020/Month=02/Day=0'+str(i)
    get_list_after_1000_and_etl(bucket_source,bucket_destination,startAfter)
    print(startAfter)

for i in range(10,30,1):
    if i != 17: #skip 17 already done
        startAfter = 'traffic/v1/ds/csv/Year=2020/Month=02/Day='+str(i)
        get_list_after_1000_and_etl(bucket_source,bucket_destination,startAfter)
        print(startAfter)

print('Finish Feb')

#Mar
for i in range(1,10,1):
    startAfter = 'traffic/v1/ds/csv/Year=2020/Month=03/Day=0'+str(i)
    get_list_after_1000_and_etl(bucket_source,bucket_destination,startAfter)
    print(startAfter)

for i in range(10,24,1):
    startAfter = 'traffic/v1/ds/csv/Year=2020/Month=03/Day='+str(i)
    get_list_after_1000_and_etl(bucket_source,bucket_destination,startAfter)
    print(startAfter)


print('copy complete')




'''
#view folders input to send out
s3client = boto3.client('s3')
startAfter = 'traffic/v1/ds/csv/Year=2020/Month=02/Day=05' #'Day' #'firstlevelFolder/secondLevelFolder  
theobjects = s3client.list_objects_v2(Bucket=bucket_source, StartAfter=startAfter)
for object in theobjects['Contents']:
    old_key= object['Key']
    new_key = old_key.replace("Year=", "year=").replace("Month=", "month=").replace("Day=", "day=")
    copy_source = {'Bucket': bucket_source,'Key': old_key}
    s3.meta.client.copy(copy_source, bucket_destination, new_key)
    print(new_key)
'''
'''
s3client = boto3.client('s3')
for i in range(2,10,1):
    startAfter = 'traffic/v1/ds/csv/Year=2020/Month=03/Day=0'+str(i)
    theobjects = s3client.list_objects_v2(Bucket=bucket_source, StartAfter=startAfter)
    for object in theobjects['Contents']:
        old_key= object['Key']
        new_key = old_key.replace("Year=", "year=").replace("Month=", "month=").replace("Day=", "day=")
        copy_source = {'Bucket': bucket_source,'Key': old_key}
        s3.meta.client.copy(copy_source, bucket_destination, new_key)
        #print(new_key)
    
for i in range(10,24,1):
    startAfter = 'traffic/v1/ds/csv/Year=2020/Month=03/Day='+str(i)
    theobjects = s3client.list_objects_v2(Bucket=bucket_source, StartAfter=startAfter)
    for object in theobjects['Contents']:
        old_key= object['Key']
        new_key = old_key.replace("Year=", "year=").replace("Month=", "month=").replace("Day=", "day=")
        copy_source = {'Bucket': bucket_source,'Key': old_key}
        s3.meta.client.copy(copy_source, bucket_destination, new_key)
        #print(new_key)
    
print('copy complete')
'''

'''
#below code uses the cli
import subprocess
#subprocess.run(['aws', 's3', 'ls', 's3://path/to/my/bucket/12434', '--recursive', '--human-readable', '--summarize'])
subprocess.run(['aws', 's3', 'ls'],capture_output=True) #https://docs.python.org/3/library/subprocess.html#subprocess.run
p = subprocess.Popen(args)

cmd='aws s3 ls'
push=subprocess.Popen(cmd, shell=True, stdout = subprocess.PIPE)
push.wait()   # the new line
print(push.returncode)
'''