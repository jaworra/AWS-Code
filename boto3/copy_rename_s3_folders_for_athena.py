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
    bucket_destination = bucket_source

    #careful! this copies and deletes s3 folders
    paths = ['traffic/v1/ds/agg/csv/Year=2019'
        
            #month 4 day 6 half done
            # 'traffic/v1/ds/csv/2019' #not done
            # 'traffic/v1/ds/csv/Year=2020/Month=04/Day=06' #this is half done

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
        #copy then delete
        try:
            get_list_after_1000_and_etl(bucket_source,bucket_destination,startAfter)
            delete_s3_files_and_folders(bucket_source,bucket_destination,startAfter)
            print('transfer complete')
        except:
            print('Error in s3 file transfer')
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
    print("Copying files in " + prefix + "  ....")
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

def delete_s3_files_and_folders(bucket_source,bucket_destination,prefix):
    #remove residuals
    print("Deleting files in " + prefix + "  ....")
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_source) 
    bucket.objects.filter(Prefix=prefix).delete() 

    return

main()
