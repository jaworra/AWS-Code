#!/bin/bash
#aws_profile=('personal_account' 'tmr_travel_time_dev' 'tmr_mpi_account');
aws_profile=('personal_account') 

detail_report=false

if ($detail_report); then #
    for i in "${aws_profile[@]}"; do
        echo "${i}"
        buckets=($(aws --profile "${i}" --region ap-southeast-2 s3 ls s3:// --recursive | awk '{print $3}'))

        #loop through buckets
        for j in "${buckets[@]}"; do
            echo "${j}"
            aws --profile "${i}" --region ap-southeast-2 s3 ls s3://"${j}" --recursive --human-readable --summarize | awk END'{print}'
        done
    done
else
    for i in "${aws_profile[@]}"; do
        echo "${i}"
        buckets=($(aws --profile "${i}" --region ap-southeast-2 s3 ls s3:// --recursive | awk '{print $3}'))

        #loop through buckets
        for j in "${buckets[@]}"; do
            echo "${j}"
        done
    done
fi
read -rsp $'Press enter to exit...\n'
