#!/bin/bash

#aws_profile=('jaworra_sys' 'aws_mpi_account');
read -rsp $'Press enter to exit...\n'

#loop AWS profiles
for i in "${aws_profile[@]}"; do
  echo "${i}"
  buckets=($(aws --profile "${i}" --region ap-southeast-2 s3 ls s3:// --recursive | awk '{print $3}'))

  #loop S3 buckets
  for j in "${buckets[@]}"; do
  echo "${j}"
  aws --profile "${i}" --region ap-southeast-2 s3 ls s3://"${j}" --recursive --human-readable --summarize | awk END'{print}'
  done

  #RDS versions
  aws --profile "${i}" --region ap-southeast-2 rds describe-db-instances | awk END'{print}'
  done

done

read -rsp $'Press enter to exit...\n'

#ToDo: check rds instances in acount, include profile loop
#RDS versions
#aws --profile jaworra_sys --region ap-southeast-2 rds describe-db-instances | awk END'{print}'

