#!/bin/bash
#aws_profile=('personal_account' 'tmr_travel_time_dev' 'tmr_mpi_account');
aws_profile=('personal_account')

#loop AWS profiles
for i in "${aws_profile[@]}"; do
  echo "${i}"
  buckets=($(aws --profile "${i}" --region ap-southeast-2 s3 ls s3:// --recursive | awk '{print $3}'))

    #loop S3 buckets
    for j in "${buckets[@]}"; do
    echo "${j}"
    done
done

read -rsp $'Press enter to exit...\n'

#manual
#https://docs.aws.amazon.com/lambda/latest/dg/build-pipeline.html
#export AWS_PROFILE=personal_account
#aws cofigure --profile personal_account
#Default region name:ap-southeast-2
aws codecommit create-repository --repository-name lambda-pipeline-repo --repository-description "microservices deploy repository" --tags Team=jaworra