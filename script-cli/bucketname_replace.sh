
aws_profile=('aws_mpi_account');


#loop AWS profiles
for i in "${aws_profile[@]}"; do
  echo "${i}"
  buckets=($(aws --profile "${i}" --region ap-southeast-2 s3 ls s3:// --recursive | awk '{print $3}'))

  buckets=('tmrmpi-docs'); #specific bucket.
  #loop S3 buckets
  for j in "${buckets[@]}"; do
    echo "${j}"
    aws --profile "${i}" --region ap-southeast-2 s3 ls s3://"${j}" --recursive --human-readable --summarize | awk END'{print}'
  
  done

done


read -rsp $'Press enter to exit...\n'
#for i in $( aws s3 ls s3://bucket-name | awk '{print $4}' ); do aws s3 mv s3://bucket-name/$i s3://bucket-name/`echo $i | tr 'A-Z' 'a-z'`; done