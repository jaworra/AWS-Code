#Script renames based on cli, requried if using Athena/glue to discover partions that are in uppercase(hive enviromnet)
#Todo: performance optimisation required. Possibly due to string list length (see flag, -A) ,substitution (see flag, -B)
#    : Tried to implment aws s3 --recursive however directories need replacing (see flag, -C)

aws_profile=('tmr_mpi_account');
buckets=('streams-gateway-raw-extract-partition'); #specific bucket.
startfrom=('streams-gateway-raw-extract-partition/traffic/v1/ds/csv/year=2020/month=02/day=01/'); #specific bucket.
#buckets=($(aws --profile "${i}" --region ap-southeast-2 s3 ls s3:// --recursive | awk '{print $3}')) #can set multiple locations here

#loop AWS profiles
for i in "${aws_profile[@]}"; do
  echo "${i}"

  #loop through listed buckets to change
  for k in "${startfrom[@]}"; do 
    #return only a list of just objects -A
    listOfObjects=($(aws --profile "${i}" --region ap-southeast-2 s3 ls --recursive s3://$k | awk '{ if($3 >0) print $4}' ));
  
    #process each object with match case -B
    for old_key in "${listOfObjects[@]}"; do
      new_key=${old_key/Year/year}
      new_key=${new_key/Month/month}
      new_key=${new_key/Day/day}
 
      { 
      aws --profile "${i}" --region ap-southeast-2 s3 cp s3://${buckets}/${old_key} s3://${buckets}/${new_key} ; 
      } &> /dev/null

    done
    #copy with new key, lowercase applied to whole string, -C
    #aws --profile "${i}" --region ap-southeast-2 s3 cp s3://$k s3://`echo $k | tr 'A-Z' 'a-z'` --recursive | awk END'{print}';
  done

done

read -rsp $'Completed press enter to exit...\n'
