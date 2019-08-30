#!/bin/bash
FILES=$1
BUCKET=$2
PROFILE=${3:-default}
for f in $FILES/*
do
    FILENAME=`basename ${f%%%}`
    S3_PATH="s3://${BUCKET}/${FILENAME}"
    echo "Uploading ${f} to ${S3_PATH}"
    aws s3 cp $f $S3_PATH --profile $PROFILE
done
