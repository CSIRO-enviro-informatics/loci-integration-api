#!/bin/bash

INDEX_NAME="default_index"
ES_JSON_TAR_FILE=loc-labels-es-json.tar.gz

echo 'get loc labels file'
#if the file exists, assume it has been mapped in. else download from loci-assets on s3 
if [ -f "$ES_JSON_TAR_FILE" ]; then
    echo "$ES_JSON_TAR_FILE exist"
else 
    echo "$ES_JSON_TAR_FILE does not exist... try to download from s3"

    S3_TARBALL_LOCATION=$S3_LABEL_TARBALL
    if [ -z "$S3_LABEL_TARBALL" ]
    then
        echo "S3_LABEL_TARBALL env variable is not set so we can't download it to index. Exiting..."
        exit 1  
    else
        wget -O $ES_JSON_TAR_FILE $S3_LABEL_TARBALL        
    fi
fi

#double check if the file exists
if [ -f "$ES_JSON_TAR_FILE" ]; then
    echo "double checking that $ES_JSON_TAR_FILE exists - it does!"
else 
    echo "double checking that $ES_JSON_TAR_FILE exists - it does NOT! exiting"
    exit 1
fi  

echo 'uncompressing loc labels file'
tar zxvf $ES_JSON_TAR_FILE

echo 'upload labels to es for indexing'
FILES=split-*.json
for f in $FILES
do
  echo "Processing $f file..."
  # take action on each file. $f store current file name
  echo "uploading $f to es"
  curl --silent --output /dev/null -XPOST -H "Content-Type: application/json" elasticsearch:9200/_bulk --data-binary @${f} 
  #cleanup
  rm $f
done

echo 'done'
