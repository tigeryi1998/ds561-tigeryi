#!/bin/bash

if [-d "/home/tigeryi/hw7"]; then
    echo "hw7 exist" 
else
    gsutil -m cp -r gs://ds561-tigeryi-hw7 /home/tigeryi/hw7/ 
fi

cd /home/tigeryi/hw7

apt install python3-pip -y
# pip3 install -r requirements.txt

pip3 install apache-beam
pip3 install 'apache-beam[gcp]'
pip3 install 'apache-beam[test]'
pip3 install 'apache-beam[docs]'

python3 hw7.py \
    --input gs://ds561-tigeryi-hw7/files/ \
    --output gs://ds561-tigeryi-hw7/output/result.txt \
    --runner DataflowRunner \
    --project feisty-gasket-398719 \
    --region us-east1 \
    --temp_location gs://ds561-tigeryi-hw7/tmp/ \
    --staging_location gs://ds561-tigeryi-hw7/staging/ \
    --job_name job1