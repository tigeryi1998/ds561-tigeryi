#!/bin/bash

if [-d "/home/tigeryi/hw10"]; then
    echo "hw10 exist" 
else
    gsutil -m cp -r gs://ds561-tigeryi-hw10 /home/tigeryi/hw10/ 
fi

cd /home/tigeryi/hw10

apt install python3-pip -y
pip3 install -r requirements.txt

python3 forbidden-requests.py