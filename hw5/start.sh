#!/bin/bash

if [-d "/home/tigeryi/hw5"]; then
    echo "hw5 exist" 
else
    gsutil -m cp -r gs://ds561-tigeryi-hw5 /home/tigeryi/hw5/ 
fi

cd /home/tigeryi/hw5

apt install python3-pip -y
pip3 install -r requirements.txt

python3 http-server-cloud.py