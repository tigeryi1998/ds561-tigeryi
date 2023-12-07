#!/bin/bash

if [-d "/home/tigeryi/hw10"]; then
    echo "hw10 exist" 
else
    gsutil -m cp -r gs://ds561-tigeryi-hw10 /home/tigeryi/hw10/ 
fi

cd /home/tigeryi/hw10

apt install python3-pip -y
pip3 install -r requirements.txt

python3 http-client.py -d '127.0.0.1' -b 'ds561-tigeryi-hw10' -w files -i 11000 -n 20 -p 8080 -r 0 -f -m 'GET'

python3 http-client.py -d '127.0.0.1' -b 'ds561-tigeryi-hw10' -w files -i 11000 -n 5 -p 8080 -r 0 -f -m 'POST'

python3 http-client.py -d '127.0.0.1' -b 'ds561-tigeryi-hw10' -w files -i 11000 -n 5 -p 8080 -r 0 -f -m 'PUT'