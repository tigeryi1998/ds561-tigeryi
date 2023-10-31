#!/usr/local/bin/python3

from http.server import BaseHTTPRequestHandler, HTTPServer
import argparse
import time
import google.cloud.storage as storage
import google.cloud.pubsub as pubsub
import os 
import sys
import pymysql
import socket, struct
from google.cloud.sql.connector import Connector, IPTypes
import sqlalchemy
from sqlalchemy import text 
from sqlalchemy import create_engine, MetaData, Table, Column, Numeric, Integer, VARCHAR 


PROJECT_ID = "feisty-gasket-398719"
INSTANCE_CONNECTION_NAME = "feisty-gasket-398719:us-east1:instance-tigeryi"
DB_USER = "root"
DB_PASS = ""
DB_NAME = "dbhw5"



def create_tables():

    # initialize Connector object
    connector = Connector()

    # the database connection object
    conn = connector.connect(
        INSTANCE_CONNECTION_NAME,
        "pymysql",
        user=DB_USER,
        password=DB_PASS,
        db=DB_NAME
    )
    
    # create connection pool with 'creator' argument to our connection object function
    pool = sqlalchemy.create_engine(
        "mysql+pymysql://",
        creator=conn,
    )

    with pool.connect() as db_conn:
        db_conn.execute(
            sqlalchemy.text(
                "CREATE TABLE IF NOT EXISTS table1 "
                "(ip VARCHAR(255) NOT NULL, "
                "time_of_day VARCHAR(255) NOT NULL, "
                "filename VARCHAR(255), "
                "PRIMARY KEY (ip, time_of_day));"
            )
        )
        db_conn.commit()

        db_conn.execute(
            sqlalchemy.text(
                "CREATE TABLE IF NOT EXISTS table2 "
                "(ip VARCHAR(255) NOT NULL, "
                "gender VARCHAR(255) NOT NULL, "
                "age VARCHAR(255) NOT NULL, "
                "income VARCHAR(255) NOT NULL, "
                "country VARCHAR(255) NOT NULL, "
                "is_banned BOOL NOT NULL, "
                "PRIMARY KEY (ip));"
            )
        )
        db_conn.commit()

        db_conn.execute(
            sqlalchemy.text(
                "CREATE TABLE IF NOT EXISTS table1 "
                "(ip VARCHAR(255) NOT NULL, "
                "time_of_day VARCHAR(255) NOT NULL, "
                "filename VARCHAR(255) NOT NULL, "
                "error INT(225), "
                "PRIMARY KEY (ip, time_of_day));"
            )
        )
        db_conn.commit()

    connector.close()


def fill_table1(ip, time_of_day, filename):

    # initialize Connector object
    connector = Connector()

    # the database connection object
    conn = connector.connect(
        INSTANCE_CONNECTION_NAME,
        "pymysql",
        user=DB_USER,
        password=DB_PASS,
        db=DB_NAME
    )
    
    # create connection pool with 'creator' argument to our connection object function
    pool = sqlalchemy.create_engine(
        "mysql+pymysql://",
        creator=conn,
    )

    # insert data into our ratings table
    insert_stmt = sqlalchemy.text(
        "INSERT INTO table1 (ip, time_of_day, filename) VALUES (:ip, :time_of_day, :filename)",
    )

    with pool.connect() as db_conn:
        db_conn.execute(insert_stmt, parameters={
            "ip": ip, 
            "time_of_day": time_of_day, 
            "filename": filename
            }
        )
        db_conn.commit()
    connector.close()



def fill_table3(ip, time_of_day, filename, error):

    # initialize Connector object
    connector = Connector()

    # the database connection object
    conn = connector.connect(
        INSTANCE_CONNECTION_NAME,
        "pymysql",
        user=DB_USER,
        password=DB_PASS,
        db=DB_NAME
    )
    
    # create connection pool with 'creator' argument to our connection object function
    pool = sqlalchemy.create_engine(
        "mysql+pymysql://",
        creator=conn,
    )

    # insert data into our ratings table
    insert_stmt = sqlalchemy.text(
        "INSERT INTO table1 (ip, time_of_day, filename, error) VALUES (:ip, :time_of_day, :filename, :error)",
    )

    with pool.connect() as db_conn:
        db_conn.execute(insert_stmt, parameters={
            "ip": ip, 
            "time_of_day": time_of_day, 
            "filename": filename,
            "error": error
            }
        )
        db_conn.commit()
    connector.close()


class MyServer(BaseHTTPRequestHandler):

    def publish_pub_sub(self, message):
        project_id = 'feisty-gasket-398719'
        topic_id = project_id + 'my-topic'
        publisher = pubsub.PublisherClient()
        # in the form `projects/{project_id}/topics/{topic_id}`
        topic_path = publisher.topic_path(project_id, topic_id)
        data = message.encode('utf-8')
        future = publisher.publish(topic_path, data)
        future.result()
    
    def do_GET(self):
        country = self.headers['X-Country']
        is_banned = True
        if country in ['North Korea', 'Iran', 'Cuba', 'Myanmar', 'Iraq', 'Libya', 'Sudan', 'Zimbabwe', 'Syria']:
            self.publish_pub_sub('Banned country ' + country)
            is_banned = False
        ip = self.headers['X-Client-IP']
        gender = self.headers['X-gender']
        age = self.headers['X-age']
        income = self.headers['X-income']
        time_of_day = self.headers['X-time']
        bucket = None
        directory = None
        filename = None
        parts = self.path.split('/')
        bucket = parts[1]
        directory = parts[2]
        filename = parts[3]            
        self.send_gcs_response(bucket, directory, filename, ip, gender, age, income, time_of_day, is_banned)

    def send_gcs_response(self, bucket, directory, filename, ip, gender, age, income, time_of_day, is_banned):
        receive_headers = self.headers
        try:
            storage_client = storage.Client()
            bucket = storage_client.bucket(bucket)
            blobname = directory + '/' + filename
            blob = bucket.blob(blobname)
            content = ''
            with blob.open("r") as f:
                self.send_response(200)
                self.send_header("Content-type", "text/html")
                self.end_headers()
                self.wfile.write(bytes("<html><head><title>https://pythonbasics.org</title></head>", "utf-8"))
                self.wfile.write(bytes("<p>Request: %s</p>" % self.path, "utf-8"))
                self.wfile.write(bytes("<body>", "utf-8"))
                self.wfile.write(bytes("<p>This is an example web server.</p>", "utf-8"))
                for key in receive_headers:
                    self.wfile.write(bytes("Got header ", "utf-8"))
                    self.wfile.write(bytes('{}:{}\n'.format(key,receive_headers[key]), "utf-8"))
                self.wfile.write(bytes("</body></html>", "utf-8"))
                content = f.read()
                self.wfile.write(bytes(content, "utf-8"))
        except:
            fill_table3(ip, time_of_day, filename, 404)
            self.send_response(404)
            self.send_header("Content-type", "text/html")
            self.end_headers()
            self.wfile.write(bytes("<html><head><title>https://pythonbasics.org</title></head>", "utf-8"))
            self.wfile.write(bytes("<p>Request: %s</p>" % self.path, "utf-8"))
            self.wfile.write(bytes("<body>", "utf-8"))
            self.wfile.write(bytes("<p>File not found.</p>", "utf-8"))
            self.wfile.write(bytes("</body></html>", "utf-8"))                
            
    def do_PUT(self):
        self.send500error()

    def do_POST(self):
        self.send500error()

    def do_HEAD(self):
        self.send500error()

    def do_DELETE(self):
        self.send500error()

    def send500error(self):
        # fill_table3(ip, time_of_day, filename, 500)
        self.send_response(500)
        self.end_headers()
        self.wfile.write(bytes("Server method unavailable", "utf-8"))
                    
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--domain", help="Domain to make requests to", type=str, default="localhost")
    parser.add_argument("-p", "--port", help="Server Port", type=int, default=8080)
    args = parser.parse_args()

    create_tables()
    
    webServer = HTTPServer((args.domain, args.port), MyServer)
    print("Server started http://%s:%s" % (args.domain, args.port))

    try:
        webServer.serve_forever()
    except KeyboardInterrupt:
        pass

    webServer.server_close()
    print("Server stopped.")
        
if __name__ == "__main__":        
    main()


