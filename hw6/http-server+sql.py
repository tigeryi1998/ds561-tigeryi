#!/usr/bin/python3

# Webserver dependencies
from http.server import BaseHTTPRequestHandler, HTTPServer
import argparse
import time
import google.cloud.storage as storage
import google.cloud.pubsub as pubsub

# Sql dependencies
import os
from google.cloud.sql.connector import Connector, IPTypes
import pymysql
import socket, struct
import sqlalchemy



PROJECT_ID = "feisty-gasket-398719"
TOPIC_ID = "my-topic"
INSTANCE_CONNECTION_NAME = "feisty-gasket-398719:us-east1:instance-tigeryi"
DB_USER = "root"
DB_PASS = ""
DB_NAME = "dbhw5"




class MySqlServer():
    pool = None
    
    def connect_with_connector(self) -> sqlalchemy.engine.base.Engine:
        """
        Initializes a connection pool for a Cloud SQL instance of MySQL.
        
        Uses the Cloud SQL Python Connector package.
        """
        # Note: Saving credentials in environment variables is convenient, but not
        # secure - consider a more secure solution such as
        # Cloud Secret Manager (https://cloud.google.com/secret-manager) to help
        # keep secrets safe.

        instance_connection_name = INSTANCE_CONNECTION_NAME # os.environ["INSTANCE_CONNECTION_NAME"]  # e.g. 'project:region:instance'
        db_user = DB_USER # os.environ["DB_USER"]  # e.g. 'my-db-user'
        db_pass = DB_PASS # os.environ["DB_PASS"]  # e.g. 'my-db-password'
        db_name = DB_NAME # os.environ["DB_NAME"]  # e.g. 'my-database'

        ip_type = IPTypes.PRIVATE if os.environ.get("DB_PRIVATE_IP") else IPTypes.PUBLIC

        connector = Connector(ip_type)

        def getconn() -> pymysql.connections.Connection:
            conn: pymysql.connections.Connection = connector.connect(
                instance_connection_name,
                "pymysql",
                user=db_user,
                password=db_pass,
                db=db_name,
            )
            return conn
        
        self.pool = sqlalchemy.create_engine(
            "mysql+pymysql://",
            creator=getconn,
            # ...
        )
        return self.pool

    def create_table(self):
        create_stmt = sqlalchemy.text(
            """
            CREATE TABLE IF NOT EXISTS accesslogs(
            country VARCHAR(64),
            ip INTEGER UNSIGNED,
            gender SMALLINT,
            age VARCHAR(16),
            income VARCHAR(32),
            timeofday DATETIME,
            index (country, gender, age, income)
            )
            """
        )
        with self.pool.connect() as db_conn:
            db_conn.execute(create_stmt)

    def insert_db(self, contents):
        insert_stmt = sqlalchemy.text(
            "INSERT INTO accesslogs (country, ip, gender, age, income, timeofday)\
            VALUES (:country, :ip, :gender, :age, :income, :timeofday)",
        )

        with self.pool.connect() as db_conn:
            db_conn.execute(insert_stmt, parameters=contents)
            db_conn.commit()

    def retrieve_db(self):
        query_stmt = sqlalchemy.text("SELECT * from accesslogs")
        with self.pool.connect() as db_conn:
            result = db_conn.execute(query_stmt).fetchall()
            return result

    def ip2long(self, ip):
        packedIP = socket.inet_aton(ip)
        return struct.unpack('!L', packedIP)[0]
    
    def long2ip(self, long):
        return socket.inet_ntoa(struct.pack('!L', long))

        
class MyServer(BaseHTTPRequestHandler):
    use_local_filesystem = True    
    sqlserver = None
    
    def publish_pub_sub(self, message):
        project_id = PROJECT_ID # 'cloudcomputingcourse-380619'
        topic_id = TOPIC_ID # project_id + '-mytopic'
        publisher = pubsub.PublisherClient()
        topic_path = publisher.topic_path(project_id, topic_id)
        data = message.encode('utf-8')
        future = publisher.publish(topic_path, data)
        future.result()
    
    def do_GET(self):
        country = self.headers['X-Country']
        if country in ['North Korea', 'Iran', 'Cuba', 'Myanmar', 'Iraq', 'Libya', 'Sudan', 'Zimbabwe', 'Syria']:
            if not self.use_local_filesystem:
                pass
                #publish_pub_sub('Banned country ' + country)
            else:
                print('Banned country ', country)
        ip = self.headers['X-Client-IP']
        bucket = None
        directory = None
        filename = None
        if self.use_local_filesystem:
            filename = "." + self.path
            self.send_local_response(filename)
        else:
            parts = self.path.split('/')
            bucket = parts[0]
            directory = parts[1]
            filename = parts[2]            
            self.send_gcs_response(bucket, directory, filename)

    def writeintodb(self, receive_headers):
        contents = {}
        headernames = ['X-country', 'X-client-IP', 'X-gender', 'X-age', 'X-income', 'X-time']
        fieldnames = ['country', 'ip', 'gender', 'age', 'income', 'timeofday']
        for key,header in zip(fieldnames,headernames):
            contents[key] = receive_headers[header]
        # Fix the ip so it is a number instead of a string
        contents['ip'] = self.sqlserver.ip2long(contents['ip'])
        contents['gender'] = (0 if contents['gender'] == 'Male' else 1)
        self.sqlserver.insert_db(contents)
            
            
    def send_gcs_response(self, bucket, directory, filename):
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
            # Write a log of the request into the database
            self.writeintodb(receive_headers)
        except:
            self.send_response(404)
            self.send_header("Content-type", "text/html")
            self.end_headers()
            self.wfile.write(bytes("<html><head><title>https://pythonbasics.org</title></head>", "utf-8"))
            self.wfile.write(bytes("<p>Request: %s</p>" % self.path, "utf-8"))
            self.wfile.write(bytes("<body>", "utf-8"))
            self.wfile.write(bytes("<p>File not found.</p>", "utf-8"))
            self.wfile.write(bytes("</body></html>", "utf-8"))
                
            
    def send_local_response(self, path):
        receive_headers = self.headers
        try:
            with open(path, "r") as f:
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
        self.send_response(500)
        self.end_headers()
        self.wfile.write(bytes("Server method unavailable", "utf-8"))
                    
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--domain", help="Domain to make requests to", type=str, default="localhost")
    parser.add_argument("-p", "--port", help="Server Port", type=int, default=8080)
    parser.add_argument("-l", "--local", help="Use local filesystem for data source", action="store_true")
    args = parser.parse_args()
    if not args.local:
        MyServer.use_local_filesystem = False

    sqlserver= MySqlServer()
    sqlserver.pool = sqlserver.connect_with_connector()
    sqlserver.create_table()
    MyServer.sqlserver = sqlserver
    
    webServer = HTTPServer((args.domain, args.port), MyServer)
    print("Server started http://%s:%s:%s" % (args.domain, args.port,args.local))

    try:
        webServer.serve_forever()
    except KeyboardInterrupt:
        pass

    webServer.server_close()
    print("Server stopped.")
        
if __name__ == "__main__":        
    main()


