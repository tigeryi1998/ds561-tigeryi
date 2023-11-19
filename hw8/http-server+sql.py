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

import numpy as np


PROJECT_ID = "feisty-gasket-398719"
TOPIC_ID = "my-topic"
SUBSCRIPTION_NAME = "my-topic-sub"
INSTANCE_CONNECTION_NAME = "feisty-gasket-398719:us-east1:instance-tigeryi"
DB_USER = "root"
DB_PASS = ""
DB_NAME = "dbhw5"
DB_PRIVATE_IP = False
BANNED_COUNTRIES = ["North Korea", "Iran", "Cuba", "Myanmar", "Iraq", "Libya", "Sudan", "Zimbabwe", "Syria"]
HTTP_METHODS = ['GET', 'HEAD', 'POST', 'PUT', 'DELETE', 'CONNECT', 'OPTIONS', 'TRACE', 'PATCH']


list_of_countries = np.array([ 'Afghanistan', 'Albania', 'Algeria', 'Andorra',
                      'Angola', 'Antigua and Barbuda', 'Argentina', 'Armenia',
                      'Australia', 'Austria', 'Azerbaijan', 'Bahamas', 'Bahrain',
                      'Bangladesh', 'Barbados', 'Belarus', 'Belgium', 'Belize', 'Benin',
                      'Bhutan', 'Bolivia', 'Bosnia and Herzegovina', 'Botswana',
                      'Brazil', 'Brunei', 'Bulgaria', 'Burkina Faso', 'Burundi', 'Cabo Verde',
                      'Cambodia', 'Cameroon', 'Canada', 'Central African Republic',
                      'Chad', 'Chile', 'China', 'Colombia', 'Comoros',
                      'Congo, Democratic Republic of the', 'Congo, Republic of the',
                      'Costa Rica', 'Cote dIvoire', 'Croatia', 'Cuba', 'Cyprus',
                      'Czechia', 'Denmark', 'Djibouti', 'Dominica', 'Dominican Republic',
                      'Ecuador', 'Egypt', 'El Salvador', 'Equatorial Guinea',
                      'Eritrea', 'Estonia', 'Eswatini', 'Ethiopia', 'Fiji', 'Finland',
                      'France', 'Gabon', 'Gambia', 'Georgia', 'Germany', 'Ghana',
                      'Greece', 'Grenada', 'Guatemala', 'Guinea', 'Guinea-Bissau',
                      'Guyana', 'Haiti', 'Honduras', 'Hungary', 'Iceland', 'India',
                      'Indonesia', 'Iran', 'Iraq', 'Ireland', 'Israel', 'Italy',
                      'Jamaica', 'Japan', 'Jordan', 'Kazakhstan', 'Kenya', 'Kiribati',
                      'Kosovo', 'Kuwait', 'Kyrgyzstan', 'Laos', 'Latvia', 'Lebanon',
                      'Lesotho', 'Liberia', 'Libya', 'Liechtenstein', 'Lithuania',
                      'Luxembourg', 'Madagascar', 'Malawi', 'Malaysia', 'Maldives',
                      'Mali', 'Malta', 'Marshall Islands', 'Mauritania', 'Mauritius',
                      'Mexico', 'Micronesia', 'Moldova', 'Monaco', 'Mongolia',
                      'Montenegro', 'Morocco', 'Mozambique', 'Myanmar', 'Namibia',
                      'Nauru', 'Nepal', 'Netherlands', 'New Zealand', 'Nicaragua',
                      'Niger', 'Nigeria', 'North Korea', 'North Macedonia', 'Norway',
                      'Oman', 'Pakistan', 'Palau', 'Palestine', 'Panama',
                      'Papua New Guinea', 'Paraguay', 'Peru', 'Philippines', 'Poland', 'Portugal',
                      'Qatar', 'Romania', 'Russia', 'Rwanda', 'Saint Kitts and Nevis',
                      'Saint Lucia', 'Saint Vincent and the Grenadines', 'Samoa',
                      'San Marino', 'Sao Tome and Principe', 'Saudi Arabia', 'Senegal',
                      'Serbia', 'Seychelles', 'Sierra Leone', 'Singapore', 'Slovakia',
                      'Slovenia', 'Solomon Islands', 'Somalia', 'South Africa', 'South Korea',
                      'South Sudan', 'Spain', 'Sri Lanka', 'Sudan', 'Suriname',
                      'Sweden', 'Switzerland', 'Syria', 'Taiwan', 'Tajikistan',
                      'Tanzania', 'Thailand', 'Timor-Leste', 'Togo', 'Tonga',
                      'Trinidad and Tobago', 'Tunisia', 'Turkey', 'Turkmenistan', 'Tuvalu',
                      'Uganda', 'Ukraine', 'United Arab Emirates (UAE)', 'United Kingdom',
                      'United States of America (USA)', 'Uruguay', 'Uzbekistan',
                      'Vanuatu', 'Vatican City (Holy See)', 'Venezuela', 'Vietnam',
                      'Yemen', 'Zambia', 'Zimbabwe' ])
list_of_genders = np.array(['Male', 'Female']) 
list_of_ages = np.array(['0-16', '17-25', '26-35', '36-45', '46-55', '56-65', '66-75', '76+'])
list_of_incomes =  np.array(['0-10k', '10k-20k', '20k-40k', '40k-60k', '60k-100k', '100k-150k', '150k-250k', '250k+']
)

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

        ip_type = IPTypes.PRIVATE if DB_PRIVATE_IP else IPTypes.PUBLIC # if os.environ.get("DB_PRIVATE_IP")

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

    def create_table1(self): # NOT NULL UNIQUE
        create_stmt = sqlalchemy.text(
            """\
            CREATE TABLE IF NOT EXISTS table1(\
            request_id INT AUTO_INCREMENT, \
            ip VARCHAR(255), \
            time_of_day DATETIME, \
            filename VARCHAR(255), \
            ip2 INT UNSIGNED, \
            PRIMARY KEY (request_id));\
            """
        )
        with self.pool.connect() as db_conn:
            db_conn.execute(create_stmt)
            # db_conn.commit()

    def create_table2(self):
        create_stmt = sqlalchemy.text(
            """\
            CREATE TABLE IF NOT EXISTS table2(\
            ip_id INT AUTO_INCREMENT, \
            ip VARCHAR(255) NOT NULL UNIQUE, \
            gender VARCHAR(255), \
            age VARCHAR(255), \
            income VARCHAR(255), \
            country VARCHAR(255), \
            is_banned BOOLEAN, \
            ip2 INT UNSIGNED, \
            gender2 INT, \
            age2 INT, \
            income2 INT, \
            country2 INT, \
            index (gender2, age2, income2, country2), \
            PRIMARY KEY (ip_id,ip));\
            """
        )
        with self.pool.connect() as db_conn:
            db_conn.execute(create_stmt)
            # db_conn.commit()

    def create_table3(self):
        create_stmt = sqlalchemy.text(
            """\
            CREATE TABLE IF NOT EXISTS table3(\
            failed_id INT AUTO_INCREMENT, \
            ip VARCHAR(255), \
            time_of_day DATETIME, \
            filename VARCHAR(255), \
            error INT, \
            ip2 INT UNSIGNED, \
            PRIMARY KEY (failed_id));\
            """
        )
        with self.pool.connect() as db_conn:
            db_conn.execute(create_stmt)
            # db_conn.commit()

    def insert_table1(self, contents):
        insert_stmt = sqlalchemy.text(
            "INSERT INTO table1 (ip, time_of_day, filename, ip2) \
            VALUES (:ip, :time_of_day, :filename, :ip2)",
        )
        with self.pool.connect() as db_conn:
            db_conn.execute(insert_stmt, contents) # parameters=
            # db_conn.commit()

    def insert_table2(self, contents):
        insert_stmt = sqlalchemy.text(
            "INSERT INTO table2 (ip, gender, age, income, country, is_banned, ip2, gender2, age2, income2, country2) \
            VALUES (:ip, :gender, :age, :income, :country, :is_banned, :ip2, :gender2, :age2, :income2, :country2) \
            ON DUPLICATE KEY UPDATE ip=ip",
        )
        with self.pool.connect() as db_conn:
            db_conn.execute(insert_stmt, contents) #parameters=
            # db_conn.commit()

    def insert_table3(self, contents):
        insert_stmt = sqlalchemy.text(
            "INSERT INTO table3 (ip, time_of_day, filename, error, ip2) \
            VALUES (:ip, :time_of_day, :filename, :error, :ip2)",
        )
        with self.pool.connect() as db_conn:
            db_conn.execute(insert_stmt, contents) # parameters=
            # db_conn.commit()

    def retrieve_table1(self):
        query_stmt = sqlalchemy.text("SELECT * from table1")
        with self.pool.connect() as db_conn:
            result = db_conn.execute(query_stmt).fetchall()
            return result
        
    def retrieve_table2(self):
        query_stmt = sqlalchemy.text("SELECT * from table2")
        with self.pool.connect() as db_conn:
            result = db_conn.execute(query_stmt).fetchall()
            return result
        
    def retrieve_table3(self):
        query_stmt = sqlalchemy.text("SELECT * from table3")
        with self.pool.connect() as db_conn:
            result = db_conn.execute(query_stmt).fetchall()
            return result

    def ip2long(self, ip):
        packedIP = socket.inet_aton(ip)
        result = struct.unpack('!L', packedIP)[0]
        return int(result)
    
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
        if country in BANNED_COUNTRIES:
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

    def writeintodb(self, receive_headers, filename, error=None):
        contents = {}
        headernames = ['X-client-IP', 'X-gender', 'X-age', 'X-income', 'X-country']
        fieldnames = ['ip', 'gender', 'age', 'income', 'country']
        for key,header in zip(fieldnames,headernames):
            contents[key] = receive_headers[header]
        
        # Fix the ip so it is a number instead of a string
        contents['ip2'] = self.sqlserver.ip2long(contents['ip'])

        ip = receive_headers['X-client-IP']
        gender = receive_headers['X-gender']
        age = receive_headers['X-age']
        income = receive_headers['X-income']
        country = receive_headers['X-Country']
        contents['is_banned'] = (1 if country in BANNED_COUNTRIES else 0)

        if country in list_of_countries:
            country2 = np.where(list_of_countries == country)[0][0]
            contents['country2'] = country2

        if gender in list_of_genders:
            gender2 = np.where(list_of_genders == gender)[0][0]
            contents['gender2'] = gender2

        if age in list_of_ages:
            age2 = np.where(list_of_ages == age)[0][0]
            contents['age2'] = age2

        if income in list_of_incomes:
            income2 = np.where(list_of_incomes == income)[0][0]
            contents['income2'] = income2

        self.sqlserver.insert_table2(contents)


        # table 1, 3

        contents2 = {}
        headernames2 = ['X-client-IP', 'X-time']
        fieldnames2 = ['ip', 'time_of_day']
        for key2,header2 in zip(fieldnames2,headernames2):
            contents2[key2] = receive_headers[header2]
        
        # Fix the ip so it is a number instead of a string
        contents2['ip2'] = self.sqlserver.ip2long(contents['ip'])

        contents2['filename'] = filename
        if error==None:
            self.sqlserver.insert_table1(contents2)
        else:
            contents2['error'] = error
            self.sqlserver.insert_table3(contents2)
            
            
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
            if self.sqlserver != None: 
                self.writeintodb(receive_headers, filename, error=None)

        except:
            self.send_response(404)
            self.send_header("Content-type", "text/html")
            self.end_headers()
            self.wfile.write(bytes("<html><head><title>https://pythonbasics.org</title></head>", "utf-8"))
            self.wfile.write(bytes("<p>Request: %s</p>" % self.path, "utf-8"))
            self.wfile.write(bytes("<body>", "utf-8"))
            self.wfile.write(bytes("<p>File not found.</p>", "utf-8"))
            self.wfile.write(bytes("</body></html>", "utf-8"))

            # Write a log of the request into the database
            if self.sqlserver != None: 
                self.writeintodb(receive_headers, filename, error=404)
                
            
    def send_local_response(self, path):
        receive_headers = self.headers
        parts = path.split('/')
        bucket = parts[0]
        directory = parts[1]
        filename = parts[2]
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

            # Write a log of the request into the database
            if self.sqlserver != None: 
                self.writeintodb(receive_headers, filename, error=None)

        except:
            self.send_response(404)
            self.send_header("Content-type", "text/html")
            self.end_headers()
            self.wfile.write(bytes("<html><head><title>https://pythonbasics.org</title></head>", "utf-8"))
            self.wfile.write(bytes("<p>Request: %s</p>" % self.path, "utf-8"))
            self.wfile.write(bytes("<body>", "utf-8"))
            self.wfile.write(bytes("<p>File not found.</p>", "utf-8"))
            self.wfile.write(bytes("</body></html>", "utf-8"))

            # Write a log of the request into the database
            if self.sqlserver != None: 
                self.writeintodb(receive_headers, filename, error=404)

    def do_PUT(self):
        bucket = None
        directory = None
        filename = None
        if self.use_local_filesystem:
            path = "." + self.path
            parts = path.split('/')
            bucket = parts[0]
            directory = parts[1]
            filename = parts[2]
        else:
            parts = self.path.split('/')
            bucket = parts[0]
            directory = parts[1]
            filename = parts[2]
        self.send500error(filename)

    def do_POST(self):
        bucket = None
        directory = None
        filename = None
        if self.use_local_filesystem:
            path = "." + self.path
            parts = path.split('/')
            bucket = parts[0]
            directory = parts[1]
            filename = parts[2]
        else:
            parts = self.path.split('/')
            bucket = parts[0]
            directory = parts[1]
            filename = parts[2]
        self.send500error(filename)

    def do_HEAD(self):
        bucket = None
        directory = None
        filename = None
        if self.use_local_filesystem:
            path = "." + self.path
            parts = path.split('/')
            bucket = parts[0]
            directory = parts[1]
            filename = parts[2]
        else:
            parts = self.path.split('/')
            bucket = parts[0]
            directory = parts[1]
            filename = parts[2]
        self.send500error(filename)

    def do_DELETE(self):
        bucket = None
        directory = None
        filename = None
        if self.use_local_filesystem:
            path = "." + self.path
            parts = path.split('/')
            bucket = parts[0]
            directory = parts[1]
            filename = parts[2]
        else:
            parts = self.path.split('/')
            bucket = parts[0]
            directory = parts[1]
            filename = parts[2]
        self.send500error(filename)

    def send500error(self, filename=None):
        self.send_response(500)
        self.end_headers()
        self.wfile.write(bytes("Server method unavailable", "utf-8"))
        # Write a log of the request into the database
        receive_headers = self.headers
        self.writeintodb(receive_headers, filename, error=500)

                    
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
    sqlserver.create_table1()
    sqlserver.create_table2()
    sqlserver.create_table3()
    MyServer.sqlserver = sqlserver
    
    webServer = HTTPServer((args.domain, args.port), MyServer)
    print("Server started http://%s:%s" % (args.domain, args.port)) # http://%s:%s:%s # ,args.local

    try:
        webServer.serve_forever()
    except KeyboardInterrupt:
        pass

    webServer.server_close()
    print("Server stopped.")
        
if __name__ == "__main__":        
    main()


