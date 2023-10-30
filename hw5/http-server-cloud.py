#!/usr/local/bin/python3

from http.server import BaseHTTPRequestHandler, HTTPServer
import argparse
import time
import google.cloud.storage as storage
import google.cloud.pubsub as pubsub


class MyServer(BaseHTTPRequestHandler):

    def publish_pub_sub(self, message):
        project_id = 'cloudcomputingcourse-380619'
        topic_id = project_id + '-mytopic'
        publisher = pubsub.PublisherClient()
        topic_path = publisher.topic_path(project_id, topic_id)
        data = message.encode('utf-8')
        future = publisher.publish(topic_path, data)
        future.result()
    
    def do_GET(self):
        country = self.headers['X-Country']
        if country in ['North Korea', 'Iran', 'Cuba', 'Myanmar', 'Iraq', 'Libya', 'Sudan', 'Zimbabwe', 'Syria']:
            publish_pub_sub('Banned country ' + country)
        ip = self.headers['X-Client-IP']
        bucket = None
        directory = None
        filename = None
        parts = self.path.split('/')
        bucket = parts[1]
        directory = parts[2]
        filename = parts[3]            
        self.send_gcs_response(bucket, directory, filename)

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
    args = parser.parse_args()
    
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


