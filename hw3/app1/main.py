import functions_framework
import re
import flask
from flask import abort
from flask import request
# import curl 
# import http.client
# import os
# import time 
# import ssl 

@functions_framework.http
def hello_http(request):
    """HTTP Cloud Function.
    Args:
        request (flask.Request): The request object.
        <https://flask.palletsprojects.com/en/1.1.x/api/#incoming-request-data>
    Returns:
        The response text, or any set of values that can be turned into a
        Response object using `make_response`
        <https://flask.palletsprojects.com/en/1.1.x/api/#flask.make_response>.
    """
    pathname = request.path
    regex = r'(\d+).html'
    filenum = re.findall(regex, pathname)  
    filenum = filenum[0]
    filenum = int(filenum)

    if request.method == "GET":
      if filenum > 10000 or filenum < 0:
        abort(404)
      else:
        return("200-OK")
    elif request.method == "PUT":
      abort(501)
    else:
      abort(501)


