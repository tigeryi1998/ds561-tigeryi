import functions_framework
import re
import flask
from flask import abort

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
    filename = request.path
    regex = '\d+' 
    filenum = re.findall(regex, filename)  
    filenum = int(filenum)

    if request.method != "GET":
      abort(501, '501-not implemented')
    else:
      if filenum >= 10000 or filenum <= 0:
        abort(404, '404-not found')
      else:
        return("200-OK") 


