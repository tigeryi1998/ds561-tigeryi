import functions_framework
import re
import flask
from flask import abort
from flask import request
# import curl 


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

    banned_countries = [
        'North Korea', 'Iran', 'Cuba', 'Myanmar', 
        'Iraq', 'Libya', 'Sudan', 'Zimbabwe', 'Syria'
    ]




    return None