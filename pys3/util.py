from StringIO import StringIO
import exceptions
from lib import S3
from S3Errors import *

__all__ = [
       "check_http_response",
       "force_delete_bucket"
]


def check_http_response(response, http_code=None):
    """ Check the HTTP Reponse for any errors, raise a S3ResponseError if found.
        optionally give a specific http_code to look for """
    
    if http_code and response.http_response.status != http_code:
        raise S3ResponseError, response
    
    if response.http_response.status > 300:
        raise S3ResponseError, response

def force_delete_bucket(conn, bucket_name):
    r = conn.list_bucket(bucket_name)
    check_http_response(r)
    
    for entry in r.entries:
        r = conn.delete(bucket_name, entry.key)
        check_http_response(r)
    
    r = conn.delete_bucket(bucket_name)
    check_http_response(r)
    
    return r
     
        
    
    
    