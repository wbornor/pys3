from StringIO import StringIO
import exceptions
from lib import S3

__all__ = [
       "S3Error",
       "S3ResponseError", 
       "check_http_response",
       "force_delete_bucket"
]

class S3Error(exceptions.Exception): pass

class S3ResponseError(S3Error):
    def __init__(self, response):
        self.response = response
        self.status = response.http_response.status
        self.reason = response.http_response.reason
        self.body = response.body
        
    def __str__(self):
        return "%s - %s\n%s\n" % (self.status, self.reason, self.body)

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
     
        
    
    
    