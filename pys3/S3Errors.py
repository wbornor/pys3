import exceptions

__all__ = [
       "S3Error",
       "S3ResponseError",
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