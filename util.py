from lib import S3
from StringIO import StringIO
import exceptions

class S3IOError(exceptions.Exception): pass

class ResponseError(S3IOError):
    def __init__(self, response):
        self.response = response
        self.status = response.http_response.status
        self.reason = response.http_response.reason
        self.body = response.body
        
    def __str__(self):
        return "%s - %s\n%s\n" % (self.status, self.reason, self.body)

def check_http_response(response, http_code=None):
    """ Check the HTTP Reponse for any errors, raise a ResponseError if found.
        optionally give a specific http_code to look for """
    
    if http_code and response.http_response.status != http_code:
        raise ResponseError, response
    
    if response.http_response.status > 300:
        raise ResponseError, response

def force_delete_bucket(conn, bucket_name):
    r = conn.list_bucket(bucket_name)
    check_http_response(r)
    
    for entry in r.entries:
        r = conn.delete(bucket_name, entry.key)
        check_http_response(r)
    
    r = conn.delete_bucket(bucket_name)
    check_http_response(r)
    
    return r
    
class S3IO(StringIO):
    def __init__(self, conn, bucket_name, object_name, meta={}, buf=''):
        StringIO.__init__(self, buf)
        
        self.MAX_OBJECT_SIZE = 5368709120 #5GB
        self.conn = conn
        self.bucket_name = bucket_name
        self.object_name = object_name
        self.meta = meta
        self.sent_len = 0 #num bytes that have been sent to Amazon
        self.get_complete = False #tells if the get of the object has been completed
        
        if buf:
            self.dirty = True
        else:
            self.dirty = False #any unflushed write will set dirty to true
        
        #make sure this bucket exists, otherwise create it
        response = self.conn.list_bucket(bucket_name)
        if response.http_response.status == 404:
            response = self.conn.create_bucket(bucket_name)
            check_http_response(response, 200)
        elif response.http_response.status != 200:
            raise ResponseError, response
        
    def _get_object(self):
        """ Retrieve the entire object from S3 and write it to the internal buffer. 
            If the object has already been retrieved or the buffer is dirty 
            it won't retrieve the object. """
            
        if not self.dirty and not self.get_complete:
            r = self.conn.get(self.bucket_name, self.object_name)
            self.get_complete = True
            
            if r.http_response.status == 404:
                return  #the object doesn't exist so just return
            
            check_http_response(r)
            self.write(r.object.data)
            self.seek(0)
            self.dirty = False
        
    def seek(self, pos, mode = 0):
        self._get_object()
        return StringIO.seek(self, pos, mode)
            
    def read(self, n = -1):
        self._get_object()
        return StringIO.read(self, n)
    
    def readline(self, length=None):
        self._get_object()
        return StringIO.readline(self, length)
    
    def truncate(self, size=None):
        self._get_object()
        self.dirty = True
        StringIO.truncate(self, size)
    
    def write(self, s):
        self.dirty = True
        StringIO.write(self, s)
        
    def flush(self):
        """ Write the whole buffer to Amazon's server, overwriting any existing object. """
        StringIO.flush(self)
        
        if not self.dirty:
            return #nothing has been written to the buffer, so there isn't anything to flush
        
        if self.len == 0:
            raise S3IOError("String length must be greater than zero.")
        
        if self.len > self.MAX_OBJECT_SIZE:
            raise S3IOError("String length must not exceed %s bytes." % self.MAX_OBJECT_SIZE)
        
        #if self.sent_len == self.len:
        #    return 
        
        obj = self.getvalue()
        
        #write the full buffer    
        response = self.conn.put(self.bucket_name,
                                 self.object_name,
                                 S3.S3Object(str(obj)),
                                 self.meta)
            
        if response.http_response.status != 200:
            raise ResponseError, response            
        
        self.sent_len = self.len
        self.dirty = False
            
    def close(self):
        self.flush()
        StringIO.close(self)  
                

        
    
        
    
    
    