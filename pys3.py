from StringIO import StringIO
import logging
import exceptions
import pickle
from datetime import datetime, timedelta


class S3Error(exceptions.Exception): pass
class S3IOError(S3Error): pass
class S3ArchiveError(S3Error): pass


class S3ResponseError(S3Error):
    def __init__(self, response):
        self.response = response
        self.status = response.http_response.status
        self.reason = response.http_response.reason
        self.body = response.body
        
    def __str__(self):
        return "%s - %s\n%s\n" % (self.status, self.reason, self.body)

def get_conn():
    if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
        return S3.AWSAuthConnection(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    else:
        raise S3Error("Global variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY not set.")

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




class S3IO(StringIO):
    """ read and write to an S3 object as if it were a StringIO object """
    
    def __init__(self, conn, bucket_name, object_name, meta={}, buf=''):
        StringIO.__init__(self, buf)
        
        self.MAX_OBJECT_SIZE = 5368709120 #5GB
        self.conn = conn
        self.bucket_name = bucket_name
        self.object_name = object_name
        self.key = '%s/%s' % (self.bucket_name, self.object_name)
        self.meta = meta
        self.sent_len = 0 #num bytes that have been sent to Amazon
        self.get_complete = False #tells if the get of the object has been completed
        self.closed = False
        
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
            raise S3ResponseError, response
    
    def __str__(self):
        return self.key
        
    def _get_object(self):
        """ Retrieve the entire object from S3 and write it to the internal buffer. 
            If the object has already been retrieved or the buffer is dirty 
            it won't retrieve the object. """
            
        if not self.dirty and not self.get_complete:
            logging.info('reading %s.%s' % (self.bucket_name, self.object_name))
            r = self.conn.get(self.bucket_name, self.object_name)
            self.get_complete = True
            
            if r.http_response.status == 404:
                return  #the object doesn't exist so just return
            
            check_http_response(r)
            logging.debug('read successful')
            self.write(r.object.data)
            #TODO - set meta data
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
        
        logging.info('flushing %s.%s meta: %s' % (self.bucket_name, self.object_name, self.meta))
        
        #write the full buffer    
        response = self.conn.put(self.bucket_name,
                                 self.object_name,
                                 S3Object(str(obj)),
                                 self.meta)
            
        if response.http_response.status != 200:
            raise S3ResponseError, response            
        
        logging.debug('flush successful')
        self.sent_len = self.len
        self.dirty = False
            
    def close(self):
        if not self.closed:
            self.flush()
            StringIO.close(self)  
                
    def __del__(self):
        self.close()
        
s3io = S3IO



class S3ArchiveIO(S3IO):
    """ an version of a logical object """ 
    
    def __init__(self, rkiv, meta={}, buf='', fqon=None, logical_date=None):
        self.rkiv = rkiv
        self.object_prefix = rkiv.object_prefix
        self.closed = False
        
        if fqon:
            self.fqon = fqon
            junk, self.logical_date, self.physical_date = fqon.rsplit('.',2)
        else:
            self.now = time.localtime()
            if logical_date:
                self.logical_date = time.strftime('%Y%m%d', logical_date)
            else: 
                self.logical_date = time.strftime('%Y%m%d', self.now)
                
            self.physical_date = time.strftime('%Y%m%d%H%M%S', self.now)
            self.fqon = '%s.%s.%s' % (self.object_prefix, self.logical_date, self.physical_date) #fully qualified object name
        
        meta['s3archive_logical_date'] = self.logical_date
        meta['s3archive_physical_date'] = self.physical_date
        meta['s3archive_object_prefix'] = self.object_prefix
        
        S3IO.__init__(self, rkiv.conn, rkiv.bucket_name, self.fqon, meta, buf)
            
    def close(self):
        if not self.closed:
            S3IO.close(self)
            
s3archiveio = S3ArchiveIO
        
class S3Archive:
    """ manage historical versions of an object, automatically handles retention """   
        
    def __init__(self, conn, bucket_name, object_prefix):

        if object_prefix.count('.'):
            raise S3ArchiveError("object_prefix cannot contain any periods.")
        
        self.conn = conn
        self.bucket_name = bucket_name
        self.object_prefix = object_prefix
        self.props = None
        self.days = None
        self.copies = None
    
    def __str__(self):
        return '<S3Archive - %s.%s>' % (self.bucket_name, self.object_prefix)
    
    def __repr__(self):
        return __str__()
    
    def _get_props(self):
        io = S3IO(self.conn, self.bucket_name, self.object_prefix+'.props')
        props = pickle.load(io)
        logging.debug('contents of %s.props: %s' % (self.object_prefix, props))
        return props

    def set_retention(self, days=400, copies=10):
        self.props = {'days':days, 'copies':copies}
        self.days = days
        self.copies = copies
        io = S3IO(self.conn, self.bucket_name, self.object_prefix+'.props')
        pickle.dump(self.props, io)
        io.close()
    
    def new_io(self, logical_date=None):
        """ get a new instance of this logical object """
        
        if logical_date and type(logical_date) != time.struct_time:
            raise TypeError("logical_date must be of type time.struct_time not %s" % (type(logical_date)))
        
        return S3ArchiveIO(self, logical_date=logical_date)
    

    def existing_io(self, fqon=None, logical_date=None, physical_date=None):
        """ find an existing instance of this logical object
            if no parameters are given, get the most recent addition to the archive """
        
        if fqon:
            return(S3ArchiveIO(self, fqon=fqon))
        
        if logical_date:
            if type(logical_date) != time.struct_time:
                raise TypeError("logical_date must be of type time.struct_time not %s" % (type(logical_date)))
            
            logical_date = time.strftime('%Y%m%d', logical_date)
            
        if physical_date:
            if type(physical_date) != time.struct_time:
                raise TypeError("physical_date must be of type time.struct_time not %s" % (type(physical_date)))
            
            physical_date = time.strftime('%Y%m%d%H%M%S', physical_date)
        
        if logical_date and physical_date:
            fqon = '%s.%s.%s' % (self.object_prefix, logical_date, physical_date)
            f = self.list(options={'prefix': fqon})
            if not f:
                raise S3ArchiveError("Match not found.")
            
            return S3ArchiveIO(self, fqon=f[0])
         
        now = time.strftime('%Y%m%d%H%M%S')
            
        if physical_date:
            #try to find the most recent existing match
            fqons = self.list()
            for fqon in fqons:
                if fqon[-14:] == physical_date:
                    return S3ArchiveIO(self, fqon=fqon)
            
            #no matches, raise an error
            raise S3ArchiveError("No matches found.")
                
        elif logical_date:
            #try to find the most recent existing match
            fqons = self.list(options={'prefix':'%s.%s' % (self.object_prefix, logical_date)})
            if fqons:
                #fqons is now a list of all the same logical_dates, get the most recent physical_date
                return S3ArchiveIO(self, fqon=fqons[-1])
           
            #no matches, raise an error
            raise S3ArchiveError("No matches found.")
            
        else:
            #return the most recent addition to the archive
            return S3ArchiveIO(self, fqon=self.list()[-1])
        
    def list(self, options=None):
        """ list all the instances of this logical object
            options is a list that is sent in the request to the webservice"""
            
        if not options:
            options = {'prefix': self.object_prefix+'.'}
        
        is_truncated = True #on the first time, assume the response is truncated
        fqons = []
        
        while is_truncated:
            logging.debug('listing the contents of \'%s\' with options \'%s\'' % (self.bucket_name, options))
            r = self.conn.list_bucket(self.bucket_name, options=options)
            if r.http_response.status == 404:
                # bucket doesn't exist, return empty list
                return []
            
            check_http_response(r)
            fqons.extend([list_entry.key for list_entry in r.entries])
            is_truncated = r.is_truncated
            if is_truncated:
                options['marker'] = r.entries[-1].key 
        
        if fqons and fqons[-1] == self.object_prefix + '.props':
            fqons.pop()
            
        logging.debug(fqons)
        return fqons
    
    def scratch(self):
        """ frees stale objects from this archive 
            objects must meet two conditions before being freed:
                -their logical age must be older than self.days 
                -they contribute to a total instance count that is greater than self.copies """
        
        if not self.props:
            try:
                self.props = self._get_props()
                self.days = self.props['days'] #if days is <= 0 then keep indefinatly
                self.copies = self.props['copies'] #if copies is <= 0 then keep indefinatly 
                
            except EOFError:
                #props file not set
                self.set_retention()
        
        logging.info("starting scratch() days:%s, copies:%s" % (self.days, self.copies))
                    
        fqons = self.list()
        logging.debug('number of copies: %d' % (len(fqons)))
        if self.copies >= 0 and len(fqons) > self.copies:
            for fqon in fqons[:len(fqons)-self.copies]:
                object_prefix, logical_date, physical_date = fqon.rsplit('.', 2)
                age = (datetime.now() - datetime.strptime(logical_date, '%Y%m%d')).days
                logging.debug('age: %d' % (age))
                if self.days == 0:
                    #delete object
                    logging.info("deleting stale object:" + fqon)
                    r = self.conn.delete(self.bucket_name, fqon)
                    check_http_response(r)
                    
                elif self.days > 0 and age > self.days:
                    #delete object
                    logging.info("deleting stale object:" + fqon)
                    r = self.conn.delete(self.bucket_name, fqon)
                    check_http_response(r)
                
    
    def __del__(self):
        self.scratch()
        

s3archive = S3Archive



#From Amazon.com AWS
#  This software code is made available "AS IS" without warranties of any
#  kind.  You may copy, display, modify and redistribute the software
#  code either by itself or as incorporated into your code; provided that
#  you do not remove any proprietary notices.  Your use of this software
#  code is at your own risk and you waive any claim against Amazon
#  Digital Services, Inc. or its affiliates with respect to your use of
#  this software code. (c) 2006 Amazon Digital Services, Inc. or its
#  affiliates.

import base64
import hmac
import httplib
import re
import sha
import sys
import time
import urllib
import xml.sax

DEFAULT_HOST = 's3.amazonaws.com'
PORTS_BY_SECURITY = { True: 443, False: 80 }
METADATA_PREFIX = 'x-amz-meta-'
AMAZON_HEADER_PREFIX = 'x-amz-'

# generates the aws canonical string for the given parameters
def canonical_string(method, path, headers, expires=None):
    interesting_headers = {}
    for key in headers:
        lk = key.lower()
        if lk in ['content-md5', 'content-type', 'date'] or lk.startswith(AMAZON_HEADER_PREFIX):
            interesting_headers[lk] = headers[key].strip()

    # these keys get empty strings if they don't exist
    if not interesting_headers.has_key('content-type'):
        interesting_headers['content-type'] = ''
    if not interesting_headers.has_key('content-md5'):
        interesting_headers['content-md5'] = ''

    # just in case someone used this.  it's not necessary in this lib.
    if interesting_headers.has_key('x-amz-date'):
        interesting_headers['date'] = ''

    # if you're using expires for query string auth, then it trumps date
    # (and x-amz-date)
    if expires:
        interesting_headers['date'] = str(expires)

    sorted_header_keys = interesting_headers.keys()
    sorted_header_keys.sort()

    buf = "%s\n" % method
    for key in sorted_header_keys:
        if key.startswith(AMAZON_HEADER_PREFIX):
            buf += "%s:%s\n" % (key, interesting_headers[key])
        else:
            buf += "%s\n" % interesting_headers[key]

    # don't include anything after the first ? in the resource...
    buf += "/%s" % path.split('?')[0]

    # ...unless there is an acl or torrent parameter
    if re.search("[&?]acl($|=|&)", path):
        buf += "?acl"
    elif re.search("[&?]torrent($|=|&)", path):
        buf += "?torrent"
    elif re.search("[&?]logging($|=|&)", path):
        buf += "?logging"

    return buf

# computes the base64'ed hmac-sha hash of the canonical string and the secret
# access key, optionally urlencoding the result
def encode(aws_secret_access_key, str, urlencode=False):
    b64_hmac = base64.encodestring(hmac.new(aws_secret_access_key, str, sha).digest()).strip()
    if urlencode:
        return urllib.quote_plus(b64_hmac)
    else:
        return b64_hmac

def merge_meta(headers, metadata):
    final_headers = headers.copy()
    for k in metadata.keys():
        final_headers[METADATA_PREFIX + k] = metadata[k]

    return final_headers



class AWSAuthConnection:
    def __init__(self, aws_access_key_id, aws_secret_access_key, is_secure=True,
                 server=DEFAULT_HOST, port=None):

        if not port:
            port = PORTS_BY_SECURITY[is_secure]

        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        if (is_secure):
            self.connection = httplib.HTTPSConnection("%s:%d" % (server, port))
        else:
            self.connection = httplib.HTTPConnection("%s:%d" % (server, port))


    def create_bucket(self, bucket, headers={}):
        return Response(self.make_request('PUT', bucket, headers))

    def list_bucket(self, bucket, options={}, headers={}):
        path = bucket
        if options:
            path += '?' + '&'.join(["%s=%s" % (param, urllib.quote_plus(str(options[param]))) for param in options])

        return ListBucketResponse(self.make_request('GET', path, headers))

    def delete_bucket(self, bucket, headers={}):
        return Response(self.make_request('DELETE', bucket, headers))

    def put(self, bucket, key, object, headers={}):
        if not isinstance(object, S3Object):
            object = S3Object(object)

        return Response(
                self.make_request(
                    'PUT',
                    '%s/%s' % (bucket, urllib.quote_plus(key)),
                    headers,
                    object.data,
                    object.metadata))

    def get(self, bucket, key, headers={}):
        return GetResponse(
                self.make_request('GET', '%s/%s' % (bucket, urllib.quote_plus(key)), headers))

    def delete(self, bucket, key, headers={}):
        return Response(
                self.make_request('DELETE', '%s/%s' % (bucket, urllib.quote_plus(key)), headers))

    def get_bucket_logging(self, bucket, headers={}):
        return GetResponse(self.make_request('GET', '%s?logging' % (bucket), headers))

    def put_bucket_logging(self, bucket, logging_xml_doc, headers={}):
        return Response(self.make_request('PUT', '%s?logging' % (bucket), headers, logging_xml_doc))

    def get_bucket_acl(self, bucket, headers={}):
        return self.get_acl(bucket, '', headers)

    def get_acl(self, bucket, key, headers={}):
        return GetResponse(
                self.make_request('GET', '%s/%s?acl' % (bucket, urllib.quote_plus(key)), headers))

    def put_bucket_acl(self, bucket, acl_xml_document, headers={}):
        return self.put_acl(bucket, '', acl_xml_document, headers)

    def put_acl(self, bucket, key, acl_xml_document, headers={}):
        return Response(
                self.make_request(
                    'PUT',
                    '%s/%s?acl' % (bucket, urllib.quote_plus(key)),
                    headers,
                    acl_xml_document))

    def list_all_my_buckets(self, headers={}):
        return ListAllMyBucketsResponse(self.make_request('GET', '', headers))

    def make_request(self, method, path, headers={}, data='', metadata={}):
        final_headers = merge_meta(headers, metadata);
        # add auth header
        self.add_aws_auth_header(final_headers, method, path)

        self.connection.request(method, "/%s" % path, data, final_headers)
        return self.connection.getresponse()


    def add_aws_auth_header(self, headers, method, path):
        if not headers.has_key('Date'):
            headers['Date'] = time.strftime("%a, %d %b %Y %X GMT", time.gmtime())

        c_string = canonical_string(method, path, headers)
        headers['Authorization'] = \
            "AWS %s:%s" % (self.aws_access_key_id, encode(self.aws_secret_access_key, c_string))


class QueryStringAuthGenerator:
    # by default, expire in 1 minute
    DEFAULT_EXPIRES_IN = 60

    def __init__(self, aws_access_key_id, aws_secret_access_key, is_secure=True,
                 server=DEFAULT_HOST, port=None):

        if not port:
            port = PORTS_BY_SECURITY[is_secure]

        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        if (is_secure):
            self.protocol = 'https'
        else:
            self.protocol = 'http'

        self.server_name = "%s:%d" % (server, port)
        self.__expires_in = QueryStringAuthGenerator.DEFAULT_EXPIRES_IN
        self.__expires = None

    def set_expires_in(self, expires_in):
        self.__expires_in = expires_in
        self.__expires = None

    def set_expires(self, expires):
        self.__expires = expires
        self.__expires_in = None

    def create_bucket(self, bucket, headers={}):
        return self.generate_url('PUT', bucket, headers)

    def list_bucket(self, bucket, options={}, headers={}):
        path = bucket
        if options:
            path += '?' + '&'.join(["%s=%s" % (param, urllib.quote_plus(options[param])) for param in options])

        return self.generate_url('GET', path, headers)

    def delete_bucket(self, bucket, headers={}):
        return self.generate_url('DELETE', bucket, headers)

    def put(self, bucket, key, object, headers={}):
        if not isinstance(object, S3Object):
            object = S3Object(object)

        return self.generate_url(
                'PUT',
                '%s/%s' % (bucket, urllib.quote_plus(key)),
                merge_meta(headers, object.metadata))

    def get(self, bucket, key, headers={}):
        return self.generate_url('GET', '%s/%s' % (bucket, urllib.quote_plus(key)), headers)

    def delete(self, bucket, key, headers={}):
        return self.generate_url('DELETE', '%s/%s' % (bucket, urllib.quote_plus(key)), headers)

    def get_bucket_logging(self, bucket, headers={}):
        return self.generate_url('GET', '%s?logging' % (bucket), headers)

    def put_bucket_logging(self, bucket, logging_xml_doc, headers={}):
        return self.generate_url('PUT', '%s?logging' % (bucket), headers)

    def get_bucket_acl(self, bucket, headers={}):
        return self.get_acl(bucket, '', headers)

    def get_acl(self, bucket, key='', headers={}):
        return self.generate_url('GET', '%s/%s?acl' % (bucket, urllib.quote_plus(key)), headers)

    def put_bucket_acl(self, bucket, acl_xml_document, headers={}):
        return self.put_acl(bucket, '', acl_xml_document, headers)

    # don't really care what the doc is here.
    def put_acl(self, bucket, key, acl_xml_document, headers={}):
        return self.generate_url('PUT', '%s/%s?acl' % (bucket, urllib.quote_plus(key)), headers)

    def list_all_my_buckets(self, headers={}):
        return self.generate_url('GET', '', headers)

    def make_bare_url(self, bucket, key=''):
        return self.protocol + '://' + self.server_name + '/' + bucket + '/' + key

    def generate_url(self, method, path, headers):
        expires = 0
        if self.__expires_in != None:
            expires = int(time.time() + self.__expires_in)
        elif self.__expires != None:
            expires = int(self.__expires)
        else:
            raise "Invalid expires state"

        canonical_str = canonical_string(method, path, headers, expires)
        encoded_canonical = encode(self.aws_secret_access_key, canonical_str, True)

        if '?' in path:
            arg_div = '&'
        else:
            arg_div = '?'

        query_part = "Signature=%s&Expires=%d&AWSAccessKeyId=%s" % (encoded_canonical, expires, self.aws_access_key_id)

        return self.protocol + '://' + self.server_name + '/' + path  + arg_div + query_part



class S3Object:
    def __init__(self, data, metadata={}):
        self.data = data
        self.metadata = metadata

class Owner:
    def __init__(self, id='', display_name=''):
        self.id = id
        self.display_name = display_name

class ListEntry:
    def __init__(self, key='', last_modified=None, etag='', size=0, storage_class='', owner=None):
        self.key = key
        self.last_modified = last_modified
        self.etag = etag
        self.size = size
        self.storage_class = storage_class
        self.owner = owner

class CommonPrefixEntry:
    def __init(self, prefix=''):
        self.prefix = prefix

class Bucket:
    def __init__(self, name='', creation_date=''):
        self.name = name
        self.creation_date = creation_date

class Response:
    def __init__(self, http_response):
        self.http_response = http_response
        # you have to do this read, even if you don't expect a body.
        # otherwise, the next request fails.
        self.body = http_response.read()

class ListBucketResponse(Response):
    def __init__(self, http_response):
        Response.__init__(self, http_response)
        if http_response.status < 300:
            handler = ListBucketHandler()
            xml.sax.parseString(self.body, handler)
            self.entries = handler.entries
            self.common_prefixes = handler.common_prefixes
            self.name = handler.name
            self.marker = handler.marker
            self.prefix = handler.prefix
            self.is_truncated = handler.is_truncated
            self.delimiter = handler.delimiter
            self.max_keys = handler.max_keys
            self.next_marker = handler.next_marker
        else:
            self.entries = []

class ListAllMyBucketsResponse(Response):
    def __init__(self, http_response):
        Response.__init__(self, http_response)
        if http_response.status < 300: 
            handler = ListAllMyBucketsHandler()
            xml.sax.parseString(self.body, handler)
            self.entries = handler.entries
        else:
            self.entries = []

class GetResponse(Response):
    def __init__(self, http_response):
        Response.__init__(self, http_response)
        response_headers = http_response.msg   # older pythons don't have getheaders
        metadata = self.get_aws_metadata(response_headers)
        self.object = S3Object(self.body, metadata)

    def get_aws_metadata(self, headers):
        metadata = {}
        for hkey in headers.keys():
            if hkey.lower().startswith(METADATA_PREFIX):
                metadata[hkey[len(METADATA_PREFIX):]] = headers[hkey]
                del headers[hkey]

        return metadata

class ListBucketHandler(xml.sax.ContentHandler):
    def __init__(self):
        self.entries = []
        self.curr_entry = None
        self.curr_text = ''
        self.common_prefixes = []
        self.curr_common_prefix = None
        self.name = ''
        self.marker = ''
        self.prefix = ''
        self.is_truncated = False
        self.delimiter = ''
        self.max_keys = 0
        self.next_marker = ''
        self.is_echoed_prefix_set = False

    def startElement(self, name, attrs):
        if name == 'Contents':
            self.curr_entry = ListEntry()
        elif name == 'Owner':
            self.curr_entry.owner = Owner()
        elif name == 'CommonPrefixes':
            self.curr_common_prefix = CommonPrefixEntry()
            

    def endElement(self, name):
        if name == 'Contents':
            self.entries.append(self.curr_entry)
        elif name == 'CommonPrefixes':
            self.common_prefixes.append(self.curr_common_prefix)
        elif name == 'Key':
            self.curr_entry.key = self.curr_text
        elif name == 'LastModified':
            self.curr_entry.last_modified = self.curr_text
        elif name == 'ETag':
            self.curr_entry.etag = self.curr_text
        elif name == 'Size':
            self.curr_entry.size = int(self.curr_text)
        elif name == 'ID':
            self.curr_entry.owner.id = self.curr_text
        elif name == 'DisplayName':
            self.curr_entry.owner.display_name = self.curr_text
        elif name == 'StorageClass':
            self.curr_entry.storage_class = self.curr_text
        elif name == 'Name':
            self.name = self.curr_text
        elif name == 'Prefix' and self.is_echoed_prefix_set:
            self.curr_common_prefix.prefix = self.curr_text
        elif name == 'Prefix':
            self.prefix = self.curr_text
            self.is_echoed_prefix_set = True            
        elif name == 'Marker':
            self.marker = self.curr_text
        elif name == 'IsTruncated':
            self.is_truncated = self.curr_text == 'true'
        elif name == 'Delimiter':
            self.delimiter = self.curr_text
        elif name == 'MaxKeys':
            self.max_keys = int(self.curr_text)
        elif name == 'NextMarker':
            self.next_marker = self.curr_text

        self.curr_text = ''

    def characters(self, content):
        self.curr_text += content


class ListAllMyBucketsHandler(xml.sax.ContentHandler):
    def __init__(self):
        self.entries = []
        self.curr_entry = None
        self.curr_text = ''

    def startElement(self, name, attrs):
        if name == 'Bucket':
            self.curr_entry = Bucket()

    def endElement(self, name):
        if name == 'Name':
            self.curr_entry.name = self.curr_text
        elif name == 'CreationDate':
            self.curr_entry.creation_date = self.curr_text
        elif name == 'Bucket':
            self.entries.append(self.curr_entry)

    def characters(self, content):
        self.curr_text = content

