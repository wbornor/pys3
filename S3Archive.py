from StringIO import StringIO
import exceptions
import pickle
import time
import logging
from lib import S3
from S3IO import *
import util

class S3ArchiveError(S3IOError): pass

class S3ArchiveIO(S3IO):
    def __init__(self, rkiv, meta={}, buf='', fqon=None, logical_date=None):
        self.rkiv = rkiv
        self.object_prefix = rkiv.object_prefix
        
        if fqon:
            self.fqon = fqon
            (self.logical_date, self.physical_date, junk) = fqon.rsplit('.',2)
        else:
            self.now = time.strftime('%Y%m%d%H%M%S')
            if logical_date:
                self.logical_date = time.strftime('%Y%m%d%H%M%S', logical_date)
            else:
                self.logical_date = self.now
            
            self.physical_date = self.now
            self.fqon = '%s.%s.%s' % (self.object_prefix, self.physical_date, self.logical_date) #fully qualified object name
        
        meta['s3archive_logical_date'] = self.logical_date
        meta['s3archive_physical_date'] = self.physical_date
        meta['s3archive_object_prefix'] = self.object_prefix
        
        S3IO.__init__(self, rkiv.conn, rkiv.bucket_name, self.fqon, meta, buf)
            
    def close(self):
        if not self.closed:
            self.rkiv.scratch()
            S3IO.close(self)
        
class S3Archive:
    """ a S3Archive is a set of physical Amazon objects that represent historical instances of the same logical object.
        rkiv = S3Archiver(self.conn, TEST_BUCKET_NAME, 'test_object')
        io = rkiv.new_io()
        io = rkiv.new_io(logical_date=ld)
        rkiv.scratch()
        rkiv.list()

        io = rkiv.existing_io()
        io = rkiv.pop() #same as rkiv.get_existing_instance() 
        io = rkiv.get_instance(fqon=rkiv.list()[0])
        io = rkiv.get_existing_instance(fqon=fqon)
        
        """
        
    def __init__(self, conn, bucket_name, object_prefix):
        self.conn = conn
        self.bucket_name = bucket_name
        self.object_prefix = object_prefix
    
    def new_io(self, logical_date=None):
        
        return S3ArchiveIO(self, logical_date=logical_date)
    

    def existing_io(self, fqon=None, logical_date=None, physical_date=None):
        
        if fqon:
            return(S3ArchiveIO(self, fqon=fqon))
        
        if logical_date:
            logical_date = time.strftime('%Y%m%d%H%M%S', logical_date)
            
        if physical_date:
            physical_date = time.strftime('%Y%m%d%H%M%S', physical_date)
        
        if logical_date and physical_date:
            fqon = '%s.%s.%s' % (self.object_prefix, physical_date, logical_date)
            f = self.list(options={'prefix': fqon})
            if not f:
                raise S3ArchiveError("Match not found.")
            
            return S3ArchiveIO(self, fqon=f[0])
         
        now = time.strftime('%Y%m%d%H%M%S')
            
        if logical_date:
            #try to find the most recent existing match
            fqons = self.list()
            for fqon in fqons:
                if fqon[-14:] == logical_date:
                    return S3ArchiveIO(self, fqon=fqon)
            
            #no matches, raise an error
            raise S3ArchiveError("No matches found.")
                
        elif physical_date:
            #try to find the most recent existing match
            fqons = self.list(options={'prefix':'%s.%s' % (self.object_prefix, physical_date)})
            if fqons:
                #fqons is now a list of all the same physical_dates, get the most recent logical_date
                return S3ArchiveIO(self, fqon=fqons[-1])
           
            #no matches, raise an error
            raise S3ArchiveError("No matches found.")
            
        else:
            #return the most recent addition to the archive
            return S3ArchiveIO(self, fqon=self.list()[-1])
        
    def list(self, options=None):
        """ list all the instances of this object filtered by prefix"""
        if not options:
            options = {'prefix': self.object_prefix}
        
        is_truncated = True #on the first time, assume the response is truncated
        fqons = []
        
        while is_truncated:
            logging.debug('listing the contents of \'%s\' with options \'%s\'' % (self.bucket_name, options))
            r = self.conn.list_bucket(self.bucket_name, options=options)
            util.check_http_response(r)
            fqons.extend([list_entry.key for list_entry in r.entries])
            logging.debug(fqons)
            is_truncated = r.is_truncated
            if is_truncated:
                options['marker'] = r.entries[-1].key 
        return fqons
        
    def scratch(self):
        pass
    
    def close(self):
        self.scratch()
        
    
class S3Scratcher:
    """S3Scratcher - Frees stale objects from a bucket """
    def __init__(self, conn, bucket_name, object_name=None, days=365*2, copies=10, compress=False):
        self.conn
        self.bucket_name = bucket_name
        self.object_prefix = object_name
        self.days = days
        self.copies = copies
        self.compress = compress
    
    def scratch(self):
        try:
            io = S3IO(self.conn, self.bucket_name, 's3scratcher.props')
            props = pickle.load(io.read())
            logging.debug('contents of s3scratcher.props: %s' % props)
        except S3IOError:
            pass #TODO - rebuild the props file from meta data if it has been deleted or corrupted
#        finally:
#            io.close()
        
        
        