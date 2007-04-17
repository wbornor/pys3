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
    """ a S3Archive is a set of physical Amazon objects that represent historical instances of the same logical object """   
        
    def __init__(self, conn, bucket_name, object_prefix):
        self.conn = conn
        self.bucket_name = bucket_name
        self.object_prefix = object_prefix
        self.props = None
        
        try:
            self.props = self._get_props()
            self.days = self.props['days'] #if days is <= 0 then keep indefinatly
            self.copies = self.props['copies'] #if copies is <= 0 then keep indefinatly 
            
        except S3IOError:
            #props file not set
            self.set_retention()
    
    def __str__(self):
        return '<S3Archive - %s.%s>' % (self.bucket_name, self.object_prefix)
    
    def __repr__(self):
        return __str__()
    
    def _get_props(self):
        io = S3IO(self.conn, self.bucket_name, self.object_prefix+'.props')
        props = pickle.load(io.read())
        logging.debug('contents of %s: %s' % (self.props_object_name, props))
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
        return S3ArchiveIO(self, logical_date=logical_date)
    

    def existing_io(self, fqon=None, logical_date=None, physical_date=None):
        """ find an existing instance of this logical object
            if no parameters are given, get the most recent addition to the archive """
        
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
        """ list all the instances of this logical object
            options is a list that is sent in the request to the webservice"""
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
        """ frees stale objects from this archive """
        fqons = self.list()
        if self.copies > 0 and len(fqons) > self.copies:
            for fqon in fqons[:len(fqons)-self.copies]:
                #delete object
                pass
    
    def close(self):
        self.scratch()
        
    

        
        
        