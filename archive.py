from lib import S3
from StringIO import StringIO
import exceptions

class S3Archiver(S3IO):
    def __init__(self, conn, bucket_name, object_name, meta={}, buf='', days=-1, copies=-1, compress=False):
        S3IO.__init__(self, conn, bucket_name, object_name, meta, buf)
        self.days = days
        self.copies = copies
        self.compress = compress
        
    def read(self, n=-1, logical_date=-1, physical_date=-1):
        pass
    
    def write(self, s, logical_date=0):
        self.meta['logical_date']
        pass
    
    def scratch(self):
        pass