import os
import unittest
import StringIO
import time
import logging
import util
from S3IO import *
from S3Archive import *
logging.root.setLevel(logging.DEBUG)

#In a file called amazon_credentials.py you must supply 
#values for two variables: AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
from amazon_credentials import *

TEST_BUCKET_NAME = AWS_ACCESS_KEY_ID + '_test_bucket'

class TestGoodNewIO(unittest.TestCase):
    def setUp(self):
        self.conn = S3.AWSAuthConnection(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
        
    def testNoParams(self):
        """ should get a brand new instance """
        rkiv = S3Archive(self.conn, TEST_BUCKET_NAME, 'testNoParams')
        io = rkiv.new_io()
        self.assertEqual(io.read(), '')
        io.write('testNoParams')
        io.close()
        
    def testLogicalDate(self):
        """ should get new S3ArchiveObject with logical_date set """
        now = time.localtime()
        rkiv = S3Archive(self.conn, TEST_BUCKET_NAME, 'testLogicalDate')
        io = rkiv.new_io(now)
        self.assertEqual(io.read(), '')
        io.write('testLogicalDate')
        self.assertEqual(io.meta['s3archive_logical_date'], time.strftime('%Y%m%d%H%M%S', now))
        io.close()

    def tearDown(self):
        try: util.force_delete_bucket(self.conn, TEST_BUCKET_NAME)
        except S3ResponseError: pass 
        
class TestGoodExistingIO(unittest.TestCase):
    def setUp(self):
        self.conn = S3.AWSAuthConnection(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
        
    def testNoParams(self):
        """ should get the most recent addition to the archive """
        
        rkiv = S3Archive(self.conn, TEST_BUCKET_NAME, 'testNoParams')
        
        for i in range(3):
            io = rkiv.new_io()
            io.write('testNoParams_%d' % i)
            io.close()
            time.sleep(1)
            
        rkiv.close()
        
        rkiv = S3Archive(self.conn, TEST_BUCKET_NAME, 'testNoParams')
        io = rkiv.existing_io()
        self.assertEqual(io.read(), 'testNoParams_2')
        
                
    def testFQON(self):
        """ should get an instance identified by the fully qualified object name """
        rkiv = S3Archive(self.conn, TEST_BUCKET_NAME, 'test_object')
        io = rkiv.new_io()
        fqon = io.fqon
        io.write('testFQON')
        io.close()
        
        io = rkiv.existing_io(fqon=fqon)
        self.assertEqual(io.read(), 'testFQON')
        io.close()
    
    def testLogicalDate(self):
        """ should get an instance identified by the logical_date """
        rkiv = S3Archive(self.conn, TEST_BUCKET_NAME, 'test_object')
        io = rkiv.new_io()
        logical_date = time.strptime(io.logical_date, '%Y%m%d%H%M%S')
        io.write('testLogicalDate')
        io.close()
        
        io = rkiv.existing_io(logical_date=logical_date)
        self.assertEqual(io.read(), 'testLogicalDate')
        io.close()
    
    def testPhysicalDate(self):
        """ should get an instance identified by the physical_date """
        rkiv = S3Archive(self.conn, TEST_BUCKET_NAME, 'test_object')
        io = rkiv.new_io()
        physical_date = time.strptime(io.physical_date, '%Y%m%d%H%M%S')
        io.write('testPhysicalDate')
        io.close()
        
        io = rkiv.existing_io(physical_date=physical_date)
        self.assertEqual(io.read(), 'testPhysicalDate')
        io.close()
    
    def testLogicalDatePhysicalDate(self):
        """ should get an instance identified by the logical_date and physical_date"""
        rkiv = S3Archive(self.conn, TEST_BUCKET_NAME, 'test_object')
        io = rkiv.new_io()
        logical_date = time.strptime(io.logical_date, '%Y%m%d%H%M%S')
        physical_date = time.strptime(io.physical_date, '%Y%m%d%H%M%S')
        io.write('testLogicalDatePhysicalDate')
        io.close()
        
        io = rkiv.existing_io(logical_date=logical_date, physical_date=physical_date)
        self.assertEqual(io.read(), 'testLogicalDatePhysicalDate')
        io.close()
    
    def testAllParams(self):
        """ should ignore the logical_date and physical_date and only use the fqon """
        rkiv = S3Archive(self.conn, TEST_BUCKET_NAME, 'test_object')
        io = rkiv.new_io()
        fqon = io.fqon
        io.write('testAllParams')
        io.close()
        
        now = time.localtime()
        io = rkiv.existing_io(fqon, now, now)
        self.assertEqual(io.read(), 'testAllParams')
        io.close()
    
    def tearDown(self):
        try: util.force_delete_bucket(self.conn, TEST_BUCKET_NAME)
        except S3ResponseError: pass   


class TestGoodList(unittest.TestCase):
    def setUp(self):
        self.conn = S3.AWSAuthConnection(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)  
        
    def testShortList(self):
        """ should return all instances of an archived object  """
        
        try: util.force_delete_bucket(self.conn, TEST_BUCKET_NAME)
        except S3ResponseError: pass
        
        rkiv = S3Archive(self.conn, TEST_BUCKET_NAME, 'test_object')
        
        for i in range(2):
            io = rkiv.new_io()
            io.write('abracadabra')
            io.close()
            time.sleep(1)
            
        rkiv.close()
        rkiv = S3Archive(self.conn, TEST_BUCKET_NAME, 'test_object')
        self.assertEqual(len(rkiv.list()), 2)
        

    def testLongList(self):
        """ should page through list of results and return a full list of all results  """
        
        try: util.force_delete_bucket(self.conn, TEST_BUCKET_NAME)
        except S3ResponseError: pass
        
        rkiv = S3Archive(self.conn, TEST_BUCKET_NAME, 'test_object')
        
        for i in range(3):
            io = rkiv.new_io()
            io.write('abracadabra')
            io.close()
            time.sleep(1)
            
        rkiv.close()
        rkiv = S3Archive(self.conn, TEST_BUCKET_NAME, 'test_object')
        self.assertEqual(len(rkiv.list(options={'max-keys':2, 'prefix':'test_object'})), 3)
        
    def tearDown(self):
        try: util.force_delete_bucket(self.conn, TEST_BUCKET_NAME)
        except S3ResponseError: pass   
 
                          
#class TestScratch(unittest.TestCase):
#    def setUp(self):
#        self.conn = S3.AWSAuthConnection(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
#        
#    def testScratchWholeBucketEmpty(self):
#        rkiv = S3Archiver(self.conn, TEST_BUCKET_NAME)
#        rkiv.scratch()
#        
#    def tearDown(self):
#        try:
#            util.force_delete_bucket(self.conn, TEST_BUCKET_NAME)
#        except S3ResponseError:
#            pass
        
        
        
        
if __name__ == '__main__':
    if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY:
        raise Exception("Must supply Amazon credentials")
    
    unittest.main()
    
    