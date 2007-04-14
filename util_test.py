import os
import unittest
import StringIO
from util import *
from S3IO import *

#In a file called amazon_credentials.py you must supply 
#values for two variables: AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
from amazon_credentials import *

TEST_BUCKET_NAME = AWS_ACCESS_KEY_ID + '_test_bucket'

class TestForceDeleteBucket(unittest.TestCase):
    def setUp(self):
        self.conn = S3.AWSAuthConnection(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
        io = S3IO(self.conn, TEST_BUCKET_NAME, 'test_object')
        io.write("abracadabra")
        io.close()
        
    def testForbiddenBucket(self):
        """ can't delete a bucket that doesn't belong to you """
        self.assertRaises(S3ResponseError, force_delete_bucket, self.conn, 'new_bucket')
        
    def testValidForceDelete(self):
        """ should be able to force delete a bucket """
        force_delete_bucket(self.conn, TEST_BUCKET_NAME)

if __name__ == '__main__':
    if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY:
        raise Exception("Must supply Amazon credentials")
    
    unittest.main()
    
    