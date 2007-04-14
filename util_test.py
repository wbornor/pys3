import os
import unittest
import StringIO
from util import *

#Must supply values for the next two variables.
AWS_ACCESS_KEY_ID = '' 
AWS_SECRET_ACCESS_KEY = ''
TEST_BUCKET_NAME = AWS_ACCESS_KEY_ID + '_test_bucket'

class TestForceDeleteBucket(unittest.TestCase):
    def setUp(self):
        self.conn = S3.AWSAuthConnection(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
        io = S3IO(self.conn, TEST_BUCKET_NAME, 'test_object')
        io.write("abracadabra")
        io.close()
        
    def testForbiddenBucket(self):
        """ can't delete a bucket that doesn't belong to you """
        self.assertRaises(ResponseError, force_delete_bucket, self.conn, 'new_bucket')
        
    def testValidForceDelete(self):
        """ should be able to force delete a bucket """
        force_delete_bucket(self.conn, TEST_BUCKET_NAME)

class TestGoodConnect(unittest.TestCase):
    def setUp(self):
        self.conn = S3.AWSAuthConnection(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
        
    def testWithObject(self):
        """ should be able to connect with a valid bucket and object name """
        io = S3IO(self.conn, TEST_BUCKET_NAME, 'test_object')
        
    def testWithMeta(self):
        """ should be able to connect with a valid bucket and object name and meta data """
        io = S3IO(self.conn, TEST_BUCKET_NAME, 'test_object', {'key': 'value'}) 
    
    def tearDown(self):
#        try:
#            force_delete_bucket(self.conn, TEST_BUCKET_NAME)
#        except ResponseError:
#            pass      
         force_delete_bucket(self.conn, TEST_BUCKET_NAME)

class TestBadConnect(unittest.TestCase):
    def setUp(self):
        self.conn = S3.AWSAuthConnection(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
        
    def testNullConn(self):
        """ must have AWSAuthConnection Object """
        self.assertRaises(TypeError, S3IO, None)
        
    def testWithoutBucket(self):
        """ must have bucket name """
        self.assertRaises(TypeError, S3IO, self.conn)
        
    def testWithoutObject(self):
        """ must have an object name """
        self.assertRaises(TypeError, S3IO, self.conn, TEST_BUCKET_NAME)
           

class TestGoodWrite( unittest.TestCase ):
    def setUp(self):
        self.conn = S3.AWSAuthConnection(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
                
    def testNewBucketNewObject(self):
        """ should be able to create a non-existent bucket an non-existant object """
        io = S3IO(self.conn, TEST_BUCKET_NAME, 'test_object')
        io.write("newBucketNewObjectTest")
        io.close()
        
        r = self.conn.list_bucket(TEST_BUCKET_NAME)
        self.assertEqual(len(r.entries), 1)
        self.assertEqual(r.entries[0].key, 'test_object')
        
        r = self.conn.get(TEST_BUCKET_NAME, 'test_object')
        self.assertEqual(r.object.data, 'newBucketNewObjectTest')
        
        force_delete_bucket(self.conn, TEST_BUCKET_NAME)
        
    def testExistingBucketNewObject(self):
        """ Should be able to write to a new object in an existing bucket """
        r = self.conn.create_bucket(TEST_BUCKET_NAME)
        check_http_response(r)
        
        io = S3IO(self.conn, TEST_BUCKET_NAME, 'test_object')
        io.write("newBucketNewObjectTest")
        io.close()
        
        r = self.conn.get(TEST_BUCKET_NAME, 'test_object')
        self.assertEqual(r.object.data, 'newBucketNewObjectTest')
        
        force_delete_bucket(self.conn, TEST_BUCKET_NAME)
    
    def testExistingBucketExistingObject(self):
        """ Should be able to overwrite an existing object in an existing bucket """
        r = self.conn.create_bucket(TEST_BUCKET_NAME)
        check_http_response(r)
        
        r = self.conn.put(TEST_BUCKET_NAME,
                                 'test_object',
                                 S3.S3Object('ExistingBucketExistingObject'))
        check_http_response(r)
        
        io = S3IO(self.conn, TEST_BUCKET_NAME, 'test_object')
        io.write("ExistingBucketExistingObject")
        io.close()
        
        r = self.conn.get(TEST_BUCKET_NAME, 'test_object')
        self.assertEqual(r.object.data, 'ExistingBucketExistingObject')
        
        force_delete_bucket(self.conn, TEST_BUCKET_NAME)
    
    def testLargestFile(self):
        """ Object size can be at most MAX_OBJECT_SIZE (5GB) """
        io = S3IO(self.conn, TEST_BUCKET_NAME, 'test_object')
        io.MAX_OBJECT_SIZE = 5
        io.write("abcde")
        io.close()
        
        r = self.conn.get(TEST_BUCKET_NAME, 'test_object')
        self.assertEqual(r.object.data, 'abcde')
        force_delete_bucket(self.conn, TEST_BUCKET_NAME)

    def testLargestBucketName(self):
        """ Bucket names can be at most 255 chars long"""
        io = S3IO(self.conn, 'l'*255, 'test_object')
        force_delete_bucket(self.conn, 'l'*255)
    
    def testValidCharsInBucketName(self):
        """ Bucket names may only contain the characters A-Z, a-z, 0-9, '_', '.', and '-' """
        io = S3IO(self.conn, TEST_BUCKET_NAME+'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_.-', 'test_object')
        force_delete_bucket(self.conn, TEST_BUCKET_NAME+'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_.-')
        
    def testWriteLines(self):
        """ Should be able to write a sequence of strings """
        lines = ['aqua', 'teen', 'hunger', 'force']
        io = S3IO(self.conn, TEST_BUCKET_NAME, 'test_object')
        io.writelines(lines)
        io.close()
        
        io = S3IO(self.conn, TEST_BUCKET_NAME, 'test_object')
        self.assertEqual(''.join(lines), io.read())
        io.close()
        
    def testUnicode(self):
        """ Should be able to write unicode data """
        raise Exception("test not implemented")
        
    def testBinary(self):
        """ Should be able to write binary content """
        binary_content = '\x00\x00\xFF\xFF'
        io = S3IO(self.conn, TEST_BUCKET_NAME, 'test_object')
        io.write(binary_content)
        io.close()
        
        io = S3IO(self.conn, TEST_BUCKET_NAME, 'test_object')
        self.assertEqual(binary_content, io.read())
        io.close()
    
    def tearDown(self):
        try:
            force_delete_bucket(self.conn, TEST_BUCKET_NAME)
        except ResponseError:
            pass
        
class TestBadWrite( unittest.TestCase ):
    def setUp(self):
        self.conn = S3.AWSAuthConnection(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    
    def testForbiddenBucket(self):
        """ Can't access buckets that don't belong to you """
        self.assertRaises(ResponseError, S3IO, self.conn, 'new_bucket', 'test_object')
        
    def testBucketNameTooLong(self):
        """ Bucket names cannot be longer than 255 chars """
        self.assertRaises(ResponseError, S3IO, self.conn, 'a'*256, 'test_object')
                    
    def testZeroByte(self):
        """ Input length must be greater than zero """
        io = S3IO(self.conn, TEST_BUCKET_NAME, 'test_object')
        io.write("")
        
        self.assertRaises(S3IOError, io.flush)

    def testFileTooBig(self):
        """ Input must not exceed MAX_OBJECT_LENGTH """
        io = S3IO(self.conn, TEST_BUCKET_NAME, 'test_object')
        io.MAX_OBJECT_SIZE = 5
        io.write("abcdef")

        self.assertRaises(S3IOError, io.flush)
        
        
    def testInvalidCharsInBucketName(self):
        """ Bucket names cannot contain any of the following characters `~!@#$%^&*()+={}[];':"<>,|\ """
        
        for char in """`~!@#$%^&*()+={}[];':"<>,|\ """:
            test_invalid_bucket_name = TEST_BUCKET_NAME+'_%s' % char
            #print test_invalid_bucket_name
            self.assertRaises(ResponseError, S3IO, self.conn, test_invalid_bucket_name, 'test_object')
            
    def testBucketNameTooShort(self):
        """ Bucket names must be at least 3 characters long """
        self.assertRaises(ResponseError, S3IO, self.conn, 'wz', 'test_object')
     
    def tearDown(self):
        try:
            force_delete_bucket(self.conn, TEST_BUCKET_NAME)
        except ResponseError:
            pass


class TestGoodRead( unittest.TestCase ):
    def setUp(self):
        self.contents = "new readable object test"
        self.conn = S3.AWSAuthConnection(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
        io = S3IO(self.conn, TEST_BUCKET_NAME, 'test_object')
        io.write(self.contents)
        io.close()
        
    def testRead(self):
        """ Should be able to read the contents of the object """
        io = S3IO(self.conn, TEST_BUCKET_NAME, 'test_object')
        object_contents = io.read()
        self.assertEqual(self.contents, object_contents)
        
        io.close()
        
    def testReadPortion(self):
        """ Should be able to read a portion (5 bytes) of the object """
        io = S3IO(self.conn, TEST_BUCKET_NAME, 'test_object')
        object_contents = io.read(5)
        self.assertEqual(self.contents[0:5], object_contents)
        io.close()
     
    def testReadTooBig(self): 
        """ Should be able to handle reading more than the size of the object (just return up to the size of the object """
        io = S3IO(self.conn, TEST_BUCKET_NAME, 'test_object')
        object_contents = io.read(len(self.contents)+10)
        self.assertEqual(self.contents, object_contents)
        io.close()
        
    def testWriteRead(self):
        """ A write on an empty buffer overwrites the remote object. A following read should return '' because the pointer is at the end of the string. """
        io = S3IO(self.conn, TEST_BUCKET_NAME, 'test_object')
        io.write('clobbers the remote object')
        object_contents = io.read()
        self.assertEqual('', object_contents)
        io.close()
        
        self.setUp() #reset the base state for other tests
        
    def testReadWrite(self):
        """ Synonymous to append """
        io = S3IO(self.conn, TEST_BUCKET_NAME, 'test_object')
        object_contents = io.read()
        io.write('alittlebitmore')
        self.assertEquals('', io.read()) #io.pos is at end of string
        io.close()
        
        
        io = S3IO(self.conn, TEST_BUCKET_NAME, 'test_object')
        object_contents = io.read()
        self.assertEquals(self.contents+'alittlebitmore', object_contents)
        io.close()
        
        self.setUp()
        
    def testReadRead(self):
        """ Read at the end of the buffer, expect '' """
        io = S3IO(self.conn, TEST_BUCKET_NAME, 'test_object')
        object_contents = io.read()
        self.assertEquals('', io.read())
        io.close()
        
    def testSeek(self):
        """ Seek should reposition the internal pointer """
        io = S3IO(self.conn, TEST_BUCKET_NAME, 'test_object')
        io.seek(1)
        self.assertEquals(self.contents[1:], io.read())
        io.close()
        
    def testTruncate(self):
        """ Truncate should set the file's size to the given parameter """
        io = S3IO(self.conn, TEST_BUCKET_NAME, 'test_object')
        io.truncate(1)
        self.assertEquals(self.contents[:1], io.read())
        io.close()
        
        io = S3IO(self.conn, TEST_BUCKET_NAME, 'test_object')
        self.assertEquals(self.contents[:1], io.read())
        io.close()
        self.setUp() #reset the base state for other tests
        
    def tearDown(self):
        force_delete_bucket(self.conn, TEST_BUCKET_NAME)
    
class TestNatural(unittest.TestCase): 
    def setUp(self):
        self.contents = 'aqua\nteen\nhunger\nforce\n'
        self.conn = S3.AWSAuthConnection(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
        io = S3IO(self.conn, TEST_BUCKET_NAME, 'test_object')
        io.write(self.contents)
        io.close()
        
    def testSimple(self):
        io = S3IO(self.conn, TEST_BUCKET_NAME, 'test_object')
        object_contents = io.read()
        self.assertEqual(object_contents, self.contents)
        io.close()
        
    def testIterator(self):
        io = S3IO(self.conn, TEST_BUCKET_NAME, 'test_object')
        lines = []
        for line in io:
            #print line
            lines.append(line)
        
        self.assertEqual(lines, self.contents.splitlines(True))
        io.close()
            
    def tearDown(self):
        force_delete_bucket(self.conn, TEST_BUCKET_NAME)
        
                
if __name__ == '__main__':
    if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY:
        raise Exception("Must supply Amazon credentials")
    
    unittest.main()
    
    