# pys3

S3IO - read and write to an S3 object as if it were a StringIO object.   

```python
import pys3

io = pys3.S3IO(conn, 'my_bucket', 'my_object')
io.read()
io.write('abracadabra')
io.close()
```

S3Archive - manage historical versions of an object, automatically handles retention. Inspired by CED's rkiv tools.  

```python
import pys3
rkiv = pys3.S3Archive(conn, 'my_bucket', 'my_object')
rkiv.set_retention(days=0, copies=4)

for i in range(5):
  io = rkiv.new_io()
  io.write('abracadabra')

rkiv.list() #shows 5 versions
rkiv.scratch()
rkiv.list() #shows 4 versions

io = rkiv.existing_io() #returns most recent logical version
io.read()
```

Comes with a test suite.
