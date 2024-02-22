# filecloudsync

A module for synchronization files in several clouds.

# S3 synchronization

Several tools to synchronize a folder with a S3 bucket.

## Connection

Some tools to make easy the connection with S3.

```python
from filecloudsync import s3

# Connect with the data of the environment variables "S3_ACCESS_KEY", "S3_SECRET_KEY" and "S3_ENDPOINT" or
# with the secrets stored in the file ".s3conf.yml" stored in the current folder or in the user home folder.
s3.connect()

# Connect using the arguments
s3.connect(ws_access_key_id=ACCESS_ID, aws_secret_access_key=SECRET_KEY, endpoint_url=ENDPOINT)
```

## Synchronizing

Synchronizes a bucket and a folder and applied the last changes of the file

```python
from filecloudsync import s3

client = s3.connect()

# Synchronizes a bucket and a folder with the last changes made in the bucket or in the folder
s3.sync(client, BUCKET_NAME, FOLDER)

# Create a new local file in the FOLDER
from os.path import join

with open(join(FOLDER, 'test.txt')) as file:
    print('Hello World!', file=file)

# Upload the new local file automatically and download the changes from the bucket if there are any
s3.sync(client, BUCKET_NAME, FOLDER)

# It also possible to get the made changes
changes = s3.sync(client, BUCKET_NAME, FOLDER)
# Print a list of (key, Operation, Location)
print(changes)
# Example of output:
# [
#   ('config.yml', <Operation.ADDED: 'added'>, <Location.BUCKET: 'keys'>), 
#   ('data/data.json', <Operation.MODIFIED: 'modified'>, <Location.LOCAL: 'files'>),
#   ('test.txt', <Operation.DELETED: 'deleted'>, <Location.BUCKET: 'keys'>)
# ] 
```

## Monitor

Check with some frequency if there are changes in both, local folder and bucket, and apply the changes
to synchronize them.

```python
from filecloudsync import s3
from time import sleep

# Start the synchronizer monitor of that bucket and folder and check each 5 seconds 
monitor = s3.Monitor(BUCKET, FOLDER, 5)
monitor.start()
try:
    # Do something
    sleep(120)  # Synchronizes 40 times
finally:
    monitor.stop()

# The same as previous but shorter and checking each 60 seconds 
with s3.Monitor(BUCKET, FOLDER):
    # Do something
    sleep(120)  # Synchronizes 2 times

# Select which files should be synchronized
with s3.Monitor(BUCKET, FOLDER, files={'config.yml', 'data/data.csv'}):
    # Do something
    sleep(120)  # Synchronizes 2 times
```

Moreover, you can add handles to detect when the monitor makes the changes. For example:

```python
from filecloudsync import s3
from time import sleep
from typing import List, Tuple


# Create the on change handdle
def my_on_change_handle(key: str, operation: s3.Operation, location: s3.Location):
    # Print the key or the file (in key format) if the key has been ADDED, MODIFIED or DELETED,
    # and if the key has been changed in LOCAL or in the BUCKET
    print(f'The key or file {key} has been {operation.value} in {location.name}')

# Create the on finish handle
def my_on_finish_handle(changes: List[Tuple[str, s3.Operation, s3.Location]]):
    # Print the list of the last changes before finishing. It could be the empty list
    print(changes)
    
# Create the monitor
with s3.Monitor(BUCKET, FOLDER) as monitor:
    # Each time a key or file is changed (modified, added or deleted) the function my_handle will be called
    monitor.add_on_change_handle(my_on_change_handle)
    # Do something
    sleep(120)
```

## Other utilities

### read_yaml(), read_json()

Read a Yaml or json file from a bucket key, even if they are compressed with gzip.

```python
from filecloudsync.s3 import connect, read_yaml, read_json

client = connect()
# Read the Yaml file from the bucket and return an object
obj1 = read_yaml(client, BUCKET, 'data/data.yml')
# Read the compressed Yaml file from the bucket and return an object
obj2 = read_yaml(client, BUCKET, 'data/data.yml.gz')
# Read the json file from the bucket and return an object
obj3 = read_yaml(client, BUCKET, 'data/data.json')
# Read the compressed json file from the bucket and return an object
obj2 = read_yaml(client, BUCKET, 'data/data.json.gz')
```

### write_json and write_yaml

Write a Yaml or json file to a bucket.

```python
from filecloudsync.s3 import connect, write_yaml, write_json

client = connect()
# Write a dictionary in a Yaml file in a bucket
write_yaml({'msg': 'Hello World!}, client, BUCKET, 'data/data.yml')
# Write a dictionary in a compressed Yaml file in a bucket
write_yaml({'msg': 'Hello World!}, client, BUCKET, 'data/data.yml.gz')
# Write a dictionary in a json file in a bucket
write_yaml({'msg': 'Hello World!}, client, BUCKET, 'data/data.json')
# Write a dictionary in a compressed json file in a bucket
write_yaml({'msg': 'Hello World!}, client, BUCKET, 'data/data.json.gz')
```
