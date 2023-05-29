#!/usr/bin/env python
# coding: utf-8

# In[18]:


import os
from google.cloud import storage

try:
    from google.cloud import storage
except Exception as e:
    print("Some Modules are Missing: {}".format(e))

# Set your Google Cloud Storage bucket name
bucket_name = 'test_bucket-jems-iot'
# Set the path to the directory containing the files you want to upload
directory_path = '/Users/yawyaw/Downloads/test'
# Set the destination directory in the Google Cloud Storage bucket
destination_directory = 'temp/'

# Create a storage client
storage_client = storage.Client.from_service_account_json("formidable-feat-386117-cea608bebc8d.json")
bucket = storage_client.get_bucket(bucket_name)

# Iterate over the files in the directory
for filename in os.listdir(directory_path):
    source_file_path = os.path.join(directory_path, filename)
    destination_blob_name = os.path.join(destination_directory, filename)

    # Upload the file to Google Cloud Storage
    blob = bucket.blob(destination_blob_name)
    with open(source_file_path, 'rb') as f:
        blob.upload_from_file(f)

    print("Uploaded file {} to {}".format(source_file_path, destination_blob_name))

    # Delete the file from the local system
    os.remove(source_file_path)
    print("Deleted file {}".format(source_file_path))

print("Upload and delete completed")


# In[ ]:




