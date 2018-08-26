#!/usr/bin/env python

# Copyright 2016 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import shutil
import logging
import os.path
from urllib2 import urlopen
from google.cloud import storage
from google.cloud.storage import Blob

def download(destdir):
   '''
     Downloads on-time performance data and returns local filename
   '''
   logging.info('Requesting data')

   url='https://www.transtats.bts.gov/DownLoad_Table.asp?Table_ID=236&Has_Group=3&Is_Zipped=0'
   
   filename = os.path.join(destdir)
   with open(filename, "wb") as fp:
     response = urlopen(url)
     fp.write(response.read())
   logging.debug("saved")
   return filename
  
class DataUnavailable(Exception):
   def __init__(self, message):
      self.message = message

class UnexpectedFormat(Exception):
   def __init__(self, message):
      self.message = message
 
 
def verify_ingest(filename):
   expected_header = 'SEGMENTID,STREET,DIRECTION,FROM_STREET,TO_STREET,LENGTH,STREET_HEADING,COMMENTS,START_LONGITUDE,START_LATITUDE,END_LONGITUDE,END_LATITUDE,CURRENT_SPEED,LAST_UPDATED'
   with open(filename, 'r') as csvfp:
      firstline = csvfp.readline().strip()
      if (firstline != expected_header):
         os.remove(csvfile)
         msg = 'Got header={}, but expected={}'.format(
                             firstline, expected_header)
         logging.error(msg)
         raise UnexpectedFormat(msg)

      if next(csvfp, None) == None:
         os.remove(filename)
         msg = ('Received a file from Chicago Trafficker that has only the header and no content')
         raise DataUnavailable(msg)
         
  def upload(filename, bucketname, blobname):
   client = storage.Client()
   bucket = client.get_bucket(bucketname)
   blob = Blob(blobname, bucket)
   blob.upload_from_filename(filename)
   gcslocation = 'gs://{}/{}'.format(bucketname, blobname)
   logging.info('Uploaded {} ...'.format(gcslocation))
   return gcslocation
   
   logging.info('Success ... ingested to {}'.format(gcslocation))
