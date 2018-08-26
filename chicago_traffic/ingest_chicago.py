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
#import zipfile
#import datetime
import tempfile
from urllib2 import urlopen
from google.cloud import storage
from google.cloud.storage import Blob


def download (ChicagoData, Historical, destdir):
   '''
     Downloads on-time performance data and returns local filename
   '''
   logging.info('Requesting data for {}-{}-*'.format(ChicagoData, Historical))
   
   url='https://data.cityofchicago.org/api/views/n4j6-wkkf/rows.csv?accessType=DOWNLOAD'
   
   filename = os.path.join(destdir, "{}{}.csv".format(ChicagoData, Historical))
   with open(filename, "wb") as fp:
     response = urlopen(url)
     fp.write(response.read())
   logging.debug("{} saved".format(filename))
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
         os.remove(filename)
         msg = 'Got header={}, but expected={}'.format(firstline, expected_header)
         logging.error(msg)
         raise UnexpectedFormat(msg)

      if next(csvfp, None) == None:
         os.remove(filename)
         msg = ('Received a file from Chicago that has only the header and no content')
         raise DataUnavailable(msg)

def upload(filename, bucketname, blobname):
   client = storage.Client()
   bucket = client.get_bucket(bucketname)
   blob = Blob(blobname, bucket)
   blob.upload_from_filename(filename)
   gcslocation = 'gs://{}/{}'.format(bucketname, blobname)
   logging.info('Uploaded {} ...'.format(gcslocation))
   return gcslocation

 
def ingest(bucket):
   '''
   ingest chicago data from chicago traffic website to Google Cloud Storage
   return cloud-storage-blob-name on success.
   raises DataUnavailable if this data is not on chicago website
   '''
   tempdir = tempfile.mkdtemp(prefix='ingest_chicago')
   try:
      filename = download(tempdir)
      verify_ingest(filename)
      gcsloc = 'chicagodata/raw/{}'.format(os.path.basename(filename))
      return upload(filename, bucket, gcsloc)
   finally:
      logging.debug('Cleaning up by removing {}'.format(tempdir))
      shutil.rmtree(tempdir)
      
import argparse
   parser = argparse.ArgumentParser(description='ingest flights data from BTS website to Google Cloud Storage')
   parser.add_argument('--bucket', help='GCS bucket to upload data to', required=True)
  
   try:
      logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
      args = parser.parse_args()
      
      logging.debug('Ingesting bucket={}'.format(bucket))
      gcsfile = ingest(args.bucket)
      logging.info('Success ... ingested to {}'.format(gcsfile))
   except DataUnavailable as e:
      logging.info('Try again later: {}'.format(e.message))


