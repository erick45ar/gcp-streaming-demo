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
import zipfile
import datetime
import tempfile
from urllib2 import urlopen
from google.cloud import storage
from google.cloud.storage import Blob

def download(YEAR, MONTH, destdir):
   '''
     Downloads on-time performance data and returns local filename
     YEAR e.g.'2015'
     MONTH e.g. '01 for January
   '''
   logging.info('Requesting data for {}-{}-*'.format(YEAR, MONTH))
   url='https://data.cityofchicago.org/api/views/n4j6-wkkf/rows.csv?accessType=DOWNLOAD'
   filename = os.path.join(destdir, "{}{}.csv".format(YEAR, MONTH))
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
 
def upload(filename, bucketname, blobname):
   client = storage.Client()
   bucket = client.get_bucket(bucketname)
   blob = Blob(blobname, bucket)
   blob.upload_from_filename(filename)
   gcslocation = 'gs://{}/{}'.format(bucketname, blobname)
   logging.info('Uploaded {} ...'.format(gcslocation))
   return gcslocation

def ingest(year, month, bucket):
   '''
   ingest flights data from BTS website to Google Cloud Storage
   return cloud-storage-blob-name on success.
   raises DataUnavailable if this data is not on BTS website
   '''
   tempdir = tempfile.mkdtemp(prefix='ingest_chicago')
   try:
      rawfile = download(year, month, tempdir)
      verify_ingest = (rawfile)
      gcsloc = 'chicagodata/raw/{}'.format(os.path.basename(rawfile))
      return upload(rawfile, bucket, gcsloc)
   finally:
      logging.debug('Cleaning up by removing {}'.format(tempdir))
      shutil.rmtree(tempdir)
   
   
def next_month(bucketname):
   '''
     Finds which months are on GCS, and returns next year,month to download
   '''
   client = storage.Client()
   bucket = client.get_bucket(bucketname)
   blobs  = list(bucket.list_blobs(prefix='chicagodata/raw/'))
   files = [blob.name for blob in blobs if 'csv' in blob.name] # csv files only
   lastfile = os.path.basename(files[-1])
   logging.debug('The latest file on GCS is {}'.format(lastfile))
   year = lastfile[:4]
   month = lastfile[4:6]
   return compute_next_month(year, month)


def compute_next_month(year, month):
   dt = datetime.datetime(int(year), int(month), 15) # 15th of month
   dt = dt + datetime.timedelta(30) # will always go to next month
   logging.debug('The next month is {}'.format(dt))
   return '{}'.format(dt.year), '{:02d}'.format(dt.month)
  
 
if __name__ == '__main__':
   import argparse
   parser = argparse.ArgumentParser(description='ingest traffic data from chicago website to Google Cloud Storage')
   parser.add_argument('--bucket', help='GCS bucket to upload data to', required=True)
   parser.add_argument('--year', help='Example: 2015.  If not provided, defaults to getting next month')
   parser.add_argument('--month', help='Specify 01 for January. If not provided, defaults to getting next month')

   try:
      logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
      args = parser.parse_args()
      if args.year is None or args.month is None:
         year, month = next_month(args.bucket)
      else:
         year = args.year
         month = args.month
      logging.debug('Ingesting year={} month={}'.format(year, month))
      gcsfile = ingest(year, month, args.bucket)
      logging.info('Success ... ingested to {}'.format(gcsfile))
   except DataUnavailable as e:
      logging.info('Try again later: {}'.format(e.message))


