#!/usr/bin/env python

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

def download(destdir):

   logging.info('Requesting data for Chicago Demo')

   url='https://data.cityofchicago.org/api/views/n4j6-wkkf/rows.csv?accessType=DOWNLOAD --output Chicago_Traffic_Tracker_-_Congestion_Estimates_by_Segments.csv'
   filename = os.path.join(destdir)
   with open(filename, "wb") as fp:
     response = urlopen(url)
     fp.write(response.read())
   logging.debug("chicago demo data saved")
   return filename

