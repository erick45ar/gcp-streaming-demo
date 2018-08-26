#!/bin/bash

RESPONSE=$(:~$ curl https://data.cityofchicago.org/api/views/n4j6-wkkf/rows.csv?accessType=DOWNLOAD --output Chicago_Traffic_Tracker_-_Congestion_Estimates_by_Segments.csv)

echo "Received $RESPONSE"

done
