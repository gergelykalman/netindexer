#!/bin/bash

URLLIST_SMALL=$1
URLLIST_LARGE=$2
TIMEOUT=$3
CONNECT_TIMEOUT=$4

echo "[+] Testing test_requests_dumb"
python3 -m tests.test_requests_dumb "$URLLIST_SMALL" $TIMEOUT

echo "[+] Testing test_requests_processpool"
python3 -m tests.processpool_engine "$URLLIST_LARGE" 50 50 $TIMEOUT $CONNECT_TIMEOUT /dev/null

echo "[+] Testing test_pycurl_with_cares"
#python3 -m tests.test_pycurl_with_cares "$URLLIST_LARGE" 8 5000 1500 $TIMEOUT $CONNECT_TIMEOUT 10 /dev/null
python3 -m tests.test_pycurl_with_cares "$URLLIST_LARGE" 4 500 100 $TIMEOUT $CONNECT_TIMEOUT 10 /dev/null
