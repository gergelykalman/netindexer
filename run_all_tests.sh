#!/bin/bash

URLLIST_SMALL=$1
URLLIST_LARGE=$2

TIMEOUT=5

echo "[+] Testing test_requests_dumb"
python3 -m tests.test_requests_dumb "$URLLIST_SMALL" $TIMEOUT

echo "[+] Testing test_requests_with_sessions"
python3 -m tests.test_requests_with_sessions "$URLLIST_SMALL" $TIMEOUT

echo "[+] Testing test_requests_processpool"
python3 -m tests.test_requests_processpool "$URLLIST_LARGE" 50 50 $TIMEOUT

echo "[+] Testing test_pycurl_with_cares"
python3 -m tests.test_pycurl_with_cares "$URLLIST_LARGE" 4 1000 100 $TIMEOUT 1
