# netcrawler

### What is this?

Netcrawler is a very fast HTTP client, capable of pushing `thousands of requests/second`.

I have used this to download the `~170 million` .com domain's index pages with a `c6g.4xlarge`
in ~10 hours.

My average speed was about **4400 requests/s**

The repository contains 3 solutions to this problem:
1) requests.get in a loop
2) multiprocessing with requests.get in a loop
3) pycurl with multiprocessing

The first two are for measurement purposes only, the only useful one is the pycurl one, however
#2 also can have pretty good performance under ideal conditions.


### Install
> apt-get install libcurl4-gnutls-dev libgnutls28-dev python3-dev
> pip install requests pycurl


### Usage

To run the crawler:
```
$ python3 -m run --backend pycurl --urlfile ./lists/sample_100.txt  --batchsize 100 --logfile logs/logfile.txt --datafile ./logs/datalog --timeout 1 --connect-timeout 1 --pycurl-workers-print-log True --pycurl-maxhandles 50 --nsserver 8.8.8.8
```

To analyse the data:
```
$ python3 -m analyse --file-glob './logs/datalog_*' --max-workers 4 --function ip
```


### Internals

The code internally uses `pycurl` with `curlmulti`, and multiprocessing's
ProcessPoolExecutor to scale to more than one core.

The files saved by the crawler and consumed by the analyser are pickle files that are gzipped.


### TODO
- Migrate from using pickle files to something less vulnerable
- Refactor the analyser so that it can be invoked from Python in a way that it returns the matching objects
  - This would enable a more fine-grained analysis
