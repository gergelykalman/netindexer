import sys
import resource
import pickle

from tests.helper import FileReader

from tests.test_pycurl_with_cares import Indexer
from tests.test_requests_processpool import test_requests_processpool


def main(indexer, outname, datalogname):
    with open(outname, "w") as outf, open(datalogname, "wb") as datalog:
        for i in indexer.run_forever():
            if i["error"] is not None:
                print("ERR", i["error"], i["url"], file=outf)
            else:
                print(i["http_code"], i["size"], i["url"], file=outf)
            pickle.dump(i, datalog)


def main2(indexer, outname, datalog):
    with open(outname, "w", buffering=1024) as outf:
        for url, status, length in indexer:
            print(status, length, url, file=outf)


if __name__ == "__main__":
    if len(sys.argv) < 9:
        print("Usage: ./{} url_list num_workers req_per_worker max_handles timeout_secs connect_timeout_secs read_interval_ms logfile datalog".format(
            sys.argv[0]))
        exit(-1)

    fr = FileReader(sys.argv[1])
    num_workers = int(sys.argv[2])
    req_per_worker = int(sys.argv[3])
    max_handles = int(sys.argv[4])
    timeout = int(sys.argv[5])
    connect_timeout = int(sys.argv[6])

    # read_interval:
    #   Very important, this is the time we wait between reads. Too small and you get cpu bound, too little
    #   and buffer is not read often enough to maximize speed. This should be adaptable, based on current conditions,
    #   but I'm lazy to write that code right now.
    #   Values: 1-100 usually work best, if you don't want to test too much, just use 10
    read_interval = int(sys.argv[7]) / 1000

    outfile = sys.argv[8]
    datalog = sys.argv[9]

    limit = (1000000, 1000000)
    resource.setrlimit(resource.RLIMIT_NOFILE, limit)

    indexer = Indexer(fr, num_workers, max_handles, req_per_worker, timeout, connect_timeout, read_interval)
    main(indexer, outfile, datalog)

    # indexer = test_requests_processpool(fr, num_workers, req_per_worker, timeout, connect_timeout)
    # main2(indexer, outfile, datalog)
