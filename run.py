import argparse
import resource
import pickle
import gzip

from helpers.config import CrawlConfig

from engines.engine_pycurl import PycurlEngine
from engines.engine_requests_processpool import processpool_engine

OUTPUT_BATCH_SIZE = 100000  # 100k works out to about 300MB files...

LOG_ERRORS = True


def __open_datalog(name, iteration):
    datalog = gzip.open(
        filename="{}_{}.pickle.gz".format(name, iteration),
        mode="wb",
        compresslevel=1,
    )
    return datalog


def main(indexer, outname, datalogname):
    with open(outname, "w") as outf:
        iteration, counter = 0, 0
        datalog = __open_datalog(datalogname, iteration)
        tmp = []

        for i in indexer.run_forever():
            if i["error"] is not None:
                print("ERR", i["error"], i["url"], file=outf)
            else:
                print(i["http_code"], i["size"], i["url"], file=outf)

            # collect result in temporary buffer
            if LOG_ERRORS is True or i["error"] is None:
                tmp.append(i)

            if len(tmp) > OUTPUT_BATCH_SIZE:
                pickle.dump(tmp, datalog)
                tmp = []
                datalog.close()
                datalog = __open_datalog(datalogname, iteration)
                iteration += 1

        # flush remaining entries to the log
        if len(tmp) > 0:
            pickle.dump(tmp, datalog)
            datalog.close()


def main2(indexer, outname, datalog):
    with open(outname, "w", buffering=1024) as outf:
        for url, status, length in indexer:
            print(status, length, url, file=outf)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--backend", type=str, choices=["pycurl", "requests"], required=True,
                        help="The backend to use")
    parser.add_argument("--urlfile", type=str, required=True,
                        help="File that contains the list of urls")
    parser.add_argument("--workers", type=int, required=True,
                        help="Number of workers to spawn")
    parser.add_argument("--batchsize", type=int, required=True,
                        help="Number of urls to fetch per worker, this means different things in pycurl and requests!")
    parser.add_argument("--timeout", type=int, default=5,
                        help="Maximum read_timeout in seconds")
    parser.add_argument("--connect-timeout", type=int, default=3,
                        help="Maximum connect_timeout in seconds")
    parser.add_argument("--logfile", type=str, required=True,
                        help="File to log output to")
    parser.add_argument("--datafile", type=str, required=True,
                        help="File to write the binary data to")
    parser.add_argument("--nsserver", type=str, default="127.0.0.1",
                        help="IP of the DNS server to use (In pycurl this requires ares as well)")
    parser.add_argument("--useragent", type=str, default=None,
                        help="User agent to use, default is libcurl's default agent")
    parser.add_argument("--output-batchsize", type=int, default=100000,
                        help="Number of responses to put into one output file chunk")

    # pycurl exclusive
    parser.add_argument("--pycurl-maxhandles", type=int, default=100,
                        help="Maximum number of handles to open (pycurl engine only)")
    parser.add_argument("--pycurl-readinterval", type=float, default=10,
                        help="Number of milliseconds to sleep in poll() (pycurl engine only), default is 10")
    parser.add_argument("--pycurl-enabled-ares", type=bool, default=False,
                        help="Is libc-ares2 enabled? (Might need custom libcurl compilation)")
    parser.add_argument("--pycurl-print-errors", type=bool, default=False,
                        help="Should the engine print errors to stdout?")
    parser.add_argument("--pycurl-workers-print-log", type=bool, default=False,
                        help="Should the workers print a status log like the taskmaster?")

    parser.add_argument("--pycurl-maxbodysize", type=int, default=4096,
                        help="Buffer size to use for response body")
    parser.add_argument("--pycurl-maxheadersize", type=int, default=4096,
                        help="Buffer size to use for response headers")
    parser.add_argument("--pycurl-lastfill_waittime", type=float, default=0.1,
                        help="Wait this much time before refilling done handles, float seconds")
    parser.add_argument("--pycurl-max-spawns-per-iteration", type=int, default=3,
                        help="Spawn this many processes at once")
    args = parser.parse_args()
    # end

    # TODO: reconcile these names!
    config = CrawlConfig()
    config.urlfile = args.urlfile
    config.workers = args.workers
    config.backend = args.backend
    config.batchsize = args.batchsize
    config.timeout = args.timeout
    config.connect_timeout = args.connect_timeout
    config.logfile = args.logfile
    config.datafile = args.datafile
    config.useragent = args.useragent
    config.nsserver = args.nsserver
    config.output_batchsize = args.output_batchsize

    ### pycurl-specific
    config.pycurl_maxhandles = args.pycurl_maxhandles
    # read_interval:
    #   Very important, this is the time we wait between reads. Too small and you get cpu bound, too little
    #   and buffer is not read often enough to maximize speed. This should be adaptable, based on current conditions,
    #   but I'm lazy to write that code right now.
    #   Values: 1-100 usually work best, if you don't want to test too much, just use 10
    config.pycurl_read_interval_ms = args.pycurl_readinterval / 1000
    config.pycurl_enabled_ares = args.pycurl_enabled_ares
    config.pycurl_print_errors = args.pycurl_print_errors
    config.pycurl_workers_print_log = args.pycurl_workers_print_log
    config.pycurl_contentbuffersize = args.pycurl_maxbodysize
    config.pycurl_headerbuffersize = args.pycurl_maxheadersize
    config.pycurl_lastfill_waittime = args.pycurl_lastfill_waittime
    config.pycurl_max_spawns_per_iteration = args.pycurl_max_spawns_per_iteration
    ### end

    # set limits
    limit = (1000000, 1000000)
    resource.setrlimit(resource.RLIMIT_NOFILE, limit)

    if config.backend == "pycurl":
        indexer = PycurlEngine(config)
        main(indexer, config.logfile, config.datafile)
    elif config.backend == "requests":
        indexer = processpool_engine(config)
        main2(indexer, config.logfile, config.datafile)
    else:
        raise ValueError("backend must be either pycurl or requests")
