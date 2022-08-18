import os
import concurrent.futures
import requests

from helpers.filereader import FileReader
from helpers.statcollector import StatCollector

READ_CHUNK_SIZE = 4096      # 4kb
SPAWN_PERCENTAGE = .05      # no more than 5% workers spawned in one iteration


def fetcher_main(url_list, timeout, connect_timeout):
    # be nice and prevent hogging more important processes like our scheduler or pdns
    os.nice(19)

    sess = requests.Session()
    results = []
    status, html = None, ""
    done = 0
    for url_raw in url_list:
        url = url_raw.strip()
        try:
            # TODO: put a limit here so that we don't try to fetch arbitrarily much data, for this we need to stream.
            r = sess.get(url, timeout=(connect_timeout, timeout))
        except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectTimeout):
            result = (done, url, "timeout", 0)
        except Exception as exc:
            result = (done, url, "Exception", 0)
        else:
            # TODO: html processing disabled for debugging
            result = (done, url, r.status_code, 0) #len(r.text))
        results.append(result)
        done += 1
    return results


def processpool_engine(config):
    urlgen = FileReader(config.urlfile)
    max_workers = config.workers
    timeout = config.timeout
    batchsize = config.batchsize
    connect_timeout = config.connect_timeout

    stats = StatCollector()

    stats.start()
    urls_exhausted = False
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = []

        while not urls_exhausted or len(futures) > 0:
            # NOTE: Watch out, spawning a lot of workers at once will HAMMER the infrastructure!
            #       TO prevent this, change the while to if, that way every concurrent.futures.wait timeout seconds
            #       a new worker will spawn. However, if some terminate, then we will never rach `workers`...
            workers_spawned = 0
            # NOTE: We only spawn MAX_WORKERS_SPAWNED in one go, as with high numbers of workers the first ones can
            #       start to timeout before we finish spawning everyone...
            while len(futures) < max_workers and workers_spawned < max_workers * SPAWN_PERCENTAGE and not urls_exhausted:
                url_list = urlgen.get_batch(batchsize)

                if len(url_list) == 0:
                    urls_exhausted = True
                    break

                args = [url_list, timeout, connect_timeout]
                future = executor.submit(fetcher_main, *args)
                futures.append(future)
                stats.add_submitted(len(url_list))
                workers_spawned += 1

            # wait for results and collect statistics
            done, not_done = concurrent.futures.wait(futures, timeout=1, return_when=concurrent.futures.FIRST_COMPLETED)
            for future in done:
                results = future.result()
                for _, url, status, length in results:
#                    print("RESULT", url, status, length)
                    # record statistics
                    if status == 200:
                        stats.add_success()
                    else:
                        stats.add_error()
                    stats.add_processed()
                    yield url, status, length
#                print(results)

            futures = list(not_done)

            stats.print_periodic(len(futures), interval=1)

    stats.print_final()
