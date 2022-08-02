import os
import sys
import concurrent.futures
import requests
from datetime import datetime as dt, timedelta as td


from .helper import FileReader


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


def test_requests_processpool(urlgen, max_workers, req_per_worker, timeout, connect_timeout):
    start = dt.now()
    urls_exhausted = False
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        start_time = dt.now()
        last_status = start_time
        urls_submitted, urls_processed, successes, errors = 0, 0, 0, 0
        while not urls_exhausted or len(futures) > 0:
            # NOTE: Watch out, spawning a lot of workers at once will HAMMER the infrastructure!
            #       TO prevent this, change the while to if, that way every concurrent.futures.wait timeout seconds
            #       a new worker will spawn. However, if some terminate, then we will never rach `workers`...
            workers_spawned = 0
            # NOTE: We only spawn MAX_WORKERS_SPAWNED in one go, as with high numbers of workers the first ones can
            #       start to timeout before we finish spawning everyone...
            while len(futures) < max_workers and workers_spawned < max_workers * SPAWN_PERCENTAGE and not urls_exhausted:
                url_list = urlgen.get_batch(req_per_worker)

                if len(url_list) == 0:
                    urls_exhausted = True
                    break

                args = [url_list, timeout, connect_timeout]
                future = executor.submit(fetcher_main, *args)
                futures.append(future)
                urls_submitted += len(url_list)
                workers_spawned += 1

            # wait for results and collect statistics
            done, not_done = concurrent.futures.wait(futures, timeout=1, return_when=concurrent.futures.FIRST_COMPLETED)
            for future in done:
                results = future.result()
                for _, url, status, length in results:
#                    print("RESULT", url, status, length)
                    # record statistics
                    if status == 200:
                        successes += 1
                    else:
                        errors += 1
                    urls_processed += 1
                    yield url, status, length
#                print(results)

            futures = list(not_done)

            # print statistics
            now = dt.now()
            elapsed = (now - last_status).total_seconds()
            if elapsed > 1:
                success_rate = successes/urls_processed*100 if urls_processed > 0 else 0
                print("STATUS: workers: %.2f%%, processed: %s, successes: %s, errors: %s, lag: %.2f, avg req/s: %.2f/s, success rate: %.2f%%" % (
                    len(futures)/max_workers*100, urls_processed, successes, errors, elapsed, urls_processed/(now-start_time).total_seconds(), success_rate))
                last_status = now
    end = dt.now()
    delta = (end-start).total_seconds()
    print("{} requests took {:.2f} seconds, avg: {:.2f}, errors: {:.2f} %".format(
        urls_processed, delta, urls_processed / delta, errors/urls_processed
    ))


if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: ./{} url_list num_workers req_per_worker timeout_secs".format(sys.argv[0]))
        exit(-1)

    fr = FileReader(sys.argv[1])
    num_workers = int(sys.argv[2])
    req_per_worker = int(sys.argv[3])
    timeout = int(sys.argv[4])
    for result in test_requests_processpool(fr, num_workers, req_per_worker, timeout):
        pass
