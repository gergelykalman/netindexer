import io
import os
import sys
import zlib
import concurrent.futures
import resource
import traceback
import time
from datetime import datetime as dt

import pycurl


from .helper import filereader


class MyCurlException(Exception):
    pass

# toggles
ENABLED_ARES = True
#ENABLED_ARES = False
PRINT_ERRORS = False
# end

NSSERVER = "127.0.0.1"
HEADERS = [
    "Accept: text/html,application/xhtml+xml,application/xml",
    "Accept-Encoding: gzip",
]
USERAGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.101 Safari/537.36"
CONTENTBUFFERSIZE = 4 * 1024
HEADERBUFFERSIZE = 1024
LASTFILL_WAITTIME = 0.1


class _Handle:
    def __init__(self, url, timeout, read_interval, nsserver=NSSERVER):
        self.__url = url
        self.__nsserver = nsserver

        # NOTE: Connection timeout is set to timeout for now
        self.__timeout = timeout
        self.__read_interval = read_interval

        # local vars
        self.handle = pycurl.Curl()
        self.__buf = None
        self.__bufsize_exceeded = None
        self.__header_buf = None
        self.__headersize_exceeded = None

        # call reset to reinitialize curl handle and buffer
        self.reset()

    def reset(self):
        self.handle.reset()

        # NOTE: Needs c-ares
        if ENABLED_ARES:
            self.handle.setopt(pycurl.DNS_SERVERS, self.__nsserver)
        self.handle.setopt(pycurl.URL, self.__url)
        self.handle.setopt(pycurl.USERAGENT, USERAGENT)
        self.handle.setopt(pycurl.HTTPHEADER, HEADERS)
        self.handle.setopt(pycurl.WRITEFUNCTION, self.__write)
        self.handle.setopt(pycurl.HEADERFUNCTION, self.__header_write)
        self.handle.setopt(pycurl.TIMEOUT, self.__timeout)
        self.handle.setopt(pycurl.CONNECTTIMEOUT, self.__timeout)
        self.handle.setopt(pycurl.FOLLOWLOCATION, 1)
        self.handle.setopt(pycurl.MAXREDIRS, 20)

        # performance tuning
        # self.handle.setopt(pycurl.IPRESOLVE, 1)
        # self.handle.setopt(pycurl.FRESH_CONNECT, 1)
        # self.handle.setopt(pycurl.FORBID_REUSE, 1)
        # self.handle.setopt(pycurl.DNS_CACHE_TIMEOUT, 0)
        self.handle.setopt(pycurl.BUFFERSIZE, CONTENTBUFFERSIZE)

        self.__buf = io.BytesIO()
        self.__bufsize_exceeded = False
        self.__header_buf = io.BytesIO()
        self.__headersize_exceeded = False

    def __header_write(self, buf):
        # TODO: this is duplicated code with write()
        if self.__headersize_exceeded:
            return

        self.__buf.seek(0, os.SEEK_END)
        length = self.__header_buf.tell()

        if length <= HEADERBUFFERSIZE:
            self.__header_buf.write(buf)
        else:
            # print("Write header buffer size exceeded!" % length)
            self.__headersize_exceeded = True

    def __write(self, buf):
        # only read first N bytes we have memory for
        if self.__bufsize_exceeded:
            return

        self.__buf.seek(0, os.SEEK_END)
        length = self.__buf.tell()

        if length <= CONTENTBUFFERSIZE:
            self.__buf.write(buf)
        else:
            # print("Write buffer size exceeded!" % length)
            self.__bufsize_exceeded = True

    def get_private_data(self):
        buf = self.__buf.getvalue()
        headers = self.__header_buf.getvalue()
        return self.__url, buf, headers


class FastFetch:
    def __init__(self, name, maxhandles, urls_to_crawl, print_enabled, timeout, read_interval):
        self.name = name
        self.__timeout = timeout
        self.__read_interval = read_interval

        self.urls_to_crawl = urls_to_crawl
        self.__url_idx = 0

        # this is here so we can test this object with vanilla file objects
        self.print_enabled = print_enabled

        self.maxhandles = maxhandles

        self.success = 0
        self.failure = 0
        self.results = []
        self.skipped = 0

        if 1000000 < self.maxhandles < 1:
            raise ValueError("maxhandles is outside the range 0..1000000")

        self.__check_for_features()

        self.multi_handle = self.__get_multi_handle()

        self.handles_inprogress = {}
        self.handles_free = []
        self.last_status = dt.now()
        self.start_time = dt.now()
        self.num_processed = 0
        self.last_num_processed = 0
        self.still_running = True
        self.lastfill = dt.now()

    def __check_for_features(self):
        # check for libcurl features
        versiondata = pycurl.version
        if ENABLED_ARES:
            if "c-ares" not in versiondata:
                print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
                print("! WARNING c-ares support is not built in! !")
                print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
                raise RuntimeError("Version [%s] has no async DNS enabled" % versiondata)

    def __get_multi_handle(self):
        handle = pycurl.CurlMulti()
        # TODO: set options properly from here: https://curl.haxx.se/libcurl/c/curl_multi_setopt.html

        # WARNING: M_PIPELINING causes segfaults, thankfully we don't need it:
        # Program terminated with signal SIGSEGV, Segmentation fault.
        # #0  Curl_removeHandleFromPipeline (handle=handle@entry=0x3c4f730, pipeline=0x0) at url.c:2866
        handle.setopt(pycurl.M_PIPELINING, 0)

        return handle

    def __fillhandles(self):
        free_handles = self.maxhandles - len(self.handles_inprogress)

        # we use the file instead of a queue
        urls = self.urls_to_crawl[self.__url_idx:self.__url_idx+free_handles]
        self.__url_idx += free_handles

        if len(urls) == 0:
            self.still_running = False
            return

        for url in urls:
            if len(self.handles_free) > 0:
                # reuse handles if we have them
                handle = self.handles_free.pop()
            else:
                try:
                    handle = _Handle(url, self.__timeout, self.__read_interval)
                except UnicodeEncodeError:
                    # TODO: handle this before _Handle is called!
                    continue

            self.multi_handle.add_handle(handle.handle)
            self.handles_inprogress[handle.handle] = handle

    def __print_status(self):
        now = dt.now()
        if (dt.now() - self.last_status).total_seconds() > 1:
            if self.print_enabled and self.num_processed > 0:
                print("%s STATUS handles: %d, processed: %d, requests: %d/s, avg r/s: %d/s, good: %d, bad: %d, success rate: %.2f%%, lag: %.2f" % (
                    self.name, len(self.handles_inprogress), self.num_processed, self.num_processed - self.last_num_processed,
                    self.num_processed / ((dt.now()-self.start_time).total_seconds()),
                    self.success, self.failure, self.success / self.num_processed * 100, (now-self.last_status).total_seconds()))
            self.last_status = now
            self.last_num_processed = self.num_processed

    def __process_headers(self, header_buf):
        headers = {}
        header_str = header_buf.decode("utf-8")
        for idx, line_raw in enumerate(header_str.split("\n")):
            line = line_raw.strip("\r")
            if line == "" or idx == 0:
                continue
            if ":" not in line:
                continue
            k, v = map(str.strip, line.split(":", 1))
            headers[k] = v
        return headers

    def __process_html(self, html_buf, headers):
        html = html_buf
        for k, v in headers.items():
            if k.lower() == "Content-Encoding":
                if v.lower() == "gzip":
                    html = zlib.decompress(html_buf)
                else:
                    raise MyCurlException("Unknown content type: {}".format(v))
                break
        return html

    def __handle_response(self, c, errno=None, errmsg=None):
        handle = self.handles_inprogress[c]
        headers, html, error = None, None, None

        # generate result
        url, html_raw, headers_raw = handle.get_private_data()

        if errno is None and errmsg is None:
            self.success += 1
        else:
            self.failure += 1
            # error = "({} - {})".format(errno, errmsg)
            # NOTE: ignore errmsg for now as it's harder to group
            error = "({})".format(errno)

        # if error is None:
        #     headers = self.__process_headers(headers_raw)
        #     html = self.__process_html(html_raw, headers)

        result = {
            "created": dt.now().isoformat(),
            "url": url,
            "html": html,
            "headers": headers,
            "http_code": handle.handle.getinfo(pycurl.RESPONSE_CODE),
            "size": handle.handle.getinfo(pycurl.SIZE_DOWNLOAD),
            "speed": handle.handle.getinfo(pycurl.SPEED_DOWNLOAD),
            # TODO: return the actual redirects here!
            "redirects": handle.handle.getinfo(pycurl.REDIRECT_COUNT),
            "error": error
        }



        # free up libcurl stuff
        self.multi_handle.remove_handle(c)
        c.close()
        del self.handles_inprogress[c]

        # free handle, or reset
        #handle.reset()
        #self.handles_free.append(handle)
        del handle

        self.results.append(result)

    def run(self):
        self.__fillhandles()

        try:
            last_read = dt.now()
            while self.still_running or len(self.handles_inprogress) > 0:
                now = dt.now()

                delta = (dt.now() - last_read).total_seconds()
                if delta > self.__read_interval:
                    while True:
                        ret, num_handles = self.multi_handle.perform()
                        if ret != pycurl.E_CALL_MULTI_PERFORM:
                            break
                    last_read = dt.now()
                else:
                    sleeptime = self.__read_interval-delta
                    if sleeptime > 0:
                        time.sleep(sleeptime)

                num_q, ok_list, err_list = self.multi_handle.info_read()
                for c in ok_list:
                    self.__handle_response(c)
                for c, errno, errmsg in err_list:
                    self.__handle_response(c, errno, errmsg)

                self.num_processed = self.num_processed + len(ok_list) + len(err_list)

                self.__print_status()

                if len(self.handles_inprogress) < self.maxhandles * .9 and (now - self.lastfill).total_seconds() > LASTFILL_WAITTIME:
                    self.__fillhandles()
                    self.lastfill = now
        except Exception as exc:
            print("%s Exception received: %s" % (self.name, exc))
            print(traceback.format_exc())
            return MyCurlException("???")
        else:
            return self.results


# IMPORTANT: This needs to be a separate function as otherwise ProcessPoolExecutor won't work
def fetcher_main(*args):
    f = FastFetch(*args)
    results = f.run()
    return results


class Indexer:
    def __init__(self, url_generator, num_workers, max_handles, req_per_worker, timeout, read_interval):
        self.__url_generator = url_generator
        self.__max_processes = num_workers
        self.__max_handles = max_handles
        self.__batchsize_per_process = req_per_worker
        self.__timeout = timeout
        self.__read_interval = read_interval

        self.__worker_id = 0
        self.__last_status = dt.now()
        self.__errortypes = {}

    def __read_url_batch(self, batchsize):
        exhausted = False
        batch = []
        while len(batch) < batchsize:
            try:
                line = next(self.__url_generator)
            except StopIteration:
                exhausted = True
                break

            line = line.strip()
            batch.append(line)
        return exhausted, batch

    def run_forever(self):
        start = dt.now()
        urls_processed, successes, errors = 0, 0, 0
        urls_exhausted = False
        with concurrent.futures.ProcessPoolExecutor(max_workers=self.__max_processes) as executor:
            futures = []
            start_time = dt.now()
            while not urls_exhausted or len(futures) > 0:
                while len(futures) < self.__max_processes:
                    urls_exhausted, urls_to_crawl = self.__read_url_batch(self.__batchsize_per_process)
                    if len(urls_to_crawl) == 0:
                        break

                    # print("Spawning task %s" % self.__worker_id)
                    args = [
                        "test_%s" % self.__worker_id,
                        self.__max_handles,
                        urls_to_crawl,
                        False,   # print_enabled
                        self.__timeout,
                        self.__read_interval,
                    ]
                    future = executor.submit(fetcher_main, *args)
                    self.__worker_id += 1
                    futures.append(future)

                # wait for results and collect statistics
                done, not_done = concurrent.futures.wait(futures, timeout=1, return_when=concurrent.futures.FIRST_COMPLETED)
                for future in done:
                    results = future.result()
                    for result in results:
                        # record statistics
                        if result["error"] is None:
                            successes += 1
                        else:
                            #print("ERROR", result["error"])
                            errors += 1
                            errormsg = result["error"]
                            if self.__errortypes.get(errormsg) is None:
                                self.__errortypes[errormsg] = 0
                            self.__errortypes[errormsg] += 1
                        urls_processed += 1
                        yield result

                    # json.dump(results, self.__resultfile)
                    # self.__resultfile.write("\n")
                    # self.__resultfile.flush()

                futures = list(not_done)

                # print statistics
                now = dt.now()
                elapsed = (now - self.__last_status).total_seconds()
                if elapsed > 1:
                    success_rate = successes/urls_processed*100 if urls_processed > 0 else 0
                    print("STATUS: workers: %d, processed: %s, successes: %s, errors: %s, lag: %.2f, avg req/s: %.2f/s, success rate: %.2f%%" % (
                        len(futures), urls_processed, successes, errors, elapsed, urls_processed/(now-start_time).total_seconds(), success_rate))
                    if PRINT_ERRORS:
                        print("ERRORS:", self.__errortypes)
                    self.__last_status = now
        end = dt.now()
        delta = (end - start).total_seconds()
        print("{} requests took {:.2f} seconds, avg: {:.2f}, errors: {:.2f} %".format(
            urls_processed, delta, urls_processed / delta, errors/urls_processed*100
        ))


def dummy_urlgen(url, n):
    while True:
       for i in range(n):
            yield url


if __name__ == "__main__":
    if len(sys.argv) < 7:
        print("Usage: ./{} url_list num_workers req_per_worker max_handles timeout_secs read_interval_ms".format(
            sys.argv[0]))
        exit(-1)

    fr = filereader(sys.argv[1])
    num_workers = int(sys.argv[2])
    req_per_worker = int(sys.argv[3])
    max_handles = int(sys.argv[4])
    timeout = int(sys.argv[5])

    # read_interval:
    #   Very important, this is the time we wait between reads. Too small and you get cpu bound, too little
    #   and buffer is not read often enough to maximize speed. This should be adaptable, based on current conditions,
    #   but I'm lazy to write that code right now.
    #   Values: 1-100 usually work best, if you don't want to test too much, just use 10
    read_interval = int(sys.argv[6]) / 1000

    limit = (1000000, 1000000)
    resource.setrlimit(resource.RLIMIT_NOFILE, limit)

    indexer = Indexer(fr, num_workers, max_handles, req_per_worker, timeout, read_interval)
    for result in indexer.run_forever():
        pass
