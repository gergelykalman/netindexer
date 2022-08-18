import io
import os
import concurrent.futures
import traceback
import time
from datetime import datetime as dt

import pycurl

from helpers.filereader import FileReader
from helpers.statcollector import StatCollector


class MyCurlException(Exception):
    pass


HEADERS = [
    "Accept: text/html,application/xhtml+xml,application/xml",
    "Accept-Encoding: gzip",
]


class _Handle:
    def __init__(self, url, config):
        self.__url = url
        self.__config = config

        self.__nsserver = config.nsserver
        self.__useragent = config.useragent
        self.__contentbuffersize = config.pycurl_contentbuffersize
        self.__headerbuffersize = config.pycurl_headerbuffersize
        self.__enabled_ares = config.pycurl_enabled_ares

        # NOTE: Connection timeout is set to timeout for now
        self.__timeout = config.timeout
        self.__connect_timeout = config.connect_timeout

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
        if self.__enabled_ares:
            self.handle.setopt(pycurl.DNS_SERVERS, self.__nsserver)
        self.handle.setopt(pycurl.URL, self.__url)
        if self.__useragent is not None:
            self.handle.setopt(pycurl.USERAGENT, self.__useragent)
        self.handle.setopt(pycurl.HTTPHEADER, HEADERS)
        self.handle.setopt(pycurl.WRITEFUNCTION, self.__write)
        self.handle.setopt(pycurl.HEADERFUNCTION, self.__header_write)
        self.handle.setopt(pycurl.TIMEOUT, self.__timeout)
        self.handle.setopt(pycurl.CONNECTTIMEOUT, self.__connect_timeout)
        self.handle.setopt(pycurl.FOLLOWLOCATION, 1)
        self.handle.setopt(pycurl.MAXREDIRS, 20)

        # performance tuning
        self.handle.setopt(pycurl.IPRESOLVE, 1)
        self.handle.setopt(pycurl.FRESH_CONNECT, 1)
        self.handle.setopt(pycurl.FORBID_REUSE, 1)
        self.handle.setopt(pycurl.DNS_CACHE_TIMEOUT, 0)
        self.handle.setopt(pycurl.BUFFERSIZE, self.__contentbuffersize)

        self.__buf = io.BytesIO()
        self.__bufsize_exceeded = False
        self.__header_buf = io.BytesIO()
        self.__headersize_exceeded = False

    def __do_write(self, buf, size_exceeded, maxsize, newdata):
        # Returns size_exceeded, always!
        if size_exceeded is True:
            return size_exceeded

        buf.seek(0, os.SEEK_END)
        length = buf.tell()

        if length <= maxsize:
            buf.write(newdata)
        else:
            print("Write buffer size exceeded!" % length)
            return True
        return False

    def __header_write(self, buf):
        self.__headersize_exceeded = self.__do_write(self.__header_buf, self.__headersize_exceeded, self.__headerbuffersize, buf)

    def __write(self, buf):
        self.__bufsize_exceeded = self.__do_write(self.__buf, self.__bufsize_exceeded, self.__contentbuffersize, buf)

    def get_private_data(self):
        buf = self.__buf.getvalue()
        headers = self.__header_buf.getvalue()
        return self.__url, buf, headers


class FastFetch:
    def __init__(self, name, urls_to_crawl, config):
        self.name = name
        self.urls_to_crawl = urls_to_crawl

        self.__config = config

        self.__read_interval = config.pycurl_read_interval_ms
        self.__print_enabled = config.pycurl_workers_print_log
        self.__maxhandles = config.pycurl_maxhandles
        self.__lastfill_waittime = config.pycurl_lastfill_waittime
        self.__enabled_ares = config.pycurl_enabled_ares

        self.__url_idx = 0

        self.success = 0
        self.failure = 0
        self.results = []
        self.skipped = 0

        if 1000000 < self.__maxhandles < 1:
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
        if self.__enabled_ares:
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
        free_handles = self.__maxhandles - len(self.handles_inprogress)

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
                    handle = _Handle(url, self.__config)
                except UnicodeEncodeError:
                    # TODO: handle this before _Handle is called!
                    raise

            self.multi_handle.add_handle(handle.handle)
            self.handles_inprogress[handle.handle] = handle

    def __print_status(self):
        now = dt.now()
        if (dt.now() - self.last_status).total_seconds() > 1:
            if self.__print_enabled and self.num_processed > 0:
                print("%s STATUS handles: %d, processed: %d, requests: %d/s, avg r/s: %d/s, good: %d, bad: %d, success rate: %.2f%%, lag: %.2f" % (
                    self.name, len(self.handles_inprogress), self.num_processed, self.num_processed - self.last_num_processed,
                    self.num_processed / ((dt.now()-self.start_time).total_seconds()),
                    self.success, self.failure, self.success / self.num_processed * 100, (now-self.last_status).total_seconds()))
            self.last_status = now
            self.last_num_processed = self.num_processed

    def __process_headers(self, header_buf):
        headers = {}
        header_str = header_buf.decode("utf-8", errors="ignore")
        for idx, line_raw in enumerate(header_str.split("\n")):
            line = line_raw.strip("\r")
            if line == "" or idx == 0:
                continue
            if ":" not in line:
                continue
            k, v = map(str.strip, line.split(":", 1))
            headers[k] = v
        return headers

    def __handle_response(self, c, errno=None, errmsg=None):
        handle = self.handles_inprogress[c]
        headers, html, error = None, None, None

        # generate result
        url, html_raw, headers_raw = handle.get_private_data()

        if errno is None and errmsg is None:
            self.success += 1
        else:
            self.failure += 1
            error = "({} - {})".format(errno, errmsg)
            # NOTE: ignore errmsg for now as it's harder to group
            # error = "({})".format(errno)

        if error is None:
            headers = self.__process_headers(headers_raw)
            html = html_raw     # we return the raw bytes for now

        result = {
            "created": dt.now().isoformat(),
            "url": url,
            "html": html,
            "headers": headers,
            "http_code": handle.handle.getinfo(pycurl.RESPONSE_CODE),
            "size": handle.handle.getinfo(pycurl.SIZE_DOWNLOAD),
            "speed": handle.handle.getinfo(pycurl.SPEED_DOWNLOAD),
            "ip": handle.handle.getinfo(pycurl.PRIMARY_IP),
            "port": handle.handle.getinfo(pycurl.PRIMARY_PORT),
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
                    # Get a fresh dt.now() as multi_handle.perform() might take a long time
                    newdelta = (dt.now() - last_read).total_seconds()
                    sleeptime = self.__read_interval-newdelta
                    if sleeptime > 0:
                        time.sleep(sleeptime)

                num_q, ok_list, err_list = self.multi_handle.info_read()
                for c in ok_list:
                    self.__handle_response(c)
                for c, errno, errmsg in err_list:
                    self.__handle_response(c, errno, errmsg)

                self.num_processed = self.num_processed + len(ok_list) + len(err_list)

                self.__print_status()

                if len(self.handles_inprogress) < self.__maxhandles * .9\
                and (now - self.lastfill).total_seconds() > self.__lastfill_waittime:
                    self.__fillhandles()
                    self.lastfill = now
        except Exception as exc:
            print("%s Exception received: %s" % (self.name, exc))
            print(traceback.format_exc())
            return MyCurlException("???")
        else:
            return self.results


# IMPORTANT: This needs to be a separate function as otherwise ProcessPoolExecutor won't work
def fetcher_main(id, urls, config):
    # be nice and prevent hogging more important processes like our scheduler or pdns
    os.nice(19)

    f = FastFetch(id, urls, config)
    results = f.run()
    return results


class PycurlEngine:
    def __init__(self, config):
        self.__config = config

        self.__url_generator = FileReader(config.urlfile)
        self.__max_processes = config.workers
        self.__batchsize_per_process = config.batchsize
        self.__max_spawns_per_iteration = config.pycurl_max_spawns_per_iteration

        self.__worker_id = 0

        self.__stats = StatCollector()

    def __read_url_batch(self, batchsize):
        exhausted = False
        batch = self.__url_generator.get_batch(batchsize)
        if len(batch) == 0:
            exhausted = True
        return exhausted, batch

    def run_forever(self):
        self.__stats.start_clock()
        urls_exhausted = False
        with concurrent.futures.ProcessPoolExecutor(max_workers=self.__max_processes) as executor:
            futures = set()
            while not urls_exhausted or len(futures) > 0:
                spawned = 0
                while len(futures) < self.__max_processes and spawned < self.__max_spawns_per_iteration:
                    urls_exhausted, urls_to_crawl = self.__read_url_batch(self.__batchsize_per_process)
                    if len(urls_to_crawl) == 0:
                        break

                    # print("Spawning task %s" % self.__worker_id)
                    args = [
                        "test_%s" % self.__worker_id,
                        urls_to_crawl,
                        self.__config,
                    ]
                    future = executor.submit(fetcher_main, *args)
                    self.__worker_id += 1
                    futures.add(future)
                    spawned += 1

                # wait for results and collect statistics
                done, not_done = concurrent.futures.wait(futures, timeout=1, return_when=concurrent.futures.FIRST_COMPLETED)
                for future in done:
                    results = future.result()
                    if type(results) is MyCurlException:
                        raise results

                    for result in results:
                        # record statistics
                        if result["error"] is None:
                            self.__stats.add_success()
                        else:
                            print("ERROR", result["error"])
                            errormsg = result["error"]
                            self.__stats.add_error(errormsg)
                        self.__stats.add_processed()
                        yield result

                futures = not_done

                self.__stats.print_periodic(len(futures), interval=1)

        self.__stats.print_final()
