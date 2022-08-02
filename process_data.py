import sys
import pickle
import gzip
import re
import pprint

import concurrent.futures
from datetime import datetime as dt, timedelta as td


MAX_WORKERS = 4
BATCH_SIZE = 100

PERMITTED_FUNCTIONS = [
    "ip", "raw_html", "headers", "html", "generator", "server", "title", "links", "regexmatch", "scripts",
    "poweredby", "hiddenwp", "phpinfo", "indexof"]


def process_html(headers, body):
    server, html = "", ""
    html_bytes = bytearray()
    for k, v in headers.items():
        k = k.lower()
        if k == "content-encoding":
            if v == "gzip":
                try:
                    html_bytes += gzip.decompress(body)
                except (EOFError, gzip.BadGzipFile):
                    # bad gzip
                    return server, html
            else:
                print("ERROR: Invalid encoding: {}".format(v), file=sys.stderr)
                return server, html

            break
        elif k == "server":
            server = v

    html = html_bytes.decode("utf-8", errors="ignore")

    return server, html


def process_object(results, function):
    ret = ""



    counter = 0
    for r in results:
        server, html = process_html(r["headers"], r["html"])
        # print("{:50s} {:15s} {:10s} {:10s} {:20s}".format(r["url"], r["ip"], extra["server"], extra["title"], extra["generator"]))

        if function == "ip":
            ret += "{}\t{}".format(r["ip"], r["url"]) + "\n"
        elif function == "raw_html":
            ret += "=" * 50 + "\n" + r["url"] + "\n"
            ret += r["html"] + "\n"
        elif function == "html":
            ret += "=" * 50 + "\n" + r["url"] + "\n"
            ret += html + "\n"
        elif function == "server":
            if len(server) > 0:
                ret += "{}\t{}".format(server, r["url"]) + "\n"
        elif function == "headers":
            ret += "=" * 50 + "\n" + r["url"] + "\n"
            ret += r["url"] + "\n"
            ret += pprint.pformat(r["headers"], indent=4) + "\n"
        elif function == "poweredby":
            for k, v in r["headers"].items():
                if "x-powered-by" == k.lower():
                    ret += "{}\t{}".format(v, r["url"]) + "\n"
                    break
        elif function == "generator":
            matches = re.findall(r'<meta name="generator" content="(?P<generator>.*?)" />', html,
                                 re.MULTILINE | re.IGNORECASE)
            if len(matches) > 0:
                ret += "{}\t{}".format(matches, r["url"]) + "\n"
        elif function == "title":
            matches = re.findall(r'<title>(?P<title>.*?)</title>', html, re.MULTILINE | re.IGNORECASE)
            if len(matches) > 0:
                ret += "{}\t{}".format(matches, r["url"]) + "\n"
        elif function == "links":
            matches = re.findall(r'href=["\'].*?["\']', html, re.IGNORECASE | re.MULTILINE)
            if len(matches) > 0:
                ret += "=" * 50 + "\n" + r["url"] + "\n"
                ret += "\n".join(set(matches)) + "\n"
        elif function == "scripts":
            matches = re.findall(r'<script>.*?<script>', html, re.IGNORECASE | re.MULTILINE)
            if len(matches) > 0:
                ret += "=" * 50 + "\n" + r["url"] + "\n"
                ret += r["url"] + "\n"
                ret += "\n".join(matches) + "\n"
        elif function == "hiddenwp":
            matches = re.findall(r'wp-content', html, re.IGNORECASE | re.MULTILINE)
            if len(matches) > 0:
                ret += r["url"] + "\n"
        elif function == "phpinfo":
            matches = re.findall(r'/(phpinfo)\.php', html, re.IGNORECASE | re.MULTILINE)
            if len(matches) > 0:
                ret += r["url"] + "\n"
        elif function == "indexof":
            matches = re.findall(r'<title>Index of /</title>', html, re.MULTILINE | re.IGNORECASE)
            if len(matches) > 0:
                matches = re.findall(r'href=["\']/.*?["\']', html, re.IGNORECASE | re.MULTILINE)
                if len(matches) > 0:
                    ret += "{}".format(r["url"]) + "\n"
                    ret += "\t" + "\n\t".join(set(matches)) + "\n"
        elif function == "regexmatch":
            matches = re.findall(r'/p\.php', html, re.IGNORECASE | re.MULTILINE)
            if len(matches) > 0:
                ret += r["url"] + "\n"

        counter += 1

    return counter, ret


def read_data_file(datafilename):
    with open(datafilename, "rb") as data:
        while True:
            # read objects one by one
            try:
                # NOTE: This is very insecure, _NEVER_ unpickle() user-provided data!
                r = pickle.load(data)
            except EOFError:
                break
            else:
                yield r


def main(datafilename, functionname):
    datareader = read_data_file(datafilename)
    total = 0

    # print(r["http_code"], r["size"], r["ip"], r["url"], r["headers"], len(r["html"]))
    exhausted = False
    processed = 0
    with concurrent.futures.ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = set()
        while not exhausted or len(futures) > 0:
            while not exhausted and len(futures) < MAX_WORKERS:
                objs = []
                while not exhausted and len(objs) < BATCH_SIZE:
                    try:
                        r = next(datareader)
                    except StopIteration:
                        exhausted = True
                        break
                    else:
                        # skip unsuccessful ones
                        processed += 1
                        if r["http_code"] != 200:
                            continue
                        objs.append(r)

                if len(objs) > 0:
                    args = [objs, functionname]
                    future = executor.submit(process_object, *args)
                    futures.add(future)

            # wait for results and collect statistics
            done, not_done = concurrent.futures.wait(futures, timeout=5,
                                                     return_when=concurrent.futures.FIRST_COMPLETED)
            for future in done:
                resultnum, result = future.result()

                if len(result) > 0:
                    print(result, end="")

                print("STATUS: {} {}".format(processed, total), file=sys.stderr)
                total += resultnum

            futures = not_done

    #process_object(r, function)


if __name__ == "__main__":
    datafilename = sys.argv[1]
    functionname = None if len(sys.argv) < 3 else sys.argv[2]
    if functionname not in PERMITTED_FUNCTIONS:
        print("Invalid function: {}, should be one of: {}".format(functionname, PERMITTED_FUNCTIONS))
        exit(1)
    main(datafilename, functionname)
