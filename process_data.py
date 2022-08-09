import sys
import pickle
import zlib
import gzip
import re
import pprint
import glob

import concurrent.futures
from datetime import datetime as dt, timedelta as td


PERMITTED_FUNCTIONS = [
    "ip", "raw_html", "headers", "html", "generator", "server", "title", "links", "regexmatch", "scripts",
    "poweredby", "hiddenwp", "phpinfo", "indexof", "adminpanel", "s3bucket", "max"]


def process_html(headers, body):
    server, html = "", ""
    html_bytes = bytearray()
    for k, v in headers.items():
        k = k.lower()
        if k == "content-encoding":
            if v == "gzip":
                try:
                    html_bytes += gzip.decompress(body)
                except (EOFError, gzip.BadGzipFile, zlib.error):
                    # bad gzip
                    return server, html
            else:
                # NOTE: this happens fairly rarely so it's muted for now
                # print("ERROR: Invalid encoding: {}".format(v), file=sys.stderr)
                return server, html

            break
        elif k == "server":
            server = v

    html = html_bytes.decode("utf-8", errors="ignore")

    return server, html


def __load_pickled_objects(filename):
    with gzip.open(filename, "rb") as f:
        while True:
            try:
                # NOTE: This is very insecure, _NEVER_ unpickle() user-provided data!
                results = pickle.load(f)
            except EOFError:
                break
            else:
                for r in results:
                    yield r


def process_object(filename, function):
    ret = ""

    counter = 0
    for r in __load_pickled_objects(filename):
        counter += 1

        if r["http_code"] != 200:
            continue

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
                ret += "{}\t{}".format(matches[0], r["url"]) + "\n"
        elif function == "title":
            matches = re.findall(r'<title>(?P<title>.*?)</title>', html, re.MULTILINE | re.IGNORECASE)
            if len(matches) > 0:
                title = matches[0]
                ret += "{}\t{}".format(r["url"], title) + "\n"
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
        elif function == "adminpanel":
            matches = re.findall(r'<title>(?P<title>.*?)</title>', html, re.MULTILINE | re.IGNORECASE)
            if len(matches) > 0:
                title = matches[0]
                matches2 = re.findall(r'.*?(admin|login).*?', title, re.IGNORECASE)
                if len(matches2) > 0:
                    ret += "{}\t{}".format(r["url"], title) + "\n"
        elif function == "s3bucket":
            matches = re.finditer('(https?://[^.]*?\.s3\.amazonaws\.com/|http?s://s3\.amazonaws\.com/[^/]*?/)', html, re.MULTILINE | re.IGNORECASE)
            buckets = set()
            for match in matches:
                bucketname = match.groups()[0]
                buckets.add(bucketname)
            for bucketname in buckets:
                ret += "{}\t{}".format(r["url"], bucketname) + "\n"
        elif function == "max":
            matches = re.findall(r'<title>(?P<title>.*?)</title>', html, re.MULTILINE | re.IGNORECASE)
            if len(matches) > 0:
                title = matches[0]
                matches2 = re.findall(r"(phpmyadmin|phpldapadmin|tivoli|nas|san|sap|torrent|router|switch|webcam|scada|plc|nvr|storage|ipmi|firewall|grafana|prometheus|dashboard|kubernetes|swagger|jira|redmine|confluence|mantis|nagios|icinga)",
                           title, re.IGNORECASE | re.MULTILINE)
                if len(matches2) > 0:
                    ret += r["url"] + "\t" + title + "\n"
        elif function == "regexmatch":
            matches = re.findall(r'/p\.php', html, re.IGNORECASE | re.MULTILINE)
            if len(matches) > 0:
                ret += r["url"] + "\n"

    return counter, ret


def main(datafileglob, functionname, max_workers):
    files = glob.glob(datafileglob)
    print("Loaded {} files".format(len(files)), file=sys.stderr)
    files_submitted = 0

    total = 0

    # print(r["http_code"], r["size"], r["ip"], r["url"], r["headers"], len(r["html"]))
    exhausted = False
    resultnum = 0
    start = dt.now()
    last_status, last_total = dt.now(), 0
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = set()
        while not exhausted or len(futures) > 0:
            while not exhausted and len(futures) < max_workers:
                if files_submitted >= len(files):
                    exhausted = True
                    break

                nextfile = files[files_submitted]

                args = [nextfile, functionname]
                future = executor.submit(process_object, *args)
                futures.add(future)

                files_submitted += 1

            # wait for results and collect statistics
            done, not_done = concurrent.futures.wait(futures, timeout=5,
                                                     return_when=concurrent.futures.FIRST_COMPLETED)
            for future in done:
                resultnum, result = future.result()

                if len(result) > 0:
                    print(result, end="")

            total += resultnum

            # print status
            now = dt.now()
            delta = (now-last_status).total_seconds()
            if delta > 1:
                print("STATUS: {}/{}, speed: {:d}/s, avg speed: {:d}/s"
                .format(
                    files_submitted, len(files),
                    int((total-last_total)/delta),
                    int(total/(now-start).total_seconds())
                ), file=sys.stderr)
                last_status = now
                last_total = total

                sys.stderr.flush()
                sys.stdout.flush()

            futures = not_done


if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: ./process_data.py datafilename max_workers functionname")
        exit(1)

    datafilename = sys.argv[1]
    max_workers = int(sys.argv[2])
    functionname = sys.argv[3]

    if functionname not in PERMITTED_FUNCTIONS:
        print("Invalid function: {}, should be one of: {}".format(functionname, PERMITTED_FUNCTIONS))
        exit(1)

    main(datafilename, functionname, max_workers)
