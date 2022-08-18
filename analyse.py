import argparse
import sys
import pickle
import zlib
import gzip
import re
import pprint
import glob

import concurrent.futures

from helpers.statcollector import StatCollector

# TODO: Refactor this entire file so that it can be used as a Python module!


ALLOWED_FUNCTIONS = [
    "error", "ip", "raw_html", "headers", "html", "generator", "server", "title", "links", "regexmatch", "scripts",
    "poweredby", "hiddenwp", "phpinfo", "indexof", "adminpanel", "s3bucket", "max"
]


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


# TODO: This function is a mess and should be refactored
def process_object(filename, function, regexp):
    ret = ""

    counter = 0
    for r in __load_pickled_objects(filename):
        counter += 1

        # This is a special case, in every other function we ignore the non 200 status codes!
        if function == "error":
            ret += "{}\t{}".format(r["error"], r["url"]) + "\n"
            continue

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
            matches = re.findall(regexp, html, re.IGNORECASE | re.MULTILINE)
            if len(matches) > 0:
                ret += r["url"] + "\n"
                ret += str(matches) + "\n"

    return counter, ret


def main(fileglob, functionname, max_workers, regexp):
    files = glob.glob(fileglob)
    print("Loaded {} files".format(len(files)), file=sys.stderr)

    stats = StatCollector()
    files_submitted = 0

    total = 0
    last_total = 0

    stats.start_clock()
    exhausted = False
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = set()
        while not exhausted or len(futures) > 0:
            while not exhausted and len(futures) < max_workers:
                if files_submitted >= len(files):
                    exhausted = True
                    break

                nextfile = files[files_submitted]

                args = [nextfile, functionname, regexp]
                future = executor.submit(process_object, *args)
                futures.add(future)
                files_submitted += 1
                stats.add_submitted()

            # wait for results and collect statistics
            done, not_done = concurrent.futures.wait(futures, timeout=5,
                                                     return_when=concurrent.futures.FIRST_COMPLETED)
            for future in done:
                resultnum, result = future.result()

                if len(result) > 0:
                    print(result, end="")

                stats.add_processed(resultnum)

            # print status
            shouldprint, now, delta = stats.should_print(1)
            if shouldprint:
                print(
                    "STATUS: {}/{}, speed: {:d}/s, avg speed: {:d}/s".format(
                        stats.submitted, len(files),
                        int((total-last_total)/delta),
                        int(total / (now - stats.start).total_seconds())
                    ),
                    file=sys.stderr,
                )
                last_total = total

                sys.stderr.flush()
                sys.stdout.flush()

            futures = not_done
    stats.print_final()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--file-glob", type=str, required=True,
                        help="Glob of the files containing the data to analyze: Example: '../datadir/datafiles_*'")
    parser.add_argument("--max-workers", type=int, required=True,
                        help="Number of workers to spawn")
    parser.add_argument("--function", type=str, choices=ALLOWED_FUNCTIONS, required=True,
                        help="Name of the function to use")
    parser.add_argument("--regexp", type=str, required=False,
                        help="A valid regular expression to match, only valid if function is 'regexmatch'")
    args = parser.parse_args()

    fileglob = args.file_glob
    max_workers = args.max_workers
    functionname = args.function
    regexp = args.regexp

    main(fileglob, functionname, max_workers, regexp)
