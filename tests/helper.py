def filereader(fname):
    with open(fname, "rt") as f:
        for line in f:
            yield line.strip()
