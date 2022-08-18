BATCHSIZE_DEFAULT = 10*1024*1024


class FileReader:
    def __init__(self, fname, batch_size_bytes=BATCHSIZE_DEFAULT):
        self.__fname = fname
        self.__f = open(fname, "rt")
        self.__batch_size_bytes = batch_size_bytes
        self.__finished = False

        self.__oldbuf = ""
        self.__used = 0
        self.__buf_lines = []

    def get_batch(self, batchsize):
        ret = []
        while True:
            need = batchsize - len(ret)
            if need == 0:
                break

            tmp = self.__buf_lines[self.__used:self.__used+need]
            ret += tmp
            self.__used += len(tmp)

            # is buf_lines exhausted?
            if self.__used == len(self.__buf_lines):
                if not self.__finished:
                    added = self.__fillbuf()
                    if added == 0:
                        self.__finished = True
                else:
                    # we won't be able to get new lines, return what we have
                    break

        # return full batch
        return ret

    def __fillbuf(self):
        # set oldbuf, used and buf_lines here

        # reset used counter
        self.__used = 0

        # reset buf_lines
        self.__buf_lines = []

        # try to preserve oldbuf and use it to fetch the next batch
        buf = self.__oldbuf
        buf += self.__f.read(self.__batch_size_bytes)
        if len(buf) == 0:
            return 0

        self.__buf_lines = buf.split("\n")
        del buf

        # handle partial lines
        if len(self.__buf_lines) > 1:
            self.__oldbuf = self.__buf_lines[-1]
            del self.__buf_lines[-1]

        return len(self.__buf_lines)
