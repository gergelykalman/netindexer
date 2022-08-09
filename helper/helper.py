import os
import pickletools


def find_next_unpicklable_offset(f_in, minimum_offset):
    """Returns None on EOF"""
    # reopen file so we don't mess things up for others
    newfd = os.dup(f_in.fileno())
    f = os.fdopen(newfd, "rb", buffering=-1)

    origpos = f_in.tell()
    f.seek(origpos, os.SEEK_SET)

    done, eof = False, False
    while not done:
        for opcode, args, pos in pickletools.genops(f):
            # print(opcode.name, args, pos, f.tell())
            curpos = f.tell()
            if curpos > origpos + minimum_offset and opcode.name == "PROTO":
                # print(opcode.name, args, pos, f.tell())

                # set pos to PROTO start
                ret = curpos - pos
                print("GOOD POS", ret, minimum_offset)

                import pickle
                f.seek(ret, os.SEEK_SET)
                print("AAAAAAAAA", pickle.load(f))

                f.seek(curpos)

                return ret

    # no such position
    return None
