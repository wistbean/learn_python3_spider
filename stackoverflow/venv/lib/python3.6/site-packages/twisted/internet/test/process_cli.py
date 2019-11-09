from __future__ import absolute_import, division

import sys
import os

try:
    # On Windows, stdout is not opened in binary mode by default,
    # so newline characters are munged on writing, interfering with
    # the tests.
    import msvcrt
    msvcrt.setmode(sys.stdout.fileno(), os.O_BINARY)
except ImportError:
    pass


# Loop over each of the arguments given and print it to stdout
for arg in sys.argv[1:]:
    res = arg + chr(0)

    if sys.version_info < (3, 0):
        stdout = sys.stdout
    else:
        stdout = sys.stdout.buffer
        res = res.encode(sys.getfilesystemencoding(), "surrogateescape")

    stdout.write(res)
    stdout.flush()
