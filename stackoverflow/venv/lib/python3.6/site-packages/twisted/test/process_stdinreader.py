# Copyright (c) Twisted Matrix Laboratories.
# See LICENSE for details.

"""
Script used by twisted.test.test_process on win32.
"""

from __future__ import absolute_import, division

import sys, os, msvcrt
msvcrt.setmode(sys.stdout.fileno(), os.O_BINARY)
msvcrt.setmode(sys.stderr.fileno(), os.O_BINARY)

# We want to write bytes directly to the output, not text, because otherwise
# newlines get mangled. Get the buffer if it is available.
if hasattr(sys.stdout, "buffer"):
    stdout = sys.stdout.buffer
else:
    stdout = sys.stdout

if hasattr(sys.stderr, "buffer"):
    stderr = sys.stderr.buffer
else:
    stderr = sys.stderr

stdout.write(b"out\n")
stdout.flush()
stderr.write(b"err\n")
stderr.flush()

data = sys.stdin.read()

stdout.write(data.encode('ascii'))
stdout.write(b"\nout\n")
stderr.write(b"err\n")

sys.stdout.flush()
sys.stderr.flush()
