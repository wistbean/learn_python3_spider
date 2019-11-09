import os, sys
while 1:
    line = sys.stdin.readline().strip()
    if not line:
        break
    os.close(int(line))
