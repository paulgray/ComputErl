#!/usr/bin/python

import sys, re

while True:
    if sys.stdin.closed:
        break

    line = sys.stdin.readline()
    print len(re.findall(r'\w+', line))

    sys.stdout.flush()
