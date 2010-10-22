#!/usr/bin/python

import sys, re

for line in sys.stdin:
    print len(re.findall(r'\w+', line))

    sys.stdout.flush()
