#!/usr/bin/python
"""
Benchmark Program to see preformance differnece between 
dbm based persistent key store or cPickled key in Files
"""

import random
import string
import time
import cPickle

def create_sample(blocksize=128*1024):
    block=[]
    for x in xrange(blocksize):
        block.append(random.choice(string.hexdigits))
    return("".join(block))
    
block = create_sample(128 * 1024)
blocklen = len(block)

start = time.time()
for x in xrange(1000):
    wfile = open("testfile.dmp", "wb")
    wfile.write(block)
    wfile.close()
    rfile = open("testfile.dmp", "rb")
    block = rfile.read(128*1024)
    assert len(block) == blocklen
    rfile.close
    block = block[::-1]
print "Duration : %s" % (time.time()-start)

start = time.time()
nref = 1
for x in xrange(1000):
    wfile = open("testfile.dmp", "wb")
    cPickle.dump((nref, block), wfile)
    wfile.close()
    rfile = open("testfile.dmp", "rb")
    nref, block = cPickle.load(rfile)
    assert len(block) == blocklen
    rfile.close()
    block = block[::-1]
print "Duration with cPickle: %s" % (time.time()-start)

start = time.time()
nref = 1
for x in xrange(1000):
    open("testfile.dmp", "wb").write(block)
    open("testfile.ifo", "wb").write(str(nref))
    block = open("testfile.dmp", "rb").read()
    nref = int(open("testfile.ifo", "rb").read())
    assert len(block) == blocklen
    block = block[::-1]
print "Duration with two line solution: %s" % (time.time()-start)

    
