#!/usr/bin/python

import os
import cPickle
import hashlib
import sys

def __blockverify(checksum):
    hashfunc = hashlib.sha1()
    hashfunc.update(open(os.path.join(block_basedir, "%s.dmp" % checksum), "rb").read())
    assert hashfunc.hexdigest() == checksum
    return(True)

def blockverify():
    for blockdigest in os.listdir(block_basedir):
        if blockdigest[-3:] == "dmp":
            filename = os.path.join(block_basedir, blockdigest)
            if __blockverify(blockdigest[:-4]) is False:
                print "block checksum is incorrect"
    return(True)

def blockdedup():
    ifos = os.listdir(block_basedir)
    counter = 0
    blockcounter = 0
    saved = 0
    totalsize = 0
    size_stored = 0
    for ifo in ifos:
        if ifo[-3:] == "ifo":
            filename = os.path.join(block_basedir, ifo)
            count = int(open(filename, "rb").read())
            size = os.stat(filename[:-3]+"dmp").st_size
            if count > 1:
                counter += count - 1
                saved += size * (count - 1)
            totalsize += size * count
            size_stored += size
            blockcounter += 1
    print "found %d unique blocks" % blockcounter
    print "in total %d bytes stored" % totalsize
    print "in total %d bytes on disk" % size_stored
    print "found %d deduplicated blocks" % counter
    print "%d bytes saved with deduplication" % saved
    assert (totalsize - saved) == size_stored

def __fileverify(filedigest):
    filename = os.path.join(filedigest_basedir, filedigest)
    try:
        refcounter, sequence = cPickle.load(open(filename, "rb"))
        digest = hashlib.sha1()
        for checksum in sequence:
            block_filename = os.path.join(block_basedir, "%s.dmp" % checksum)
            block_data = open(block_filename, "rb").read()
            digest.update(block_data)
        assert filedigest == digest.hexdigest()
    except EOFError, exc:
        print "EOFError at file %s" % filename
    return(True)

def fileverify():
    for filedigest in os.listdir(filedigest_basedir):
        try:
            __fileverify(filedigest)
        except AssertionError:
            print "file checksum %s is incorrect" % filedigest

def filededup():
    dedupcounter = 0
    filecounter = 0
    for filedigest in os.listdir(filedigest_basedir):
        filename = os.path.join(filedigest_basedir, filedigest)
        try:
            refcounter, sequence = cPickle.load(open(filename, "rb"))
        except EOFError, exc:
            print "EOFError at file %s" % filename
        if refcounter > 1:
            dedupcounter += refcounter - 1
        filecounter += 1
    print "found %d unique files in total" % filecounter
    print "found %d duplicate files" % dedupcounter 

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print "usage %s <base path to pydedup data>" % sys.argv[0]
        sys.exit(1)
    if not os.path.isdir(sys.argv[1]):
        print "path %s does not exist or is not accessible" % sys.argv[1]
        sys.exit(2)
    block_basedir = os.path.join(sys.argv[1], "blocks")
    filedigest_basedir = os.path.join(sys.argv[1], "filedigest")
    blockdedup()
    filededup()
    blockverify()
    fileverify()
