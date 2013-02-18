#!/usr/bin/env python
# -*- coding: utf-8 -*- 
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Library General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
#
"""MetaStorage Object"""
__author__ = "Arthur Messner <arthur.messner@gmail.com>"
__copyright__ = "Copyright (c) 2013 Arthur Messner"
__license__ = "GPL"
__version__ = "$Revision$"
__date__ = "$Date$"
# $Id

import os
import stat
import sqlite3
import cPickle
import time
import hashlib
import logging
# for statistics and housekeeping threads
import threading
# own modules
from PyDedupFSExceptions import *
from WriteBuffer import WriteBuffer as WriteBuffer

# import any kind of BlockStorage, but they are not compatible,
# so do not change after you have added files
# SQLite base Version - slow !
# from BlockStorageSqlite import BlockStorageSqlite as BlockStorage
# GDBM based version - fastest
from BlockStorageGdbm import BlockStorageGdbm as BlockStorage
# GDBM with Encrypting
# from BlockStorageGdbmEnc import BlockStorageGdbmEnc as BlockStorage
# GDBM and Zlib Compression
#from BlockStorageGdbm import BlockStorageGdbm as BlockStorage

from FileStorage import FileStorage as FileStorage
from StatDefaultDir import StatDefaultDir as StatDefaultDir
from StatDefaultFile import StatDefaultFile as StatDefaultFile


class MetaStorage(object):
    """Holds information about directory structure"""

    def __init__(self, root, blocksize=1024*128, hashfunc=hashlib.sha1):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.INFO)
        self.blocksize = blocksize
        self.hashfunc = hashfunc

        # create root if it doesnt exist
        self.root = root
        if not os.path.isdir(self.root):
            os.mkdir(self.root)
        # under root there will be two directories
        # one holds databases
        db_path = os.path.join(self.root, "meta")
        if not os.path.isdir(db_path):
            os.mkdir(db_path)
        # one holds blocks
        block_path = os.path.join(self.root, "blocks")
        if not os.path.isdir(block_path):
            os.mkdir(block_path)

        # additional classes 
        self.file_storage = FileStorage(db_path)
        self.block_storage = BlockStorage(db_path, block_path)
        self.write_buffer = WriteBuffer(self, self.block_storage, blocksize, hashfunc)

        # sqlite database
        self.conn = sqlite3.connect(os.path.join(db_path, "metastorage.db"))
        # return dict not list
        self.conn.row_factory = sqlite3.Row
        cur = self.conn.cursor()
        # cur.execute("DROP TABLE IF EXISTS metastorage")
        cur.execute("CREATE TABLE IF NOT EXISTS metastorage (parent text, abspath text PRIMARY KEY, digest text, st text)") 
        # create root directory node, if not exists
        if not self.exists("/"):
            self.mkdir("/")
            self.logger.debug("created root node")
        if not self.exists("/."):
            self.mkdir("/.")
        if not self.exists("/.."):
            self.mkdir("/..")
        # start statistics Thread
        self.threads = []
        self.threads.append(threading.Thread(target=self.do_statistics))
        for thread in self.threads:
            # set to daemon thread, so main thread can exit
            thread.setDaemon(True)
            thread.start()

    def do_statistics(self, interval=60):
        """statistics thread"""
        # TODO how to end thread on unmount correctly
        self.logger.error("Statistics Thread started")
        while True:
            # begin endless loop
            self.logger.info("Block Storage Statistics: %s", self.block_storage)
            # sleep for a couple of seconds
            time.sleep(interval)
      
    def __del__(self):
        self.conn.commit()
        self.conn.close()

    def __write(self, abspath, digest, sequence, size):
        """Low level write call to write data to filei_storage and create entry in meta_storage"""
        self.logger.debug("MetaStorage.write(%s, %s, <sequence>)", abspath, digest)
        # generate st struct
        st = StatDefaultFile()
        st.st_size = size
        cur = self.conn.cursor()
        cur.execute("UPDATE metastorage set digest=?, st=? where abspath=?", (digest, cPickle.dumps(st), unicode(abspath)))
        self.conn.commit()
        self.file_storage.put(digest, sequence)

    def write(self, abspath, data):
        """write data to file over write_buffer"""
        len_buf = self.write_buffer.add(data)
        return(len_buf)

    def release(self, abspath):
        """close file after writing, write remaining data in buffers"""
        (filehash, sequence, size) = self.write_buffer.release()
        self.__write(abspath, filehash, sequence, size)

    def __get_st(self, abspath=None, digest=None):
        """returns stat struct"""
        self.logger.debug("MetaStorage.__get_st(%s, %s)", abspath, digest)
        cur = self.conn.cursor()
        c_st = None
        if abspath is not None:
            cur.execute("SELECT st from metastorage where abspath=? limit 1", (unicode(abspath), ))
            for row in cur.fetchall():
                c_st = row["st"]
        elif digest is not None:
            cur.execute("SELECT st from metastorage where digest=?", (digest, ))
            for row in cur.fetchall():
                c_st = row["st"]
        if c_st is None:
            raise NoEntry("No Entry in database")
        st = cPickle.loads(c_st.encode("utf-8"))
        return(st)

    def __put_st(self, st, abspath=None, digest=None):
        """updates table with new """
        self.logger.debug("MetaStorage.__put_st(%s, %s, %s)", st, abspath, digest)
        c_st = cPickle.dumps(st)
        cur = self.conn.cursor()
        if abspath is not None:
            cur.execute("UPDATE metastorage set st=? where abspath=?", (c_st,  unicode(abspath)))
            self.conn.commit()
        elif digest is not None:
            cur.execute("UPDATE metastorage set st=? where digest=?", (c_st, digest))
            self.conn.commit()

    def __path_to_digest(self, abspath):
        """get digest from path"""
        self.logger.debug("__path_to_digest(%s)", abspath)
        cur = self.conn.cursor()
        cur.execute("SELECT digest from metastorage where abspath=?", (unicode(abspath),))
        row = cur.fetchone()
        digest = row["digest"]
        if digest is not None:
            return(digest)
        else:
            raise NoEntry("File %s not found" % abspath)

    def __digest_to_path(self, digest):
        """get path from digest"""
        self.logger.debug("__digest_to_path(%s)", digest)
        cur = self.conn.cursor()
        cur.execute("SELECT abspath from metastorage where digest=?", (digest,))
        row = cur.fetchone()
        abspath = row["abspath"]
        if abspath is not None:
            return(abspath)
        else:
            raise NoEntry("Digest %s not found" % digest)
           
    def read(self, abspath, length, offset):
        """get sequence type list of blocks for path if path exists"""
        self.logger.debug("MetaStorage.read(%s)", abspath)
        digest = self.__path_to_digest(abspath)
        # get sequence from FileStorage
        sequence = self.file_storage.get(digest)
        if sequence is not None:
            return(self.__read(sequence, length, offset))
        else:
            return("")

    def __read(self, sequence, length, offset):
        """Low level Reading function, returns data"""
        index = int(offset / self.blocksize)
        start = offset % self.blocksize
        buf = ""
        while (len(buf) < length) and (index < len(sequence)):
            self.logger.debug("index : %d, start %d", index, start)
            if start > 0:
                buf += self.block_storage.get(sequence[index])[start:]
                start = 0
            else:
                buf += self.block_storage.get(sequence[index])
            index += 1
        if len(buf) > length:
            self.logger.debug("Buffer will be shortened to %d", length)
            buf = buf[:length] + "0x00"
        return(buf[:length])

    def exists(self, abspath):
        """return stat of path if file exists"""
        self.logger.debug("MetaStorage.exists(%s)", abspath)
        try:
            st = self.__get_st(abspath=abspath)
            return(st)
        except NoEntry:
            return(False)

    def isfile(self, abspath):
        """true if entry is a file"""
        self.logger.debug("MetaStorage.isfile(%s)", abspath)
        entry = self.__get_entry(abspath)
        st = entry["st"]
        if st.mode & stat.S_IFREG :
            return(True)
        else:
            return(False)
        raise NoEntry("No such Entry")

    def utime(self, abspath, atime, mtime):
        """sets utimes in st structure"""
        self.logger.debug("MetaStorage.utime(abspath=%s, atime=%s, mtime=%s)", abspath, atime, mtime)
        st = self.__get_st(abspath)
        st.st_mtime = mtime
        st.st_atime = atime
        self.__put_st(st, abspath)
        

    def isdir(self, abspath):
        """true if entry is a directory"""
        self.logger.debug("MetaStorage.isdir(%s)", abspath)
        entry = self.__get_entry(abspath)
        st = entry["st"]
        if st.mode & stat.S_IFDIR :
            return(True)
        else:
            return(False)
        raise NoEntry("No such Entry")

    def readdir(self, abspath):
        """return list of files in parent"""
        self.logger.debug("MetaStorage.readdir(%s)", abspath)
        cur = self.conn.cursor()
        cur.execute("SELECT abspath from metastorage where parent=?", (unicode(abspath),))
        direntries = []
        for row in cur.fetchall():
            # ignore root
            if row["abspath"] == "/":
                continue
            # return only basename ob abspath
            direntries.append(row["abspath"].split("/")[-1].encode("utf-8"))
        return(direntries)

    def __get_path_parent(self, abspath):
        """returns filename and prent directory as string"""
        basename = os.path.basename(abspath)
        dirname = os.path.dirname(abspath)
        return(basename, dirname)

    def touch(self, abspath, mode=None):
        """add a new file in database, but no data to filestorage like touch"""
        self.logger.debug("MetaStorage.touch(%s, %s)", abspath, mode)
        (basename, dirname) = self.__get_path_parent(abspath)
        st = StatDefaultFile()
        if mode is not None:
            st.mode = mode
        self.__add_entry(dirname, abspath, st)

    def mkdir(self, abspath, mode=None):
        """add new directory to database"""
        self.logger.debug("MetaStorage.mkdir(%s, %s)", abspath, mode)
        (basename, dirname) = self.__get_path_parent(abspath)
        st = StatDefaultDir()
        if mode is not None:
            # set directory bit if not set
            st.mode = mode | stat.S_IFDIR
        self.__add_entry(dirname, abspath, st)

    def __add_entry(self, parent, abspath, st, digest=None):
        """add entry to database, both file or directory"""
        self.logger.debug("MetaStorage.__add_entry(%s, %s)", abspath, st)
        cur = self.conn.cursor()
        cur.execute("INSERT into metastorage values (?, ?, ?, ?)", (unicode(parent), unicode(abspath), digest, cPickle.dumps(st)))
        self.conn.commit()

    def __get_entry(self, abspath=None, digest=None):
        """returns full data row to abspath, or digest if not found NoEntry Exception"""
        self.logger.debug("MetaStorage.__get_entry(%s, %s)", abspath, digest)
        cur = self.conn.cursor()
        ret = None
        if abspath is not None:
            cur.execute("SELECT * from metastorage where abspath=?", (abspath,))
            ret = cur.fetchone()
        elif digest is not None:
            cur.execute("SELECT * from metastorage where digest=?", (digest,))
            ret = cur.fetchone()
        else:
            raise NoEntry("either abspath nor digest given, so what do you expect ?")
        if ret is not None:
            return(ret)
        else:
            raise NoEntry("No such entry")

    def unlink(self, abspath):
        """delete file from database"""
        self.logger.debug("MetaStorage.delete(%s)", abspath)
        # special case if file is 0-byte size
        # TODO critical sequence, what to delete first, and what if something went wrong
        try:
            digest = self.__path_to_digest(abspath)
            # get sequence for file
            sequence = self.file_storage.get(digest)
            if sequence is not None or len(sequence) == 0:
                # delete block in sequence
                for block in sequence:
                    self.block_storage.delete(block)
            # delete file in filestorage
            self.file_storage.delete(digest)
        except NoEntry:
            # there is no digest for 0-byte files __path__to_digest will return exception
            self.logger.error("No digest to abspath=%s found, zero byte file", abspath)
        # delete from meta_storage, so directory entry will be deleted
        cur = self.conn.cursor()
        cur.execute("DELETE FROM metastorage WHERE abspath=?", (unicode(abspath), ))
        self.conn.commit()

    def rename(self, abspath, abspath1):
        """rename entry"""
        self.logger.debug("MetaStorage.rename(%s, %s)", abspath, abspath1)
        cur = self.conn.cursor()
        # if file already exists, overwrite
        # TODO ist this the best way ?
        cur.execute("DELETE FROM metastorage WHERE abspath=?", (unicode(abspath1), ))
        cur.execute("UPDATE metastorage SET abspath=? WHERE abspath=?", (unicode(abspath1), unicode(abspath)))    
        self.conn.commit()

    def chown(self, abspath, uid, gid):
        """change ownership information"""
        self.logger.debug("MetaStorage.chown(%s, %s, %s)", abspath, uid, gid)
        st = self.__get_st(abspath=abspath)
        st.st_uid = uid
        st.st_gid = gid
        self.__put_st(st, abspath=abspath)

    def chmod(self, abspath, mode):
        """change mode"""
        self.logger.debug("MetaStorage.chmod(%s, %s)", abspath, mode)
        st = self.__get_st(abspath=abspath)
        st.st_mode = mode
        self.__put_st(st, abspath=abspath)
