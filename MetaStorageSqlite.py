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

# import one type of FileStorage
# original version, also sqlite but not tuned
# from FileStorage import FileStorage as FileStorage
# tuned sqlite version
# from FileStorageSqlite import FileStorageSqlite as FileStorage
# gdbm version
from FileStorageGdbm import FileStorageGdbm as FileStorage

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
        # TODO no journal and temporary store in memory
        # self.conn.execute("PRAGMA temp_store=MEMORY;")
        self.conn.execute("PRAGMA journal_mode=WAL;")
        # return dict not list
        self.conn.row_factory = sqlite3.Row
        # cur.execute("DROP TABLE IF EXISTS metastorage")
        self.conn.execute("CREATE TABLE IF NOT EXISTS metastorage (parent text, abspath text PRIMARY KEY, digest text, st text)") 
        # vacuum table on statup
        self.conn.execute("VACUUM metastorage")
        # create root directory node, if not exists
        try:
            self.getattr("/")
        except NoEntry:
            self.mkdir("/")
            self.mkdir("/.")
            self.mkdir("/..")
            self.logger.debug("created root nodes")
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

    def write(self, abspath, data):
        """write data to file over write_buffer"""
        # from write_buffer len(writen_data) comes back
        return(self.write_buffer.add(data))

    def release(self, abspath):
        """
        close file after writing, write remaining data in buffers
        and finalize file information in meta_storage
        """
        # get the latest informations about the written file
        (digest, sequence, size) = self.write_buffer.release()
        # generate st struct
        st = StatDefaultFile()
        st.st_size = size
        with self.conn:
            self.conn.execute("UPDATE metastorage set digest=?, st=? where abspath=?", (digest, cPickle.dumps(st), unicode(abspath)))
            self.file_storage.put(digest, sequence)

    def __get_stat_by_abspath(self, abspath=None):
        """returns stat struct"""
        self.logger.debug("MetaStorage.__get_stat_by_abspath(%s)", abspath)
        cur = self.conn.cursor()
        cur.execute("SELECT st from metastorage where abspath=?", (unicode(abspath), ))
        row = cur.fetchone()
        if row is None:
            raise NoEntry("No Entry in database")
        st = cPickle.loads(row["st"].encode("utf-8"))
        return(st)

    def __put_stat_by_abspath(self, st, abspath):
        """updates table with new stat informations"""
        self.logger.debug("MetaStorage.__put_stat_by_abspath(%s, %s)", st, abspath)
        c_st = cPickle.dumps(st)
        with self.conn:
            self.conn.execute("UPDATE metastorage set st=? where abspath=?", (c_st,  unicode(abspath)))

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

    def getattr(self, abspath):
        """return stat of path if file exists or throws NoEntry"""
        self.logger.debug("MetaStorage.exists(%s)", abspath)
        st = self.__get_stat_by_abspath(abspath)
        return(st)

    def utime(self, abspath, atime, mtime):
        """sets utimes in st structure"""
        self.logger.debug("MetaStorage.utime(abspath=%s, atime=%s, mtime=%s)", abspath, atime, mtime)
        st = self.__get_stat_by_abspath(abspath)
        st.st_mtime = mtime
        st.st_atime = atime
        self.__put_stat_by_abspath(st, abspath)
        
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
            # return only relative pathnames
            direntries.append(row["abspath"].split("/")[-1].encode("utf-8"))
        return(direntries)

    def create(self, abspath, mode=None):
        """add a new file in database, but no data to filestorage like touch"""
        self.logger.debug("MetaStorage.create(%s, %s)", abspath, mode)
        dirname = os.path.dirname(abspath)
        st = StatDefaultFile()
        if mode is not None:
            st.mode = mode
        self.__add_entry(dirname, abspath, st)
        return(st)

    def mkdir(self, abspath, mode=None):
        """add new directory to database"""
        self.logger.debug("MetaStorage.mkdir(%s, %s)", abspath, mode)
        dirname = os.path.dirname(abspath)
        st = StatDefaultDir()
        if mode is not None:
            # set directory bit if not set
            st.mode = mode | stat.S_IFDIR
        self.__add_entry(dirname, abspath, st)

    def __add_entry(self, parent, abspath, st, digest=None):
        """add entry to database, both file or directory"""
        self.logger.debug("MetaStorage.__add_entry(%s, %s)", abspath, st)
        with self.conn:
            self.conn.execute("INSERT into metastorage values (?, ?, ?, ?)", (unicode(parent), unicode(abspath), digest, cPickle.dumps(st)))

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
                map(self.block_storage.delete, sequence)
            # delete file in filestorage
            self.file_storage.delete(digest)
        except NoEntry:
            # there is no digest for 0-byte files __path__to_digest will return exception
            self.logger.error("No digest to abspath=%s found, zero byte file", abspath)
        # delete from meta_storage, so directory entry will be deleted
        with self.conn:
            self.conn.execute("DELETE FROM metastorage WHERE abspath=?", (unicode(abspath), ))

    def rmdir(self, abspath):
        """delete directory entry"""
        # we trust fuse that it dont delete parent directories before childs
        with self.conn:
            self.conn.execute("DELETE from metastorage where abspath=?", (unicode(abspath), ))

    def rename(self, abspath, abspath1):
        """rename entry"""
        self.logger.debug("MetaStorage.rename(%s, %s)", abspath, abspath1)
        # if file already exists, delete
        # TODO ist this the best way ?
        with self.conn:
            self.conn.execute("DELETE FROM metastorage WHERE abspath=?", (unicode(abspath1), ))
            self.conn.execute("UPDATE metastorage SET abspath=? WHERE abspath=?", (unicode(abspath1), unicode(abspath)))    

    def chown(self, abspath, uid, gid):
        """change ownership information"""
        self.logger.debug("MetaStorage.chown(%s, %s, %s)", abspath, uid, gid)
        st = self.__get_stat_by_abspath(abspath)
        st.st_uid = uid
        st.st_gid = gid
        self.__put_stat_by_abspath(st, abspath)

    def chmod(self, abspath, mode):
        """change mode"""
        self.logger.debug("MetaStorage.chmod(%s, %s)", abspath, mode)
        st = self.__get_stat_by_abspath(abspath)
        st.st_mode = mode
        self.__put_stat_by_abspath(st, abspath)
