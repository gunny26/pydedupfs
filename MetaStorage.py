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
"""Object"""
__author__ = "Arthur Messner <arthur.messner@gmail.com>"
__copyright__ = "Copyright (c) 2013 Arthur Messner"
__license__ = "GPL"
__version__ = "$Revision$"
__date__ = "$Date$"
# $Id

import os
import cPickle
import time
import logging
# for statistics and housekeeping threads
import threading
# own modules
from WriteBuffer import WriteBuffer as WriteBuffer
from BlockStorageGdbm import BlockStorageGdbm as BlockStorage
from StatDefaultFile import StatDefaultFile as StatDefaultFile


class MetaStorage(object):
    """Holds information about directory structure"""

    def __init__(self, root, blocksize, hashfunc):
        """just __init__"""
        self.logger = logging.getLogger(self.__class__.__name__)
        self.blocksize = blocksize
        self.hashfunc = hashfunc

        # create root if it doesnt exist
        self.root = root
        if not os.path.isdir(self.root):
            os.mkdir(self.root)
        # under root there will be tree directories
        # one holds databases
        db_path = os.path.join(self.root, "meta")
        if not os.path.isdir(db_path):
            os.mkdir(db_path)
        # there are files and directories,
        # in files there is only cPickled information how to assemble
        # real data
        self.file_path = os.path.join(self.root, "files")
        if not os.path.isdir(self.file_path):
            os.mkdir(self.file_path)
        # holds blocks
        block_path = os.path.join(self.root, "blocks")
        if not os.path.isdir(block_path):
            os.mkdir(block_path)

        # additional classes 
        self.block_storage = BlockStorage(db_path, block_path)
        self.write_buffer = WriteBuffer(self, self.block_storage, blocksize, hashfunc)

        # start statistics Thread
        self.threads = []
        self.threads.append(threading.Thread(target=self.do_statistics))
        for thread in self.threads:
            # set to daemon thread, so main thread can exit
            thread.setDaemon(True)
            thread.start()

    def do_statistics(self, interval=60):
        """statistics thread, writes to logger every 60s"""
        # TODO make interval a command line parameter
        self.logger.error("Statistics Thread started")
        while True:
            self.logger.warning("BlockStorage Statistics Report")
            self.block_storage.report(self.logger.warning)
            time.sleep(interval)
      
    def __to_realpath(self, abspath):
        """from abspath with leading / to relative pathnames
        and join it with file_base path"""
        self.logger.debug("__to_realpath(%s)", abspath)
        realpath = os.path.join(self.file_path, abspath[1:])
        self.logger.debug("__to_realpath return %s", realpath)
        return(realpath)

    def write(self, abspath, data):
        """write data to file over write_buffer"""
        # write_buffer returns len(writen_data)
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
        self.__put_entry(abspath, digest, st, sequence)

    def read(self, abspath, length, offset):
        """get sequence type list of blocks for path if path exists"""
        self.logger.info("read(%s)", abspath)
        (digest, st, sequence) = self.__get_entry(abspath)
        if len(sequence) > 0:
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
            # append EOF
            buf = buf[:length] + "0x00"
        return(buf[:length])

    def getattr(self, abspath):
        """return stat of path if file exists"""
        self.logger.info("getattr(%s)", abspath)
        realpath = self.__to_realpath(abspath)
        if os.path.isfile(realpath) or os.path.islink(realpath):
            (digest, st, sequence) = self.__get_entry(abspath)
            return(st)
        else:
            return(os.stat(realpath))

    def utime(self, abspath, atime, mtime):
        """sets utimes in st structure"""
        self.logger.info("utime(abspath=%s, atime=%s, mtime=%s)", abspath, atime, mtime)
        realpath = self.__to_realpath(abspath)
        if os.path.isdir(realpath):
            os.utime(realpath, (atime, mtime))
        else:
            (digest, st, sequence) = self.__get_entry(abspath)
            st.st_mtime = mtime
            st.st_atime = atime
            self.__put_entry(abspath, digest, st, sequence)
        
    def readdir(self, abspath):
        """return list of files of directory"""
        self.logger.info("readdir(%s)", abspath)
        return(os.listdir(self.__to_realpath(abspath)))

    def create(self, abspath, mode=None):
        """like touch, create 0-byte if not exists"""
        self.logger.info("create(%s, %s)", abspath, mode)
        if os.path.exists(self.__to_realpath(abspath)):
            # fail silently if file exists
            return
        else:
            st = StatDefaultFile()
            if mode is not None:
                st.mode = mode
            self.__put_entry(abspath, 0, st, [])
            return(0)

    def mkdir(self, abspath, mode=None):
        """add new directory to database"""
        self.logger.info("mkdir(%s, %s)", abspath, mode)
        os.mkdir(self.__to_realpath(abspath), mode)

    def __get_entry(self, abspath):
        """returns full data row to abspath, or digest"""
        self.logger.debug("__get_entry(%s)", abspath)
        cp_file = file(self.__to_realpath(abspath),"rb")
        (digest, st, sequence) = cPickle.load(cp_file)
        cp_file.close()
        return((digest, st, sequence))

    def __put_entry(self, abspath, digest, st, sequence):
        """write data to file"""
        self.logger.debug("__put_entry(%s, digest=%s, st=%s, <sequence>)", abspath, digest, st)
        cp_file = file(self.__to_realpath(abspath), "wb")
        cPickle.dump((digest, st, sequence), cp_file)
        cp_file.close()
        # TODO remove verify if correct
        (digest1, st1, sequence1) = self.__get_entry(abspath)
        assert digest == digest1
        assert sequence == sequence

    def unlink(self, abspath):
        """delete file from database"""
        self.logger.info("delete(%s)", abspath)
        # TODO critical sequence, what to delete first, and what if something went wrong
        (digest, st, sequence) = self.__get_entry(abspath)
        if sequence is not None or len(sequence) == 0:
            # delete block in sequence
            map(self.block_storage.delete, sequence)
        # finally delete file
        os.unlink(self.__to_realpath(abspath))

    def rmdir(self, abspath):
        """delete directory entry"""
        # we trust fuse that it dont delete parent directories before childs
        os.rmdir(self.__to_realpath(abspath))

    def rename(self, abspath, abspath1):
        """rename entry"""
        self.logger.info("rename(%s, %s)", abspath, abspath1)
        os.rename(self.__to_realpath(abspath), self.__to_realpath(abspath1))

    def copy(self, abspath, abspath1):
        """copy file, not blocks, imitates hardlink"""
        self.logger.info("copy(%s, %s)", abspath, abspath1)
        shutil.copy(self.__to_realpath(abspath), self.__to_relapath(abspath1))

    def chown(self, abspath, uid, gid):
        """change ownership information"""
        self.logger.info("chown(%s, %s, %s)", abspath, uid, gid)
        realpath = self.__to_realpath(abspath)
        if os.path.isdir(realpath):
            os.chown(realpath, uid, gid)
        else:
            (digest, st, sequence) = self.__get_entry(abspath)
            st.st_uid = uid
            st.st_gid = gid

    def chmod(self, abspath, mode):
        """change mode"""
        self.logger.info("chmod(%s, %s)", abspath, mode)
        realpath = self.__to_realpath(abspath)
        if os.path.isdir(realpath):
            os.chmod(realpath, mode)
        else:
            (digest, st, sequence) = self.__get_entry(abspath)
            st.st_mode = mode
            self.__put_entry(abspath, digest, st, sequence)
