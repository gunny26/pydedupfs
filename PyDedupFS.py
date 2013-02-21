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
"""PyDeDupFS Main Program"""
__author__ = "Arthur Messner <arthur.messner@gmail.com>"
__copyright__ = "Copyright (c) 2013 Arthur Messner"
__license__ = "GPL"
__version__ = "$Revision$"
__date__ = "$Date$"
# $Id

import os  
import sys
# hack to set default encoding to utf-8
reload(sys)
sys.setdefaultencoding('utf-8')
import fuse
if not hasattr(fuse, '__version__'):
    raise RuntimeError, \
        "your fuse-py doesn't know of fuse.__version__, probably it's too old."
fuse.fuse_python_api = (0, 2)
fuse.feature_assert('stateful_files', 'has_init')
# Logging
import logging
import logging.config
# read logging config from logging.conf
logging.config.fileConfig("logging.conf")
# hashing library
import hashlib
# own modules
from MetaStorage import MetaStorage as MetaStorage
from PyDedupFile import PyDedupFile as PyDedupFile

# DEFAULT values use, if no others are applied
DEFAULT_BLOCKSIZE = 1024 * 128
DEFAULT_HASHFUNC = "sha1"

class PyDedupFS(fuse.Fuse):
    """Fuse Interface Class"""

    def __init__(self, *args, **kw):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.debug("PyDedupFS.__init__(%s, %s)", args, kw)
        # TODO : are there other parameters to set
        self.direct_io = False
        self.keep_cache = False
        self.fsname = "PyDedupFS"
        # define meta_storage
        self.meta_storage = None
        # the last element of command line options
        # is the mount_point
        self.mountpoint = sys.argv[-1]
        # dirty flag, to indicate, some data must be written to disk
        self.isdirty = False
        fuse.Fuse.__init__(self, *args, **kw)
        # set default values, 
        # cmd options have to be initialized in __init__ first
        self.base = None
        self.blocksize = DEFAULT_BLOCKSIZE
        self.str_hashfunc = "sha1"
        self.hashfunc = hashlib.sha1
        # wrapper to own file class, to pass meta_storage object
        # has to be in __init__ to function properly
        class PyDedupFileWrapper(PyDedupFile):
            def __init__(self2, *a, **kw):
                PyDedupFile.__init__(self2, self.meta_storage, *a, **kw)
        # set own file class
        self.file_class = PyDedupFileWrapper

    def getattr(self, path):
        """
        return stat information
        return errno.ENOENT if File does not exist
        """
        self.logger.debug("PyDedupFS.getattr(%s)", path)
        return(self.meta_storage.getattr(path))

    def readlink(self, path):
        """
        return path to which link is pointing to
        NOT IMPLEMENTED : it doesnt make sense on this type of filesystem
        """
        self.logger.debug("PyDedupFS.readlink(%s)", path)
        return(None)

    def readdir(self, path, offset):
        """
        yield fuse.Direntry(str(name of file), inode)
        prepend . and .. entries
        """
        self.logger.debug("PyDedupFS.readdir(%s, offset=%s)", path, offset)
        for entry in self.meta_storage.readdir(path):
            yield fuse.Direntry(entry)

    def unlink(self, path):
        """unlink file"""
        self.logger.debug("PyDedupFS.unlink(%s)", path)
        self.meta_storage.unlink(path)
        return(0)

    def rmdir(self, path):
        """remove directory"""
        self.logger.debug("PyDedupFS.rmdir(%s)", path)
        self.meta_storage.rmdir(path)
        return(0)

    def truncate(self, path, offset):
        """change the size of a file"""
        self.logger.debug("PyDedupFS.truncate(%s, offset=%s)", path, offset)

    def symlink(self, path, symlink):
        """
        create symlink from path to path1
        path must exist, symlink must not exist
        path is no absolute path, relative to actual position
        NOT IMPLEMENTED, conecpt of symlinks is realy hard to implement in concep of PyDedupFS
        """
        self.logger.debug("PyDedupFS.symlink(%s, %s)", path, symlink)
        # path is the real existing file
        # given relative to fuse mountpoint
        #relpath = os.path.relpath(path, self.mountpoint)
        #self.logger.debug("corrected relpath:%s", os.path.relpath(path, self.mountpoint))
        #self.logger.debug("corrected absolute path:%s", os.path.normpath(os.path.join(self.mountpoint, relpath)))
        #self.logger.debug("corrected absolute path:%s", os.path.normpath(os.path.join(self.mountpoint, path)))
        #self.meta_storage.symlink(path, symlink)
        return(None)

    def rename(self, path, path1):
        """
        return 0 if all went ok
        return EACCESS if privileges permit renaming
        retunr ENOENT if file doesn not exist
        return EROFS if filesystem is read only
        if target name exists, delete file it
        """
        self.logger.debug("PyDedupFS.rename(%s, %s)", path, path1)
        self.meta_storage.rename(path, path1)
        return(0)

    def link(self, path, path1):
        """
        return 0 if all went ok
        # From the link(2) manual page: "If link_path names a directory, link()
        # shall fail unless the process has appropriate privileges and the
        # implementation supports using link() on directories." ... :-)
        # However I've read that FUSE doesn't like multiple directory pathnames
        # with the same inode number (maybe because of internal caching based on
        # inode numbers?).
        """
        self.logger.debug("PyDedupFS.link(%s, %s)", path, path1)
        # map link to copy
        self.meta_storage.copy(path, path1)

    def chmod(self, path, mode):
        """
        0 if success
        errno.EIO if something went wrong
        """
        self.logger.debug("PyDedupFS.chmod(%s, mode=%s)", path, mode)
        self.meta_storage.chmod(path, mode)
        return(0)

    def chown(self, path, user, group):
        """
        0 is success
        errno.EIO if something went wrong
        """
        self.logger.debug("PyDedupFS.chown(%s, user=%s, group=%s)", path, user, group)
        self.meta_storage.chown(path, user, group)
        return(0)

    def mknod(self, path, mode, dev):
        """
        we dont want special Files
        NOT IMPLEMENTED : we dont want special Files
        """
        self.logger.debug("PyDedupFS.mknod(%s, mode=%s, dev=%s)", path, mode, dev)
        return(None)

    def mkdir(self, path, mode):
        """create directory"""
        self.logger.debug("PyDedupFS.mkdir(%s, mode=%s)", path, mode)
        self.meta_storage.mkdir(path, mode)
        return(0)

    def utime(self, path, times):
        """deprecated applications should use utimens"""
        self.logger.debug("PyDedupFS.utime(%s, times=%s)", path, times)
        atime, mtime = times
        self.meta_storage.utime(path, atime, mtime)
        return(0)

    def utimens(self, path, ts_atime, ts_mtime):
        """sets mtime and atime of file"""
        self.logger.debug("PyDedupFS.utimens(%s, %s.%s, %s.%s)", path, ts_atime.tv_sec, ts_atime.tv_nsec, ts_mtime.tv_sec, ts_mtime.tv_nsec)
        atime = ts_atime.tv_sec + (ts_atime.tv_nsec / 1000000.0)
        mtime = ts_mtime.tv_sec + (ts_mtime.tv_nsec / 1000000.0)
        self.meta_storage.utime(path, atime, mtime)

    def access(self, path, mode):
        """
        0 if file is accessible
        -errno.EACCES if file is not accessible
        -errno.ENOENT if file does not exists or other error
        """
        self.logger.debug("PyDedupFS.access(%s, mode=%s)", path, mode)

    def statfs(self):
        """
        Should return an object with statvfs attributes (f_bsize, f_frsize...).
        Eg., the return value of os.statvfs() is such a thing (since py 2.2).
        If you are not reusing an existing statvfs object, start with
        fuse.StatVFS(), and define the attributes.

        To provide usable information (ie., you want sensible df(1)
        output, you are suggested to specify the following attributes:

            - f_bsize - preferred size of file blocks, in bytes
            - f_frsize - fundamental size of file blcoks, in bytes
                [if you have no idea, use the same as blocksize]
            - f_blocks - total number of blocks in the filesystem
            - f_bfree - number of free blocks
            - f_files - total number of file inodes
            - f_ffree - nunber of free file inodes
        """
        self.logger.debug("PyDedupFS.statfs()")
        # from https://github.com/xolox/dedupfs/blob/master/dedupfs.py
        # TODO make this a parameter
        host_fs = os.statvfs(self.base)
        return fuse.StatVfs(
            f_bavail = (host_fs.f_bsize * host_fs.f_bavail) / self.blocksize, 
            # The total number of free blocks available to a non privileged process.
            f_bfree = (host_fs.f_frsize * host_fs.f_bfree) / self.blocksize, 
            # The total number of free blocks in the file system.
            f_blocks = (host_fs.f_frsize * host_fs.f_blocks) / self.blocksize, 
            # The total number of blocks in the file system in terms of f_frsize.
            f_bsize = self.blocksize, 
            # The file system block size in bytes.
            f_favail = 0, 
            # The number of free file serial numbers available to a non privileged process.
            f_ffree = 0, 
            # The total number of free file serial numbers.
            f_files = 0, 
            # The total number of file serial numbers.
            f_flag = 0, 
            # File system flags. Symbols are defined in the <sys/statvfs.h> header file to refer to bits in this field (see The f_flags field).
            f_frsize = self.blocksize, 
            # The fundamental file system block size in bytes.
            f_namemax = 4294967295) 
            # The maximum file name length in the file system. Some file systems may return the maximum value that can be stored in an unsigned long to indicate the file system has no maximum file name length. The maximum value that can be stored in an unsigned long is defined in <limits.h> as ULONG_MAX.

    def fsinit(self):
        """called after fs initialization"""
        self.logger.debug("PyDedupFS.fsinit()")
        try:
            options = self.cmdline[0]
            self.logger.info(options.blocksize)
            self.base = options.base
            self.logger.info("Base Directory : %s", self.base)
            self.blocksize = options.blocksize
            self.logger.info("Blocksize      : %s", self.blocksize)
            self.str_hasfunc = options.str_hashfunc
            self.logger.info("Hash Function  : hashlib.%s", self.hashfunc)
            self.hashfunc = getattr(hashlib, self.str_hashfunc)
        except StandardError, exc:
            self.logger.exception(exc)
        # initialize meta_storage properly
        self.meta_storage = MetaStorage(self.base, self.blocksize, self.hashfunc)

        # wrapper to own file class, to pass meta_storage object
        #class wrapped_file_class(PyDedupFile):
        #    def __init__(self2, *a, **kw):
        #        PyDedupFile.__init__(self2, self.meta_storage, *a, **kw)
        # set own file class
        #self.file_class = wrapped_file_class

    def main(self, *a, **kw):
        # enter endless loop
        print self.__dict__
        print sys.argv[-1]
        return fuse.Fuse.main(self, *a, **kw)


def main():
    """main() what else"""
    usage = """PyDedupFS : Python Deduplication Filesystem """+ fuse.Fuse.fusage
    server = PyDedupFS(version="%prog " + fuse.__version__,
                 usage=usage,
                 dash_s_do='setsingle')
    # put some additional values into parser
    # TODO should be in -o Parameters to function in fstab entry
    server.parser.add_option("--base", dest="base", type="str", 
        help="Directory to store the real data blocks and meta informations")
    server.parser.add_option("--hashfunc", dest="str_hashfunc", 
        type="str", default=DEFAULT_HASHFUNC,
        help="Hashfunction to use <sha1, md5, sha256>, default %default")
    server.parser.add_option("--blocksize", dest="blocksize", 
        type="int", default=DEFAULT_BLOCKSIZE,
        help="Blocksize to use, default %default")
   
    # does not work with threads
    server.multithreaded = False
    server.parse(values=server, errex=1)
    server.main()


if __name__ == '__main__':
    # add option -f for foreground display an fuse for mountpoint
    # add --base as base directory for real data
    # sys.argv = [sys.argv[0], "-f", "--hashfunc=sha1", "--base=/home/mesznera/pydedupfs", "/home/mesznera/fuse"]
    # TODO: make Profiling a command line switch
    if "--profile" in sys.argv:
        # Profiling
        # dont pass this option to fuse
        sys.argv.remove("--profile")
        import cProfile
        import pstats
        profile = "PyDedupFS.profile"
        cProfile.runctx( "main()", globals(), locals(), filename=profile)
        s = pstats.Stats(profile)
        s.sort_stats('time')
        s.print_stats(0.1)
        os.unlink(profile)
    else:
        main()
