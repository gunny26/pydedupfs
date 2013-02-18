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
import errno
import fuse
if not hasattr(fuse, '__version__'):
    raise RuntimeError, \
        "your fuse-py doesn't know of fuse.__version__, probably it's too old."
fuse.fuse_python_api = (0, 2)
fuse.feature_assert('stateful_files', 'has_init')
# Logging
import logging
logging.basicConfig(level=logging.DEBUG)
# Profiling
import cProfile
import pstats
# own modules
from MetaStorage import MetaStorage as MetaStorage


class PyDedupFS(fuse.Fuse):
    """Fuse Interface Class"""

    def __init__(self, *args, **kw):
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.INFO)
        self.logger.debug("PyDedupFS.__init__(%s, %s)", args, kw)
        fuse.Fuse.__init__(self, *args, **kw)

    def getattr(self, path):
        """
        return stat information
        return errno.ENOENT if File does not exist
        """
        self.logger.debug("PyDedupFS.getattr(%s)", path)
        st = meta_storage.exists(path)
        if st is not False:
            return(st)
        else:
            return(-errno.ENOENT)

    def readlink(self, path):
        self.logger.debug("PyDedupFS.readlink(%s)", path)
        

    def readdir(self, path, offset):
        """
        yield fuse.Direntry(str(name of file), inode)
        prepend . and .. entries
        """
        self.logger.debug("PyDedupFS.readdir(%s, %s)", path, offset)
        for entry in meta_storage.readdir(path):
            yield fuse.Direntry(entry)

    def unlink(self, path):
        self.logger.debug("PyDedupFS.unlink(%s)", path)
        meta_storage.unlink(path)

    def rmdir(self, path):
        self.logger.debug("PyDedupFS.rmdir(%s)", path)

    def symlink(self, path, path1):
        self.logger.debug("PyDedupFS.symlink(%s, %s)", path, path1)

    def rename(self, path, path1):
        """
        return 0 if all went ok
        return EACCESS if privileges permit renaming
        retunr ENOENT if file doesn not exist
        return EROFS if filesystem is read only
        if target name exists, delete file it
        """
        self.logger.debug("PyDedupFS.rename(%s, %s)", path, path1)
        meta_storage.rename(path, path1)
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

    def chmod(self, path, mode):
        """
        0 if success
        errno.EIO if something went wrong
        """
        self.logger.debug("PyDedupFS.chmod(%s, %s)", path, mode)
        meta_storage.chmod(path, mode)
        return(0)

    def chown(self, path, user, group):
        """
        0 is success
        errno.EIO if something went wrong
        """
        self.logger.debug("PyDedupFS.chown(%s, %s, %s)", path, user, group)
        meta_storage.chown(path, user, group)
        return(0)

    def truncate(self, path, length):
        self.logger.debug("PyDedupFS.truncate(%s, %s)", path, length)

    def mknod(self, path, mode, dev):
        self.logger.debug("PyDedupFS.mknod(%s, %s, %s)", path, mode, dev) 

    def mkdir(self, path, mode):
        self.logger.debug("PyDedupFS.mkdir(%s, %s)", path, mode)
        meta_storage.mkdir(path, mode)
        return(0)

    def utime(self, path, times):
        self.logger.debug("PyDedupFS.utime(%s, %s)", path, times)
        atime, mtime = times
        meta_storage.utime(path, atime, mtime)
        return(0)

    def access(self, path, mode):
        """
        0 if file is accessible
        -errno.EACCES if file is not accessible
        -errno.ENOENT if file does not exists or other error
        """
        self.logger.debug("PyDedupFS.access(%s, %s)", path, mode)

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
        statvfs = fuse.StatVfs()
        statvfs.f_bsize = 1024 * 128
        statvfs.f_frsize = 1024 * 128

    def fsinit(self):
        self.logger.debug("PyDedupFS.fsinit()")


    class PyDedupFile(object):
        """represents a deduplicted file"""

        def __init__(self, path, flags, *mode):
            self.logger = logging.getLogger(self.__class__.__name__)
            self.logger.setLevel(logging.INFO)
            self.logger.debug("PyDedupFile.__init__(%s, %s, %s)", path, flags, mode)
            self.path = path
            # TODO : are there other parameters to set
            self.direct_io = False
            self.keep_cache = False
            # dirty flag
            self.isdirty = False
            # Buffer Converter
            st = meta_storage.exists(self.path)
            if st is False:
                self.logger.debug("creating new file")
                meta_storage.touch(self.path, mode)
            else:
                self.logger.info("working on existing file")

        def read(self, length, offset):
            """
            return data on success
            return errno.EIO is something went wrong
            """
            self.logger.debug("PyDedupFile.read(%s, %s)", length, offset)
            dd_buf = meta_storage.read(self.path, length, offset)
            return(dd_buf)

        def write(self, buf, offset):
            """
            return len of written data
            return errno.EACCES is File is not writeable
            return errno.EIO if something went wrong
            """
            self.logger.debug("PyDedupFile.write(<buf>, %s)", offset)
            self.isdirty = True
            len_buf = meta_storage.write(self.path, buf)
            return len_buf

        def release(self, flags):
            """
            return 0 if all is OK
            return errno.EIO if something went wrong
            close file an write all remaining dirty buffers
            """
            self.logger.debug("PyDedupFile.release(%s)", flags)

        def fsync(self, isfsyncfile):
            """TODO dont know"""
            self.logger.debug("PyDedupFile.fsync(%s)", isfsyncfile)

        def flush(self):
            """end file write"""
            self.logger.debug("PyDedupFile.flush()")
            if self.isdirty is True:
                meta_storage.release(self.path)
            else:
                self.isdirty = False

        def fgetattr(self):
            """return st struct for file"""
            self.logger.debug("PyDedupFile.fgetattr()")
            st = meta_storage.exists(self.path)
            return(st)

        def ftruncate(self, length):
            """TODO dont know"""
            self.logger.debug("PyDedupFile.ftruncate(%s)", length)

    def main(self, *a, **kw):
        # set own file class
        self.file_class = self.PyDedupFile
        # enter endless loop
        return fuse.Fuse.main(self, *a, **kw)


def main():
    usage = """PyDedupFS : Python Deduplication Filesystem """+ fuse.Fuse.fusage
    server = PyDedupFS(version="%prog " + fuse.__version__,
                 usage=usage,
                 dash_s_do='setsingle')
    server.multithreaded = False
    server.parse(values=server, errex=1)
    server.main()


if __name__ == '__main__':
    # add option -d for foreground display an fuse for mountpoint
    # add --base as base directory for real data
    sys.argv = [sys.argv[0], "-f", "/home/mesznera/fuse"]
    # global MetaStorage Object
    meta_storage = MetaStorage(root="/home/mesznera/pydedupfs")
    profile = "PyDedupFS.profile"
    cProfile.runctx( "main()", globals(), locals(), filename=profile)
    s = pstats.Stats(profile)
    s.sort_stats('time')
    s.print_stats(0.1)
    os.unlink(profile)
    # cProfile.run("main()")
