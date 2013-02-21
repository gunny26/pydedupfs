#/usr/bin/env python
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
"""PyDeDupFile Object"""
__author__ = "Arthur Messner <arthur.messner@gmail.com>"
__copyright__ = "Copyright (c) 2013 Arthur Messner"
__license__ = "GPL"
__version__ = "$Revision$"
__date__ = "$Date$"
# $Id

# Logging
import logging


class PyDedupFile(object):
    """Fuse File Interface"""

    def __init__(self, meta_storage, path, flags, *mode):
        """called to open file"""
        # use explicit Logger Naming, because this class is wrapped
        # so __class__.__name__ would be not PyDedupFile
        self.logger = logging.getLogger("PyDedupFile")
        self.logger.info("PyDedupFile.__init__(%s, %s, %s)", path, flags, mode)
        self.path = path
        self.mode = mode
        self.flags = flags
        self.meta_storage = meta_storage
        # TODO : are there other parameters to set
        self.direct_io = False
        self.keep_cache = False
        # dirty flag, used in release()
        self.isdirty = False
        # touch file, it has to exist after __init__
        self.meta_storage.create(self.path)

    def read(self, length, offset):
        """
        return data on success
        return errno.EIO is something went wrong
        """
        self.logger.debug("PyDedupFile.read(%s, %s)", length, offset)
        return(self.meta_storage.read(self.path, length, offset))

    def write(self, buf, offset):
        """
        return len of written data
        return errno.EACCES is File is not writeable
        return errno.EIO if something went wrong
        """
        self.logger.debug("PyDedupFile.write(<buf>, %s)", offset)
        self.isdirty = True
        # write returns lenght of written data
        return(self.meta_storage.write(self.path, buf, offset))

    def release(self, flags):
        """
        return 0 if all is OK
        return errno.EIO if something went wrong
        close file reference, no idea how to implement
        for every open one release
        NOT IMPLEMETED stub
        """
        self.logger.info("PyDedupFile.release(%s)", flags)

    def fsync(self, isfsyncfile):
        """NOT Implemented stub, is it necessary"""
        self.logger.info("PyDedupFile.fsync(%s)", isfsyncfile)

    def flush(self):
        """close file, write remaining buffers if dirty"""
        self.logger.info("PyDedupFile.flush()")
        if self.isdirty is True:
            self.meta_storage.release(self.path)
            self.isdirty = False

    def fgetattr(self):
        """return st struct for file"""
        self.logger.info("PyDedupFile.fgetattr()")
        return(self.meta_storage.getattr(self.path))

    def ftruncate(self, length):
        """NOT Implemented stub"""
        self.logger.info("PyDedupFile.ftruncate(%s)", length)
