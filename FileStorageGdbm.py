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
"""FileStorage Object"""
__author__ = "Arthur Messner <arthur.messner@gmail.com>"
__copyright__ = "Copyright (c) 2013 Arthur Messner"
__license__ = "GPL"
__version__ = "$Revision$"
__date__ = "$Date$"
# $Id

import os
import gdbm
import cPickle
import logging

class FileStorageGdbm(object):
    """storage of unique file digests in gdbm backend"""

    def __init__(self, db_path):
        """holds information, to build file with digest from list of blocks"""
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.ERROR)
        # holds mapping digest of file : (sequence, number of references)
        self.gdbm = gdbm.open(os.path.join(db_path, "filestorage.gdbm"), "c")

    def get(self, digest):
        """returns information about file with digest"""
        self.logger.debug("FileStorage.get(%s)", digest)
        try:
            (nlinks, sequence) = cPickle.loads(self.gdbm[digest])
            return(sequence)
        except KeyError:
            return(None)

    def put(self, digest, sequence):
        """adds information to file with digest"""
        self.logger.debug("FileStorage.put(%s, <sequence>)", digest)
        if self.gdbm.has_key(digest):
            # entry exists, refrence counter up by 1
            (nlinks, sequence) = cPickle.loads(self.gdbm[digest])
            nlinks = str(int(nlinks) + 1)
            self.gdbm[digest] = cPickle.dumps((nlinks, sequence))
        else:
            # new entry
            self.gdbm[digest] = cPickle.dumps(("1", sequence))

    def delete(self, digest):
        """delte entry in database"""
        self.logger.debug("FileStorage.delete(%s)", digest)
        if self.gdbm.has_key(digest):
            (nlinks, sequence) = cPickle.loads(self.gdbm[digest])
            nlinks = int(nlinks)
            if nlinks == 1:
                # this is the last reference, so delete entry
                del self.gdbm[digest]
            else:
                # reference counter down by 1
                self.gdbm[digest] = cPickle.dumps((str(nlinks - 1), sequence))
        else:
            logging.error("File with digest %s not found in file_storage", digest)
        # nothing special to return either if found or not found
        return()
