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
import sqlite3
import cPickle
import logging

class FileStorage(object):
    """storage of unique file digests"""

    def __init__(self, db_path):
        """holds information, to build file with digest from list of blocks"""
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.ERROR)
        # holds mapping digest of file : (sequence, number of references)
        self.conn = sqlite3.connect(os.path.join(db_path, "filestorage"))
        # return dict not list
        self.conn.row_factory = sqlite3.Row
        cur = self.conn.cursor()
        # cur.execute("DROP TABLE IF EXISTS filestorage")
        cur.execute("CREATE TABLE IF NOT EXISTS filestorage (digest text PRIMARY KEY, nlink int, sequence text)")

    def get(self, digest):
        """returns information about file with digest"""
        self.logger.debug("FileStorage.get(%s)", digest)
        cur = self.conn.cursor()
        cur.execute("SELECT sequence FROM filestorage WHERE digest = ?", (digest, ))
        row = cur.fetchone()
        if row is not None:
            return(cPickle.loads(row["sequence"].encode("utf-8")))
        else:
            return(None)

    def put(self, digest, sequence):
        """adds information to file with digest"""
        self.logger.debug("FileStorage.put(%s, <sequence>)", digest)
        cur = self.conn.cursor()
        try:
            cur.execute("INSERT INTO filestorage VALUES (?, 1, ?)", (digest, cPickle.dumps(sequence)))
        except sqlite3.IntegrityError:
            # TODO find better to insert OR update
            cur.execute("UPDATE filestorage set nlink=(nlink+1) where digest=?", (digest, ))
        self.conn.commit()

    def delete(self, digest):
        """delte entry in database"""
        self.logger.debug("FileStorage.delete(%s)", digest)
        cur = self.conn.cursor()
        cur.execute("SELECT nlink FROM filestorage WHERE digest=?", (digest, ))
        row = cur.fetchone()
        if row is None:
            # return silently if no entry is found in database
            return()
        if row["nlink"] == 1:
            cur.execute("DELETE FROM filestorage WHERE digest=?", (digest, ))
        else:
            cur.execute("UPDATE filestorage SET nlink=nlink-1 where digest=?", (digest, ))
            self.logger.info("Duplicate File found with digest %s", digest)
        self.conn.commit()

    def __destroy__(self):
        self.conn.commit()
        self.conn.close()
