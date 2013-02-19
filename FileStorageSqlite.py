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

class FileStorageSqlite(object):
    """storage of unique file digests with sqlite backend"""

    def __init__(self, db_path):
        """holds information, to build file with digest from list of blocks"""
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.ERROR)
        # holds mapping digest of file : (sequence, number of references)
        self.conn = sqlite3.connect(os.path.join(db_path, "filestorage.db"))
        # return dict not list
        self.conn.row_factory = sqlite3.Row
        # self.conn.execute("DROP TABLE IF EXISTS filestorage")
        self.conn.execute("CREATE TABLE IF NOT EXISTS filestorage (digest text PRIMARY KEY, nlink int, sequence text)")
        # set isolation level to None, we use only key value store no transactions
        self.conn.execute("PRAGMA journal_mode=OFF")
        # vacuum database at start
        self.conn.execute("VACUUM filestorage")

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
        # TODO find a better way to realize INSERT or UPDATE behaviour
        try:
            with self.conn:
                self.conn.execute("INSERT INTO filestorage VALUES (?, 1, ?)", (digest, cPickle.dumps(sequence)))
        except sqlite3.IntegrityError:
            # so it must be an update
            with self.conn:
                self.conn.execute("UPDATE filestorage set nlink=(nlink+1) where digest=?", (digest, ))
        self.conn.commit()

    def delete(self, digest):
        """delte entry in database"""
        self.logger.debug("FileStorage.delete(%s)", digest)
        with self.conn:
            # first decrease by one
            self.conn.execute("UPDATE filestorage SET nlink=nlink-1 where digest=?", (digest, ))
            # the delete all rows with nlink=0
            self.conn.execute("DELETE FROM filestorage WHERE digest=? and nlink=0", (digest, ))

    def __del__(self):
        self.conn.commit()
        self.conn.close()
