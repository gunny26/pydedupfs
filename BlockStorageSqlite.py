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
"""BlockStorage Object"""
__author__ = "Arthur Messner <arthur.messner@gmail.com>"
__copyright__ = "Copyright (c) 2013 Arthur Messner"
__license__ = "GPL"
__version__ = "$Revision$"
__date__ = "$Date$"
# $Id

import os
import sqlite3
import hashlib
import logging
logging.basicConfig(level=logging.DEBUG)


class BlockStorageSqlite(object):
    """Object to handle blocks of data"""

    def __init__(self, root=BLOCKSTOR_PATH):
        logging.debug("BlockStorage.__init__(%s)" , root)
        # base directory of block storage
        self.root = root
        if not os.path.isdir(self.root):
            os.mkdir(self.root)
        assert os.path.isdir(self.root)
        # block reference, to check if block is used
        # holds mapping digest to number of references
        self.conn = sqlite3.connect(os.path.join(METASTOR_PATH, "blockstorage.db"))
        # return dict not list
        self.conn.row_factory = sqlite3.Row
        self.cur = self.conn.cursor()
        self.cur.execute("DROP TABLE IF EXISTS blockstorage")
        self.cur.execute("CREATE TABLE IF NOT EXISTS blockstorage (digest text PRIMARY KEY, nlink int)")

    def put(self, buf, digest):
        """writes buf to filename <hexdigest>"""
        logging.debug("BlockStorage.put(<buf>, digest=%s)" , digest)
        filename = os.path.join(self.root, digest)
        if os.path.isfile(filename):
            # blockref counter up
            logging.debug("BlockStorage.put: duplicate found")
            self.cur.execute("UPDATE blockstorage SET nlink=(nlink+1) where digest=?", (digest,))
        else:
            # write if this is the first block
            logging.debug("BlockStorage.put: new block")
            open(filename, "wb").write(buf)
            self.cur.execute("INSERT INTO blockstorage VALUES (?, 1)", (digest,))
        self.conn.commit()

    def get(self, digest):
        """reads data from filename <hexdigest>"""
        logging.debug("BlockStorage.get(digest=%s)" , digest)
        filename = os.path.join(self.root, digest)
        return(open(filename, "rb").read())

    def exists(self, digest):
        """true if file exists"""
        logging.debug("BlockStorage.exists(digest=%s)" , digest)
        filename = os.path.join(self.root, digest)
        return(os.path.isfile(filename))

    def delete(self, digest):
        """if last reference delete block, else delete only reference"""
        logging.debug("BlockStorage.delete(digest=%s)" , digest)
        self.cur.execute("SELECT nlink FROM blockstorage WHERE digest=?", (digest,))
        row = self.cur.fetchone()
        if row["nlink"] == 1:
            filename = os.path.join(self.root, digest)
            os.unlink(filename)
            self.cur.execute("DELETE FROM blockstorage WHERE digest=?", (digest,))
        else:
            # reference counter down by one
            self.cur.execute("UPDATE blockstorage SET nlink=(nlink-1) where digest=?", (digest,))
        self.conn.commit()

    def __del__(self):
        self.conn.commit()
        self.conn.close()

