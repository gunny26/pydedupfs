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
import pytc
import logging


class BlockStorageTokyoCabinet2(object):
    """
    Object to handle blocks of data
    this version stores reference information in TokyoCabinet database
    and blocks also stored in TokyoCabinet but in different database
    """

    def __init__(self, db_path, block_path):
        logging.debug("BlockStorage.__init__(%s, %s)" , db_path, block_path)
        self.block_path = block_path
        # block reference, to check if block is used
        # holds mapping digest to number of references
        self.db = pytc.HDB(os.path.join(db_path, "blockref.hdb"), pytc.HDBOWRITER | pytc.HDBOCREAT)
        # hold block data
        self.blockdb = pytc.HDB(os.path.join(db_path, "blockstorage.hdb"), pytc.HDBOWRITER | pytc.HDBOCREAT)

    def put(self, buf, digest):
        """writes buf to filename <hexdigest>"""
        logging.debug("BlockStorage.put(<buf>, digest=%s)", digest)
        filename = os.path.join(self.block_path, digest)
        if self.db.has_key(digest):
            # blockref counter up
            logging.debug("BlockStorage.put: duplicate found")
            self.db.put(digest, str(int(self.db.get(digest)) + 1))
        else:
            # write if this is the first block
            logging.debug("BlockStorage.put: new block")
            self.blockdb.put(digest, buf)
            self.db.put(digest, "1")

    def get(self, digest):
        """reads data from filename <hexdigest>"""
        logging.debug("BlockStorage.get(digest=%s)" , digest)
        return(self.blockdb.get(digest))

    def exists(self, digest):
        """true if entry exists"""
        logging.debug("BlockStorage.exists(digest=%s)" , digest)
        return(self.db.hash_key(digest))

    def delete(self, digest):
        """if last reference delete block, else delete only reference"""
        logging.debug("BlockStorage.delete(digest=%s)" , digest)
        if self.db.has_key(digest):
            if self.db.get(digest) == "1":
                filename = os.path.join(self.block_path, digest)
                self.db.out(digest)
                self.blockdb.out(digest)
            else:
                # reference counter down by one
                self.db.put(digest, str(int(self.db.get(digest)) - 1))

    def report(self, outfunc):
        """prints report with outfunc"""
        num_stored = 0
        num_blocks = len(self.db)
        for key in self.db.keys():
            num_stored += int(self.db.get(key))
        string = ""
        outfunc("Blocks in block_storage : %s" % num_blocks)
        outfunc("de-dedupped blocks      : %s" % num_stored)
        if num_blocks > 0:
                outfunc("Dedup Value         : %0.3f" % (float(num_stored) / float(num_blocks)))
