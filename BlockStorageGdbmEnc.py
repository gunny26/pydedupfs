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
import gdbm
import logging
# for encryption
from Crypto.Cipher import DES3


class BlockStorageGdbmEnc(object):
    """Object to handle blocks of data"""

    def __init__(self, db_path, block_path):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.ERROR)
        self.logger.debug("BlockStorage.__init__(%s, %s)" , db_path, block_path)
        self.block_path = block_path
        # block reference, to check if block is used
        # holds mapping digest to number of references
        self.db = gdbm.open(os.path.join(db_path, "blockstorage.gdbm"), "c")
        # see http://www.laurentluce.com/posts/python-and-cryptography-with-pycrypto/
        # for a short introduction to python_crypto
        self.des3 = DES3.new("0123456701234567", DES3.MODE_ECB)

    def put(self, buf, digest):
        """writes buf to filename <hexdigest>"""
        self.logger.debug("BlockStorage.put(<buf>, digest=%s)", digest)
        filename = os.path.join(self.block_path, digest)
        if self.db.has_key(digest):
            # blockref counter up
            self.logger.debug("BlockStorage.put: duplicate found")
            self.db[digest] = str(int(self.db[digest]) + 1)
        else:
            # write if this is the first block
            self.logger.debug("BlockStorage.put: new block")
            open(filename, "wb").write(self.des3.encrypt(buf))
            self.db[digest] = "1"

    def get(self, digest):
        """reads data from filename <hexdigest>"""
        self.logger.debug("BlockStorage.get(digest=%s)" , digest)
        filename = os.path.join(self.block_path, digest)
        return(self.des3.decrypt(open(filename, "rb").read()))

    def exists(self, digest):
        """true if file exists"""
        self.logger.debug("BlockStorage.exists(digest=%s)" , digest)
        filename = os.path.join(self.block_path, digest)
        return(os.path.isfile(filename))

    def delete(self, digest):
        """if last reference delete block, else delete only reference"""
        self.logger.debug("BlockStorage.delete(digest=%s)" , digest)
        if self.db.has_key(digest):
            if int(self.db[digest]) == 1:
                filename = os.path.join(self.block_path, digest)
                os.unlink(filename)
                del self.db[digest]
            else:
                # reference counter down by one
                self.db[digest] = str(int(self.db[digest]) -1)

    def __str__(self):
        """returns statistics dictionary"""
        num_stored = 0
        num_blocks = len(self.db)
        for key in self.db.keys():
            num_stored += int(self.db[key])
        string = ""
        string += "Blocks in block_storage : %s" % num_blocks
        string += "uncompressed blocks     : %s" % num_stored
        if num_blocks > 0:
                string += "savings in percent      : %0.2f" % (100 * float(num_stored) / float(num_blocks) - 1)
        return(string)
