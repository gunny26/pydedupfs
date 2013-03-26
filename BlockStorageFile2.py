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
import logging


class BlockStorageFile(object):
    """Object to handle blocks of data"""

    def __init__(self, db_path, block_path):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.ERROR)
        self.logger.debug("BlockStorage.__init__(%s, %s)" , db_path, block_path)
        self.block_path = block_path

    def put(self, buf, digest):
        """writes buf to filename <hexdigest>"""
        self.logger.debug("BlockStorage.put(<buf>, digest=%s)", digest)
        filename = os.path.join(self.block_path, "%s.dmp" % digest)
        ifo_filename = os.path.join(self.block_path, "%s.ifo" % digest)
        if os.path.isfile(filename):
            # blockref counter up
            self.logger.debug("BlockStorage.put: duplicate found")
            # reference counter up by one
            nref = int(open(ifo_filename).read()) + 1
            # write it back
            open(ifo_filename, "wb").write(str(nref))
        else:
            # block is written the first time
            self.logger.debug("BlockStorage.put: new block")
            open(filename, "wb").write(buf)
            open(ifo_filename, "wb").write("1")    

    def get(self, digest):
        """reads data from filename <hexdigest>"""
        self.logger.debug("BlockStorage.get(digest=%s)" , digest)
        rfile = open(os.path.join(self.block_path, "%s.dmp" % digest), "rb")
        data = rfile.read()
        rfile.close()
        return(data)

    def exists(self, digest):
        """true if file exists"""
        self.logger.debug("BlockStorage.exists(digest=%s)" , digest)
        filename = os.path.join(self.block_path, "%s.dmp" % digest)
        return(os.path.isfile(filename))

    def delete(self, digest):
        """if last reference delete block, else delete only reference"""
        self.logger.debug("BlockStorage.delete(digest=%s)" , digest)
        filename = os.path.join(self.block_path, "%s.dmp" % digest)
        ifo_filename = os.path.join(self.block_path, "%s.ifo" % digest)
        if os.path.isfile(filename):
            nref = int(open(ifo_filename).read())
            if nref == 1:
                os.unlink(filename)
                os.unlink(ifo_filename)
            else:
                # reference counter down by one
                open(ifo_filename).write(str(nref -1))

    def report(self, outfunc):
        """prints report with outfunc"""
        num_stored = 0
        num_blocks = 0
        for filename in os.path.listdir(self.block_path):
            if filename.name[-3:] == "dmp":
                num_blocks += 1
            if filename.name[-3:] == "ifo":
                num_stored += int(open(os.path.join(self.block_path, filename).read()))
        outfunc("Blocks in block_storage : %s" % num_blocks)
        outfunc("de-dedupped blocks      : %s" % num_stored)
        if num_blocks > 0:
                outfunc("Dedup Value         : %0.3f" % (float(num_stored) / float(num_blocks)))
