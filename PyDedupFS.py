#!/usr/bin/env python

#    Copyright (C) 2001  Jeff Epler  <jepler@unpythonic.dhs.org>
#    Copyright (C) 2006  Csaba Henk  <csaba.henk@creo.hu>
#
#    This program can be distributed under the terms of the GNU LGPL.
#    See the file COPYING.
#

import os
import sys
import errno
import stat
import fuse
if not hasattr(fuse, '__version__'):
    raise RuntimeError, \
        "your fuse-py doesn't know of fuse.__version__, probably it's too old."
fuse.fuse_python_api = (0, 2)
fuse.feature_assert('stateful_files', 'has_init')

import sqlite3
import gdbm
import cPickle
import time
import hashlib
import logging
import cProfile
logging.basicConfig(level=logging.DEBUG)


BLOCKSTOR_PATH = "./blockstor"
METASTOR_PATH = "./metastor"


def flag2mode(flags):
    md = {os.O_RDONLY: 'r', os.O_WRONLY: 'w', os.O_RDWR: 'w+'}
    m = md[flags & (os.O_RDONLY | os.O_WRONLY | os.O_RDWR)]
    if flags | os.O_APPEND:
        m = m.replace('w', 'a', 1)
    return m


class ZeroLengthBlock(Exception):
    """Exception thrown when length of data is zero"""
    def __init__(self, msg):
        self.msg = msg


class NotInMetaStorage(Exception):
    """Exception thrown when not entry in MetaStore exists"""
    def __init__(self, msg):
        self.msg = msg

class NoSuchFile(Exception):
    """Exception thrown if file doesnt exist in metastorage"""
    def __init__(self, msg):
        self.msg = msg

class NoEntry(Exception):
    """represents errno.ENOENT"""
    def __init__(self, msg):
        self.msg = msg

class ReadOnlyFS(Exception):
    """represents errno.EROFS"""
    def __init__(self, msg):
        self.msg = msg


class StatDefaultDir(fuse.Stat):
    """stat struct"""

    def __init__(self):
        self.st_mode = stat.S_IFDIR | 0755
        self.st_ino = 0
        self.st_dev = 0
        self.st_nlink = 2
        self.st_uid = 0
        self.st_gid = 0
        self.st_size = 4096
        self.st_atime = int(time.time())
        self.st_mtime = int(time.time())
        self.st_ctime = int(time.time())


class StatDefaultFile(fuse.Stat):
    """stat struct"""

    def __init__(self):
        self.st_mode = stat.S_IFREG | 0666
        self.st_ino = 0
        self.st_dev = 0
        self.st_nlink = 1
        self.st_uid = 0
        self.st_gid = 0
        self.st_size = 0
        self.st_atime = int(time.time())
        self.st_mtime = int(time.time())
        self.st_ctime = int(time.time())


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
        logging.debug("BlockStorage.put(len(buf)=%s, digest=%s)" , len(buf), digest)
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

class BlockStorageGdbm(object):
    """Object to handle blocks of data"""

    def __init__(self, root=BLOCKSTOR_PATH):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.ERROR)
        self.logger.debug("BlockStorage.__init__(%s)" , root)
        # base directory of block storage
        self.root = root
        # check root
        if not os.path.isdir(self.root):
            os.mkdir(self.root)
        # block reference, to check if block is used
        # holds mapping digest to number of references
        self.db = gdbm.open("blockstorage.gdbm", "c")

    def put(self, buf, digest):
        """writes buf to filename <hexdigest>"""
        self.logger.debug("BlockStorage.put(len(buf)=%s, digest=%s)" , len(buf), digest)
        filename = os.path.join(self.root, digest)
        if self.db.has_key(digest):
            # blockref counter up
            self.logger.debug("BlockStorage.put: duplicate found")
            self.db[digest] = str(int(self.db[digest]) + 1)
        else:
            # write if this is the first block
            self.logger.debug("BlockStorage.put: new block")
            open(filename, "wb").write(buf)
            self.db[digest] = "1"

    def get(self, digest):
        """reads data from filename <hexdigest>"""
        self.logger.debug("BlockStorage.get(digest=%s)" , digest)
        filename = os.path.join(self.root, digest)
        return(open(filename, "rb").read())

    def exists(self, digest):
        """true if file exists"""
        self.logger.debug("BlockStorage.exists(digest=%s)" , digest)
        filename = os.path.join(self.root, digest)
        return(os.path.isfile(filename))

    def delete(self, digest):
        """if last reference delete block, else delete only reference"""
        self.logger.debug("BlockStorage.delete(digest=%s)" , digest)
        if self.db.has_key(digest):
            if int(self.db[digest]) == 1:
                filename = os.path.join(self.root, digest)
                os.unlink(filename)
                del self.db[digest]
            else:
                # reference counter down by one
                self.db[digest] = str(int(self.db[digest]) -1)

    def __destroy__(self):
        size = 0
        blocks = len(self.db)
        for key in self.db.keys():
            size += 1
        self.logger.debug("Blocks in block_storage: %s", blocks)
        self.logger.debug("uncompressed size      : %s", size)
        self.logger.debug("savings in percent     : %0.2f", float(size/blocks))
        self.db.close()


class FileStorage(object):
    """storage of unique file digests"""

    def __init__(self, root=METASTOR_PATH):
        """holds information, to build file with digest from list of blocks"""
        self.root = root
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.ERROR)
        # holds mapping digest of file : (sequence, number of references)
        self.conn = sqlite3.connect(os.path.join(self.root, "filestorage"))
        # return dict not list
        self.conn.row_factory = sqlite3.Row
        cur = self.conn.cursor()
        cur.execute("DROP TABLE IF EXISTS filestorage")
        cur.execute("CREATE TABLE IF NOT EXISTS filestorage (digest text PRIMARY KEY, nlink int, sequence text)")

    def get(self, digest):
        """returns information about file with digest"""
        self.logger.debug("FileStorage.get(%s)", digest)
        cur = self.conn.cursor()
        cur.execute("SELECT sequence FROM filestorage WHERE digest = ?", (digest, ))
        row = cur.fetchone()
        return(cPickle.loads(str(row["sequence"])))

    def put(self, digest, sequence):
        """adds information to file with digest"""
        self.logger.debug("FileStorage.put(%s, %s)", digest, len(sequence))
        cur = self.conn.cursor()
        cur.execute("UPDATE filestorage set nlink=(nlink+1) where digest=?", (digest, ))
        try:
            cur.execute("INSERT INTO filestorage VALUES (?, 1, ?)", (digest, cPickle.dumps(sequence)))
        except sqlite3.IntegrityError:
            # TODO find better to insert OR update
            pass
        self.conn.commit()

    def delete(self, digest):
        """delte entry in database"""
        self.logger.debug("FileStorage.delete(%s)", digest)
        cur = self.conn.cursor()
        cur.execute("SELECT nlink FROM filestorage WHERE digest=?", (digest, ))
        row = cur.fetchone()
        if row["nlink"] == 1:
            cur.execute("DELETE FROM filestorage WHERE digest=?", (digest, ))
        else:
            cur.execute("UPDATE filestorage SET nlink=nlink-1 where digest=?", (digest, ))
        self.conn.commit()

    def __destroy__(self):
        self.conn.commit()
        self.conn.close()


class MetaStorage(object):
    """Holds information about directory structure"""

    def __init__(self, root=METASTOR_PATH, blocksize=1024*128, hashfunc=hashlib.sha1):
        self.root = root
        if not os.path.isdir(self.root):
            os.mkdir(self.root)
        self.blocksize = blocksize
        self.hashfunc = hashfunc
        self.file_storage = FileStorage()
        self.block_storage = BlockStorageGdbm()
        self.write_buffer = WriteBuffer(self, self.block_storage, blocksize, hashfunc)


        # sqlite database
        self.conn = sqlite3.connect(os.path.join(root, "metastorage.db"))
        # return dict not list
        self.conn.row_factory = sqlite3.Row
        cur = self.conn.cursor()
        cur.execute("DROP TABLE IF EXISTS metastorage")
        cur.execute("CREATE TABLE IF NOT EXISTS metastorage (parent text, abspath text PRIMARY KEY, digest text, st text)") 
        # create root directory node, if not exists
        if not self.exists("/"):
            self.mkdir("/")
            logging.debug("created root node")
        if not self.exists("/."):
            self.mkdir("/.")
        if not self.exists("/.."):
            self.mkdir("/..")

    def __del__(self):
        self.conn.commit()
        self.conn.close()

    def __write(self, abspath, digest, sequence, size):
        """Low level write call to write data to filei_storage and create entry in meta_storage"""
        logging.debug("MetaStorage.write(%s, %s, len(%s))", abspath, digest, len(sequence))
        # generate st struct
        st = StatDefaultFile()
        st.st_size = size
        cur = self.conn.cursor()
        cur.execute("UPDATE metastorage set digest=?, st=? where abspath=?", (digest, cPickle.dumps(st), abspath))
        self.conn.commit()
        self.file_storage.put(digest, sequence)

    def write(self, abspath, data):
        """write data to file over write_buffer"""
        len_buf = self.write_buffer.add(data)
        return(len_buf)

    def release(self, abspath):
        """close file after writing, write remaining data in buffers"""
        (filehash, sequence, size) = self.write_buffer.release()
        self.__write(abspath, filehash, sequence, size)

    def __get_st(self, abspath=None, digest=None):
        """returns stat struct"""
        logging.debug("MetaStorage.__get_st(%s, %s)", abspath, digest)
        cur = self.conn.cursor()
        c_st = None
        if abspath is not None:
            cur.execute("SELECT st from metastorage where abspath=? limit 1", (abspath, ))
            for row in cur.fetchall():
                c_st = row["st"]
        elif digest is not None:
            cur.execute("SELECT st from metastorage where digest=?", (digest, ))
            for row in cur.fetchall():
                c_st = row["st"]
        if c_st is None:
            raise NoEntry("No Entry in database")
        st = cPickle.loads(str(c_st))
        return(st)

    def __put_st(self, st, abspath=None, digest=None):
        """updates table with new """
        logging.debug("MetaStorage.__put_st(%s, %s, %s)", st, abspath, digest)
        c_st = cPickle.dumps(st)
        cur = self.conn.cursor()
        if abspath is not None:
            cur.execute("UPDATE metastorage set st=? where abspath=?", (c_st,  abspath))
            self.conn.commit()
        elif digest is not None:
            cur.execute("UPDATE metastorage set st=? where digest=?", (c_st, digest))
            self.conn.commit()

    def __path_to_digest(self, abspath):
        """get digest from path"""
        cur = self.conn.cursor()
        cur.execute("SELECT digest from metastorage where abspath=?", (abspath,))
        row = cur.fetchone()
        digest = str(row["digest"])
        logging.debug("__path_to_digest found %s", digest)
        if digest is not None:
            return(digest)
        else:
            raise NoEntry("File %s not found" % abspath)

    def __digest_to_path(self, digest):
        """get path from digest"""
        cur = self.conn.cursor()
        cur.execute("SELECT abspath from metastorage where digest=?", (digest,))
        row = cur.fetchone()
        abspath = str(row["abspath"])
        logging.debug("__digest_to_path found %s", digest)
        if path is not None:
            return(abspath)
        else:
            raise NoEntry("Digest %s not found" % digest)
           
    def read(self, abspath, length, offset):
        """get sequence type list of blocks for path if path exists"""
        logging.debug("MetaStorage.read(%s)", abspath)
        digest = self.__path_to_digest(abspath)
        # get sequence from FileStorage
        sequence = self.file_storage.get(digest)
        if sequence is not None:
            return(self.__read(sequence, length, offset))
        else:
            return("")

    def __read(self, sequence, length, offset):
        """Low level Reading function, returns data"""
        index = int(offset / self.blocksize)
        start = offset % self.blocksize
        buf = ""
        while (len(buf) < length) and (index < len(sequence)):
            logging.debug("index : %d, start %d, len(seq) %d", index, start, len(sequence))
            if start > 0:
                buf += self.block_storage.get(sequence[index])[start:]
                start = 0
            else:
                buf += self.block_storage.get(sequence[index])
            index += 1
        logging.debug("Total len(buf): %d", len(buf))
        if len(buf) > length:
            logging.debug("Buffer will be shortened to %d", length)
            buf = buf[:length] + "0x00"
        return(buf[:length])

    def exists(self, abspath):
        """return stat of path if file exists"""
        logging.debug("MetaStorage.exists(%s)", abspath)
        try:
            st = self.__get_st(abspath=abspath)
            return(st)
        except NoEntry:
            return(False)

    def isfile(self, abspath):
        """true if entry is a file"""
        logging.debug("MetaStorage.isfile(%s)", abspath)
        entry = self.__get_entry(abspath)
        st = entry["st"]
        if st.mode & stat.S_IFREG :
            return(True)
        else:
            return(False)
        raise NoEntry("No such Entry")

    def utime(self, abspath, atime, mtime):
        """sets utimes in st structure"""
        logging.debug("MetaStorage.utime(abspath=%s, atime=%s, mtime=%s)", abspath, atime, mtime)
        st = self.__get_st(abspath)
        st.st_mtime = mtime
        st.st_atime = atime
        self.__put_st(st, abspath)
        

    def isdir(self, abspath):
        """true if entry is a directory"""
        logging.debug("MetaStorage.isdir(%s)", abspath)
        entry = self.__get_entry(abspath)
        st = entry["st"]
        if st.mode & stat.S_IFDIR :
            return(True)
        else:
            return(False)
        raise NoEntry("No such Entry")

    def readdir(self, abspath):
        """return list of files in parent"""
        logging.debug("MetaStorage.readdir(%s)", abspath)
        cur = self.conn.cursor()
        logging.debug("SELECT abspath from metastorage where parent=%s" % abspath)
        cur.execute("SELECT abspath from metastorage where parent=?", (abspath,))
        direntries = []
        for row in cur.fetchall():
            # ignore root
            if row["abspath"] == "/":
                continue
            # return only basename ob abspath
            direntries.append(str(row["abspath"].split("/")[-1]))
        return(direntries)

    def __get_path_parent(self, abspath):
        """returns filename and prent directory as string"""
        basename = os.path.basename(abspath)
        dirname = os.path.dirname(abspath)
        return(basename, dirname)

    def touch(self, abspath, mode=None):
        """add a new file in database, but no data to filestorage like touch"""
        logging.debug("MetaStorage.touch(%s, %s)", abspath, mode)
        (basename, dirname) = self.__get_path_parent(abspath)
        st = StatDefaultFile()
        if mode is not None:
            st.mode = mode
        self.__add_entry(dirname, abspath, st)

    def mkdir(self, abspath, mode=None):
        """add new directory to database"""
        logging.debug("MetaStorage.mkdir(%s, %s)", abspath, mode)
        (basename, dirname) = self.__get_path_parent(abspath)
        st = StatDefaultDir()
        if mode is not None:
            # set directory bit if not set
            st.mode = mode | stat.S_IFDIR
        self.__add_entry(dirname, abspath, st)

    def __add_entry(self, parent, abspath, st, digest=None):
        """add entry to database, both file or directory"""
        logging.debug("MetaStorage.__add_entry(%s, %s)", abspath, st)
        cur = self.conn.cursor()
        cur.execute("INSERT into metastorage values (?, ?, ?, ?)", (parent, abspath, digest, cPickle.dumps(st)))
        self.conn.commit()

    def __get_entry(self, abspath=None, digest=None):
        """returns full data row to abspath, or digest if not found NoEntry Exception"""
        logging.debug("MetaStorage.__get_entry(%s, %s)", abspath, digest)
        cur = self.conn.cursor()
        ret = None
        if abspath is not None:
            cur.execute("SELECT * from metastorage where abspath=?", (abspath,))
            ret = cur.fetchone()
        elif digest is not None:
            cur.execute("SELECT * from metastorage where digest=?", (digest,))
            ret = cur.fetchone()
        else:
            raise NoEntry("either abspath nor digest given, so what do you expect ?")
        if ret is not None:
            return(ret)
        else:
            raise NoEntry("No such entry")

    def unlink(self, abspath):
        """delete file from database"""
        logging.debug("MetaStorage.delete(%s)", abspath)
        # TODO critical sequence, what to delete first, and what if something went wrong 
        digest = self.__path_to_digest(abspath)
        if digest != "0":
            # get sequence for file
            sequence = self.file_storage.get(digest)
            for block in sequence:
                # delete block in sequence
                self.block_storage.delete(block)
            # delete file in filestorage
            self.file_storage.delete(digest)
        cur = self.conn.cursor()
        cur.execute("DELETE FROM metastorage WHERE abspath=?", (abspath, ))
        self.conn.commit()

    def rename(self, abspath, abspath1):
        """rename entry"""
        logging.debug("MetaStorage.rename(%s, %s)", abspath, abspath1)
        cur = self.conn.cursor()
        # if file already exists, overwrite
        # TODO ist this the best way ?
        cur.execute("DELETE FROM metastorage WHERE abspath=?", (abspath1, ))
        cur.execute("UPDATE metastorage SET abspath=? WHERE abspath=?", (abspath1, abspath))    
        self.conn.commit()

    def chown(self, abspath, uid, gid):
        """change ownership information"""
        logging.debug("MetaStorage.chown(%s, %s, %s)", abspath, uid, gid)
        st = self.__get_st(abspath=abspath)
        st.st_uid = uid
        st.st_gid = gid
        self.__put_st(st, abspath=abspath)

    def chmod(self, abspath, mode):
        """change mode"""
        logging.debug("MetaStorage.chmod(%s, %s)", abspath, mode)
        st = self.__get_st(abspath=abspath)
        st.st_mode = mode
        self.__put_st(st, abspath=abspath)


class WriteBuffer(object):
    """Object to hold data with maximum length blocksize, the flush"""

    def __init__(self, meta_storage, block_storage, blocksize, hashfunc=hashlib.sha1):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.ERROR)
        self.logger.debug("WriteBuffer.__init__()")
        self.blocksize = blocksize
        self.hashfunc = hashfunc
        self.meta_storage = meta_storage
        self.block_storage = block_storage
        self.__reinit()

    def flush(self):
        """write self.buf to block_storage"""
        self.logger.debug("WriteBuffer.flush()")
        # flush self.buf
        # blocklevel hash
        blockhash = self.hashfunc()
        blockhash.update(self.buf)
        # filelevel hash
        self.filehash.update(self.buf)
        self.bytecounter += len(self.buf)
        # dedup
        if self.deduphash.has_key(blockhash.hexdigest()):
            self.deduphash[blockhash.hexdigest()] += 1
        else:
            self.deduphash[blockhash.hexdigest()] =1
            # store
            self.block_storage.put(self.buf, blockhash.hexdigest())
        # store in sequence of blocks
        self.sequence.append(blockhash.hexdigest())

    def add(self, data):
        """adds data to buffer and flushes if length > blocksize"""
        #DEBUG: logging.debug("WriteBuffer.add(len(%s))", len(data))
        if (len(self.buf) + len(data)) >= self.blocksize:
            # add only remaining bytes to internal buffer
            self.buf += data[:self.blocksize-len(self.buf)]
            self.logger.debug("Buffer flush len(buf)=%s", len(self.buf))
            assert len(self.buf) == self.blocksize
            self.flush()
            # begin next block buffer
            self.buf = data[self.blocksize:]
        else:
            self.logger.debug("Adding buffer")
            assert len(self.buf) < self.blocksize
            self.buf += data
        return(len(data))

    def __reinit(self):
        """set some coutners to zero"""
        logging.debug("WriteBuffer.__reinit()")
        self.buf = ""
        # counting bytes = len
        self.bytecounter = 0
        # hash for whole file
        self.filehash = self.hashfunc()
        # starttime
        self.starttime = time.time()
        # deduphash
        self.deduphash = {}
        # sequence of blocks
        self.sequence = []

    def release(self):
        """write remaining data, and closes file"""
        self.logger.debug("WriteBuffer.release()")
        if len(self.buf) != 0:
            self.logger.debug("adding remaining data with len %s", len(self.buf))
            self.flush()
        self.logger.debug("File was %d bytes long", self.bytecounter)
        duration = time.time() - self.starttime
        self.logger.debug("Duration %s seconds", duration)
        self.logger.debug("Speed %0.2f B/s", self.bytecounter / duration)
        # write meta information
        # save informations for return
        filehash = self.filehash.hexdigest()
        sequence = self.sequence
        size = self.bytecounter
        # reinitialize counters for next file
        self.__reinit()
        return(filehash, sequence, size)


class PyDedupFS(fuse.Fuse):

    def __init__(self, *args, **kw):
        logging.debug("PyDedupFS.__init__(%s, %s)", args, kw)
        fuse.Fuse.__init__(self, *args, **kw)
        # self.root = '/media/btrfs_pool/PyDedupFS'

    def getattr(self, path):
        """
        return stat information
        return errno.ENOENT if File does not exist
        """
        logging.debug("PyDedupFS.getattr(%s)", path)
        st = meta_storage.exists(path)
        if st is not False:
            return(st)
        else:
            return(-errno.ENOENT)

    def readlink(self, path):
        logging.debug("PyDedupFS.readlink(%s)", path)
        

    def readdir(self, path, offset):
        """
        yield fuse.Direntry(str(name of file), inode)
        prepend . and .. entries
        """
        logging.debug("PyDedupFS.readdir(%s, %s)", path, offset)
        for entry in meta_storage.readdir(path):
            yield fuse.Direntry(entry)

    def unlink(self, path):
        logging.debug("PyDedupFS.unlink(%s)", path)
        meta_storage.unlink(path)

    def rmdir(self, path):
        logging.debug("PyDedupFS.rmdir(%s)", path)

    def symlink(self, path, path1):
        logging.debug("PyDedupFS.symlink(%s, %s)", path, path1)

    def rename(self, path, path1):
        """
        return 0 if all went ok
        return EACCESS if privileges permit renaming
        retunr ENOENT if file doesn not exist
        return EROFS if filesystem is read only
        if target name exists, delete file it
        """
        logging.debug("PyDedupFS.rename(%s, %s)", path, path1)
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
        logging.debug("PyDedupFS.link(%s)", path)

    def chmod(self, path, mode):
        """
        0 if success
        errno.EIO if something went wrong
        """
        logging.debug("PyDedupFS.chmod(%s, %s)", path, mode)
        meta_storage.chmod(path, mode)
        return(0)

    def chown(self, path, user, group):
        """
        0 is success
        errno.EIO if something went wrong
        """
        logging.debug("PyDedupFS.chown(%s, %s, %s)", path, user, group)
        meta_storage.chown(path, user, group)
        return(0)

    def truncate(self, path, length):
        logging.debug("PyDedupFS.truncate(%s, %s)", path, length)

    def mknod(self, path, mode, dev):
        logging.debug("PyDedupFS.mknod(%s, %s, %s)", path, mode, dev) 

    def mkdir(self, path, mode):
        logging.debug("PyDedupFS.mkdir(%s, %s)", path, mode)
        meta_storage.mkdir(path, mode)
        return(0)

    def utime(self, path, times):
        logging.debug("PyDedupFS.utime(%s, %s)", path, times)
        atime, mtime = times
        meta_storage.utime(path, atime, mtime)
        return(0)

    def access(self, path, mode):
        """
        0 if file is accessible
        -errno.EACCES if file is not accessible
        -errno.ENOENT if file does not exists or other error
        """
        logging.debug("PyDedupFS.access(%s, %s)", path, mode)

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
        logging.debug("PyDedupFS.statfs()")
        statvfs = fuse.StatVfs()
        statvfs.f_bsize = 1024 * 128
        statvfs.f_frsize = 1024 * 128

    def fsinit(self):
        logging.debug("PyDedupFS.fsinit()")


    class PyDedupFile(object):
        """represents a deduplicted file"""

        def __init__(self, path, flags, *mode):
            logging.debug("PyDedupFile.__init__(%s, %s, %s)", path, flags, mode)
            self.path = path
            # TODO : are there other parameters to set
            self.direct_io = False
            self.keep_cache = False
            # dirty flag
            self.isdirty = False
            # Buffer Converter
            st = meta_storage.exists(self.path)
            logging.info("exists = %s" % st)
            if st is False:
                logging.info("creating new file")
                meta_storage.touch(self.path, mode)
            else:
                logging.info("working on existing file")

        def read(self, length, offset):
            """
            return data on success
            return errno.EIO is something went wrong
            """
            logging.debug("PyDedupFile.read(%s, %s)", length, offset)
            dd_buf = meta_storage.read(self.path, length, offset)
            logging.debug("buffer dedup store len(buf):%d", len(dd_buf))
            return(dd_buf)

        def write(self, buf, offset):
            """
            return len of written data
            return errno.EACCES is File is not writeable
            return errno.EIO if something went wrong
            """
            # DEBUG: logging.debug("PyDedupFile.write(<buf>, %s)", offset)
            self.isdirty = True
            len_buf = meta_storage.write(self.path, buf)
            return len_buf

        def release(self, flags):
            """
            return 0 if all is OK
            return errno.EIO if something went wrong
            close file an write all remaining dirty buffers
            """
            logging.debug("PyDedupFile.release(%s)", flags)

        def fsync(self, isfsyncfile):
            """TODO dont know"""
            logging.debug("PyDedupFile.fsync(%s)", isfsyncfile)

        def flush(self):
            """end file write"""
            logging.debug("PyDedupFile.flush()")
            if self.isdirty is True:
                meta_storage.release(self.path)
            else:
                self.isdirty = False

        def fgetattr(self):
            """return st struct for file"""
            logging.debug("PyDedupFile.fgetattr()")
            st = meta_storage.exists(self.path)
            return(st)

        def ftruncate(self, length):
            """TODO dont know"""
            logging.debug("PyDedupFile.ftruncate(%s)", length)

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
    server.root = "./metastore"
    server.multithreaded = False
    server.parser.add_option(mountopt="root", metavar="PATH", default='/',
                             help="mirror filesystem from under PATH [default: %default]")
    server.parse(values=server, errex=1)
    server.main()


if __name__ == '__main__':
    # add option -d for foreground display an fuse for mountpoint
    sys.argv = [sys.argv[0], "-f", "fuse"]
    # global MetaStorage Object
    meta_storage = MetaStorage()
    cProfile.run("main()")
