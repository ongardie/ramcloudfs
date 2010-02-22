#!/usr/bin/env python

# Copyright (c) 2009-2010 Stanford University
#
# Permission to use, copy, modify, and distribute this software for any
# purpose with or without fee is hereby granted, provided that the above
# copyright notice and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
# WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
# ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
# WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
# ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
# OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

"""
Implements a filesystem on top of a RAMCloud database.

Internal
========

I{inode} in this file refers to the data structure, not the inode number as in
the llfuse bindings. The I{oid}, or object ID, on the I{inodes} table is used
instead as the inode number.

The inodes table is used to store serialized L{Directory} or L{File} objects.

The root directory of the filesystem is stored in a well-known place in the
inodes table: L{ROOT_OID}. Clients make sure it exists when they start up and
assume it exists during normal operations.
"""

import sys
import stat
import errno
import cPickle as pickle
import logging
import llfuse

import ramcloud
from txramcloud import TxRAMCloud

PICKLE_PROTO = 2
"""The version of the Pickle protocol to use.

Version 2 of the protocol encodes binary data efficiently, which is useful for
a filesystem.

@type: C{int}
"""

FUSE_DEBUG = True
"""Whether to output llfuse's log to the console.
@type: C{bool}
"""

ROOT_OID = 1
"""The inode number for the root directory.

This is assumed by fuse.
@type: C{int}
"""


def serialize(data):
    """Pickle an object.

    @param data: The object to pickle.
    @type  data: object

    @return: The pickled data, the input to L{unserialize}.
    @rtype: C{bytes}
    """

    return pickle.dumps(data, protocol=PICKLE_PROTO)


def unserialize(serialized):
    """Unpickle an object.

    @param serialized: The output of L{serialize}.
    @type  serialized: C{bytes}

    @return: The unpickled object.
    @rtype: object
    """

    return pickle.loads(serialized)


class Inode(object):
    """The abstract base class for L{File} and L{Directory}.

    This class manages the object ID and stat data associated with the L{File}
    or L{Directory}. It is picklable.

    @ivar oid: The object ID.
    @type oid: C{int}
    """

    def __init__(self, oid=None, st=None):
        """
        @param oid: The object ID or inode number.
        @type  oid: C{int} or C{None}

        @param st: The stat data.
        @type  st: C{dict} or C{None}
        """

        self.oid = oid
        if st is None:
            self._st = {}
        else:
            self._st = st

    def __getstate__(self):
        return {'st': self._st}

    def __setstate__(self, state):
        self._st = state['st']
        self.oid = None

    def getattr(self):
        """
        @return: a copy of the stat data for the inode.
        @rtype: C{dict}
        """

        st = dict(self._st)
        if 'st_ino' not in st and self.oid is not None:
            st['st_ino'] = self.oid
        return st

    @classmethod
    def from_blob(cls, oid, blob):
        """Unserialize a blob from RAMCloud into an inode.

        @param oid: The object ID or inode number.
        @type  oid: C{int} or C{None}

        @return: The inode with its object ID set.
        @rtype: L{Inode}
        """

        inode = unserialize(blob)
        assert isinstance(inode, cls)
        inode.oid = oid
        return inode


class Directory(Inode):
    """An L{Inode} that is a directory.

    Beyond the functionality in L{Inode}, this class adds directory entries.
    """

    def __init__(self, oid=None, st=None):
        """
        @param oid: The object ID or inode number.
        @type  oid: C{int} or C{None}

        @param st: The stat data.
        @type  st: C{dict} or C{None}
        """

        Inode.__init__(self, oid, st)
        self._entries = {}

    def __getstate__(self):
        return {'Inode': Inode.__getstate__(self),
                '_entries': self._entries}

    def __setstate__(self, state):
        Inode.__setstate__(self, state['Inode'])
        self._entries = state['_entries']


class File(Inode):
    """An L{Inode} that is a regular file.

    @todo: implement
    """

    pass


class Operations(llfuse.Operations):

    def _make_root_dir(self):
        """Ensure that a root directory exists in the inodes table.

        If the root directory does not exist, this method will create it.
        """

        st = {}
        st['st_mode'] = 0755 | stat.S_IFDIR
        st['st_nlink'] = 2
        inode = Directory(oid=ROOT_OID, st=st)
        blob = serialize(inode)
        try:
            self.rc.create(self.inodes_table, ROOT_OID, blob)
        except ramcloud.ObjectExistsError:
            pass

    def __init__(self):
        self.rc = None
        self.inodes_table = None
        self.oidres = None

    def init(self):
        self.rc = TxRAMCloud(7) # TODO: using table 7 doesn't make much sense
        self.rc.connect()

        try:
            self.rc.create_table("inodes")
        except ramcloud.RCException:
            pass
        self.inodes_table = self.rc.open_table("inodes")

        self._make_root_dir()

    def getattr(self, oid):
        try:
            blob, version = self.rc.read(self.inodes_table, oid)
        except ramcloud.NoObjectError:
            raise llfuse.FUSEError(errno.ENOENT)
        inode = Inode.from_blob(oid, blob)
        attr = inode.getattr()
        attr['attr_timeout'] = 1
        return attr


if __name__ == '__main__':

    if FUSE_DEBUG:
        console = logging.StreamHandler()
        console.setLevel(logging.DEBUG)

        fuse_log = logging.getLogger("fuse")
        fuse_log.setLevel(logging.DEBUG)
        fuse_log.addHandler(console)

    mountpoint, args = sys.argv[1], sys.argv[2:]
    llfuse.init(Operations(), mountpoint, args)

    llfuse.main()
