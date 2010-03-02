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

Another special object ID in the inodes table, L{OIDRES_OID}, is used for the
L{OIDRes}. It reserves object IDs for normal inodes. Object IDs are never
reused, so for instance you can never follow a link to a directory and find a
regular file there.

"""

import sys
import stat
import errno
import cPickle as pickle
import itertools
import logging
import llfuse

import ramcloud
import txramcloud
from oidres import OIDRes
from retries import ExponentialBackoff as RetryStrategy

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

OIDRES_OID = 0
"""The object ID used by L{OIDRes} to reserve inode numbers.

@type: C{int}
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

    def getattr(self):
        """Return stat data.

        Ensures that C{S_IFDIR} is set in C{st_mode}.

        If st_nlink is not set, this method calculates it based on the number
        of entries that are directories.
        """

        st = Inode.getattr(self)

        if 'st_mode' not in st:
            st['st_mode'] = stat.S_IFDIR
        elif not stat.S_ISDIR(st['st_mode']):
            st['st_mode'] |= stat.S_IFDIR

        if 'st_nlink' not in st:
            st['st_nlink'] = 1
            for (name, entry) in self._entries.items():
                if entry['is_dir']:
                    st['st_nlink'] += 1

        return st

    def add_entry(self, name, oid, is_dir):
        """Add a new directory entry.

        @param name: The name of the link.
        @type  name: C{bytes}

        @param oid: The object ID linked.
        @type  oid: C{int}

        @param is_dir: Whether the link points to a directory.
        @type  is_dir: C{bool}

        @raises Exception: C{name} belongs to an existing directory entry.

        @todo: Raise a FUSEError instead.
        """

        assert name is not '.'
        assert name not in self._entries
        self._entries[name] = {'oid': oid, 'is_dir': is_dir}

    def del_entry(self, name, oid):
        """Delete a directory entry.

        @param name: The name of the link.
        @type  name: C{bytes}

        @param oid: The object ID linked.
        @type  oid: C{int}

        @raises Exception: C{name} does not belong to an existing directory
        entry.

        @raises Exception: The directory entry does not link to C{oid}.
        """

        assert name in self._entries
        assert self._entries[name]['oid'] == oid
        del self._entries[name]

    def lookup(self, name):
        """
        @param name: The name of the directory entry (link).
        @type  name: C{bytes}

        @return: A dict with the keys C{oid} (object ID as C{int}) and
                 C{is_dir} (whether the inode is a directory as C{bool}).
        @rtype: C{dict}

        @raise Exception: There is no directory entry under C{name}.
        """

        if name == '.':
            return {'oid': self.oid, 'is_dir': True}
        else:
            return dict(self._entries[name])

    def readdir(self):
        """Iterate over directory entries.

        @return: Iterator over tuples from the name of the link (C{bytes}) to
        a partial stat dict.
        @rtype: C{iter}
        """

        yield ('.', self.getattr())
        for (name, entry) in self._entries.items():
            st = {}
            st['st_ino'] = entry['oid']
            if entry['is_dir']:
                st['st_mode'] = stat.S_IFDIR
            else:
                st['st_mode'] = 0
            yield (name, st)

    def __len__(self):
        """
        @return: The number of directory entries, including '.' and '..'.
        @rtype: C{int}
        """

        return len(self._entries) + 1


class File(Inode):
    """An L{Inode} that is a regular file.

    @todo: implement
    """

    pass


class Operations(llfuse.Operations):

    def _get_version(self, table, oid):
        """Return the current version number for an object.

        This does not ship the object's data over the network, so it's more
        efficient than doing a normal read.

        @param table: The ID for the table which contains the object.
        @type  table: C{int}

        @param oid: The object ID whose version to retrieve.
        @type  oid: C{int}

        @return: The version number, or C{None} if the object does not exist.
        @rtype:  C{int} or C{None}

        @todo: Maybe this belongs in the L{ramcloud} module.
        """

        rr = ramcloud.RejectRules(object_doesnt_exist=True,
                                  version_eq_given=True,
                                  version_gt_given=True,
                                  given_version=0)
        try:
            self.rc.read_rr(table, oid, rr)
        except ramcloud.NoObjectError:
            return None
        except ramcloud.VersionError as e:
            return e.got_version
        else:
            assert False

    def _next_oid(self):
        """Reserve an object ID for an inode."""

        oid = None
        while oid in [None, OIDRES_OID, ROOT_OID]:
            oid = self.oidres.next()
        return oid

    def _make_root_dir(self):
        """Ensure that a root directory exists in the inodes table.

        If the root directory does not exist, this method will create it.
        """

        st = {}
        st['st_mode'] = 0755 | stat.S_IFDIR
        inode = Directory(oid=ROOT_OID, st=st)
        inode.add_entry('..', ROOT_OID, True)
        blob = serialize(inode)
        try:
            self.rc.create(self.inodes_table, ROOT_OID, blob)
        except ramcloud.ObjectExistsError:
            pass

    def __init__(self):
        self.rc = None
        self.inodes_table = None
        self.oidres = None
        self.open_directories = {}
        self.file_handles = itertools.count(10)

    def init(self):
        # TODO: using table 7 doesn't make much sense
        self.rc = txramcloud.TxRAMCloud(7)
        self.rc.connect()

        try:
            self.rc.create_table("inodes")
        except ramcloud.RCException:
            pass
        self.inodes_table = self.rc.open_table("inodes")

        self._make_root_dir()
        self.oidres = OIDRes(self.rc, self.inodes_table, OIDRES_OID)

    def getattr(self, oid):
        try:
            blob, version = self.rc.read(self.inodes_table, oid)
        except ramcloud.NoObjectError:
            raise llfuse.FUSEError(errno.ENOENT)
        inode = Inode.from_blob(oid, blob)
        attr = inode.getattr()
        attr['attr_timeout'] = 1
        return attr

    def lookup(self, parent_oid, name):
        try:
            blob, version = self.rc.read(self.inodes_table, parent_oid)
        except ramcloud.NoObjectError:
            raise llfuse.FUSEError(errno.ENOENT)
        inode = Inode.from_blob(parent_oid, blob)
        try:
            oid = inode.lookup(name)['oid']
        except Exception:
            raise llfuse.FUSEError(errno.ENOENT)
        attr = self.getattr(oid)
        attr['generation'] = 1
        attr['attr_timeout'] = 1
        attr['entry_timeout'] = 1
        return attr

    def mkdir(self, parent_oid, name, mode, ctx):
        oid = self._next_oid()
        st = {}
        st['st_mode'] = mode | stat.S_IFDIR
        inode = Directory(oid, st)
        inode.add_entry('..', parent_oid, True)

        for retry in RetryStrategy():
            try:
                blob, parent_version = self.rc.read(self.inodes_table,
                                                    parent_oid)
            except ramcloud.NoObjectError:
                raise llfuse.FUSEError(errno.ENOENT)
            parent_inode = Inode.from_blob(parent_oid, blob)
            parent_inode.add_entry(name, oid, True)

            mt = txramcloud.MiniTransaction()

            rr = ramcloud.RejectRules.exactly(parent_version)
            op = txramcloud.MTWrite(serialize(parent_inode), rr)
            mt[(self.inodes_table, parent_oid)] = op

            rr = ramcloud.RejectRules(object_exists=True)
            op = txramcloud.MTWrite(serialize(inode), rr)
            mt[(self.inodes_table, oid)] = op

            try:
                self.rc.mt_commit(mt)
            except self.rc.TransactionRejected as e:
                msg = "Reserved object ID already in use"
                assert (self.inodes_table, oid) not in e.reasons, msg
                retry.later()
            except self.rc.TransactionExpired:
                retry.later()

        attr = inode.getattr()
        attr['generation'] = 1
        attr['attr_timeout'] = 1
        attr['entry_timeout'] = 1
        return attr

    def opendir(self, oid):
        # Just check if the directory exists for now. Get the directory entries
        # later in readdir to work around the case of concurrent modification
        # between opendir and readdir, as in ( mkdir -p a/b && rm -r a ).
        rr = ramcloud.RejectRules(object_doesnt_exist=True, object_exists=True)
        try:
            self.rc.read_rr(self.inodes_table, oid, rr)
        except ramcloud.NoObjectError:
            raise llfuse.FUSEError(errno.ENOENT)
        except ramcloud.ObjectExistsError:
            dir_handle = self.file_handles.next()
            self.open_directories[dir_handle] = oid
            return dir_handle
        assert False

    def readdir(self, dir_handle, offset):
        if offset == 0:
            try:
                oid = int(self.open_directories[dir_handle])
            except KeyError:
                raise llfuse.FUSEError(errno.EBADF)
            try:
                blob, version = self.rc.read(self.inodes_table, oid)
            except ramcloud.NoObjectError:
                return
            inode = Inode.from_blob(oid, blob)
            entries = list(inode.readdir())
            self.open_directories[dir_handle] = entries
        else:
            try:
                entries = self.open_directories[dir_handle]
            except KeyError:
                raise llfuse.FUSEError(errno.EBADF)
        for entry in entries[offset:]:
            yield entry

    def releasedir(self, dir_handle):
        try:
            del self.open_directories[dir_handle]
        except KeyError:
            raise llfuse.FUSEError(errno.EBADF)

    def rename(self, old_parent_oid, old_name, new_parent_oid, new_name):

        assert old_name not in ['.', '..']
        assert new_name not in ['.', '..']

        if old_parent_oid == new_parent_oid and old_name == new_name:
            return

        for retry in RetryStrategy():

            # Read old_parent inode.
            try:
                blob, old_parent_version = self.rc.read(self.inodes_table,
                                                        old_parent_oid)
            except ramcloud.NoObjectError:
                raise llfuse.FUSEError(errno.ENOENT)
            old_parent_inode = Inode.from_blob(old_parent_oid, blob)
            # mt should verify old_parent_inode is exactly old_parent_version.

            # Check whether old_name exists.
            try:
                old_entry = old_parent_inode.lookup(old_name)
            except KeyError:
                raise llfuse.FUSEError(errno.ENOENT)

            # Read new_parent inode.
            if old_parent_oid == new_parent_oid:
                new_parent_inode = old_parent_inode
                new_parent_version = old_parent_version
            else:
                try:
                    blob, new_parent_version = self.rc.read(self.inodes_table,
                                                            new_parent_oid)
                except ramcloud.NoObjectError:
                    raise llfuse.FUSEError(errno.ENOENT)
                new_parent_inode = Inode.from_blob(new_parent_oid, blob)
                # mt should verify new_parent_inode is exactly
                # new_parent_version.

            # Check whether new_name exists.
            try:
                new_entry = new_parent_inode.lookup(new_name)
            except KeyError:
                new_entry = None

            # Change directory entries in local copy of parent inodes.
            old_parent_inode.del_entry(old_name, old_entry['oid'])
            if new_entry is not None:
                new_parent_inode.del_entry(new_name, new_entry['oid'])
            new_parent_inode.add_entry(new_name, old_entry['oid'],
                                       old_entry['is_dir'])

            mt = txramcloud.MiniTransaction()

            # Add write of parent inodes to the transaction.
            rr = ramcloud.RejectRules.exactly(old_parent_version)
            op = txramcloud.MTWrite(serialize(old_parent_inode), rr)
            mt[(self.inodes_table, old_parent_oid)] = op

            if old_parent_inode is not new_parent_inode:
                rr = ramcloud.RejectRules.exactly(new_parent_version)
                op = txramcloud.MTWrite(serialize(new_parent_inode), rr)
                mt[(self.inodes_table, new_parent_oid)] = op

            if old_entry['is_dir']:
                # Need to worry about the .. entry.

                # Read old_name's inode.
                try:
                    blob, old_version = self.rc.read(self.inodes_table,
                                                     old_entry['oid'])
                except ramcloud.NoObjectError:
                    # This is inconsistent: old_parent_inode linked to
                    # old_entry['oid'], yet it does not exist.
                    retry.later()
                    continue
                old_inode = Inode.from_blob(old_entry['oid'], blob)

                # old_inode was listed as a directory in old_parent_inode, and
                # object IDs are never reused.
                assert isinstance(old_inode, Directory)

                if old_inode.lookup('..')['oid'] != old_parent_inode.oid:
                    # The transaction is definitely going to abort since
                    # old_parent must have changed. Might as well stop now.
                    retry.later()
                    continue

                rr = ramcloud.RejectRules.exactly(old_version)
                if (old_parent_inode is not new_parent_inode):
                    # Change .. directory entry in old_inode to point to
                    # new_parent_inode
                    old_inode.del_entry('..', old_parent_inode.oid)
                    old_inode.add_entry('..', new_parent_inode.oid, True)
                    op = txramcloud.MTWrite(serialize(old_inode), rr)
                else:
                    # Make sure it doesn't change
                    op = txramcloud.MTOperation(rr)
                mt[(self.inodes_table, old_entry['oid'])] = op

            if old_entry['is_dir'] and (new_entry is not None and
                                        new_entry['is_dir']):
                # Atomic replace of new_name, which must be an empty directory.

                # Read new_name's inode.
                try:
                    blob, new_version = self.rc.read(self.inodes_table,
                                                     new_entry['oid'])
                except ramcloud.NoObjectError:
                    # This is inconsistent: new_parent_inode linked to
                    # new_entry['oid'], yet it does not exist.
                    retry.later()
                    continue
                new_inode = Inode.from_blob(new_entry['oid'], blob)

                # new_inode was listed as a directory in new_parent_inode, and
                # object IDs are never reused.
                assert isinstance(new_inode, Directory)

                if new_inode.lookup('..')['oid'] != new_parent_inode.oid:
                    # The transaction is definitely going to abort since
                    # new_parent must have changed. Might as well stop now.
                    retry.later()
                    continue

                # Make sure it's empty.
                if len(new_inode) > 2:
                    # If new_parent still has new_parent_version, then there
                    # existed a point in time during which new_parent_inode
                    # pointed to new_inode and new_inode wasn't empty.
                    if (self._get_version(self.inodes_table,
                                          new_parent_inode.oid) ==
                        new_parent_version):
                        raise llfuse.FUSEError(errno.ENOTEMPTY)
                    else:
                        retry.later()
                        continue

                # Add delete of new_name's inode to the transaction.
                rr = ramcloud.RejectRules.exactly(new_version)
                op = txramcloud.MTDelete(rr)
                mt[(self.inodes_table, new_entry['oid'])] = op

            # Handling files as well as directories will open up more
            # cases later.

            # TODO: check for EINVAL

            # Execute and commit transaction.
            try:
                self.rc.mt_commit(mt)
            except self.rc.TransactionRejected, self.rc.TransactionExpired:
                retry.later()

    def rmdir(self, parent_oid, name):
        if parent_oid == ROOT_OID:
            assert name not in ['.', '..']

        for retry in RetryStrategy():

            # Read parent_inode.
            try:
                blob, parent_version = self.rc.read(self.inodes_table,
                                                    parent_oid)
            except ramcloud.NoObjectError:
                raise llfuse.FUSEError(errno.ENOENT)
            parent_inode = Inode.from_blob(parent_oid, blob)

            # Lookup name in parent_inode.
            try:
                entry = parent_inode.lookup(name)
            except KeyError:
                raise llfuse.FUSEError(errno.ENOENT)
            if not entry['is_dir']:
                raise llfuse.FUSEError(errno.ENOTDIR)
            oid = entry['oid']

            # Read inode to be deleted.
            try:
                blob, version = self.rc.read(self.inodes_table, oid)
            except ramcloud.NoObjectError:
                # This is inconsistent: parent_inode linked to oid, yet oid
                # does not exist.
                retry.later()
                continue
            inode = Inode.from_blob(oid, blob)

            # inode was listed as a directory in parent_inode, and object IDs
            # are never reused.
            assert isinstance(inode, Directory)

            if inode.lookup('..')['oid'] != parent_oid:
                # The transaction is definitely going to abort since
                # parent_inode must have changed. Might as well stop now.
                retry.later()
                continue

            # Make sure inode is empty.
            if len(inode) > 2:
                # If parent_inode still has parent_version, then, at some point
                # during this operation, parent_inode pointed to inode and
                # inode wasn't empty.
                if (parent_version == self._get_version(self.inodes_table,
                                                        parent_inode.oid)):
                    raise llfuse.FUSEError(errno.ENOTEMPTY)
                else:
                    retry.later()
                    continue

            mt = txramcloud.MiniTransaction()

            # Delete name from local copy of parent_inode, and add it to the
            # transaction.
            parent_inode.del_entry(name, oid)
            rr = ramcloud.RejectRules.exactly(parent_version)
            op = txramcloud.MTWrite(serialize(parent_inode), rr)
            mt[(self.inodes_table, parent_oid)] = op

            # Add deletion of inode to the transaction.
            rr = ramcloud.RejectRules.exactly(version)
            op = txramcloud.MTDelete(rr)
            mt[(self.inodes_table, oid)] = op

            # Execute and commit transaction.
            try:
                self.rc.mt_commit(mt)
            except self.rc.TransactionRejected, self.rc.TransactionExpired:
                # TODO: don't need to invalidate both inodes
                retry.later()


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
