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

import common_ancestor

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

    Beyond the functionality in L{Inode}, this class adds a binary blob.
    """

    def __init__(self, oid=None, st=None):
        """
        @param oid: The object ID or inode number.
        @type  oid: C{int} or C{None}

        @param st: The stat data.
        @type  st: C{dict} or C{None}
        """

        Inode.__init__(self, oid, st)
        self._data = ''

    def __getstate__(self):
        return {'Inode': Inode.__getstate__(self),
                '_data': self._data}

    def __setstate__(self, state):
        Inode.__setstate__(self, state['Inode'])
        self._data = state['_data']

    def getattr(self):
        """Return stat data."""

        st = Inode.getattr(self)

        if 'st_mode' not in st:
            st['st_mode'] = 0

        if 'st_nlink' not in st:
            st['st_nlink'] = 1

        if 'st_size' not in st:
            st['st_size'] = len(self._data)

        return st

    def read(self, offset, length):
        return self._data[offset:(offset + length)]

    def write(self, offset, data):
        datav = []
        datav.append(self._data[:offset])
        if offset > len(self._data):
            datav.append('\0' * (offset - len(self._data)))
        datav.append(data)
        datav.append(self._data[(offset + len(data)):])
        self._data = ''.join(datav)


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
        self.open_files = {}
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

    def _mkinode(self, parent_oid, name, inode):
        for retry in RetryStrategy():
            try:
                blob, parent_version = self.rc.read(self.inodes_table,
                                                    parent_oid)
            except ramcloud.NoObjectError:
                raise llfuse.FUSEError(errno.ENOENT)
            parent_inode = Inode.from_blob(parent_oid, blob)
            # TODO: raise FUSEError if name exists
            parent_inode.add_entry(name, inode.oid,
                                   is_dir=isinstance(inode, Directory))

            mt = txramcloud.MiniTransaction()

            rr = ramcloud.RejectRules.exactly(parent_version)
            op = txramcloud.MTWrite(serialize(parent_inode), rr)
            mt[(self.inodes_table, parent_oid)] = op

            rr = ramcloud.RejectRules(object_exists=True)
            op = txramcloud.MTWrite(serialize(inode), rr)
            mt[(self.inodes_table, inode.oid)] = op

            try:
                self.rc.mt_commit(mt)
            except self.rc.TransactionRejected as e:
                msg = "Reserved object ID already in use"
                assert (self.inodes_table, inode.oid) not in e.reasons, msg
                retry.later()
            except self.rc.TransactionExpired:
                retry.later()

    def mkdir(self, parent_oid, name, mode, ctx):
        oid = self._next_oid()
        st = {}
        st['st_mode'] = mode | stat.S_IFDIR
        inode = Directory(oid, st)
        inode.add_entry('..', parent_oid, True)

        self._mkinode(parent_oid, name, inode)

        attr = inode.getattr()
        attr['generation'] = 1
        attr['attr_timeout'] = 1
        attr['entry_timeout'] = 1
        return attr

    def mknod(self, parent_oid, name, mode, rdev, ctx):

        assert not stat.S_ISDIR(mode)
        if rdev:
            return llfuse.FUSEError(errno.ENOSYS)

        oid = self._next_oid()
        st = {}
        st['st_mode'] = mode
        inode = File(oid, st)

        self._mkinode(parent_oid, name, inode)

        attr = inode.getattr()
        attr['generation'] = 1
        attr['attr_timeout'] = 1
        attr['entry_timeout'] = 1
        return attr

    def open(self, oid, flags):
        # Just check if the file exists for now, since we just want NFS open
        # file semantics.
        rr = ramcloud.RejectRules(object_doesnt_exist=True, object_exists=True)
        try:
            self.rc.read_rr(self.inodes_table, oid, rr)
        except ramcloud.NoObjectError:
            raise llfuse.FUSEError(errno.ENOENT)
        except ramcloud.ObjectExistsError:
            file_handle = self.file_handles.next()
            self.open_files[file_handle] = oid
            return file_handle
        assert False

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

    def read(self, file_handle, offset, length):
        try:
            oid = int(self.open_files[file_handle])
        except KeyError:
            raise llfuse.FUSEError(errno.EBADF)
        try:
            blob, version = self.rc.read(self.inodes_table, oid)
        except ramcloud.NoObjectError:
            # TODO: should this be silent or throw another type of error?
            raise llfuse.FUSEError(errno.EIO)
        inode = Inode.from_blob(oid, blob)
        return inode.read(offset, length)

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

    def release(self, file_handle):
        del self.open_files[file_handle]

    def releasedir(self, dir_handle):
        try:
            del self.open_directories[dir_handle]
        except KeyError:
            raise llfuse.FUSEError(errno.EBADF)

    def _check_rename_safety(self, old_oid, old_parent_inode,
                             new_parent_inode):
        """
        To show the old link is not an ancestor of the new link, it's
        sufficient to show either of the following:
         - The old link is not in the path from the new link to the root. I'll
         refer to this as the I{path from new} or I{PFN} approach.
         - The old link and the new link have a common ancestor that is above
         the old link. I'll refer to this as the I{lowest common ancestor} or
         I{LCA} approach.

        Transaction Read Set Sizes
        ==========================

        If the level of the root directory is 0, PFN results in a read set of
        size M{(level of the new parent) - 1}. For example, moving to
        C{/a/b/c/d/e}, where C{d} is at level 4, would have a read set of size
        3, C{{a, b, c}}. Note that C{d} is already in the write-set and C{/} is
        not necessary to include in the read set because C{/a/..} points to it.

        Assuming the old parent and the new parent are at the same depth in the
        tree, if the common ancestor is C{n} levels towards the root from the
        parents, LCA results in a read set of size M{2 (n - 1)}. For example,
        moving from C{/a/w/x/y/z} to C{/a/b/c/d/e}, where the common ancestor
        (C{/a}) is 3 levels towards the root from the parents, results in a
        read set of size 4, C{{w, x, b, c}}. Note that C{y} and C{z} are
        already in the write-set and C{/a} is not necessary to include in the
        read set because C{/a/w/..} and C{/a/b/..} point to it.

        Without any numbers on the level of the new parent and the distance to
        a common ancestor, there's no obvious winner here.

        On my system, from C{/} and including several large code repositories
        in my home directory::
            Filename depth cumulative distribution:
            00: (0.00)
            01: (0.00)
            02: (0.12)
            03: X (1.25)
            04: XX (3.49)
            05: XXXXXX (12.11)
            06: XXXXXXXXXXX (21.40)
            07: XXXXXXXXXXXXXXXXXXXXXX (44.93)
            08: XXXXXXXXXXXXXXXXXXXXXXXXXXXX (55.03)
            09: XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX (67.65)
            10: XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX (78.24)
            11: XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX (85.50)
            12: XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX (90.50)
            13: XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX (93.20)
            14: XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX (95.28)
            15: XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX (96.71)
            16: XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX (98.12)
            17: XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX (99.05)
            18: XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX (99.60)
            19: XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX (99.83)
            20: XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX (99.92)
            21: XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX (99.97)
            22: XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX (99.98)
            23: XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX (100.00)
            24: XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX (100.00)
            where each X stands for 2%, rounded, over 787,593 files.

        About 90% of paths are at level 12 or less from the root, and about 55%
        of paths are at level 8 or less from the root. Assuming renames are
        random across all my files, the expected read set is then size 5 or 6
        with PFN. In LCA, moving between links that share a common
        great-great-great-grandparent would use about the same size read set.

        Current assumptions
        ===================
         - Adding objects to the read set of a transaction is extremely
         expensive and mutually excludes other transactions.
         - Neither of the two paths to the root are cached.
         - The tree will not be very deep.
         - Renames will share a common ancestor "near" the leaves, for varying
         definitions of near.

        For now, I'm going with LCA because:
         - It will reduce contention higher in the tree, so it will likely
         reduce contention overall.
         - The cost of a rename in PFN is based on the depth of the link,
         while the cost of a rename in LCA is based on the distance between
         the links. As a user, the cost of PFN may be surprising but the cost
         of LFA is close to that of moving an object in the real world.
         - It's an algorithm! Perhaps RAMCloud's first!

        Future assumptions
        ==================
         - Adding objects to the read set of a transaction will be cheaper
         (though still expensive). While the read set will not mutually exclude
         other transactions that read those objects, it will potentially starve
         other transactions that want to write those objects.
         - Both of the two paths to the root will be cached.
         - The tree may be deeper (as people start to trust and use the
         filesystem).
         - Renames will still share a common ancestor "near" the leaves, for
         varying definitions of near.

        If the two paths really are cached, the best thing would be to run both
        algorithms and select the one with the smaller read set. Then, if it
        turns out that one of them wins a large majority of the time, or if it
        turns out that there's no significant difference a large majority of
        the time, we can consider removing one of them.

        @return: A list of predicates on the safety of the rename. Each element
        in the list is a tuple of object ID and version. If each of the object
        IDs have the given version at some instant in time, it is safe to
        execute the rename operation. The old link, old parent, and new parent
        are not included in this list.
        @rtype: C{list}

        """

        class DirectoryNode(common_ancestor.Node):

            class ConcurrentModification(Exception):
                pass

            def __init__(dnself, oid, version=None, parent=None):
                dnself.oid = oid
                dnself.version = version
                dnself.parent = parent

            def __repr__(dnself):
                return ('DirectoryNode(%d, version=%s, parent=%s)' %
                        (dnself.oid, dnself.version, dnself.parent))

            def __eq__(dnself, other):
                return dnself.oid == other.oid

            def __neq__(dnself, other):
                return dnself.oid != other.oid

            def get_parent(dnself):
                if dnself.parent is None:
                    try:
                        blob, version = self.rc.read(self.inodes_table,
                                                     dnself.oid)
                    except ramcloud.NoObjectError:
                        raise dnself.ConcurrentModification()
                    inode = Inode.from_blob(dnself.oid, blob)
                    assert isinstance(inode, Directory)
                    dnself.parent = DirectoryNode(inode.lookup('..')['oid'])
                    dnself.version = version
                return dnself.parent


        for retry in RetryStrategy():

            root = DirectoryNode(ROOT_OID)
            left_parent = DirectoryNode(old_parent_inode.oid)
            left = DirectoryNode(old_oid, parent=left_parent)
            right = DirectoryNode(new_parent_inode.oid)
            fca = common_ancestor.find_common_ancestor
            try:
                left_path, right_path = fca(root, left, right)
            except DirectoryNode.ConcurrentModification:
                retry.later()
                continue

            if left == right_path[-1]:
                # Although we have found a candidate upward path from
                # new_parent to old_link, we can't be sure that this path
                # existed at some instant in time. We need to verify this path
                # before we can throw EINVAL.
                for node in right_path[1:-1]:
                    assert node.version is not None
                    if (self._get_version(self.inodes_table, node.oid) !=
                        node.version):
                        retry.later()
                        break
                if retry.need_retry:
                    continue
                # The nodes between old_link and new_parent (exclusive, in
                # any order) still had the same version numbers during the
                # check, so there existed an instant in time during which they
                # all had those version numbers. We have implicitly checked
                # old_link with the directory entry .. in its child and
                # new_parent with the directory entry in its parent.
                raise llfuse.FUSEError(errno.EINVAL)

            # The common ancestor doesn't need to be in the read set since its
            # direct children have a .. link up to it. The old link, old
            # parent, and new parent don't need to be in read_set since they're
            # already in the transaction's read set. Since all of these are
            # intermediate nodes, they all have version numbers.
            read_set = []
            for node in set.union(set(left_path[2:-1]), set(right_path[1:-1])):
                assert node.version is not None
                read_set.append((node.oid, node.version))
            return read_set

    def rename(self, old_parent_oid, old_name, new_parent_oid, new_name):
        """
        Rename Safety
        =============
        Since directories must form a tree, we must guarantee that, during the
        instant of the rename:
         1. The new link is not an ancestor of the old link, and
         2. The old link is not an ancestor of the new link.

        Proving (1) is easy and follows from our other rules. If the new link
        is a directory, it must be empty. Therefore it's not an ancestor of
        anything.

        Unfortunately, proving (2) is not trivial. See L{_check_rename_safety}.
        """

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
            else:
                if old_entry['is_dir'] and not new_entry['is_dir']:
                    raise llfuse.FUSEError(errno.ENOTDIR)
                if not old_entry['is_dir'] and new_entry['is_dir']:
                    raise llfuse.FUSEError(errno.EISDIR)
                if old_entry['oid'] == new_entry['oid']:
                    # Although it makes no sense to me, a rename of hard links
                    # to the same inode does nothing and is successful.
                    return

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

            if new_entry is not None:
                # Atomic replace of new_name

                if new_entry['is_dir']:
                    # new_name must be an empty directory.

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

                    # new_inode was listed as a directory in new_parent_inode,
                    # and object IDs are never reused.
                    assert isinstance(new_inode, Directory)

                    if new_inode.lookup('..')['oid'] != new_parent_inode.oid:
                        # The transaction is definitely going to abort since
                        # new_parent must have changed. Might as well stop now.
                        retry.later()
                        continue

                    # Make sure it's empty.
                    if len(new_inode) > 2:
                        # If new_parent still has new_parent_version, then
                        # there existed a point in time during which
                        # new_parent_inode pointed to new_inode and new_inode
                        # wasn't empty.
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

                else:
                    # new_name is a file

                    # TODO: With hard links, this may need to update new_name
                    # instead. We'll have to read it first to know.
                    rr = ramcloud.RejectRules(object_doesnt_exist=True)
                    op = txramcloud.MTDelete(rr)
                    mt[(self.inodes_table, new_entry['oid'])] = op

            if old_entry['is_dir']:
                read_set = self._check_rename_safety(old_entry['oid'],
                                                     old_parent_inode,
                                                     new_parent_inode)
                for oid, version in read_set:
                    rr = ramcloud.RejectRules.exactly(version)
                    assert (self.inodes_table, oid) not in mt
                    mt[(self.inodes_table, oid)] = txramcloud.MTOperation(rr)

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

    def unlink(self, parent_oid, name):
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
            if entry['is_dir']:
                raise llfuse.FUSEError(errno.EISDIR)
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

            # inode was listed as a file in parent_inode, and object IDs
            # are never reused.
            assert isinstance(inode, File)

            mt = txramcloud.MiniTransaction()

            # Delete name from local copy of parent_inode, and add it to the
            # transaction.
            parent_inode.del_entry(name, oid)
            rr = ramcloud.RejectRules.exactly(parent_version)
            op = txramcloud.MTWrite(serialize(parent_inode), rr)
            mt[(self.inodes_table, parent_oid)] = op

            # Add deletion of inode to the transaction.
            # TODO: With hard links, this may need to update inode instead.
            rr = ramcloud.RejectRules.exactly(version)
            op = txramcloud.MTDelete(rr)
            mt[(self.inodes_table, oid)] = op

            # Execute and commit transaction.
            try:
                self.rc.mt_commit(mt)
            except self.rc.TransactionRejected, self.rc.TransactionExpired:
                # TODO: don't need to invalidate both inodes
                retry.later()

    def write(self, file_handle, offset, data):
        try:
            oid = int(self.open_files[file_handle])
        except KeyError:
            raise llfuse.FUSEError(errno.EBADF)

        for retry in RetryStrategy():
            try:
                blob, version = self.rc.read(self.inodes_table, oid)
            except ramcloud.NoObjectError:
                # TODO: should this be silent or throw another type of error?
                raise llfuse.FUSEError(errno.EIO)
            inode = Inode.from_blob(oid, blob)

            inode.write(offset, data)

            try:
                self.rc.update(self.inodes_table, oid, serialize(inode),
                               version)
            except ramcloud.NoObjectError:
                # TODO: should this be silent or throw another type of error?
                raise llfuse.FUSEError(errno.EIO)
            except ramcloud.VersionError:
                retry.later()

        return len(data)


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
