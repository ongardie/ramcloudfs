#!/usr/bin/env python

# Copyright (c) 2010 Stanford University
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

"""Unit tests for L{ramcloudfs}."""

import unittest
import stat

import ramcloudfs


class TestInode(unittest.TestCase):
    """Test L{ramcloudfs.Inode}."""

    @staticmethod
    def assertSerializable(tc, inode):
        out = ramcloudfs.unserialize(ramcloudfs.serialize(inode))
        tc.assertEquals(out.oid, None)
        tc.assertEquals(out._st, inode._st)
        return out

    def test_init(self):
        inode = ramcloudfs.Inode()
        self.assertEquals(inode.oid, None)
        self.assertEquals(inode._st, {})

        inode = ramcloudfs.Inode(oid=78)
        self.assertEquals(inode.oid, 78)
        self.assertEquals(inode._st, {})

        st = {'rofl': 'copter'}
        inode = ramcloudfs.Inode(st=st)
        self.assertEquals(inode.oid, None)
        self.assertEquals(inode._st, st)

    def test_serializable(self):
        st = {'rofl': 'copter'}
        inode = ramcloudfs.Inode(oid=None, st=st)
        TestInode.assertSerializable(self, inode)

    def test_getattr(self):
        st = {'rofl': 'copter'}
        inode = ramcloudfs.Inode(oid=None, st=st)
        attr = inode.getattr()
        self.assertEquals(attr, st)
        self.assert_(attr is not st)

    def test_getattr_implicit_oid(self):
        inode = ramcloudfs.Inode(oid=78)
        attr = inode.getattr()
        self.assert_('st_ino' in attr)
        self.assertEquals(attr['st_ino'], 78)

    def test_setattr(self):
        inode = ramcloudfs.Inode(oid=78)
        st = {'st_mode': 3048, 'st_ino': 80}
        inode.setattr(st)
        self.assertEquals(inode.getattr(), {'st_ino': 78, 'st_mode': 3048})

    def test_from_blob(self):
        blob = ramcloudfs.serialize(ramcloudfs.Inode())
        inode = ramcloudfs.Inode.from_blob(78, blob)
        self.assertEquals(inode.oid, 78)


class TestDirectory(unittest.TestCase):
    """Test L{ramcloudfs.Directory}."""

    def assertSerializable(self, inode):
        out = TestInode.assertSerializable(self, inode)
        self.assertEquals(out._entries, inode._entries)

    def test_serializable(self):
        inode = ramcloudfs.Directory()
        self.assertSerializable(inode)
        inode._entries = {'foo': 'bar'}
        self.assertSerializable(inode)

    def test_getattr(self):
        inode = ramcloudfs.Directory()
        st = inode.getattr()
        self.assert_(stat.S_ISDIR(st['st_mode']))
        self.assertEquals(st['st_nlink'], 1)

        inode.add_entry('rofl', 0, False)
        st = inode.getattr()
        self.assertEquals(st['st_nlink'], 1)

        inode.add_entry('copter', 0, True)
        st = inode.getattr()
        self.assertEquals(st['st_nlink'], 2)

    def test_add_entry(self):
        inode = ramcloudfs.Directory()
        entries = {}
        self.assertEquals(inode._entries, entries)
        inode.add_entry('rofl', 0, False)
        entries['rofl'] = {'oid': 0, 'is_dir': False}
        self.assertEquals(inode._entries, entries)
        self.assertRaises(Exception, inode.add_entry, 'rofl', 0, False)

    def test_del_entry(self):
        inode = ramcloudfs.Directory()
        inode.add_entry('rofl', 0, False)
        self.assertRaises(Exception, inode.del_entry, 'foo', 0)
        self.assertRaises(Exception, inode.del_entry, 'rofl', 10)
        inode.del_entry('rofl', 0)
        self.assertEquals(inode._entries, {})

    def test_lookup(self):
        inode = ramcloudfs.Directory(oid=832)
        self.assertEquals(inode.lookup('.'), {'oid': 832, 'is_dir': True})
        self.assertRaises(Exception, inode.lookup, 'rofl')
        inode.add_entry('rofl', 12, True)
        self.assertEquals(inode.lookup('rofl'), {'oid': 12, 'is_dir': True})

    def test_readdir(self):
        inode = ramcloudfs.Directory(oid=832)
        inode.add_entry('rofl', 12, True)
        inode.add_entry('copter', 80, False)
        entries = dict(inode.readdir())
        self.assertEquals(set(entries.keys()), set(['.', 'rofl', 'copter']))
        self.assertEquals(entries['.']['st_ino'], 832)
        self.assertEquals(entries['.']['st_mode'], stat.S_IFDIR)
        self.assertEquals(entries['rofl']['st_ino'], 12)
        self.assertEquals(entries['rofl']['st_mode'], stat.S_IFDIR)
        self.assertEquals(entries['copter']['st_ino'], 80)
        self.assertEquals(entries['copter']['st_mode'], 0)


class TestFile(unittest.TestCase):
    """Test L{ramcloudfs.File}."""

    def test_read_write(self):
        st = {'st_nlink': 1}
        inode = ramcloudfs.File(oid=832, st=st)
        self.assertEquals(inode.read(0, 4096), '')
        inode.write(0, 'hi')
        self.assertEquals(inode.read(0, 4096), 'hi')
        inode.write(1, 'rofl')
        self.assertEquals(inode.read(0, 4096), 'hrofl')
        inode.write(10, 'copter')
        self.assertEquals(inode.read(0, 4096), 'hrofl\0\0\0\0\0copter')
        self.assertEquals(inode.read(8, 4), '\0\0co')

    def test_setattr(self):
        st = {'st_nlink': 1}
        inode = ramcloudfs.File(oid=832, st=st)

        # extend
        inode.setattr({'st_size': 30})
        self.assertEquals(inode.getattr()['st_size'], 30)
        self.assertEquals(inode.read(0, 4096), '\0' * 30)

        inode.write(0, '012345678901234567890123456789')

        # truncate
        inode.setattr({'st_size': 10})
        self.assertEquals(inode.getattr()['st_size'], 10)
        self.assertEquals(inode.read(0, 4096), '0123456789')


class TestOperations(unittest.TestCase):
    """Test L{ramcloudfs.Operations}.

    @todo: Implement"""

    pass


if __name__ == '__main__':
    unittest.main()
