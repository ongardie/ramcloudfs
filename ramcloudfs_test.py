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


class TestFile(unittest.TestCase):
    """Test L{ramcloudfs.File}."""

    pass


class TestOperations(unittest.TestCase):
    """Test L{ramcloudfs.Operations}.

    @todo: Implement"""

    pass


if __name__ == '__main__':
    unittest.main()
