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

"""Integration tests for L{ramcloudfs}.

Run these tests on a fresh RAMCloud server.

These are done from the perspective of FUSE (i.e., calling methods of
L{ramcloudfs.Operations}. They do not mock out anything (not even RAMCloud),
but ensure the filesystem works under the FUSE API. These are gray-box tests in
that they only access the module through the FUSE API but they are written with
the module's code in mind.
"""

import unittest
import stat

from llfuse import FUSEError

import ramcloudfs


class TestOperations(unittest.TestCase):
    """Test L{ramcloudfs.Operations}."""

    def no_inodes(self):
        """Test without any inodes.

        If the filesystem is working correctly, this should have no side
        effects on observable state.
        """

        self.assertRaises(FUSEError, self.ops.getattr, 999)
        self.assertRaises(FUSEError, self.ops.lookup, 999, 'foo')
        self.assertRaises(FUSEError, self.ops.lookup, 999, 'foo')
        self.assertRaises(FUSEError, self.ops.mkdir, 999, 'foo', 0, None)
        self.assertRaises(FUSEError, self.ops.opendir, 999)
        self.assertRaises(FUSEError, self.ops.readdir(999, 0).next)
        self.assertRaises(FUSEError, self.ops.readdir(999, 1).next)
        self.assertRaises(FUSEError, self.ops.releasedir, 999)
        self.assertRaises(FUSEError, self.ops.rmdir, 999, 'foo')

    def root_inode(self):
        """Test with just the root inode.

        Directory structure::
            /

        If the filesystem is working correctly, this should have no side
        effects on observable state.
        """

        # getattr /
        st = self.ops.getattr(self.oids['/'])
        self.assertEquals(st['st_ino'], self.oids['/'])
        self.assert_(stat.S_ISDIR(st['st_mode']))

        # lookup / .
        st = self.ops.lookup(self.oids['/'], '.')
        self.assertEquals(st['st_ino'], self.oids['/'])
        self.assert_(stat.S_ISDIR(st['st_mode']))

        # lookup / ..
        st = self.ops.lookup(self.oids['/'], '..')
        self.assertEquals(st['st_ino'], self.oids['/'])
        self.assert_(stat.S_ISDIR(st['st_mode']))

        # lookup non-existent
        self.assertRaises(FUSEError, self.ops.lookup, self.oids['/'], 'foo')

        # mkdir / . and / ..
        self.assertRaises(Exception, self.ops.mkdir, self.oids['/'], '.',
                                                     0, None)
        self.assertRaises(Exception, self.ops.mkdir, self.oids['/'], '..',
                                                     0, None)

        # opendir, readdir, releasedir /
        dh = self.ops.opendir(self.oids['/'])
        entries = dict(self.ops.readdir(dh, 0))
        self.ops.releasedir(dh)
        self.assertEquals(set(entries.keys()), set(['.', '..']))
        self.assertEquals(entries['.']['st_ino'], self.oids['/'])
        self.assert_(stat.S_ISDIR(entries['.']['st_mode']))
        self.assertEquals(entries['..']['st_ino'], self.oids['/'])
        self.assert_(stat.S_ISDIR(entries['..']['st_mode']))

        # rmdir /., /..
        self.assertRaises(Exception, self.ops.rmdir, self.oids['/'], '.')
        self.assertRaises(Exception, self.ops.rmdir, self.oids['/'], '..')

    def subdir(self):
        """Test with the root inode and a subdirectory.

        Directory structure::
            /
            /a/

        If the filesystem is working correctly, this should have no side
        effects on observable state.
        """

        # getattr /a
        st = self.ops.getattr(self.oids['/a/'])
        self.assertEquals(st['st_ino'], self.oids['/a/'])
        self.assert_(stat.S_ISDIR(st['st_mode']))

        # lookup / a
        st = self.ops.lookup(self.oids['/'], 'a')
        self.assertEquals(st['st_ino'], self.oids['/a/'])
        self.assert_(stat.S_ISDIR(st['st_mode']))

        # lookup /a/ ..
        st = self.ops.lookup(self.oids['/a/'], '..')
        self.assertEquals(st['st_ino'], self.oids['/'])
        self.assert_(stat.S_ISDIR(st['st_mode']))

        # opendir, readdir, releasedir /
        dh = self.ops.opendir(self.oids['/'])
        entries = dict(self.ops.readdir(dh, 0))
        self.ops.releasedir(dh)
        self.assertEquals(set(entries.keys()), set(['.', '..', 'a']))
        self.assertEquals(entries['a']['st_ino'], self.oids['/a/'])
        self.assert_(stat.S_ISDIR(entries['a']['st_mode']))

        # opendir, readdir, releasedir /a
        dh = self.ops.opendir(self.oids['/a/'])
        entries = dict(self.ops.readdir(dh, 0))
        self.ops.releasedir(dh)
        self.assertEquals(set(entries.keys()), set(['.', '..']))
        self.assertEquals(entries['.']['st_ino'], self.oids['/a/'])
        self.assert_(stat.S_ISDIR(entries['.']['st_mode']))
        self.assertEquals(entries['..']['st_ino'], self.oids['/'])
        self.assert_(stat.S_ISDIR(entries['..']['st_mode']))

    def removed_subdir(self):
        """Test with the root inode and a removed subdirectory.

        Directory structure::
            /

        If the filesystem is working correctly, this should have no side
        effects on observable state.
        """

        self.assertRaises(FUSEError, self.ops.rmdir, self.oids['/'], 'a')
        self.assertRaises(FUSEError, self.ops.rmdir, self.oids['/a/'], '.')
        self.assertRaises(FUSEError, self.ops.rmdir, self.oids['/a/'], '..')
        self.assertRaises(FUSEError, self.ops.getattr, self.oids['/a/'])
        self.assertRaises(FUSEError, self.ops.opendir, self.oids['/a/'])
        self.assertRaises(FUSEError, self.ops.mkdir, self.oids['/a/'], 'a',
                                                     0, None)
        self.assertRaises(FUSEError, self.ops.lookup, self.oids['/a/'], '.')
        self.assertRaises(FUSEError, self.ops.lookup, self.oids['/'], 'a')
        # ok, it's probably gone
        del self.oids['/a/']

    def subsubdir(self):
        """Test with the root inode, a subdirectory, and a subsubdirectory.

        Directory structure::
            /
            /a/
            /a/b/

        If the filesystem is working correctly, this should have no side
        effects on observable state.
        """

        # rmdir non-empty /a
        self.assertRaises(FUSEError, self.ops.rmdir, self.oids['/'], 'a')

    def test_x(self):
        self.ops = ramcloudfs.Operations()
        self.ops.init()

        self.oids = {}
        self.oids['/'] = ramcloudfs.ROOT_OID

        self.no_inodes()
        self.root_inode()

        # mkdir / a
        st = self.ops.mkdir(self.oids['/'], 'a', 0, None)
        self.oids['/a/'] = int(st['st_ino'])
        self.assert_(stat.S_ISDIR(st['st_mode']))

        self.subdir()

        # remove / a

        dh = self.ops.opendir(self.oids['/a/'])
        self.ops.rmdir(self.oids['/'], 'a')
        entries = dict(self.ops.readdir(dh, 0))
        self.ops.releasedir(dh)
        self.assert_('a' not in entries) # rm -r depends on this

        self.removed_subdir()

        # mkdir / a
        st = self.ops.mkdir(self.oids['/'], 'a', 0, None)
        self.oids['/a/'] = int(st['st_ino'])
        self.assert_(stat.S_ISDIR(st['st_mode']))

        # mkdir /a/ b
        st = self.ops.mkdir(self.oids['/a/'], 'b', 0, None)
        self.oids['/a/b/'] = int(st['st_ino'])
        self.assert_(stat.S_ISDIR(st['st_mode']))

        self.subsubdir()

        # rmdir /a/b, rmdir /a
        self.ops.rmdir(self.oids['/a/'], 'b')
        self.ops.rmdir(self.oids['/'], 'a')
        del self.oids['/a/b/']
        del self.oids['/a/']


if __name__ == '__main__':
    unittest.main()
