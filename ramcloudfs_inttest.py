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

    def test_x(self):
        ops = ramcloudfs.Operations()
        ops.init()


        # Test what we can with the one inode we have first (/):

        # getattr /
        st = ops.getattr(ramcloudfs.ROOT_OID)
        self.assertEquals(st['st_ino'], ramcloudfs.ROOT_OID)
        self.assert_(stat.S_ISDIR(st['st_mode']))

        # lookup / .
        st = ops.lookup(ramcloudfs.ROOT_OID, '.')
        self.assertEquals(st['st_ino'], ramcloudfs.ROOT_OID)
        self.assert_(stat.S_ISDIR(st['st_mode']))

        # lookup / ..
        st = ops.lookup(ramcloudfs.ROOT_OID, '..')
        self.assertEquals(st['st_ino'], ramcloudfs.ROOT_OID)
        self.assert_(stat.S_ISDIR(st['st_mode']))

        # lookup non-existent
        self.assertRaises(FUSEError, ops.lookup, ramcloudfs.ROOT_OID, 'foo')

        # mkdir / . and / ..
        self.assertRaises(Exception, ops.mkdir, ramcloudfs.ROOT_OID, '.',
                          0, None)
        self.assertRaises(Exception, ops.mkdir, ramcloudfs.ROOT_OID, '..',
                          0, None)

        # opendir, readdir, releasedir /
        dh = ops.opendir(ramcloudfs.ROOT_OID)
        entries = dict(ops.readdir(dh, 0))
        ops.releasedir(dh)
        self.assertEquals(set(entries.keys()), set(['.', '..']))
        self.assertEquals(entries['.']['st_ino'], ramcloudfs.ROOT_OID)
        self.assert_(stat.S_ISDIR(entries['.']['st_mode']))
        self.assertEquals(entries['..']['st_ino'], ramcloudfs.ROOT_OID)
        self.assert_(stat.S_ISDIR(entries['..']['st_mode']))

        # rmdir /., /..
        self.assertRaises(Exception, ops.rmdir, ramcloudfs.ROOT_OID, '.')
        self.assertRaises(Exception, ops.rmdir, ramcloudfs.ROOT_OID, '..')


        # Now make /a and test with it

        oids = {}

        # mkdir / a
        st = ops.mkdir(ramcloudfs.ROOT_OID, 'a', 0, None)
        oids['/a'] = int(st['st_ino'])
        self.assert_(stat.S_ISDIR(st['st_mode']))

        # getattr /a
        st = ops.getattr(oids['/a'])
        self.assertEquals(st['st_ino'], oids['/a'])
        self.assert_(stat.S_ISDIR(st['st_mode']))

        # lookup / a
        st = ops.lookup(ramcloudfs.ROOT_OID, 'a')
        self.assertEquals(st['st_ino'], oids['/a'])
        self.assert_(stat.S_ISDIR(st['st_mode']))

        # lookup /a/ ..
        st = ops.lookup(oids['/a'], '..')
        self.assertEquals(st['st_ino'], ramcloudfs.ROOT_OID)
        self.assert_(stat.S_ISDIR(st['st_mode']))

        # opendir, readdir, releasedir /
        dh = ops.opendir(ramcloudfs.ROOT_OID)
        entries = dict(ops.readdir(dh, 0))
        ops.releasedir(dh)
        self.assertEquals(set(entries.keys()), set(['.', '..', 'a']))
        self.assertEquals(entries['a']['st_ino'], oids['/a'])
        self.assert_(stat.S_ISDIR(entries['a']['st_mode']))

        # opendir, readdir, releasedir /a
        dh = ops.opendir(oids['/a'])
        entries = dict(ops.readdir(dh, 0))
        ops.releasedir(dh)
        self.assertEquals(set(entries.keys()), set(['.', '..']))
        self.assertEquals(entries['.']['st_ino'], oids['/a'])
        self.assert_(stat.S_ISDIR(entries['.']['st_mode']))
        self.assertEquals(entries['..']['st_ino'], ramcloudfs.ROOT_OID)
        self.assert_(stat.S_ISDIR(entries['..']['st_mode']))


        # Now remove / a

        dh = ops.opendir(oids['/a'])
        ops.rmdir(ramcloudfs.ROOT_OID, 'a')
        entries = dict(ops.readdir(dh, 0))
        ops.releasedir(dh)
        self.assert_('a' not in entries) # rm -r depends on this
        self.assertRaises(FUSEError, ops.rmdir, ramcloudfs.ROOT_OID, 'a')
        self.assertRaises(FUSEError, ops.rmdir, oids['/a'], '.')
        self.assertRaises(FUSEError, ops.rmdir, oids['/a'], '..')
        self.assertRaises(FUSEError, ops.getattr, oids['/a'])
        self.assertRaises(FUSEError, ops.opendir, oids['/a'])
        self.assertRaises(FUSEError, ops.mkdir, oids['/a'], 'b', 0, None)
        self.assertRaises(FUSEError, ops.lookup, oids['/a'], '.')
        self.assertRaises(FUSEError, ops.lookup, ramcloudfs.ROOT_OID, 'a')
        # ok, it's probably gone
        del oids['/a']


        # mkdir / a, mkdir /a/ b
        st = ops.mkdir(ramcloudfs.ROOT_OID, 'a', 0, None)
        oids['/a'] = int(st['st_ino'])
        self.assert_(stat.S_ISDIR(st['st_mode']))

        st = ops.mkdir(oids['/a'], 'b', 0, None)
        oids['/a/b'] = int(st['st_ino'])
        self.assert_(stat.S_ISDIR(st['st_mode']))

        # rmdir non-empty /a
        self.assertRaises(FUSEError, ops.rmdir, ramcloudfs.ROOT_OID, 'a')


        # rmdir /a/b, rmdir /a
        ops.rmdir(oids['/a'], 'b')
        ops.rmdir(ramcloudfs.ROOT_OID, 'a')
        del oids['/a/b']
        del oids['/a']


if __name__ == '__main__':
    unittest.main()
