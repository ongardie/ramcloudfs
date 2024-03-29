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
import re

from llfuse import FUSEError

import ramcloudfs


def dict_key_replace(d, pattern, replacement):
    new = {}
    for (key, value) in d.items():
        new[re.sub(pattern, replacement, key)] = value
    d.clear()
    d.update(new)


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
        self.assertRaises(FUSEError, self.ops.rename, 999, 'foo', 999, 'bar')
        self.assertRaises(FUSEError, self.ops.rename, 999, 'foo', 997, 'bar')
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

        # rename from . and ..
        self.assertRaises(Exception, self.ops.rename, self.oids['/'], '.',
                                                      self.oids['/'], 'foo')
        self.assertRaises(Exception, self.ops.rename, self.oids['/'], '..',
                                                      self.oids['/'], 'foo')
        self.assertRaises(Exception, self.ops.rename, self.oids['/'], '.',
                                                      self.oids['/'], '..')
        self.assertRaises(Exception, self.ops.rename, self.oids['/'], '..',
                                                      self.oids['/'], '.')

        # rename from non-existent name
        self.assertRaises(FUSEError, self.ops.rename, self.oids['/'], 'foo',
                                                      self.oids['/'], 'bar')

        # rename to/from bad inode numbers
        self.assertRaises(FUSEError, self.ops.rename, self.oids['/'], 'foo',
                                                      999, 'bar')
        self.assertRaises(FUSEError, self.ops.rename, 999, 'foo',
                                                      self.oids['/'], 'bar')

    def subdir(self):
        """Test with the root inode and a subdirectory.

        Directory structure::
            /
            /a/

        /a/ was recently created.

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

        # rename to . and ..
        self.assertRaises(Exception, self.ops.rename, self.oids['/'], 'a',
                                                      self.oids['/'], '.')
        self.assertRaises(Exception, self.ops.rename, self.oids['/'], 'a',
                                                      self.oids['/'], '..')

    def renamed_subdir(self):
        """Test with the root inode and a renamed subdirectory.

        Directory structure::
            /
            /b/

        /b/ was recently renamed from /a/.

        If the filesystem is working correctly, this should have no side
        effects on observable state.
        """

        # Make sure /a/ is gone
        self.assertRaises(FUSEError, self.ops.lookup, self.oids['/'], 'a')

        self.assertEquals(self.ops.lookup(self.oids['/'], 'b')['st_ino'],
                          self.oids['/b/'])

        dh = self.ops.opendir(self.oids['/'])
        entries = dict(self.ops.readdir(dh, 0))
        self.ops.releasedir(dh)
        self.assertEquals(set(entries.keys()), set(['.', '..', 'b']))

    def removed_subdir(self):
        """Test with the root inode and a removed subdirectory.

        Directory structure::
            /

        /b/ was recently removed.

        If the filesystem is working correctly, this should have no side
        effects on observable state.
        """

        self.assertRaises(FUSEError, self.ops.rmdir, self.oids['/'], 'b')
        self.assertRaises(FUSEError, self.ops.rmdir, self.oids['/b/'], '.')
        self.assertRaises(FUSEError, self.ops.rmdir, self.oids['/b/'], '..')
        self.assertRaises(FUSEError, self.ops.getattr, self.oids['/b/'])
        self.assertRaises(FUSEError, self.ops.opendir, self.oids['/b/'])
        self.assertRaises(FUSEError, self.ops.mkdir, self.oids['/b/'], 'b',
                                                     0, None)
        self.assertRaises(FUSEError, self.ops.lookup, self.oids['/b/'], '.')
        self.assertRaises(FUSEError, self.ops.lookup, self.oids['/'], 'b')
        # ok, it's probably gone
        del self.oids['/b/']

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

    def dirs(self):
        self.no_inodes()
        self.root_inode()

        # mkdir / a
        st = self.ops.mkdir(self.oids['/'], 'a', 0, None)
        self.oids['/a/'] = int(st['st_ino'])
        self.assert_(stat.S_ISDIR(st['st_mode']))

        self.subdir()

        # rename / a to / b
        self.ops.rename(self.oids['/'], 'a', self.oids['/'], 'b')
        dict_key_replace(self.oids, '^/a/', '/b/')

        self.renamed_subdir()

        self.ops.rename(self.oids['/'], 'b', self.oids['/'], 'b')

        self.renamed_subdir()

        # remove / b

        dh = self.ops.opendir(self.oids['/b/'])
        self.ops.rmdir(self.oids['/'], 'b')
        entries = dict(self.ops.readdir(dh, 0))
        self.ops.releasedir(dh)
        self.assert_('b' not in entries) # rm -r depends on this

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


        # And this quickly degrades into some ugly tests for rename...

        # mkdir /c/, mkdir /c/d/, mkdir /e/
        st = self.ops.mkdir(self.oids['/'], 'c', 0, None)
        self.oids['/c/'] = int(st['st_ino'])
        self.assert_(stat.S_ISDIR(st['st_mode']))

        st = self.ops.mkdir(self.oids['/c/'], 'd', 0, None)
        self.oids['/c/d/'] = int(st['st_ino'])
        self.assert_(stat.S_ISDIR(st['st_mode']))

        st = self.ops.mkdir(self.oids['/'], 'e', 0, None)
        self.oids['/e/'] = int(st['st_ino'])
        self.assert_(stat.S_ISDIR(st['st_mode']))

        self.assertRaises(FUSEError, self.ops.rename, self.oids['/'], 'e',
                                                      self.oids['/'], 'c')
        self.ops.rename(self.oids['/'], 'c', self.oids['/'], 'f')
        dict_key_replace(self.oids, '^/c/', '/f/')
        self.ops.rename(self.oids['/'], 'f', self.oids['/'], 'e')
        dict_key_replace(self.oids, '^/f/', '/e/')

        st = self.ops.mkdir(self.oids['/e/d/'], 'g', 0, None)
        self.oids['/e/d/g/'] = int(st['st_ino'])
        self.assert_(stat.S_ISDIR(st['st_mode']))

        # Now we have:
        #  /
        #  /a/
        #  /a/b/
        #  /e/
        #  /e/d/
        #  /e/d/g/

        self.assertRaises(FUSEError, self.ops.rename, self.oids['/a/'], 'b',
                                                      self.oids['/e/'], 'd')
        self.ops.rename(self.oids['/a/'], 'b', self.oids['/e/'], 'h')
        dict_key_replace(self.oids, '^/a/b/', '/e/h/')
        self.ops.rename(self.oids['/e/'], 'h', self.oids['/e/d/'], 'g')
        dict_key_replace(self.oids, '^/e/h/', '/e/d/g/')
        # Now we have:
        #  /
        #  /a/
        #  /e/
        #  /e/d/
        #  /e/d/g/

        # Make sure you can't move a dir below itself (EINVAL)
        self.assertRaises(FUSEError, self.ops.rename, self.oids['/'], 'e',
                          self.oids['/e/d/g/'], 'i')
        self.assertRaises(FUSEError, self.ops.rename, self.oids['/'], 'e',
                          self.oids['/e/d/'], 'i')
        self.assertRaises(FUSEError, self.ops.rename, self.oids['/'], 'e',
                          self.oids['/e/d/'], 'g')
        self.assertRaises(FUSEError, self.ops.rename, self.oids['/'], 'e',
                          self.oids['/e/'], 'd')

        # rm -r /*
        self.ops.rmdir(self.oids['/'], 'a')
        del self.oids['/a/']
        self.ops.rmdir(self.oids['/e/d/'], 'g')
        del self.oids['/e/d/g/']
        self.ops.rmdir(self.oids['/e/'], 'd')
        del self.oids['/e/d/']
        self.ops.rmdir(self.oids['/'], 'e')
        del self.oids['/e/']
        self.assertEquals(self.oids.keys(), ['/'])

    def files(self):

        # mknod / z
        attr = self.ops.mknod(self.oids['/'], 'z', 0, None, None)
        self.oids['/z'] = attr['st_ino']
        self.assertEquals(self.ops.lookup(self.oids['/'], 'z')['st_ino'],
                          self.oids['/z'])

        # open, read and write, release /z
        fh = self.ops.open(self.oids['/z'], 0)
        self.assertEquals(self.ops.read(fh, 0, 4096), '')
        self.assertEquals(self.ops.write(fh, 8, 'rofl'), len('rofl'))
        self.assertEquals(self.ops.getattr(self.oids['/z'])['st_size'],
                          8 + len('rofl'))
        self.assertEquals(self.ops.read(fh, 0, 4096), '\x00' * 8 + 'rofl')
        self.ops.release(fh)

        attr = self.ops.setattr(self.oids['/z'], {'st_size': 16})
        self.assertEquals(attr['st_size'], 16)

        # renames
        self.ops.rename(self.oids['/'], 'z', self.oids['/'], 'y')
        dict_key_replace(self.oids, '^/z$', '/y')

        attr = self.ops.mknod(self.oids['/'], 'x', 0, None, None)
        self.oids['/x'] = attr['st_ino']

        self.ops.rename(self.oids['/'], 'y', self.oids['/'], 'x')
        self.assertRaises(FUSEError, self.ops.getattr, self.oids['/x'])
        del self.oids['/x']
        dict_key_replace(self.oids, '^/y$', '/x')

        attr = self.ops.mkdir(self.oids['/'], 'a', 0, None)
        self.oids['/a/'] = attr['st_ino']

        self.assertRaises(FUSEError, self.ops.rename, self.oids['/'], 'a',
                                                      self.oids['/'], 'x')
        self.assertRaises(FUSEError, self.ops.rename, self.oids['/'], 'x',
                                                      self.oids['/'], 'a')
        self.assertEquals(self.ops.lookup(self.oids['/'], 'x')['st_ino'],
                          self.oids['/x'])

        # hard links
        attr = self.ops.link(self.oids['/x'], self.oids['/'], 'w')
        self.assertEquals(attr['st_ino'], self.oids['/x'])
        self.assertEquals(attr['st_nlink'], 2)
        self.assertEquals(self.ops.lookup(self.oids['/'], 'w')['st_ino'],
                          self.oids['/x'])
        self.oids['/w'] = self.oids['/x']

        self.ops.rename(self.oids['/'], 'x', self.oids['/'], 'w')
        self.assertEquals(self.ops.getattr(self.oids['/x'])['st_nlink'], 2)

        attr = self.ops.link(self.oids['/w'], self.oids['/'], 'v')
        self.oids['/v'] = attr['st_ino']
        self.assertEquals(self.ops.getattr(self.oids['/v'])['st_nlink'], 3)

        attr = self.ops.mknod(self.oids['/'], 'u', 0, None, None)
        self.oids['/u'] = attr['st_ino']

        self.ops.rename(self.oids['/'], 'u', self.oids['/'], 'v')
        del self.oids['/v']
        dict_key_replace(self.oids, '^/u$', '/v')
        self.assertRaises(FUSEError, self.ops.lookup, self.oids['/'], 'u')
        self.assertEquals(self.ops.getattr(self.oids['/x'])['st_nlink'], 2)
        self.assertEquals(self.ops.getattr(self.oids['/v'])['st_nlink'], 1)

        self.ops.unlink(self.oids['/'], 'w')
        del self.oids['/w']
        self.assertEquals(self.ops.getattr(self.oids['/x'])['st_nlink'], 1)

        # rmdir / a
        self.ops.rmdir(self.oids['/'], 'a')
        del self.oids['/a/']

        # unlink / x
        self.ops.unlink(self.oids['/'], 'x')
        self.assertRaises(FUSEError, self.ops.lookup, self.oids['/'], 'x')
        del self.oids['/x']

        # unlink / v
        self.ops.unlink(self.oids['/'], 'v')
        self.assertRaises(FUSEError, self.ops.lookup, self.oids['/'], 'v')
        del self.oids['/v']

        self.assertEquals(self.oids.keys(), ['/'])

    def test_x(self):
        self.ops = ramcloudfs.Operations()
        self.ops.init()

        self.oids = {}
        self.oids['/'] = ramcloudfs.ROOT_OID

        self.dirs()
        self.files()

if __name__ == '__main__':
    unittest.main()
