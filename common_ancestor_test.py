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

"""Unit tests for L{common_ancestor}."""

import unittest

from common_ancestor import Node, find_common_ancestor


class TreeNode(Node):

    def __init__(self, parent):
        self.parent = parent

    def get_parent(self):
        assert self.parent is not None # parent of root
        return self.parent


class TestFindCommonAncestor(unittest.TestCase):
    """Test L{common_ancestor.find_common_ancestor}."""

    def assertFCA(self, root, left, right, left_path, right_path):
        paths = find_common_ancestor(root, left, right)
        self.assertEquals(left_path, paths[0])
        self.assertEquals(right_path, paths[1])

    def test_fca(self):

        # R
        # |
        # |-- A
        # |   |
        # |   '-- C
        # |
        # '-- B

        R = TreeNode(None)
        A = TreeNode(R)
        B = TreeNode(R)
        C = TreeNode(A)

        self.assertFCA(R, R, R, [R], [R])
        self.assertFCA(R, A, R, [A, R], [R])
        self.assertFCA(R, R, A, [R], [A, R])
        self.assertFCA(R, A, B, [A, R], [B, R])
        self.assertFCA(R, R, C, [R], [C, A, R])
        self.assertFCA(R, A, C, [A], [C, A])
        self.assertFCA(R, C, B, [C, A, R], [B, R])

if __name__ == '__main__':
    unittest.main()
