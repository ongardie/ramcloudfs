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

"""See L{find_common_ancestor}.

This is a general algorithm that is independent from the RAMCloud filesystem.
It is used as a helper for the filesystem rename operation.
"""


class Node(object):
    """An interface for a tree node. See L{find_common_ancestor}.

    You should also define equality (C{__eq__} or C{__cmp__} or whatever
    it is these days) if the default reference equality isn't what you want.
    """

    def get_parent(self):
        """
        @return: The parent of this node in the tree.
        @rtype: L{Node}
        """

        raise NotImplemented


def find_common_ancestor(root, left, right):
    """Find the paths to the lowest common ancestor of two L{Node}s in a tree.

    This algorithm assumes that calling L{Node.get_parent} may be expensive but
    testing L{Node} equality is cheap. It also assumes that the caller has no
    knowledge of the depth of the nodes in the tree (though it slightly favors
    C{left} being deeper). The number of L{Node.get_parent} operations required
    is on the order of twice the distance to the lowest common ancestor from
    the node that is deeper in the tree.

    @precondition: C{root} is a common ancestor of both C{left} and C{right}.

    @param root: The root node in the tree.
    @type  root: L{Node}

    @param left: One node to find an ancestor from.
    @type  left: L{Node}

    @param right: The other node from which to find an ancestor.
    @type  right: L{Node}

    @return: A tuple of two paths, the path from the left node to the lowest
             common ancestor, inclusive, and the path from the right node to
             the lowest common ancestor, inclusive. The paths are represented
             as ordered C{list}s of L{Node}s.
    @rtype:  C{tuple}
    """

    left_path = [left]
    right_path = [right]
    if left == right:
        return (left_path, right_path)
    while True:
        # The root of the tree must be a common ancestor, so this loop will
        # eventually terminate.
        if left != root:
            left = left.get_parent()
            left_path.append(left)
            if left in right_path:
                return (left_path, right_path[:right_path.index(left) + 1])
        if right != root:
            right = right.get_parent()
            right_path.append(right)
            if right in left_path:
                return (left_path[:left_path.index(right) + 1], right_path)
