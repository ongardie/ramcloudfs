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

import sys
import logging
import llfuse

FUSE_DEBUG=True

class Operations(llfuse.Operations):
    pass

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
