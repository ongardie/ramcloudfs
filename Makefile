PYTHON := /usr/bin/env python2.6

SOURCE_FILES    := ramcloudfs.py common_ancestor.py
UNIT_TEST_FILES := ramcloudfs_test.py common_ancestor_test.py
INT_TEST_FILES  := ramcloudfs_inttest.py
CHECK_FILES     := $(SOURCE_FILES) $(UNIT_TEST_FILES) $(INT_TEST_FILES)
DOC_FILES       := $(SOURCE_FILES) $(UNIT_TEST_FILES) $(INT_TEST_FILES)

all:

unittest:
	@ failed=0; \
	for test in $(UNIT_TEST_FILES); do \
		echo "Running unit test $$test:"; \
		$(PYTHON) $$test -q || failed=1; \
		echo; \
	done; \
	exit $$failed

inttest:
	@ failed=0; \
	for test in $(INT_TEST_FILES); do \
		echo "Running integration test $$test:"; \
		$(PYTHON) $$test -q || failed=1; \
		echo; \
	done; \
	exit $$failed

check:
	pep8 --repeat --show-pep8 $(CHECK_FILES)

docs:
	/usr/bin/env python2.6 /usr/bin/epydoc --html \
		-n "RAMCloud Filesystem" -o epydoc/ --simple-term -v \
		$(DOC_FILES)

clean:
	rm -rf epydoc/ *.pyc

.PHONY: unittest inttest check docs clean
