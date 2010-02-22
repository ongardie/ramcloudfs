PYTHON := /usr/bin/env python2.6
SOURCE_FILES := ramcloudfs.py
TEST_FILES := ramcloudfs_test.py

all:

.PHONY:
test:
	@ failed=0; \
	for test in $(TEST_FILES); do \
		echo "Running $$test:"; \
		$(PYTHON) $$test -q || failed=1; \
		echo; \
	done; \
	exit $$failed

.PHONY:
check:
	pep8 --repeat --show-pep8 $(SOURCE_FILES) $(TEST_FILES)

.PHONY:
docs:
	/usr/bin/env python2.6 /usr/bin/epydoc --html \
		-n "RAMCloud Filesystem" -o epydoc/ --simple-term -v \
		$(SOURCE_FILES) $(TEST_FILES)

.PHONY:
clean:
	rm -rf epydoc/ *.pyc
