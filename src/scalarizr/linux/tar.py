'''
Created on Jan 11, 2013

@author: uty
'''

from __future__ import with_statement


class Tar:
    EXECUTABLE = "tar"

    _executable = None
    _options = None
    _files = None

    def __init__(self, executable=None):
        self._executable = executable if executable else self.EXECUTABLE
        self._options = []
        self._files = []

    def version(self):
        self._options.append("--version")
        return self

    def verbose(self):
        self._options.append("-v")
        return self

    def create(self):
        self._options.append("-c")
        return self

    def bzip2(self):
        self._options.append("-j")
        return self

    def diff(self):
        self._options.append("-d")
        return self

    def gzip(self):
        self._options.append("-z")
        return self

    def extract(self):
        self._options.append("-x")
        return self

    def update(self):
        self._options.append("-u")
        return self

    def sparse(self):
        self._options.append("-S")
        return self

    def dereference(self):
        self._options.append("-h")
        return self

    def archive(self, filename):
        self._options.append("-f " + filename if filename else "-")
        return self

    def chdir(self, dir):
        self._options.append("-C " + dir)
        return self

    def add(self, filename, dir=None):
        item = filename if dir is None else "-C " + dir + " " + filename
        self._files.append(item)
        return self

    def __str__(self):
        ret = "%(executable)s %(options)s %(files)s" % dict(
            executable=self._executable,
            options=" ".join(self._options),
            files=" ".join(self._files))
        return ret.strip()
