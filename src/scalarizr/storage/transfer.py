from __future__ import with_statement
'''
Created on Aug 25, 2010

@author: marat
@author: Dmytro Korsakov
'''

import logging
from Queue import Queue, Empty
from threading import Thread, Lock
from urlparse import urlparse
import sys
import os

'''
Example:

scalarizr.platform.ec2.storage:

from scalarizr.storage.transfer import Transfer, TransferProvider

class S3TransferProvider(TransferProvider):
        schema = 's3'
        pass

Transfer.explore_provider(S3TransferProvider)

trn = Transfer(pool=5, max_attempts=3)
trn.upload(files, 'cf://container/path/to/candy')
trn.download('s3://scalr-files/path/to/some-shit/', dst, recursive=True)

Create separate environment for each operation

trn.upload(files, 'cf://container/path/to/candy', pvd_options=None)
  -> pvd = self._resolve_path(path, pvd_options=None)
  -> create queue
  -> create pool
  -> start pool
  -> pvd.put(file, os.path.join(path, os.path.basename(file)))
  -> return tuple of remote path


trn.download('s3://scalr-files/path/to/some-shit/', dst, recursive=True)
        -> ('s3://scalr-files/path/to/data.1', 's3://scalr-files/path/to/data.2' ...)
        -> pvd = self._resolve_path()
        -> if recursive:
        ->     files = pvd.list(path)
        -> else:
        ->     files = (path,)
        -> create queue
        -> create pool
        -> start pool
        -> pvd.get(remote_path, os.path.join(dst, os.path.basename(remote_path)))
        -> return tuple of local path

'''

class TransferError(BaseException):
    pass

class StopTransfer(Exception):
    pass

class Transfer(object):
    providers = {}

    @staticmethod
    def explore_provider(PvdClass):
        self = Transfer
        schema = PvdClass.schema
        self.providers[schema] = PvdClass

    @staticmethod
    def lookup_provider(remote_path, **kwargs):
        o = urlparse(remote_path)
        schema = o.scheme
        self = Transfer

        try:
            pvd = self.providers[schema]
        except KeyError:
            raise LookupError('Unknown provider "%s"' % (schema,))
        obj = pvd()
        obj.configure(remote_path, **kwargs)
        return obj

    def __init__(self, pool=3, max_attempts=3, logger=None):
        self._logger = logger or logging.getLogger(__name__)
        self._pool = pool
        self._max_attempts = max_attempts

    def upload(self, files, remote_path, **pvd_options):
        pvd = self.lookup_provider(remote_path, **pvd_options)
        action = self._put_action(pvd, remote_path)
        return self._transfer(files, action)

    def download(self, rfiles, dst, recursive=False, **pvd_options):
        if isinstance(rfiles, basestring):
            rfiles = (rfiles,)
        pvd = self.lookup_provider(rfiles[0], **pvd_options)
        if recursive:
            rfiles = pvd.list(rfiles[0])
        action = self._get_action(pvd, dst)
        return self._transfer(rfiles, action)

    def _put_action(self, pvd, dst):
        def g(filename):
            return pvd.put(filename, dst)
        return g

    def _get_action(self, pvd, local_dst):
        def g(filename):
            return pvd.get(filename,local_dst)
        return g

    def _transfer(self, files, action):
        # Enqueue
        queue = Queue()
        for file in files:
            os.access(file, os.R_OK)
            queue.put((file, 0))

        workers = []
        failed_files = []
        result = dict()

        self._failed_files_lock = Lock()

        #Starting threads
        for n in range(min(self._pool, len(files))):
            worker = Thread(name="Worker-%s" % n, target=self._worker,
                            args=(action, queue, result, failed_files))
            self._logger.debug("Starting worker '%s'", worker.getName())
            worker.start()
            workers.append(worker)

        # Join workers
        for worker in workers:
            worker.join()
            self._logger.debug("Worker '%s' finished", worker.getName())

        if failed_files:
            raise TransferError("Cannot process several files. %s" % [", ".join(failed_files)])

        self._logger.info("Transfer complete!")

        # Return tuple of all files     def set_access_data(self, access_data):
        self._logger.debug('Transfer result: %s', (result,))
        return tuple(result[file] for file in files)

    def _worker(self, action, queue, result, failed_files):
        try:
            while 1:
                filename, attempts = queue.get(False)
                try:
                    result[filename] = action(filename)
                except StopTransfer:
                    break
                except (Exception, BaseException), e:
                    self._logger.warn("Cannot transfer '%s'. %s", filename, e, exc_info=sys.exc_info())
                    # For all transfer errors give a second chance
                    if isinstance(e, TransferError):
                        if attempts < self._max_attempts:
                            self._logger.debug("File '%s' will be transfered within the next attempt", filename)
                            attempts += 1
                            queue.put((filename, attempts))
                            continue
                    # Append file to failed list
                    try:
                        self._failed_files_lock.acquire()
                        failed_files.append(filename)
                    finally:
                        self._failed_files_lock.release()
        except Empty:
            return


class TransferProvider:
    schema = None
    prefix = None

    def __init__(self):
        pass

    def configure(self, remote_path, **kwargs):
        pass

    def put(self, local_path, remote_path):
        pass

    def get(self, remote_path, local_path):
        pass

    def list(self, remote_path):
        pass
