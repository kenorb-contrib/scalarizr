import os
import re
import sys
import time
import uuid
import Queue
import shutil
import bisect
import signal
import thread
import logging
import hashlib
import tempfile
import urlparse
import threading
import traceback
import subprocess
import multiprocessing

from multiprocessing.pool import ThreadPool

from scalarizr import util
from scalarizr.util import cryptotool
from scalarizr.linux import pkgmgr
from scalarizr.storage2.cloudfs import cloudfs, NamedStream, Manifest


LOG = logging.getLogger(__name__)


__thread_error__ = None

DEFAULT_POOL_SIZE = 4
DEFAULT_CALLBACK_INTERVAL = 2
DEFAULT_CHUNK_SIZE = 100
DEFAULT_SLEEP_TIME = 0.1
DEFAULT_RETRY_NUMBER = 3


def raise_thread_error():
    global __thread_error__
    try:
        raise __thread_error__[0], __thread_error__[1], __thread_error__[2]
    finally:
        __thread_error__ = None


class TransferError(Exception):
    pass


class MD5SumError(Exception):
    pass


def thread_name():
    return threading.current_thread().name


_url_re = re.compile(r'^[\w-]+://')


def _is_remote_path(path):
    return isinstance(path, basestring) and _url_re.match(path)


class FileInfo(object):

    """
    Simple class to store file info
    """

    def __init__(self, path, md5_sum=None, size=None):
        self.path = path
        self.name = os.path.basename(self.path)
        self.md5_sum = md5_sum
        self.size = size
        if not _is_remote_path(path):
            assert os.path.isfile(path), path
            if self.size is None:
                self.size = os.path.getsize(path)
            if self.md5_sum is None:
                self.md5_sum = cryptotool.calculate_md5_sum(path)


class NonBlockingLifoQueue(Queue.LifoQueue):

    """
    This class makes join method interruptable
    """

    def join(self):
        self.all_tasks_done.acquire()
        try:
            while self.unfinished_tasks:
                self.all_tasks_done.wait(sys.maxint)
        finally:
            self.all_tasks_done.release()


class _Transfer(object):

    """
    Internal class for multithreaded upload and download
    """

    def __init__(self, method, pool_size=None):
        assert method in ['put', 'get']
        self.method = method
        self._pool_size = pool_size or DEFAULT_POOL_SIZE
        self._queue = NonBlockingLifoQueue(maxsize=self._pool_size * 2)
        self._start_workers()

    def _start_workers(self):
        self._pool = ThreadPool(self._pool_size)
        self._workers = [_Worker(self._queue) for i in xrange(self._pool_size)]
        self._pool.map_async(_Worker.start, self._workers)
        map(_Worker.wait_start, self._workers)
        self._pool.close()

    def wait_completion(self):
        self._queue.join()

    def stop(self, wait=True):
        map(_Worker.stop, self._workers)
        # Forcefully do task_done to avoid infinity lock in wait_completion()
        try:
            while True:
                self._queue.task_done()
        except ValueError:
            pass
        if wait:
            self._pool.join()

    def apply_async(self, src, dst, complete_cb=None, progress_cb=None):
        for worker in self._workers:
            if worker.status == 'running':
                break
        else:
            assert False, 'Running workers not found'

        if self.method == 'put':
            scheme = urlparse.urlparse(dst).scheme or 'file'
        else:
            scheme = urlparse.urlparse(src.path).scheme or 'file'

        driver = cloudfs(scheme)

        def complete_cb_wrapper(task):
            if complete_cb:
                info = {
                    'src': task['args'][0],
                    'dst': task['args'][1],
                    'size': task['size'],
                    'md5_sum': task['md5_sum'],
                    'retry': task['retry'],
                    'status': task['status'],
                    'result': task['result'],
                    'error': task['error'],
                }
                complete_cb(info)

        task = {
            'size': src.size,
            'md5_sum': src.md5_sum,
            'fn': getattr(driver, self.method),
            'args': (src.path, dst),
            'kwds': {'report_to': progress_cb},
            'retry': 0,
            'status': 'submitted',
            'error': None,
            'result': None,
            'complete_cb': complete_cb_wrapper,
        }

        self._queue.put(task)


class _Worker(object):

    def __init__(self, queue, retry=None):
        self._queue = queue
        self._retry = retry or DEFAULT_RETRY_NUMBER
        self._name = threading.current_thread().name
        self._status = 'stopped'
        self._start_ev = threading.Event()

    @property
    def name(self):
        return self._name

    @property
    def status(self):
        """
        Possible values are
          - running
          - stopped
        """
        return self._status

    def wait_start(self):
        self._start_ev.wait(sys.maxint)

    def stop(self):
        self._status = 'stopped'

    def start(self):
        self._start_ev.set()
        self._name = threading.current_thread().name
        self._status = 'running'
        try:

            LOG.debug('%s Large Transfer worker starts' % self.name)

            while self._status == 'running':
                try:
                    task = self._queue.get(False)

                    #LOG.debug('Worker %s get task: %s' % (self.name, task))

                    task['status'] = 'running'
                    task['retry'] += 1

                    try:
                        task['result'] = task['fn'](*task['args'], **task['kwds'])
                        task['status'] = 'done'
                    except:
                        if task['retry'] < self._retry:
                            self._queue.put(task)
                        else:
                            task['status'] = 'error'
                            task['error'] = sys.exc_info()
                    finally:
                        if task['status'] != 'running':
                            task['complete_cb'](task)
                        try:
                            self._queue.task_done()
                        except ValueError:
                            pass

                except Queue.Empty:
                    time.sleep(DEFAULT_SLEEP_TIME)
                except:
                    global __thread_error__
                    __thread_error__ = sys.exc_info()
                    thread.interrupt_main()
                    return
        finally:
            self._status = 'stopped'
            self._start_ev.clear()
            LOG.debug('%s Large Transfer worker exits' % self.name)


class Transfer(object):

    """
    Base class for Upload, Download
    """

    def __init__(self, pool_size=None, progress_cb=None, cb_interval=None):
        self.process = None

        self._pool_size = pool_size or DEFAULT_POOL_SIZE
        self._tmp_dir = tempfile.mkdtemp()
        self._error_queue = None
        self._progress = multiprocessing.Value('i', 0)
        self._progress_cb = progress_cb
        self._callback_shutdown_flag = multiprocessing.Value('i', 0)
        self._cb_interval = cb_interval or DEFAULT_CALLBACK_INTERVAL
        self._started_ev = multiprocessing.Event()
        self._semaphore = threading.Semaphore(self._pool_size)
        self._bytes_completed = 0
        self._unfinished_progress = {}
        self._callback_thread = None
        self._error = None

    def _run(self):
        """
        Override in the derived class
        """
        return

    def _wait_start(self):
        self._started_ev.wait(sys.maxint)

    def _wait_stop(self):
        while self._started_ev.is_set():
            time.sleep(0.1)

    def _update_progress(self):
        self._progress.value = self._bytes_completed + sum(self._unfinished_progress.values())

    def _on_file_complete(self, info):
        self._semaphore.release()
        if info['status'] == 'error':
            raise info['error'][0], info['error'][1], info['error'][2]
        if info['status'] == 'done' and info['size']:
            self._bytes_completed += info['size']
            self._update_progress()
        self._unfinished_progress[thread_name()] = 0

    def _on_progress(self, bytes_completed, size):
        self._unfinished_progress[thread_name()] = bytes_completed
        self._update_progress()

    def _cleanup_on_error(self):
        """
        Override in the derived class
        """
        return

    def __call__(self):
        try:
            self._started_ev.set()
            try:
                self._run()
            except KeyboardInterrupt:
                LOG.debug('Interrupt')
                if __thread_error__:
                    raise_thread_error()
                raise
            finally:
                if os.path.exists(self._tmp_dir):
                    shutil.rmtree(self._tmp_dir)
                self._started_ev.clear()
        except:
            er_type = str(sys.exc_info()[0].__name__)
            er_str = str(sys.exc_info()[1])
            tb = ''.join(traceback.format_tb(sys.exc_info()[2]))
            self._error_queue.put(TransferError(er_type, er_str, tb))
            self._cleanup_on_error()
        finally:
            self._callback_shutdown_flag.value = 1

    def _callback_executor(self):
        """
        Periodically call progress callback function
        """
        while self._callback_shutdown_flag.value == 0:
            try:
                self._progress_cb(self.progress)
            except (KeyboardInterrupt, SystemExit):
                raise
            except:
                LOG.error('Error in callback function: %s' % sys.exc_info())
            time.sleep(self._cb_interval)
        self._callback_thread = None

    @property
    def running(self):
        return self.process and self.process.is_alive()

    @property
    def progress(self):
        return self._progress.value

    @property
    def error(self):
        if self._error_queue:
            try:
                self._error = self._error_queue.get(False)
            except Queue.Empty:
                pass
        return self._error

    def apply_async(self):
        assert not self.running
        self.process = multiprocessing.Process(target=self)
        self._error_queue = multiprocessing.Queue(maxsize=1)
        self._error = None
        self.process.start()
        if self._progress_cb:
            self._callback_thread = threading.Thread(target=self._callback_executor)
            self._callback_thread.start()

    def join(self, timeout=None):
        if self.process:
            self.process.join(timeout=timeout)
        if self._error_queue:
            try:
                self._error = self._error_queue.get(False)
            except Queue.Empty:
                pass
        if self._error:
            raise self._error
        self.process = None
        self._callback_thread = None

    def _kill(self, sig):
        msg = 'Send signal {sig} to process with pid {pid}'.format(sig=sig, pid=self.process.pid)
        LOG.debug(msg)
        os.kill(self.process.pid, sig)

    def stop(self):
        """
        Send SIGINT to transfer process
        """
        if not self.running:
            return
        LOG.debug('Stop')
        self._wait_start()
        self._kill(signal.SIGINT)
        self._wait_stop()
        self.process.join()
        self.process = None
        self._callback_thread = None
        self._callback_shutdown_flag.value = 1

    def terminate(self):
        LOG.debug('Terminate')
        if self.running:
            self._kill(signal.SIGKILL)
        self.process = None
        self._callback_thread = None
        self._callback_shutdown_flag.value = 1
        if os.path.exists(self._tmp_dir):
            shutil.rmtree(self._tmp_dir)


class Upload(Transfer):

    def __init__(self, src, dst, transfer_id=None, manifest='manifest.json', description='', tags='',
                 gzip=True, use_pigz=True, simple=False, pool_size=None,
                 chunk_size=None, progress_cb=None, cb_interval=None):
        """
        :type src: string / list / generator / iterator / NamedStream
        :param src: Transfer source, file path or stream

        :type dst: string
        :param dst: Transfer destination

        :type simple: bool
        :param simple: if True handle src as file path and don't use split and gzip
        """
        super(Upload, self).__init__(pool_size=pool_size, progress_cb=progress_cb,
                                     cb_interval=cb_interval)

        if not hasattr(src, '__iter__') or hasattr(src, 'read'):
            self.src = [src]
        else:
            self.src = src
        self.dst = dst
        self.gzip = gzip
        self.use_pigz = use_pigz

        self._simple = simple
        self._chunk_size = chunk_size or DEFAULT_CHUNK_SIZE
        self._manifest = None
        self._manifest_queue = None

        if not self._simple:
            transfer_id = transfer_id or uuid.uuid4().hex
            self.dst = os.path.join(dst, transfer_id)
            self._manifest_name = manifest
            self._manifest = Manifest()
            self._manifest['description'] = description
            self._manifest.cloudfs_path = os.path.join(self.dst, manifest)
            if tags:
                self._manifest['tags'] = tags
            self._manifest_queue = multiprocessing.Queue()

    @property
    def manifest(self):
        if self._manifest_queue and not self._manifest_queue.empty():
            self._manifest = self._manifest_queue.get()
        return self._manifest

    def _cleanup_on_error(self):
        LOG.debug('Cleanup on error')
        self._manifest.delete()

    def _simple_upload(self):
        uploader = _Transfer('put', pool_size=1)
        try:
            file_generator = map(FileInfo, self.src)

            for file_info in file_generator:
                dst = os.path.join(self.dst, file_info.name)
                uploader.apply_async(file_info, dst,
                                     complete_cb=self._on_file_complete,
                                     progress_cb=self._on_progress)
                while not self._semaphore.acquire(False):
                    time.sleep(DEFAULT_SLEEP_TIME)

            uploader.wait_completion()
            uploader.stop()
        except:
            uploader.stop(wait=False)
            raise

    def _large_upload(self):
        uploader = _Transfer('put', pool_size=self._pool_size)
        try:
            if self.gzip and self.use_pigz:
                self._check_pigz()

            for src in self.src:
                name = streamer = extension = None

                if hasattr(src, 'fileno'):
                    # Popen stdout/fileobj/NamedStream
                    if isinstance(src, NamedStream):
                        stream = src
                    else:
                        name = 'stream-%s' % hash(src)
                        stream = NamedStream(src, name)
                elif isinstance(src, basestring) and os.path.isfile(src):
                    # file path
                    dirname, name = os.path.split(src)
                    cmd = ['/bin/tar', 'cp', '-C', dirname, name]
                    popen = subprocess.Popen(cmd, stdout=subprocess.PIPE)
                    stream = NamedStream(popen.stdout, name, streamer='tar', extension='tar')
                else:
                    raise TransferError('Unsupported source %s' % src)

                name = os.path.basename(stream.name).strip('<>')
                streamer = stream.streamer
                extension = stream.extension

                if self.gzip:
                    if extension:
                        extension += '.gz'
                    else:
                        extension = 'gz'
                    stream = NamedStream(gzip_compressor(stream, self.use_pigz),
                                         stream.name, extension=extension, streamer=streamer)
                file_generator = split(stream, self._tmp_dir,
                                       chunk_size=self._chunk_size, extension=extension)

                uploaded_chunks = []

                # add file info to manifest
                file_info = {
                    'name': name,
                    'streamer': streamer,
                    'compressor': 'gzip' if self.gzip and not self._simple else '',
                    'chunks': uploaded_chunks,
                }
                self._manifest['files'].append(file_info)

                def on_chunk_complete(info):
                    self._on_file_complete(info)
                    if info['status'] == 'done':
                        data = (os.path.basename(info['src']), info['md5_sum'], info['size'])
                        bisect.insort(uploaded_chunks, data)
                    os.remove(info['src'])

                for file_info in file_generator:
                    dst = os.path.join(self.dst, file_info.name)
                    uploader.apply_async(file_info, dst,
                                         complete_cb=on_chunk_complete,
                                         progress_cb=self._on_progress)
                    while not self._semaphore.acquire(False):
                        time.sleep(DEFAULT_SLEEP_TIME)

                uploader.wait_completion()

                # add file info to manifest
                file_info = {
                    'name': name,
                    'streamer': streamer,
                    'compressor': 'gzip' if self.gzip and not self._simple else '',
                    'chunks': sorted(uploaded_chunks),
                }

            manifest_file = os.path.join(self._tmp_dir, self._manifest_name)
            self._manifest.write(manifest_file)

            manifest_url = os.path.join(self.dst, self._manifest_name)

            def on_manifest_complete(info):
                if info['status'] == 'error':
                    raise info['error'][0], info['error'][1], info['error'][2]
                os.remove(info['src'])

            uploader.apply_async(FileInfo(manifest_file), manifest_url,
                                 complete_cb=on_manifest_complete)

            uploader.wait_completion()
            uploader.stop()

            self._manifest_queue.put(self._manifest)
        except:
            uploader.stop(wait=False)
            raise

    def _check_pigz(self):
        try:
            pkgmgr.check_software(['pigz'])
        except pkgmgr.SoftwareError:
            pkgmgr.epel_repository()
            pkgmgr.installed('pigz', updatedb=True)

    def _run(self):
        if self._simple:
            self._simple_upload()
        else:
            self._large_upload()


class Download(Transfer):

    def __init__(self, src, dst=None, simple=False, use_pigz=True, pool_size=None,
                 progress_cb=None, cb_interval=None):
        """
        :type src: string
        :param src: manifest file url
        """
        super(Download, self).__init__(pool_size=pool_size, progress_cb=progress_cb,
                                       cb_interval=cb_interval)
        self._simple = simple
        self._read_fd = None
        self._write_fd = None
        self._manifest = Manifest()

        if self._simple and not hasattr(src, '__iter__'):
            self.src = [src]
            assert self.dst, 'For simple download you must specify dst param'
        else:
            self.src = src
        self.dst = dst
        self.use_pigz = use_pigz
        self.output = None

    def apply_async(self):
        assert not self.running
        if not self._simple:
            # create new pipe and output before each start
            self._read_fd, self._write_fd = os.pipe()
            self.output = os.fdopen(self._read_fd, 'rb')
        super(Download, self).apply_async()
        if not self._simple:
            # close write fd after fork
            os.close(self._write_fd)

    def _chunk_generator(self):
        """
        Download chunks from manifest file and yield them in sorted order
        """
        downloader = _Transfer('get', pool_size=self._pool_size)
        try:
            # step 1
            # download manifest
            local_manifest_file = os.path.join(self._tmp_dir, os.path.basename(self.src))

            def on_manifest_complete(info):
                if info['status'] == 'error':
                    raise info['error'][0], info['error'][1], info['error'][2]

            downloader.apply_async(FileInfo(self.src), self._tmp_dir,
                                   complete_cb=on_manifest_complete)
            downloader.wait_completion()
            self._manifest.read(local_manifest_file)

            # step 2
            # download chunks and yield them in right order
            yield_cntr = 0
            remote_dir = os.path.dirname(self.src)
            results = {}

            def on_chunk_complete(info):
                self._on_file_complete(info)
                if info['status'] == 'done':
                    if cryptotool.calculate_md5_sum(info['result']) != info['md5_sum']:
                        raise MD5SumError('md5 sum mismatch', info)
                    priority = int(os.path.basename(info['src'])[-3:])
                    results[priority] = os.path.join(info['dst'], os.path.basename(info['src']))

            for f in self._manifest['files']:
                for chunk_data in sorted(f['chunks']):
                    chunk_rem_path = os.path.join(remote_dir, chunk_data[0])
                    if len(chunk_data) == 3:
                        md5_sum, size = chunk_data[1], chunk_data[2]
                    else:
                        md5_sum, size = chunk_data[1], None
                    chunk = FileInfo(chunk_rem_path, md5_sum=md5_sum, size=size)
                    downloader.apply_async(chunk, self._tmp_dir, complete_cb=on_chunk_complete,
                                           progress_cb=self._on_progress)
                    while self._semaphore.acquire(False):
                        time.sleep(DEFAULT_SLEEP_TIME)
                    while yield_cntr in results:
                        yield results[yield_cntr], f['streamer'], f['compressor']
                        yield_cntr += 1

                while yield_cntr < len(f['chunks']):
                    while self._semaphore.acquire(False):
                        time.sleep(DEFAULT_SLEEP_TIME)
                    while yield_cntr in results:
                        yield results[yield_cntr], f['streamer'], f['compressor']
                        yield_cntr += 1
                    time.sleep(DEFAULT_SLEEP_TIME)

                results.clear()

            downloader.wait_completion()
            downloader.stop()
            raise StopIteration
        except:
            downloader.stop(wait=False)
            raise

    def _simple_download(self):
        downloader = _Transfer('get', pool_size=1)
        try:
            for src in self.src:
                downloader.apply_async(FileInfo(src), self.dst,
                                       complete_cb=self._on_file_complete,
                                       progress_cb=self._on_progress)
            downloader.wait_completion()
            downloader.stop()
        except:
            downloader.stop(wait=False)
            raise

    def _large_download(self):
        stdin = None
        stdout = os.fdopen(self._write_fd, 'wb')
        compressors = {}

        for chunk_path, streamer, compressor in self._chunk_generator():
            if compressor:
                # create compressor if it dosn't exist
                if compressor not in compressors:
                    if compressor == 'gzip':
                        if self.use_pigz:
                            cmd = ['/usr/bin/pigz', '-d']
                        else:
                            cmd = ['/bin/gzip', '-d']
                    # Add custom compressor where
                    # elif compressor == 'xxx':
                    #   cmd = []
                    else:
                        raise Exception('Unsupported compressor: %s' % compressor)
                    compressors[compressor] = subprocess.Popen(cmd,
                                                               stdin=subprocess.PIPE,
                                                               stdout=stdout,
                                                               stderr=subprocess.PIPE,
                                                               close_fds=True)
                stdin = compressors[compressor].stdin
            else:
                stdin = stdout

            util.write_file_to_stream(chunk_path, stdin)
            os.remove(chunk_path)

        if stdin:
            stdin.close()

        for compressor in compressors.values():
            compressor.wait()

    def _run(self):
        if self._simple:
            self._simple_download()
        else:
            self._large_download()


def gzip_compressor(stream, use_pigz=True):
    """
    gzip incoming stream
    """
    if use_pigz:
        cmd = ['/usr/bin/pigz', '-5']
    else:
        cmd = ['/bin/gzip', '-5']
    popen = subprocess.Popen(cmd,
                             stdin=stream,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE,
                             close_fds=True)
    return popen.stdout


def split(stream, storage_dir, chunk_size=None, extension=None):
    """
    Split incoming stream into chunks and save them on disk
    """
    chunk_size = (chunk_size or DEFAULT_CHUNK_SIZE) * 1024 * 1024

    if hasattr(stream, 'name'):
        name = os.path.basename(stream.name).strip('<>')
    else:
        name = 'stream-%s' % hash(stream)
    if extension:
        name += '.%s' % extension

    def read_size():
        while True:
            size = 4096
            for i in range(int(chunk_size / size)):
                yield size
            end = int(chunk_size - int(chunk_size / size) * size)
            if end:
                yield end
            raise StopIteration

    chunk_idx = 0

    while True:
        chunk_name = name + '.%03d' % chunk_idx
        md5_sum = hashlib.md5()

        chunk_path = os.path.join(storage_dir, chunk_name)

        eof = False
        with open(chunk_path, 'wb') as f:
            for size in read_size():
                data = stream.read(size)
                if not data:
                    eof = True
                    break
                f.write(data)
                md5_sum.update(data)

        file_info = FileInfo(chunk_path, md5_sum=md5_sum.hexdigest())
        yield file_info
        if eof:
            raise StopIteration
        else:
            chunk_idx += 1
