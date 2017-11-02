import sys
import urlparse
import os


class DriverError(Exception):
    pass


def raises(exc_class):
    """
    Catches all exceptions from the underlying function, raises *exc_class*
    instead.

    .. code-block:: python

        @raises(MyError)
        def func():
            raise Exception(message)

        func()  # raises MyError(message)
    """

    def decorator(f):
        def wrapper(*args, **kwargs):
            try:
                return f(*args, **kwargs)
            except:
                exc = sys.exc_info()
                raise exc_class, exc[1], exc[2]
        return wrapper
    return decorator


def decorate_public_methods(decorator):
    """
    An easy way to decorate all methods of a class and it's descendants with
    the same decorator. The two following examples are equal:

    .. code-block:: python

        class Foo(object):

            __metaclass__ = decorate_public_methods(decorator)

            def foo(self):
                pass


        class Bar(Foo):

            def bar(self):
                pass

    .. code-block:: python

        class Foo(object):

            @decorator
            def foo(self):
                pass


        class Bar(Foo):

            @decorator
            def bar(self):
                pass
    """

    class DecoratePublicMethods(type):

        def __init__(self, name, bases, dic):
            super(DecoratePublicMethods, self).__init__(name, bases, dic)

            for key, val in dic.iteritems():
                if not key.startswith('_') and callable(val):
                    setattr(self, key, decorator(val))

    return DecoratePublicMethods


class CloudFileSystem(object):

    __metaclass__ = decorate_public_methods(raises(DriverError))

    schema = None
    features = {
            'multipart': False
    }

    def _parse_url(self, url):
        """
        :returns: bucket, key
        """
        o = urlparse.urlparse(url)
        assert o.scheme == self.schema, 'Wrong schema: %s' % o.scheme
        return o.netloc, o.path[1:]

    def _format_url(self, bucket, key):
        return '%s://%s/%s' % (self.schema, bucket, key)

    def exists(self, url):
        parent = os.path.dirname(url.rstrip('/'))
        # NOTE: s3 & gcs driver converts bucket names to lowercase while url
        # arg in this method stays uncoverted -> url with uppercase bucket
        # name will never be found
        return url in self.ls(parent)

    def ls(self, url):
        raise NotImplementedError()

    def stat(self, url):
        '''
        size in bytes
        type = dir | file | container
        '''
        raise NotImplementedError()

    def put(self, src, url, report_to=None):
        raise NotImplementedError()

    def get(self, url, dst, report_to=None):
        raise NotImplementedError()

    def delete(self, url):
        raise NotImplementedError()


    def multipart_init(self, path, part_size):
        '''
        :returns: upload_id
        '''
        raise NotImplementedError()

    def multipart_put(self, upload_id, src):
        raise NotImplementedError()

    def multipart_complete(self, upload_id):
        raise NotImplementedError()

    def multipart_abort(self, upload_id):
        raise NotImplementedError()
