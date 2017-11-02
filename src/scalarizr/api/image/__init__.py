

class ImageAPIDelegate(object):

    def prepare(self, operation, name):
        raise NotImplementedError()

    def snapshot(self, operation, name):
        raise NotImplementedError()

    def finalize(self, operation, name):
        raise NotImplementedError()


class ImageAPIError(BaseException):
    pass
