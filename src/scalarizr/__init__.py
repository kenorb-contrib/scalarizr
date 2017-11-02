import os
import sys
__version__ = open(os.path.join(os.path.dirname(__file__), 'version')).read().strip()


class ScalarizrModule(object):
    def __init__(self, module):
        self.module = module

    def __getattr__(self, name):
        if name == 'WindowsService':
            from scalarizr import app
            return app.WindowsService
        else:
            return getattr(self.module, name)


sys.modules[__name__] = ScalarizrModule(sys.modules[__name__])
