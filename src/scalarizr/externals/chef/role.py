from __future__ import with_statement
from scalarizr.externals.chef.base import ChefObject

class Role(ChefObject):
    """A Chef role object."""

    url = '/roles'
    attributes = {
        'description': str,
        'run_list': list,
        'default_attributes': dict,
        'override_attributes': dict,
    }
