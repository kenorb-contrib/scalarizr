import logging
 
logging.basicConfig(
                stream=open('/var/log/scalarizr.lettuce.log', 'w'),
                level=logging.DEBUG)
 
 
# convenience functions
# should be moved to commons.py if we'll have one
# terrain problem: imports automatically only if test were run from this dir
 
from lettuce import world
from lettuce.core import Feature, Scenario
 
 
class ThisFeatureOnly(object):
    """
    Usage:
            from lettuce import world
            this_feature_only = world.ThisFeatureOnly("your feature name")
 
            @before.each_scenario
            @this_feature_only
            def setup(scenario):
                    ...
 
            Please use 'this_feature_only' name for readability.
 
    Can be used with:
            @before.each_scenario
            @after.each_scenario
            @before.each_feature
            @after.each_feature
    """
 
    def __init__(self, feature):
        self.feature = feature
 
    def __call__(self, f):
        def wrapper(arg):
            if isinstance(arg, Scenario):
                feature_name = arg.feature.name
            elif isinstance(arg, Feature):
                feature_name = arg.name
            else:
                raise Exception("this_feature_only is supposed to decorate only "
                                                "before/after each feature/scenario functions that"
                                                " accept one argument of type Feature or Scenario")
            if feature_name == self.feature:
                return f(arg)
        return wrapper
 
world.absorb(ThisFeatureOnly)
 
