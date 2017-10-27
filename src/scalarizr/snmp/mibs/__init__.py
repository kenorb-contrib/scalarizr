from __future__ import with_statement
from pyasn1.type.constraint import ValueRangeConstraint
 
def validate(syntax, value, clone = True):
    value = int(value)
    spec = syntax.subtypeSpec
 
    range_constr = filter(lambda o: isinstance(o, ValueRangeConstraint), spec.getValueMap().keys())[0]
    stop = range_constr.stop
 
    if stop < value:
        return syntax.clone(value % stop) if clone else value % stop
    return syntax.clone(value) if clone else value
 
