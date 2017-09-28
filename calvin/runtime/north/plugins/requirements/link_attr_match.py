# -*- coding: utf-8 -*-

from calvin.utilities.calvin_callback import CalvinCB
from calvin.utilities.attribute_resolver import format_index_string
from calvin.utilities import dynops

req_type = "placement"

def req_op(node, index, actor_id=None, component=None):
    """ Lockup index returns a dynamic iterable which 
        actor_id is the actor that this is requested for
        component contains a list of all actor_ids of the component if the actor belongs to a component else None
    """
    print "LINK_ATTR_MATCH"
    print index
    index_str = format_index_string(index)
    print index_str
    it = node.storage.get_index_iter(index_str)
    it.set_name("attr_match")
    return it
