# -*- coding: utf-8 -*-

from calvin.utilities.calvin_callback import CalvinCB
from calvin.utilities import dynops
from . import all_nodes

req_type = "placement"

def get_node_resource(key, value, node_id, out_iter, kwargs):
    """
    Auxiliary method to verify if node is capable to answer client's request.
    """
    print "Node " + str(node_id)
    kwargs['counter'] -= 1

    if not value:
        print "No valid response. Ignoring..."
        return

    ok = True
    for res in kwargs['resource']:
        print "Analysing " + str(res)
        print " Requesting: " + str(kwargs['resource'][res])
        print " Available: " + str(value[res])
        if value[res] < kwargs['resource'][res]:
            ok = False
            break

    if ok:
        print "Node " + str(node_id) + ": added to list"
        out_iter.append(node_id)

def verify_node(out_iter, kwargs, final, value):
    """
    Verifies if node respects the resource requirements asked by client
    outer_iter: output iterable
    kwargs: (node, resource, counter):
     - node: Node structure, contains storage
     - resource:
     - counter: indicates how many callbacks we are waiting for responses.
    final: indicates the end of dynops.Map. Used together with counter variable to set when we
    have finished verifing the requirements.
    value: Node ID being verified.
    """
    if not final[0] and value != dynops.FailedElement:
        node = kwargs['node']
        kwargs['counter'] += 1
        node.storage.get(prefix='nodeMonitor-', key=value, cb=CalvinCB(get_node_resource, node_id=value, out_iter=out_iter, kwargs=kwargs))

    if final[0] and kwargs['counter'] == 0:
        print "Resource requirement match ended"
        out_iter.final()

def req_op(node, resource, actor_id=None, component=None):
    """ Lockup index returns a dynamic iterable which 
        actor_id is the actor that this is requested for
        component contains a list of all actor_ids of the component if the actor belongs to a component else None
    """
    nodes_list = all_nodes.req_op(node)
    filtered = dynops.Map(verify_node, nodes_list, eager=True, node=node, resource=resource, counter = 0)
    filtered.set_name("node_resource_min")
    return filtered
