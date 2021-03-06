import json
from calvin.utilities.calvinlogger import get_logger
from calvin.utilities.calvin_callback import CalvinCB
import calvin.requests.calvinresponse as response
from calvin.utilities.attribute_resolver import AttributeResolver
from calvin.csparser.port_property_syntax import list_port_property_capabilities

_log = get_logger(__name__)

def set_proxy_config_cb(key, value, will_sleep, link, callback):
    if not value:
        callback(status=response.CalvinResponse(response.INTERNAL_ERROR, {'peer_node_id': key}))
        return
    callback(status=response.CalvinResponse(response.OK))
    if will_sleep:
        link.set_peer_insleep()

def set_proxy_config(peer_id, capabilities, port_property_capability, will_sleep, link, storage, callback, attributes):
    """
    Store node
    """
    try:
        for c in list_port_property_capabilities(which=port_property_capability):
            storage.add_index(['node', 'capabilities', c], peer_id, root_prefix_level=3)
        for c in capabilities:
            storage.add_index(['node', 'capabilities', c], peer_id, root_prefix_level=3)
    except:
        _log.error("Failed to set capabilities")

    public = None
    indexed_public = None

    if attributes is not None:
        attributes = json.loads(attributes)
        attributes = AttributeResolver(attributes)
        indexes = attributes.get_indexed_public()
        for index in indexes:
            storage.add_index(index, peer_id)
        public = attributes.get_public()
        indexed_public = attributes.get_indexed_public(as_list=False)

    storage.set(prefix="node-", key=peer_id,
                value={"proxy": storage.node.id,
                "uris": None,
                "control_uris": None,
                "authz_server": None, # Set correct value
                "sleeping": will_sleep,
                "attributes": {'public': public,
                'indexed_public': indexed_public}},
                cb=CalvinCB(set_proxy_config_cb, will_sleep=will_sleep, link=link, callback=callback))
