# -*- coding: utf-8 -*-

from calvin.runtime.south.async import async
from calvin.utilities.calvin_callback import CalvinCB
from calvin.utilities.attribute_resolver import AttributeResolver
from calvin.utilities.calvinlogger import get_logger
from calvin.requests import calvinresponse

_log = get_logger(__name__)

class ResourceMonitorHelper(object):
    def __init__(self, storage):
        self.storage = storage

    def _set_aux(self, key, value, prefix_index, ident, discretizer, new_value=None):
        """
        Auxiliary method to set indexes .
        Removes old indexes before adding the new ones. Triggered by a get in the database
        """
        # if new value is exactly the same, we don't need to change anything..
        if value is new_value:
            _log.debug("%s, value: %s. Nothing changed, just return.." % (prefix_index, value))
            return

        if value is not None and value != calvinresponse.NOT_FOUND:
            value = discretizer(float(value))
        if new_value is not None:
            new_value = discretizer(float(new_value))

        # erase indexes related to old value
        if value is not None and value != calvinresponse.NOT_FOUND:
            old_data = AttributeResolver({"indexed_public": {prefix_index: str(value)}})
            _log.debug("Removing " + str(key) + " for " + prefix_index + ": " + str(value))
            for index in old_data.get_indexed_public():
                self.storage.remove_index(index=index, value=key, root_prefix_level=2)

        # insert the new ones
        if new_value is not None:
            new_data = AttributeResolver({"indexed_public": {prefix_index: str(new_value)}})
            _log.debug("After possible removal, adding new node " + str(ident) + " for " + prefix_index + ": " + str(new_value))
            for index in new_data.get_indexed_public():
                self.storage.add_index(index=index, value=ident, root_prefix_level=2, cb=None)

    def _set_aux2(self, key, value, prefix_get, prefix_index, ident, discretizer, force, new_value=None):

        if not force and value is not None and value == "true":
            return

        # get old value to cleanup indexes
        self.storage.get(prefix=prefix_get, key=ident, cb=CalvinCB(self._set_aux,
            ident=ident, prefix_index=prefix_index, discretizer = discretizer, new_value=new_value))

        self.storage.set(prefix=prefix_get, key=ident, value=new_value, cb=None)


    def set(self, ident, prefix, prefix_index, value, discretizer, force=False, cb=None):
        """
        Sets a certain resource of a node.
        Gets the old value to erase from indexes.
        Parameters:
        prefix: String used in storage for attribute, e.g. nodeCpuAvail.
        prefix_index: String used in indexed_public structure for this field, e.g. cpuAvail.
        value: new value to set.
        cb: callback to receive response. Signature: cb(value, True/False)
        """

        self.storage.get(prefix="", key="batch", cb=CalvinCB(self._set_aux2, ident=ident, prefix_get=prefix, prefix_index=prefix_index, discretizer = discretizer, new_value=value, force=force))

        if cb:
            async.DelayedCall(0, cb, value, value=calvinresponse.CalvinResponse(True))

