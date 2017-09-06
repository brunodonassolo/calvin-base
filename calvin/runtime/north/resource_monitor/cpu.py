# -*- coding: utf-8 -*-

from calvin.runtime.south.plugins.async import async
from calvin.utilities.calvin_callback import CalvinCB

class CpuMonitor(object):
    def __init__(self, node_id, storage):
        self.storage = storage
        self.node_id = node_id
        self.acceptable_avail = [0, 25, 50, 75, 100]

    def _get_indexes(self, avail):
        """
        Auxiliary method to get indexes list
        """
        indexes = ['cpu', 'avail']
        indexes += map(str, self.acceptable_avail[:self.acceptable_avail.index(avail) + 1])
        return indexes

    def _set_avail_aux(self, key, value, new_value=None, node_id=None):
        """
        Auxiliary method to set indexes for CPU availability.
        Removes old indexes before adding the new ones. Triggered by a get in the database
        """
        if value is not None:
            print "Removing " + str(key) + " for CPU avail: " + str(value)
            self.storage.remove_index(index=self._get_indexes(value), value=key, root_prefix_level=2)
        if new_value is not None:
            print "After possible removal, adding new node " + str(node_id) + " for CPU avail: " + str(new_value)
            self.storage.add_index(index=self._get_indexes(new_value), value=node_id, root_prefix_level=2, cb=None)

    def set_avail(self, node_id, avail, cb=None):
        """
        Sets the CPU availability of a node.
        Acceptable range: [0, 25, 50, 75, 100]
        """
        if avail not in self.acceptable_avail:
            print "Invalid CPU avail value: " + str(avail)
            async.DelayedCall(0, cb, avail, False)
            return
        
        # get old value to cleanup indexes
        self.storage.get(prefix="nodeCpuAvail-", key=node_id, cb=CalvinCB(self._set_avail_aux, new_value=avail, node_id=node_id))

        self.storage.set(prefix="nodeCpuAvail-", key=node_id, value=avail, cb=None)
        if cb:
            async.DelayedCall(0, cb, avail, True)

    def stop(self):
        """
        Stops monitoring, cleaning storage
        """
        # get old value to cleanup indexes
        self.storage.get(prefix="nodeCpuAvail-", key=self.node_id, cb=CalvinCB(self._set_avail_aux))
        self.storage.delete(prefix="nodeCpuAvail-", key=self.node_id, cb=None)
