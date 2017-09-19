# -*- coding: utf-8 -*-

from calvin.runtime.south.plugins.async import async
from calvin.runtime.north.resource_monitor.helper import ResourceMonitorHelper
from calvin.utilities.calvinlogger import get_logger

_log = get_logger(__name__)

class CpuMonitor(object):
    def __init__(self, node_id, storage):
        self.storage = storage
        self.node_id = node_id
        self.acceptable_avail = [0, 25, 50, 75, 100]
        self.helper = ResourceMonitorHelper(storage)

    def set_avail(self, avail, cb=None):
        """
        Sets the CPU availability of a node.
        Acceptable range: [0, 25, 50, 75, 100]
        """
        if avail not in self.acceptable_avail:
            _log.error("Invalid CPU avail value: %s" % str(avail))
            if cb:
                async.DelayedCall(0, cb, avail, False)
            return

        self.helper.set(ident=self.node_id, prefix="nodeCpuAvail-", prefix_index="cpuAvail", value=avail, cb=cb)

    def stop(self):
        """
        Stops monitoring, cleaning storage
        """
        # get old value to cleanup indexes
        self.helper.set(self.node_id, "nodeCpuAvail-", "cpuAvail", value=None, cb=None)
        self.storage.delete(prefix="nodeCpuAvail-", key=self.node_id, cb=None)
