# -*- coding: utf-8 -*-

from calvin.runtime.south.plugins.async import async
from calvin.runtime.north.resource_monitor.helper import ResourceMonitorHelper
from calvin.utilities.calvinlogger import get_logger
from calvin.requests import calvinresponse

_log = get_logger(__name__)

def cpu_avail_discretizer(cpu):
    INTERVAL=25
    perc_rounded = INTERVAL * round((cpu)/INTERVAL)
    return int(perc_rounded)

def cpu_discretizer(cpu):
    cpu_keys = [1, 100, 1000, 10000, 100000]
    if cpu < 1:
        return 1
    if cpu >= 100000:
        return 100000
    idx = next(idx for idx, value in enumerate(cpu_keys) if int(value) > int(cpu))
    return cpu_keys[idx - 1]

class CpuMonitor(object):
    def __init__(self, node_id, storage, cpuTotal):
        self.storage = storage
        self.node_id = node_id
        self.acceptable_avail = [0, 25, 50, 75, 100]
        self.helper = ResourceMonitorHelper(storage)
        self.cpu_total = int(cpuTotal)

    def set_avail(self, avail, cb=None):
        """
        Sets the CPU availability of a node.
        Acceptable range: [0, 25, 50, 75, 100]
        """
        #if avail not in self.acceptable_avail:
        if avail < 0 or avail > 100:
            _log.error("Invalid CPU avail value: %s" % str(avail))
            if cb:
                async.DelayedCall(0, cb, avail, value=None)
            return

        self.helper.set(ident=self.node_id, prefix="nodeCpuAvail-", prefix_index="cpuAvail", value=avail, discretizer=cpu_avail_discretizer, cb=cb)

        self.helper.set(ident=self.node_id, prefix="nodeCpu-", prefix_index="cpu", value=int(avail*(self.cpu_total/100)), discretizer=cpu_discretizer, cb=cb)

    def stop(self):
        """
        Stops monitoring, cleaning storage
        """
        # get old value to cleanup indexes
        self.helper.set(self.node_id, "nodeCpuAvail-", "cpuAvail", value=None, discretizer=cpu_avail_discretizer, cb=None)
        self.helper.set(self.node_id, "nodeCpu-", "cpu", value=None, discretizer=cpu_discretizer, cb=None)
        self.storage.delete(prefix="nodeCpuAvail-", key=self.node_id, cb=None)
        self.storage.delete(prefix="nodeCpu-", key=self.node_id, cb=None)
