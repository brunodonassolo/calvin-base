# -*- coding: utf-8 -*-

from calvin.runtime.south.async import async
from calvin.runtime.north.resource_monitor.helper import ResourceMonitorHelper
from calvin.utilities.calvinlogger import get_logger
from calvin.requests import calvinresponse

_log = get_logger(__name__)

RAM_VALUES = [1000, 100000, 1000000, 100000000, 1000000000, 10000000000]
RAM_ACCEPTABLE = ["1K", "100K", "1M", "100M", "1G", "10G"]

def ram_text2number(text):
    return RAM_VALUES[RAM_ACCEPTABLE.index(text)]

def memory_discretizer(memory):
    INTERVAL=25
    perc_rounded = INTERVAL * round((memory)/INTERVAL)
    return int(perc_rounded)

def ram_discretizer(ram):
    if ram < 1000:
        return 1000
    if ram >= 10000000000:
        return 10000000000
    idx = next(idx for idx, value in enumerate(RAM_VALUES) if value > int(ram))

    return RAM_VALUES[idx - 1]

class MemMonitor(object):
    def __init__(self, node_id, storage, ramTotal):
        self.storage = storage
        self.node_id = node_id
        self.acceptable_avail = [0, 25, 50, 75, 100]
        self.helper = ResourceMonitorHelper(storage)
        self.ram_total = ram_text2number(ramTotal)

    def set_avail(self, avail, cb=None):
        """
        Sets the RAM availability of a node.
        Acceptable range: [0, 25, 50, 75, 100]
        """
#        if avail not in self.acceptable_avail:
        if avail < 0 or avail > 100:
            _log.error("Invalid RAM avail value: " + str(avail))
            if cb:
                async.DelayedCall(0, cb, avail, value=None)
            return

        self.helper.set(ident=self.node_id, prefix="nodeMemAvail-", prefix_index="memAvail", value=avail, discretizer= memory_discretizer, cb=cb)

        self.helper.set(ident=self.node_id, prefix="nodeRam-", prefix_index="ram", value=int(avail*(self.ram_total/100)), discretizer=ram_discretizer, cb=cb)

    def stop(self):
        """
        Stops monitoring, cleaning storage
        """
        # get old value to cleanup indexes
        self.helper.set(self.node_id, "nodeMemAvail-", "memAvail", value=None, discretizer=memory_discretizer, cb=None)
        self.helper.set(self.node_id, "nodeRam-", "ram", value=None, discretizer=ram_discretizer, cb=None)
        self.storage.delete(prefix="nodeMemAvail-", key=self.node_id, cb=None)
        self.storage.delete(prefix="nodeRam-", key=self.node_id, cb=None)
