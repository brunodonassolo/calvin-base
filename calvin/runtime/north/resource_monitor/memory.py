# -*- coding: utf-8 -*-

from calvin.runtime.south.async import async
from calvin.runtime.north.resource_monitor.helper import ResourceMonitorHelper
from calvin.utilities.attribute_resolver import AttributeResolver
from calvin.utilities.calvinlogger import get_logger
from calvin.requests import calvinresponse
from calvin.utilities.calvin_callback import CalvinCB

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
        self.storage.set(prefix="nodeMemTotal-", key=node_id, value=self.ram_total, cb=None)
        self.set_avail(avail=100)

    def _cb_mem_total(self, key, value, avail_bytes, node_id):
        avail = (avail_bytes/value)*100

        if avail < 0:
            avail = 0
        if avail > 100:
            avail = 100

        self.helper.set(ident=node_id, prefix="nodeMemAvail-", prefix_index="memAvail", value=avail, discretizer= memory_discretizer, cb=None, force=True)

        self.helper.set(ident=node_id, prefix="nodeRam-", prefix_index="ram", value=avail_bytes, discretizer=ram_discretizer, cb=None, force=True)


    def set_avail_for_node(self, avail_bytes, node_id):
        self.storage.get(prefix="nodeMemTotal-", key=node_id, cb=CalvinCB(self._cb_mem_total, avail_bytes=avail_bytes, node_id=node_id))

    def set_avail(self, avail, cb=None):
        """
        Sets the RAM availability of a node.
        Acceptable range: [0, 25, 50, 75, 100]
        """
        if avail < 0:
            avail = 0
        if avail > 100:
            avail = 100

        self.helper.set(ident=self.node_id, prefix="nodeMemAvail-", prefix_index="memAvail", value=avail, discretizer= memory_discretizer, cb=None)

        ram = int(avail*(self.ram_total/100))
        self.helper.set(ident=self.node_id, prefix="nodeRam-", prefix_index="ram", value=ram, discretizer=ram_discretizer, cb=cb)

        #adding runtime for ramRaw, independently of available RAM
        new_data = AttributeResolver({"indexed_public": {"ramRaw": "1000"}})
        for index in new_data.get_indexed_public():
            self.storage.add_index(index=index, value=self.node_id, root_prefix_level=2, cb=None)

        _log.info("Update RAM, node: %s CPU: %s, avail: %d, total: %d" % (self.node_id, ram, avail, self.ram_total))

    def _stop(self, key, value):
        self.storage.delete(prefix="nodeMemAvail-", key=self.node_id, cb=None)
        self.storage.delete(prefix="nodeRam-", key=self.node_id, cb=None)

    def stop(self):
        """
        Stops monitoring, cleaning storage
        """
        # get old value to cleanup indexes
        self.helper.set(self.node_id, "nodeMemAvail-", "memAvail", value=None, discretizer=memory_discretizer, cb=None)
        self.helper.set(self.node_id, "nodeRam-", "ram", value=None, discretizer=ram_discretizer, cb=CalvinCB(self._stop))

        new_data = AttributeResolver({"indexed_public": {"ramRaw": "1000"}})
        for index in new_data.get_indexed_public():
            self.storage.remove_index(index=index, value=self.node_id, root_prefix_level=2, cb=None)
