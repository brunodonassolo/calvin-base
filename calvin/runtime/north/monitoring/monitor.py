# -*- coding: utf-8 -*-

import psutil
from calvin.runtime.south.plugins.async import async
from calvin.utilities.calvin_callback import CalvinCB

class ResourceMonitor(object):
    def __init__ (self, node_id, storage, period = 60):
        self.memory_total = 0
        self.memory_avail = 0
        self._node_id = node_id
        self._storage = storage
        self._period = period
        async.DelayedCall(0, self.read)

    def read_memory(self):
        mem = psutil.virtual_memory()
        self.memory_total = mem.total
        self.memory_avail = mem.available
        return { "memory_total": self.memory_total,
                 "memory_avail": self.memory_avail }

    def read_cpu(self):
        freq = psutil.cpu_freq()
        count = psutil.cpu_count()
        usage = psutil.cpu_percent(interval=None)
        self.cpu_freq = freq.max
        self.cpu_number = count
        self.cpu_usage = usage
        return { "cpu_freq": self.cpu_freq,
                 "cpu_number": self.cpu_number,
                 "cpu_usage": self.cpu_usage }

    def read(self):
        value = {}
        value.update(self.read_memory())
        value.update(self.read_cpu())
        print "Monitoring, update: " + str(value)
        self._storage.set_monitoring(self._node_id, value)
        timeout = async.DelayedCall(self._period, self.read)

