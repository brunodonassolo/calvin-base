# -*- coding: utf-8 -*-

from calvin.runtime.north import storage
from calvin.runtime.north.resource_monitor.cpu import CpuMonitor
from calvin.runtime.south.plugins.async import threads
from calvin.utilities.calvin_callback import CalvinCB
from calvin.tests.helpers_twisted import create_callback, wait_for
import calvin.tests
import pytest
import time

# So it skipps if we dont have twisted plugin
def _dummy_inline(*args):
    pass

if not hasattr(pytest, 'inlineCallbacks'):
    pytest.inlineCallbacks = _dummy_inline

@pytest.mark.essential
@pytest.mark.skipif(pytest.inlineCallbacks == _dummy_inline,
                    reason="No inline twisted plugin enabled, please use --twisted to py.test")


class TestCpuMonitor(object):
    CPUAVAIL_INDEX_BASE = ['node', 'resource', 'cpuAvail']

    @pytest.inlineCallbacks
    def setup(self):
        self.node = calvin.tests.TestNode(["127.0.0.1:5000"])
        self.storage = storage.Storage(self.node)
        self.cpu = CpuMonitor(self.node.id, self.storage)
        self.done = False
        yield threads.defer_to_thread(time.sleep, .01)

    @pytest.inlineCallbacks
    def teardown(self):
        yield threads.defer_to_thread(time.sleep, .001)

    def test_done(self):
        return self.done

    def cb(self, key, value):
        self.get_ans = value
        self.done = True

    @pytest.inlineCallbacks
    def test_avail_invalid(self):
        """
        Verify invalid values for CPU avail
        """
        for i in [-1, 40, 101]:
            self.done = False
            self.cpu.set_avail(i, CalvinCB(self.cb))
            yield wait_for(self.test_done)
            assert self.get_ans == False

    @pytest.inlineCallbacks
    def test_avail_valid(self):
        """
        Test valid values for CPU avail.
        Verify if storage is as expected
        """
        values = [0, 25, 50, 75, 100]
        for i in values:
            # verify set return
            self.done = False
            self.cpu.set_avail(i, CalvinCB(self.cb))
            yield wait_for(self.test_done)
            assert self.get_ans == True
            
            # verify nodeCpuAvail in DB
            self.done = False
            self.storage.get(prefix="nodeCpuAvail-", key=self.node.id, cb=CalvinCB(self.cb))
            yield wait_for(self.test_done)
            assert self.get_ans == i

            # verify index ok and present for level i
            self.done = False
            print "get " + str(self.CPUAVAIL_INDEX_BASE + map(str, values[:values.index(i)+1]))
            self.storage.get_index(index=self.CPUAVAIL_INDEX_BASE + map(str, values[:values.index(i)+1]), cb=CalvinCB(self.cb))
            yield wait_for(self.test_done)
            assert self.node.id in self.get_ans

    @pytest.inlineCallbacks
    def test_avail_change(self):
        """
        Verify if indexes are ok after a change in CPU avail.
        Old value must be erased from indexes
        """
        self.cpu.set_avail(50)
        self.cpu.set_avail(25, CalvinCB(self.cb))
        yield wait_for(self.test_done)
        assert self.get_ans == True

        # node id must not be present at level 50, only at 25
        self.done = False
        self.storage.get_index(index=self.CPUAVAIL_INDEX_BASE + ['0', '25', '50'], cb=CalvinCB(self.cb))
        yield wait_for(self.test_done)
        assert self.get_ans is None

    @pytest.inlineCallbacks
    def test_stop_node(self):
        """
        Verify if indexes are cleared after node stop
        Old value must be erased from indexes
        """
        self.cpu.set_avail(25, CalvinCB(self.cb))
        yield wait_for(self.test_done)
        assert self.get_ans == True

        self.done = False
        self.storage.get_index(index=self.CPUAVAIL_INDEX_BASE + ['0', '25'], cb=CalvinCB(self.cb))
        yield wait_for(self.test_done)
        assert self.node.id in self.get_ans

        self.cpu.stop()

        # nodeCpuAvail must not exist
        self.done = False
        self.storage.get(prefix="nodeCpuAvail-", key=self.node.id, cb=CalvinCB(self.cb))
        yield wait_for(self.test_done)
        assert self.get_ans is False

        # node id must not be present at level 25
        self.done = False
        self.storage.get_index(index=self.CPUAVAIL_INDEX_BASE + ['0', '25'], cb=CalvinCB(self.cb))
        yield wait_for(self.test_done)
        print self.get_ans
        assert self.get_ans is None

