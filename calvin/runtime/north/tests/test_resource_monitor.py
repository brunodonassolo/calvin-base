# -*- coding: utf-8 -*-

from calvin.runtime.north import storage
from calvin.requests import calvinresponse
import calvin.utilities.calvinconfig
from calvin.runtime.north.resource_monitor.cpu import CpuMonitor, cpu_discretizer
from calvin.runtime.north.resource_monitor.memory import MemMonitor, ram_discretizer
from calvin.runtime.south.async import threads
from calvin.runtime.north.resource_monitor.link import LinkMonitor
from calvin.utilities.calvin_callback import CalvinCB
from calvin.utilities.attribute_resolver import AttributeResolver
from calvin.tests.helpers_twisted import create_callback, wait_for
import calvin.tests
import pytest
import time

_conf = calvin.utilities.calvinconfig.get()

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
    CPU_INDEX_BASE = ['node', 'resource', 'cpu']
    CPUTOTAL_INDEX_BASE = ['node', 'attribute', 'cpuTotal']

    @pytest.inlineCallbacks
    def setup(self):
        _conf.set('global', 'storage_type', 'local')
        self.node = calvin.tests.TestNode(["127.0.0.1:5000"])
        self.node.attributes = AttributeResolver({"indexed_public": {"cpuTotal": "1000000" }})
        self.storage = storage.Storage(self.node)
        self.cpu_total = 1000000
        self.cpu = CpuMonitor(self.node.id, self.storage, self.cpu_total)
        self.done = False
        self.storage.add_node(self.node)
        yield threads.defer_to_thread(time.sleep, .01)

    @pytest.inlineCallbacks
    def teardown(self):
        yield threads.defer_to_thread(time.sleep, .001)

    def _test_done(self):
        return self.done

    def cb(self, key, value):
        self.get_ans = value
        self.done = True

    def cb2(self, value):
        self.get_ans = value
        self.done = True

    @pytest.inlineCallbacks
    def test_avail_valid(self):
        """
        Test valid values for CPU avail.
        Verify if storage is as expected
        """
        values = [0, 25, 50, 75, 100]
        cpu_values = [1, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 3000, 6000, 10000, 100000]
        for i in values:
            # verify set return
            self.done = False
            self.cpu.set_avail(i, CalvinCB(self.cb))
            yield wait_for(self._test_done)
            assert isinstance(self.get_ans, calvinresponse.CalvinResponse) and self.get_ans == calvinresponse.OK

            # verify nodeCpuAvail in DB
            self.done = False
            self.storage.get(prefix="nodeCpuAvail-", key=self.node.id, cb=CalvinCB(self.cb))
            yield wait_for(self._test_done)
            assert self.get_ans == i

            # verify index ok and present until level i
            for j in range(0, cpu_values.index(cpu_discretizer(self.cpu_total*i/100))):
                self.done = False
                self.storage.get_index(index=self.CPU_INDEX_BASE + map(str, cpu_values[:j+1]), root_prefix_level=2, cb=CalvinCB(self.cb2))
                yield wait_for(self._test_done)
                assert self.node.id in self.get_ans

            # verify index ok and present until level i
            for j in range(0, values.index(i)):
                self.done = False
                self.storage.get_index(index=self.CPUAVAIL_INDEX_BASE + map(str, values[:j+1]), root_prefix_level=2, cb=CalvinCB(self.cb2))
                yield wait_for(self._test_done)
                assert self.node.id in self.get_ans

    @pytest.inlineCallbacks
    def test_avail_change(self):
        """
        Verify if indexes are ok after a change in CPU avail.
        Old value must be erased from indexes
        """
        self.cpu.set_avail(50)
        self.cpu.set_avail(25, CalvinCB(self.cb))
        yield wait_for(self._test_done)
        assert isinstance(self.get_ans, calvinresponse.CalvinResponse) and self.get_ans == calvinresponse.OK

        # node id must not be present at level 50, only at 25
        self.done = False
        self.storage.get_index(index=self.CPUAVAIL_INDEX_BASE + ['0', '25', '50'], root_prefix_level=2, cb=CalvinCB(self.cb2))
        yield wait_for(self._test_done)
        assert self.get_ans == []

    @pytest.inlineCallbacks
    def test_stop_node(self):
        """
        Verify if indexes are cleared after node stop
        Old value must be erased from indexes
        """
        self.cpu.set_avail(25, CalvinCB(self.cb))
        yield wait_for(self._test_done)
        assert isinstance(self.get_ans, calvinresponse.CalvinResponse) and self.get_ans == calvinresponse.OK

        self.done = False
        self.storage.get_index(index=self.CPUAVAIL_INDEX_BASE + ['0', '25'], root_prefix_level=2, cb=CalvinCB(self.cb2))
        yield wait_for(self._test_done)
        assert self.node.id in self.get_ans

        self.cpu.stop()
        self.storage.delete_node(self.node)

        # nodeCpuAvail must not exist
        self.done = False
        self.storage.get(prefix="nodeCpuAvail-", key=self.node.id, cb=CalvinCB(self.cb))
        yield wait_for(self._test_done)
        assert isinstance(self.get_ans, calvinresponse.CalvinResponse) and self.get_ans == calvinresponse.NOT_FOUND

        # node id must not be present at level 25
        self.done = False
        self.storage.get_index(index=self.CPUAVAIL_INDEX_BASE + ['0', '25'], root_prefix_level=2, cb=CalvinCB(self.cb2))
        yield wait_for(self._test_done)
        assert self.get_ans == []

        # node id must not be present
        self.done = False
        self.storage.get_index(index=self.CPU_INDEX_BASE + ['1'], root_prefix_level=2, cb=CalvinCB(self.cb2))
        yield wait_for(self._test_done)
        assert self.get_ans == []

        # no node in total indexes
        self.done = False
        self.storage.get_index(index=self.CPUTOTAL_INDEX_BASE + ['1'], root_prefix_level=2, cb=CalvinCB(self.cb2))
        yield wait_for(self._test_done)
        assert self.get_ans == []

    @pytest.inlineCallbacks
    def test_total_valid(self):
        """
        Test valid values for CPU power.
        Verify if storage is as expected
        """
        values = ["1", "100", "200", "300", "400", "500", "600", "700", "800", "900", "1000", "3000", "6000", "10000", "100000"]
        for i in values:
            # verify set return
            self.done = False
            self.node.attributes = AttributeResolver({"indexed_public": {"cpuTotal": i }})
            self.storage.add_node(self.node, cb=self.cb)
            yield wait_for(self._test_done)
            assert isinstance(self.get_ans, calvinresponse.CalvinResponse) and self.get_ans == calvinresponse.OK

            # verify index ok and present until level i
            for j in range(0, values.index(i)):
                self.done = False
                self.storage.get_index(index=self.CPUTOTAL_INDEX_BASE + map(str, values[:j+1]), root_prefix_level=2, cb=CalvinCB(self.cb2))
                yield wait_for(self._test_done)
                assert self.node.id in self.get_ans


class TestMemMonitor(object):
    MEMAVAIL_INDEX_BASE = ['node', 'resource', 'memAvail']
    RAM_INDEX_BASE = ['node', 'resource', 'ram']
    MEMTOTAL_INDEX_BASE = ['node', 'attribute', 'memTotal']

    @pytest.inlineCallbacks
    def setup(self):
        _conf.set('global', 'storage_type', 'local')
        self.node = calvin.tests.TestNode(["127.0.0.1:5000"])
        self.node.attributes = AttributeResolver({"indexed_public": {"memTotal": "10G" }})
        self.storage = storage.Storage(self.node)
        self.ram_total = 100000000
        self.mem = MemMonitor(self.node.id, self.storage, "100M")
        self.done = False
        self.storage.add_node(self.node)
        yield threads.defer_to_thread(time.sleep, .01)

    @pytest.inlineCallbacks
    def teardown(self):
        yield threads.defer_to_thread(time.sleep, .001)

    def _test_done(self):
        return self.done

    def cb(self, key, value):
        self.get_ans = value
        self.done = True

    def cb2(self, value):
        self.get_ans = value
        self.done = True

    @pytest.inlineCallbacks
    def test_avail_valid(self):
        """
        Test valid values for RAM avail.
        Verify if storage is as expected
        """
        values = [0, 25, 50, 75, 100]
        ram_values = [1000, 1000000, 100000000, 1000000000, 10000000000]
        for i in values:
            # verify set return
            self.done = False
            self.mem.set_avail(i, CalvinCB(self.cb))
            yield wait_for(self._test_done)
            assert isinstance(self.get_ans, calvinresponse.CalvinResponse) and self.get_ans == calvinresponse.OK

            # verify nodeMemAvail in DB
            self.done = False
            self.storage.get(prefix="nodeMemAvail-", key=self.node.id, cb=CalvinCB(self.cb))
            yield wait_for(self._test_done)
            assert self.get_ans == i

            # verify index ok and present until level i
            for j in range(0, ram_values.index(ram_discretizer(self.ram_total*i/100))):
                self.done = False
                self.storage.get_index(index=self.RAM_INDEX_BASE + map(str,ram_values[:j+1]), root_prefix_level=2, cb=CalvinCB(self.cb2))
                yield wait_for(self._test_done)
                assert self.node.id in self.get_ans

            # verify index ok and present until level i
            for j in range(0, values.index(i)):
                self.done = False
                self.storage.get_index(index=self.MEMAVAIL_INDEX_BASE + map(str, values[:j+1]), root_prefix_level=2, cb=CalvinCB(self.cb2))
                yield wait_for(self._test_done)
                assert self.node.id in self.get_ans

    @pytest.inlineCallbacks
    def test_avail_change(self):
        """
        Verify if indexes are ok after a change in RAM avail.
        Old value must be erased from indexes
        """
        self.mem.set_avail(50)
        self.mem.set_avail(25, CalvinCB(self.cb))
        yield wait_for(self._test_done)
        assert isinstance(self.get_ans, calvinresponse.CalvinResponse) and self.get_ans == calvinresponse.OK

        # node id must not be present at level 50, only at 25
        self.done = False
        self.storage.get_index(index=self.MEMAVAIL_INDEX_BASE + ['0', '25', '50'], root_prefix_level=2, cb=CalvinCB(self.cb2))
        yield wait_for(self._test_done)
        assert self.get_ans == []

    @pytest.inlineCallbacks
    def test_stop_node(self):
        """
        Verify if indexes are cleared after node stop
        Old value must be erased from indexes
        """
        self.mem.set_avail(25, CalvinCB(self.cb))
        yield wait_for(self._test_done)
        assert isinstance(self.get_ans, calvinresponse.CalvinResponse) and self.get_ans == calvinresponse.OK

        self.done = False
        self.storage.get_index(index=self.MEMAVAIL_INDEX_BASE + ['0', '25'], root_prefix_level=2, cb=CalvinCB(self.cb2))
        yield wait_for(self._test_done)
        assert self.node.id in self.get_ans

        self.mem.stop()
        self.storage.delete_node(self.node)

        # nodeMemAvail must not exist
        self.done = False
        self.storage.get(prefix="nodeMemAvail-", key=self.node.id, cb=CalvinCB(self.cb))
        yield wait_for(self._test_done)
        assert isinstance(self.get_ans, calvinresponse.CalvinResponse) and self.get_ans == calvinresponse.NOT_FOUND

        # node id must not be present at level 25
        self.done = False
        self.storage.get_index(index=self.MEMAVAIL_INDEX_BASE + ['0', '25'], root_prefix_level=2, cb=CalvinCB(self.cb2))
        yield wait_for(self._test_done)
        assert self.get_ans == []

        # no node in ram indexes
        self.done = False
        self.storage.get_index(index=self.RAM_INDEX_BASE + ['1000'], root_prefix_level=2, cb=CalvinCB(self.cb2))
        yield wait_for(self._test_done)
        assert self.get_ans == []

        # no node in total indexes
        self.done = False
        self.storage.get_index(index=self.MEMTOTAL_INDEX_BASE + ['1K'], root_prefix_level=2, cb=CalvinCB(self.cb2))
        yield wait_for(self._test_done)
        assert self.get_ans == []

    @pytest.inlineCallbacks
    def test_total_valid(self):
        """
        Test valid values for total RAM.
        Verify if storage is as expected
        """
        values = ["1K", "100K", "1M", "100M", "1G", "10G"]
        for i in values:
            # verify set return
            self.done = False
            self.node.attributes = AttributeResolver({"indexed_public": {"memTotal": i }})
            self.storage.add_node(self.node, cb=self.cb)
            yield wait_for(self._test_done)
            assert isinstance(self.get_ans, calvinresponse.CalvinResponse) and self.get_ans == calvinresponse.OK

            # verify index ok and present until level i
            for j in range(0, values.index(i)):
                self.done = False
                self.storage.get_index(index=self.MEMTOTAL_INDEX_BASE + map(str, values[:j+1]), root_prefix_level=2, cb=CalvinCB(self.cb2))
                yield wait_for(self._test_done)
                assert self.node.id in self.get_ans

class TestLinkMonitor(object):
    BANDWIDTH_INDEX_BASE = ['links', 'resource', 'bandwidth']
    LATENCY_INDEX_BASE = ['links', 'resource', 'latency']

    @pytest.inlineCallbacks
    def setup(self):
        self.node = calvin.tests.TestNode(["127.0.0.1:5000"])
        self.node2 = calvin.tests.TestNode(["127.0.0.1:5002"])
        self.storage = storage.Storage(self.node)
        self.link = LinkMonitor(self.node.id, self.storage)
        self.done = False
        self.storage.add_node(self.node)
        self.link_id = self.link._create_links_cb(key=None, value=None, rt=self.node2.id)
        yield threads.defer_to_thread(time.sleep, .01)

    @pytest.inlineCallbacks
    def teardown(self):
        yield threads.defer_to_thread(time.sleep, .001)

    def _test_done(self):
        return self.done

    def cb(self, key, value):
        self.get_ans = value
        self.done = True

    def cb2(self, value):
        self.get_ans = value
        self.done = True

    @pytest.inlineCallbacks
    @pytest.mark.skip
    def test_bandwidth_invalid(self):
        """
        Verify invalid values for bandwidth
        """
        for i in ['10K', '2G', '1T']:
            self.done = False
            self.link.set_bandwidth(self.node.id, self.node2.id, i, CalvinCB(self.cb))
            yield wait_for(self._test_done)
            assert isinstance(self.get_ans, calvinresponse.CalvinResponse) and self.get_ans == calvinresponse.INTERNAL_ERROR

    @pytest.inlineCallbacks
    def test_bandwidth_valid(self):
        """
        Test valid values for bandwidth.
        Verify if storage is as expected
        """
        values = [100, 1000, 10000, 100000, 1000000]
        values_str = ['100K', '1M', '10M', '100M', '1G']
        for i in values:
            # verify set return
            self.done = False
            self.link.set_bandwidth(self.node.id, self.node2.id, i, CalvinCB(self.cb))
            yield wait_for(self._test_done)
            assert isinstance(self.get_ans, calvinresponse.CalvinResponse) and self.get_ans == calvinresponse.OK

            # verify linkBandwidth in DB
            self.done = False
            self.storage.get(prefix="linkBandwidth-", key=self.link_id, cb=CalvinCB(self.cb))
            yield wait_for(self._test_done)
            assert self.get_ans == i

            # verify index ok and present until level i
            for j in range(0, values.index(i)):
                self.done = False
                self.storage.get_index(index=self.BANDWIDTH_INDEX_BASE + map(str, values_str[:j+1]), root_prefix_level=2, cb=CalvinCB(self.cb2))
                yield wait_for(self._test_done)
                assert self.link_id in self.get_ans

    @pytest.inlineCallbacks
    def test_bandwidth_change(self):
        """
        Verify if indexes are ok after a change in bandwidth.
        Old value must be erased from indexes
        """
        self.done = False
        self.link.set_bandwidth(self.node.id, self.node2.id, '100000', CalvinCB(self.cb))
        yield wait_for(self._test_done)
        assert isinstance(self.get_ans, calvinresponse.CalvinResponse) and self.get_ans == calvinresponse.OK
        self.done = False
        self.link.set_bandwidth(self.node.id, self.node2.id, '10000', CalvinCB(self.cb))
        yield wait_for(self._test_done)
        assert isinstance(self.get_ans, calvinresponse.CalvinResponse) and self.get_ans == calvinresponse.OK

        # node id must not be present at level 100M, only at 10M
        self.done = False
        self.storage.get_index(index=self.BANDWIDTH_INDEX_BASE + ['100K', '1M', '10M', '100M'], root_prefix_level=2, cb=CalvinCB(self.cb2))
        yield wait_for(self._test_done)
        assert self.get_ans == []

    @pytest.inlineCallbacks
    @pytest.mark.skip
    def test_latency_invalid(self):
        """
        Verify invalid values for latency
        """
        for i in ['10us', '2ms', '10s']:
            self.done = False
            self.link.set_latency(self.node.id, self.node2.id, i, CalvinCB(self.cb))
            yield wait_for(self._test_done)
            assert isinstance(self.get_ans, calvinresponse.CalvinResponse) and self.get_ans == calvinresponse.INTERNAL_ERROR

    @pytest.inlineCallbacks
    def test_latency_valid(self):
        """
        Test valid values for latency.
        Verify if storage is as expected
        """
        values = [1000000, 100000, 1000, 100, 1]
        values_str = ['1s', '100ms', '1ms', '100us', '1us']
        for i in values:
            # verify set return
            self.done = False
            self.link.set_latency(self.node.id, self.node2.id, i, CalvinCB(self.cb))
            yield wait_for(self._test_done)
            assert isinstance(self.get_ans, calvinresponse.CalvinResponse) and self.get_ans == calvinresponse.OK

            # verify linkLatency in DB
            self.done = False
            self.storage.get(prefix="linkLatency-", key=self.link_id, cb=CalvinCB(self.cb))
            yield wait_for(self._test_done)
            assert self.get_ans == i

            # verify index ok and present until level i
            for j in range(0, values.index(i)):
                self.done = False
                self.storage.get_index(index=self.LATENCY_INDEX_BASE + map(str, values_str[:j+1]), root_prefix_level=2, cb=CalvinCB(self.cb2))
                yield wait_for(self._test_done)
                assert self.link_id in self.get_ans

    @pytest.inlineCallbacks
    def test_latency_change(self):
        """
        Verify if indexes are ok after a change in latency.
        Old value must be erased from indexes
        """
        self.done = False
        self.link.set_latency(self.node.id, self.node2.id, '100000', CalvinCB(self.cb))
        yield wait_for(self._test_done)
        assert isinstance(self.get_ans, calvinresponse.CalvinResponse) and self.get_ans == calvinresponse.OK
        self.done = False
        self.link.set_latency(self.node.id, self.node2.id, '1000000', CalvinCB(self.cb))
        yield wait_for(self._test_done)
        assert isinstance(self.get_ans, calvinresponse.CalvinResponse) and self.get_ans == calvinresponse.OK

        # node id must not be present at level 100ms, only at 1ms
        self.done = False
        self.storage.get_index(index=self.LATENCY_INDEX_BASE + ['1s', '100ms'], root_prefix_level=2, cb=CalvinCB(self.cb2))
        yield wait_for(self._test_done)
        assert self.get_ans == []

    @pytest.inlineCallbacks
    def test_stop_node(self):
        """
        Verify if indexes are cleared after node stop
        Old value must be erased from indexes
        """
        self.link.set_bandwidth(self.node.id, self.node2.id, '1M', CalvinCB(self.cb))
        yield wait_for(self._test_done)
        assert isinstance(self.get_ans, calvinresponse.CalvinResponse) and self.get_ans == calvinresponse.OK

        self.done = False
        self.storage.get_index(index=self.BANDWIDTH_INDEX_BASE + ['100K', '1M'], root_prefix_level=2, cb=CalvinCB(self.cb2))
        yield wait_for(self._test_done)
        assert self.link_id in self.get_ans

        self.done = False
        self.link.set_latency(self.node.id, self.node2.id, '100ms', CalvinCB(self.cb))
        yield wait_for(self._test_done)
        assert isinstance(self.get_ans, calvinresponse.CalvinResponse) and self.get_ans == calvinresponse.OK

        self.done = False
        self.storage.get_index(index=self.LATENCY_INDEX_BASE + ['1s', '100ms'], root_prefix_level=2, cb=CalvinCB(self.cb2))
        yield wait_for(self._test_done)
        assert self.link_id in self.get_ans

        self.link.stop()
        self.storage.delete_node(self.node)
        yield threads.defer_to_thread(time.sleep, .01)

        # linkBandwidth- must not exist
        self.done = False
        self.storage.get(prefix="linkBandwidth-", key=self.link_id, cb=CalvinCB(self.cb))
        yield wait_for(self._test_done)
        assert isinstance(self.get_ans, calvinresponse.CalvinResponse) and self.get_ans == calvinresponse.NOT_FOUND

        # link id must not be present at level 100M
        self.done = False
        self.storage.get_index(index=self.BANDWIDTH_INDEX_BASE + ['1M', '100M'], root_prefix_level=2, cb=CalvinCB(self.cb2))
        yield wait_for(self._test_done)
        assert self.get_ans == []

        # no link in total indexes
        self.done = False
        self.storage.get_index(index=self.BANDWIDTH_INDEX_BASE + ['1M'], root_prefix_level=2, cb=CalvinCB(self.cb2))
        yield wait_for(self._test_done)
        assert self.get_ans == []

        # linkLatency- must not exist
        self.done = False
        self.storage.get(prefix="linkLatency-", key=self.link_id, cb=CalvinCB(self.cb))
        yield wait_for(self._test_done)
        assert isinstance(self.get_ans, calvinresponse.CalvinResponse) and self.get_ans == calvinresponse.NOT_FOUND

        # link id must not be present at level 100M
        self.done = False
        self.storage.get_index(index=self.LATENCY_INDEX_BASE + ['1us', '100us'], root_prefix_level=2, cb=CalvinCB(self.cb2))
        yield wait_for(self._test_done)
        assert self.get_ans == []
