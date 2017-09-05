# -*- coding: utf-8 -*-

from calvin.runtime.north import storage
from calvin.runtime.north.monitoring.monitor import ResourceMonitor
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

class TestMonitoring(object):

    @pytest.inlineCallbacks
    def setup_class(self):
        self.node = calvin.tests.TestNode(["127.0.0.1:5000"])
        self.storage = storage.Storage(self.node)
        ResourceMonitor(self.node.id, self.storage)
        yield threads.defer_to_thread(time.sleep, .01)

    @pytest.inlineCallbacks
    def teardown_class(self):
        yield threads.defer_to_thread(time.sleep, .001)

    @pytest.inlineCallbacks
    def test_initial_values(self):
        self.get_ans = {}

        def test_done():
            return self.get_ans

        def cb(key, value):
           self.get_ans = value

        self.storage.get(prefix="nodeMonitor-", key=self.node.id, cb=CalvinCB(func=cb))
        yield wait_for(test_done)

        print "Get monitoring: " + str(self.get_ans)
        assert self.get_ans['memory_total'] != 0
        assert self.get_ans['memory_avail'] != 0
        assert self.get_ans['cpu_number'] != 0
        assert self.get_ans['cpu_freq'] != 0
        assert self.get_ans['cpu_usage'] != 0

