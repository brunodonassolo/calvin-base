# -*- coding: utf-8 -*-

# Copyright (c) 2015 Ericsson AB
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest
import time
import copy
import multiprocessing
import pytest
from collections import namedtuple
from calvin.requests.request_handler import RequestHandler, RT
from calvin.utilities.nodecontrol import dispatch_node, dispatch_storage_node
from calvin.utilities.attribute_resolver import format_index_string
import socket
import os
import json
from calvin.utilities import calvinlogger
from calvin.utilities import calvinconfig
from calvin.tests import helpers

_log = calvinlogger.get_logger(__name__)
_conf = calvinconfig.get()
request_handler = RequestHandler()

from calvin.tests.helpers import get_ip_addr
ip_addr = get_ip_addr()

rt1 = None
rt2 = None
rt3 = None
test_script_dir = None

deploy_attr = ['node', 'attr', 'script','reqs', 'check', 'credentials', 'signer', 'security_dir']
DeployArgsTuple = namedtuple('DeployArgs', deploy_attr)
def DeployArgs(**kwargs):
    deployargs = DeployArgsTuple(*[None]*len(deploy_attr))
    return deployargs._replace(**kwargs)

def absolute_filename(filename):
    import os.path
    return os.path.join(os.path.dirname(__file__), filename)

def assert_helper(runtimes, condition, times=5):
    def get_actors(runtimes):
        actors=[]
        for rt in runtimes:
            actors += request_handler.get_actors(rt)
        return actors
    from functools import partial
    rt_id = helpers.retry(times, partial(get_actors, runtimes), condition, "Failed to assert")

def verify_storage(runtimes):

    from functools import partial
    rt_ids = set()
    for i in runtimes:
        rt_id = helpers.retry(30, partial(request_handler.get_node_id, i), lambda _: True, "Failed to get node id")
        rt_ids.add(rt_id)

    print "RUNTIMES:", str(rt_ids)
    _log.analyze("TESTRUN", "+ IDS", {})

    # Try 30 times waiting for storage to be connected
    caps = []
    for rt in runtimes:
        failed = True
        for i in range(30):
            index = "node/capabilities/json" 
            for rt_id in rt_ids:
                if not (rt_id in caps):
                    caps = helpers.retry(30, partial(request_handler.get_index, rt, index, root_prefix_level=3), lambda _: True, "Failed to get index")['result']
            if rt_ids == set(caps):
                failed = False
                break
            else:
                time.sleep(0.1)
        assert not failed

@pytest.mark.slow
class TestDeployScript(unittest.TestCase):

    @pytest.fixture(autouse=True, scope="class")
    def setup(self, request):
        global rt1
        global rt2
        global rt3
        global test_script_dir
        import calvin.runtime.north.storage
        calvin.runtime.north.storage._conf.set('global', 'storage_type', 'local')
        rt1, _ = dispatch_node(["calvinip://%s:5000" % (ip_addr,)], "http://%s:5003" % ip_addr,
             attributes={'indexed_public':
                  {'owner':{'organization': 'org.testexample', 'personOrGroup': 'testOwner1'},
                   'node_name': {'organization': 'org.testexample', 'name': 'testNode1'},
                   'address': {'country': 'SE', 'locality': 'testCity', 'street': 'testStreet', 'streetNumber': 1}, 'cpuTotal': '100000', 'memTotal': '1G' }})
        helpers.wait_for_runtime(request_handler, rt1)
        calvin.runtime.north.storage._conf.set('global', 'storage_type', 'proxy')
        calvin.runtime.north.storage._conf.set('global', 'storage_proxy', "calvinip://%s:5000" % (ip_addr,))
        rt2, _ = dispatch_node(["calvinip://%s:5001" % (ip_addr,)], "http://%s:5004" % ip_addr,
             attributes={'indexed_public':
                  {'owner':{'organization': 'org.testexample', 'personOrGroup': 'testOwner1'},
                   'node_name': {'organization': 'org.testexample', 'name': 'testNode2'},
                   'address': {'country': 'SE', 'locality': 'testCity', 'street': 'testStreet', 'streetNumber': 1}}})
        helpers.wait_for_runtime(request_handler, rt2)
        rt3, _ = dispatch_node(["calvinip://%s:5002" % (ip_addr,)], "http://%s:5005" % ip_addr,
             attributes={'indexed_public':
                  {'owner':{'organization': 'org.testexample', 'personOrGroup': 'testOwner2'},
                   'node_name': {'organization': 'org.testexample', 'name': 'testNode3'},
                   'address': {'country': 'SE', 'locality': 'testCity', 'street': 'testStreet', 'streetNumber': 2}}})
        helpers.wait_for_runtime(request_handler, rt3)

        test_script_dir = absolute_filename('scripts/')
        request.addfinalizer(self.teardown)

        verify_storage([rt1, rt2, rt3])

    def teardown(self):
        global rt1
        global rt2
        global rt3
        helpers.teardown_test_type("local", [rt1, rt2, rt3], request_handler)

    def assert_storage(self):
        from functools import partial
        assert helpers.retry(30, partial(request_handler.get_index, rt2, format_index_string(['node_name', {'organization': 'org.testexample', 'name': 'testNode1'}])), lambda _: True, "Failed to get index")
        assert helpers.retry(30, partial(request_handler.get_index, rt3, format_index_string(['node_name', {'organization': 'org.testexample', 'name': 'testNode1'}])), lambda _: True, "Failed to get index")

        assert helpers.retry(30, partial(request_handler.get_index, rt1, format_index_string(['node_name', {'organization': 'org.testexample', 'name': 'testNode2'}])), lambda _: True, "Failed to get index")

        assert helpers.retry(30, partial(request_handler.get_index, rt3, format_index_string(['node_name', {'organization': 'org.testexample', 'name': 'testNode2'}])), lambda _: True, "Failed to get index")

        assert helpers.retry(30, partial(request_handler.get_index, rt1, format_index_string(['node_name', {'organization': 'org.testexample', 'name': 'testNode3'}])), lambda _: True, "Failed to get index")

        assert helpers.retry(30, partial(request_handler.get_index, rt2, format_index_string(['node_name', {'organization': 'org.testexample', 'name': 'testNode3'}])), lambda _: True, "Failed to get index")

    @pytest.mark.slow
    def testCpu(self):
        _log.analyze("TESTRUN", "+", {})

        from functools import partial
        helpers.retry(30, partial(request_handler.set_cpu_avail, rt1, 100), lambda _: True, "Failed to set cpu_avail")

        from calvin.Tools.cscontrol import control_deploy as deploy_app
        args = DeployArgs(node='http://%s:5003' % ip_addr,
                          script=open(test_script_dir+"test_cpu.calvin"), attr=None,
                                reqs=test_script_dir+"test_cpu.deployjson",
                                check=True)
        result = {}
        try:
            result = deploy_app(args)
        except:
            _log.exception("Test deploy failed")
            raise Exception("Failed deployment of app %s, no use to verify if requirements fulfilled" % args.script.name)

        # src -> rt1, sum, snk -> rt1 or rt2 or rt3
        assert_helper([rt1], lambda actors: result['actor_map']['test_cpu:src'] in actors)
        assert_helper([rt1, rt2, rt3], lambda actors: result['actor_map']['test_cpu:sum'] in actors)
        assert_helper([rt1, rt2, rt3], lambda actors: result['actor_map']['test_cpu:snk'] in actors)
        request_handler.delete_application(rt1, result['application_id'])

    @pytest.mark.slow
    def testNotSatisfiedCpu(self):
        _log.analyze("TESTRUN", "+", {})

        from functools import partial
        helpers.retry(30, partial(request_handler.set_cpu_avail, rt1, 99), lambda _: True, "Failed to set cpu_avail")

        from calvin.Tools.cscontrol import control_deploy as deploy_app
        args = DeployArgs(node='http://%s:5003' % ip_addr,
                          script=open(test_script_dir+"test_cpu.calvin"), attr=None,
                                reqs=test_script_dir+"test_cpu.deployjson",
                                check=True)
        result = {}
        try:
            result = deploy_app(args)
        except:
            _log.exception("Test deploy failed")
            raise Exception("Failed deployment of app %s, no use to verify if requirements fulfilled" % args.script.name)
        assert(result == None)


    @pytest.mark.slow
    def testRam(self):
        _log.analyze("TESTRUN", "+", {})

        from functools import partial
        helpers.retry(30, partial(request_handler.set_mem_avail, rt1, 10), lambda _: True, "Failed to set cpu_avail")

        from calvin.Tools.cscontrol import control_deploy as deploy_app
        args = DeployArgs(node='http://%s:5003' % ip_addr,
                          script=open(test_script_dir+"test_cpu.calvin"), attr=None,
                                reqs=test_script_dir+"test_ram.deployjson",
                                check=True)
        result = {}
        try:
            result = deploy_app(args)
        except:
            _log.exception("Test deploy failed")
            raise Exception("Failed deployment of app %s, no use to verify if requirements fulfilled" % args.script.name)

        # src -> rt1, sum, snk -> rt1 or rt2 or rt3
        assert_helper([rt1], lambda actors: result['actor_map']['test_cpu:src'] in actors)
        assert_helper([rt1, rt2, rt3], lambda actors: result['actor_map']['test_cpu:sum'] in actors)
        assert_helper([rt1, rt2, rt3], lambda actors: result['actor_map']['test_cpu:snk'] in actors)
        request_handler.delete_application(rt1, result['application_id'])

    @pytest.mark.slow
    def testNotSatisfiedRam(self):
        _log.analyze("TESTRUN", "+", {})

        from functools import partial
        helpers.retry(30, partial(request_handler.set_mem_avail, rt1, 9), lambda _: True, "Failed to set cpu_avail")

        from calvin.Tools.cscontrol import control_deploy as deploy_app
        args = DeployArgs(node='http://%s:5003' % ip_addr,
                          script=open(test_script_dir+"test_cpu.calvin"), attr=None,
                                reqs=test_script_dir+"test_ram.deployjson",
                                check=True)
        result = {}
        try:
            result = deploy_app(args)
        except:
            _log.exception("Test deploy failed")
            raise Exception("Failed deployment of app %s, no use to verify if requirements fulfilled" % args.script.name)
        assert(result == None)

