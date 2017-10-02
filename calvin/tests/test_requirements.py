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
rt1_id = None
rt2_id = None
rt3_id = None
test_script_dir = None
TEST_TIMEOUT=15

deploy_attr = ['node', 'attr', 'script','reqs', 'check', 'credentials', 'signer', 'security_dir', 'timeout']
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
                    caps = helpers.retry(30, partial(request_handler.get_index, rt, index), lambda _: True, "Failed to get index")['result']
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
        rt1, _ = dispatch_node(["calvinip://%s:5000" % (ip_addr,)], "http://%s:5003" % ip_addr,
             attributes={'indexed_public':
                  {'owner':{'organization': 'org.testexample', 'personOrGroup': 'testOwner1'},
                   'node_name': {'organization': 'org.testexample', 'name': 'testNode1'},
                   'address': {'country': 'SE', 'locality': 'testCity', 'street': 'testStreet', 'streetNumber': 1}}})
        helpers.wait_for_runtime(request_handler, rt1)
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
    def testDeploySimple(self):
        _log.analyze("TESTRUN", "+", {})
        verify_storage([rt1, rt2, rt3])
        self.assert_storage()
        
        from calvin.Tools.cscontrol import control_deploy as deploy_app
        args = DeployArgs(node='http://%s:5003' % ip_addr,
                          script=open(test_script_dir+"test_deploy1.calvin"), attr=None,
                                reqs=test_script_dir+"test_deploy1.deployjson",
                                check=True, timeout=TEST_TIMEOUT)
        result = {}
        try:
            result = deploy_app(args)
        except:
            _log.exception("Test deploy failed")
            raise Exception("Failed deployment of app %s, no use to verify if requirements fulfilled" % args.script.name)
        # src -> rt2, sum -> rt2, snk -> rt3
        assert_helper([rt2], lambda actors: result['actor_map']['test_deploy1:src'] in actors)
        assert_helper([rt2], lambda actors: result['actor_map']['test_deploy1:sum'] in actors)
        assert_helper([rt3], lambda actors: result['actor_map']['test_deploy1:snk'] in actors)
        request_handler.delete_application(rt1, result['application_id'])

    @pytest.mark.slow
    def testDeployLongActorChain(self):
        _log.analyze("TESTRUN", "+", {})
        verify_storage([rt1, rt2, rt3])
        self.assert_storage()

        from calvin.Tools.cscontrol import control_deploy as deploy_app
        args = DeployArgs(node='http://%s:5003' % ip_addr,
                          script=open(test_script_dir+"test_deploy2.calvin"), attr=None,
                                reqs=test_script_dir+"test_deploy2.deployjson",
                                check=True, timeout=TEST_TIMEOUT)
        result = {}
        try:
            result = deploy_app(args)
        except:
            _log.exception("Test deploy failed")
            raise Exception("Failed deployment of app %s, no use to verify if requirements fulfilled" % args.script.name)
        # src -> rt1, sum[1:8] -> [rt1, rt2, rt3], snk -> rt3
        assert_helper([rt1], lambda actors: result['actor_map']['test_deploy2:src'] in actors)
        assert_helper([rt3], lambda actors: result['actor_map']['test_deploy2:snk'] in actors)
        actors = [request_handler.get_actors(rt1), request_handler.get_actors(rt2), request_handler.get_actors(rt3)]
        sum_list=[result['actor_map']['test_deploy2:sum%d'%i] for i in range(1,9)]
        sum_place = [0 if a in actors[0] else 1 if a in actors[1] else 2 if a in actors[2] else -1 for a in sum_list]
        assert not any([p==-1 for p in sum_place])
        assert all(x<=y for x, y in zip(sum_place, sum_place[1:]))
        request_handler.delete_application(rt1, result['application_id'])

    @pytest.mark.slow
    def testDeployComponent(self):
        _log.analyze("TESTRUN", "+", {})
        verify_storage([rt1, rt2, rt3])
        self.assert_storage()

        from calvin.Tools.cscontrol import control_deploy as deploy_app
        args = DeployArgs(node='http://%s:5003' % ip_addr,
                          script=open(test_script_dir+"test_deploy3.calvin"), attr=None,
                                reqs=test_script_dir+"test_deploy3.deployjson",
                                check=True, timeout=TEST_TIMEOUT)
        result = {}
        try:
            result = deploy_app(args)
        except:
            _log.exception("Test deploy failed")
            raise Exception("Failed deployment of app %s, no use to verify if requirements fulfilled" % args.script.name)
        # src:(first, second) -> rt1, sum -> rt2, snk -> rt3
        assert_helper([rt1], lambda actors: result['actor_map']['test_deploy3:src:first'] in actors)
        assert_helper([rt1], lambda actors: result['actor_map']['test_deploy3:src:second'] in actors)
        assert_helper([rt2], lambda actors: result['actor_map']['test_deploy3:sum'] in actors)
        assert_helper([rt3], lambda actors: result['actor_map']['test_deploy3:snk'] in actors)
        request_handler.delete_application(rt1, result['application_id'])

@pytest.mark.slow
class TestDeployment3NodesProxyStorage(unittest.TestCase):

    @pytest.fixture(autouse=True, scope="class")
    def setup(self, request):
        from calvin.Tools.csruntime import csruntime
        from conftest import _config_pytest
        global rt1
        global rt2
        global rt3
        global test_script_dir
        use_proxy_storage = True

        rt1_conf = copy.deepcopy(_conf)
        rt1_conf.set('global', 'capabilities_blacklist', ['calvinsys.events.timer'])
        if use_proxy_storage:
            rt1_conf.set('global', 'storage_type', 'local')
        rt1_conf.save("/tmp/calvin5000.conf")
        try:
            logfile = _config_pytest.getoption("logfile")+"5000"
            outfile = os.path.join(os.path.dirname(logfile), os.path.basename(logfile).replace("log", "out"))
            if outfile == logfile:
                outfile = None
        except:
            logfile = None
            outfile = None
        csruntime(ip_addr, port=5000, controlport=5003, attr={'indexed_public':
                  {'node_name': {'name': 'display'}}},
                   loglevel=_config_pytest.getoption("loglevel"), logfile=logfile, outfile=outfile,
                   configfile="/tmp/calvin5000.conf")
        rt1 = RT("http://%s:5003" % ip_addr)
        helpers.wait_for_runtime(request_handler, rt1)

        rt2_3_conf = copy.deepcopy(_conf)
        if use_proxy_storage:
            rt2_3_conf.set('global', 'storage_type', 'proxy')
            rt2_3_conf.set('global', 'storage_proxy', "calvinip://%s:5000" % ip_addr)
        rt2_3_conf.save("/tmp/calvin5001.conf")
        try:
            logfile = _config_pytest.getoption("logfile")+"5001"
            outfile = os.path.join(os.path.dirname(logfile), os.path.basename(logfile).replace("log", "out"))
            if outfile == logfile:
                outfile = None
        except:
            logfile = None
            outfile = None
        csruntime(ip_addr, port=5001, controlport=5004, attr={'indexed_public':
                  {'node_name': {'name': 'serv'},
                   'address': {"locality" : "outside"}}},
                   loglevel=_config_pytest.getoption("loglevel"), logfile=logfile, outfile=outfile,
                   configfile="/tmp/calvin5001.conf")
        rt2 = RT("http://%s:5004" % ip_addr)
        helpers.wait_for_runtime(request_handler, rt2)

        rt2_3_conf.save("/tmp/calvin5002.conf")
        try:
            logfile = _config_pytest.getoption("logfile")+"5002"
            outfile = os.path.join(os.path.dirname(logfile), os.path.basename(logfile).replace("log", "out"))
            if outfile == logfile:
                outfile = None
        except:
            logfile = None
            outfile = None
        csruntime(ip_addr, port=5002, controlport=5005, attr={'indexed_public':
                  {'node_name': {'name': 'mtrx'},
                   'address': {"locality" : "inside"}}},
                   loglevel=_config_pytest.getoption("loglevel"), logfile=logfile, outfile=outfile,
                   configfile="/tmp/calvin5002.conf")
        rt3 = RT("http://%s:5005" % ip_addr)
        helpers.wait_for_runtime(request_handler, rt3)

        test_script_dir = absolute_filename('scripts/')
        request.addfinalizer(self.teardown)

    def teardown(self):
        global rt1
        global rt2
        global rt3
        helpers.teardown_test_type("local", [rt1, rt2, rt3], request_handler)
        # They will die eventually (about 5 seconds) in most cases, but this makes sure without wasting time
        os.system("pkill -9 -f 'csruntime -n %s -p 5000'" % (ip_addr,))
        os.system("pkill -9 -f 'csruntime -n %s -p 5001'" % (ip_addr,))
        os.system("pkill -9 -f 'csruntime -n %s -p 5002'" % (ip_addr,))
        time.sleep(0.2)

    @pytest.mark.slow
    def testDeployEmptySimple(self):
        _log.analyze("TESTRUN", "+", {})
        global rt1
        global rt2
        global rt3
        verify_storage([rt1, rt2, rt3])
        
        with open(test_script_dir+"test_deploy1.calvin", 'r') as app_file:
            script = app_file.read()
        result = {}
        try:
            # Empty requirements
            result = request_handler.deploy_application(rt1, name="test_deploy1", script=script, deploy_info={'requirements': {}})
        except:
            _log.exception("Test deploy failed")
            raise Exception("Failed deployment of app %s, no use to verify if requirements fulfilled" % "test_deploy1")
        # src -> rt2 or rt3, sum & snk -> rt1, rt2 or rt3
        assert_helper([rt2, rt3], lambda actors: result['actor_map']['test_deploy1:src'] in actors)
        assert_helper([rt1, rt2, rt3], lambda actors: result['actor_map']['test_deploy1:sum'] in actors)
        assert_helper([rt1, rt2, rt3], lambda actors: result['actor_map']['test_deploy1:snk'] in actors)
        request_handler.delete_application(rt1, result['application_id'])

    @pytest.mark.slow
    def testDeploy3NodesProxyStorageShadow(self):
        _log.analyze("TESTRUN", "+", {})
        global rt1
        global rt2
        global rt3
        global test_script_dir

        verify_storage([rt1, rt2, rt3])

        from calvin.Tools.cscontrol import control_deploy as deploy_app
        args = DeployArgs(node='http://%s:5003' % ip_addr,
                          script=open(test_script_dir+"test_shadow4.calvin"), attr=None,
                                reqs=test_script_dir+"test_shadow4.deployjson",
                                check=False, timeout=TEST_TIMEOUT)
        result = {}
        try:
            result = deploy_app(args)
        except:
            raise Exception("Failed deployment of app %s, no use to verify if requirements fulfilled" % args.script.name)
        #print "RESULT:", result
        # src -> rt1, sum -> rt2, snk -> rt1
        assert_helper([rt2], lambda actors: result['actor_map']['test_shadow4:button'] in actors)
        assert_helper([rt1], lambda actors: result['actor_map']['test_shadow4:check'] in actors)
        assert_helper([rt3], lambda actors: result['actor_map']['test_shadow4:bell'] in actors)
        
        time.sleep(2)
        actual = request_handler.report(rt3, result['actor_map']['test_shadow4:bell'])
        assert len(actual) > 5
        assert all([y-x > 0 for x, y in zip(actual, actual[1:])])

        request_handler.delete_application(rt1, result['application_id'])

    @pytest.mark.slow
    def testDeploy3NodesProxyStorageMoveAgain(self):
        _log.analyze("TESTRUN", "+", {})
        global rt1
        global rt2
        global rt3
        global test_script_dir

        verify_storage([rt1, rt2, rt3])

        from calvin.Tools.cscontrol import control_deploy as deploy_app
        args = DeployArgs(node='http://%s:5003' % ip_addr,
                          script=open(test_script_dir+"test_shadow4.calvin"), attr=None,
                                reqs=test_script_dir+"test_shadow4.deployjson",
                                check=False, timeout=TEST_TIMEOUT)
        result = {}
        try:
            result = deploy_app(args)
        except:
            raise Exception("Failed deployment of app %s, no use to verify if requirements fulfilled" % args.script.name)

        # src -> rt1, sum -> rt2, snk -> rt1
        assert_helper([rt2], lambda actors: result['actor_map']['test_shadow4:button'] in actors)
        assert_helper([rt1], lambda actors: result['actor_map']['test_shadow4:check'] in actors)
        assert_helper([rt3], lambda actors: result['actor_map']['test_shadow4:bell'] in actors)
        
        time.sleep(2)
        actual = request_handler.report(rt3, result['actor_map']['test_shadow4:bell'])
        assert len(actual) > 5
        request_handler.migrate_use_req(rt3, result['actor_map']['test_shadow4:bell'], 
                                [{
                                    "op": "node_attr_match",
                                    "kwargs": {"index": ["address", {"locality": "outside"}]},
                                    "type": "+"
                                }])
        time.sleep(1)
        assert_helper([rt2], lambda actors: result['actor_map']['test_shadow4:bell'] in actors)
        actual2 = request_handler.report(rt2, result['actor_map']['test_shadow4:bell'])
        assert len(actual2) > len(actual)
        assert all([y-x > 0 for x, y in zip(actual2, actual2[1:])])

        request_handler.delete_application(rt1, result['application_id'])

    @pytest.mark.slow
    def testDeploy3NodesProxyStorageMoveAllAgain(self):
        _log.analyze("TESTRUN", "+", {})
        global rt1
        global rt2
        global rt3
        global test_script_dir

        verify_storage([rt1, rt2, rt3])

        from calvin.Tools.cscontrol import control_deploy as deploy_app
        args = DeployArgs(node='http://%s:5003' % ip_addr,
                          script=open(test_script_dir+"test_shadow4.calvin"), attr=None,
                                reqs=test_script_dir+"test_shadow4.deployjson",
                                check=False, timeout=TEST_TIMEOUT)
        result = {}
        try:
            result = deploy_app(args)
        except:
            raise Exception("Failed deployment of app %s, no use to verify if requirements fulfilled" % args.script.name)
        #print "RESULT:", result
        # src -> rt1, sum -> rt2, snk -> rt1
        assert_helper([rt2], lambda actors: result['actor_map']['test_shadow4:button'] in actors)
        assert_helper([rt1], lambda actors: result['actor_map']['test_shadow4:check'] in actors)
        assert_helper([rt3], lambda actors: result['actor_map']['test_shadow4:bell'] in actors)
        
        time.sleep(2)
        actual = request_handler.report(rt3, result['actor_map']['test_shadow4:bell'])
        assert len(actual) > 5
        request_handler.migrate_app_use_req(rt3, result['application_id'], 
                            {
                                "requirements": {
                                    "button": [
                                        {
                                          "op": "node_attr_match",
                                            "kwargs": {"index": ["address", {"locality": "inside"}]},
                                            "type": "+"
                                       }],
                                        "bell": [
                                        {
                                            "op": "node_attr_match",
                                            "kwargs": {"index": ["address", {"locality": "outside"}]},
                                            "type": "+"
                                        }],
                                        "check": [
                                        {
                                            "op": "node_attr_match",
                                            "kwargs": {"index": ["node_name", {"name": "display"}]},
                                            "type": "+"
                                        }]
                                }
                            })
        assert_helper([rt2], lambda actors: result['actor_map']['test_shadow4:bell'] in actors)
        assert_helper([rt1], lambda actors: result['actor_map']['test_shadow4:check'] in actors)
        assert_helper([rt3], lambda actors: result['actor_map']['test_shadow4:button'] in actors)
        time.sleep(2)
        actual2 = request_handler.report(rt2, result['actor_map']['test_shadow4:bell'])
        assert len(actual2) > len(actual)
        assert all([y-x > 0 for x, y in zip(actual2, actual2[1:])])

        request_handler.delete_application(rt1, result['application_id'])


    @pytest.mark.slow
    def testDeploy3NodesProxyStorageComponentMoveAllAgain(self):
        _log.analyze("TESTRUN", "+", {})
        global rt1
        global rt2
        global rt3
        global test_script_dir

        verify_storage([rt1, rt2, rt3])

        from calvin.Tools.cscontrol import control_deploy as deploy_app
        args = DeployArgs(node='http://%s:5003' % ip_addr,
                          script=open(test_script_dir+"test_shadow5.calvin"), attr=None,
                                reqs=test_script_dir+"test_shadow4.deployjson",
                                check=False, timeout=TEST_TIMEOUT)
        result = {}
        try:
            result = deploy_app(args)
        except:
            raise Exception("Failed deployment of app %s, no use to verify if requirements fulfilled" % args.script.name)
        #print "RESULT:", result
        # src -> rt1, sum -> rt2, snk -> rt1
        assert_helper([rt2], lambda actors: result['actor_map']['test_shadow5:button:first'] in actors)
        assert_helper([rt2], lambda actors: result['actor_map']['test_shadow5:button:second'] in actors)
        assert_helper([rt1], lambda actors: result['actor_map']['test_shadow5:check'] in actors)
        assert_helper([rt3], lambda actors: result['actor_map']['test_shadow5:bell'] in actors)
        
        time.sleep(2)
        actual = request_handler.report(rt3, result['actor_map']['test_shadow5:bell'])
        assert len(actual) > 5
        request_handler.migrate_app_use_req(rt3, result['application_id'], 
                            {
                                "requirements": {
                                    "button": [
                                        {
                                          "op": "node_attr_match",
                                            "kwargs": {"index": ["address", {"locality": "inside"}]},
                                            "type": "+"
                                       }],
                                        "bell": [
                                        {
                                            "op": "node_attr_match",
                                            "kwargs": {"index": ["address", {"locality": "outside"}]},
                                            "type": "+"
                                        }],
                                        "check": [
                                        {
                                            "op": "node_attr_match",
                                            "kwargs": {"index": ["node_name", {"name": "display"}]},
                                            "type": "+"
                                        }]
                                }
                            })
        assert_helper([rt2], lambda actors: result['actor_map']['test_shadow5:bell'] in actors)
        assert_helper([rt1], lambda actors: result['actor_map']['test_shadow5:check'] in actors)
        assert_helper([rt3], lambda actors: result['actor_map']['test_shadow5:button:first'] in actors)
        assert_helper([rt3], lambda actors: result['actor_map']['test_shadow5:button:second'] in actors)

        time.sleep(1)
        actual2 = request_handler.report(rt2, result['actor_map']['test_shadow5:bell'])
        assert len(actual2) > len(actual)
        assert all([y-x > 0 for x, y in zip(actual2, actual2[1:])])

        request_handler.delete_application(rt1, result['application_id'])

    @pytest.mark.slow
    def testDeploy3NodesProxyStorageMoveManyTimes(self):
        _log.analyze("TESTRUN", "+", {})
        global rt1
        global rt2
        global rt3
        global test_script_dir

        verify_storage([rt1, rt2, rt3])

        from calvin.Tools.cscontrol import control_deploy as deploy_app
        args = DeployArgs(node='http://%s:5003' % ip_addr,
                          script=open(test_script_dir+"test_deploy1.calvin"), attr=None,
                                reqs=test_script_dir+"test_deploy4.deployjson",
                                check=False, timeout = 10)
        result = {}
        try:
            result = deploy_app(args)
        except:
            raise Exception("Failed deployment of app %s, no use to verify if requirements fulfilled" % args.script.name)
        print "RESULT:", result

        assert result['requirements_fulfilled']

        # src -> rt1, sum -> rt1, snk -> rt2
        assert_helper([rt2], lambda actors: result['actor_map']['test_deploy1:src'] in actors)
        assert_helper([rt1], lambda actors: result['actor_map']['test_deploy1:sum'] in actors)
        assert_helper([rt2], lambda actors: result['actor_map']['test_deploy1:snk'] in actors)

        for i in range(10):
            request_handler.migrate_app_use_req(rt1, result['application_id'],
                             {"requirements":
                                {"snk":
                                    [{"op": "node_attr_match",
                                     "kwargs": {"index": ["node_name", {"name": "mtrx"}]},
                                     "type": "+"
                                     }]
                                }
                            }, move=False)
            assert_helper([rt3], lambda actors: result['actor_map']['test_deploy1:snk'] in actors)
            request_handler.migrate_app_use_req(rt1, result['application_id'],
                             {"requirements":
                                {"snk":
                                    [{"op": "node_attr_match",
                                     "kwargs": {"index": ["node_name", {"name": "serv"}]},
                                     "type": "+"
                                     }]
                                }
                            }, move=False)
            assert_helper([rt2], lambda actors: result['actor_map']['test_deploy1:snk'] in actors)

        request_handler.delete_application(rt1, result['application_id'])
