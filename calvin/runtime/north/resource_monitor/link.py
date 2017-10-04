# -*- coding: utf-8 -*-

from calvin.runtime.south.plugins.async import async
from calvin.runtime.north.resource_monitor.helper import ResourceMonitorHelper
from calvin.utilities.calvinlogger import get_logger
from calvin.utilities.calvin_callback import CalvinCB

_log = get_logger(__name__)

class LinkMonitor(object):
    def __init__(self, node_id, storage):
        self.storage = storage
        self.node_id = node_id
        self.acceptable = ['1M', '100M', '1G', '10G', '100G']
        self.helper = ResourceMonitorHelper(storage)

    def _set_bandwidth_cb(self, key, value, bandwidth, org_cb):
        if not value:
            print "Link not found for key: " + key
            if org_cb:
                async.DelayedCall(0, org_cb, bandwidth, False)
            return

        print key
        print "link id found " + str(value)
        self.helper.set(ident=value, prefix="linkBandwidth-", prefix_index="bandwidth", value=bandwidth, cb=org_cb)

    def set_bandwidth(self, runtime1, runtime2, bandwidth, cb=None):
        """
        Sets the link bandwidth
        Acceptable range: ['1M', '100M', '1G', '10G', '100G']
        """
        if bandwidth not in self.acceptable:
            _log.error("Invalid bandwidth value: " + str(bandwidth))
            if cb:
                async.DelayedCall(0, cb, bandwidth, False)
            return

        self.storage.get("rt-link-", runtime1 + runtime2, CalvinCB(func=self._set_bandwidth_cb, bandwidth=bandwidth, org_cb=cb))

    def _verify_links_initialization(self, key, value):
        if not value:
            self._create_links()
            async.DelayedCall(10, self._get_links, self.node_id, self._verify_links_initialization)
            print "Links not initialized yet, trying in 10s..."
        else:
            print "Links already initialized, everything ok"
            print value

    def start(self):
        # Start links if needed
        self._get_links(self.node_id, cb=CalvinCB(self._verify_links_initialization))

    def stop(self):
        """
        Stops monitoring, cleaning storage
        """
        # get old value to cleanup indexes
        #self.helper.set("nodeMemAvail-", "memAvail", value=None, cb=None)
        #self.storage.delete(prefix="nodeMemAvail-", key=self.node_id, cb=None)
        self._delete_links()

    def _create_links_cb(self, key, value, runtime1):
        """
        Create 1 link to each pair of runtimes
        Adds this link to storage: link-ID : { "runtime1_id", "runtime2_id" }
        Also, associate the link with both runtimes to find it easier (on node removal)
        """
        from calvin.utilities import calvinuuid
        links_id = []
        for rt in value:
            if (rt == runtime1):
                print "Skipping same runtime:" + str(runtime1)
                continue
            data = { "runtime1" : runtime1,
                     "runtime2" : rt}
            link_id = calvinuuid.uuid("Link")
            links_id.append(link_id)
            print "Adding link-" + str(link_id)
            print data
            self.storage.set(prefix="link-", key=link_id, value=data, cb = None)
            # search link id by its origin/dst runtimes
            self.storage.set(prefix="rt-link-", key=runtime1 + rt, value=link_id, cb=None)
            self.storage.set(prefix="rt-link-", key=rt + runtime1, value=link_id, cb=None)
            # get all links of 1 runtime, so adds link_id index to both runtimes
            self.storage.add_index(['links', runtime1], link_id, root_prefix_level=2, cb=None)
            self.storage.add_index(['links', rt], link_id, root_prefix_level=2, cb=None)
        return links_id

    def _create_links(self):
        """
        Create links between node_id and all nodes available
        """
        from calvin.utilities.attribute_resolver import format_index_string
        index_str = format_index_string(("node_name", {}))
        self.storage.get_index(index_str, CalvinCB(func=self._create_links_cb, runtime1=self.node_id))

    def _get_links(self, node_id, cb):
        """
        Gets all links related to a node id, i.e. source or destination is the node_id
        """
        self.storage.get_index(['links', node_id], cb=cb)

    def _delete_links_1link_cb(self, key, value):
        """
        Callback for _delete_links_cb
        Do steps: 
        - Remove links from /links-ID/ database
        - Remove from /links/runtimes the link
        """
        print "Removing link: " + str(key)
        print "Removing association with runtime: " + str(value['runtime1'])
        print "Removing association with runtime: " + str(value['runtime2'])
        self.helper.set(key, "linkBandwidth-", "bandwidth", value=None, cb=None)
        self.storage.remove_index(['links', value['runtime1']], key, root_prefix_level=1, cb=None)
        self.storage.remove_index(['links', value['runtime2']], key, root_prefix_level=1, cb=None)
        self.storage.delete('rt-link-', value['runtime1'] + value['runtime2'], cb=None)
        self.storage.delete('rt-link-', value['runtime2'] + value['runtime1'], cb=None)
        self.storage.delete('link-', key, cb=None)
        self.storage.delete('linkBandwidth-', key, cb=None)

    def _delete_links_cb(self, key, value):
        """
        Callback for delete_links
        Do step: Gets all runtimes that could use these links
        """
        if not value:
            print "Empty link list, nothing to do..."
            return
        for link in value:
            self.storage.get("link-", link, CalvinCB(func=self._delete_links_1link_cb))

    def _delete_links(self):
        """
        Delete all links related to specified node
        Steps:
        - Search all links whose src or dst is the node
        - Gets all runtimes that could use these links
        - Remove links from /links-ID/ database
        - Remove from /links/runtimes the link
        """
        self._get_links(self.node_id, cb=CalvinCB(self._delete_links_cb))
