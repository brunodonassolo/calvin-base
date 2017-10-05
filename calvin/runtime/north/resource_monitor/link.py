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
        self.band_acceptable = ['1M', '100M', '1G', '10G', '100G']
        self.lat_acceptable = ['1us', '100us', '1ms', '100ms', '1s']
        self.helper = ResourceMonitorHelper(storage)

    def _set_helper_cb(self, key, value, link_prefix, link_prefix_index, link_value, org_cb):
        if not value:
            _log.error("LinkMonitor (%s, %s): Link not found for key: %s. Value %s not updated" % (link_prefix, link_prefix_index, key, str(link_value)))
            if org_cb:
                async.DelayedCall(0, org_cb, link_value, False)
            return

        _log.debug("LinkMonitor (%s, %s): Link found (%s) for key: %s. Value %s will be updated" % (link_prefix, link_prefix_index, value, key, str(link_value)))
        self.helper.set(ident=value, prefix=link_prefix, prefix_index=link_prefix_index, value=link_value, cb=org_cb)

    def set_bandwidth(self, runtime1, runtime2, bandwidth, cb=None):
        """
        Sets the link bandwidth
        Acceptable range: ['1M', '100M', '1G', '10G', '100G']
        """
        if bandwidth.upper() not in self.band_acceptable:
            _log.error("Invalid bandwidth value: " + str(bandwidth))
            if cb:
                async.DelayedCall(0, cb, bandwidth, False)
            return

        self.storage.get("rt-link-", runtime1 + runtime2, CalvinCB(func=self._set_helper_cb, link_prefix = "linkBandwidth-", link_prefix_index = "bandwidth", link_value=bandwidth.upper(), org_cb=cb))

    def set_latency(self, runtime1, runtime2, latency, cb=None):
        """
        Sets the link latency
        Acceptable range: ['1us', '100us', '1ms', '100ms', '1s']
        """
        if latency.lower() not in self.lat_acceptable:
            _log.error("Invalid latency value: " + str(latency))
            if cb:
                async.DelayedCall(0, cb, latency, False)
            return

        self.storage.get("rt-link-", runtime1 + runtime2, CalvinCB(func=self._set_helper_cb, link_prefix = "linkLatency-", link_prefix_index = "latency", link_value=latency.lower(), org_cb=cb))

    def get_info(self, phys_link, cb):
        """
        Gets information saved in storage about a physical link
        phys_link: Link identifier (UUID)
        cb: callback to receive information
        """
        self.storage.get("phyLink-", phys_link, cb=cb)

    def _verify_links_initialization(self, key, value):
        if not value:
            self._create_links()
            async.DelayedCall(10, self._get_links, self.node_id, self._verify_links_initialization)
            _log.debug("Links not initialized yet for node %s, trying again later..." % (key))
        else:
            _log.debug("Links already initialized for node %s: %s" % (key, str(value)))

    def start(self):
        # Start links if needed
        self._get_links(self.node_id, cb=CalvinCB(self._verify_links_initialization))

    def stop(self):
        """
        Stops monitoring, cleaning storage
        Delete all links related to specified node
        Steps:
        - Search all links whose src or dst is the node
        - Gets all runtimes that could use these links
        - Remove links from /links-ID/ database
        - Remove from /phyLinks/runtimes the link
        """
        self._get_links(self.node_id, cb=CalvinCB(self._delete_links_cb))

    def _create_links_cb(self, key, value, runtime1):
        """
        Create 1 link to each pair of runtimes
        Adds this link to storage: phyLink-ID : { "runtime1_id", "runtime2_id" }
        Also, associate the link with both runtimes to find it easier (on node removal)
        """
        from calvin.utilities import calvinuuid
        links_id = []
        for rt in value:
            if (rt == runtime1):
                _log.debug("Skipping same runtime:" + str(runtime1))
                continue
            data = { "runtime1" : runtime1,
                     "runtime2" : rt}
            link_id = calvinuuid.uuid("Link")
            links_id.append(link_id)
            self.storage.set(prefix="phyLink-", key=link_id, value=data, cb = None)
            # search link id by its origin/dst runtimes
            self.storage.set(prefix="rt-link-", key=runtime1 + rt, value=link_id, cb=None)
            self.storage.set(prefix="rt-link-", key=rt + runtime1, value=link_id, cb=None)
            # get all links of 1 runtime, so adds link_id index to both runtimes
            self.storage.add_index(['phyLinks', runtime1], link_id, root_prefix_level=2, cb=None)
            self.storage.add_index(['phyLinks', rt], link_id, root_prefix_level=2, cb=None)
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
        self.storage.get_index(['phyLinks', node_id], cb=cb)

    def _delete_links_1link_cb(self, key, value):
        """
        Callback for _delete_links_cb
        Do steps: 
        - Remove links from /phyLink-ID/ database
        - Remove from /phyLinks/runtimes the link
        """
        self.helper.set(key, "linkBandwidth-", "bandwidth", value=None, cb=None)
        self.helper.set(key, "linkLatency-", "latency", value=None, cb=None)
        self.storage.remove_index(['phyLinks', value['runtime1']], key, root_prefix_level=1, cb=None)
        self.storage.remove_index(['phyLinks', value['runtime2']], key, root_prefix_level=1, cb=None)
        self.storage.delete('rt-link-', value['runtime1'] + value['runtime2'], cb=None)
        self.storage.delete('rt-link-', value['runtime2'] + value['runtime1'], cb=None)
        self.storage.delete('phyLink-', key, cb=None)
        self.storage.delete('linkBandwidth-', key, cb=None)
        self.storage.delete('linkLatency-', key, cb=None)

    def _delete_links_cb(self, key, value):
        """
        Callback for delete_links
        Do step: Gets all runtimes that could use these links
        """
        if not value:
            return
        for link in value:
            self.storage.get("phyLink-", link, CalvinCB(func=self._delete_links_1link_cb))

