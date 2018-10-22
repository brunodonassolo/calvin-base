# -*- coding: utf-8 -*-

from calvin.runtime.south.plugins.async import async
from calvin.runtime.north.resource_monitor.helper import ResourceMonitorHelper
from calvin.utilities.calvinlogger import get_logger
from calvin.utilities.calvin_callback import CalvinCB
from calvin.requests import calvinresponse

_log = get_logger(__name__)

BAND_ACCEPTABLE = ['100K', '1M', '10M', '100M', '1G']
# valid values in kbits
BAND_VALUES = [100, 1000, 10000, 100000, 1000000]
LAT_ACCEPTABLE = ['100us', '1ms', '10ms', '50ms', '100ms', '1s']
# valid values in microseconds
LAT_VALUES = [100, 1000, 10000, 50000, 100000, 1000000]

def latency_number2text(lat):
    minus = [abs(i - lat) for i in LAT_VALUES]
    return LAT_ACCEPTABLE[minus.index(min(minus))]

def bandwidth_number2text(band):
    minus = [abs(i - band) for i in BAND_VALUES]
    return BAND_ACCEPTABLE[minus.index(min(minus))]

def bandwidth_text2number(text):
    return BAND_VALUES[BAND_ACCEPTABLE.index(text)]

def latency_text2number(text):
    if text in LAT_ACCEPTABLE:
        return LAT_VALUES[LAT_ACCEPTABLE.index(text)]
    else:
        for idx, value in enumerate(LAT_VALUES):
            if int(text) < value:
                break
        if idx == 0 or idx == len(LAT_VALUES) - 1:
            return LAT_VALUES[idx]
        else:
            return LAT_VALUES[idx - 1]

class LinkMonitor(object):
    def __init__(self, node_id, storage):
        self.storage = storage
        self.node_id = node_id
        self.helper = ResourceMonitorHelper(storage)

    def _set_helper_cb(self, key, value, link_prefix, link_prefix_index, link_value, discretizer, org_cb):
        if not value:
            _log.error("LinkMonitor (%s, %s): Link not found for key: %s. Value %s not updated" % (link_prefix, link_prefix_index, key, str(link_value)))
            if org_cb:
                async.DelayedCall(0, org_cb, link_value, value=None)
            return

        _log.debug("LinkMonitor (%s, %s): Link found (%s) for key: %s. Value %s will be updated" % (link_prefix, link_prefix_index, value, key, str(link_value)))
        self.helper.set(ident=value, prefix=link_prefix, prefix_index=link_prefix_index, value=link_value, discretizer=discretizer, force=True, cb=org_cb)

    def set_bandwidth(self, runtime1, runtime2, bandwidth, cb=None):
        """
        Sets the link bandwidth
        Acceptable range: ['1M', '100M', '1G', '10G', '100G']
        """
        #if bandwidth.upper() not in self.band_acceptable:
        #    _log.error("Invalid bandwidth value: " + str(bandwidth))
        #    if cb:
        #        async.DelayedCall(0, cb, bandwidth, False)
        #    return
        if bandwidth in BAND_ACCEPTABLE:
            bandwidth = bandwidth_text2number(bandwidth)
        else:
            bandwidth = int(bandwidth)

        self.storage.get("rt-link-", runtime1 + runtime2, CalvinCB(func=self._set_helper_cb, link_prefix = "linkBandwidth-", link_prefix_index = "bandwidth", link_value=bandwidth, discretizer=bandwidth_number2text, org_cb=cb))

    def set_latency(self, runtime1, runtime2, latency, cb=None):
        """
        Sets the link latency
        Acceptable range: ['100us', '1ms', '10ms', '50ms', '100ms', '1s']
        """
        #if latency.lower() not in self.lat_acceptable:
        #    _log.error("Invalid latency value: " + str(latency))
        #    if cb:
        #        async.DelayedCall(0, cb, latency, False)
        #    return

        if latency in LAT_ACCEPTABLE:
            latency = latency_text2number(latency)
        else:
            latency = int(latency)

        self.storage.get("rt-link-", runtime1 + runtime2, CalvinCB(func=self._set_helper_cb, link_prefix = "linkLatency-", link_prefix_index = "latency", link_value=latency, discretizer=latency_number2text, org_cb=cb))

    def get_info(self, phys_link, cb):
        """
        Gets information saved in storage about a physical link
        phys_link: Link identifier (UUID)
        cb: callback to receive information
        """
        self.storage.get("phyLink-", phys_link, cb=cb)

    def _verify_links_init_step1(self, value):
        """
        This callback recovers the list of availables runtimes
        and gets the number of links already create for this runtime
        """
        self._get_links(self.node_id, cb=CalvinCB(self._verify_links_init_step2, n_runtimes = value))

    def _verify_links_init_step2(self, value, n_runtimes):
        """
        In step2, we verify whether we already create all links between runtimes.
        Each runtime will have n-1 links.
        """
        if not value or len(value) < len(n_runtimes) - 1:
            self._create_links(n_runtimes)
            from calvin.utilities.attribute_resolver import format_index_string
            index_str = format_index_string(("node_name", {}))
            async.DelayedCall(10, self.storage.get_index, index=index_str, cb = self._verify_links_init_step1)
            _log.debug("Links not initialized yet for node %s, trying again later... Expected: %d, created: %d" % (self.node_id, len(n_runtimes) - 1, len(value)))
        else:
            _log.debug("Links already initialized for node %s: %s" % (self.node_id, str(value)))

    def start(self):
        # Start links if needed
        from calvin.utilities.attribute_resolver import format_index_string
        index_str = format_index_string(("node_name", {}))
        self.storage.get_index(index_str, cb=CalvinCB(func=self._verify_links_init_step1))

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

    def _create_links_cb(self, key, value, rt):
        """
        Creates link between this node and runtime rt if it doesn't exist
        Returns the link_id used for tests (test_resource_monitor.py)
        """
        if value:
            _log.debug("Link (%s) already created between runtimes %s:" % (str(value),str(key)))
            return
        data = { "runtime1" : self.node_id,
                "runtime2" : rt}
        from calvin.utilities import calvinuuid
        link_id = calvinuuid.uuid("Link")
        # set default values for bandwidth (max possible) and latency (0)
        self.helper.set(link_id, "linkBandwidth-", "bandwidth", value=BAND_VALUES[-1], discretizer=bandwidth_number2text, force=True, cb=None)
        self.helper.set(link_id, "linkLatency-", "latency", value=0, discretizer=latency_number2text, force=True, cb=None)

        self.storage.set(prefix="phyLink-", key=link_id, value=data, cb = None)
        # search link id by its origin/dst runtimes
        self.storage.set(prefix="rt-link-", key=self.node_id + rt, value=link_id, cb=None)
        self.storage.set(prefix="rt-link-", key=rt + self.node_id, value=link_id, cb=None)
        # get all links of 1 runtime, so adds link_id index to both runtimes
        self.storage.add_index(['phyLinks', self.node_id], link_id, root_prefix_level=2, cb=None)
        self.storage.add_index(['phyLinks', rt], link_id, root_prefix_level=2, cb=None)
        return link_id

    def _create_links(self, runtimes):
        """
        Create 1 link to each pair of runtimes
        Adds this link to storage: phyLink-ID : { "runtime1_id", "runtime2_id" }
        Also, associate the link with both runtimes to find it easier (on node removal)
        """
        for rt in runtimes:
            if (rt == self.node_id):
                _log.debug("Skipping same runtime:" + str(self.node_id))
                continue
            self.storage.get("rt-link-", key= self.node_id + rt, cb=CalvinCB(func=self._create_links_cb, rt = rt))

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
        self.helper.set(key, "linkBandwidth-", "bandwidth", value=None, discretizer=bandwidth_number2text, force=True, cb=None)
        self.helper.set(key, "linkLatency-", "latency", value=None, discretizer=latency_number2text, force=True, cb=None)
        self.storage.remove_index(['phyLinks', value['runtime1']], key, root_prefix_level=1, cb=None)
        self.storage.remove_index(['phyLinks', value['runtime2']], key, root_prefix_level=1, cb=None)
        self.storage.delete('rt-link-', value['runtime1'] + value['runtime2'], cb=None)
        self.storage.delete('rt-link-', value['runtime2'] + value['runtime1'], cb=None)
        self.storage.delete('phyLink-', key, cb=None)
        self.storage.delete('linkBandwidth-', key, cb=None)
        self.storage.delete('linkLatency-', key, cb=None)

    def _delete_links_cb(self, value):
        """
        Callback for delete_links
        Do step: Gets all runtimes that could use these links
        """
        if not value:
            return
        for link in value:
            self.storage.get("phyLink-", link, CalvinCB(func=self._delete_links_1link_cb))

