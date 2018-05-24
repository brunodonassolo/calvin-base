# -*- coding: utf-8 -*-

import unittest
import pytest

from calvin.utilities.attribute_resolver import AttributeResolver

class AttributeResolverTester(unittest.TestCase):

    def test_cpu_resources(self):
        """
        Tests valid cpu resources in the indexed_public field
        """
        att = AttributeResolver({"indexed_public": {"cpuAvail": "100"}})
        att_list = att.get_indexed_public(as_list=True)
        self.assertEqual(att_list[0][2], 'cpuAvail')
        self.assertEqual(att_list[0][3], '0')
        self.assertEqual(att_list[0][4], '25')
        self.assertEqual(att_list[0][5], '50')
        self.assertEqual(att_list[0][6], '75')
        self.assertEqual(att_list[0][7], '100')

        self.assertEqual(att.get_indexed_public()[0], '/node/resource/cpuAvail/0/25/50/75/100')

    def test_cpu_invalid_value(self):
        """
        Tests invalid cpu resources in the indexed_public field
        """
        att = AttributeResolver({"indexed_public": {"cpuAvail": "1"}})
        att_list = att.get_indexed_public(as_list=True)
        self.assertEqual(att_list[0][2], 'cpuAvail')

        self.assertEqual(att.get_indexed_public()[0], '/node/resource/cpuAvail')

    def test_cpu_total(self):
        """
        Tests valid CPU power in the indexed_public field
        """
        att = AttributeResolver({"indexed_public": {"cpuTotal": "100000"}})
        att_list = att.get_indexed_public(as_list=True)
        self.assertEqual(att_list[0][2], 'cpuTotal')
        self.assertEqual(att_list[0][3], '1')
        self.assertEqual(att_list[0][4], '100')
        self.assertEqual(att_list[0][5], '500')
        self.assertEqual(att_list[0][6], '1000')
        self.assertEqual(att_list[0][7], '3000')
        self.assertEqual(att_list[0][8], '6000')
        self.assertEqual(att_list[0][9], '10000')
        self.assertEqual(att_list[0][10], '100000')

        self.assertEqual(att.get_indexed_public()[0], '/node/attribute/cpuTotal/1/100/500/1000/3000/6000/10000/100000')

    def test_cpu_total_invalid_value(self):
        """
        Tests invalid CPU power in the indexed_public field
        """
        att = AttributeResolver({"indexed_public": {"cpuTotal": "2"}})
        att_list = att.get_indexed_public(as_list=True)
        self.assertEqual(att_list[0][2], 'cpuTotal')

        self.assertEqual(att.get_indexed_public()[0], '/node/attribute/cpuTotal')

    def test_mem_avail(self):
        """
        Tests valid RAM resources in the indexed_public field
        """
        att = AttributeResolver({"indexed_public": {"memAvail": "100"}})
        att_list = att.get_indexed_public(as_list=True)
        self.assertEqual(att_list[0][2], 'memAvail')
        self.assertEqual(att_list[0][3], '0')
        self.assertEqual(att_list[0][4], '25')
        self.assertEqual(att_list[0][5], '50')
        self.assertEqual(att_list[0][6], '75')
        self.assertEqual(att_list[0][7], '100')

        self.assertEqual(att.get_indexed_public()[0], '/node/resource/memAvail/0/25/50/75/100')

    def test_cpu_affinity(self):
        """
        Tests cpu affinity parameter in indexed_public field
        """
        att = AttributeResolver({"indexed_public": {"cpuAffinity": "dedicated"}})
        att_list = att.get_indexed_public(as_list=True)
        self.assertEqual(att_list[0][2], 'cpuAffinity')
        self.assertEqual(att_list[0][3], 'dedicated')

        self.assertEqual(att.get_indexed_public()[0], '/node/attribute/cpuAffinity/dedicated')

    def test_mem_avail_invalid_value(self):
        """
        Tests invalid RAM resources in the indexed_public field
        """
        att = AttributeResolver({"indexed_public": {"memAvail": "1"}})
        att_list = att.get_indexed_public(as_list=True)
        self.assertEqual(att_list[0][2], 'memAvail')

        self.assertEqual(att.get_indexed_public()[0], '/node/resource/memAvail')

    def test_mem_total(self):
        """
        Tests valid RAM resources in the indexed_public field
        """
        att = AttributeResolver({"indexed_public": {"memTotal": "10G"}})
        att_list = att.get_indexed_public(as_list=True)
        self.assertEqual(att_list[0][2], 'memTotal')
        self.assertEqual(att_list[0][3], '1K')
        self.assertEqual(att_list[0][4], '100K')
        self.assertEqual(att_list[0][5], '1M')
        self.assertEqual(att_list[0][6], '100M')
        self.assertEqual(att_list[0][7], '1G')
        self.assertEqual(att_list[0][8], '10G')

        self.assertEqual(att.get_indexed_public()[0], '/node/attribute/memTotal/1K/100K/1M/100M/1G/10G')

    def test_mem_total_invalid_value(self):
        """
        Tests invalid RAM resources in the indexed_public field
        """
        att = AttributeResolver({"indexed_public": {"memTotal": "10K"}})
        att_list = att.get_indexed_public(as_list=True)
        self.assertEqual(att_list[0][2], 'memTotal')

        self.assertEqual(att.get_indexed_public()[0], '/node/attribute/memTotal')

    def test_mem_affinity(self):
        """
        Tests RAM affinity parameter in indexed_public field
        """
        att = AttributeResolver({"indexed_public": {"memAffinity": "dedicated"}})
        att_list = att.get_indexed_public(as_list=True)
        print str(att_list)
        self.assertEqual(att_list[0][2], 'memAffinity')
        self.assertEqual(att_list[0][3], 'dedicated')

        self.assertEqual(att.get_indexed_public()[0], '/node/attribute/memAffinity/dedicated')

    def test_bandwidth(self):
        """
        Tests valid bandwidth values in the indexed_public field
        """
        att = AttributeResolver({"indexed_public": {"bandwidth": "1G"}})
        att_list = att.get_indexed_public(as_list=True)
        print str(att_list)
        self.assertEqual(att_list[0][2], 'bandwidth')
        self.assertEqual(att_list[0][3], '100K')
        self.assertEqual(att_list[0][4], '1M')
        self.assertEqual(att_list[0][5], '10M')
        self.assertEqual(att_list[0][6], '100M')
        self.assertEqual(att_list[0][7], '1G')

        self.assertEqual(att.get_indexed_public()[0], '/links/resource/bandwidth/100K/1M/10M/100M/1G')

    def test_bandwidth_invalid_value(self):
        """
        Tests invalid bandwidth in the indexed_public field
        """
        att = AttributeResolver({"indexed_public": {"bandwidth": "10K"}})
        att_list = att.get_indexed_public(as_list=True)
        self.assertEqual(att_list[0][2], 'bandwidth')

        self.assertEqual(att.get_indexed_public()[0], '/links/resource/bandwidth')



    def test_bandwidth_invalid_value(self):
        """
        Tests invalid bandwidth in the indexed_public field
        """
        att = AttributeResolver({"indexed_public": {"bandwidth": "10K"}})
        att_list = att.get_indexed_public(as_list=True)
        self.assertEqual(att_list[0][2], 'bandwidth')

        self.assertEqual(att.get_indexed_public()[0], '/links/resource/bandwidth')

    def test_latency(self):
        """
        Tests valid latency values in the indexed_public field
        """
        att = AttributeResolver({"indexed_public": {"latency": "1us"}})
        att_list = att.get_indexed_public(as_list=True)
        self.assertEqual(att_list[0][2], 'latency')
        self.assertEqual(att_list[0][3], '1s')
        self.assertEqual(att_list[0][4], '100ms')
        self.assertEqual(att_list[0][5], '1ms')
        self.assertEqual(att_list[0][6], '100us')
        self.assertEqual(att_list[0][7], '1us')

        self.assertEqual(att.get_indexed_public()[0], '/links/resource/latency/1s/100ms/1ms/100us/1us')

    def test_latency_invalid_value(self):
        """
        Tests invalid latency in the indexed_public field
        """
        att = AttributeResolver({"indexed_public": {"latency": "10ms"}})
        att_list = att.get_indexed_public(as_list=True)
        self.assertEqual(att_list[0][2], 'latency')

        self.assertEqual(att.get_indexed_public()[0], '/links/resource/latency')

    def test_ram(self):
        """
        Tests valid RAM resources in the indexed_public field
        """
        att = AttributeResolver({"indexed_public": {"ram": "10"}})
        att_list = att.get_indexed_public(as_list=True)
        self.assertEqual(att_list[0][2], 'ram')
        self.assertEqual(att_list[0][3], '1000')

        self.assertEqual(att.get_indexed_public()[0], '/node/resource/ram/1000')

        att = AttributeResolver({"indexed_public": {"ram": "2000"}})
        att_list = att.get_indexed_public(as_list=True)
        self.assertEqual(att_list[0][2], 'ram')
        self.assertEqual(att_list[0][3], '1000')
        self.assertEqual(att_list[0][4], '1000000')

        self.assertEqual(att.get_indexed_public()[0], '/node/resource/ram/1000/1000000')

    def test_cpu(self):
        """
        Tests valid CPU resources in the indexed_public field
        """
        att = AttributeResolver({"indexed_public": {"cpu": "10"}})
        att_list = att.get_indexed_public(as_list=True)
        self.assertEqual(att_list[0][2], 'cpu')
        self.assertEqual(att_list[0][3], '1')
        self.assertEqual(att_list[0][4], '100')

        self.assertEqual(att.get_indexed_public()[0], '/node/resource/cpu/1/100')

        att = AttributeResolver({"indexed_public": {"cpu": "100"}})
        att_list = att.get_indexed_public(as_list=True)
        self.assertEqual(att_list[0][2], 'cpu')
        self.assertEqual(att_list[0][3], '1')
        self.assertEqual(att_list[0][4], '100')

        self.assertEqual(att.get_indexed_public()[0], '/node/resource/cpu/1/100')
