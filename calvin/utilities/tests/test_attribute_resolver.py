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
