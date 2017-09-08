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

    def test_mem_resources(self):
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

    def test_mem_invalid_value(self):
        """
        Tests invalid RAM resources in the indexed_public field
        """
        att = AttributeResolver({"indexed_public": {"memAvail": "1"}})
        att_list = att.get_indexed_public(as_list=True)
        self.assertEqual(att_list[0][2], 'memAvail')

        self.assertEqual(att.get_indexed_public()[0], '/node/resource/memAvail')

