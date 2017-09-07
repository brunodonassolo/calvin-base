# -*- coding: utf-8 -*-

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

