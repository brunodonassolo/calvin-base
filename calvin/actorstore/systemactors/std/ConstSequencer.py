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

from calvin.actor.actor import Actor, manage, condition


class ConstSequencer(Actor):
    """
    Consume a token and produce a constant from a sequence
    Input:
      in
    Output:
      out
    """

    @manage()
    def init(self, sequence):
        self.index = 0
        self.constsequence = sequence

    @condition(['in'], ['out'])
    def constantify(self, input):
        constant = self.constsequence[self.index]
        self.index = (self.index + 1) % len(self.constsequence)
        return (constant, )

    action_priority = (constantify, )

    test_args = [[42, 0]]

    test_set = [
        {
            'inports': {'in': [1, 2, 3]},
            'outports': {'out': [42, 0, 42]},
        },
    ]

