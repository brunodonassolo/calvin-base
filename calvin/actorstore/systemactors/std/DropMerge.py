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


class DropMerge(Actor):
    """
     Receive 2 tokens and send 1 token unchanged
    Inputs:
      token_1 : a token
      token_2 : a token
    Outputs:
      token :token received from token_1
    """
    @manage([])
    def init(self):
        pass

    @condition(['token_1', 'token_2'], ['token'])
    def donothing(self, token_1, token_2):
        if token_1 > token_2:
            return (token_1, )
        else:
            return (token_2, )

    action_priority = (donothing, )

    test_set = [
        {
            'inports': {'token': [1, 2]},
            'outports': {'token': [1]}
        }
    ]
