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
from calvin.runtime.north.calvin_token import EOSToken


class SuffixString(Actor):

    """
    Appends <suffix> to input tokens and passes them to ouput port as strings
    Inputs:
      in : Text token
    Outputs:
      out : Suffixed text token
    """
    @manage(['suffix'])
    def init(self, suffix='-'):
        self.suffix = str(suffix)

    def exception_handler(self, action, args):
        return (EOSToken(), )

    @condition(['in'], ['out'])
    def suffix(self, token):
        return (str(token) + self.suffix, )

    action_priority = (suffix, )

    test_kwargs = {'suffix': 'P'}

    test_set = [
        {
            'in': {'in': ['a', 'b', 'c']},
            'out': {'out': ['aP', 'bP', 'cP']}
        }
    ]
