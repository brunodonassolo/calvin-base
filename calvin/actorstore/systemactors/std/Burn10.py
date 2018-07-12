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
import time  # NEVER DO THIS OUTSIDE OF TEST
from calvin.utilities.calvinlogger import get_logger

_log = get_logger(__name__)

class Burn10(Actor):
    """
    forward a token unchanged and Burns cycles
    Inputs:
      token_1 : a token
      token_2 : a token
      token_3 : a token
      token_4 : a token
      token_5 : a token
      token_6 : a token
      token_7 : a token
      token_8 : a token
      token_9 : a token
      token_10 : a token
    Outputs:
      token : the same token
    """
    @manage(['dump', 'last', 'duration'])
    def init(self, dump=False, duration=1000000):
        self.dump = dump
        self.last = None
        self.duration = duration

    def log(self, data):
        _log.info("%s<%s>: %s" % (self.__class__.__name__, self.id, str(data).strip()))

    @condition(['token_1', 'token_2', 'token_3', 'token_4', 'token_5', 'token_6', 'token_7', 'token_8', 'token_9', 'token_10'], ['token'])
    def donothing(self, token_1, token_2, token_3, token_4, token_5, token_6, token_7, token_8, token_9, token_10):
        if self.dump:
            self.log(input)
        self.last = input
        # Burn cycles
        t = time.time()
        while time.time() - t < self.duration:
            pass
        return (token_1, )

    def report(self, **kwargs):
        self.duration = kwargs.get('duration', self.duration)
        return self.last

    action_priority = (donothing, )

    test_set = [
        {
            'setup': [lambda self: self.init(duration=1)],
            'inports': {'token': [1, 2, 3]},
            'outports': {'token': [1]}
        }
    ]
