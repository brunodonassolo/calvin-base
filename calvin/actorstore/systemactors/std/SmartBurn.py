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
import numpy
import random
from calvin.utilities.calvinlogger import get_logger

_log = get_logger(__name__)

class SmartBurn(Actor):
    """
    forward a token unchanged and Burns cycles
    Inputs:
      token : a token
    Outputs:
      token : the same token
    """
    @manage(['dump', 'last', 'size', 'seed'])
    def init(self, size, dump=False, seed=None):
        self.dump = dump
        self.last = None
        self.size = size
        random.seed(seed)
        self.A = [[random.random() for i in range(0,size)] for j in range(0,size)]
        self.B = [[random.random() for i in range(0,size)] for j in range(0,size)]

    def log(self, data):
        _log.info("%s<%s>: %s" % (self.__class__.__name__, self.id, str(data).strip()))

    @condition(['token'], ['token'])
    def donothing(self, input):
        if self.dump:
            self.log(input)
        self.last = input
        try:
            import datetime
            input["timestamp"].append({"uid": self.id, "date": str(datetime.datetime.now())})
        except:
            pass
        # Burn cycles
        numpy.matmul(self.A, self.B)
        return (input, )

    action_priority = (donothing, )

    test_set = [
        {
            'setup': [lambda self: self.init(size=100)],
            'inports': {'token': [1, 2, 3]},
            'outports': {'token': [1, 2, 3]}
        }
    ]
