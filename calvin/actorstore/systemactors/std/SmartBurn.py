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

from calvin.actor.actor import Actor, manage, condition, stateguard, calvinsys
import numpy
import random
import re
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
    @manage(['dump', 'last', 'size', 'seed', 'optimized', 'registry'])
    def init(self, size, dump=False, seed=None, optimized=False):
        self.dump = dump
        self.last = None
        self.size = size
        self.seed = seed
        self.registry = calvinsys.open(self, "sys.attribute.indexed")
        self.optimized = optimized
        calvinsys.write(self.registry, "node_name.name")
        self.setup()

    def setup(self):
        random.seed(self.seed)
        attr = calvinsys.read(self.registry)
        actor_name = re.sub(r".+burn", "", self._name)
        size = self.size
        if self.optimized and attr.isdigit() and actor_name.isdigit():
            actor_number = int(actor_name)
            runtime_number = int(attr)
            if (actor_number % 5 == runtime_number % 5):
                size = self.size/20
                _log.info("%s<%s>: Runtime optimized for actor, new size: %d, old size: %d" % (self.__class__.__name__, self.id, size, self.size))

            _log.info("%s<%s>: Actor number: %d, Runtime number: %d, size: %d" % (self.__class__.__name__, self.id, actor_number, runtime_number, size))

        self.A = [[random.random() for i in range(0,size)] for j in range(0,size)]
        self.B = [[random.random() for i in range(0,size)] for j in range(0,size)]

    def will_migrate(self):
        _log.info("%s<%s>: Actor migration triggered" % (self.__class__.__name__, self.id))

    def did_migrate(self):
        self.setup()
        _log.info("%s<%s>: Actor migration finished" % (self.__class__.__name__, self.id))

    def log(self, data):
        _log.info("%s<%s>: %s" % (self.__class__.__name__, self.id, str(data).strip()))

    @condition(['token'], ['token'])
    def donothing(self, input):
        if self.dump:
            self.log(input)
        self.last = input
        elapsed = 0
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
