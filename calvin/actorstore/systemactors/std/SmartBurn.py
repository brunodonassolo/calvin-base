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
import random, datetime, collections
from calvin.utilities.calvinlogger import get_logger

_log = get_logger(__name__)

TRIGGER_THRESHOLD=13

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
        self.seed = seed
        self.setup()

    def setup(self):
        random.seed(self.seed)
        self.A = [[random.random() for i in range(0,self.size)] for j in range(0,self.size)]
        self.B = [[random.random() for i in range(0,self.size)] for j in range(0,self.size)]
        timestamp = time.time()
        numpy.matmul(self.A, self.B)
        self.processing_time = time.time() - timestamp
        self.token_process_time = collections.deque(maxlen=TRIGGER_THRESHOLD)

    def did_migrate(self):
        self.setup()

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
            if "timestamp" in input:
                import datetime
                elapsed = (datetime.datetime.now() - datetime.datetime.strptime(input["timestamp"][0]["date"], "%Y-%m-%d %H:%M:%S.%f")).total_seconds()
        except:
            pass
        # Burn cycles
        numpy.matmul(self.A, self.B)

        # check processing time and migration conditions
        self.token_process_time.append(elapsed)
        mean = float(sum(self.token_process_time))/len(self.token_process_time)
        if elapsed > TRIGGER_THRESHOLD*self.processing_time:
            _log.info("%s<%s>: Elapsed time in burn higher than threshold, elapsed: %f threshold: %f mean: %f" % (self.__class__.__name__, self.id, elapsed, TRIGGER_THRESHOLD*self.processing_time, mean))
        if mean > TRIGGER_THRESHOLD*self.processing_time:
            _log.warning("%s<%s>: Actor must be migrated, mean: %f, threshold: %f" % (self.__class__.__name__, self.id, mean, TRIGGER_THRESHOLD*self.processing_time))
            self.better_migrate = True
        elif self.better_migrate:
            _log.info("%s<%s>: Actor doesn't need to be migrated anymore, mean: %f, threshold: %f" % (self.__class__.__name__, self.id, mean, TRIGGER_THRESHOLD*self.processing_time))
            self.better_migrate = False

        return (input, )

    action_priority = (donothing, )

    test_set = [
        {
            'setup': [lambda self: self.init(size=100)],
            'inports': {'token': [1, 2, 3]},
            'outports': {'token': [1, 2, 3]}
        }
    ]
