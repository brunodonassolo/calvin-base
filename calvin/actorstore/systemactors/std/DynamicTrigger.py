# -*- coding: utf-8 -*-

# Copyright (c) 2016 Ericsson AB
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
from calvin.utilities.calvinlogger import get_logger
from collections import namedtuple

_log = get_logger(__name__)

class DynamicTrigger(Actor):
    """
    Pass on token every _tick_ seconds based on config file
    Outputs:
        data: given data
    """

    @manage(['filename',  'timer', 'state_timer', 'N', 'state_info', 'timestamps', 'data'])
    def init(self, filename):
        self.filename = filename
        self.data = ""
        self.timer = None
        self.state_timer = None
        self.N = 0
        self.state_info = {}
        self.timestamps = []

        self.setup()

    def setup(self):
        # File format:
        # N - number of states
        # state_name1 interval1 message1 (N times)
        # timestamp state_name (ordered, until EOF)
        with open(self.filename, "r") as f:
            self.N = int(f.readline())
            for i in range(0, self.N):
                split_line = f.readline().replace('\n', '').split(' ')
                interval = float(split_line[1])

                if interval < 0:
                    self.state_info[split_line[0]] = (interval,"")
                else:
                    self.state_info[split_line[0]] = (float(split_line[1]), split_line[2])

            for line in f.readlines():
                split_line = line.replace('\n', '').split(' ')
                self.timestamps.append((float(split_line[0]), split_line[1]))
            f.close()

        #print self.state_info
        #print self.timestamps

        self.timer = calvinsys.open(self, "sys.timer.repeating")
        next_state_change = self.timestamps[0][0]
        self.state_timer = calvinsys.open(self, "sys.timer.once", period = next_state_change)

    @stateguard(lambda self: calvinsys.can_read(self.state_timer))
    @condition([], [])
    def set_state(self):
        calvinsys.read(self.state_timer) # Ack
        calvinsys.close(self.timer)
        calvinsys.close(self.state_timer)
        state = self.timestamps.pop(0)
        if len(self.timestamps) > 0:
            next_state_change = self.timestamps[0][0]
            #print "Setting next timestamp: %f" % (next_state_change - state[0])
            self.state_timer = calvinsys.open(self, "sys.timer.once", period = next_state_change - state[0])
        else:
            self.state_timer = calvinsys.open(self, "sys.timer.once")
        data_state = self.state_info[state[1]]
        if (data_state[0] > 0):
            self.timer = calvinsys.open(self, "sys.timer.repeating", period = data_state[0])
            self.data = data_state[1]
        else:
            self.timer = calvinsys.open(self, "sys.timer.repeating")


    @stateguard(lambda self: calvinsys.can_read(self.timer))
    @condition([], ['data'])
    def trigger(self):
        calvinsys.read(self.timer) # Ack
        _log.info("%s<%s>: %s" % (self.__class__.__name__, self.id, str(self.data).strip()))
        return (self.data, )

    action_priority = (set_state, trigger)
    requires = ['sys.timer.repeating', 'sys.timer.once']

