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
import time

_log = get_logger(__name__)

class DynamicTrigger(Actor):
    """
    Pass on token every _tick_ seconds based on config file
    Outputs:
        data: given data
    """

    @manage()
    def init(self, filename, verbose=True):
        self.filename = filename
        self.data = ""
        self.timer = None
        self.state_timer = None
        self.number_states = 0
        self.state_info = {}
        self.timestamps = []
        self.seq_number = 0
        self.initial_setup_date = None
        self.last_token_date = None
        self.last_state_date = None
        self.current_token_interval = None
        self.verbose = verbose
        self.setup()

    def _set_timer(self):
        """ Set the timer to send the next token """

        if (self.current_token_interval < 0):
            self.timer = None
            return

        current_time = time.time()
        next_token = self.current_token_interval

        if (self.last_token_date != None):
            # relax the condition to log warning message
            delta = 2*self.current_token_interval
            if ((self.last_token_date + delta) - current_time < 0):
                _log.warning("%s<%s>: Cannot send token in the expected rate: %f, last successful token sent: %f, current time: %f " % (self.__class__.__name__, self.id, self.current_token_interval, self.last_token_date, current_time))

            # I sent one token right now, set the next date to 2xtoken_interval, or 0 if it has already elapsed
            next_token = max(0, (self.last_token_date + 2*self.current_token_interval) - current_time)

        self.timer = calvinsys.open(self, "sys.timer.once", period = next_token)

    def will_migrate(self):
        _log.info("%s<%s>: Actor migration triggered" % (self.__class__.__name__, self.id))

    def did_migrate(self):
        _log.info("%s<%s>: Actor migration finished" % (self.__class__.__name__, self.id))

    def _set_state_timer(self):
        """ Set the timer to next state change """
        current_time = time.time()

        # no more state in the list, do nothing
        if len(self.timestamps) <= 0:
            self.state_timer = calvinsys.open(self, "sys.timer.once")
            return

        while True:
            next_state_change = self.timestamps[0][0] + self.initial_setup_date
            if next_state_change < current_time:
                lost_state = self.timestamps.pop(0)
                _log.error("%s<%s>: Lost state transition, state: %s, estimated time: %f, current time: %f " % (self.__class__.__name__, self.id, lost_state[1], next_state_change, current_time))
            else:
                break

        self.state_timer = calvinsys.open(self, "sys.timer.once", period = next_state_change - current_time)

    def _set_state(self):
        if len(self.timestamps) <= 0:
            return

        current_time = time.time()
        state = self.timestamps.pop(0)

        #log when we are 10% delayed (considering the last state change)
        if self.last_state_date != None:
            delta = 1.1*(self.initial_setup_date + state[0] - self.last_state_date)
            if current_time > (self.initial_setup_date + state[0] + delta):
                _log.warning("%s<%s>: Delayed state change, state:%s, expected time: %f, current time:%f" % (self.__class__.__name__, self.id, state[1], self.initial_setup_date + state[0], current_time))

        _log.info("%s<%s>: State change, state:%s, expected time: %f, current time:%f" % (self.__class__.__name__, self.id, state[1], self.initial_setup_date + state[0], current_time))

        self.last_state_date = current_time
        data_state = self.state_info[state[1]]
        self.data = data_state[1]
        self.current_token_interval = data_state[0]
        self.last_token_date = None

    def setup(self):
        # File format:
        # N - number of states
        # state_name1 interval1 message1 (N times)
        # timestamp state_name (ordered, until EOF)
        with open(self.filename, "r") as f:
            self.number_states = int(f.readline())
            for i in range(0, self.number_states):
                split_line = f.readline().replace('\n', '').split(' ')
                interval = float(split_line[1])

                _log.info("%s<%s>: Setup DynamicTrigger, state: %s, token interval: %f" % (self.__class__.__name__, self.id, split_line[0], interval))
                if interval < 0:
                    self.state_info[split_line[0]] = (interval,"")
                else:
                    self.state_info[split_line[0]] = (float(split_line[1]), split_line[2])

            for line in f.readlines():
                split_line = line.replace('\n', '').split(' ')
                self.timestamps.append((float(split_line[0]), split_line[1]))
            f.close()

        self.initial_setup_date = time.time()
        self._set_state_timer()
        #print self.state_info
        #print self.timestamps


    @stateguard(lambda self: calvinsys.can_read(self.state_timer))
    @condition([], [])
    def set_state(self):
        calvinsys.read(self.state_timer) # Ack
        calvinsys.close(self.timer)
        calvinsys.close(self.state_timer)
        self._set_state()
        self._set_state_timer()
        self._set_timer()

    @stateguard(lambda self: self.timer is not None and calvinsys.can_read(self.timer))
    @condition([], ['data'])
    def trigger(self):
        calvinsys.read(self.timer) # Ack
        calvinsys.close(self.timer) # Ack
        current_time = time.time()
        # log if we are more than 50% token_interval late
        delta = 1.5*self.current_token_interval
        if (self.verbose and self.last_token_date != None and (current_time > (self.last_token_date + delta))):
            _log.info("%s<%s>: Delay sending token, token interval: %f, expected date: %f, current date: %f" % (self.__class__.__name__, self.id, self.current_token_interval, self.last_token_date + self.current_token_interval, current_time))

        import datetime
        message = { "seq_number" : self.seq_number, "timestamp":  [ {"uid" : self.id,"date" : str(datetime.datetime.now())}], "data" : self.data}
        if self.verbose:
            _log.info("%s<%s>: %s" % (self.__class__.__name__, self.id, str(message).strip()))
        self.seq_number += 1
        self._set_timer()
        self.last_token_date = current_time #save last token date only in the end. _set_timer uses it to configure next token interval and log warning message
        return (message, )

    action_priority = (set_state, trigger)
    requires = ['sys.timer.once']

