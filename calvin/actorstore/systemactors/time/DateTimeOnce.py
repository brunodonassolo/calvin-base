# -*- coding: utf-8 -*-

# Copyright (c) 2017 Ericsson AB
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

from calvin.actor.actor import Actor, manage, condition, stateguard, calvinsys, calvinlib
import datetime

class DateTimeOnce(Actor):
    """
    Generates one datetime token and send it

    Detailed information

    Input:

    Output:
      token : sent timestamp that came back from the app
    """

    @manage([])
    def init(self):
        self.setup()

    def did_migrate(self):
        self.setup()

    def setup(self):
        rng = calvinlib.use("math.random")
        delay = rng.random_number(lower=0, upper=1)
        self.timer = calvinsys.open(self, "sys.timer.once")
        calvinsys.write(self.timer, delay)

    @stateguard(lambda self: self.timer != None and calvinsys.can_read(self.timer))
    @condition(action_input=[], action_output=['token'])
    def send(self):
        calvinsys.read(self.timer)
        calvinsys.close(self.timer)
        self.timer = None
        return (str(datetime.datetime.now()),)

    action_priority = (send, )
    requires = ['sys.timer.once', 'math.random']

