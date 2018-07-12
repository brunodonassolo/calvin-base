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

from calvin.actor.actor import Actor, manage, condition
from calvin.utilities.calvinlogger import get_logger
import datetime

_log = get_logger(__name__)

class ElapsedTime(Actor):
    """
    Calculates the elapsed time between token is received and base datetime

    Detailed information

    Input:
      base : base datetime token
      token: processed token

    Output:
    """

    @manage([])
    def init(self):
        return

    def did_migrate(self):
        return

    def setup(self):
        return

    @condition(action_input=['base', 'token'])
    def log(self, base, token):
        elapsed = datetime.datetime.now() - datetime.datetime.strptime(base, "%Y-%m-%d %H:%M:%S.%f")
        _log.info("%s<%s>: %f" % (self.__class__.__name__, self.id, elapsed.total_seconds()))

    action_priority = (log, )
    requires = []

