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

from calvin.actor.actor import Actor, manage, condition, calvinsys, stateguard

from calvin.utilities.calvinlogger import get_logger

_log = get_logger(__name__)


class Twitter(Actor):
    """
    Post incoming tokens (text) as twitter status

    Input:
      status : A text (with a maximum length)
    """

    @manage([])
    def init(self):
        self.setup()

    def did_migrate(self):
        self.setup()

    def setup(self):
        self._twit = calvinsys.open(self, "web.twitter.post")

    def teardown(self):
        calvinsys.close(self._twit)

    def will_migrate(self):
        self.teardown()

    def will_end(self):
        self.teardown()

    @stateguard(lambda self: self._twit and calvinsys.can_write(self._twit))
    @condition(action_input=['status'])
    def post_update(self, status):
        calvinsys.write(self._twit, status)

    action_priority = (post_update,)
    requires = ['web.twitter.post']


    test_calvinsys = {'web.twitter.post': {'write': ["A twitter message"]}}
    test_set = [
        {
            'inports': {'status': ["A twitter message"]},
        }
    ]
