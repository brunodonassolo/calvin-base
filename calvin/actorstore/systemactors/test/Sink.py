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

from calvin.actor.actor import Actor, manage, condition, stateguard
from calvin.utilities.calvinlogger import get_logger
import datetime, collections

_log = get_logger(__name__)

class Sink(Actor):
    """
    Write tokens to standard output
    Input:
      token : Any token
    """

    def exception_handler(self, action, args):
        # Check args to verify that it is EOSToken
        return action(self, *args)

    @manage(['tokens', 'store_tokens', 'quiet', 'active', 'threshold', 'n_tokens_mean'])
    def init(self, store_tokens=False, quiet=False, active=True, threshold=5, n_tokens_mean=20):
        self.store_tokens = store_tokens
        self.tokens = []
        self.quiet = quiet
        self.active = active
        self.threshold = threshold
        self.n_tokens_mean = n_tokens_mean
        self.setup()

    def will_migrate(self):
        _log.info("%s<%s>: Actor migration triggered" % (self.__class__.__name__, self.id))

    def did_migrate(self):
        self.setup()
        self.better_migrate = Actor.RECONF_STATUS.DONE
        _log.info("%s<%s>: Actor migration finished" % (self.__class__.__name__, self.id))

    def setup(self):
        self.token_process_time = collections.deque([0]*self.n_tokens_mean, maxlen=self.n_tokens_mean)
        if self.quiet:
            self.logger = _log.debug
        else:
            self.logger = _log.info

    @stateguard(lambda self: self.active)
    @condition(action_input=['token'])
    def log(self, token):
        if self.store_tokens:
            self.tokens.append(token)
        self.logger("%s<%s>: %s" % (self.__class__.__name__, self.id, str(token).strip()))
        if "timestamp" in token:
            import datetime
            elapsed = datetime.datetime.now() - datetime.datetime.strptime(token["timestamp"][0]["date"], "%Y-%m-%d %H:%M:%S.%f")
            elapsed = elapsed.total_seconds()
            self.logger("%s<%s>: %f" % (self.__class__.__name__, self.id, elapsed))
            # migration done, reset old values
            if self.better_migrate == Actor.RECONF_STATUS.DONE:
                self.token_process_time = collections.deque([0]*self.n_tokens_mean, maxlen=self.n_tokens_mean)
                self.better_migrate = Actor.RECONF_STATUS.NONE

            # check processing time and migration conditions
            self.token_process_time.append(elapsed)
            mean = float(sum(self.token_process_time))/len(self.token_process_time)
            self._elapsed_time = float(sum(self.token_process_time))/(self.n_tokens_mean - self.token_process_time.count(0))
            if elapsed > self.threshold:
                _log.info("%s<%s>: Elapsed time in sink higher than threshold, elapsed: %f threshold: %f mean: %f" % (self.__class__.__name__, self.id, elapsed, self.threshold, mean))
            if mean > self.threshold and self.better_migrate != Actor.RECONF_STATUS.PENDING:
                _log.warning("%s<%s>: Actor must be migrated, mean: %f, threshold: %f" % (self.__class__.__name__, self.id, mean, self.threshold))
                self.better_migrate = Actor.RECONF_STATUS.REQUESTED
            elif self.better_migrate == Actor.RECONF_STATUS.REQUESTED:
                _log.info("%s<%s>: Actor doesn't need to be migrated anymore, mean: %f, threshold: %f" % (self.__class__.__name__, self.id, mean, self.threshold))
                self.better_migrate = Actor.RECONF_STATUS.NONE

    action_priority = (log, )

    def report(self, **kwargs):
        self.active = kwargs.get('active', self.active)
        if 'port' in kwargs:
            return self.inports['token']._state()
        return self.tokens

    test_kwargs = {'store_tokens': True}

    test_set = [
        {
            'inports': {'token': ['aa', 'ba', 'ca', 'da']},
            'outports': {},
            'postcond': [lambda self: self.tokens == ['aa', 'ba', 'ca', 'da']]
        }
    ]
