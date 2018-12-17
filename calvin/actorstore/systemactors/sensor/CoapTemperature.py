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

from calvin.actor.actor import Actor, manage, condition, stateguard
import txthings.coap as coap
import txthings.resource as resource
from ipaddress import ip_address
from twisted.internet import reactor

class CoapTemperature(Actor):
    """
    forward a token unchanged
    Inputs:
      trigger : any token triggers measurement
    Outputs:
      centigrade : temperature in centigrades
    """
    @manage(['url', 'port', 'temperature'])
    def init(self, url, port):
        self.url = unicode(url)
        self.port = int(port)
        self.temperature = 0.0
        self.setup()

    def getResponse(self, response):
        self.temperature = response.payload
        self.request = False
        self.answer = True

    def noResponse(self, failure):
        self.request = False

    def setup(self):
        self.protocol = coap.Coap(resource.Endpoint(None))
        reactor.listenUDP(0, self.protocol, interface="::")
        self.request = False
        self.answer = False

    def did_migrate(self):
        self.setup()

    @stateguard(lambda self: self.answer is True)
    @condition(action_output=['centigrade'])
    def send_read(self):
        self.answer = False
        return (self.temperature, )

    @stateguard(lambda self: self.request is False and self.answer is False)
    @condition(action_input=['trigger'])
    def read_temperature(self, input):
        self.request = True
        self.answer = False
        request = coap.Message(code=coap.GET)
        request.opt.uri_path = ('sensors/temperature',)
        request.remote = (ip_address(self.url), self.port)
        d = self.protocol.request(request)
        d.addCallback(self.getResponse)
        d.addErrback(self.noResponse)

    action_priority = (send_read, read_temperature, )

