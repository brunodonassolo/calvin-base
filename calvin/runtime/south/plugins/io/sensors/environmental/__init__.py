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

"""
        Abstraction for the diffrent frameworks that can be used by the system.

"""

import os

from calvin.utilities import calvinconfig

_CONF = calvinconfig.get()
_FW_PATH = _CONF.get(None, 'environmental_sensor_plugin')

if _FW_PATH is not None:
    # Spec
    _MODULES = {'environmental': ['Environmental']}

    _FW_MODULES = []
    __all__ = []

    if not _FW_MODULES:
        DIRNAME = os.path.dirname(__file__) + "/"
        for fw_module, _, _ in os.walk(DIRNAME):
            if "impl" in fw_module:
                _FW_MODULES.append(fw_module.replace(DIRNAME, ""))

    if _FW_PATH not in _FW_MODULES:
        raise Exception("No framework '%s' with that name, avalible ones are '%s'" % (_FW_PATH, _FW_MODULES))

    for module, _classes in _MODULES.items():
        import_path = _FW_PATH.replace("/", ".")
        module_obj = __import__("%s.%s" % (import_path, module), globals=globals(), fromlist=[''])
        globals()[module] = module_obj
        __all__.append(module_obj)
