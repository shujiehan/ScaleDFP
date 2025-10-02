#Copyright 2025 Northwestern Polytechnical University
#@author Shujie Han
#
#Licensed under the Apache License, Version 2.0 (the "License");
#you may not use this file except in compliance with the License.
#You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#Unless required by applicable law or agreed to in writing, software
#distributed under the License is distributed on an "AS IS" BASIS,
#WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#See the License for the specific language governing permissions and
#limitations under the License.

import sys
sys.path.append("..")
import pandas as pd
import numpy as np
from abc import ABCMeta, abstractmethod
from instances.instance import Instance
from instances.instances import Instances
from itertools import islice


class AbstractPredict(metaclass=ABCMeta):
    """
    This class is used to unit test for validating correctness.
    """

    def __init__(self):
        # keep a sequence of instances of one disk
        # dict{sn:Instances}
        self.keep_delay = {}
        return

    def keep(self, inst, queue_size):
        if inst.sn in self.keep_delay.keys():
            self.keep_delay[inst.sn].enqueue(inst)
        else:
            instances = Instances(inst.sn, queue_size)
            instances.enqueue(inst)
            self.keep_delay[inst.sn] = instances

    def inspect(self, data, class_name, num_classes, inspect_start_idx,
                validation_window):
        sns = data['serial_number'].values
        data = data.drop(['serial_number'], axis=1)
        if inspect_start_idx > 0:
            for index, row in islice(data.iterrows(), inspect_start_idx, None):
                inst = Instance(1, sns[index], row, class_name, num_classes)
                self.keep(inst, validation_window)
