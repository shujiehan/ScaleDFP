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

# aggregate instances with same serial number in a evaluated window


class Instances:
    def __init__(self, sn, queue_size):
        self.sn = sn
        self.queue = []
        self.queue_size = queue_size

    def enqueue(self, inst):
        assert (len(self.queue) <= self.queue_size)
        self.queue.append(inst)

    def dequeue(self):
        del self.queue[0]
