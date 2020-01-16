################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

from pyflink import keyword
from pyflink.java_gateway import get_gateway
from pyflink.ml import JavaTransformer
from pyflink.ml.param.colname import *


class VectorAssembler(JavaTransformer, HasSelectedCols, HasOutputCol):

    @keyword
    def __init__(self, *, selected_cols=None, output_col=None):
        self.gateway = get_gateway()
        self._j_obj = self.gateway.jvm.org.apache.flink.ml.pipeline.dataproc.VectorAssembler()
        super().__init__(self._j_obj)
        kwargs = self._input_kwargs
        self._set(**kwargs)
