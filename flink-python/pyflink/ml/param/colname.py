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

from pyflink.ml.param import WithParams, ParamInfo, TypeConverters


class HasSelectedCols(WithParams):
    selected_cols = ParamInfo("selectedCols", "Names of the columns used for processing", type_converter=TypeConverters.to_list_string)

    def set_selected_cols(self, v):
        return super().set(self.selected_cols, v)

    def get_selected_cols(self):
        return super().get(self.selected_cols)


class HasOutputCol(WithParams):
    output_col = ParamInfo("outputCol", "Name of the output column", type_converter=TypeConverters.to_string)

    def set_output_col(self, v):
        return super().set(self.output_col, v)

    def get_output_col(self):
        return super().get(self.output_col)


class HasVectorCol(WithParams):
    vector_col = ParamInfo("vectorCol", "Name of a vector column", type_converter=TypeConverters.to_string)

    def set_vector_col(self, v):
        return super().set(self.vector_col, v)

    def get_vector_col(self):
        return super().get(self.vector_col)


class HasKDefaultAs2(WithParams):
    k = ParamInfo("k", "Number of clusters.", type_converter=TypeConverters.to_int)

    def set_k(self, v):
        return super().set(self.k, v)

    def get_k(self):
        return super().get(self.k)


class HasPredictionCol(WithParams):
    prediction_col = ParamInfo("predictionCol", "Column name of prediction.", type_converter=TypeConverters.to_string)

    def set_prediction_col(self, v):
        return super().set(self.prediction_col, v)

    def get_prediction_col(self):
        return super().get(self.prediction_col)


class HasReservedCols(WithParams):
    reserved_cols = ParamInfo("reservedCols", "Names of the columns to be retained in the output table", type_converter=TypeConverters.to_list_string)

    def set_reserved_cols(self, v):
        return super().set(self.reserved_cols, v)

    def get_reserved_cols(self):
        return super().get(self.reserved_cols)

