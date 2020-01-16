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


class WithParams(object):
    """
    Parameters are widely used in machine learning realm. This class defines a common interface to
    interact with classes with parameters.
    """

    def get_params(self):
        pass

    def set(self, k, v):
        self.get_params().set(k, v)
        return self

    def get(self, k):
        return self.get_params().get(k)

    def _set(self, **kwargs):
        """
        Sets user-supplied params.
        """
        for param, value in kwargs.items():
            p = getattr(self, param)
            if value is not None:
                try:
                    value = p.type_converter(value)
                except TypeError as e:
                    raise TypeError('Invalid param value given for param "%s". %s' % (p.name, e))
            self.get_params().set(p, value)
        return self


class Params(object):
    """
    The map-like container class for parameter. This class is provided to unify the interaction with
    parameters.
    """
    def __init__(self):
        self._paramMap = {}

    def set(self, k, v):
        self._paramMap[k] = v

    def get(self, k):
        return self._paramMap[k]


class ParamInfo(object):
    """
    Definition of a parameter, including name, description, type_converter and so on.
    """
    def __init__(self, name, description, type_converter=None):
        self.name = str(name)
        self.description = str(description)
        self.type_converter = TypeConverters.identity if type_converter is None else type_converter


class TypeConverters(object):
    """
    .. note:: DeveloperApi

    Factory methods for common type conversion functions for `Param.typeConverter`.

    .. versionadded:: 2.0.0
    """

    @staticmethod
    def _is_numeric(value):
        vtype = type(value)
        return vtype in [int, float] or vtype.__name__ == 'long'

    @staticmethod
    def _is_integer(value):
        return TypeConverters._is_numeric(value) and float(value).is_integer()

    @staticmethod
    def _can_convert_to_list(value):
        vtype = type(value)
        return vtype in [list, tuple, range]

    @staticmethod
    def _can_convert_to_string(value):
        return isinstance(value, str)

    @staticmethod
    def identity(value):
        """
        Dummy converter that just returns value.
        """
        return value

    @staticmethod
    def toList(value):
        """
        Convert a value to a list, if possible.
        """
        if type(value) == list:
            return value
        else:
            raise TypeError("Could not convert %s to list" % value)

    @staticmethod
    def toListFloat(value):
        """
        Convert a value to list of floats, if possible.
        """
        if TypeConverters._can_convert_to_list(value):
            value = TypeConverters.toList(value)
            if all(map(lambda v: TypeConverters._is_numeric(v), value)):
                return [float(v) for v in value]
        raise TypeError("Could not convert %s to list of floats" % value)

    @staticmethod
    def toListListFloat(value):
        """
        Convert a value to list of list of floats, if possible.
        """
        if TypeConverters._can_convert_to_list(value):
            value = TypeConverters.toList(value)
            return [TypeConverters.toListFloat(v) for v in value]
        raise TypeError("Could not convert %s to list of list of floats" % value)

    @staticmethod
    def toListInt(value):
        """
        Convert a value to list of ints, if possible.
        """
        if TypeConverters._can_convert_to_list(value):
            value = TypeConverters.toList(value)
            if all(map(lambda v: TypeConverters._is_integer(v), value)):
                return [int(v) for v in value]
        raise TypeError("Could not convert %s to list of ints" % value)

    @staticmethod
    def toListString(value):
        """
        Convert a value to list of strings, if possible.
        """
        if TypeConverters._can_convert_to_list(value):
            value = TypeConverters.toList(value)
            if all(map(lambda v: TypeConverters._can_convert_to_string(v), value)):
                return [TypeConverters.toString(v) for v in value]
        raise TypeError("Could not convert %s to list of strings" % value)

    @staticmethod
    def toFloat(value):
        """
        Convert a value to a float, if possible.
        """
        if TypeConverters._is_numeric(value):
            return float(value)
        else:
            raise TypeError("Could not convert %s to float" % value)

    @staticmethod
    def toInt(value):
        """
        Convert a value to an int, if possible.
        """
        if TypeConverters._is_integer(value):
            return int(value)
        else:
            raise TypeError("Could not convert %s to int" % value)

    @staticmethod
    def toString(value):
        """
        Convert a value to a string, if possible.
        """
        if isinstance(value, str):
            return value
        else:
            raise TypeError("Could not convert %s to string type" % type(value))

    @staticmethod
    def toBoolean(value):
        """
        Convert a value to a boolean, if possible.
        """
        if type(value) == bool:
            return value
        else:
            raise TypeError("Boolean Param requires value of type bool. Found %s." % type(value))
